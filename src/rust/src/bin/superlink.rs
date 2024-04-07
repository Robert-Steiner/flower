use std::{iter::once, time::Duration};

use flwr::{
    auth::noop,
    config::read_config,
    error::Error,
    middleware::metrics::ServerMetricsLayer,
    server::{new_driver_server, new_fleet_server},
    service::{
        driver::DriverService,
        fleet::FleetService,
        pb::{driver_server::DriverServer, fleet_server::FleetServer},
    },
    state::postgres::Postgres,
    tracing::{init_tracing, tracing_span},
};
use garde::Validate;
use http::header::AUTHORIZATION;
use metrics_exporter_prometheus::PrometheusBuilder;

use tokio::{
    signal::unix::{signal, SignalKind},
    sync::oneshot::{self},
};
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tower::ServiceBuilder;
use tower_http::{
    auth::AsyncRequireAuthorizationLayer,
    request_id::MakeRequestUuid,
    sensitive_headers::SetSensitiveHeadersLayer,
    trace::{DefaultOnFailure, DefaultOnResponse, TraceLayer},
    ServiceBuilderExt,
};
use tracing::{debug, info};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let config = read_config()?;
    if config.show_config {
        println!("{config:#?}");
        config.validate(&())?;
        return Ok(());
    }
    config.validate(&())?;

    init_tracing(&config)?;
    debug!("config: {config:#?}");

    let state = Box::new(Postgres::new(&config.database.uri.unwrap()).await?);

    PrometheusBuilder::new().install()?;

    info!("Configure health service");
    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();

    info!("Configure Driver service");
    let driver = new_driver_server(&config.driver, state.clone());

    info!("Configure Fleet service");
    let fleet = new_fleet_server(&config.fleet, state);

    let middleware = ServiceBuilder::new()
        // https://docs.rs/tower-http/0.4.4/tower_http/metrics/in_flight_requests/index.html
        .layer(ServerMetricsLayer)
        .layer(SetSensitiveHeadersLayer::new(once(AUTHORIZATION)))
        .set_x_request_id(MakeRequestUuid)
        .layer(
            TraceLayer::new_for_grpc()
                .make_span_with(move |request: &_| tracing_span(request, config.logging.verbose))
                .on_response(DefaultOnResponse::new())
                .on_failure(DefaultOnFailure::new()),
        )
        .layer(AsyncRequireAuthorizationLayer::new(noop::noop))
        .propagate_x_request_id()
        .into_inner();

    let (signal_tx, signal_rx) = oneshot::channel();
    let shutdown_handle = tokio::spawn(wait_for_signal(signal_tx));

    let mut builder = Server::builder();

    if let (Some(cert), Some(key)) = (config.server.certificate, config.server.private_key) {
        let cert = std::fs::read_to_string(cert)?;
        let key = std::fs::read_to_string(key)?;
        let identity = Identity::from_pem(cert, key);
        builder = builder.tls_config(ServerTlsConfig::new().identity(identity))?;
    }

    let builder = builder
        .max_frame_size(config.server.max_frame_size)
        .http2_keepalive_interval(
            config
                .server
                .http2_keepalive_interval
                .map(Duration::from_secs),
        )
        .http2_keepalive_timeout(
            config
                .server
                .http2_keepalive_timeout
                .map(Duration::from_secs),
        )
        .tcp_keepalive(config.server.tcp_keepalive.map(Duration::from_secs))
        .timeout(Duration::from_secs(config.server.timeout))
        .layer(middleware)
        .add_service(health_service)
        .add_service(driver)
        .add_service(fleet);

    health_reporter
        .set_serving::<DriverServer<DriverService>>()
        .await;

    health_reporter
        .set_serving::<FleetServer<FleetService>>()
        .await;

    info!("Start gRPC server");
    builder
        .serve_with_shutdown(config.server.bind_to, async {
            signal_rx.await.ok();
            info!("Graceful context shutdown");
        })
        .await?;

    shutdown_handle.await.map_err(Into::into)
}

async fn wait_for_signal(tx: oneshot::Sender<()>) {
    let mut sigint = signal(SignalKind::interrupt()).expect("failed to listen for SIGINT signal");
    let mut sigterm = signal(SignalKind::terminate()).expect("failed to listen for SIGTERM signal");

    tokio::select! {
        _ = sigint.recv() => info!("SIGINT received: shutting down"),
        _ = sigterm.recv() => info!("SIGTERM received: shutting down"),
    }

    let _ = tx.send(());
}

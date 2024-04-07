use std::{iter::once, time::Duration};

use clap::Parser;
use figment::{
    providers::{Env, Format, Serialized, Yaml},
    Figment,
};
use flwr::{
    auth::{noop, unauthorized::unauthorized},
    config::Config,
    error::Error,
    handler::{
        driver::{self, DriverHandler},
        fleet::{self, FleetHandler},
    },
    middleware::metrics::ServerMetricsLayer,
    service::{
        self,
        driver::DriverService,
        fleet::FleetService,
        pb::{driver_server::DriverServer, fleet_server::FleetServer},
    },
    state::postgres::Postgres,
    tracer,
};
use garde::Validate;
use http::header::AUTHORIZATION;
use metrics_exporter_prometheus::PrometheusBuilder;

use tokio::{
    signal::unix::{signal, SignalKind},
    sync::oneshot::{self},
};
use tonic::{
    server::NamedService,
    transport::{Body, Identity, Server, ServerTlsConfig},
};
use tonic_health::pb::health_server::HealthServer;
use tower::ServiceBuilder;
use tower_http::{
    auth::AsyncRequireAuthorizationLayer,
    request_id::MakeRequestUuid,
    sensitive_headers::SetSensitiveHeadersLayer,
    trace::{DefaultOnFailure, DefaultOnResponse, TraceLayer},
    ServiceBuilderExt,
};
use tracing::{debug, info, Span};
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

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
    let driver_handler = DriverHandler::new(state.clone(), driver::Config {});
    let driver_svc = DriverService::new(
        driver_handler,
        service::driver::Config {
            message_expires_after: Duration::from_secs(config.driver.message_expires_after),
        },
    );
    let driver = DriverServer::new(driver_svc)
        .max_decoding_message_size(config.driver.max_decoding_message_size)
        .max_encoding_message_size(config.driver.max_encoding_message_size);

    info!("Configure Fleet service");
    let fleet_handler = FleetHandler::new(state, fleet::Config {});
    let fleet_svc = FleetService::new(
        fleet_handler,
        service::fleet::Config {
            message_expires_after: Duration::from_secs(config.fleet.message_expires_after),
        },
    );
    let fleet = FleetServer::new(fleet_svc)
        .max_decoding_message_size(config.fleet.max_decoding_message_size)
        .max_encoding_message_size(config.fleet.max_encoding_message_size);

    let a = <FleetServer<FleetService>>::NAME;
    let b = <HealthServer<tonic_health::server::HealthService>>::NAME;

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

fn read_config() -> Result<Config, Error> {
    let cli = Config::parse();
    let mut figment = Figment::new().merge(Serialized::defaults(&cli));
    if let Some(file) = cli.config.as_ref() {
        figment = figment.merge(Yaml::file_exact(file));
    }
    figment
        .merge(Env::prefixed("FLWR_").split("__"))
        .extract()
        .map_err(Into::into)
}

fn init_tracing(config: &Config) -> Result<(), Error> {
    let mut subscribers = Vec::new();

    match config.logging.fmt {
        flwr::config::Format::Json => subscribers.push(fmt::layer().json().boxed()),
        flwr::config::Format::Text => subscribers.push(fmt::layer().boxed()),
    }

    if config.tracer.enabled {
        let tracer = tracer::install(&config.tracer)?;
        subscribers.push(OpenTelemetryLayer::new(tracer).boxed());
    }

    tracing_subscriber::registry()
        .with(subscribers)
        .with(EnvFilter::from(&config.logging.level))
        .init();
    Ok(())
}

fn tracing_span(request: &http::Request<Body>, verbose: bool) -> Span {
    let request_id = request
        .headers()
        .get("x-request-id")
        .map(|v| v.to_str().unwrap_or_default())
        .unwrap_or_default();

    if verbose {
        tracing::info_span!(
            "request",
            method = %request.method(),
            uri = %request.uri(),
            version = ?request.version(),
            headers = ?request.headers(),
            request_id = request_id
        )
    } else {
        tracing::info_span!(
            "request",
            method = %request.method(),
            uri = %request.uri(),
            version = ?request.version(),
            request_id = request_id
        )
    }
}

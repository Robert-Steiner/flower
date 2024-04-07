use anyhow::anyhow;
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    runtime,
    trace::{BatchConfig, Sampler, Tracer},
    Resource,
};
use opentelemetry_semantic_conventions::{
    resource::{SERVICE_NAME, SERVICE_VERSION},
    SCHEMA_URL,
};
use tonic::transport::Body;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

use crate::{
    config::{self, Config, Format},
    error::Error,
};

pub fn init_tracing(config: &Config) -> Result<(), Error> {
    let mut subscribers = Vec::new();

    match config.logging.fmt {
        Format::Json => subscribers.push(fmt::layer().json().boxed()),
        Format::Text => subscribers.push(fmt::layer().boxed()),
    }

    if config.tracer.enabled {
        let tracer = install(&config.tracer)?;
        subscribers.push(OpenTelemetryLayer::new(tracer).boxed());
    }

    tracing_subscriber::registry()
        .with(subscribers)
        .with(EnvFilter::from(&config.logging.level))
        .init();
    Ok(())
}

pub fn tracing_span(request: &http::Request<Body>, verbose: bool) -> Span {
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

fn install(config: &config::Tracer) -> Result<Tracer, Error> {
    let endpoint = config
        .endpoint
        .as_ref()
        .ok_or(anyhow!("Expects collector endpoint URL."))?;

    opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_trace_config(
            opentelemetry_sdk::trace::Config::default()
                // Customize sampling strategy
                .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
                    config.sample_ratio,
                ))))
                .with_resource(resource()),
        )
        .with_batch_config(BatchConfig::default())
        .with_exporter(
            opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint(endpoint),
        )
        .install_batch(runtime::Tokio)
        .map_err(Into::into)
}

fn resource() -> Resource {
    Resource::from_schema_url(
        [
            KeyValue::new(SERVICE_NAME, env!("CARGO_PKG_NAME")),
            KeyValue::new(SERVICE_VERSION, env!("CARGO_PKG_VERSION")),
        ],
        SCHEMA_URL,
    )
}

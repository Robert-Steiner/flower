use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
};

use clap::{Parser, ValueEnum};
use serde::{Deserialize, Serialize};

/// Flower Superlink
#[derive(Parser, Debug, Serialize, Deserialize, garde::Validate)]
#[command(author, version, about, long_about = None)]
pub struct Config {
    #[arg(short, long)]
    #[garde(skip)]
    /// yaml config file
    pub config: Option<PathBuf>,

    #[arg(long)]
    #[garde(skip)]
    /// Show config and exit program.
    pub show_config: bool,

    #[clap(flatten)]
    #[garde(skip)]
    pub logging: Logging,

    #[clap(flatten)]
    #[garde(skip)]
    pub fleet: Fleet,

    #[command(flatten)]
    #[garde(skip)]
    pub driver: Driver,

    #[command(flatten)]
    #[garde(skip)]
    pub server: Server,

    #[command(flatten)]
    #[garde(dive)]
    pub database: Database,

    #[command(flatten)]
    #[garde(skip)]
    pub tracer: Tracer,
}

#[derive(Copy, Debug, Clone, ValueEnum, Serialize, Deserialize)]
pub enum Format {
    /// JSON format
    Json,
    /// Text format
    Text,
}

#[derive(Parser, Debug, Serialize, Deserialize)]
#[clap(next_help_heading = "Logging options")]
pub struct Logging {
    #[arg(id = "logging-level", long = "logging-level", default_value_t = String::from("INFO"))]
    pub level: String,

    #[arg(id = "logging-fmt", long = "logging-fmt", value_enum, default_value_t = Format::Text)]
    pub fmt: Format,

    #[arg(id = "logging-verbose", long = "logging-verbose")]
    pub verbose: bool,
}

#[derive(Parser, Debug, Serialize, Deserialize)]
#[clap(next_help_heading = "Tracer options")]
pub struct Tracer {
    /// Flag that determines if tracing is enabled.
    #[arg(id = "tracer-enabled", long = "tracer-enabled")]
    pub enabled: bool,

    /// The Sample ratio.
    #[arg(
        id = "tracer-sample-ratio",
        long = "tracer-sample-ratio",
        default_value_t = 1.0
    )]
    pub sample_ratio: f64,

    /// The Endpoint URL of the tracing collector.
    #[arg(id = "tracer-endpoint", long = "tracer-endpoint")]
    pub endpoint: Option<String>,
}

#[derive(Parser, Debug, Serialize, Deserialize)]
#[clap(next_help_heading = "Server options")]
pub struct Server {
    /// The address of the gRPC server.
    #[arg(long, default_value_t = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 50051) )]
    pub bind_to: SocketAddr,

    /// Connection timeout in seconds.
    #[arg(long, default_value_t = 30)]
    pub timeout: u64,

    /// Sets the maximum frame size to use for HTTP2.
    #[arg(long)]
    pub max_frame_size: Option<u32>,

    /// Sets the SETTINGS_MAX_CONCURRENT_STREAMS option for HTTP2 connections.
    #[arg(long)]
    pub max_concurrent_streams: Option<u32>,

    /// Set whether HTTP2 Ping frames are enabled on accepted connections.
    #[arg(long)]
    pub http2_keepalive_interval: Option<u64>,

    /// Sets a timeout for receiving an acknowledgement of the keepalive ping. Duration in seconds.
    #[arg(long)]
    pub http2_keepalive_timeout: Option<u64>,

    /// Set whether TCP keepalive messages are enabled on accepted connections. Duration in seconds.
    #[arg(long)]
    pub tcp_keepalive: Option<u64>,

    /// The TLS certificate of the server
    #[arg(long)]
    pub certificate: Option<PathBuf>,

    /// The private key of the server
    #[arg(long)]
    pub private_key: Option<PathBuf>,
}

#[derive(Parser, Debug, Serialize, Deserialize)]
#[clap(next_help_heading = "Fleet options")]
pub struct Fleet {
    /// Limits the maximum size of a fleet decoded message.
    #[arg(
        id = "fleet-max-decoding-message-size",
        long = "fleet-max-decoding-message-size",
        default_value_t = 4194304
    )]
    pub max_decoding_message_size: usize,

    /// Limits the maximum size of a fleet encoded message.
    #[arg(
        id = "fleet-max-encoding-message-size",
        long = "fleet-max-encoding-message-size",
        default_value_t = 4194304
    )]
    pub max_encoding_message_size: usize,

    /// The allowed time offset in seconds of a fleet message based on message creation.
    #[arg(
        id = "fleet-message-expires-after",
        long = "fleet-message-expires-after",
        default_value_t = 10 // 10 sec
    )]
    pub message_expires_after: u64,
}

#[derive(Parser, Debug, Serialize, Deserialize)]
#[clap(next_help_heading = "Driver options")]
pub struct Driver {
    /// Limits the maximum size of a driver decoded message.
    #[arg(
        id = "driver-max-decoding-message-size",
        long = "driver-max-decoding-message-size",
        default_value_t = 4194304
    )]
    pub max_decoding_message_size: usize,

    /// Limits the maximum size of a driver encoded message.
    #[arg(
        id = "driver-max-encoding-message-size",
        long = "driver-max-encoding-message-size",
        default_value_t = 4194304
    )]
    pub max_encoding_message_size: usize,

    /// The allowed time offset in seconds of a driver message based on message creation.
    #[arg(
        id = "driver-message-expires-after",
        long = "driver-message-expires-after",
        default_value_t = 10 // 10 sec
    )]
    pub message_expires_after: u64,
}

#[derive(Parser, Debug, Serialize, Deserialize, garde::Validate)]
#[clap(next_help_heading = "Database options")]
pub struct Database {
    /// The URI of the database to connect to.
    #[arg(id = "database-uri", long = "database-uri")]
    #[garde(required)]
    pub uri: Option<String>,
}

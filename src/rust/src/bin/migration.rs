use clap::Parser;
use figment::{
    providers::{Env, Serialized},
    Figment,
};
use flwr::{error::Error, state::postgres::Postgres};
use serde::{Deserialize, Serialize};
use tracing::debug;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let config = read_config()?;
    init_tracing(&config);
    debug!("config: {config:#?}");

    let db: Postgres = Postgres::new(&config.url).await?;
    db.migrate().await
}

fn read_config() -> Result<Config, Error> {
    Figment::new()
        .merge(Serialized::defaults(Config::parse()))
        .merge(Env::prefixed("FLWR_").split("__"))
        .extract()
        .map_err(Into::into)
}

fn init_tracing(config: &Config) {
    let mut subscribers = Vec::new();

    if config.fmt == "json" {
        subscribers.push(fmt::layer().json().boxed())
    } else {
        subscribers.push(fmt::layer().boxed())
    };

    tracing_subscriber::registry()
        .with(subscribers)
        .with(EnvFilter::from(&config.level))
        .init();
}

#[derive(Parser, Debug, Serialize, Deserialize)]
pub struct Config {
    /// URl of the database to connect to.
    #[arg(long, default_value_t = String::from(""))]
    pub url: String,

    #[arg(short, long, default_value_t = String::from("INFO"))]
    pub level: String,

    #[arg(short, long, default_value_t = String::from("cli"))]
    pub fmt: String,
}

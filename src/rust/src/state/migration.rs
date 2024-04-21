use tokio_postgres::NoTls;
use tracing::error;

use crate::{config::Database, error::Error};
mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("./src/state/migrations");
}

pub async fn run_migration(config: &Database) -> Result<(), Error> {
    let (mut client, connection) =
        tokio_postgres::connect(config.uri.as_ref().unwrap(), NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("connection error: {}", e);
        }
    });

    embedded::migrations::runner()
        .run_async(&mut client)
        .await?;
    Ok(())
}

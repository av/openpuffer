//! openpuffer binary — `openpuffer serve` starts the HTTP API.

use anyhow::Result;
use openpuffer::config::{parse_cli, build_s3_from_serve, Commands, ServeArgs};
use openpuffer::storage::Storage;
use openpuffer::{router, AppConfig, AppState};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "openpuffer=info,tower_http=info".into()),
        )
        .init();

    match parse_cli().command {
        Commands::Serve(args) => serve(args).await,
    }
}

async fn serve(args: ServeArgs) -> Result<()> {
    let config: AppConfig = args.app_config();
    let client = build_s3_from_serve(&args).await?;
    let storage = Storage::new(client, config.bucket.clone());

    let state = AppState {
        storage,
        config: config.clone(),
    };

    let app = router(state);
    let listener = tokio::net::TcpListener::bind(&config.listen).await?;
    info!(
        listen = %config.listen,
        bucket = %config.bucket,
        "openpuffer serve listening (stateless, S3-compatible storage)"
    );
    axum::serve(listener, app).await?;
    Ok(())
}
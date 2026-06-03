use anyhow::{Context, Result};
use aws_credential_types::Credentials;
use aws_sdk_s3::Client;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::time::Duration;

use crate::buffer::WriteBufferConfig;

#[derive(Parser, Debug)]
#[command(name = "openpuffer", about = "S3-backed vector and FTS search")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Start the HTTP server
    Serve(ServeArgs),
}

#[derive(Parser, Debug, Clone)]
pub struct ServeArgs {
    #[arg(long, default_value = "0.0.0.0:8080")]
    pub listen: String,

    #[arg(long, env = "OPENPUFFER_S3_ENDPOINT")]
    pub s3_endpoint: String,

    #[arg(long, env = "OPENPUFFER_S3_BUCKET")]
    pub s3_bucket: String,

    #[arg(long, env = "OPENPUFFER_S3_REGION", default_value = "us-east-1")]
    pub s3_region: String,

    #[arg(long, env = "OPENPUFFER_S3_ACCESS_KEY")]
    pub s3_access_key: String,

    #[arg(long, env = "OPENPUFFER_S3_SECRET_KEY")]
    pub s3_secret_key: String,

    /// Local disk cache for index segments (empty = memory-only, no disk I/O).
    #[arg(long, env = "OPENPUFFER_CACHE_DIR", default_value = "/tmp/openpuffer-cache")]
    pub cache_dir: String,

    /// Max namespaces with hot in-memory views (LRU eviction).
    #[arg(long, env = "OPENPUFFER_MAX_PINNED_NAMESPACES", default_value = "32")]
    pub max_pinned_namespaces: usize,

    /// Group-commit max delay before flushing a namespace buffer (milliseconds).
    #[arg(long, env = "OPENPUFFER_WRITE_MAX_DELAY_MS", default_value = "1000")]
    pub write_max_delay_ms: u64,

    /// Group-commit flush when pending upserts+patches+deletes reach this count.
    #[arg(long, env = "OPENPUFFER_WRITE_MAX_BATCH_OPS", default_value = "512")]
    pub write_max_batch_ops: usize,
}

#[derive(Clone)]
pub struct AppConfig {
    pub listen: String,
    pub bucket: String,
    pub cache_dir: Option<PathBuf>,
    pub max_pinned_namespaces: usize,
    pub write_buffer: WriteBufferConfig,
}

pub async fn s3_client(args: &ServeArgs) -> Result<Client> {
    let creds = Credentials::new(
        &args.s3_access_key,
        &args.s3_secret_key,
        None,
        None,
        "openpuffer",
    );

    let shared = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .credentials_provider(creds)
        .region(aws_config::Region::new(args.s3_region.clone()))
        .load()
        .await;

    let s3_conf = aws_sdk_s3::config::Builder::from(&shared)
        .endpoint_url(&args.s3_endpoint)
        .force_path_style(true)
        .build();

    Ok(Client::from_conf(s3_conf))
}

impl ServeArgs {
    pub fn app_config(&self) -> AppConfig {
        let cache_dir = {
            let s = self.cache_dir.trim();
            if s.is_empty() {
                None
            } else {
                Some(PathBuf::from(s))
            }
        };
        AppConfig {
            listen: self.listen.clone(),
            bucket: self.s3_bucket.clone(),
            cache_dir,
            max_pinned_namespaces: self.max_pinned_namespaces,
            write_buffer: self.write_buffer_config(),
        }
    }

    pub fn write_buffer_config(&self) -> WriteBufferConfig {
        WriteBufferConfig {
            max_delay: Duration::from_millis(self.write_max_delay_ms),
            max_batch_ops: self.write_max_batch_ops,
        }
    }
}

pub fn parse_cli() -> Cli {
    Cli::parse()
}

pub async fn build_s3_from_serve(args: &ServeArgs) -> Result<Client> {
    s3_client(args)
        .await
        .context("failed to configure S3 client")
}
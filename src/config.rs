use anyhow::{Context, Result};
use aws_credential_types::Credentials;
use aws_sdk_s3::Client;
use aws_smithy_http_client::{tls, Builder as HttpClientBuilder};
use aws_smithy_runtime_api::client::http::SharedHttpClient;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::Duration;

use crate::buffer::WriteBufferConfig;
use crate::index::vector::{
    ann_version_from_env, ANN_VERSION_V2, DEFAULT_PROBE_COARSE, DEFAULT_PROBE_FINE,
};
use crate::limits::{DEFAULT_MAX_FILTER_BATCH_ROWS, DEFAULT_MAX_UPSERT_ROWS};
use crate::wal::WalCorruptPolicy;

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

    /// Max upsert/patch/delete rows per write request (filter-based ops excluded).
    #[arg(long, env = "OPENPUFFER_MAX_UPSERT_ROWS", default_value = "10000")]
    pub max_upsert_rows: usize,

    /// Max documents per `delete_by_filter` / `patch_by_filter` batch.
    #[arg(long, env = "OPENPUFFER_MAX_FILTER_BATCH_ROWS", default_value = "5000")]
    pub max_filter_batch_rows: usize,

    /// ANN query: coarse centroids to probe (stored in `centroids-l0.bin` on index build).
    #[arg(long, env = "OPENPUFFER_ANN_COARSE_PROBE", default_value_t = DEFAULT_PROBE_COARSE)]
    pub ann_coarse_probe: u32,

    /// ANN query: fine centroids to probe per coarse cell.
    #[arg(long, env = "OPENPUFFER_ANN_FINE_PROBE", default_value_t = DEFAULT_PROBE_FINE)]
    pub ann_fine_probe: u32,

    /// ANN index layout version written at index build (`2` default, `3` for scalable hierarchy).
    #[arg(long, env = "OPENPUFFER_ANN_VERSION", default_value_t = ANN_VERSION_V2)]
    pub ann_version: u8,

    /// WAL replay on corrupt segment: `fail` (default) aborts load; `skip` logs and continues.
    #[arg(long, env = "OPENPUFFER_WAL_CORRUPT_POLICY", default_value = "fail")]
    pub wal_corrupt_policy: String,

    /// ANN query: re-rank probed cluster pool with exact view vectors (higher recall, larger candidate pool).
    #[arg(long, env = "OPENPUFFER_ANN_RERANK", default_value_t = false)]
    pub ann_rerank: bool,
}

/// Default cap on cluster `GetObject` count per probed vector query (`C + C×F + 4`; 4× default plan).
pub const DEFAULT_ANN_MAX_PROBE_CLUSTERS: usize = 64;

/// Max cluster segments fetched per probed query; override with `OPENPUFFER_ANN_MAX_PROBE_CLUSTERS` (≥ 8).
pub fn ann_max_probe_clusters_from_env() -> usize {
    std::env::var("OPENPUFFER_ANN_MAX_PROBE_CLUSTERS")
        .ok()
        .and_then(|s| s.trim().parse::<usize>().ok())
        .filter(|&n| n >= 8)
        .unwrap_or(DEFAULT_ANN_MAX_PROBE_CLUSTERS)
}

/// Whether vector queries re-rank the full probed ANN pool with exact view vectors.
pub fn ann_rerank_from_env() -> bool {
    matches!(
        std::env::var("OPENPUFFER_ANN_RERANK")
            .ok()
            .as_deref()
            .map(str::trim),
        Some("1") | Some("true") | Some("yes") | Some("on")
    )
}

/// ANN probe widths written into vector index metadata at build time.
#[derive(Debug, Clone, Copy)]
pub struct AnnProbeConfig {
    pub coarse: u32,
    pub fine: u32,
}

impl Default for AnnProbeConfig {
    fn default() -> Self {
        Self {
            coarse: DEFAULT_PROBE_COARSE,
            fine: DEFAULT_PROBE_FINE,
        }
    }
}

/// ANN probe widths + on-disk layout version for vector index builds.
#[derive(Debug, Clone, Copy)]
pub struct AnnBuildConfig {
    pub probes: AnnProbeConfig,
    pub ann_version: u8,
}

impl Default for AnnBuildConfig {
    fn default() -> Self {
        Self {
            probes: AnnProbeConfig::default(),
            ann_version: ann_version_from_env(),
        }
    }
}

impl AnnBuildConfig {
    pub fn from_probes(probes: AnnProbeConfig) -> Self {
        Self {
            probes,
            ann_version: ann_version_from_env(),
        }
    }

    pub fn with_ann_version(mut self, ann_version: u8) -> Self {
        self.ann_version = ann_version;
        self
    }
}

#[derive(Clone)]
pub struct LimitsConfig {
    pub max_upsert_rows: usize,
    pub max_filter_batch_rows: usize,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_upsert_rows: DEFAULT_MAX_UPSERT_ROWS,
            max_filter_batch_rows: DEFAULT_MAX_FILTER_BATCH_ROWS,
        }
    }
}

#[derive(Clone)]
pub struct AppConfig {
    pub listen: String,
    pub bucket: String,
    pub cache_dir: Option<PathBuf>,
    pub max_pinned_namespaces: usize,
    pub write_buffer: WriteBufferConfig,
    pub limits: LimitsConfig,
    pub ann_probes: AnnProbeConfig,
    pub ann_build: AnnBuildConfig,
    /// Re-rank probed ANN pool with exact vectors from the namespace view at query time.
    pub ann_rerank: bool,
    pub wal_corrupt_policy: WalCorruptPolicy,
}

/// Process-wide hyper-backed HTTP client (connection pool reused by all S3 `GetObject` calls).
static SHARED_S3_HTTP: OnceLock<SharedHttpClient> = OnceLock::new();

/// Process-wide Smithy HTTP client (hyper keep-alive pool; reused by all S3 traffic).
/// In-flight cold GET parallelism is capped separately via [`cold_s3_concurrency`].
pub fn shared_s3_http_client() -> SharedHttpClient {
    SHARED_S3_HTTP
        .get_or_init(|| {
            HttpClientBuilder::new()
                .tls_provider(tls::Provider::Rustls(
                    tls::rustls_provider::CryptoMode::AwsLc,
                ))
                .build_https()
        })
        .clone()
}

pub async fn s3_client(args: &ServeArgs) -> Result<Client> {
    let creds = Credentials::new(
        &args.s3_access_key,
        &args.s3_secret_key,
        None,
        None,
        "openpuffer",
    );

    let http = shared_s3_http_client();
    let shared = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .credentials_provider(creds)
        .region(aws_config::Region::new(args.s3_region.clone()))
        .http_client(http.clone())
        .load()
        .await;

    let s3_conf = aws_sdk_s3::config::Builder::from(&shared)
        .endpoint_url(&args.s3_endpoint)
        .force_path_style(true)
        .http_client(http)
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
            limits: LimitsConfig {
                max_upsert_rows: self.max_upsert_rows,
                max_filter_batch_rows: self.max_filter_batch_rows,
            },
            ann_probes: AnnProbeConfig {
                coarse: self.ann_coarse_probe.max(1),
                fine: self.ann_fine_probe.max(1),
            },
            ann_build: AnnBuildConfig::from_probes(AnnProbeConfig {
                coarse: self.ann_coarse_probe.max(1),
                fine: self.ann_fine_probe.max(1),
            })
            .with_ann_version(self.ann_version),
            ann_rerank: self.ann_rerank,
            wal_corrupt_policy: WalCorruptPolicy::from_env_str(&self.wal_corrupt_policy),
        }
    }

    pub fn wal_corrupt_policy(&self) -> WalCorruptPolicy {
        WalCorruptPolicy::from_env_str(&self.wal_corrupt_policy)
    }

    pub fn write_buffer_config(&self) -> WriteBufferConfig {
        let max_delay = Duration::from_millis(self.write_max_delay_ms);
        WriteBufferConfig {
            max_delay,
            max_batch_ops: self.write_max_batch_ops,
            min_commit_interval: max_delay,
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
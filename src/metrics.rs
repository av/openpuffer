//! Optional Prometheus metrics (`--features metrics`).
//!
//! `GET /metrics` exposes:
//! - `openpuffer_wal_commits_total`
//! - `openpuffer_index_lag_segments`
//! - `openpuffer_query_duration_seconds` (histogram)
//! - `openpuffer_s3_get_total`

#[cfg(feature = "metrics")]
mod inner {
    use prometheus::{
        register_counter, register_gauge, register_histogram_vec, Encoder, HistogramOpts,
        HistogramVec, Opts, TextEncoder,
    };
    use std::sync::LazyLock;

    pub static WAL_COMMITS: LazyLock<prometheus::Counter> = LazyLock::new(|| {
        register_counter!(Opts::new(
            "openpuffer_wal_commits_total",
            "Total durable WAL commits (group-commit flushes)"
        ))
        .expect("openpuffer_wal_commits_total")
    });

    pub static INDEX_LAG: LazyLock<prometheus::Gauge> = LazyLock::new(|| {
        register_gauge!(Opts::new(
            "openpuffer_index_lag_segments",
            "Max unindexed WAL segments across namespaces (wal_commit_seq - index_cursor)"
        ))
        .expect("openpuffer_index_lag_segments")
    });

    pub static QUERY_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec!(
            HistogramOpts::new(
                "openpuffer_query_duration_seconds",
                "Query execution time (search planner + scoring)"
            )
            .buckets(vec![
                0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
            ]),
            &[]
        )
        .expect("openpuffer_query_duration_seconds")
    });

    pub static S3_GETS: LazyLock<prometheus::Counter> = LazyLock::new(|| {
        register_counter!(Opts::new(
            "openpuffer_s3_get_total",
            "Total S3 GetObject calls for index segment reads"
        ))
        .expect("openpuffer_s3_get_total")
    });

    pub fn inc_wal_commits() {
        WAL_COMMITS.inc();
    }

    pub fn set_index_lag_segments(lag: u64) {
        INDEX_LAG.set(lag as f64);
    }

    pub fn observe_query_duration_seconds(secs: f64) {
        QUERY_DURATION.with_label_values(&[]).observe(secs);
    }

    pub fn inc_s3_get() {
        S3_GETS.inc();
    }

    pub fn render() -> Result<String, prometheus::Error> {
        let _ = &*WAL_COMMITS;
        let _ = &*INDEX_LAG;
        let _ = &*QUERY_DURATION;
        let _ = &*S3_GETS;
        let metric_families = prometheus::gather();
        let mut buf = Vec::new();
        TextEncoder::new().encode(&metric_families, &mut buf)?;
        Ok(String::from_utf8(buf).unwrap_or_default())
    }
}

#[cfg(feature = "metrics")]
pub use inner::*;

#[cfg(not(feature = "metrics"))]
pub fn inc_wal_commits() {}

#[cfg(not(feature = "metrics"))]
pub fn set_index_lag_segments(_lag: u64) {}

#[cfg(not(feature = "metrics"))]
pub fn observe_query_duration_seconds(_secs: f64) {}

#[cfg(not(feature = "metrics"))]
pub fn inc_s3_get() {}

#[cfg(feature = "metrics")]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_render_contains_expected_series() {
        inc_wal_commits();
        inc_s3_get();
        set_index_lag_segments(3);
        observe_query_duration_seconds(0.042);
        let body = render().expect("encode metrics");
        assert!(
            body.contains("openpuffer_wal_commits_total"),
            "missing wal commits: {body}"
        );
        assert!(
            body.contains("openpuffer_index_lag_segments"),
            "missing index lag: {body}"
        );
        assert!(
            body.contains("openpuffer_query_duration_seconds"),
            "missing query histogram: {body}"
        );
        assert!(
            body.contains("openpuffer_s3_get_total"),
            "missing s3 gets: {body}"
        );
        assert!(
            body.contains("openpuffer_wal_commits_total 1"),
            "wal counter should be 1: {body}"
        );
    }
}
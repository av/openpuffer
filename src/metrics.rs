//! Optional Prometheus metrics (`--features metrics`).
//!
//! `GET /metrics` exposes:
//! - `openpuffer_wal_commits_total`
//! - `openpuffer_index_lag_segments`
//! - `openpuffer_query_duration_seconds` (histogram)
//! - `openpuffer_cold_query_duration_seconds` (histogram; cold S3 batch path only)
//! - `openpuffer_s3_get_total`
//! - `openpuffer_cold_s3_keys_fetched` (counter)
//! - `openpuffer_ann_probed_clusters` (counter)

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

    pub static COLD_S3_KEYS_FETCHED: LazyLock<prometheus::Counter> = LazyLock::new(|| {
        register_counter!(Opts::new(
            "openpuffer_cold_s3_keys_fetched",
            "S3 object keys fetched on cold-query batch plans (parallel batch counts each key)"
        ))
        .expect("openpuffer_cold_s3_keys_fetched")
    });

    pub static ANN_PROBED_CLUSTERS: LazyLock<prometheus::Counter> = LazyLock::new(|| {
        register_counter!(Opts::new(
            "openpuffer_ann_probed_clusters",
            "Cluster segments selected by ANN probe planning per vector query"
        ))
        .expect("openpuffer_ann_probed_clusters")
    });

    pub static COLD_QUERY_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec!(
            HistogramOpts::new(
                "openpuffer_cold_query_duration_seconds",
                "Query execution time when namespace was loaded via cold S3 batching"
            )
            .buckets(vec![
                0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
            ]),
            &[]
        )
        .expect("openpuffer_cold_query_duration_seconds")
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

    pub fn add_cold_s3_keys_fetched(n: u64) {
        if n > 0 {
            COLD_S3_KEYS_FETCHED.inc_by(n as f64);
        }
    }

    pub fn add_ann_probed_clusters(n: u64) {
        if n > 0 {
            ANN_PROBED_CLUSTERS.inc_by(n as f64);
        }
    }

    pub fn observe_cold_query_duration_seconds(secs: f64) {
        COLD_QUERY_DURATION.with_label_values(&[]).observe(secs);
    }

    pub fn render() -> Result<String, prometheus::Error> {
        let _ = &*WAL_COMMITS;
        let _ = &*INDEX_LAG;
        let _ = &*QUERY_DURATION;
        let _ = &*COLD_QUERY_DURATION;
        let _ = &*S3_GETS;
        let _ = &*COLD_S3_KEYS_FETCHED;
        let _ = &*ANN_PROBED_CLUSTERS;
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

#[cfg(not(feature = "metrics"))]
pub fn add_cold_s3_keys_fetched(_n: u64) {}

#[cfg(not(feature = "metrics"))]
pub fn add_ann_probed_clusters(_n: u64) {}

#[cfg(not(feature = "metrics"))]
pub fn observe_cold_query_duration_seconds(_secs: f64) {}

#[cfg(feature = "metrics")]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_render_contains_expected_series() {
        inc_wal_commits();
        inc_s3_get();
        add_cold_s3_keys_fetched(12);
        add_ann_probed_clusters(8);
        set_index_lag_segments(3);
        observe_query_duration_seconds(0.042);
        observe_cold_query_duration_seconds(0.128);
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
            body.contains("openpuffer_cold_s3_keys_fetched"),
            "missing cold keys: {body}"
        );
        assert!(
            body.contains("openpuffer_ann_probed_clusters"),
            "missing ann probed: {body}"
        );
        assert!(
            body.contains("openpuffer_cold_query_duration_seconds"),
            "missing cold query histogram: {body}"
        );
        assert!(
            body.contains("openpuffer_wal_commits_total 1"),
            "wal counter should be 1: {body}"
        );
        assert!(
            body.contains("# TYPE openpuffer_cold_s3_keys_fetched counter"),
            "cold keys type: {body}"
        );
        assert!(
            body.contains("# TYPE openpuffer_ann_probed_clusters counter"),
            "ann probed type: {body}"
        );
    }

    #[test]
    fn cold_and_ann_counters_increment_on_record() {
        let cold_before = COLD_S3_KEYS_FETCHED.get();
        let ann_before = ANN_PROBED_CLUSTERS.get();
        add_cold_s3_keys_fetched(3);
        add_ann_probed_clusters(2);
        assert!(
            (COLD_S3_KEYS_FETCHED.get() - cold_before) >= 3.0,
            "cold keys should increase"
        );
        assert!(
            (ANN_PROBED_CLUSTERS.get() - ann_before) >= 2.0,
            "ann probed should increase"
        );
    }
}
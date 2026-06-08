//! Batched parallel S3 fetches for cold queries (turbopuffer multi-roundtrip model).
//!
//! **Round 1:** `meta.json` + WAL snapshot/tail when `consistency: strong`.
//! **Round 2:** `centroids-l0.bin` + latest FTS + filter segment (parallel).
//! **Round 3:** probed `centroids-l1-*` + probed `clusters-*` (parallel sub-batches, one roundtrip).
//! **Round 4 (optional):** unindexed WAL tail when `index_cursor < wal_commit_seq`.
//!
//! Sub-batches inside a round still count as one `storage_roundtrip`.
//!
//! Large key lists are split by [`cold_max_keys_per_round`] (env
//! `OPENPUFFER_COLD_MAX_KEYS_PER_ROUND`, default [`DEFAULT_COLD_MAX_KEYS_PER_ROUND`]).
//! Within each sub-batch, in-flight GETs are capped by [`cold_s3_concurrency`] (env
//! `OPENPUFFER_COLD_S3_CONCURRENCY`, default [`DEFAULT_COLD_S3_CONCURRENCY`]).

use crate::index::filter::FilterSegment;
use crate::index::fts::FtsSegment;
use crate::index::vector::{
    probe_fine_centroids_parts, CentroidIndexL0, CentroidIndexL1, CentroidIndexL2,
    CentroidRouting, ClusterSegment, VectorIndex,
};
use crate::models::{
    ColdPlanDebugOpts, ColdPlanDebugResponse, ColdPlanRoundKeyCounts, ColdPlanVectorProbe,
};
use crate::limits::{validate_namespace_name, validate_s3_object_key, validate_s3_path_segment};
use crate::meta::{effective_vector_fields, meta_key, vector_index_uses_legacy_paths, NamespaceMeta};
use crate::namespace::fetch_meta;
use anyhow::{Context, Result};
use aws_sdk_s3::Client;
use futures::stream::{self, StreamExt};
use std::collections::HashMap;

/// Default max parallel `GetObject` keys per cold-query round (sub-batches share one roundtrip).
pub const DEFAULT_COLD_MAX_KEYS_PER_ROUND: usize = 128;

/// Default in-flight `GetObject` calls per sub-batch (within one [`fetch_round`] chunk).
pub const DEFAULT_COLD_S3_CONCURRENCY: usize = 32;

/// In-flight parallel S3 GETs per sub-batch; override with `OPENPUFFER_COLD_S3_CONCURRENCY` (≥ 1).
pub fn cold_s3_concurrency() -> usize {
    std::env::var("OPENPUFFER_COLD_S3_CONCURRENCY")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n >= 1)
        .unwrap_or(DEFAULT_COLD_S3_CONCURRENCY)
}

/// Max parallel keys per cold round; override with `OPENPUFFER_COLD_MAX_KEYS_PER_ROUND` (≥ 1).
pub fn cold_max_keys_per_round() -> usize {
    std::env::var("OPENPUFFER_COLD_MAX_KEYS_PER_ROUND")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n >= 1)
        .unwrap_or(DEFAULT_COLD_MAX_KEYS_PER_ROUND)
}

/// How many parallel sub-batches [`fetch_round`] uses for `key_count` keys at `max_per_batch`.
pub fn cold_fetch_sub_batch_count(key_count: usize, max_per_batch: usize) -> usize {
    if key_count == 0 {
        0
    } else {
        key_count.div_ceil(max_per_batch.max(1))
    }
}

/// Index artifacts loaded via the cold batch plan (full index — warm prefetch / export).
#[derive(Debug, Default)]
pub struct ColdIndexArtifacts {
    pub fts: Option<FtsSegment>,
    pub filter: Option<FilterSegment>,
    pub vectors: HashMap<String, VectorIndex>,
    pub storage_roundtrips: u32,
}

/// Partial cold index: L0 + FTS + filter at query bootstrap; L1/clusters loaded per probe plan.
#[derive(Debug, Default)]
pub struct ColdBootstrapArtifacts {
    pub fts: Option<FtsSegment>,
    pub filter: Option<FilterSegment>,
    pub l0_by_field: HashMap<String, CentroidIndexL0>,
    pub storage_roundtrips: u32,
    /// Keys fetched in the bootstrap round (for metrics / `performance` JSON).
    pub s3_keys_fetched: u32,
}

/// Cold query planner: logical S3 rounds and key lists (for tests and accounting).
#[derive(Debug, Default, Clone)]
pub struct ColdQueryPlan {
    /// Round 1: `meta.json` + WAL snapshot/tail (namespace open).
    pub round1_keys: Vec<String>,
    /// Round 2: L0 centroids + FTS + filter.
    pub round2_keys: Vec<String>,
    /// Round 3: probed L1 + probed clusters for vector `rank_by`.
    pub round3_keys: Vec<String>,
    /// Round 4: unindexed WAL segments after index cursor.
    pub round4_keys: Vec<String>,
}

/// Options for [`plan_cold_query`].
#[derive(Debug, Default, Clone)]
pub struct ColdPlanOpts {
    /// Include round-1 WAL keys (strong namespace open / first cold load).
    pub include_wal_round: bool,
    /// Include round-4 WAL tail keys when index lags commit seq.
    pub include_wal_tail: bool,
}

fn validate_cold_namespace(namespace: &str) -> Result<()> {
    validate_namespace_name(namespace).map_err(|e| anyhow::anyhow!("{e}"))
}

fn validate_cold_vector_field(field: &str) -> Result<()> {
    validate_s3_path_segment(field, "vector field name").map_err(|e| anyhow::anyhow!("{e}"))
}

fn validate_cold_s3_keys(keys: &[String]) -> Result<()> {
    for key in keys {
        validate_s3_object_key(key).map_err(|e| anyhow::anyhow!("{e}: {key}"))?;
    }
    Ok(())
}

/// Plan cold-query S3 rounds. Pass `l0_by_field` and optional `l1_by_field` (synthetic or
/// decoded) so round-3 cluster keys reflect the probe plan, not `num_fine_total`.
pub fn plan_cold_query(
    namespace: &str,
    meta: &NamespaceMeta,
    vector_probes: &[(String, Vec<f64>)],
    l0_by_field: &HashMap<String, CentroidIndexL0>,
    l1_by_field: Option<&HashMap<String, HashMap<u32, CentroidIndexL1>>>,
    opts: ColdPlanOpts,
) -> ColdQueryPlan {
    if validate_cold_namespace(namespace).is_err() {
        return ColdQueryPlan::default();
    }
    let mut plan = ColdQueryPlan::default();

    if opts.include_wal_round {
        plan.round1_keys = wal_round_keys(namespace, meta);
    }

    plan.round2_keys = round2_bootstrap_keys(namespace, meta);

    for (field, query) in vector_probes {
        if validate_cold_vector_field(field).is_err() {
            continue;
        }
        let Some(l0) = l0_by_field.get(field) else {
            continue;
        };
        let empty_l1 = HashMap::new();
        let l1_loaded = l1_by_field
            .and_then(|m| m.get(field))
            .unwrap_or(&empty_l1);
        if let Ok(r3) = round3_keys_for_query(
            namespace,
            meta,
            field,
            l0,
            l1_loaded,
            query,
            None,
            &HashMap::new(),
        ) {
            plan.round3_keys.extend(r3);
        }
    }
    plan.round3_keys.sort();
    plan.round3_keys.dedup();

    if opts.include_wal_tail {
        plan.round4_keys = unindexed_wal_tail_keys(namespace, meta);
    }

    plan
}

/// Count logical `storage_roundtrips` for a plan (non-empty key list = one roundtrip).
pub fn cold_plan_storage_roundtrips(plan: &ColdQueryPlan) -> u32 {
    let mut n = 0u32;
    if !plan.round1_keys.is_empty() {
        n += 1;
    }
    if !plan.round2_keys.is_empty() {
        n += 1;
    }
    if !plan.round3_keys.is_empty() {
        n += 1;
    }
    if !plan.round4_keys.is_empty() {
        n += 1;
    }
    n
}

fn cold_plan_total_keys(plan: &ColdQueryPlan) -> u32 {
    (plan.round1_keys.len()
        + plan.round2_keys.len()
        + plan.round3_keys.len()
        + plan.round4_keys.len()) as u32
}

/// Per-vector probe summary for debug / ops (uses [`plan_cold_query`] round-3 key lists).
pub fn cold_plan_vector_probes(
    namespace: &str,
    meta: &NamespaceMeta,
    vector_probes: &[(String, Vec<f64>)],
    l0_by_field: &HashMap<String, CentroidIndexL0>,
) -> Vec<ColdPlanVectorProbe> {
    let empty_l1 = HashMap::new();
    let empty_l2 = HashMap::new();
    let mut out = Vec::new();
    for (field, query) in vector_probes {
        let Some(l0) = l0_by_field.get(field) else {
            continue;
        };
        let l0 = l0.clone().clamp_probe_plan_for_query();
        let round3_key_count = round3_keys_for_query(
            namespace,
            meta,
            field,
            &l0,
            &empty_l1,
            query,
            None,
            &empty_l2,
        )
        .map(|k| k.len())
        .unwrap_or(0);
        out.push(ColdPlanVectorProbe {
            vector_field: field.clone(),
            probe_coarse: l0.probe_coarse,
            probe_fine: l0.probe_fine,
            cluster_get_upper_bound: cluster_get_upper_bound(&l0),
            round3_key_count,
        });
    }
    out
}

/// Build cold-plan debug JSON from meta + optional decoded L0 (no query execution).
pub fn build_cold_plan_debug(
    namespace: &str,
    meta: &NamespaceMeta,
    vector_probes: &[(String, Vec<f64>)],
    l0_by_field: &HashMap<String, CentroidIndexL0>,
    opts: ColdPlanOpts,
    consistency: &str,
    view_pinned: bool,
) -> ColdPlanDebugResponse {
    let plan = plan_cold_query(namespace, meta, vector_probes, l0_by_field, None, opts.clone());
    ColdPlanDebugResponse {
        consistency: consistency.to_string(),
        plan_opts: ColdPlanDebugOpts {
            include_wal_round: opts.include_wal_round,
            include_wal_tail: opts.include_wal_tail,
            view_pinned,
        },
        round_key_counts: ColdPlanRoundKeyCounts {
            round1: plan.round1_keys.len(),
            round2: plan.round2_keys.len(),
            round3: plan.round3_keys.len(),
            round4: plan.round4_keys.len(),
        },
        storage_roundtrips: cold_plan_storage_roundtrips(&plan),
        total_keys: cold_plan_total_keys(&plan),
        probe_plan: cold_plan_vector_probes(namespace, meta, vector_probes, l0_by_field),
    }
}

/// L0 centroid keys only (round 2 extension for vector / hybrid cold queries).
pub fn l0_keys_for_meta(namespace: &str, meta: &NamespaceMeta) -> Vec<String> {
    let mut keys = Vec::new();
    for cfg in effective_vector_fields(meta) {
        if cfg.segment_id > 0 && meta.index_cursor > 0 && cfg.dimensions > 0 {
            if vector_index_uses_legacy_paths(meta, &cfg.name) {
                keys.push(CentroidIndexL0::legacy_key(namespace));
            } else {
                keys.push(CentroidIndexL0::key(namespace, &cfg.name));
            }
        }
    }
    keys.sort();
    keys.dedup();
    keys
}

/// FTS + filter keys for cold query bootstrap (round 2). Omits L0 so BM25-only / filter-only
/// queries do not fetch vector index metadata.
///
/// FTS must be loaded here (not in the probed L1/cluster round) so hybrid
/// `Sum`/`Product` queries have a BM25 index on the cold vector path.
pub fn round2_bootstrap_keys(namespace: &str, meta: &NamespaceMeta) -> Vec<String> {
    let mut keys = Vec::new();
    if meta.fts_segment_id > 0 && meta.index_cursor > 0 {
        keys.push(FtsSegment::key(namespace, meta.fts_segment_id));
    }
    if meta.filter_segment_id > 0 && meta.index_cursor > 0 {
        keys.push(FilterSegment::key(namespace, meta.filter_segment_id));
    }
    keys.sort();
    keys.dedup();
    keys
}

/// Probed L1 + optional v3 routing/L2 + probed cluster keys for one vector query (round 3).
pub fn round3_keys_for_query(
    namespace: &str,
    meta: &NamespaceMeta,
    field: &str,
    l0: &CentroidIndexL0,
    l1_loaded: &HashMap<u32, CentroidIndexL1>,
    query: &[f64],
    routing: Option<&CentroidRouting>,
    l2_loaded: &HashMap<(u32, u32), CentroidIndexL2>,
) -> Result<Vec<String>> {
    validate_cold_namespace(namespace)?;
    validate_cold_vector_field(field)?;
    let mut keys = l1_keys_for_query_probe(namespace, meta, field, l0, query)?;
    if l0.has_routing {
        keys.push(CentroidRouting::key(namespace, field));
        if let Some(routing) = routing {
            keys.extend(l2_keys_for_query_probe(namespace, field, l0, routing, query));
        }
    }
    keys.extend(cluster_keys_for_query(
        namespace, meta, field, l0, l1_loaded, query, routing, l2_loaded,
    )?);
    keys.sort();
    keys.dedup();
    validate_cold_s3_keys(&keys)?;
    Ok(keys)
}

/// WAL keys for round 1 (meta is fetched separately in [`cold_load_meta_and_wal`]).
pub fn wal_round_keys(namespace: &str, meta: &NamespaceMeta) -> Vec<String> {
    let mut keys = Vec::new();
    if meta.wal_snapshot_seq > 0 {
        keys.push(crate::wal::WalSnapshot::key(namespace));
    }
    if let Some(from) = wal_commit_replay_from(meta) {
        for seq in from..=meta.wal_commit_seq {
            keys.push(crate::wal::wal_key(namespace, seq));
        }
    }
    keys
}

/// WAL segment replay start for namespace open (snapshot-aware; no fallback to seq 1 after compaction).
pub fn wal_commit_replay_from(meta: &NamespaceMeta) -> Option<u64> {
    if meta.wal_snapshot_seq > 0 {
        crate::wal_compaction::wal_replay_from(meta.wal_snapshot_seq, meta.wal_commit_seq)
    } else if meta.wal_commit_seq > 0 {
        Some(1)
    } else {
        None
    }
}

/// WAL segment keys for round 4 (unindexed tail), if any.
pub fn unindexed_wal_tail_keys(namespace: &str, meta: &NamespaceMeta) -> Vec<String> {
    unindexed_wal_tail_range(meta)
        .map(|(from, to)| {
            (from..=to)
                .map(|seq| crate::wal::wal_key(namespace, seq))
                .collect()
        })
        .unwrap_or_default()
}

/// WAL segment range for unindexed tail (round 4), if any.
pub fn unindexed_wal_tail_range(meta: &NamespaceMeta) -> Option<(u64, u64)> {
    if meta.index_cursor >= meta.wal_commit_seq {
        return None;
    }
    let to = meta.wal_commit_seq;
    let from = if meta.wal_snapshot_seq > 0 {
        crate::wal_compaction::wal_replay_from(meta.wal_snapshot_seq, to)
            .unwrap_or(meta.index_cursor.saturating_add(1))
    } else {
        meta.index_cursor.saturating_add(1)
    };
    if from > to {
        None
    } else {
        Some((from, to))
    }
}

/// Keys for turbopuffer-style round 1 (meta + L0 centroids + latest FTS).
pub fn round1_keys(namespace: &str, meta: &NamespaceMeta) -> Vec<String> {
    let mut keys = vec![meta_key(namespace)];
    keys.extend(round1_index_keys(namespace, meta));
    keys
}

/// Round 1 index objects when `meta.json` is already in memory.
pub fn round1_index_keys(namespace: &str, meta: &NamespaceMeta) -> Vec<String> {
    let mut keys = l0_keys_for_meta(namespace, meta);
    if meta.fts_segment_id > 0 && meta.index_cursor > 0 {
        keys.push(FtsSegment::key(namespace, meta.fts_segment_id));
    }
    keys
}

/// Keys for round 2 cold load: filter + all L1 + all cluster files for one vector field.
pub fn round2_keys_for_field(
    namespace: &str,
    meta: &NamespaceMeta,
    field: &str,
    l0: &CentroidIndexL0,
) -> Result<Vec<String>> {
    validate_cold_namespace(namespace)?;
    validate_cold_vector_field(field)?;
    let use_legacy = vector_index_uses_legacy_paths(meta, field);
    let mut keys = Vec::new();
    for coarse_id in 0..l0.num_coarse {
        if use_legacy {
            keys.push(CentroidIndexL1::legacy_key(namespace, coarse_id));
        } else {
            keys.push(CentroidIndexL1::key(namespace, field, coarse_id));
        }
    }
    for fine_id in 0..l0.num_fine_total {
        if use_legacy {
            keys.push(ClusterSegment::legacy_key(namespace, fine_id));
        } else {
            keys.push(ClusterSegment::key(namespace, field, fine_id));
        }
    }
    validate_cold_s3_keys(&keys)?;
    Ok(keys)
}

/// Keys for round 2 cold load: filter + all L1 + all cluster files (all vector columns).
pub fn round2_keys(
    namespace: &str,
    meta: &NamespaceMeta,
    l0_by_field: &[(String, CentroidIndexL0)],
) -> Vec<String> {
    let mut keys = Vec::new();
    if meta.filter_segment_id > 0 && meta.index_cursor > 0 {
        keys.push(FilterSegment::key(namespace, meta.filter_segment_id));
    }
    for (field, l0) in l0_by_field {
        if let Ok(field_keys) = round2_keys_for_field(namespace, meta, field, l0) {
            keys.extend(field_keys);
        }
    }
    keys.sort();
    keys.dedup();
    keys
}

/// Probed L1 object keys only (no filter, no clusters).
pub fn l1_keys_for_query_probe(
    namespace: &str,
    meta: &NamespaceMeta,
    field: &str,
    l0: &CentroidIndexL0,
    query: &[f64],
) -> Result<Vec<String>> {
    validate_cold_namespace(namespace)?;
    validate_cold_vector_field(field)?;
    let use_legacy = vector_index_uses_legacy_paths(meta, field);
    let mut keys = Vec::new();
    for coarse_id in l0.nearest_coarse(query, l0.probe_coarse_count()) {
        if use_legacy {
            keys.push(CentroidIndexL1::legacy_key(namespace, coarse_id));
        } else {
            keys.push(CentroidIndexL1::key(namespace, field, coarse_id));
        }
    }
    validate_cold_s3_keys(&keys)?;
    Ok(keys)
}

/// Upper bound on cluster `GetObject` calls for one probed vector query (spec slack +4).
pub fn cluster_get_upper_bound(l0: &CentroidIndexL0) -> usize {
    crate::index::vector::cluster_get_upper_bound(l0)
}

/// Probed cluster object keys given L1 (+ optional v3 routing/L2) already in memory.
pub fn cluster_keys_for_query(
    namespace: &str,
    meta: &NamespaceMeta,
    field: &str,
    l0: &CentroidIndexL0,
    l1_loaded: &HashMap<u32, CentroidIndexL1>,
    query: &[f64],
    routing: Option<&CentroidRouting>,
    l2_loaded: &HashMap<(u32, u32), CentroidIndexL2>,
) -> Result<Vec<String>> {
    validate_cold_namespace(namespace)?;
    validate_cold_vector_field(field)?;
    let use_legacy = vector_index_uses_legacy_paths(meta, field);
    let mut keys = Vec::new();
    for fine_id in probe_fine_centroids_parts(l0, l1_loaded, routing, l2_loaded, query) {
        if use_legacy {
            keys.push(ClusterSegment::legacy_key(namespace, fine_id));
        } else {
            keys.push(ClusterSegment::key(namespace, field, fine_id));
        }
    }
    validate_cold_s3_keys(&keys)?;
    Ok(keys)
}

/// `centroids-routing.bin` key when L0 marks v3 routing.
pub fn routing_key_for_field(namespace: &str, field: &str, l0: &CentroidIndexL0) -> Option<String> {
    if l0.has_routing {
        Some(CentroidRouting::key(namespace, field))
    } else {
        None
    }
}

/// All L2 segment keys for probed coarse cells that have routing splits.
pub fn l2_keys_for_query_probe(
    namespace: &str,
    field: &str,
    l0: &CentroidIndexL0,
    routing: &CentroidRouting,
    query: &[f64],
) -> Vec<String> {
    let mut keys = Vec::new();
    for coarse_id in l0.nearest_coarse(query, l0.probe_coarse_count()) {
        let l2_count = routing.l2_count_for_coarse(coarse_id);
        if l2_count <= 1 {
            continue;
        }
        for l2_id in 0..l2_count {
            keys.push(CentroidIndexL2::key(namespace, field, coarse_id, l2_id));
        }
    }
    keys
}

/// Probe plan without requiring L1 in memory: filter + L1 for top coarse (clusters after decode).
pub fn round2_keys_for_query_probe(
    namespace: &str,
    meta: &NamespaceMeta,
    field: &str,
    l0: &CentroidIndexL0,
    query: &[f64],
) -> Result<Vec<String>> {
    let mut keys = l1_keys_for_query_probe(namespace, meta, field, l0, query)?;
    if meta.filter_segment_id > 0 && meta.index_cursor > 0 {
        keys.push(FilterSegment::key(namespace, meta.filter_segment_id));
    }
    validate_cold_s3_keys(&keys)?;
    Ok(keys)
}

/// Record cold-query S3 key volume (Prometheus + per-query accounting).
pub fn record_cold_s3_keys_fetched(keys: usize) -> u32 {
    let n = keys.min(u32::MAX as usize) as u32;
    if n > 0 {
        crate::metrics::add_cold_s3_keys_fetched(n as u64);
    }
    n
}

/// One parallel batch of S3 `GetObject` calls (counts as one storage roundtrip).
///
/// Keys beyond [`cold_max_keys_per_round`] are fetched in sequential sub-batches; callers
/// still increment `storage_roundtrips` once per [`fetch_round`] invocation.
pub async fn fetch_round(
    client: &Client,
    bucket: &str,
    keys: &[String],
) -> Result<HashMap<String, Vec<u8>>> {
    fetch_round_inner(client, bucket, keys, false).await
}

/// Like [`fetch_round`], but omits keys that are not present (probed cluster segments may be absent).
pub async fn fetch_round_optional(
    client: &Client,
    bucket: &str,
    keys: &[String],
) -> Result<HashMap<String, Vec<u8>>> {
    fetch_round_inner(client, bucket, keys, true).await
}

async fn fetch_round_inner(
    client: &Client,
    bucket: &str,
    keys: &[String],
    allow_missing: bool,
) -> Result<HashMap<String, Vec<u8>>> {
    if keys.is_empty() {
        return Ok(HashMap::new());
    }
    validate_cold_s3_keys(keys)?;
    let max = cold_max_keys_per_round();
    let mut out = HashMap::with_capacity(keys.len());
    for chunk in keys.chunks(max) {
        let batch = fetch_round_batch(client, bucket, chunk, allow_missing).await?;
        out.extend(batch);
    }
    Ok(out)
}

async fn fetch_round_batch(
    client: &Client,
    bucket: &str,
    keys: &[String],
    allow_missing: bool,
) -> Result<HashMap<String, Vec<u8>>> {
    let concurrency = cold_s3_concurrency();
    let mut out = HashMap::with_capacity(keys.len());
    let mut stream = stream::iter(keys.iter().cloned())
        .map(|key| {
            let client = client.clone();
            let bucket = bucket.to_string();
            async move {
                let result = get_object_bytes_optional(&client, &bucket, &key).await?;
                Ok::<_, anyhow::Error>((key, result))
            }
        })
        .buffer_unordered(concurrency);
    while let Some(result) = stream.next().await {
        let (key, maybe_bytes) = result?;
        match maybe_bytes {
            Some((k, bytes)) => {
                out.insert(k, bytes);
            }
            None if allow_missing => {}
            None => anyhow::bail!("object not found: {key}"),
        }
    }
    Ok(out)
}

async fn get_object_bytes_optional(
    client: &Client,
    bucket: &str,
    key: &str,
) -> Result<Option<(String, Vec<u8>)>> {
    let result = crate::namespace::get_object_bytes_optional(client, bucket, key).await?;
    Ok(result.map(|(bytes, _)| {
        crate::metrics::inc_s3_get();
        (key.to_string(), bytes)
    }))
}

/// Cold query bootstrap: L0 + FTS + filter in one logical roundtrip. L1/clusters are per-query.
pub async fn fetch_cold_index_bootstrap(
    client: &Client,
    bucket: &str,
    namespace: &str,
    meta: &NamespaceMeta,
) -> Result<ColdBootstrapArtifacts> {
    let mut storage_roundtrips = 0u32;

    let r2_keys = round2_bootstrap_keys(namespace, meta);
    let mut s3_keys_fetched = record_cold_s3_keys_fetched(r2_keys.len());
    let fetched = if r2_keys.is_empty() {
        HashMap::new()
    } else {
        storage_roundtrips += 1;
        fetch_round(client, bucket, &r2_keys).await?
    };

    let mut fts = decode_fts_from_round(namespace, meta, &fetched)?;
    if fts.is_none() && meta.fts_segment_id > 0 && meta.index_cursor > 0 {
        let fts_key = FtsSegment::key(namespace, meta.fts_segment_id);
        if !fetched.contains_key(&fts_key) {
            s3_keys_fetched = s3_keys_fetched.saturating_add(record_cold_s3_keys_fetched(1));
            let extra = fetch_round(client, bucket, std::slice::from_ref(&fts_key)).await?;
            fts = decode_fts_from_round(namespace, meta, &extra)?;
        }
    }
    let filter = decode_filter_from_round(namespace, meta, &fetched)?;

    Ok(ColdBootstrapArtifacts {
        fts,
        filter,
        l0_by_field: HashMap::new(),
        storage_roundtrips,
        s3_keys_fetched,
    })
}

/// Fetch L0 centroids for vector / hybrid cold queries (one logical roundtrip; not used for BM25-only).
pub async fn fetch_cold_vector_l0(
    client: &Client,
    bucket: &str,
    namespace: &str,
    meta: &NamespaceMeta,
) -> Result<(HashMap<String, CentroidIndexL0>, u32, u32)> {
    let keys = l0_keys_for_meta(namespace, meta);
    if keys.is_empty() {
        return Ok((HashMap::new(), 0, 0));
    }
    let fetched = fetch_round(client, bucket, &keys).await?;
    let l0_by_field = decode_l0_by_field_from_fetched(namespace, meta, &fetched);
    Ok((
        l0_by_field,
        1,
        record_cold_s3_keys_fetched(keys.len()),
    ))
}

/// After probed vector fetch, ensure FTS is present for hybrid BM25 (bootstrap should have loaded it).
pub async fn ensure_cold_fts_for_hybrid(
    client: &Client,
    bucket: &str,
    namespace: &str,
    meta: &NamespaceMeta,
    fts: &mut Option<FtsSegment>,
    storage_roundtrips: &mut Option<u32>,
    cold_s3_keys_fetched: &mut u32,
) -> Result<()> {
    if fts.is_some() || meta.fts_segment_id == 0 || meta.index_cursor == 0 {
        return Ok(());
    }
    let fts_key = FtsSegment::key(namespace, meta.fts_segment_id);
    let fetched = fetch_round(client, bucket, std::slice::from_ref(&fts_key)).await?;
    *fts = decode_fts_from_round(namespace, meta, &fetched)?;
    if fts.is_some() {
        if let Some(rt) = storage_roundtrips {
            *rt = rt.saturating_add(1);
        }
        *cold_s3_keys_fetched =
            cold_s3_keys_fetched.saturating_add(record_cold_s3_keys_fetched(1));
    }
    Ok(())
}

/// Probed vector index for one query: L1 then clusters (one logical roundtrip).
pub async fn fetch_cold_vector_probed(
    client: &Client,
    bucket: &str,
    namespace: &str,
    meta: &NamespaceMeta,
    field: &str,
    l0: CentroidIndexL0,
    query: &[f64],
) -> Result<(VectorIndex, u32, u32, u32)> {
    let mut storage_roundtrips = 0u32;
    let mut fetched = HashMap::new();
    let mut s3_keys_fetched = 0u32;

    validate_cold_namespace(namespace)?;
    validate_cold_vector_field(field)?;
    let l0 = l0.clamp_probe_plan_for_query();
    let l1_keys = l1_keys_for_query_probe(namespace, meta, field, &l0, query)?;
    if !l1_keys.is_empty() {
        storage_roundtrips = 1;
        s3_keys_fetched = s3_keys_fetched.saturating_add(record_cold_s3_keys_fetched(l1_keys.len()));
        let l1_map = fetch_round(client, bucket, &l1_keys).await?;
        fetched.extend(l1_map);
    }

    let routing_key = routing_key_for_field(namespace, field, &l0);
    if let Some(ref key) = routing_key {
        if storage_roundtrips == 0 {
            storage_roundtrips = 1;
        }
        s3_keys_fetched = s3_keys_fetched.saturating_add(record_cold_s3_keys_fetched(1));
        let routing_map = fetch_round_optional(client, bucket, std::slice::from_ref(key)).await?;
        fetched.extend(routing_map);
    }

    let routing = decode_routing_probed(namespace, field, &l0, &fetched)?;
    let l2_keys = routing
        .as_ref()
        .map(|r| l2_keys_for_query_probe(namespace, field, &l0, r, query))
        .unwrap_or_default();
    if !l2_keys.is_empty() {
        if storage_roundtrips == 0 {
            storage_roundtrips = 1;
        }
        s3_keys_fetched =
            s3_keys_fetched.saturating_add(record_cold_s3_keys_fetched(l2_keys.len()));
        let l2_map = fetch_round(client, bucket, &l2_keys).await?;
        fetched.extend(l2_map);
    }

    let cluster_keys =
        cluster_keys_for_query_after_l1(namespace, meta, field, &l0, &fetched, query)?;
    if !cluster_keys.is_empty() {
        if storage_roundtrips == 0 {
            storage_roundtrips = 1;
        }
        s3_keys_fetched =
            s3_keys_fetched.saturating_add(record_cold_s3_keys_fetched(cluster_keys.len()));
        // Empty fine centroids have no cluster object on S3 (warm cache skips misses too).
        let cluster_map = fetch_round_optional(client, bucket, &cluster_keys).await?;
        fetched.extend(cluster_map);
    }

    let (vindex, probed_clusters) =
        assemble_vector_index_probed(namespace, meta, field, l0, &fetched, query)?;
    Ok((vindex, storage_roundtrips, s3_keys_fetched, probed_clusters))
}

/// Build a probed [`VectorIndex`] from bytes already fetched (L1 keys) plus optional cluster keys.
pub fn assemble_vector_index_probed(
    namespace: &str,
    meta: &NamespaceMeta,
    field: &str,
    l0: CentroidIndexL0,
    fetched: &HashMap<String, Vec<u8>>,
    query: &[f64],
) -> Result<(VectorIndex, u32)> {
    let l1 = decode_l1_probed(namespace, meta, field, &l0, fetched)?;
    let routing = decode_routing_probed(namespace, field, &l0, fetched)?;
    let l2_key_list = routing
        .as_ref()
        .map(|r| {
            l2_keys_for_query_probe(namespace, field, &l0, r, query)
        })
        .unwrap_or_default();
    let l2 = decode_l2_probed(namespace, field, fetched, &l2_key_list)?;
    let fine_ids = probe_fine_centroids_parts(&l0, &l1, routing.as_ref(), &l2, query);
    let probed_clusters = fine_ids.len().min(u32::MAX as usize) as u32;
    if probed_clusters > 0 {
        crate::metrics::add_ann_probed_clusters(probed_clusters as u64);
    }
    let clusters = decode_clusters_probed(namespace, meta, field, fetched, &fine_ids)?;
    Ok((
        VectorIndex {
            l0,
            l1,
            clusters,
            routing,
            l2,
        },
        probed_clusters,
    ))
}

pub(crate) fn decode_routing_probed(
    namespace: &str,
    field: &str,
    l0: &CentroidIndexL0,
    fetched: &HashMap<String, Vec<u8>>,
) -> Result<Option<CentroidRouting>> {
    if !l0.has_routing {
        return Ok(None);
    }
    let key = CentroidRouting::key(namespace, field);
    let Some(bytes) = fetched.get(&key) else {
        return Ok(None);
    };
    Ok(Some(CentroidRouting::decode(bytes)?))
}

pub(crate) fn decode_l2_probed(
    _namespace: &str,
    _field: &str,
    fetched: &HashMap<String, Vec<u8>>,
    keys: &[String],
) -> Result<HashMap<(u32, u32), CentroidIndexL2>> {
    let mut l2 = HashMap::new();
    for key in keys {
        let Some(bytes) = fetched.get(key) else {
            continue;
        };
        let seg = CentroidIndexL2::decode(bytes)?;
        l2.insert((seg.coarse_id, seg.l2_id), seg);
    }
    Ok(l2)
}

/// After L1 objects are in `fetched`, return cluster keys still needed for `query`.
pub fn cluster_keys_for_query_after_l1(
    namespace: &str,
    meta: &NamespaceMeta,
    field: &str,
    l0: &CentroidIndexL0,
    fetched: &HashMap<String, Vec<u8>>,
    query: &[f64],
) -> Result<Vec<String>> {
    let l1 = decode_l1_probed(namespace, meta, field, l0, fetched)?;
    let routing = decode_routing_probed(namespace, field, l0, fetched)?;
    let l2_key_list = routing
        .as_ref()
        .map(|r| l2_keys_for_query_probe(namespace, field, l0, r, query))
        .unwrap_or_default();
    let l2 = decode_l2_probed(namespace, field, fetched, &l2_key_list)?;
    cluster_keys_for_query(
        namespace, meta, field, l0, &l1, query, routing.as_ref(), &l2,
    )
}

pub(crate) fn decode_l1_probed(
    namespace: &str,
    meta: &NamespaceMeta,
    field: &str,
    l0: &CentroidIndexL0,
    fetched: &HashMap<String, Vec<u8>>,
) -> Result<HashMap<u32, CentroidIndexL1>> {
    let use_legacy = vector_index_uses_legacy_paths(meta, field);
    let mut l1 = HashMap::new();
    for coarse_id in 0..l0.num_coarse {
        let key = CentroidIndexL1::key(namespace, field, coarse_id);
        let bytes = fetched_segment(
            fetched,
            &key,
            use_legacy,
            || CentroidIndexL1::legacy_key(namespace, coarse_id),
        );
        let Some(bytes) = bytes else {
            continue;
        };
        l1.insert(coarse_id, CentroidIndexL1::decode(bytes)?);
    }
    Ok(l1)
}

pub(crate) fn decode_clusters_probed(
    namespace: &str,
    meta: &NamespaceMeta,
    field: &str,
    fetched: &HashMap<String, Vec<u8>>,
    fine_ids: &[u32],
) -> Result<HashMap<u32, ClusterSegment>> {
    let use_legacy = vector_index_uses_legacy_paths(meta, field);
    let mut clusters = HashMap::new();
    for &fine_id in fine_ids {
        let key = ClusterSegment::key(namespace, field, fine_id);
        let bytes = fetched_segment(
            fetched,
            &key,
            use_legacy,
            || ClusterSegment::legacy_key(namespace, fine_id),
        );
        let Some(bytes) = bytes else {
            continue;
        };
        if bytes.is_empty() {
            continue;
        }
        clusters.insert(fine_id, ClusterSegment::decode(bytes)?);
    }
    Ok(clusters)
}

/// Full cold index load (all L1 + clusters). Used for warm prefetch, not the query hot path.
pub async fn fetch_cold_index_artifacts(
    client: &Client,
    bucket: &str,
    namespace: &str,
    meta: &NamespaceMeta,
) -> Result<ColdIndexArtifacts> {
    let mut storage_roundtrips = 0u32;

    let r1_keys = round1_index_keys(namespace, meta);
    let r1 = if r1_keys.is_empty() {
        HashMap::new()
    } else {
        storage_roundtrips += 1;
        fetch_round(client, bucket, &r1_keys).await?
    };

    let l0_map = decode_l0_by_field_from_fetched(namespace, meta, &r1);
    let l0_by_field: Vec<_> = l0_map.into_iter().collect();

    let mut r2_keys = round2_keys(namespace, meta, &l0_by_field);
    if r2_keys.is_empty() && meta.filter_segment_id > 0 && meta.index_cursor > 0 {
        r2_keys.push(FilterSegment::key(namespace, meta.filter_segment_id));
    }

    let r2 = if r2_keys.is_empty() {
        HashMap::new()
    } else {
        storage_roundtrips += 1;
        fetch_round(client, bucket, &r2_keys).await?
    };

    let fts = decode_fts_from_round(namespace, meta, &r1)?;
    let filter = decode_filter_from_round(namespace, meta, &r2)?;
    let mut vectors = HashMap::new();
    for (field, l0) in l0_by_field {
        if let Some(v) = decode_vector_from_rounds(namespace, meta, &field, l0, &r2)? {
            vectors.insert(field, v);
        }
    }

    Ok(ColdIndexArtifacts {
        fts,
        filter,
        vectors,
        storage_roundtrips,
    })
}

/// Replay WAL segment range using one parallel fetch round (no metrics; see [`fetch_cold_unindexed_wal_tail`]).
pub async fn replay_wal_entries_batched(
    client: &Client,
    bucket: &str,
    namespace: &str,
    from_seq: u64,
    to_seq: u64,
) -> Result<Vec<crate::wal::WalEntry>> {
    if from_seq == 0 || to_seq == 0 || from_seq > to_seq {
        return Ok(Vec::new());
    }
    let keys: Vec<String> = (from_seq..=to_seq)
        .map(|seq| crate::wal::wal_key(namespace, seq))
        .collect();
    let raw = fetch_round(client, bucket, &keys).await?;
    let mut entries = Vec::with_capacity(keys.len());
    for seq in from_seq..=to_seq {
        let key = crate::wal::wal_key(namespace, seq);
        let bytes = raw
            .get(&key)
            .with_context(|| format!("wal segment {seq:08} missing in batch"))?;
        if let Some(entry) =
            crate::wal::decode_segment_with_policy(bytes, seq, crate::wal::WalCorruptPolicy::current())?
        {
            entries.push(entry);
        }
    }
    Ok(entries)
}

/// Cold round 4: parallel fetch of unindexed WAL tail `(index_cursor, wal_commit_seq]`.
///
/// Returns `(entries, storage_roundtrips, s3_keys_fetched)`; all zero when caught up.
pub async fn fetch_cold_unindexed_wal_tail(
    client: &Client,
    bucket: &str,
    namespace: &str,
    meta: &NamespaceMeta,
) -> Result<(Vec<crate::wal::WalEntry>, u32, u32)> {
    let Some((from, to)) = unindexed_wal_tail_range(meta) else {
        return Ok((Vec::new(), 0, 0));
    };
    let keys_fetched = record_cold_s3_keys_fetched(unindexed_wal_tail_keys(namespace, meta).len());
    let entries = replay_wal_entries_batched(client, bucket, namespace, from, to).await?;
    Ok((entries, 1, keys_fetched))
}

/// Cold namespace open for `consistency: eventual`: `meta.json` only (no WAL round).
pub async fn cold_load_meta_only(
    client: &Client,
    bucket: &str,
    namespace: &str,
) -> Result<(NamespaceMeta, Option<String>, u32, u32)> {
    let Some((meta, etag)) = fetch_meta(client, bucket, namespace).await? else {
        return Ok((NamespaceMeta::default(), None, 0, 0));
    };
    Ok((meta, etag, 1, 0))
}

/// Cold namespace bootstrap: meta fetch + parallel WAL segments (2 roundtrips when WAL present).
pub async fn cold_load_meta_and_wal(
    client: &Client,
    bucket: &str,
    namespace: &str,
) -> Result<(NamespaceMeta, Option<String>, HashMap<u64, Vec<u8>>, u32, u32)> {
    let Some((meta, etag)) = fetch_meta(client, bucket, namespace).await? else {
        return Ok((NamespaceMeta::default(), None, HashMap::new(), 0, 0));
    };
    let mut storage_roundtrips = 1u32;
    let mut s3_keys_fetched = 0u32;

    let mut fetch_keys = Vec::new();
    if meta.wal_snapshot_seq > 0 {
        fetch_keys.push(crate::wal::WalSnapshot::key(namespace));
    }

    let replay_from = wal_commit_replay_from(&meta);
    if let Some(from) = replay_from {
        for seq in from..=meta.wal_commit_seq {
            fetch_keys.push(crate::wal::wal_key(namespace, seq));
        }
    }

    if fetch_keys.is_empty() {
        return Ok((meta, etag, HashMap::new(), storage_roundtrips, s3_keys_fetched));
    }

    s3_keys_fetched = record_cold_s3_keys_fetched(fetch_keys.len());
    let wal_map_raw = fetch_round(client, bucket, &fetch_keys).await?;
    storage_roundtrips += 1;

    let mut wal_by_seq = HashMap::new();
    let snap_key = crate::wal::WalSnapshot::key(namespace);
    if let Some(bytes) = wal_map_raw.get(&snap_key) {
        wal_by_seq.insert(0, bytes.clone());
    }
    if let Some(from) = replay_from {
        for seq in from..=meta.wal_commit_seq {
            let key = crate::wal::wal_key(namespace, seq);
            if let Some(bytes) = wal_map_raw.get(&key) {
                wal_by_seq.insert(seq, bytes.clone());
            }
        }
    }

    Ok((meta, etag, wal_by_seq, storage_roundtrips, s3_keys_fetched))
}

fn fetched_segment<'a>(
    fetched: &'a HashMap<String, Vec<u8>>,
    key: &str,
    use_legacy: bool,
    legacy_key: impl FnOnce() -> String,
) -> Option<&'a Vec<u8>> {
    fetched.get(key).or_else(|| {
        if use_legacy {
            fetched.get(&legacy_key())
        } else {
            None
        }
    })
}

fn decode_l0_by_field_from_fetched(
    namespace: &str,
    meta: &NamespaceMeta,
    fetched: &HashMap<String, Vec<u8>>,
) -> HashMap<String, CentroidIndexL0> {
    let mut l0_by_field = HashMap::new();
    for cfg in effective_vector_fields(meta) {
        if cfg.segment_id == 0 || meta.index_cursor == 0 || cfg.dimensions == 0 {
            continue;
        }
        let key = CentroidIndexL0::key(namespace, &cfg.name);
        let use_legacy = vector_index_uses_legacy_paths(meta, &cfg.name);
        let Some(bytes) = fetched_segment(
            fetched,
            &key,
            use_legacy,
            || CentroidIndexL0::legacy_key(namespace),
        ) else {
            continue;
        };
        if let Ok(l0) = CentroidIndexL0::decode(bytes) {
            if l0.num_fine_total > 0 {
                let l0 = l0
                    .align_with_namespace_meta(meta, None)
                    .clamp_probe_plan_for_query();
                l0_by_field.insert(cfg.name.clone(), l0);
            }
        }
    }
    l0_by_field
}

fn decode_fts_from_round(
    namespace: &str,
    meta: &NamespaceMeta,
    r1: &HashMap<String, Vec<u8>>,
) -> Result<Option<FtsSegment>> {
    if meta.fts_segment_id == 0 || meta.index_cursor == 0 {
        return Ok(None);
    }
    let key = FtsSegment::key(namespace, meta.fts_segment_id);
    let Some(bytes) = r1.get(&key) else {
        return Ok(None);
    };
    Ok(Some(FtsSegment::decode(bytes)?))
}

fn decode_filter_from_round(
    namespace: &str,
    meta: &NamespaceMeta,
    r2: &HashMap<String, Vec<u8>>,
) -> Result<Option<FilterSegment>> {
    if meta.filter_segment_id == 0 || meta.index_cursor == 0 {
        return Ok(None);
    }
    let key = FilterSegment::key(namespace, meta.filter_segment_id);
    let Some(bytes) = r2.get(&key) else {
        return Ok(None);
    };
    Ok(Some(FilterSegment::decode(bytes)?))
}

fn decode_vector_from_rounds(
    namespace: &str,
    meta: &NamespaceMeta,
    field: &str,
    l0: CentroidIndexL0,
    r2: &HashMap<String, Vec<u8>>,
) -> Result<Option<VectorIndex>> {
    if l0.num_fine_total == 0 {
        return Ok(None);
    }
    let l1 = decode_l1_probed(namespace, meta, field, &l0, r2)?;
    let fine_ids: Vec<u32> = (0..l0.num_fine_total).collect();
    let clusters = decode_clusters_probed(namespace, meta, field, r2, &fine_ids)?;
    Ok(Some(VectorIndex {
        l0,
        l1,
        clusters,
        routing: None,
        l2: HashMap::new(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::vector::{DEFAULT_PROBE_COARSE, DEFAULT_PROBE_FINE};
    use crate::meta::{NamespaceMeta, VectorFieldConfig};

    #[test]
    fn round1_keys_include_meta_centroids_fts() {
        let meta = NamespaceMeta {
            index_cursor: 5,
            fts_segment_id: 5,
            vector_segment_id: 5,
            vector_field: "embedding".into(),
            dimensions: 4,
            vector_fields: vec![VectorFieldConfig {
                name: "embedding".into(),
                dimensions: 4,
                segment_id: 5,
                segment_ids: vec![5],
                ..Default::default()
            }],
            wal_commit_seq: 5,
            ..Default::default()
        };
        let keys = round1_keys("ns", &meta);
        assert_eq!(keys.len(), 3);
        assert!(keys.iter().any(|k| k.ends_with("meta.json")));
        assert!(keys.iter().any(|k| k.contains("centroids-l0.bin")));
        assert!(keys.iter().any(|k| k.contains("fts-00000005")));
    }

    #[test]
    fn round2_keys_filter_l1_and_clusters() {
        let meta = NamespaceMeta {
            index_cursor: 3,
            filter_segment_id: 3,
            vector_segment_id: 3,
            vector_field: "emb".into(),
            dimensions: 2,
            vector_fields: vec![VectorFieldConfig {
                name: "emb".into(),
                dimensions: 2,
                segment_id: 3,
                segment_ids: vec![3],
                ..Default::default()
            }],
            ..Default::default()
        };
        let l0 = CentroidIndexL0 {
            vector_field: "emb".into(),
            num_coarse: 4,
            num_fine_total: 16,
            fine_counts: vec![4, 4, 4, 4],
            centroids: vec![vec![0.0, 0.0]; 4],
            dimensions: 2,
            ..Default::default()
        };
        let keys = round2_keys("ns", &meta, &[("emb".into(), l0)]);
        assert!(keys.iter().any(|k| k.contains("filter-00000003")));
        assert_eq!(
            keys.iter().filter(|k| k.contains("centroids-l1-")).count(),
            4
        );
        assert_eq!(
            keys.iter().filter(|k| k.contains("clusters-")).count(),
            16
        );
    }

    #[test]
    fn round2_query_probe_fetches_subset() {
        let meta = NamespaceMeta {
            index_cursor: 1,
            vector_segment_id: 1,
            vector_field: "emb".into(),
            dimensions: 2,
            vector_fields: vec![VectorFieldConfig {
                name: "emb".into(),
                dimensions: 2,
                segment_id: 1,
                segment_ids: vec![1],
                ..Default::default()
            }],
            ..Default::default()
        };
        let l0 = CentroidIndexL0 {
            vector_field: "emb".into(),
            num_coarse: 8,
            num_fine_total: 64,
            probe_coarse: DEFAULT_PROBE_COARSE,
            probe_fine: DEFAULT_PROBE_FINE,
            fine_counts: vec![4; 8],
            centroids: (0..8)
                .map(|i| vec![if i == 0 { 1.0 } else { 0.0 }, 0.0])
                .collect(),
            dimensions: 2,
            distance_metric: crate::meta::DistanceMetric::CosineDistance,
            ..Default::default()
        };
        let query = vec![1.0, 0.0];
        let keys = round2_keys_for_query_probe("ns", &meta, "emb", &l0, &query).unwrap();
        assert!(
            keys.iter().filter(|k| k.contains("centroids-l1-")).count()
                <= DEFAULT_PROBE_COARSE as usize
        );
        assert!(!keys.iter().any(|k| k.contains("clusters-")));
    }

    #[test]
    fn round1_index_keys_omit_meta() {
        let meta = NamespaceMeta {
            index_cursor: 2,
            fts_segment_id: 2,
            vector_segment_id: 2,
            vector_field: "emb".into(),
            dimensions: 2,
            vector_fields: vec![VectorFieldConfig {
                name: "emb".into(),
                dimensions: 2,
                segment_id: 2,
                segment_ids: vec![2],
                ..Default::default()
            }],
            ..Default::default()
        };
        let keys = round1_index_keys("ns", &meta);
        assert_eq!(keys.len(), 2);
        assert!(!keys.iter().any(|k| k.ends_with("meta.json")));
    }

    #[test]
    fn cold_wal_fetch_after_compaction_needs_no_deleted_segments() {
        let meta = NamespaceMeta {
            wal_commit_seq: 15,
            wal_snapshot_seq: 15,
            index_cursor: 15,
            ..Default::default()
        };
        assert!(wal_commit_replay_from(&meta).is_none());
    }

    #[test]
    fn bootstrap_round2_fetches_filter_not_clusters() {
        let meta = NamespaceMeta {
            index_cursor: 3,
            filter_segment_id: 3,
            vector_segment_id: 3,
            vector_field: "emb".into(),
            dimensions: 2,
            vector_fields: vec![VectorFieldConfig {
                name: "emb".into(),
                dimensions: 2,
                segment_id: 3,
                segment_ids: vec![3],
                ..Default::default()
            }],
            ..Default::default()
        };
        let l0 = CentroidIndexL0 {
            vector_field: "emb".into(),
            num_coarse: 4,
            num_fine_total: 64,
            fine_counts: vec![4, 4, 4, 4],
            centroids: vec![vec![0.0, 0.0]; 4],
            dimensions: 2,
            ..Default::default()
        };
        let r2_keys = round2_bootstrap_keys("ns", &meta);
        assert!(r2_keys.iter().any(|k| k.contains("filter-")));
        assert!(!r2_keys.iter().any(|k| k.contains("clusters-")));
        let full_r2 = round2_keys("ns", &meta, &[("emb".into(), l0)]);
        assert!(full_r2.iter().filter(|k| k.contains("clusters-")).count() == 64);
    }

    #[test]
    fn cluster_get_upper_bound_matches_probe_defaults() {
        let l0 = CentroidIndexL0 {
            probe_coarse: DEFAULT_PROBE_COARSE,
            probe_fine: DEFAULT_PROBE_FINE,
            ..CentroidIndexL0::default()
        };
        assert_eq!(cluster_get_upper_bound(&l0), 16);
    }

    #[test]
    fn probed_cluster_keys_bounded_at_large_num_fine_total() {
        use crate::index::vector::CentroidIndexL1;
        let meta = NamespaceMeta {
            index_cursor: 1,
            vector_segment_id: 1,
            vector_field: "emb".into(),
            dimensions: 2,
            vector_fields: vec![VectorFieldConfig {
                name: "emb".into(),
                dimensions: 2,
                segment_id: 1,
                segment_ids: vec![1],
                ..Default::default()
            }],
            ..Default::default()
        };
        let l0 = CentroidIndexL0 {
            vector_field: "emb".into(),
            num_coarse: 16,
            num_fine_total: 4000,
            probe_coarse: DEFAULT_PROBE_COARSE,
            probe_fine: DEFAULT_PROBE_FINE,
            fine_counts: vec![250; 16],
            centroids: (0..16)
                .map(|i| vec![if i == 0 { 1.0 } else { 0.0 }, 0.0])
                .collect(),
            dimensions: 2,
            distance_metric: crate::meta::DistanceMetric::CosineDistance,
            ..Default::default()
        };
        let query = vec![1.0, 0.0];
        let mut l1_loaded = HashMap::new();
        for coarse_id in l0.nearest_coarse(&query, l0.probe_coarse_count()) {
            let start = l0.global_id_start(coarse_id);
            l1_loaded.insert(
                coarse_id,
                CentroidIndexL1 {
                    segment_id: 1,
                    coarse_id,
                    global_id_start: start,
                    num_fine: 250,
                    centroids: (0..250)
                        .map(|i| vec![if i == 0 { 1.0 } else { 0.0 }, 0.0])
                        .collect(),
                },
            );
        }
        let probed = cluster_keys_for_query(
            "ns", &meta, "emb", &l0, &l1_loaded, &query, None, &HashMap::new(),
        )
        .unwrap();
        let full_clusters = (0..l0.num_fine_total)
            .map(|fid| ClusterSegment::key("ns", "emb", fid))
            .count();
        assert!(probed.len() >= 8 && probed.len() <= 64);
        assert!(probed.len() < full_clusters / 10);
    }

    #[test]
    fn assemble_probed_v3_includes_routing_and_l2() {
        use crate::config::AnnBuildConfig;
        use crate::index::vector::{ANN_VERSION_V3, CentroidIndexL2, CentroidRouting};
        use crate::meta::DistanceMetric;
        use crate::models::Document;
        use serde_json::json;

        let mut docs = Vec::new();
        const N: usize = 8_000;
        for i in 0..N {
            let angle = (i as f64) * 0.02;
            let id = format!("doc-{i}");
            docs.push((
                id.clone(),
                Document {
                    id,
                    attributes: [(
                        "embedding".into(),
                        json!([angle.cos(), angle.sin(), 0.1, 0.2]),
                    )]
                    .into(),
                },
            ));
        }
        let index = VectorIndex::build(
            1,
            "embedding",
            DistanceMetric::CosineDistance,
            &docs,
            &json!({ "embedding": "[4]f32" }),
            AnnBuildConfig::default().with_ann_version(ANN_VERSION_V3),
        )
        .unwrap()
        .expect("v3 index");
        if !index.l0.has_routing {
            return;
        }
        let query = vec![1.0, 0.0, 0.1, 0.2];
        let namespace = "ns";
        let field = "embedding";
        let mut fetched = HashMap::new();
        for (coarse_id, l1) in &index.l1 {
            if l0_nearest_coarse_includes(*coarse_id, &index.l0, &query) {
                fetched.insert(
                    CentroidIndexL1::key(namespace, field, *coarse_id),
                    l1.encode().unwrap(),
                );
            }
        }
        let routing = index.routing.as_ref().expect("routing table");
        fetched.insert(
            CentroidRouting::key(namespace, field),
            routing.encode().unwrap(),
        );
        for coarse_id in index.l0.nearest_coarse(&query, index.l0.probe_coarse_count()) {
            let l2_count = routing.l2_count_for_coarse(coarse_id);
            for l2_id in 0..l2_count {
                if l2_count <= 1 {
                    continue;
                }
                if let Some(seg) = index.l2.get(&(coarse_id, l2_id)) {
                    fetched.insert(
                        CentroidIndexL2::key(namespace, field, coarse_id, l2_id),
                        seg.encode().unwrap(),
                    );
                }
            }
        }
        let fine_ids = probe_fine_centroids_parts(
            &index.l0,
            &index.l1,
            Some(routing),
            &index.l2,
            &query,
        );
        for fine_id in &fine_ids {
            if let Some(cluster) = index.clusters.get(fine_id) {
                fetched.insert(
                    ClusterSegment::key(namespace, field, *fine_id),
                    cluster.encode().unwrap(),
                );
            }
        }
        let (probed, _) = assemble_vector_index_probed(
            namespace,
            &NamespaceMeta::default(),
            field,
            index.l0.clone(),
            &fetched,
            &query,
        )
        .unwrap();
        assert!(probed.routing.is_some(), "probed assemble must decode routing");
        assert!(!probed.l2.is_empty(), "probed assemble must decode L2 segments");
        assert_eq!(
            probe_fine_centroids_parts(
                &probed.l0,
                &probed.l1,
                probed.routing.as_ref(),
                &probed.l2,
                &query,
            ),
            index.probe_fine_centroids(&query),
            "probed fine ids must match full index probe descent"
        );
    }

    fn l0_nearest_coarse_includes(coarse_id: u32, l0: &CentroidIndexL0, query: &[f64]) -> bool {
        l0.nearest_coarse(query, l0.probe_coarse_count())
            .contains(&coarse_id)
    }

    #[test]
    fn round2_bootstrap_keys_fts_filter_no_l0_no_clusters() {
        let meta = NamespaceMeta {
            index_cursor: 3,
            fts_segment_id: 3,
            filter_segment_id: 3,
            vector_segment_id: 3,
            vector_field: "emb".into(),
            dimensions: 2,
            vector_fields: vec![VectorFieldConfig {
                name: "emb".into(),
                dimensions: 2,
                segment_id: 3,
                segment_ids: vec![3],
                ..Default::default()
            }],
            ..Default::default()
        };
        let keys = round2_bootstrap_keys("ns", &meta);
        assert!(!keys.iter().any(|k| k.contains("centroids-l0")));
        assert!(keys.iter().any(|k| k.contains("fts-")));
        assert!(keys.iter().any(|k| k.contains("filter-")));
        assert!(!keys.iter().any(|k| k.contains("clusters-")));
        let l0_keys = l0_keys_for_meta("ns", &meta);
        assert!(l0_keys.iter().any(|k| k.contains("centroids-l0")));
    }

    #[test]
    fn plan_cold_query_round3_bounded_at_4k_fine_total() {
        use crate::index::vector::CentroidIndexL1;
        let meta = NamespaceMeta {
            index_cursor: 1,
            vector_segment_id: 1,
            vector_field: "emb".into(),
            dimensions: 2,
            vector_fields: vec![VectorFieldConfig {
                name: "emb".into(),
                dimensions: 2,
                segment_id: 1,
                segment_ids: vec![1],
                ..Default::default()
            }],
            ..Default::default()
        };
        let l0 = CentroidIndexL0 {
            vector_field: "emb".into(),
            num_coarse: 16,
            num_fine_total: 4000,
            probe_coarse: DEFAULT_PROBE_COARSE,
            probe_fine: DEFAULT_PROBE_FINE,
            fine_counts: vec![250; 16],
            centroids: (0..16)
                .map(|i| vec![if i == 0 { 1.0 } else { 0.0 }, 0.0])
                .collect(),
            dimensions: 2,
            distance_metric: crate::meta::DistanceMetric::CosineDistance,
            ..Default::default()
        };
        let query = vec![1.0, 0.0];
        let mut l1_by_field = HashMap::new();
        let mut l1_loaded = HashMap::new();
        for coarse_id in l0.nearest_coarse(&query, l0.probe_coarse_count()) {
            let start = l0.global_id_start(coarse_id);
            l1_loaded.insert(
                coarse_id,
                CentroidIndexL1 {
                    segment_id: 1,
                    coarse_id,
                    global_id_start: start,
                    num_fine: 250,
                    centroids: (0..250)
                        .map(|i| vec![if i == 0 { 1.0 } else { 0.0 }, 0.0])
                        .collect(),
                },
            );
        }
        l1_by_field.insert("emb".into(), l1_loaded);
        let mut l0_by_field = HashMap::new();
        l0_by_field.insert("emb".into(), l0);
        let plan = plan_cold_query(
            "ns",
            &meta,
            &[("emb".into(), query)],
            &l0_by_field,
            Some(&l1_by_field),
            ColdPlanOpts::default(),
        );
        let r3 = plan.round3_keys.len();
        assert!(
            r3 >= 8 && r3 <= 64,
            "round-3 key list should be probe-bounded (8–64), got {r3}"
        );
        assert!(r3 < 4000, "round-3 must not scale with num_fine_total");
        assert!(plan.round3_keys.iter().any(|k| k.contains("centroids-l1-")));
        assert!(plan.round3_keys.iter().any(|k| k.contains("clusters-")));
    }

    #[test]
    fn cold_vector_field_path_traversal_rejected_in_probe_keys() {
        let meta = NamespaceMeta::default();
        let l0 = CentroidIndexL0::default();
        let query = vec![1.0, 0.0];
        assert!(l1_keys_for_query_probe("ns", &meta, "../other", &l0, &query).is_err());
        assert!(round3_keys_for_query(
            "ns",
            &meta,
            "../../escape",
            &l0,
            &HashMap::new(),
            &query,
            None,
            &HashMap::new(),
        )
        .is_err());
    }

    #[test]
    fn cold_s3_keys_with_dot_dot_fail_validation_before_fetch() {
        let bad = format!(
            "{}ns/index/../../other-ns/index/emb/centroids-l0.bin",
            crate::models::ROOT_PREFIX
        );
        assert!(validate_cold_s3_keys(&[bad]).is_err());
    }

    #[test]
    fn record_cold_s3_keys_fetched_returns_key_count() {
        assert_eq!(record_cold_s3_keys_fetched(0), 0);
        assert_eq!(record_cold_s3_keys_fetched(9), 9);
    }

    #[cfg(feature = "metrics")]
    #[test]
    fn record_cold_s3_keys_fetched_increments_prometheus_counter() {
        use crate::metrics::COLD_S3_KEYS_FETCHED;
        let before = COLD_S3_KEYS_FETCHED.get();
        record_cold_s3_keys_fetched(4);
        assert!(
            COLD_S3_KEYS_FETCHED.get() >= before + 4.0,
            "cold S3 keys counter should increase"
        );
    }

    #[test]
    fn cold_plan_storage_roundtrips_at_most_four_when_caught_up() {
        let meta = NamespaceMeta {
            index_cursor: 10,
            wal_commit_seq: 10,
            fts_segment_id: 2,
            filter_segment_id: 2,
            vector_segment_id: 2,
            vector_field: "emb".into(),
            dimensions: 2,
            vector_fields: vec![VectorFieldConfig {
                name: "emb".into(),
                dimensions: 2,
                segment_id: 2,
                segment_ids: vec![2],
                ..Default::default()
            }],
            ..Default::default()
        };
        let l0 = CentroidIndexL0 {
            vector_field: "emb".into(),
            num_coarse: 4,
            num_fine_total: 16,
            fine_counts: vec![4; 4],
            centroids: vec![vec![1.0, 0.0]; 4],
            dimensions: 2,
            probe_coarse: DEFAULT_PROBE_COARSE,
            probe_fine: DEFAULT_PROBE_FINE,
            ..Default::default()
        };
        let mut l0_by_field = HashMap::new();
        l0_by_field.insert("emb".into(), l0);
        let plan = plan_cold_query(
            "ns",
            &meta,
            &[("emb".into(), vec![1.0, 0.0])],
            &l0_by_field,
            None,
            ColdPlanOpts {
                include_wal_round: true,
                include_wal_tail: false,
            },
        );
        assert_eq!(cold_plan_storage_roundtrips(&plan), 3);
        let full = plan_cold_query(
            "ns",
            &meta,
            &[("emb".into(), vec![1.0, 0.0])],
            &l0_by_field,
            None,
            ColdPlanOpts {
                include_wal_round: true,
                include_wal_tail: true,
            },
        );
        assert!(cold_plan_storage_roundtrips(&full) <= 4);
    }

    #[test]
    fn cold_s3_concurrency_default_and_parse() {
        assert_eq!(DEFAULT_COLD_S3_CONCURRENCY, 32);
        std::env::remove_var("OPENPUFFER_COLD_S3_CONCURRENCY");
        assert_eq!(cold_s3_concurrency(), 32);
        std::env::set_var("OPENPUFFER_COLD_S3_CONCURRENCY", "16");
        assert_eq!(cold_s3_concurrency(), 16);
        std::env::remove_var("OPENPUFFER_COLD_S3_CONCURRENCY");
    }

    #[test]
    fn cold_fetch_sub_batch_count_caps_parallel_gets() {
        assert_eq!(cold_fetch_sub_batch_count(0, 128), 0);
        assert_eq!(cold_fetch_sub_batch_count(1, 128), 1);
        assert_eq!(cold_fetch_sub_batch_count(128, 128), 1);
        assert_eq!(cold_fetch_sub_batch_count(129, 128), 2);
        assert_eq!(
            cold_fetch_sub_batch_count(500, DEFAULT_COLD_MAX_KEYS_PER_ROUND),
            4,
            "500 keys at cap 128 → four sub-batches"
        );
    }

    #[test]
    fn plan_cold_query_500_round3_keys_one_roundtrip_unchanged() {
        let mut plan = ColdQueryPlan::default();
        plan.round3_keys = (0..500)
            .map(|i| format!("ns/index/clusters-{i:08}.bin"))
            .collect();
        assert_eq!(plan.round3_keys.len(), 500);
        assert_eq!(
            cold_plan_storage_roundtrips(&plan),
            1,
            "round-3 key count does not add logical roundtrips"
        );
        assert_eq!(
            cold_fetch_sub_batch_count(plan.round3_keys.len(), DEFAULT_COLD_MAX_KEYS_PER_ROUND),
            4,
            "fetch_round would issue four capped sub-batches inside one roundtrip"
        );
    }

    #[test]
    fn plan_cold_query_hybrid_probe_includes_fts_in_round2() {
        let meta = NamespaceMeta {
            index_cursor: 3,
            fts_segment_id: 7,
            filter_segment_id: 2,
            vector_segment_id: 3,
            vector_field: "emb".into(),
            dimensions: 2,
            vector_fields: vec![VectorFieldConfig {
                name: "emb".into(),
                dimensions: 2,
                segment_id: 3,
                segment_ids: vec![3],
                ..Default::default()
            }],
            ..Default::default()
        };
        let l0 = CentroidIndexL0 {
            vector_field: "emb".into(),
            num_coarse: 4,
            num_fine_total: 16,
            dimensions: 2,
            ..Default::default()
        };
        let mut l0_map = HashMap::new();
        l0_map.insert("emb".into(), l0);
        let plan = plan_cold_query(
            "ns",
            &meta,
            &[("emb".into(), vec![1.0, 0.0])],
            &l0_map,
            None,
            ColdPlanOpts {
                include_wal_round: false,
                include_wal_tail: false,
            },
        );
        assert!(
            plan.round2_keys.iter().any(|k| k.contains("fts-00000007")),
            "hybrid cold plan must load FTS in bootstrap round 2, got {:?}",
            plan.round2_keys
        );
    }

    #[test]
    fn plan_cold_query_eventual_omits_wal_rounds() {
        let meta = NamespaceMeta {
            index_cursor: 5,
            wal_commit_seq: 8,
            wal_snapshot_seq: 0,
            ..Default::default()
        };
        let eventual = plan_cold_query(
            "ns",
            &meta,
            &[],
            &HashMap::new(),
            None,
            ColdPlanOpts {
                include_wal_round: false,
                include_wal_tail: false,
            },
        );
        assert!(eventual.round1_keys.is_empty(), "eventual skips round-1 WAL");
        assert!(eventual.round4_keys.is_empty(), "eventual skips round-4 tail");
        let strong = plan_cold_query(
            "ns",
            &meta,
            &[],
            &HashMap::new(),
            None,
            ColdPlanOpts {
                include_wal_round: true,
                include_wal_tail: true,
            },
        );
        assert!(!strong.round1_keys.is_empty());
        assert!(!strong.round4_keys.is_empty());
        assert!(
            cold_plan_storage_roundtrips(&eventual) < cold_plan_storage_roundtrips(&strong),
            "eventual plan must use fewer logical roundtrips than strong when index lags"
        );
    }

    #[test]
    fn unindexed_wal_tail_keys_match_plan_round4() {
        let meta = NamespaceMeta {
            index_cursor: 5,
            wal_commit_seq: 8,
            ..Default::default()
        };
        let keys = unindexed_wal_tail_keys("ns", &meta);
        assert_eq!(keys.len(), 3);
        assert!(keys[0].ends_with("00000006.bin"));
        let plan = plan_cold_query(
            "ns",
            &meta,
            &[],
            &HashMap::new(),
            None,
            ColdPlanOpts {
                include_wal_round: false,
                include_wal_tail: true,
            },
        );
        assert_eq!(plan.round4_keys, keys);
        assert_eq!(cold_plan_storage_roundtrips(&plan), 1);
    }
}
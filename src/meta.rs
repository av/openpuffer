//! Namespace metadata: `openpuffer/{ns}/meta.json` with CAS commit point.

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::index::vector::{vector_fields_from_schema, ANN_VERSION_V2};
use crate::schema::{vector_dimensions_for_field, vector_element_for_field, VectorElement};

pub const META_RETRIES: u32 = 8;

/// turbopuffer allows at most two vector columns per namespace.
pub const MAX_VECTOR_FIELDS: usize = 2;

/// One indexed vector column (ANN centroid tree on S3 under `index/{name}/`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct VectorFieldConfig {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub dimensions: u32,
    #[serde(default)]
    pub element: VectorElement,
    /// Latest WAL seq when this field's centroids/clusters were written.
    #[serde(default)]
    pub segment_id: u64,
    #[serde(default)]
    pub segment_ids: Vec<u64>,
}

/// ANN distance metric stored in namespace metadata.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DistanceMetric {
    #[default]
    CosineDistance,
    EuclideanSquared,
}

/// Parse turbopuffer write/query `distance_metric` string.
pub fn parse_distance_metric(s: &str) -> Result<DistanceMetric> {
    match s {
        "cosine_distance" => Ok(DistanceMetric::CosineDistance),
        "euclidean_squared" => Ok(DistanceMetric::EuclideanSquared),
        other => Err(anyhow!(
            "invalid distance_metric {other:?}; expected cosine_distance or euclidean_squared"
        )),
    }
}

/// Resolve metric for the next meta commit: set on first WAL, enforce match thereafter.
pub fn resolve_distance_metric(
    meta: &NamespaceMeta,
    requested: Option<DistanceMetric>,
) -> Result<DistanceMetric> {
    match requested {
        None => Ok(meta.distance_metric),
        Some(m) if meta.wal_commit_seq == 0 => Ok(m),
        Some(m) if m == meta.distance_metric => Ok(meta.distance_metric),
        Some(m) => Err(anyhow!(
            "distance_metric {} conflicts with namespace {}",
            metric_name(m),
            metric_name(meta.distance_metric)
        )),
    }
}

fn metric_name(m: DistanceMetric) -> &'static str {
    match m {
        DistanceMetric::CosineDistance => "cosine_distance",
        DistanceMetric::EuclideanSquared => "euclidean_squared",
    }
}

/// Durable namespace state on object storage (turbopuffer-style commit + index cursor).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NamespaceMeta {
    /// Last WAL sequence fully merged into `index/` (0 until indexer runs).
    pub index_cursor: u64,
    /// Latest FTS segment id on S3 (`index/fts-{id:08}.bin`).
    #[serde(default)]
    pub fts_segment_id: u64,
    /// FTS segment generation chain (each indexer pass appends WAL seq of new segment file).
    #[serde(default)]
    pub fts_segment_ids: Vec<u64>,
    /// Latest vector index segment id (centroids + clusters written at this WAL seq).
    #[serde(default)]
    pub vector_segment_id: u64,
    /// Vector index generation chain (WAL seq when centroids/clusters were written).
    #[serde(default)]
    pub vector_segment_ids: Vec<u64>,
    /// Latest attribute filter index segment id (`index/filter-{id:08}.bin`).
    #[serde(default)]
    pub filter_segment_id: u64,
    /// Filter segment generation chain.
    #[serde(default)]
    pub filter_segment_ids: Vec<u64>,
    /// Indexed vector columns (max 2). Preferred over legacy `vector_field` / `dimensions`.
    #[serde(default)]
    pub vector_fields: Vec<VectorFieldConfig>,
    /// Primary indexed vector attribute (first entry in `vector_fields`; legacy compat).
    #[serde(default)]
    pub vector_field: String,
    /// Dimensions of `vector_field` (0 if no ANN index).
    #[serde(default)]
    pub dimensions: u32,
    /// Last committed WAL file sequence (`wal/{seq:08}.bin`).
    pub wal_commit_seq: u64,
    /// WAL seq materialized in `wal/snapshot.bin` after compaction (0 = none).
    #[serde(default)]
    pub wal_snapshot_seq: u64,
    #[serde(default)]
    pub schema: Value,
    #[serde(default)]
    pub distance_metric: DistanceMetric,
    /// Preferred ANN layout for index builds on this namespace (`2` default, `3` when indexed under v3).
    #[serde(
        default = "default_preferred_ann_version",
        skip_serializing_if = "is_default_preferred_ann_version"
    )]
    pub preferred_ann_version: u8,
}

fn default_preferred_ann_version() -> u8 {
    ANN_VERSION_V2
}

fn is_default_preferred_ann_version(v: &u8) -> bool {
    *v == ANN_VERSION_V2
}

impl Default for NamespaceMeta {
    fn default() -> Self {
        Self {
            index_cursor: 0,
            fts_segment_id: 0,
            fts_segment_ids: Vec::new(),
            vector_segment_id: 0,
            vector_segment_ids: Vec::new(),
            filter_segment_id: 0,
            filter_segment_ids: Vec::new(),
            vector_fields: Vec::new(),
            vector_field: String::new(),
            dimensions: 0,
            wal_commit_seq: 0,
            wal_snapshot_seq: 0,
            schema: Value::Object(serde_json::Map::new()),
            distance_metric: DistanceMetric::default(),
            preferred_ann_version: default_preferred_ann_version(),
        }
    }
}

pub fn meta_key(namespace: &str) -> String {
    format!("{}{namespace}/meta.json", crate::models::ROOT_PREFIX)
}

/// Next WAL sequence after a successful commit.
pub fn next_wal_seq(meta: &NamespaceMeta) -> u64 {
    meta.wal_commit_seq.saturating_add(1)
}

/// Append a segment id to a generation chain (dedupes consecutive duplicates).
pub fn push_segment_id(ids: &mut Vec<u64>, id: u64) {
    if id == 0 {
        return;
    }
    if ids.last() != Some(&id) {
        ids.push(id);
    }
}

/// Build updated metadata after appending WAL object `seq` (CAS payload).
pub fn meta_after_wal_commit(meta: &NamespaceMeta, seq: u64) -> Result<NamespaceMeta> {
    meta_after_wal_commit_options(meta, seq, None, None)
}

/// Like [`meta_after_wal_commit`], optionally merging a write-time schema patch.
pub fn meta_after_wal_commit_with_schema(
    meta: &NamespaceMeta,
    seq: u64,
    schema_patch: Option<&Value>,
) -> Result<NamespaceMeta> {
    meta_after_wal_commit_options(meta, seq, schema_patch, None)
}

/// Like [`meta_after_wal_commit_with_schema`], optionally setting/enforcing `distance_metric`.
pub fn meta_after_wal_commit_options(
    meta: &NamespaceMeta,
    seq: u64,
    schema_patch: Option<&Value>,
    distance_metric: Option<DistanceMetric>,
) -> Result<NamespaceMeta> {
    if seq != meta.wal_commit_seq.saturating_add(1) {
        return Err(anyhow!(
            "wal seq {seq} does not follow commit point {}",
            meta.wal_commit_seq
        ));
    }
    let mut next = meta.clone();
    next.wal_commit_seq = seq;
    if let Some(patch) = schema_patch {
        next.schema = crate::schema::merge_schema(&next.schema, patch)?;
    }
    hydrate_vector_fields_from_schema(&mut next);
    sync_legacy_vector_fields(&mut next);
    next.distance_metric = resolve_distance_metric(meta, distance_metric)?;
    Ok(next)
}

/// Effective vector field configs (new `vector_fields` or legacy single-field meta).
pub fn effective_vector_fields(meta: &NamespaceMeta) -> Vec<VectorFieldConfig> {
    if !meta.vector_fields.is_empty() {
        return meta.vector_fields.clone();
    }
    if meta.vector_field.is_empty() || meta.dimensions == 0 {
        return Vec::new();
    }
    vec![VectorFieldConfig {
        name: meta.vector_field.clone(),
        dimensions: meta.dimensions,
        element: VectorElement::default(),
        segment_id: meta.vector_segment_id,
        segment_ids: meta.vector_segment_ids.clone(),
    }]
}

/// Ensure `vector_fields` reflects vector columns declared in `schema` (pre-index).
pub fn hydrate_vector_fields_from_schema(meta: &mut NamespaceMeta) {
    let names: Vec<String> = vector_fields_from_schema(&meta.schema)
        .into_iter()
        .take(MAX_VECTOR_FIELDS)
        .collect();
    for name in names {
        let dims = vector_dimensions_for_field(&meta.schema, &name).unwrap_or(0);
        let element = vector_element_for_field(&meta.schema, &name);
        if let Some(slot) = meta.vector_fields.iter_mut().find(|f| f.name == name) {
            if slot.segment_id == 0 {
                slot.dimensions = dims;
            }
            slot.element = element;
            continue;
        }
        if meta.vector_fields.len() < MAX_VECTOR_FIELDS {
            meta.vector_fields.push(VectorFieldConfig {
                name,
                dimensions: dims,
                element,
                segment_id: 0,
                segment_ids: Vec::new(),
            });
        }
    }
}

/// Keep legacy `vector_field` / `dimensions` / `vector_segment_id` aligned with the first column.
pub fn sync_legacy_vector_fields(meta: &mut NamespaceMeta) {
    if let Some(first) = meta.vector_fields.first() {
        meta.vector_field = first.name.clone();
        meta.dimensions = first.dimensions;
        meta.vector_segment_id = first.segment_id;
        meta.vector_segment_ids = first.segment_ids.clone();
    } else if !meta.vector_field.is_empty() && meta.dimensions > 0 {
        // Legacy meta without `vector_fields` populated yet.
    } else {
        meta.vector_field.clear();
        meta.dimensions = 0;
        meta.vector_segment_id = 0;
        meta.vector_segment_ids.clear();
    }
}

/// Lookup indexed config for a vector attribute name.
pub fn vector_field_config<'a>(meta: &'a NamespaceMeta, name: &str) -> Option<&'a VectorFieldConfig> {
    meta.vector_fields
        .iter()
        .find(|f| f.name == name)
        .or_else(|| {
            if meta.vector_field == name && meta.dimensions > 0 {
                None // caller should use effective_vector_fields for legacy-only meta
            } else {
                None
            }
        })
}

/// True when this field's index lives at legacy `index/centroids-l0.bin` (pre multi-column).
pub fn vector_index_uses_legacy_paths(meta: &NamespaceMeta, field: &str) -> bool {
    meta.vector_fields.is_empty()
        && meta.vector_field == field
        && meta.vector_segment_id > 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_json_roundtrip() {
        let meta = NamespaceMeta {
            index_cursor: 3,
            fts_segment_id: 3,
            fts_segment_ids: vec![3],
            filter_segment_id: 3,
            filter_segment_ids: vec![3],
            vector_fields: Vec::new(),
            vector_segment_id: 3,
            vector_segment_ids: vec![3],
            vector_field: "embedding".into(),
            dimensions: 128,
            wal_commit_seq: 10,
            wal_snapshot_seq: 0,
            schema: serde_json::json!({"embedding": {"type": "[]f32"}}),
            distance_metric: DistanceMetric::EuclideanSquared,
            ..Default::default()
        };
        let json = serde_json::to_vec(&meta).unwrap();
        let back: NamespaceMeta = serde_json::from_slice(&json).unwrap();
        assert_eq!(back, meta);
    }

    #[test]
    fn meta_after_wal_commit_advances_seq() {
        let meta = NamespaceMeta::default();
        let next = meta_after_wal_commit(&meta, 1).unwrap();
        assert_eq!(next.wal_commit_seq, 1);
        assert_eq!(next.index_cursor, 0);
    }

    #[test]
    fn meta_after_wal_commit_merges_schema() {
        let meta = NamespaceMeta::default();
        let patch = serde_json::json!({
            "text": {"type": "string", "full_text_search": true},
            "embedding": "[128]f32"
        });
        let next = meta_after_wal_commit_with_schema(&meta, 1, Some(&patch)).unwrap();
        assert_eq!(next.vector_field, "embedding");
        assert_eq!(next.dimensions, 128);
        assert_eq!(next.schema["text"]["full_text_search"], serde_json::json!(true));
        assert_eq!(next.schema["embedding"], serde_json::json!("[128]f32"));
    }

    #[test]
    fn meta_after_wal_commit_rejects_gap() {
        let meta = NamespaceMeta {
            wal_commit_seq: 5,
            ..Default::default()
        };
        assert!(meta_after_wal_commit(&meta, 7).is_err());
        assert!(meta_after_wal_commit(&meta, 6).is_ok());
    }

    #[test]
    fn push_segment_id_dedupes_consecutive() {
        let mut ids = vec![1, 2];
        push_segment_id(&mut ids, 2);
        assert_eq!(ids, vec![1, 2]);
        push_segment_id(&mut ids, 3);
        assert_eq!(ids, vec![1, 2, 3]);
    }

    #[test]
    fn next_wal_seq_from_commit_point() {
        let meta = NamespaceMeta {
            wal_commit_seq: 42,
            ..Default::default()
        };
        assert_eq!(next_wal_seq(&meta), 43);
    }

    #[test]
    fn parse_distance_metric_accepts_turbopuffer_names() {
        assert_eq!(
            parse_distance_metric("cosine_distance").unwrap(),
            DistanceMetric::CosineDistance
        );
        assert_eq!(
            parse_distance_metric("euclidean_squared").unwrap(),
            DistanceMetric::EuclideanSquared
        );
        assert!(parse_distance_metric("l2").is_err());
    }

    #[test]
    fn resolve_distance_metric_first_write_and_enforce() {
        let fresh = NamespaceMeta::default();
        assert_eq!(
            resolve_distance_metric(&fresh, Some(DistanceMetric::EuclideanSquared)).unwrap(),
            DistanceMetric::EuclideanSquared
        );
        let committed = NamespaceMeta {
            wal_commit_seq: 1,
            distance_metric: DistanceMetric::EuclideanSquared,
            ..Default::default()
        };
        assert_eq!(
            resolve_distance_metric(&committed, None).unwrap(),
            DistanceMetric::EuclideanSquared
        );
        assert_eq!(
            resolve_distance_metric(&committed, Some(DistanceMetric::EuclideanSquared)).unwrap(),
            DistanceMetric::EuclideanSquared
        );
        assert!(resolve_distance_metric(&committed, Some(DistanceMetric::CosineDistance)).is_err());
    }

    #[test]
    fn meta_json_omits_default_preferred_ann_version() {
        let meta = NamespaceMeta::default();
        let json = serde_json::to_value(&meta).unwrap();
        assert!(json.get("preferred_ann_version").is_none());
        let back: NamespaceMeta = serde_json::from_value(json).unwrap();
        assert_eq!(back.preferred_ann_version, ANN_VERSION_V2);
    }

    #[test]
    fn meta_after_wal_commit_sets_distance_metric_on_first_write() {
        let meta = NamespaceMeta::default();
        let next = meta_after_wal_commit_options(
            &meta,
            1,
            None,
            Some(DistanceMetric::EuclideanSquared),
        )
        .unwrap();
        assert_eq!(next.distance_metric, DistanceMetric::EuclideanSquared);
    }
}
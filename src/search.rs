//! Query planner: candidate generation from FTS postings / ANN clusters / WAL tail,
//! then score-only-on-candidates ranked retrieval (hybrid Sum/Product supported).

use crate::filter::{eval_filter, parse_filter, FilterExpr};
use crate::index::filter::FilterSegment;
use crate::index::fts::{bm25_doc_score, extract_index_text, FtsSegment};
use crate::index::vector::{extract_vector, score_vector, value_to_f64_vec, VectorIndex};

use crate::billing::{
    avg_document_logical_bytes, billable_logical_bytes_queried, billable_logical_bytes_returned,
};
use crate::meta::NamespaceMeta;
use crate::models::{Document, QueryBilling, QueryPerformance, QueryRequest, QueryResponse, QueryRow};
use crate::vector_encoding::{
    project_row_attributes, IncludeVectors, VectorEncoding,
};
use anyhow::{anyhow, bail, Result};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::time::Instant;

/// Default `top_k` when omitted (turbopuffer-style).
pub const DEFAULT_TOP_K: usize = 10;
/// Hard cap to avoid candidate-pool OOM on pathological requests.
pub const MAX_TOP_K: usize = 1200;

/// How unindexed WAL tail participates in candidate collection and scoring.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum QueryConsistency {
    /// Indexed segments + exhaustive scan of docs touched in `(index_cursor, wal_commit_seq]`.
    #[default]
    Strong,
    /// Indexed segments only; skip WAL tail (faster, may miss very recent writes until indexed).
    Eventual,
}

impl QueryConsistency {
    pub fn parse(s: Option<&str>) -> Result<Self> {
        match s.map(|x| x.to_ascii_lowercase()).as_deref() {
            None | Some("strong") => Ok(Self::Strong),
            Some("eventual") => Ok(Self::Eventual),
            Some(other) => bail!("unknown consistency mode: {other} (use strong or eventual)"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct QueryContext<'a> {
    pub docs: &'a HashMap<String, Document>,
    pub meta: &'a NamespaceMeta,
    pub fts: Option<&'a FtsSegment>,
    /// ANN indexes keyed by vector attribute name (up to 2 columns).
    pub vectors: &'a HashMap<String, VectorIndex>,
    pub filter_index: Option<&'a FilterSegment>,
    pub tail_doc_ids: &'a HashSet<String>,
    pub consistency: QueryConsistency,
    /// Logical S3 fetch rounds for cold query (batched parallel plan).
    pub storage_roundtrips: Option<u32>,
    /// Cold-query S3 keys fetched (when `storage_roundtrips` is set).
    pub cold_s3_keys_fetched: Option<u32>,
    /// ANN probe cluster count for this query.
    pub ann_probed_clusters: Option<u32>,
    /// When `Some(true)`, widen ANN candidates to the full probed cluster pool and exact-score view vectors.
    /// When `None`, falls back to `OPENPUFFER_ANN_RERANK` env. Default query path uses probe-only (`query_ann` pool).
    pub ann_rerank: Option<bool>,
}

impl QueryContext<'_> {
    pub fn ann_rerank_enabled(&self) -> bool {
        self.ann_rerank
            .unwrap_or_else(crate::config::ann_rerank_from_env)
    }
}

#[derive(Debug, Clone)]
enum Ranker {
    Vector {
        field: String,
        query: Vec<f64>,
    },
    Bm25 {
        field: String,
        query: String,
    },
    Sum(Vec<Ranker>),
    Product(Vec<Ranker>),
}

#[derive(Debug, Clone, Copy)]
enum CandidateMerge {
    Union,
    Intersection,
}

/// Counters for O(n) regression detection (full scan vs indexed candidates).
#[derive(Debug, Default, Clone, Copy)]
struct QueryStats {
    /// Docs scanned in no-index fallback (entire namespace).
    full_scan_docs: u64,
    /// Docs in unindexed WAL tail examined for candidates/scoring.
    tail_docs_examined: u64,
}

struct QueryPlanner<'a, 'b> {
    ctx: &'a QueryContext<'b>,
    /// Widen indexed candidate pools before final top_k truncation.
    candidate_pool: usize,
    stats: QueryStats,
}

/// Parse turbopuffer `include_attributes` (`true` | `false` | attribute name list).
fn parse_include_attributes(v: &Option<Value>) -> Result<(bool, Option<HashSet<String>>)> {
    match v {
        None => Ok((true, None)),
        Some(Value::Bool(false)) => Ok((false, None)),
        Some(Value::Bool(true)) => Ok((true, None)),
        Some(Value::Array(arr)) => {
            let names: HashSet<String> = arr
                .iter()
                .map(|x| {
                    x.as_str()
                        .map(str::to_string)
                        .ok_or_else(|| anyhow!("include_attributes entries must be strings"))
                })
                .collect::<Result<_>>()?;
            Ok((true, Some(names)))
        }
        Some(_) => bail!("include_attributes must be true, false, or an array of field names"),
    }
}

pub fn execute_query(ctx: &QueryContext<'_>, req: &QueryRequest) -> Result<QueryResponse> {
    let started = Instant::now();
    let filter_expr = match req.filters.as_ref() {
        None => None,
        Some(v) if v.is_null() => None,
        Some(v) => Some(parse_filter(v)?),
    };

    let top_k = parse_top_k(req.top_k)?;
    let consistency = QueryConsistency::parse(req.consistency.as_deref())?;
    let effective_ctx = QueryContext {
        consistency,
        ..ctx.clone()
    };

    let ranker = parse_rank_by(&req.rank_by)?;
    let order_by = match req.order_by.as_ref() {
        None => None,
        Some(v) if v.is_null() => None,
        Some(v) => Some(parse_order_by(v)?),
    };
    validate_ranker_vector_dims(&effective_ctx, &ranker)?;
    let mut planner = QueryPlanner {
        ctx: &effective_ctx,
        candidate_pool: top_k.saturating_mul(8).max(64),
        stats: QueryStats::default(),
    };

    let mut candidates = planner.collect_candidates(&ranker, CandidateMerge::Union)?;
    if let Some(expr) = filter_expr.as_ref() {
        let allowed = matching_doc_ids_for_filter(&effective_ctx, expr)?;
        candidates.retain(|id| allowed.contains(id));
    }
    let scored = if candidates.is_empty() {
        Vec::new()
    } else {
        planner.score_candidates(&ranker, &candidates)?
    };

    let mut ranked = scored;
    ranked.sort_by(|a, b| {
        let by_score = b
            .1
            .partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal);
        if let Some(ob) = order_by.as_ref() {
            by_score
                .then_with(|| compare_docs_by_attribute(&effective_ctx, &a.0, &b.0, ob))
                .then_with(|| a.0.cmp(&b.0))
        } else {
            by_score.then_with(|| a.0.cmp(&b.0))
        }
    });
    let scored_count = ranked.len() as u64;
    ranked.truncate(top_k);

    let (include_attrs, include_attr_names) = parse_include_attributes(&req.include_attributes)?;
    let include_vectors = IncludeVectors::parse(req.include_vectors.as_ref())?;
    let vector_encoding = VectorEncoding::parse(req.vector_encoding.as_deref())?;
    let rows: Vec<QueryRow> = ranked
        .into_iter()
        .map(|(id, score)| {
            let attributes = effective_ctx.docs.get(&id).and_then(|doc| {
                project_row_attributes(
                    doc,
                    effective_ctx.meta,
                    include_attrs,
                    include_attr_names.as_ref(),
                    &include_vectors,
                    vector_encoding,
                )
            });
            QueryRow {
                id,
                attributes,
                dist: Some(score),
            }
        })
        .collect();

    let namespace_size = effective_ctx.docs.len() as u64;
    let candidate_count = candidates.len() as u64;
    let candidates_ratio = if namespace_size == 0 {
        0.0
    } else {
        candidate_count as f64 / namespace_size as f64
    };
    let avg_doc_bytes = avg_document_logical_bytes(effective_ctx.docs);
    let billing = QueryBilling {
        billable_logical_bytes_queried: billable_logical_bytes_queried(
            candidate_count,
            avg_doc_bytes,
        ),
        billable_logical_bytes_returned: billable_logical_bytes_returned(&rows),
    };
    let elapsed = started.elapsed();
    let elapsed_secs = elapsed.as_secs_f64();
    crate::metrics::observe_query_duration_seconds(elapsed_secs);
    if effective_ctx.storage_roundtrips.is_some() {
        crate::metrics::observe_cold_query_duration_seconds(elapsed_secs);
    }

    let performance = QueryPerformance {
        approx_namespace_size: namespace_size,
        candidates: candidate_count,
        candidates_ratio,
        scored: scored_count,
        exhaustive_search_count: planner
            .stats
            .full_scan_docs
            .saturating_add(planner.stats.tail_docs_examined),
        query_execution_us: elapsed.as_micros() as u64,
        storage_roundtrips: effective_ctx.storage_roundtrips,
        cold_s3_keys_fetched: effective_ctx.cold_s3_keys_fetched,
        ann_probed_clusters: effective_ctx.ann_probed_clusters.filter(|&n| n > 0),
        billing,
    };

    Ok(QueryResponse {
        rows,
        performance: Some(performance),
    })
}

/// Doc ids matching a filter (indexed segment + strong-consistency WAL tail corrections).
///
/// Used by query filtering and `delete_by_filter` / `patch_by_filter` on the write path.
pub fn matching_doc_ids_for_filter(
    ctx: &QueryContext<'_>,
    expr: &FilterExpr,
) -> Result<HashSet<String>> {
    let mut ids = if let Some(seg) = ctx.filter_index {
        seg.matching_doc_ids(expr)
    } else {
        HashSet::new()
    };

    if ctx.filter_index.is_none() {
        for (id, doc) in ctx.docs {
            if eval_filter(expr, doc) {
                ids.insert(id.clone());
            }
        }
        return Ok(ids);
    }

    if ctx.consistency == QueryConsistency::Strong {
        for id in ctx.tail_doc_ids {
            let Some(doc) = ctx.docs.get(id) else {
                ids.remove(id);
                continue;
            };
            if eval_filter(expr, doc) {
                ids.insert(id.clone());
            } else {
                ids.remove(id);
            }
        }
    }

    Ok(if ctx.consistency == QueryConsistency::Eventual {
        ids
    } else {
        ids.into_iter()
            .filter(|id| ctx.docs.contains_key(id))
            .collect()
    })
}

impl<'a, 'b> QueryPlanner<'a, 'b> {
    fn use_tail(&self) -> bool {
        self.ctx.consistency == QueryConsistency::Strong
    }

    fn for_each_tail<F>(&mut self, mut f: F)
    where
        F: FnMut(&String),
    {
        if !self.use_tail() {
            return;
        }
        for id in self.ctx.tail_doc_ids {
            self.stats.tail_docs_examined += 1;
            f(id);
        }
    }

    /// Collect doc ids that may appear in the final ranking for this ranker subtree.
    fn collect_candidates(
        &mut self,
        ranker: &Ranker,
        _merge: CandidateMerge,
    ) -> Result<HashSet<String>> {
        match ranker {
            Ranker::Bm25 { field, query } => self.collect_bm25_candidates(field, query),
            Ranker::Vector { field, query } => self.collect_vector_candidates(field, query),
            Ranker::Sum(subs) => self.merge_child_candidates(subs, CandidateMerge::Union),
            Ranker::Product(subs) => {
                self.merge_child_candidates(subs, CandidateMerge::Intersection)
            }
        }
        .map(|set| self.filter_existing_docs(set))
    }

    fn merge_child_candidates(
        &mut self,
        subs: &[Ranker],
        merge: CandidateMerge,
    ) -> Result<HashSet<String>> {
        let mut acc: Option<HashSet<String>> = None;
        for sub in subs {
            let child = self.collect_candidates(sub, merge)?;
            acc = Some(match acc {
                None => child,
                Some(prev) => match merge {
                    CandidateMerge::Union => prev.union(&child).cloned().collect(),
                    CandidateMerge::Intersection => prev.intersection(&child).cloned().collect(),
                },
            });
        }
        Ok(acc.unwrap_or_default())
    }

    fn filter_existing_docs(&self, ids: HashSet<String>) -> HashSet<String> {
        if self.ctx.consistency == QueryConsistency::Eventual {
            // Indexed ANN/FTS candidates are authoritative; cold eventual may omit WAL replay.
            return ids;
        }
        ids.into_iter()
            .filter(|id| self.ctx.docs.contains_key(id))
            .collect()
    }



    fn collect_bm25_candidates(&mut self, field: &str, query: &str) -> Result<HashSet<String>> {
        let mut ids = HashSet::new();
        if let Some(fts) = self.ctx.fts {
            let _fts_field = if fts.field.is_empty() { field } else { &fts.field };
            for id in fts.candidate_doc_ids(query) {
                if self.use_tail() && self.ctx.tail_doc_ids.contains(&id) {
                    continue;
                }
                ids.insert(id);
            }
            // Also pull high-BM25 hits from index (posting union may miss rare terms).
            for (id, _) in fts.query_bm25(query, self.candidate_pool) {
                if self.use_tail() && self.ctx.tail_doc_ids.contains(&id) {
                    continue;
                }
                ids.insert(id);
            }
        }
        self.for_each_tail(|id| {
            if let Some(doc) = self.ctx.docs.get(id) {
                let text = extract_index_text(doc, field);
                if bm25_doc_score(
                    &text,
                    query,
                    self.ctx.fts.map(|f| f.avg_doc_len()).unwrap_or(1.0),
                    self.ctx.fts.map(|f| f.num_docs).unwrap_or(1).max(1),
                ) > 0.0
                {
                    ids.insert(id.clone());
                }
            }
        });
        if self.ctx.fts.is_none() && ids.is_empty() {
            // No index: fall back to tail-only or full doc map for BM25-only namespaces.
            self.stats.full_scan_docs += self.ctx.docs.len() as u64;
            for (id, doc) in self.ctx.docs {
                let text = extract_index_text(doc, field);
                if bm25_doc_score(&text, query, 1.0, 1) > 0.0 {
                    ids.insert(id.clone());
                }
            }
        }
        Ok(ids)
    }

    fn collect_vector_candidates(&mut self, field: &str, query: &[f64]) -> Result<HashSet<String>> {
        let mut ids = HashSet::new();
        if let Some(vindex) = self.ctx.vectors.get(field) {
            if query.len() == vindex.l0.dimensions as usize {
                if self.ctx.ann_rerank_enabled() {
                    // Re-rank: full probed cluster membership, exact-scored in score_candidates.
                    for id in vindex.ann_pool_doc_ids(query) {
                        if self.use_tail() && self.ctx.tail_doc_ids.contains(&id) {
                            continue;
                        }
                        ids.insert(id);
                    }
                } else {
                    // Probe-only: approximate cluster-vector pool (smaller candidates_ratio).
                    for (id, _) in vindex.query_ann(query, self.candidate_pool) {
                        if self.use_tail() && self.ctx.tail_doc_ids.contains(&id) {
                            continue;
                        }
                        ids.insert(id);
                    }
                }
            }
        }
        self.for_each_tail(|id| {
            if let Some(doc) = self.ctx.docs.get(id) {
                if let Ok(doc_vec) = extract_vector(&doc.attributes, field) {
                    if doc_vec.len() == query.len() {
                        ids.insert(id.clone());
                    }
                }
            }
        });
        if !self.ctx.vectors.contains_key(field) && ids.is_empty() {
            self.stats.full_scan_docs += self.ctx.docs.len() as u64;
            for (id, doc) in self.ctx.docs {
                if extract_vector(&doc.attributes, field)
                    .map(|v| v.len() == query.len())
                    .unwrap_or(false)
                {
                    ids.insert(id.clone());
                }
            }
        }
        Ok(ids)
    }

    fn score_candidates(
        &mut self,
        ranker: &Ranker,
        candidates: &HashSet<String>,
    ) -> Result<Vec<(String, f64)>> {
        match ranker {
            Ranker::Bm25 { field, query } => {
                let raw: Vec<(String, f64)> = candidates
                    .iter()
                    .filter_map(|id| {
                        let doc = self.ctx.docs.get(id)?;
                        Some((id.clone(), self.bm25_raw_score(doc, field, query)))
                    })
                    .collect();
                Ok(raw)
            }
            Ranker::Vector { field, query } => {
                let raw: Vec<(String, f64)> = candidates
                    .iter()
                    .filter_map(|id| {
                        if let Some(doc) = self.ctx.docs.get(id) {
                            return Some((id.clone(), self.vector_raw_score(doc, field, query)));
                        }
                        let vindex = self.ctx.vectors.get(field)?;
                        let score = vindex.score_doc_id(id, query)?;
                        Some((id.clone(), score))
                    })
                    .collect();
                Ok(raw)
            }
            Ranker::Sum(subs) => self.score_composite(subs, candidates, true),
            Ranker::Product(subs) => self.score_composite(subs, candidates, false),
        }
    }

    fn score_composite(
        &mut self,
        subs: &[Ranker],
        candidates: &HashSet<String>,
        sum: bool,
    ) -> Result<Vec<(String, f64)>> {
        let mut per_signal: Vec<HashMap<String, f64>> = Vec::with_capacity(subs.len());
        let mut any_positive_raw: HashMap<String, bool> = HashMap::new();
        for sub in subs {
            let raw = self.score_candidates(sub, candidates)?;
            for (id, s) in &raw {
                if s.is_finite() && *s > 0.0 {
                    any_positive_raw.insert(id.clone(), true);
                }
            }
            per_signal.push(min_max_normalize(raw));
        }
        let mut out = Vec::new();
        for id in candidates {
            let mut parts = Vec::with_capacity(per_signal.len());
            for norm in &per_signal {
                parts.push(*norm.get(id).unwrap_or(&0.0));
            }
            let score: f64 = if sum {
                parts.iter().sum::<f64>()
            } else {
                parts.iter().product::<f64>()
            };
            // Min-max per signal can zero every normalized part for a doc that still has
            // positive raw BM25/vector (common for WAL tail docs in hybrid queries).
            // Only apply the fallback for Sum: one positive raw signal is enough to keep
            // the doc.  For Product, a zero in any signal is semantically correct (the
            // doc failed one criterion), so drop it.
            let keep = if sum {
                score.is_finite()
                    && (score > 0.0
                        || any_positive_raw.get(id).copied().unwrap_or(false))
            } else {
                score.is_finite() && score > 0.0
            };
            if keep {
                out.push((id.clone(), score.max(0.0)));
            }
        }
        Ok(out)
    }

    fn bm25_raw_score(&self, doc: &Document, field: &str, query: &str) -> f64 {
        let text = extract_index_text(doc, field);
        if let Some(fts) = self.ctx.fts {
            bm25_doc_score(&text, query, fts.avg_doc_len(), fts.num_docs.max(1))
        } else {
            bm25_score_legacy(&text, query)
        }
    }

    fn vector_raw_score(&self, doc: &Document, field: &str, query: &[f64]) -> f64 {
        extract_vector(&doc.attributes, field)
            .ok()
            .filter(|v| v.len() == query.len())
            .map(|v| score_vector(query, &v, self.ctx.meta.distance_metric))
            .unwrap_or(f64::NEG_INFINITY)
    }
}

/// Min-max normalize scores to [0, 1] so BM25 and vector signals are comparable in hybrid Sum/Product.
fn min_max_normalize(scored: Vec<(String, f64)>) -> HashMap<String, f64> {
    if scored.is_empty() {
        return HashMap::new();
    }
    let mut min = f64::INFINITY;
    let mut max = f64::NEG_INFINITY;
    for (_, s) in &scored {
        if s.is_finite() {
            min = min.min(*s);
            max = max.max(*s);
        }
    }
    let span = (max - min).max(1e-12);
    scored
        .into_iter()
        .map(|(id, s)| {
            let norm = if !s.is_finite() {
                0.0
            } else if (max - min).abs() < 1e-12 {
                if s > 0.0 { 1.0 } else { 0.0 }
            } else {
                ((s - min) / span).clamp(0.0, 1.0)
            };
            (id, norm)
        })
        .collect()
}

fn parse_top_k(v: Option<u32>) -> Result<usize> {
    match v {
        None => Ok(DEFAULT_TOP_K),
        Some(0) => bail!("top_k must be at least 1"),
        Some(n) if n as usize > MAX_TOP_K => {
            bail!("top_k {n} exceeds maximum of {MAX_TOP_K}");
        }
        Some(n) => Ok(n as usize),
    }
}

/// Reject vector queries whose length disagrees with the indexed dimensionality.
fn validate_ranker_vector_dims(ctx: &QueryContext<'_>, ranker: &Ranker) -> Result<()> {
    match ranker {
        Ranker::Vector { field, query } => validate_query_vector_dims(ctx, field, query)?,
        Ranker::Sum(subs) | Ranker::Product(subs) => {
            for sub in subs {
                validate_ranker_vector_dims(ctx, sub)?;
            }
        }
        Ranker::Bm25 { .. } => {}
    }
    Ok(())
}

fn validate_query_vector_dims(ctx: &QueryContext<'_>, field: &str, query: &[f64]) -> Result<()> {
    if query.is_empty() {
        return Ok(());
    }
    if let Some(vindex) = ctx.vectors.get(field) {
        let dim = vindex.l0.dimensions as usize;
        if dim > 0 && query.len() != dim {
            bail!(
                "query vector length {} does not match index dimensions {} for field '{field}'",
                query.len(),
                dim
            );
        }
        return Ok(());
    }
    if let Some(cfg) = ctx
        .meta
        .vector_fields
        .iter()
        .find(|f| f.name == field)
    {
        if cfg.dimensions > 0 && query.len() as u32 != cfg.dimensions {
            bail!(
                "query vector length {} does not match index dimensions {} for field '{field}'",
                query.len(),
                cfg.dimensions
            );
        }
    } else if ctx.meta.vector_field == field && ctx.meta.dimensions > 0 {
        let dim = ctx.meta.dimensions as usize;
        if query.len() != dim {
            bail!(
                "query vector length {} does not match namespace dimensions {} for field '{field}'",
                query.len(),
                dim
            );
        }
    }
    Ok(())
}

/// Attribute sort applied after `rank_by` relevance scoring (tie-breaker for v1).
#[derive(Debug, Clone)]
struct OrderBy {
    field: String,
    descending: bool,
}

/// Sort key for attribute ordering (string / number); nulls sort first in asc, last in desc.
#[derive(Debug, Clone, PartialEq)]
enum AttrSortKey {
    Null,
    Number(f64),
    String(String),
}

fn parse_order_by(v: &Value) -> Result<OrderBy> {
    let arr = v
        .as_array()
        .ok_or_else(|| anyhow!("order_by must be a JSON array"))?;
    if arr.len() < 2 {
        bail!("order_by needs [field, asc|desc]");
    }
    let field = arr[0]
        .as_str()
        .ok_or_else(|| anyhow!("order_by[0] must be a string field name"))?
        .to_string();
    let dir = arr[1]
        .as_str()
        .ok_or_else(|| anyhow!("order_by[1] must be asc or desc"))?;
    let descending = match dir.to_ascii_lowercase().as_str() {
        "asc" | "ascending" => false,
        "desc" | "descending" => true,
        other => bail!("unknown order_by direction: {other} (use asc or desc)"),
    };
    Ok(OrderBy { field, descending })
}

fn attr_sort_key(doc: Option<&Document>, field: &str) -> AttrSortKey {
    let Some(doc) = doc else {
        return AttrSortKey::Null;
    };
    let Some(v) = doc.attributes.get(field) else {
        return AttrSortKey::Null;
    };
    match v {
        Value::Null => AttrSortKey::Null,
        Value::String(s) => AttrSortKey::String(s.clone()),
        Value::Number(n) => n
            .as_f64()
            .map(AttrSortKey::Number)
            .unwrap_or(AttrSortKey::Null),
        Value::Bool(b) => AttrSortKey::Number(if *b { 1.0 } else { 0.0 }),
        _ => AttrSortKey::Null,
    }
}

fn compare_attr_keys(a: &AttrSortKey, b: &AttrSortKey, descending: bool) -> std::cmp::Ordering {
    let ord = match (a, b) {
        (AttrSortKey::Null, AttrSortKey::Null) => std::cmp::Ordering::Equal,
        (AttrSortKey::Null, _) => std::cmp::Ordering::Less,
        (_, AttrSortKey::Null) => std::cmp::Ordering::Greater,
        (AttrSortKey::Number(x), AttrSortKey::Number(y)) => {
            x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal)
        }
        (AttrSortKey::String(x), AttrSortKey::String(y)) => x.cmp(y),
        // Mixed types: numbers sort before strings (stable, deterministic).
        (AttrSortKey::Number(_), AttrSortKey::String(_)) => std::cmp::Ordering::Less,
        (AttrSortKey::String(_), AttrSortKey::Number(_)) => std::cmp::Ordering::Greater,
    };
    if descending {
        ord.reverse()
    } else {
        ord
    }
}

fn compare_docs_by_attribute(
    ctx: &QueryContext<'_>,
    id_a: &str,
    id_b: &str,
    ob: &OrderBy,
) -> std::cmp::Ordering {
    let key_a = attr_sort_key(ctx.docs.get(id_a), &ob.field);
    let key_b = attr_sort_key(ctx.docs.get(id_b), &ob.field);
    compare_attr_keys(&key_a, &key_b, ob.descending)
}

fn parse_rank_by(v: &Value) -> Result<Ranker> {
    let arr = v
        .as_array()
        .ok_or_else(|| anyhow!("rank_by must be an array"))?;
    if arr.is_empty() {
        bail!("rank_by array is empty");
    }
    let head = arr[0].as_str().ok_or_else(|| anyhow!("rank_by[0] must be string"))?;
    match head {
        "vector" | "Vector" => {
            if arr.len() < 4 {
                bail!("vector rank_by needs [vector, ANN, field, query]");
            }
            let field = arr[2].as_str().ok_or_else(|| anyhow!("field must be string"))?;
            let query = value_to_f64_vec(&arr[3])?;
            Ok(Ranker::Vector {
                field: field.to_string(),
                query,
            })
        }
        "BM25" | "bm25" => {
            if arr.len() < 3 {
                bail!("BM25 rank_by needs [BM25, field, query]");
            }
            let field = arr[1].as_str().ok_or_else(|| anyhow!("field must be string"))?;
            let query = arr[2]
                .as_str()
                .ok_or_else(|| anyhow!("query must be string"))?;
            Ok(Ranker::Bm25 {
                field: field.to_string(),
                query: query.to_string(),
            })
        }
        "Sum" | "sum" => {
            let subs = arr[1..]
                .iter()
                .map(parse_rank_by)
                .collect::<Result<Vec<_>>>()?;
            Ok(Ranker::Sum(subs))
        }
        "Product" | "product" => {
            let subs = arr[1..]
                .iter()
                .map(parse_rank_by)
                .collect::<Result<Vec<_>>>()?;
            Ok(Ranker::Product(subs))
        }
        other => bail!("unknown rank_by operator: {other}"),
    }
}

/// Vector fields and query vectors referenced by `rank_by` (for cold probed index fetch).
pub fn vector_probe_specs(rank_by: &Value) -> Result<Vec<(String, Vec<f64>)>> {
    let ranker = parse_rank_by(rank_by)?;
    let mut out = Vec::new();
    collect_vector_probe_specs(&ranker, &mut out);
    Ok(out)
}

fn collect_vector_probe_specs(ranker: &Ranker, out: &mut Vec<(String, Vec<f64>)>) {
    match ranker {
        Ranker::Vector { field, query } => out.push((field.clone(), query.clone())),
        Ranker::Sum(subs) | Ranker::Product(subs) => {
            for sub in subs {
                collect_vector_probe_specs(sub, out);
            }
        }
        Ranker::Bm25 { .. } => {}
    }
}

/// Cosine similarity (higher is better). Re-exported for tests and legacy callers.
pub fn cosine_similarity(a: &[f64], b: &[f64]) -> f64 {
    crate::index::vector::cosine_similarity(a, b)
}

/// Legacy per-doc BM25 when no FTS index is available (full scan fallback).
pub fn bm25_score_legacy(document: &str, query: &str) -> f64 {
    bm25_doc_score(document, query, document.split_whitespace().count().max(1) as f64, 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::filter::FilterSegment;
    use crate::index::fts::FtsSegment;
    use crate::index::vector::VectorIndex;
    use crate::meta::{DistanceMetric, NamespaceMeta};
    use crate::models::{Document, QueryRequest};
    use serde_json::json;

    /// Build a QueryContext with defaults for cold-query and ANN optional fields.
    /// Tests that need non-default values can override with struct update syntax:
    /// `QueryContext { fts: Some(&seg), ..test_ctx(&docs, &meta, &tail, &vecs) }`.
    fn test_ctx<'a>(
        docs: &'a HashMap<String, Document>,
        meta: &'a NamespaceMeta,
        tail_doc_ids: &'a HashSet<String>,
        vectors: &'a HashMap<String, VectorIndex>,
    ) -> QueryContext<'a> {
        QueryContext {
            docs,
            meta,
            fts: None,
            vectors,
            filter_index: None,
            tail_doc_ids,
            consistency: QueryConsistency::Strong,
            storage_roundtrips: None,
            cold_s3_keys_fetched: None,
            ann_probed_clusters: None,
            ann_rerank: None,
        }
    }

    fn doc(id: &str, text: &str, emb: Vec<f64>) -> Document {
        Document {
            id: id.into(),
            attributes: [
                ("text".into(), json!(text)),
                ("embedding".into(), json!(emb)),
            ]
            .into(),
        }
    }

    #[test]
    fn cosine_identical() {
        let v = vec![1.0, 0.0, 0.0];
        assert!((cosine_similarity(&v, &v) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn bm25_indexed_query_returns_top_doc() {
        let mut map: HashMap<String, Document> = HashMap::new();
        map.insert(
            "a".into(),
            Document {
                id: "a".into(),
                attributes: [("text".into(), json!("rust fast programming"))].into(),
            },
        );
        map.insert(
            "b".into(),
            Document {
                id: "b".into(),
                attributes: [("text".into(), json!("python slow scripting"))].into(),
            },
        );
        let pairs: Vec<(String, Document)> = map
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let seg = FtsSegment::build(1, "text", &pairs);
        let meta = NamespaceMeta {
            index_cursor: 1,
            wal_commit_seq: 1,
            fts_segment_id: 1,
            ..Default::default()
        };
        let tail = HashSet::new();
        let vecs = HashMap::new();
        let ctx = QueryContext {
            fts: Some(&seg),
            ..test_ctx(&map, &meta, &tail, &vecs)
        };
        let req = QueryRequest {
            rank_by: json!(["BM25", "text", "rust programming"]),
            top_k: Some(1),
            filters: None,
            include_attributes: None,
            consistency: None,
            order_by: None,
            include_vectors: None,
            vector_encoding: None,
        };
        let resp = execute_query(&ctx, &req).unwrap();
        assert_eq!(resp.rows[0].id, "a");
        assert!(resp.rows[0].dist.unwrap() > 0.0);
    }

    #[test]
    fn tail_doc_uses_exhaustive_not_stale_index() {
        let mut map = HashMap::new();
        map.insert(
            "a".into(),
            Document {
                id: "a".into(),
                attributes: [("text".into(), json!("old content"))].into(),
            },
        );
        let indexed = vec![(
            "a".into(),
            Document {
                id: "a".into(),
                attributes: [("text".into(), json!("rust fast programming"))].into(),
            },
        )];
        let seg = FtsSegment::build(1, "text", &indexed);
        let meta = NamespaceMeta {
            index_cursor: 1,
            wal_commit_seq: 2,
            fts_segment_id: 1,
            ..Default::default()
        };
        let mut tail = HashSet::new();
        tail.insert("a".into());
        let vecs = HashMap::new();
        let ctx = QueryContext {
            fts: Some(&seg),
            ..test_ctx(&map, &meta, &tail, &vecs)
        };
        let req = QueryRequest {
            rank_by: json!(["BM25", "text", "rust"]),
            top_k: Some(5),
            filters: None,
            include_attributes: None,
            consistency: None,
            order_by: None,
            include_vectors: None,
            vector_encoding: None,
        };
        let resp = execute_query(&ctx, &req).unwrap();
        assert!(resp.rows.is_empty() || resp.rows[0].dist.unwrap_or(0.0) == 0.0);
    }

    #[test]
    fn hybrid_sum_filter_includes_strong_tail_doc_during_index_lag() {
        let mut map: HashMap<String, Document> = HashMap::new();
        map.insert(
            "indexed-pro".into(),
            Document {
                id: "indexed-pro".into(),
                attributes: [
                    ("embedding".into(), json!([1.0, 0.0, 0.0])),
                    ("text".into(), json!("indexed baseline alpha")),
                    ("tier".into(), json!("pro")),
                ]
                .into(),
            },
        );
        map.insert(
            "indexed-free".into(),
            Document {
                id: "indexed-free".into(),
                attributes: [
                    ("embedding".into(), json!([0.0, 1.0, 0.0])),
                    ("text".into(), json!("indexed baseline bravo")),
                    ("tier".into(), json!("free")),
                ]
                .into(),
            },
        );
        map.insert(
            "tail-pro-hybrid".into(),
            Document {
                id: "tail-pro-hybrid".into(),
                attributes: [
                    ("embedding".into(), json!([0.99, 0.01, 0.0])),
                    ("text".into(), json!("tail alpha stressterm unindexed")),
                    ("tier".into(), json!("pro")),
                ]
                .into(),
            },
        );
        let indexed_pairs: Vec<(String, Document)> = map
            .iter()
            .filter(|(id, _)| *id != "tail-pro-hybrid")
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let fts = FtsSegment::build(1, "text", &indexed_pairs);
        let vindex = VectorIndex::build(
            1,
            "embedding",
            DistanceMetric::CosineDistance,
            &indexed_pairs,
            &json!({}),
            crate::config::AnnBuildConfig::default(),
        )
        .unwrap()
        .expect("vector index");
        let filter_seg = FilterSegment::build(
            1,
            &json!({ "tier": { "type": "string" } }),
            &indexed_pairs,
        );
        let mut tail = HashSet::new();
        tail.insert("tail-pro-hybrid".into());
        let meta = NamespaceMeta {
            index_cursor: 1,
            wal_commit_seq: 2,
            fts_segment_id: 1,
            vector_segment_id: 1,
            filter_segment_id: 1,
            dimensions: 3,
            ..Default::default()
        };
        let vectors = HashMap::from([("embedding".to_string(), vindex)]);
        let ctx = QueryContext {
            fts: Some(&fts),
            vectors: &vectors,
            filter_index: Some(&filter_seg),
            ..test_ctx(&map, &meta, &tail, &vectors)
        };
        let req = QueryRequest {
            rank_by: json!([
                "Sum",
                ["vector", "ANN", "embedding", [1.0, 0.0, 0.0]],
                ["BM25", "text", "alpha"]
            ]),
            top_k: Some(5),
            filters: Some(json!(["tier", "Eq", "pro"])),
            include_attributes: None,
            consistency: None,
            order_by: None,
            include_vectors: None,
            vector_encoding: None,
        };
        let resp = execute_query(&ctx, &req).unwrap();
        let ids: Vec<_> = resp.rows.iter().map(|r| r.id.as_str()).collect();
        assert!(
            ids.contains(&"tail-pro-hybrid"),
            "hybrid Sum + filter must include strong tail doc, got {ids:?}"
        );
        assert!(
            !ids.contains(&"indexed-free"),
            "filter must exclude free-tier doc, got {ids:?}"
        );
    }

    #[test]
    fn hybrid_sum_beats_single_signal_wrong_doc() {
        // Doc A: great BM25 for "rust", vector far from query.
        // Doc B: weak BM25, vector identical to query.
        // Pure BM25 → A; pure vector → B; hybrid Sum should prefer B when both signals matter.
        let mut map: HashMap<String, Document> = HashMap::new();
        let query_vec = vec![1.0, 0.0, 0.0, 0.0];
        map.insert(
            "lexical-winner".into(),
            doc(
                "lexical-winner",
                "rust rust rust programming systems kernel",
                vec![0.0, 1.0, 0.0, 0.0],
            ),
        );
        // Weak BM25 (one token) but near-perfect vector → hybrid Sum beats BM25-only pick.
        map.insert(
            "vector-winner".into(),
            doc("vector-winner", "rust", query_vec.clone()),
        );
        map.insert(
            "decoy".into(),
            doc("decoy", "python java kotlin", vec![0.0, 0.0, 1.0, 0.0]),
        );

        let pairs: Vec<(String, Document)> = map
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let fts = FtsSegment::build(1, "text", &pairs);
        let vindex = VectorIndex::build(
            1,
            "embedding",
            DistanceMetric::CosineDistance,
            &pairs,
            &json!({}),
            crate::config::AnnBuildConfig::default(),
        )
        .unwrap()
        .expect("vector index");

        let bm25_only = QueryRequest {
            rank_by: json!(["BM25", "text", "rust programming systems"]),
            top_k: Some(1),
            filters: None,
            include_attributes: None,
            consistency: None,
            order_by: None,
            include_vectors: None,
            vector_encoding: None,
        };
        let vector_only = QueryRequest {
            rank_by: json!(["vector", "ANN", "embedding", query_vec.clone()]),
            top_k: Some(1),
            filters: None,
            include_attributes: None,
            consistency: None,
            order_by: None,
            include_vectors: None,
            vector_encoding: None,
        };
        let hybrid = QueryRequest {
            rank_by: json!([
                "Sum",
                ["BM25", "text", "rust programming systems"],
                ["vector", "ANN", "embedding", query_vec]
            ]),
            top_k: Some(1),
            filters: None,
            include_attributes: None,
            consistency: None,
            order_by: None,
            include_vectors: None,
            vector_encoding: None,
        };

        let meta = NamespaceMeta {
            index_cursor: 1,
            wal_commit_seq: 1,
            fts_segment_id: 1,
            vector_segment_id: 1,
            dimensions: 4,
            ..Default::default()
        };
        let tail = HashSet::new();
        let vectors = HashMap::from([("embedding".to_string(), vindex)]);
        let ctx = QueryContext {
            fts: Some(&fts),
            vectors: &vectors,
            ..test_ctx(&map, &meta, &tail, &vectors)
        };

        let bm25_resp = execute_query(&ctx, &bm25_only).unwrap();
        assert_eq!(bm25_resp.rows[0].id, "lexical-winner");

        let vec_resp = execute_query(&ctx, &vector_only).unwrap();
        assert_eq!(vec_resp.rows[0].id, "vector-winner");

        let hybrid_resp = execute_query(&ctx, &hybrid).unwrap();
        assert_eq!(hybrid_resp.rows[0].id, "vector-winner");
    }

    #[test]
    fn eventual_consistency_skips_tail_scan() {
        let mut map = HashMap::new();
        map.insert(
            "a".into(),
            Document {
                id: "a".into(),
                attributes: [("text".into(), json!("brand new unindexed rust"))].into(),
            },
        );
        let indexed = vec![(
            "b".into(),
            Document {
                id: "b".into(),
                attributes: [("text".into(), json!("python only"))].into(),
            },
        )];
        let seg = FtsSegment::build(1, "text", &indexed);
        let meta = NamespaceMeta {
            index_cursor: 1,
            wal_commit_seq: 2,
            fts_segment_id: 1,
            ..Default::default()
        };
        let mut tail = HashSet::new();
        tail.insert("a".into());
        let vecs = HashMap::new();
        let ctx = QueryContext {
            fts: Some(&seg),
            consistency: QueryConsistency::Eventual,
            ..test_ctx(&map, &meta, &tail, &vecs)
        };
        let req = QueryRequest {
            rank_by: json!(["BM25", "text", "rust"]),
            top_k: Some(5),
            filters: None,
            include_attributes: None,
            consistency: Some("eventual".into()),
            order_by: None,
            include_vectors: None,
            vector_encoding: None,
        };
        let resp = execute_query(&ctx, &req).unwrap();
        assert!(resp.rows.is_empty() || resp.rows.iter().all(|r| r.id != "a"));
    }

    #[test]
    fn eventual_cold_vector_returns_indexed_without_docs_map() {
        let indexed = vec![
            (
                "indexed-only".into(),
                Document {
                    id: "indexed-only".into(),
                    attributes: [("embedding".into(), json!([1.0, 0.0, 0.0]))].into(),
                },
            ),
            (
                "indexed-far".into(),
                Document {
                    id: "indexed-far".into(),
                    attributes: [("embedding".into(), json!([0.0, 1.0, 0.0]))].into(),
                },
            ),
        ];
        let vindex = VectorIndex::build(
            1,
            "embedding",
            DistanceMetric::CosineDistance,
            &indexed,
            &json!({}),
            crate::config::AnnBuildConfig::default(),
        )
        .unwrap()
        .expect("vector index");
        let meta = NamespaceMeta {
            index_cursor: 1,
            wal_commit_seq: 2,
            vector_segment_id: 1,
            dimensions: 3,
            ..Default::default()
        };
        let empty_docs = HashMap::new();
        let tail = HashSet::new();
        let vectors = HashMap::from([("embedding".to_string(), vindex)]);
        let ctx = QueryContext {
            vectors: &vectors,
            consistency: QueryConsistency::Eventual,
            ..test_ctx(&empty_docs, &meta, &tail, &vectors)
        };
        let req = QueryRequest {
            rank_by: json!(["vector", "ANN", "embedding", [1.0, 0.0, 0.0]]),
            top_k: Some(1),
            filters: None,
            include_attributes: None,
            consistency: Some("eventual".into()),
            order_by: None,
            include_vectors: None,
            vector_encoding: None,
        };
        let resp = execute_query(&ctx, &req).unwrap();
        assert_eq!(resp.rows[0].id, "indexed-only");
    }

    #[test]
    fn include_vectors_returns_base64_encoding() {
        let meta = NamespaceMeta {
            schema: json!({ "embedding": "[2]f32" }),
            ..Default::default()
        };
        let doc = Document {
            id: "v".into(),
            attributes: [
                ("text".into(), json!("x")),
                ("embedding".into(), json!([1.0, 0.0])),
            ]
            .into_iter()
            .collect(),
        };
        let mut docs = HashMap::new();
        docs.insert("v".into(), doc);
        let tail = HashSet::new();
        let vecs = HashMap::new();
        let ctx = test_ctx(&docs, &meta, &tail, &vecs);
        let req = QueryRequest {
            rank_by: json!(["BM25", "text", "x"]),
            top_k: Some(1),
            filters: None,
            include_attributes: Some(json!(["text"])),
            consistency: None,
            order_by: None,
            include_vectors: Some(json!(true)),
            vector_encoding: Some("base64".into()),
        };
        let resp = execute_query(&ctx, &req).unwrap();
        let emb = resp.rows[0]
            .attributes
            .as_ref()
            .unwrap()
            .get("embedding")
            .unwrap();
        assert!(emb.as_str().is_some());
        assert!(resp.rows[0].attributes.as_ref().unwrap().contains_key("text"));
    }

    #[test]
    fn include_attributes_false_omits_attrs() {
        let mut docs = HashMap::new();
        docs.insert(
            "a".into(),
            Document {
                id: "a".into(),
                attributes: [("text".into(), Value::String("hi".into()))]
                    .into_iter()
                    .collect(),
            },
        );
        let meta = NamespaceMeta::default();
        let tail = HashSet::new();
        let vecs = HashMap::new();
        let ctx = test_ctx(&docs, &meta, &tail, &vecs);
        let req = QueryRequest {
            rank_by: Value::Array(vec![
                Value::String("BM25".into()),
                Value::String("text".into()),
                Value::String("hi".into()),
            ]),
            top_k: Some(1),
            filters: None,
            include_attributes: Some(Value::Bool(false)),
            consistency: None,
            order_by: None,
            include_vectors: None,
            vector_encoding: None,
        };
        let resp = execute_query(&ctx, &req).unwrap();
        assert_eq!(resp.rows.len(), 1);
        assert!(resp.rows[0].attributes.is_none());
    }

    #[test]
    fn filter_eq_with_vector_query_returns_subset() {
        let mut map: HashMap<String, Document> = HashMap::new();
        map.insert(
            "pro-close".into(),
            Document {
                id: "pro-close".into(),
                attributes: [
                    ("text".into(), json!("text")),
                    ("embedding".into(), json!([0.99, 0.01, 0.0, 0.0])),
                    ("tier".into(), json!("pro")),
                ]
                .into(),
            },
        );
        map.insert(
            "pro-exact".into(),
            Document {
                id: "pro-exact".into(),
                attributes: [
                    ("text".into(), json!("text")),
                    ("embedding".into(), json!([1.0, 0.0, 0.0, 0.0])),
                    ("tier".into(), json!("pro")),
                ]
                .into(),
            },
        );
        map.insert(
            "free-exact".into(),
            Document {
                id: "free-exact".into(),
                attributes: [
                    ("text".into(), json!("text")),
                    ("embedding".into(), json!([1.0, 0.0, 0.0, 0.0])),
                    ("tier".into(), json!("free")),
                ]
                .into(),
            },
        );

        let pairs: Vec<(String, Document)> = map
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let vindex = VectorIndex::build(
            1,
            "embedding",
            DistanceMetric::CosineDistance,
            &pairs,
            &json!({}),
            crate::config::AnnBuildConfig::default(),
        )
        .unwrap()
        .expect("vector index");
        let filter_seg = FilterSegment::build(1, &json!({}), &pairs);

        let meta = NamespaceMeta {
            index_cursor: 1,
            wal_commit_seq: 1,
            vector_segment_id: 1,
            filter_segment_id: 1,
            dimensions: 4,
            ..Default::default()
        };
        let tail = HashSet::new();
        let vectors = HashMap::from([("embedding".to_string(), vindex)]);
        let ctx = QueryContext {
            vectors: &vectors,
            filter_index: Some(&filter_seg),
            ..test_ctx(&map, &meta, &tail, &vectors)
        };
        let req = QueryRequest {
            rank_by: json!(["vector", "ANN", "embedding", [1.0, 0.0, 0.0, 0.0]]),
            top_k: Some(5),
            filters: Some(json!(["tier", "Eq", "pro"])),
            include_attributes: None,
            consistency: None,
            order_by: None,
            include_vectors: None,
            vector_encoding: None,
        };
        let resp = execute_query(&ctx, &req).unwrap();
        assert!(!resp.rows.is_empty());
        assert!(resp.rows.iter().all(|r| r.id == "pro-close" || r.id == "pro-exact"));
        assert!(!resp.rows.iter().any(|r| r.id == "free-exact"));
    }

    #[test]
    fn query_performance_includes_storage_roundtrips() {
        let docs = HashMap::new();
        let meta = NamespaceMeta::default();
        let tail = HashSet::new();
        let vecs = HashMap::new();
        let ctx = QueryContext {
            storage_roundtrips: Some(4),
            cold_s3_keys_fetched: Some(42),
            ann_probed_clusters: Some(8),
            ..test_ctx(&docs, &meta, &tail, &vecs)
        };
        let req = QueryRequest {
            rank_by: json!(["BM25", "text", "x"]),
            top_k: Some(1),
            filters: None,
            include_attributes: None,
            consistency: None,
            order_by: None,
            include_vectors: None,
            vector_encoding: None,
        };
        let resp = execute_query(&ctx, &req).unwrap();
        let perf = resp.performance.as_ref().unwrap();
        assert_eq!(perf.storage_roundtrips, Some(4));
        assert_eq!(perf.cold_s3_keys_fetched, Some(42));
        assert_eq!(perf.ann_probed_clusters, Some(8));
    }

    #[test]
    fn query_performance_billing_estimates_queried_and_returned() {
        let doc = Document {
            id: "bill-a".into(),
            attributes: [
                ("text".into(), json!("billing estimate smoke")),
                ("embedding".into(), json!([1.0, 0.0, 0.0, 0.0])),
            ]
            .into(),
        };
        let map = HashMap::from([(doc.id.clone(), doc.clone())]);
        let pairs = vec![(doc.id.clone(), doc)];
        let vindex = VectorIndex::build(
            1,
            "embedding",
            DistanceMetric::CosineDistance,
            &pairs,
            &json!({}),
            crate::config::AnnBuildConfig::default(),
        )
        .unwrap()
        .expect("vector index");
        let meta = NamespaceMeta {
            index_cursor: 1,
            wal_commit_seq: 1,
            vector_segment_id: 1,
            dimensions: 4,
            ..Default::default()
        };
        let tail = HashSet::new();
        let vectors = HashMap::from([("embedding".to_string(), vindex)]);
        let ctx = QueryContext {
            vectors: &vectors,
            ..test_ctx(&map, &meta, &tail, &vectors)
        };
        let req = QueryRequest {
            rank_by: json!(["vector", "ANN", "embedding", [1.0, 0.0, 0.0, 0.0]]),
            top_k: Some(1),
            filters: None,
            include_attributes: Some(json!(true)),
            consistency: None,
            order_by: None,
            include_vectors: None,
            vector_encoding: None,
        };
        let resp = execute_query(&ctx, &req).unwrap();
        let perf = resp.performance.expect("performance");
        assert!(perf.billing.billable_logical_bytes_queried > 0);
        assert!(perf.billing.billable_logical_bytes_returned > 0);
        assert!(
            perf.billing.billable_logical_bytes_queried
                >= perf.billing.billable_logical_bytes_returned
        );
    }

    #[test]
    fn ann_rerank_increases_candidates_ratio_vs_probe_only() {
        const N: usize = 5_000;
        const DIM: usize = 128;
        let mut map: HashMap<String, Document> = HashMap::new();
        let mut pairs = Vec::with_capacity(N);
        for i in 0..N {
            let id = format!("doc-{i}");
            let embedding: Vec<f64> = (0..DIM)
                .map(|d| ((i * DIM + d) as f64 * 0.001).sin())
                .collect();
            let doc = Document {
                id: id.clone(),
                attributes: [("embedding".into(), json!(embedding.clone()))].into(),
            };
            pairs.push((id, doc.clone()));
            map.insert(doc.id.clone(), doc);
        }
        let vindex = VectorIndex::build(
            1,
            "embedding",
            DistanceMetric::CosineDistance,
            &pairs,
            &json!({}),
            crate::config::AnnBuildConfig::default(),
        )
        .unwrap()
        .expect("vector index");
        let meta = NamespaceMeta {
            index_cursor: 1,
            wal_commit_seq: 1,
            vector_segment_id: 1,
            dimensions: DIM as u32,
            ..Default::default()
        };
        let tail = HashSet::new();
        let query_vec: Vec<f64> = (0..DIM).map(|d| (d as f64 * 0.01).cos()).collect();
        let req = QueryRequest {
            rank_by: json!(["vector", "ANN", "embedding", query_vec]),
            top_k: Some(10),
            filters: None,
            include_attributes: None,
            consistency: None,
            order_by: None,
            include_vectors: None,
            vector_encoding: None,
        };
        let vectors = HashMap::from([("embedding".to_string(), vindex.clone())]);
        let ctx_probe = QueryContext {
            vectors: &vectors,
            ann_rerank: Some(false),
            ..test_ctx(&map, &meta, &tail, &vectors)
        };
        let ctx_rerank = QueryContext {
            ann_rerank: Some(true),
            ..ctx_probe
        };
        let probe_ratio = execute_query(&ctx_probe, &req)
            .unwrap()
            .performance
            .expect("perf")
            .candidates_ratio;
        let rerank_ratio = execute_query(&ctx_rerank, &req)
            .unwrap()
            .performance
            .expect("perf")
            .candidates_ratio;
        assert!(
            rerank_ratio > probe_ratio,
            "re-rank candidates_ratio {rerank_ratio} should exceed probe-only {probe_ratio}"
        );
    }

    #[test]
    fn indexed_vector_candidates_ratio_under_ten_percent() {
        const N: usize = 5_000;
        const DIM: usize = 128;
        let mut map: HashMap<String, Document> = HashMap::new();
        let mut pairs = Vec::with_capacity(N);
        for i in 0..N {
            let id = format!("doc-{i}");
            let embedding: Vec<f64> = (0..DIM)
                .map(|d| ((i * DIM + d) as f64 * 0.001).sin())
                .collect();
            let doc = Document {
                id: id.clone(),
                attributes: [
                    ("text".into(), json!(format!("document {i}"))),
                    ("embedding".into(), json!(embedding.clone())),
                ]
                .into(),
            };
            pairs.push((id, doc.clone()));
            map.insert(doc.id.clone(), doc);
        }
        let vindex = VectorIndex::build(
            1,
            "embedding",
            DistanceMetric::CosineDistance,
            &pairs,
            &json!({}),
            crate::config::AnnBuildConfig::default(),
        )
        .unwrap()
        .expect("vector index");
        let meta = NamespaceMeta {
            index_cursor: 1,
            wal_commit_seq: 1,
            vector_segment_id: 1,
            dimensions: DIM as u32,
            ..Default::default()
        };
        let tail = HashSet::new();
        let vectors = HashMap::from([("embedding".to_string(), vindex)]);
        let ctx = QueryContext {
            vectors: &vectors,
            ..test_ctx(&map, &meta, &tail, &vectors)
        };
        let query_vec: Vec<f64> = (0..DIM).map(|d| (d as f64 * 0.01).cos()).collect();
        let req = QueryRequest {
            rank_by: json!(["vector", "ANN", "embedding", query_vec]),
            top_k: Some(10),
            filters: None,
            include_attributes: None,
            consistency: None,
            order_by: None,
            include_vectors: None,
            vector_encoding: None,
        };
        let resp = execute_query(&ctx, &req).unwrap();
        let perf = resp.performance.expect("performance stats");
        assert_eq!(perf.approx_namespace_size, N as u64);
        assert!(
            perf.candidates_ratio < 0.12,
            "indexed ANN must not scan whole namespace: candidates={} ratio={}",
            perf.candidates,
            perf.candidates_ratio
        );
        assert!(perf.candidates < perf.approx_namespace_size);
        assert_eq!(perf.exhaustive_search_count, 0);
    }

    #[test]
    fn empty_namespace_query_returns_empty_rows() {
        let docs = HashMap::new();
        let meta = NamespaceMeta::default();
        let tail = HashSet::new();
        let vecs = HashMap::new();
        let ctx = test_ctx(&docs, &meta, &tail, &vecs);
        let req = QueryRequest {
            rank_by: json!(["BM25", "text", "anything"]),
            top_k: Some(10),
            filters: None,
            include_attributes: None,
            consistency: None,
            order_by: None,
            include_vectors: None,
            vector_encoding: None,
        };
        let resp = execute_query(&ctx, &req).unwrap();
        assert!(resp.rows.is_empty());
        let perf = resp.performance.unwrap();
        assert_eq!(perf.approx_namespace_size, 0);
        assert_eq!(perf.candidates, 0);
        assert_eq!(perf.billing.billable_logical_bytes_queried, 0);
        assert_eq!(perf.billing.billable_logical_bytes_returned, 0);
    }

    #[test]
    fn top_k_zero_and_huge_are_rejected() {
        let docs = HashMap::new();
        let meta = NamespaceMeta::default();
        let tail = HashSet::new();
        let vecs = HashMap::new();
        let ctx = test_ctx(&docs, &meta, &tail, &vecs);
        for top_k in [Some(0), Some((MAX_TOP_K as u32) + 1)] {
            let req = QueryRequest {
                rank_by: json!(["BM25", "text", "x"]),
                top_k,
                filters: None,
                include_attributes: None,
                consistency: None,
                order_by: None,
                include_vectors: None,
                vector_encoding: None,
            };
            assert!(execute_query(&ctx, &req).is_err());
        }
    }

    #[test]
    fn vector_probe_specs_collects_hybrid_fields() {
        let rank_by = json!([
            "Sum",
            ["BM25", "text", "rust"],
            ["vector", "ANN", "embedding", [1.0, 0.0, 0.0, 0.0]]
        ]);
        let specs = super::vector_probe_specs(&rank_by).unwrap();
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].0, "embedding");
        assert_eq!(specs[0].1.len(), 4);
    }

    #[test]
    fn vector_probe_specs_collects_two_vector_fields_in_sum() {
        let rank_by = json!([
            "Sum",
            ["vector", "ANN", "embedding_a", [1.0, 0.0, 0.0, 0.0]],
            ["vector", "ANN", "embedding_b", [0.0, 1.0, 0.0, 0.0]]
        ]);
        let specs = super::vector_probe_specs(&rank_by).unwrap();
        assert_eq!(specs.len(), 2);
        assert_eq!(specs[0].0, "embedding_a");
        assert_eq!(specs[1].0, "embedding_b");
    }

    #[test]
    fn vector_probe_specs_collects_hybrid_vector_in_product() {
        let rank_by = json!([
            "Product",
            ["BM25", "text", "rust"],
            ["vector", "ANN", "embedding", [1.0, 0.0, 0.0, 0.0]]
        ]);
        let specs = super::vector_probe_specs(&rank_by).unwrap();
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].0, "embedding");
        assert_eq!(specs[0].1.len(), 4);
    }

    #[test]
    fn vector_probe_specs_collects_two_vector_fields_in_product() {
        let rank_by = json!([
            "Product",
            ["vector", "ANN", "embedding_a", [1.0, 0.0, 0.0, 0.0]],
            ["vector", "ANN", "embedding_b", [0.0, 1.0, 0.0, 0.0]]
        ]);
        let specs = super::vector_probe_specs(&rank_by).unwrap();
        assert_eq!(specs.len(), 2);
        assert_eq!(specs[0].0, "embedding_a");
        assert_eq!(specs[1].0, "embedding_b");
    }

    #[test]
    fn malformed_rank_by_rejected() {
        let docs = HashMap::new();
        let meta = NamespaceMeta::default();
        let tail = HashSet::new();
        let vecs = HashMap::new();
        let ctx = test_ctx(&docs, &meta, &tail, &vecs);
        let cases = [
            json!([]),
            json!(["bogus", "x"]),
            json!(["vector", "ANN", "embedding"]),
            json!(["BM25", "text"]),
        ];
        for rank_by in cases {
            let req = QueryRequest {
                rank_by,
                top_k: Some(1),
                filters: None,
                include_attributes: None,
                consistency: None,
                order_by: None,
                include_vectors: None,
                vector_encoding: None,
            };
            assert!(execute_query(&ctx, &req).is_err(), "expected error for {req:?}");
        }
    }

    #[test]
    fn vector_dimension_mismatch_rejected() {
        let mut map: HashMap<String, Document> = HashMap::new();
        map.insert(
            "a".into(),
            Document {
                id: "a".into(),
                attributes: [
                    ("text".into(), json!("x")),
                    ("embedding".into(), json!([1.0, 0.0, 0.0, 0.0])),
                ]
                .into(),
            },
        );
        let pairs: Vec<(String, Document)> = map
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let vindex = VectorIndex::build(
            1,
            "embedding",
            DistanceMetric::CosineDistance,
            &pairs,
            &json!({}),
            crate::config::AnnBuildConfig::default(),
        )
        .unwrap()
        .expect("vector index");
        let meta = NamespaceMeta {
            index_cursor: 1,
            wal_commit_seq: 1,
            vector_segment_id: 1,
            dimensions: 4,
            ..Default::default()
        };
        let tail = HashSet::new();
        let vectors = HashMap::from([("embedding".to_string(), vindex)]);
        let ctx = QueryContext {
            vectors: &vectors,
            ..test_ctx(&map, &meta, &tail, &vectors)
        };
        let req = QueryRequest {
            rank_by: json!(["vector", "ANN", "embedding", [1.0, 0.0]]),
            top_k: Some(5),
            filters: None,
            include_attributes: None,
            consistency: None,
            order_by: None,
            include_vectors: None,
            vector_encoding: None,
        };
        let err = execute_query(&ctx, &req).unwrap_err().to_string();
        assert!(err.contains("does not match"), "{err}");
    }

    #[test]
    fn filter_vector_empty_intersection_returns_empty_not_error() {
        let mut map: HashMap<String, Document> = HashMap::new();
        map.insert(
            "match-filter".into(),
            Document {
                id: "match-filter".into(),
                attributes: [
                    ("embedding".into(), json!([0.0, 1.0, 0.0, 0.0])),
                    ("tier".into(), json!("free")),
                ]
                .into(),
            },
        );
        let pairs: Vec<(String, Document)> = map
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let vindex = VectorIndex::build(
            1,
            "embedding",
            DistanceMetric::CosineDistance,
            &pairs,
            &json!({}),
            crate::config::AnnBuildConfig::default(),
        )
        .unwrap()
        .expect("vector index");
        let filter_seg = FilterSegment::build(1, &json!({}), &pairs);
        let meta = NamespaceMeta {
            index_cursor: 1,
            wal_commit_seq: 1,
            vector_segment_id: 1,
            filter_segment_id: 1,
            dimensions: 4,
            ..Default::default()
        };
        let tail = HashSet::new();
        let vectors = HashMap::from([("embedding".to_string(), vindex)]);
        let ctx = QueryContext {
            vectors: &vectors,
            filter_index: Some(&filter_seg),
            ..test_ctx(&map, &meta, &tail, &vectors)
        };
        let req = QueryRequest {
            rank_by: json!(["vector", "ANN", "embedding", [1.0, 0.0, 0.0, 0.0]]),
            top_k: Some(5),
            filters: Some(json!(["tier", "Eq", "pro"])),
            include_attributes: None,
            consistency: None,
            order_by: None,
            include_vectors: None,
            vector_encoding: None,
        };
        let resp = execute_query(&ctx, &req).unwrap();
        assert!(resp.rows.is_empty());
        assert_eq!(resp.performance.unwrap().candidates, 0);
    }

    #[test]
    fn matching_doc_ids_for_filter_exhaustive_without_index() {
        let mut map: HashMap<String, Document> = HashMap::new();
        map.insert(
            "keep".into(),
            Document {
                id: "keep".into(),
                attributes: [("tier".into(), json!("pro"))].into(),
            },
        );
        map.insert(
            "drop".into(),
            Document {
                id: "drop".into(),
                attributes: [("tier".into(), json!("free"))].into(),
            },
        );
        let meta = NamespaceMeta::default();
        let tail = HashSet::new();
        let vecs = HashMap::new();
        let ctx = test_ctx(&map, &meta, &tail, &vecs);
        let expr = parse_filter(&json!(["tier", "Eq", "free"])).unwrap();
        let ids = matching_doc_ids_for_filter(&ctx, &expr).unwrap();
        assert_eq!(ids.len(), 1);
        assert!(ids.contains("drop"));
    }

    #[test]
    fn order_by_priority_desc_breaks_score_ties() {
        let tie_text = "tie score match token";
        let mut map: HashMap<String, Document> = HashMap::new();
        for (id, priority) in [("tie-a", 1), ("tie-b", 3), ("tie-c", 2)] {
            map.insert(
                id.into(),
                Document {
                    id: id.into(),
                    attributes: [
                        ("text".into(), json!(tie_text)),
                        ("priority".into(), json!(priority)),
                    ]
                    .into(),
                },
            );
        }
        let pairs: Vec<(String, Document)> = map
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let seg = FtsSegment::build(1, "text", &pairs);
        let meta = NamespaceMeta {
            index_cursor: 1,
            wal_commit_seq: 1,
            fts_segment_id: 1,
            ..Default::default()
        };
        let tail = HashSet::new();
        let vecs = HashMap::new();
        let ctx = QueryContext {
            fts: Some(&seg),
            ..test_ctx(&map, &meta, &tail, &vecs)
        };
        let req = QueryRequest {
            rank_by: json!(["BM25", "text", "tie score"]),
            top_k: Some(3),
            filters: None,
            include_attributes: None,
            consistency: None,
            order_by: Some(json!(["priority", "desc"])),
            include_vectors: None,
            vector_encoding: None,
        };
        let resp = execute_query(&ctx, &req).unwrap();
        let ids: Vec<_> = resp.rows.iter().map(|r| r.id.as_str()).collect();
        assert_eq!(ids, vec!["tie-b", "tie-c", "tie-a"]);
        let scores: Vec<f64> = resp.rows.iter().map(|r| r.dist.unwrap()).collect();
        assert!(
            (scores[0] - scores[1]).abs() < 1e-9 && (scores[1] - scores[2]).abs() < 1e-9,
            "expected tied BM25 scores, got {scores:?}"
        );
    }

    #[test]
    fn order_by_negative_numbers_sorted_correctly() {
        let tie_text = "tie score match token";
        let mut map: HashMap<String, Document> = HashMap::new();
        for (id, value) in [("neg10", -10), ("neg1", -1), ("pos5", 5), ("zero", 0)] {
            map.insert(
                id.into(),
                Document {
                    id: id.into(),
                    attributes: [
                        ("text".into(), json!(tie_text)),
                        ("val".into(), json!(value)),
                    ]
                    .into(),
                },
            );
        }
        let pairs: Vec<(String, Document)> = map
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let seg = FtsSegment::build(1, "text", &pairs);
        let meta = NamespaceMeta {
            index_cursor: 1,
            wal_commit_seq: 1,
            fts_segment_id: 1,
            ..Default::default()
        };
        let tail = HashSet::new();
        let vecs = HashMap::new();
        let ctx = QueryContext {
            fts: Some(&seg),
            ..test_ctx(&map, &meta, &tail, &vecs)
        };
        // Ascending: -10, -1, 0, 5
        let req_asc = QueryRequest {
            rank_by: json!(["BM25", "text", "tie score"]),
            top_k: Some(4),
            filters: None,
            include_attributes: None,
            consistency: None,
            order_by: Some(json!(["val", "asc"])),
            include_vectors: None,
            vector_encoding: None,
        };
        let resp = execute_query(&ctx, &req_asc).unwrap();
        let ids: Vec<_> = resp.rows.iter().map(|r| r.id.as_str()).collect();
        assert_eq!(
            ids,
            vec!["neg10", "neg1", "zero", "pos5"],
            "ascending order_by must sort negative numbers correctly"
        );
        // Descending: 5, 0, -1, -10
        let req_desc = QueryRequest {
            rank_by: json!(["BM25", "text", "tie score"]),
            top_k: Some(4),
            filters: None,
            include_attributes: None,
            consistency: None,
            order_by: Some(json!(["val", "desc"])),
            include_vectors: None,
            vector_encoding: None,
        };
        let resp = execute_query(&ctx, &req_desc).unwrap();
        let ids: Vec<_> = resp.rows.iter().map(|r| r.id.as_str()).collect();
        assert_eq!(
            ids,
            vec!["pos5", "zero", "neg1", "neg10"],
            "descending order_by must sort negative numbers correctly"
        );
    }

    #[test]
    fn order_by_malformed_rejected() {
        let map: HashMap<String, Document> = HashMap::new();
        let meta = NamespaceMeta::default();
        let tail = HashSet::new();
        let vecs = HashMap::new();
        let ctx = test_ctx(&map, &meta, &tail, &vecs);
        let req = QueryRequest {
            rank_by: json!(["BM25", "text", "x"]),
            top_k: Some(1),
            filters: None,
            include_attributes: None,
            consistency: None,
            order_by: Some(json!(["priority"])),
            include_vectors: None,
            vector_encoding: None,
        };
        let err = execute_query(&ctx, &req).unwrap_err().to_string();
        assert!(err.contains("order_by"), "{err}");
    }
}
# `queries.json` specification

Each synthetic-128 tier ships a committed **`queries.json`** next to `manifest.json`. Harnesses (`bench-large.sh`, `run_benchmark.py`, G2 Rust gates, id-overlap spot-check) read the same file so openpuffer and turbopuffer execute identical query shapes, vectors, and protocol defaults.

**Canonical generator:** [`generate_synthetic.py`](generate_synthetic.py) (`queries_dict()`).  
**Embedding of query vectors:** [`EMBEDDINGS.md`](EMBEDDINGS.md) (`bench_sin_v1` at each spec’s `doc_index` / `reference_doc_index`).  
**Committed example (L1):** [`synthetic-128/l1-100k/queries.json`](synthetic-128/l1-100k/queries.json).

---

## File layout

| Section | Count (default) | Role |
|---------|-----------------|------|
| Top-level metadata | — | Tier identity; must match `manifest.json` |
| `vector_queries` | **50** | Pure ANN queries; cold primary + spot-check source |
| `filter_queries` | **6** | ANN + attribute filters (secondary bench / G2) |
| `hybrid_queries` | **4** | ANN + BM25 `Sum`/`Product` (secondary bench / G2) |
| `recall_defaults` | — | `/recall` and tpuf `namespace.recall()` billing |
| `cold_query_protocol` | — | Mandatory 7× strong cold series (Phase 4) |
| `warm_query_protocol` | — | Optional 20× eventual warm series (`--warm`) |
| `spot_check` | — | Phase 3.3 id-overlap (first N vector queries) |

Regenerate after schema changes:

```bash
python3 benchmarks/workloads/generate_synthetic.py \
  --output-dir benchmarks/workloads/synthetic-128/l1-100k
./scripts/normalize-benchmark-json.sh benchmarks/workloads/synthetic-128/l1-100k/queries.json
```

Do **not** pretty-print `queries.json` with jq alone — float formatting must match the generator ([`benchmarks/README.md`](../README.md)).

---

## Top-level metadata

| Field | Type | Meaning |
|-------|------|---------|
| `schema_version` | int | Workload format version (**1** today; distinct from benchmark artifact `large_benchmark_v1`) |
| `seed` | int | Workload run label (must match manifest; does not affect `bench_sin_v1` vectors) |
| `num_docs` | int | Corpus size for this tier |
| `dim` | int | Vector dimension (**128**) |
| `embedding_fn` | string | How `vector` arrays were produced (**`bench_sin_v1`** on committed tiers) |

---

## `vector_queries`

Array of pure vector ANN specs. Doc indices are spread across the corpus:

```
step = max(1, num_docs // count)   # count defaults to 50
doc_index[q] = min(q * step, num_docs - 1)
name = vector-q{q:02d}
```

| Field | Required | Description |
|-------|----------|-------------|
| `name` | yes | Stable id (e.g. `vector-q00` … `vector-q49`) |
| `doc_index` | yes | Reference document; vector = `embedding_fn(doc_index)` |
| `vector` | yes | **128** floats (committed in git for G2 fixture checks) |
| `openpuffer_query` | yes | Query template; `$vector` replaced at runtime |

**`openpuffer_query` (vector):**

```json
{
  "rank_by": ["vector", "ANN", "embedding", "$vector"],
  "top_k": 10,
  "consistency": "strong"
}
```

**Tier doc_index examples** (`count = 50`):

| Tier | `num_docs` | `step` | First indices (`vector-q00` … `vector-q09`) |
|------|------------|--------|-----------------------------------------------|
| L1 | 100k | 2000 | 0, 2000, 4000, …, 18000 |
| L2 | 500k | 10000 | 0, 10000, 20000, …, 90000 |
| L3 | 1M | 20000 | 0, 20000, 40000, …, 180000 |

**Harness usage:**

- **Cold primary:** `vector_queries[0]` (`vector-q00`) — see `cold_query_protocol`.
- **Spot-check:** first `spot_check.count` entries (default **10**).
- **Remaining vector queries:** not run in A3/A4 cold bench by default (available for future expansion / recall sampling).

**Minimal example** (vector array abbreviated):

```json
{
  "name": "vector-q00",
  "doc_index": 0,
  "vector": ["/* 128 f32: bench_sin_v1(0) */"],
  "openpuffer_query": {
    "rank_by": ["vector", "ANN", "embedding", "$vector"],
    "top_k": 10,
    "consistency": "strong"
  }
}
```

---

## `filter_queries`

Six fixed filter templates; all share one **reference** query vector (`reference_doc_index = min(1000, num_docs - 1)`).

| `name` | Filter expression (summary) |
|--------|---------------------------|
| `filter-category-in-012` | `category In [cat-0, cat-1, cat-2]` |
| `filter-priority-gt-50` | `priority Gt 50` |
| `filter-category-eq-cat-3` | `category Eq cat-3` |
| `filter-priority-lte-10` | `priority Lte 10` |
| `filter-category-ne-cat-6-7` | `And` of two `category Ne` |
| `filter-priority-between` | `priority Gte 20` and `Lte 30` |

| Field | Required | Description |
|-------|----------|-------------|
| `name` | yes | Stable id |
| `filters` | yes | Filter AST (also copied into `openpuffer_query.filters`) |
| `reference_doc_index` | yes | Doc used for query vector |
| `vector` | yes | Embedding at `reference_doc_index` |
| `openpuffer_query` | yes | ANN + filters; `$vector` placeholder |

**Example:**

```json
{
  "name": "filter-category-in-012",
  "filters": ["category", "In", ["cat-0", "cat-1", "cat-2"]],
  "reference_doc_index": 1000,
  "vector": ["/* 128 f32: bench_sin_v1(1000) */"],
  "openpuffer_query": {
    "rank_by": ["vector", "ANN", "embedding", "$vector"],
    "filters": ["category", "In", ["cat-0", "cat-1", "cat-2"]],
    "top_k": 10,
    "consistency": "strong"
  }
}
```

**Harness:** `bench-large.sh` / `run_benchmark.py` run **each** filter **1×** after the cold series (`strong`). With `--warm`, repeats at `warm_query_protocol.consistency` (`eventual`). G2 integration tests assert all six on MinIO.

---

## `hybrid_queries`

Four templates combining vector ANN with BM25 on attribute `text` (see manifest `text.pattern`).

| `name` | `rank_by` combiner | Extra |
|--------|-------------------|--------|
| `hybrid-sum-vector-bm25` | `Sum` + BM25 `stressterm` | — |
| `hybrid-sum-with-category-filter` | `Sum` + BM25 `document` | `category Eq cat-1` |
| `hybrid-product-vector-bm25` | `Product` + BM25 `number` | — |
| `hybrid-sum-priority-filter` | `Sum` + BM25 `synthetic` | `priority Lt 25` |

Per spec, `doc_index = min((i + 1) * 500, num_docs - 1)` for template index `i`.

| Field | Required | Description |
|-------|----------|-------------|
| `name` | yes | Stable id |
| `bm25_term` | yes | Term passed to `["BM25", "text", …]` |
| `doc_index` | yes | Query vector source |
| `vector` | yes | Embedding at `doc_index` |
| `filters` | optional | Attribute filter AST |
| `openpuffer_query` | yes | Full rank_by tree with `$vector` |

**Example:**

```json
{
  "name": "hybrid-sum-vector-bm25",
  "bm25_term": "stressterm",
  "doc_index": 500,
  "vector": ["/* 128 f32: bench_sin_v1(500) */"],
  "openpuffer_query": {
    "rank_by": [
      "Sum",
      ["vector", "ANN", "embedding", "$vector"],
      ["BM25", "text", "stressterm"]
    ],
    "top_k": 10,
    "consistency": "strong"
  }
}
```

**Harness:** same as filters — 1× each after cold; openpuffer **resets cache before each hybrid** on cold path (`storage_roundtrips ≤ 4` gate). Warm path skips per-hybrid cache bust.

---

## `recall_defaults`

Parameters for ground-truth recall after the cold series.

```json
{
  "num": 20,
  "top_k": 10,
  "vector_field": "embedding"
}
```

| Field | Default | Override env |
|-------|---------|--------------|
| `num` | **20** | `OPENPUFFER_BENCH_RECALL_NUM` / `TURBOPUFFER_BENCH_RECALL_NUM` |
| `top_k` | **10** | `OPENPUFFER_BENCH_RECALL_TOP_K` / `TURBOPUFFER_BENCH_RECALL_TOP_K` |
| `vector_field` | **`embedding`** | — (openpuffer `/recall` body) |

**Consumers:** `bench-large.sh` (`POST …/recall`), `run_benchmark.py` (`namespace.recall()`), `recall_http_*` integration tests, `synthetic_workload_gate`.

**Cost note (tpuf):** billed queries scale with namespace size ([turbopuffer recall billing](https://turbopuffer.com/docs/recall#billing)). L3 operators may lower `num` (e.g. **10**) with a methodology note in the G5 report — keep openpuffer and tpuf on the same `num` for a given run pair.

---

## `cold_query_protocol`

Mandatory openpuffer ↔ turbopuffer cold latency comparison ([`docs/BENCHMARKS.md`](../../docs/BENCHMARKS.md) Phase 4).

```json
{
  "top_k": 10,
  "consistency": "strong",
  "runs": 7,
  "primary_query": "vector_queries[0]"
}
```

| Field | Default | Override env |
|-------|---------|--------------|
| `runs` | **7** | `OPENPUFFER_BENCH_COLD_RUNS` / `TURBOPUFFER_BENCH_COLD_RUNS` |
| `top_k` | **10** | from file (scripts read `cold_query_protocol.top_k`) |
| `consistency` | **`strong`** | from file |
| `primary_query` | **`vector_queries[0]`** | Resolved to `vector-q00` + its `vector` |

**Procedure (summary):** index caught up → cache bust each run → execute primary vector ANN `runs` times → aggregate p50/p95 → openpuffer `s3_get_count` probe → `/recall` with `recall_defaults`. Full steps in BENCHMARKS Phase 4.

---

## `warm_query_protocol`

Optional secondary latency path when `./scripts/bench-large.sh --warm` or tpuf `--warm` is set.

```json
{
  "top_k": 10,
  "consistency": "eventual",
  "runs": 20,
  "primary_query": "vector_queries[0]"
}
```

| Field | Default | Override env |
|-------|---------|--------------|
| `runs` | **20** | `OPENPUFFER_BENCH_WARM_RUNS` (bench-large) |
| `top_k` | **10** | from file |
| `consistency` | **`eventual`** | from file |
| `primary_query` | **`vector_queries[0]`** | Same vector as cold primary |

**openpuffer:** dedicated `--cache-dir`, `POST /v1/namespaces/{ns}/warm`, then `runs` queries without per-run cache bust. Filter/hybrid warm runs use the same `eventual` consistency.

**turbopuffer:** `hint_cache_warm` + warm series per driver README.

---

## `spot_check`

Config for Phase 3.3 **id overlap** between engines (not rank parity).

```json
{
  "count": 10,
  "top_k": 10,
  "include_attributes": true,
  "consistency": "strong",
  "source": "vector_queries",
  "notes": "Compare id overlap@k between openpuffer and turbopuffer ..."
}
```

| Field | Meaning |
|-------|---------|
| `count` | Use first **N** entries of `vector_queries` (default **10**) |
| `top_k` | ANN k for intersection@k (must match each query run) |
| `source` | **`vector_queries`** — specs are `vector_queries[0:count]` |
| `include_attributes` | Spot-check clients request ids in results |

**Script:** [`benchmarks/cross_check/run_spotcheck.py`](../cross_check/run_spotcheck.py) → `id-overlap-{tier}.json`. Helpers: [`id_overlap.py`](../cross_check/id_overlap.py) (`spot_check_query_specs()`).

Because `vector_queries` stride scales with `num_docs`, spot-check doc indices differ by tier (e.g. L2: 0, 10k, …, 90k; L3: 0, 20k, …, 180k) while **names** stay `vector-q00` … `vector-q09`.

---

## `$vector` substitution

`openpuffer_query` trees may contain the string **`"$vector"`** anywhere in `rank_by` (and nested lists). Harnesses and Rust G2 code replace it with the spec’s `vector` array before `POST /v1/namespaces/{ns}/query` (same contract as [`tests/common/synthetic_workload.rs`](../../tests/common/synthetic_workload.rs) and [`id_overlap.substitute_vector()`](../cross_check/id_overlap.py)).

---

## Consumers (quick map)

| Consumer | Reads |
|----------|--------|
| [`scripts/bench-large.sh`](../../scripts/bench-large.sh) | `cold_query_protocol`, `warm_query_protocol`, `recall_defaults`, all `filter_queries` / `hybrid_queries` |
| [`benchmarks/tpuf_driver/run_benchmark.py`](../tpuf_driver/run_benchmark.py) | Same + tpuf API mapping |
| G2 `synthetic_workload_gate` | Vector parity vs `bench_sin_v1`; `recall_defaults` |
| G2 `integration_s3` | Cold vector, all filters/hybrids, `/recall` |
| [`run-id-overlap-spotcheck.sh`](../../scripts/run-id-overlap-spotcheck.sh) | `spot_check` + first N `vector_queries` |

---

## Related docs

- Workload tiers and ingest: [`synthetic-128/README.md`](synthetic-128/README.md)
- Embeddings: [`EMBEDDINGS.md`](EMBEDDINGS.md)
- Operator cold/warm/recall procedure: [`docs/BENCHMARKS.md`](../../docs/BENCHMARKS.md) (Phase 4–6)
- Plan query emission: [`docs/PLAN_LARGE_DATASET_BENCHMARK.md`](../../docs/PLAN_LARGE_DATASET_BENCHMARK.md) Phase 1 / A1
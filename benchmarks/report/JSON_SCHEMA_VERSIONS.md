# Benchmark result JSON `schema_version` values

Single-line version files are the canonical source for harness emitters and validators.

| Artifact family | Canonical file | `schema_version` | JSON Schema |
|-----------------|----------------|-------------------|-------------|
| Large-dataset harness (L1–L3) | [`LARGE_BENCHMARK_JSON_SCHEMA_VERSION`](LARGE_BENCHMARK_JSON_SCHEMA_VERSION) | `large_benchmark_v1` | `schema/large-aws-l1.schema.json`, `tpuf-l1`, `ingest-large-l1`, `id-overlap-l1` |
| openpuffer MinIO scaling tiers | [`OP_SCALING_JSON_SCHEMA_VERSION`](OP_SCALING_JSON_SCHEMA_VERSION) | `op_scaling_v1` | [`schema/op-scaling.schema.json`](schema/op-scaling.schema.json) |
| tpuf official reference | (inline in artifact) | `tpuf_official_reference_v1` | — |
| Scaling comparison summary | (inline in `compare_op_scaling_to_tpuf.py`) | `scaling_comparison_summary_v1` | [`schema/scaling-comparison-summary.schema.json`](schema/scaling-comparison-summary.schema.json) |

Emitters: `ingest-large.sh`, `bench-large.sh`, `benchmarks/tpuf_driver/run_benchmark.py`, `benchmarks/cross_check/id_overlap.py` → `large_benchmark_v1`; `scripts/run-op-scaling-benchmark.sh` → `op_scaling_v1`.

Validation: [`scripts/validate-benchmark-json.sh`](../../scripts/validate-benchmark-json.sh).
# Comparison report assets (A5)

- **Fixtures:** `fixtures/large-aws-l1.json`, `fixtures/tpuf-l1.json`, `fixtures/ingest-large-l1.json` — offline inputs for `scripts/render-report.sh --dry-run` (L1-shaped; same schema as L2/L3 live artifacts).
- **JSON Schema (L1–L3):** `schema/large-aws-l1.schema.json`, `schema/tpuf-l1.schema.json`, `schema/ingest-large-l1.schema.json`, `schema/id-overlap-l1.schema.json` — one schema per artifact type with `tier` ∈ {l1,l2,l3}, tier-specific `benchmark` ids (`cold_large_l*`, `cold_tpuf_l*`, `ingest_large`, `id_overlap_spotcheck`), and doc counts enforced in [`scripts/validate-benchmark-json.sh`](../../scripts/validate-benchmark-json.sh).
- **Tier reference:**

| Tier | Docs | Workload dir | openpuffer `benchmark` | tpuf `benchmark` |
|------|------|--------------|------------------------|------------------|
| l1 | 100k | `benchmarks/workloads/synthetic-128/l1-100k` | `cold_large_l1` | `cold_tpuf_l1` |
| l2 | 500k | `benchmarks/workloads/synthetic-128/l2-500k` | `cold_large_l2` | `cold_tpuf_l2` |
| l3 | 1M | `benchmarks/workloads/synthetic-128/l3-1m` | `cold_large_l3` | `cold_tpuf_l3` |

- **Schema examples (not measured):** `../results/large-aws-{l2,l3}.example.json`, `tpuf-{l2,l3}.example.json`, `ingest-large-{l2,l3}.example.json`, `id-overlap-l1.example.json`; MinIO L1 shape: `large-aws-l1-schema-minio*.example.json`.
- **Live results:** `../results/large-aws-{tier}.json`, `../results/tpuf-{tier}.json`, `../results/ingest-large-{tier}.json`, `../results/id-overlap-{tier}.json`.
- **Output:** `../../docs/reports/BENCHMARK_VS_TURBOPUFFER_<date>.md`.

```bash
./scripts/validate-benchmark-json.sh   # fixtures + *.example.json (all tiers where present)
./scripts/test_render-report.sh
./scripts/test_render-report-measured.sh   # schema + interpretation + appendix redaction
./scripts/render-report.sh --dry-run --tier l1
./scripts/render-report.sh --tier l1 --date YYYY-MM-DD   # measured (requires live JSON)
./scripts/validate-benchmark-json.sh benchmarks/results/large-aws-l2.example.json
```

**Measured mode** (no `--dry-run`): validates both JSON files exist, checks required schema fields and workload alignment, scans for secrets before merge, emits **Comparison interpretation** (latency/recall deltas per plan §6.2), and embeds redacted JSON snapshots in the appendix.
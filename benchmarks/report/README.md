# Comparison report assets (A5)

- **Fixtures:** `fixtures/large-aws-l1.json`, `fixtures/tpuf-l1.json` — offline inputs for `scripts/render-report.sh --dry-run`.
- **JSON Schema:** `schema/large-aws-l1.schema.json`, `schema/tpuf-l1.schema.json` — validated by [`scripts/validate-benchmark-json.sh`](../../scripts/validate-benchmark-json.sh).
- **Live results:** `../results/large-aws-{tier}.json`, `../results/tpuf-{tier}.json`.
- **Output:** `../../docs/reports/BENCHMARK_VS_TURBOPUFFER_<date>.md`.

```bash
./scripts/test_render-report.sh
./scripts/test_render-report-measured.sh   # schema + interpretation + appendix redaction
./scripts/render-report.sh --dry-run --tier l1
./scripts/render-report.sh --tier l1 --date YYYY-MM-DD   # measured (requires live JSON)
```

**Measured mode** (no `--dry-run`): validates both JSON files exist, checks required schema fields and workload alignment, scans for secrets before merge, emits **Comparison interpretation** (latency/recall deltas per plan §6.2), and embeds redacted JSON snapshots in the appendix.
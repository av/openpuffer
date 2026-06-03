# Comparison report assets (A5)

- **Fixtures:** `fixtures/large-aws-l1.json`, `fixtures/tpuf-l1.json` — offline inputs for `scripts/render-report.sh --dry-run`.
- **Live results:** `../results/large-aws-{tier}.json`, `../results/tpuf-{tier}.json`.
- **Output:** `../../docs/reports/BENCHMARK_VS_TURBOPUFFER_<date>.md`.

```bash
./scripts/test_render-report.sh
./scripts/render-report.sh --dry-run --tier l1
```
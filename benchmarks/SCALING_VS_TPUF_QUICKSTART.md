# openpuffer vs turbopuffer scaling — 5-command quickstart

Reproduce the full **MinIO tier sweep + official tpuf reference comparison** from scratch. Requires **Docker** (MinIO testcontainers), **Rust release** build, and **~1–3 hours** wall time for 50k/100k tiers on a typical dev host.

Full report: [docs/reports/BENCHMARK_VS_TURBOPUFFER_SCALING_2026-06-04.md](../docs/reports/BENCHMARK_VS_TURBOPUFFER_SCALING_2026-06-04.md).

```bash
# 1. Regenerate all op-scaling JSON (10k / 50k / 100k cold + 10k/100k warm + synthetic-128)
make bench-op-scaling

# 2. Print extrapolation, models, and side-by-side vs tpuf-official-reference.json
make bench-compare-tpuf

# 3. Offline CI gate on committed artifacts (schema + compare smoke; no Docker)
make bench-verify-op-scaling

# 4. One-paragraph operator verdict (scaling shape vs tpuf 10M reference)
./scripts/print-scaling-verdict.sh

# 5. Commit results and open the report appendix
git add benchmarks/results/op-scaling-*.json && git diff --stat docs/reports/BENCHMARK_VS_TURBOPUFFER_SCALING_2026-06-04.md
```

**Notes**

- Step 1 is optional if you only need the **offline** comparison on existing `benchmarks/results/op-scaling-*.json` — run steps **2–4** only.
- turbopuffer numbers come from [`benchmarks/results/tpuf-official-reference.json`](results/tpuf-official-reference.json) (not re-fetched; no `TURBOPUFFER_API_KEY` required).
- Per-tier instead of `make bench-op-scaling`: `./scripts/run-op-scaling-benchmark.sh 10k|50k|100k|warm|100k-warm`.
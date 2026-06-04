# After the EC2 live run — activate `@spec` facts `8zb` and `7ow`

Use this checklist **after** G3/G4 on EC2 produced real comparison JSON. The harness run itself is [OPERATOR_RUNBOOK_QUICK.md](OPERATOR_RUNBOOK_QUICK.md); this doc covers validation, `git add -f`, and flipping placeholder `@spec` facts in [`.facts`](../.facts) from `pending`/`skipped` to `implemented`.

## Placeholder facts (before activation)

| Fact ID | Tag(s) today | Artifact | Section in `.facts` |
|---------|--------------|----------|---------------------|
| **`8zb`** | `bench-large`, `pending`, `skipped` | `benchmarks/results/large-aws-l1.json` (+ sidecar `ingest-large-l1.json`) | `# bench-large live G3` |
| **`7ow`** | `bench-tpuf`, `pending`, `skipped` | `benchmarks/results/tpuf-l1.json` | `# bench-tpuf live G4` |

While the JSON files are absent, `facts check --tags bench-large` / `bench-tpuf` still **pass**: each command prints `PENDING @spec (skipped): …` and exits 0. After the files exist and gates pass, **activate** the facts so they run real validation on every `facts check`.

Inspect a fact:

```bash
facts get 8zb
facts get 7ow
```

## Prerequisites

- [ ] G3 finished on EC2: `benchmarks/results/large-aws-l1.json` with `environment=aws-s3` (not `minio`)
- [ ] G3 ingest sidecar present: `benchmarks/results/ingest-large-l1.json`
- [ ] G4 finished: `benchmarks/results/tpuf-l1.json` with `environment=turbopuffer:<region>` and `cold_query_runs == 7`
- [ ] Repo root is the clone used on EC2 (or JSON copied in without renaming MinIO exemplars)

**Do not** commit `*-schema-minio*.example.json` or dry-run output as live `large-aws-l1.json` / `tpuf-l1.json`. Guards: `check-benchmark-artifacts.sh`, `run-aws-large-benchmark.sh` endpoint checks — see [results/OPERATOR_G3_G4_ATTEMPT.md](results/OPERATOR_G3_G4_ATTEMPT.md).

---

## 1. Validate G3 artifact — fact `8zb`

From repo root on the machine that holds the live JSON (typically the same EC2 instance):

```bash
./scripts/validate-benchmark-json.sh benchmarks/results/large-aws-l1.json
./scripts/check-benchmark-artifacts.sh benchmarks/results/large-aws-l1.json
./scripts/check-large-aws-gates.sh benchmarks/results/large-aws-l1.json
```

**SLO checks enforced by the fact command (and gates):**

| Field | Expectation |
|-------|-------------|
| `environment` | `aws-s3` |
| `preferred_ann_version` | `3` |
| `storage_roundtrips` | ≤ 4 |
| `cold_query_runs` | `7` |

Optional ingest sidecar validation:

```bash
./scripts/validate-benchmark-json.sh benchmarks/results/ingest-large-l1.json
```

Stage for commit (live basenames are gitignored until `-f`):

```bash
git add -f benchmarks/results/large-aws-l1.json benchmarks/results/ingest-large-l1.json
```

---

## 2. Validate G4 artifact — fact `7ow`

```bash
export TURBOPUFFER_API_KEY=<test-org-key>   # never commit
export TURBOPUFFER_REGION=aws-us-east-1   # align with S3 region

./scripts/validate-benchmark-json.sh benchmarks/results/tpuf-l1.json
./scripts/preflight-tpuf.sh --check-results benchmarks/results/tpuf-l1.json
./scripts/check-benchmark-artifacts.sh benchmarks/results/tpuf-l1.json
./scripts/check-tpuf-gates.sh benchmarks/results/tpuf-l1.json
```

| Field | Expectation |
|-------|-------------|
| `environment` | `turbopuffer:*` |
| `tpuf_region` | set (matches `TURBOPUFFER_REGION`) |
| `cold_query_runs` | `7` |

```bash
git add -f benchmarks/results/tpuf-l1.json
```

---

## 3. Activate facts `8zb` and `7ow`

Run **only after** the validation commands above succeed (artifacts on disk).

```bash
# Canonical (one command per fact):
facts edit 8zb --add-tag implemented --remove-tag pending,skipped
facts edit 7ow --add-tag implemented --remove-tag pending,skipped
```

CLI aliases (equivalent):

```bash
facts at 8zb implemented && facts rt 8zb pending && facts rt 8zb skipped
facts at 7ow implemented && facts rt 7ow pending && facts rt 7ow skipped
```

Confirm tags:

```bash
facts get 8zb   # expect: spec, bench-large, implemented (no pending/skipped)
facts get 7ow   # expect: spec, bench-tpuf, implemented (no pending/skipped)
```

---

## 4. Verify `facts check`

```bash
facts check --tags bench-large    # 8zb runs validate + artifacts + large-aws gates
facts check --tags bench-tpuf     # 7ow runs validate + tpuf preflight + gates
```

Both should show **`8zb` ✓** and **`7ow` ✓** without `PENDING @spec (skipped)` messages.

If you touched engine code during the live session:

```bash
facts check --tags "ann or cold"
```

Offline harness regression (laptop or CI, no AWS spend):

```bash
./scripts/verify-large-benchmark-program.sh
```

---

## 5. Commit (facts + JSON)

```bash
./scripts/check-benchmark-artifacts.sh --staged
git status   # large-aws-l1.json, ingest-large-l1.json, tpuf-l1.json staged with -f
git add .facts   # tag edits for 8zb / 7ow
git commit -m "bench: live L1 AWS/tpuf JSON; activate @spec facts 8zb and 7ow"
```

**Commit `.facts` in the same commit** as the live JSON so `facts check` on `main` validates real artifacts, not placeholders.

---

## 6. Still manual (not `8zb` / `7ow`)

These deliverables are **not** covered by the two placeholder facts; add new `@spec` facts when you publish them ([PLAN § Fact sheet](../docs/PLAN_LARGE_DATASET_BENCHMARK.md#fact-sheet)):

| Deliverable | When |
|-------------|------|
| `benchmarks/results/id-overlap-l1.json` | After `./scripts/run-id-overlap-spotcheck.sh --tier l1` (G3+G4 namespaces) |
| `docs/reports/BENCHMARK_VS_TURBOPUFFER_<date>.md` | After `./scripts/run-large-benchmark-program.sh --tier l1 --measured-report` |
| [COMPARISON.md](../docs/COMPARISON.md) L1 measured rows | After `fill-comparison-from-report.sh` |

Quick G5 path once JSON + overlap exist: [OPERATOR_RUNBOOK_QUICK.md § L1 measured run](OPERATOR_RUNBOOK_QUICK.md#l1-measured-run-copy-paste).

---

## Related docs

| Doc | Role |
|-----|------|
| [OPERATOR_RUNBOOK_QUICK.md](OPERATOR_RUNBOOK_QUICK.md) | EC2 env + G3→G5 copy-paste |
| [docs/reports/LARGE_DATASET_HARNESS_HANDOFF.md](../docs/reports/LARGE_DATASET_HARNESS_HANDOFF.md) | Harness PR summary; § Post-live facts |
| [docs/PLAN_LARGE_DATASET_BENCHMARK.md § Fact sheet](../docs/PLAN_LARGE_DATASET_BENCHMARK.md#fact-sheet) | Program fact inventory |
| [docs/BENCHMARKS.md](../docs/BENCHMARKS.md) | Full G3/G4 operator runbook |
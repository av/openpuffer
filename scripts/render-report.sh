#!/usr/bin/env bash
# Merge openpuffer + turbopuffer large-tier JSON into a comparison report (Phase 1 / A5).
#
# Usage:
#   ./scripts/render-report.sh                    # l1, today's date
#   ./scripts/render-report.sh --tier l2
#   ./scripts/render-report.sh --all-tiers        # l1,l2,l3 where JSON exists
#   ./scripts/render-report.sh --dry-run          # fixtures, no required live artifacts
#   ./scripts/render-report.sh --date 2026-06-04 --output docs/reports/BENCHMARK_VS_TURBOPUFFER_2026-06-04.md
#
# Inputs (per tier):
#   benchmarks/results/large-aws-{tier}.json  (from scripts/bench-large.sh)
#   benchmarks/results/tpuf-{tier}.json       (from benchmarks/tpuf_driver/run_benchmark.py)
#
# See docs/PLAN_LARGE_DATASET_BENCHMARK.md Phase 7 (G5).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

DRY_RUN=0
ALL_TIERS=0
TIER="${OPENPUFFER_REPORT_TIER:-l1}"
REPORT_DATE="${OPENPUFFER_REPORT_DATE:-$(date -u +%Y-%m-%d)}"
OUTPUT=""
OP_JSON=""
TPUF_JSON=""
OVERLAP_JSON=""
FIXTURES_DIR="${OPENPUFFER_REPORT_FIXTURES:-$ROOT/benchmarks/report/fixtures}"
ALLOW_PARTIAL=0

tier_docs_label() {
  case "$1" in
    l1) echo "100k" ;;
    l2) echo "500k" ;;
    l3) echo "1M" ;;
    *) echo "$1" ;;
  esac
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run|-n) DRY_RUN=1 ;;
    --all-tiers) ALL_TIERS=1 ;;
    --allow-partial) ALLOW_PARTIAL=1 ;;
    --tier=*) TIER="${1#*=}" ;;
    --tier)
      shift
      TIER="${1:?--tier requires l1|l2|l3}"
      ;;
    --date=*) REPORT_DATE="${1#*=}" ;;
    --date)
      shift
      REPORT_DATE="${1:?--date requires YYYY-MM-DD}"
      ;;
    --output=*) OUTPUT="${1#*=}" ;;
    --output)
      shift
      OUTPUT="${1:?--output requires path}"
      ;;
    --openpuffer-json=*) OP_JSON="${1#*=}" ;;
    --openpuffer-json)
      shift
      OP_JSON="${1:?path required}"
      ;;
    --tpuf-json=*) TPUF_JSON="${1#*=}" ;;
    --tpuf-json)
      shift
      TPUF_JSON="${1:?path required}"
      ;;
    --overlap-json=*) OVERLAP_JSON="${1#*=}" ;;
    --overlap-json)
      shift
      OVERLAP_JSON="${1:?path required}"
      ;;
    -h|--help)
      sed -n '2,16p' "$0"
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 1
      ;;
  esac
  shift
done
[[ "${OPENPUFFER_REPORT_DRY_RUN:-}" == "1" ]] && DRY_RUN=1

if [[ -z "$OUTPUT" ]]; then
  OUTPUT="$ROOT/docs/reports/BENCHMARK_VS_TURBOPUFFER_${REPORT_DATE}.md"
fi

need_cmd jq
need_cmd python3

COMMIT_SHA="$(git -C "$ROOT" rev-parse HEAD 2>/dev/null || echo "unknown")"
COMMIT_SHORT="$(git -C "$ROOT" rev-parse --short HEAD 2>/dev/null || echo "unknown")"

# Redact API keys / secrets from strings embedded in the report.
redact_text() {
  python3 -c '
import re, sys
text = sys.stdin.read()
patterns = [
    (r"tpuf_[A-Za-z0-9_-]+", "[REDACTED_TPUF_KEY]"),
    (r"sk-[A-Za-z0-9]{16,}", "[REDACTED_SECRET_KEY]"),
    (r"(?i)(api[_-]?key|secret[_-]?key|access[_-]?key)\s*[:=]\s*\S+", r"\1=[REDACTED]"),
    (r"OPENPUFFER_S3_SECRET_KEY=\S+", "OPENPUFFER_S3_SECRET_KEY=[REDACTED]"),
    (r"TURBOPUFFER_API_KEY=\S+", "TURBOPUFFER_API_KEY=[REDACTED]"),
]
for pat, repl in patterns:
    text = re.sub(pat, repl, text)
sys.stdout.write(text)
'
}

redact_json_file() {
  local path="$1"
  jq -c . "$path" | redact_text
}

json_path_for_tier() {
  local side="$1" tier="$2"
  if [[ "$side" == "op" ]]; then
    if [[ -n "$OP_JSON" && "$tier" == "$TIER" && "$ALL_TIERS" == "0" ]]; then
      echo "$OP_JSON"
      return 0
    fi
    echo "$ROOT/benchmarks/results/large-aws-${tier}.json"
    return 0
  fi
  if [[ -n "$TPUF_JSON" && "$tier" == "$TIER" && "$ALL_TIERS" == "0" ]]; then
    echo "$TPUF_JSON"
    return 0
  fi
  echo "$ROOT/benchmarks/results/tpuf-${tier}.json"
}

fixture_path_for_tier() {
  local side="$1" tier="$2"
  if [[ "$side" == "op" ]]; then
    echo "$FIXTURES_DIR/large-aws-${tier}.json"
  else
    echo "$FIXTURES_DIR/tpuf-${tier}.json"
  fi
}

resolve_input_json() {
  local side="$1" tier="$2"
  local primary fallback
  primary="$(json_path_for_tier "$side" "$tier")"
  if [[ -f "$primary" ]]; then
    echo "$primary"
    return 0
  fi
  if [[ "$DRY_RUN" == "1" ]]; then
    fallback="$(fixture_path_for_tier "$side" "$tier")"
    if [[ -f "$fallback" ]]; then
      echo "$fallback"
      return 0
    fi
  fi
  echo ""
}

resolve_overlap_json() {
  local tier="$1"
  if [[ -n "$OVERLAP_JSON" && ( "$tier" == "$TIER" || "$ALL_TIERS" == "1" ) ]]; then
    if [[ -f "$OVERLAP_JSON" ]]; then
      echo "$OVERLAP_JSON"
      return 0
    fi
  fi
  local primary="$ROOT/benchmarks/results/id-overlap-${tier}.json"
  if [[ -f "$primary" ]]; then
    echo "$primary"
    return 0
  fi
  if [[ "$DRY_RUN" == "1" && "$tier" == "l1" ]]; then
    local mock="$ROOT/benchmarks/cross_check/fixtures/overlap-l1-mock.json"
    if [[ -f "$mock" ]]; then
      echo "$mock"
      return 0
    fi
  fi
  echo ""
}

fmt_num() {
  local val="$1"
  if [[ -z "$val" || "$val" == "null" ]]; then
    echo "—"
    return 0
  fi
  echo "$val"
}

fmt_ratio() {
  local op="$1" tpuf="$2"
  if [[ -z "$op" || "$op" == "null" || -z "$tpuf" || "$tpuf" == "null" ]]; then
    echo "—"
    return 0
  fi
  python3 -c "op=float('$op'); tp=float('$tpuf'); print(f'{op/tp:.2f}×' if tp else '—')"
}

fmt_recall() {
  local val="$1"
  if [[ -z "$val" || "$val" == "null" ]]; then
    echo "—"
    return 0
  fi
  python3 -c "print(f'{float('$val'):.3f}')"
}

jq_field() {
  local file="$1" filter="$2"
  jq -r "$filter // \"null\"" "$file" 2>/dev/null || echo "null"
}

ingest_field() {
  local file="$1" top="$2" nested="$3"
  local val
  val="$(jq_field "$file" ".$top")"
  if [[ "$val" != "null" ]]; then
    echo "$val"
    return 0
  fi
  jq_field "$file" ".$nested"
}

collect_tiers() {
  if [[ "$ALL_TIERS" == "1" ]]; then
    echo "l1 l2 l3"
    return 0
  fi
  echo "$TIER"
}

validate_tier() {
  case "$1" in
    l1|l2|l3) return 0 ;;
    *)
      echo "unknown tier: $1 (use l1, l2, or l3)" >&2
      exit 1
      ;;
  esac
}

render_executive_summary() {
  local tier="$1" op_file="$2" tpuf_file="$3"
  local op_p50 tpuf_p50 op_recall tpuf_recall ratio
  local op_warm_p50 tpuf_warm_p50 warm_line=""
  op_p50="$(jq_field "$op_file" '.p50_query_latency_ms')"
  tpuf_p50="$(jq_field "$tpuf_file" '.p50_query_latency_ms')"
  op_recall="$(jq_field "$op_file" '.recall_at_10')"
  tpuf_recall="$(jq_field "$tpuf_file" '.recall_at_10')"
  ratio="$(fmt_ratio "$op_p50" "$tpuf_p50")"
  op_warm_p50="$(jq_field "$op_file" '.p50_warm_query_latency_ms')"
  tpuf_warm_p50="$(jq_field "$tpuf_file" '.p50_warm_query_latency_ms')"
  if [[ "$op_warm_p50" != "null" || "$tpuf_warm_p50" != "null" ]]; then
    warm_line="- **Warm query p50:** openpuffer $(fmt_num "$op_warm_p50") ms vs turbopuffer $(fmt_num "$tpuf_warm_p50") ms (ratio $(fmt_ratio "$op_warm_p50" "$tpuf_warm_p50"); openpuffer: \`POST /warm\` + eventual; tpuf: \`hint_cache_warm\` + eventual, plan §4.3).
"
  fi

  cat <<EOF
## Executive summary

- **Workload:** synthetic-128, tier **${tier}** ($(tier_docs_label "$tier") docs × 128-dim cosine, seed from manifest).
- **Cold query p50:** openpuffer $(fmt_num "$op_p50") ms vs turbopuffer $(fmt_num "$tpuf_p50") ms (ratio ${ratio}).
${warm_line}- **Recall@10 (num=20):** openpuffer $(fmt_recall "$op_recall") vs turbopuffer $(fmt_recall "$tpuf_recall").
- **Self-host vs managed:** openpuffer exposes S3 cold-path metrics (\`storage_roundtrips\`, \`cold_s3_keys_fetched\`); turbopuffer is managed (those fields n/a).
- _Interpretation (edit after review):_ see [COMPARISON.md](../COMPARISON.md) § measured maturity and plan §6.2 outcome table.

EOF
}

render_methodology_skeleton() {
  local tier="$1" op_file="$2" tpuf_file="$3"
  local docs dim seed emb op_env tpuf_env tpuf_region
  docs="$(jq_field "$op_file" '.namespace_docs')"
  dim="$(jq_field "$op_file" '.dimensions')"
  seed="$(jq_field "$op_file" '.seed')"
  emb="$(jq_field "$op_file" '.embedding_fn')"
  op_env="$(jq_field "$op_file" '.environment')"
  tpuf_env="$(jq_field "$tpuf_file" '.environment')"
  tpuf_region="$(jq_field "$tpuf_file" '.tpuf_region // .environment')"

  cat <<EOF
## Methodology

| Field | Value |
|-------|-------|
| Report date (UTC) | ${REPORT_DATE} |
| Tier | ${tier} ($(tier_docs_label "$tier") documents) |
| Document count | ${docs} |
| Dimensions | ${dim} |
| Seed | ${seed} |
| Embedding function | ${emb} |
| Distance | cosine (workload manifest) |
| openpuffer environment | ${op_env} |
| turbopuffer environment | ${tpuf_env} |
| turbopuffer region | ${tpuf_region} |
| openpuffer commit | \`${COMMIT_SHA}\` (\`${COMMIT_SHORT}\`) |
| Cache policy (openpuffer) | \`serve --cache-dir ""\` (no local segment cache) |
| Cache policy (turbopuffer) | fresh ephemeral namespace per run |
| ANN version (openpuffer) | preferred_ann_version=3 (gate) |
| Query protocol | strong consistency, vector-only ANN, top_k=10, 7 cold runs |
| Recall protocol | num=20, top_k=10, vector_field=embedding |
| Client placement | _fill: EC2 instance type + AZ, same-region as S3/tpuf_ |
| Ingest cadence (openpuffer) | ~1.1s between 10k batches (WAL-limited) |

**Artifacts:** \`benchmarks/results/large-aws-${tier}.json\`, \`benchmarks/results/tpuf-${tier}.json\`.

**Regenerate:**

\`\`\`bash
./scripts/ingest-large.sh --tier ${tier}
./scripts/bench-large.sh --tier ${tier}
python3 benchmarks/tpuf_driver/run_benchmark.py --tier ${tier}
./scripts/run-id-overlap-spotcheck.sh --tier ${tier}
./scripts/render-report.sh --tier ${tier} --date ${REPORT_DATE}
\`\`\`

EOF
}

render_setup_summary() {
  local tier="$1" op_file="$2" tpuf_file="$3"
  local op_ns tpuf_ns op_caught tpuf_indexed op_ingest tpuf_ingest
  local op_index_wait op_batches_ps op_docs_ps op_ingest_path
  op_ns="$(jq_field "$op_file" '.namespace' | redact_text)"
  tpuf_ns="$(jq_field "$tpuf_file" '.namespace' | redact_text)"
  op_caught="$(jq_field "$op_file" '.index_cursor_eq_wal_commit_seq')"
  tpuf_indexed="$(jq_field "$tpuf_file" '.index_up_to_date')"
  op_ingest="$(ingest_field "$op_file" 'ingest_elapsed_secs' 'ingest_timing.upsert_wall_sec')"
  tpuf_ingest="$(jq_field "$tpuf_file" '.ingest_elapsed_secs')"
  op_index_wait="$(ingest_field "$op_file" 'index_wait_sec' 'ingest_timing.index_wait_sec')"
  op_batches_ps="$(ingest_field "$op_file" 'ingest_batches_per_sec' 'ingest_timing.batches_per_sec')"
  op_docs_ps="$(ingest_field "$op_file" 'ingest_docs_per_sec' 'ingest_timing.docs_per_sec')"
  op_ingest_path="$(jq_field "$op_file" '.ingest_summary_path')"

  cat <<EOF
## Setup summary @ tier ${tier}

| Item | openpuffer | turbopuffer |
|------|------------|-------------|
| Namespace | ${op_ns} | ${tpuf_ns} |
| Index ready | index_cursor == wal_commit_seq: ${op_caught} | index up-to-date: ${tpuf_indexed} |
| Ingest upsert wall (s) | $(fmt_num "$op_ingest") | $(fmt_num "$tpuf_ingest") |
| Index wait (s) | $(fmt_num "$op_index_wait") | n/a (included in tpuf ingest) |
| Ingest docs/s | $(fmt_num "$op_docs_ps") | $(fmt_num "$(jq_field "$tpuf_file" '.ingest_docs_per_sec')") |
| Ingest batches/s | $(fmt_num "$op_batches_ps") | — |
| Ingest sidecar | $(if [[ "$op_ingest_path" != "null" ]]; then echo "\`${op_ingest_path}\`"; else echo "—"; fi) | — |
| Workload dir | \`$(jq_field "$op_file" '.workload_dir')\` | \`$(jq_field "$tpuf_file" '.workload_dir')\` |

_Notes:_ openpuffer upsert is WAL-limited (~1 commit/s); \`index_wait_sec\` is meta poll until \`index_cursor == wal_commit_seq\` and \`preferred_ann_version == 3\`. tpuf \`ingest_elapsed_secs\` is write-path only.

EOF
}

render_results_table() {
  local tier="$1" op_file="$2" tpuf_file="$3"
  local docs seed
  docs="$(jq_field "$op_file" '.namespace_docs')"
  seed="$(jq_field "$op_file" '.seed')"

  local op_p50 op_p95 tpuf_p50 tpuf_p95
  local op_warm_p50 op_warm_p95 tpuf_warm_p50 tpuf_warm_p95
  local op_recall tpuf_recall op_rt tpuf_rt op_cold tpuf_cold op_ratio tpuf_ratio op_idx tpuf_idx
  local op_ingest tpuf_ingest op_indexed tpuf_indexed
  local op_index_wait op_batch_p50 tpuf_docs_ps
  local warm_protocol_note=""

  op_p50="$(jq_field "$op_file" '.p50_query_latency_ms')"
  op_p95="$(jq_field "$op_file" '.p95_query_latency_ms')"
  op_warm_p50="$(jq_field "$op_file" '.p50_warm_query_latency_ms')"
  op_warm_p95="$(jq_field "$op_file" '.p95_warm_query_latency_ms')"
  tpuf_warm_p50="$(jq_field "$tpuf_file" '.p50_warm_query_latency_ms')"
  tpuf_warm_p95="$(jq_field "$tpuf_file" '.p95_warm_query_latency_ms')"
  tpuf_p50="$(jq_field "$tpuf_file" '.p50_query_latency_ms')"
  tpuf_p95="$(jq_field "$tpuf_file" '.p95_query_latency_ms')"
  op_recall="$(jq_field "$op_file" '.recall_at_10')"
  tpuf_recall="$(jq_field "$tpuf_file" '.recall_at_10')"
  op_rt="$(jq_field "$op_file" '.storage_roundtrips')"
  tpuf_rt="$(jq_field "$tpuf_file" '.storage_roundtrips')"
  op_cold="$(jq_field "$op_file" '.cold_s3_keys_fetched')"
  tpuf_cold="$(jq_field "$tpuf_file" '.cold_s3_keys_fetched')"
  op_ratio="$(jq_field "$op_file" '.candidates_ratio')"
  tpuf_ratio="$(jq_field "$tpuf_file" '.candidates_ratio')"
  op_idx="$(jq_field "$op_file" '.index_object_count')"
  tpuf_idx="$(jq_field "$tpuf_file" '.index_object_count')"
  op_ingest="$(ingest_field "$op_file" 'ingest_elapsed_secs' 'ingest_timing.upsert_wall_sec')"
  tpuf_ingest="$(jq_field "$tpuf_file" '.ingest_elapsed_secs')"
  op_index_wait="$(ingest_field "$op_file" 'index_wait_sec' 'ingest_timing.index_wait_sec')"
  op_batch_p50="$(jq_field "$op_file" '.ingest_timing.batch_latency_ms.p50')"
  tpuf_docs_ps="$(jq_field "$tpuf_file" '.ingest_docs_per_sec')"
  op_indexed="$(jq_field "$op_file" '.index_cursor_eq_wal_commit_seq')"
  tpuf_indexed="$(jq_field "$tpuf_file" '.index_up_to_date')"
  if [[ "$op_warm_p50" != "null" ]]; then
    local warm_runs warm_consistency warm_cache
    warm_runs="$(jq_field "$op_file" '.warm_query_runs')"
    warm_consistency="$(jq_field "$op_file" '.warm_consistency')"
    warm_cache="$(jq_field "$op_file" '.warm_cache_dir')"
    warm_protocol_note=" **Warm (openpuffer):** ${warm_runs} runs, consistency=${warm_consistency}, cache-dir=\`${warm_cache}\`, no cache bust between runs."
  fi
  if [[ "$tpuf_warm_p50" != "null" ]]; then
    local tpuf_warm_runs tpuf_warm_consistency tpuf_warm_proto
    tpuf_warm_runs="$(jq_field "$tpuf_file" '.warm_query_runs')"
    tpuf_warm_consistency="$(jq_field "$tpuf_file" '.warm_consistency')"
    tpuf_warm_proto="$(jq_field "$tpuf_file" '.warm_protocol // "hint_cache_warm"')"
    warm_protocol_note="${warm_protocol_note} **Warm (turbopuffer):** ${tpuf_warm_runs} runs, consistency=${tpuf_warm_consistency}, protocol=${tpuf_warm_proto}."
  fi

  cat <<EOF
## Results @ $(tier_docs_label "$tier") × 128-dim cosine (synthetic seed=${seed})

| Metric | openpuffer (AWS) | turbopuffer | Ratio (op/tpuf) |
|--------|-------------------|-------------|-----------------|
| Ingest upsert wall (s) | $(fmt_num "$op_ingest") | $(fmt_num "$tpuf_ingest") | $(fmt_ratio "$op_ingest" "$tpuf_ingest") |
| Index wait (s) | $(fmt_num "$op_index_wait") | — | — |
| Batch upsert p50 (ms) | $(fmt_num "$op_batch_p50") | — | — |
| Ingest docs/s | $(fmt_num "$(ingest_field "$op_file" 'ingest_docs_per_sec' 'ingest_timing.docs_per_sec')") | $(fmt_num "$tpuf_docs_ps") | $(fmt_ratio "$(ingest_field "$op_file" 'ingest_docs_per_sec' 'ingest_timing.docs_per_sec')" "$tpuf_docs_ps") |
| Time to indexed | ${op_indexed} | ${tpuf_indexed} | — |
| Cold p50 query (ms) | $(fmt_num "$op_p50") | $(fmt_num "$tpuf_p50") | $(fmt_ratio "$op_p50" "$tpuf_p50") |
| Cold p95 query (ms) | $(fmt_num "$op_p95") | $(fmt_num "$tpuf_p95") | $(fmt_ratio "$op_p95" "$tpuf_p95") |
| Warm p50 query (ms) | $(fmt_num "$op_warm_p50") | $(fmt_num "$tpuf_warm_p50") | $(fmt_ratio "$op_warm_p50" "$tpuf_warm_p50") |
| Warm p95 query (ms) | $(fmt_num "$op_warm_p95") | $(fmt_num "$tpuf_warm_p95") | $(fmt_ratio "$op_warm_p95" "$tpuf_warm_p95") |
| recall@10 (num=20) | $(fmt_recall "$op_recall") | $(fmt_recall "$tpuf_recall") | $(fmt_ratio "$op_recall" "$tpuf_recall") |
| storage_roundtrips | $(fmt_num "$op_rt") | n/a | — |
| cold_s3_keys_fetched | $(fmt_num "$op_cold") | n/a | — |
| candidates_ratio | $(fmt_num "$op_ratio") | $(fmt_num "$tpuf_ratio") | $(fmt_ratio "$op_ratio" "$tpuf_ratio") |
| index_object_count | $(fmt_num "$op_idx") | n/a | — |

**Query protocol:** strong consistency, vector-only ANN, top_k=10, cache cold (openpuffer: POST cache reset each run), ${docs} docs.${warm_protocol_note}

**Client:** EC2 localhost (openpuffer \`serve\`) vs turbopuffer SDK from same host (_document instance type in Methodology_).

EOF
}

render_secondary_query_table() {
  local tier="$1" tpuf_file="$2"
  local n_filter n_hybrid
  n_filter="$(jq_field "$tpuf_file" '.filter_query_runs | length')"
  n_hybrid="$(jq_field "$tpuf_file" '.hybrid_query_runs | length')"
  if [[ "$n_filter" == "0" && "$n_hybrid" == "0" ]]; then
    return 0
  fi
  if [[ "$n_filter" == "null" && "$n_hybrid" == "null" ]]; then
    return 0
  fi

  cat <<EOF
### Secondary queries (turbopuffer, tier ${tier})

Per-query latency from \`filter_query_runs\` / \`hybrid_query_runs\` in tpuf JSON (1× each, \`consistency: strong\`). openpuffer bench-large records cold vector only; G2 integration gates cover filter/hybrid correctness on MinIO.

EOF
  if [[ "$n_filter" != "0" && "$n_filter" != "null" ]]; then
    echo "| Filter query | Latency (ms) |"
    echo "|--------------|-------------:|"
    jq -r '.filter_query_runs[]? | "| \(.query_name) | \(.latency_ms) |"' "$tpuf_file"
    echo ""
  fi
  if [[ "$n_hybrid" != "0" && "$n_hybrid" != "null" ]]; then
    echo "| Hybrid query | Latency (ms) |"
    echo "|--------------|-------------:|"
    jq -r '.hybrid_query_runs[]? | "| \(.query_name) | \(.latency_ms) |"' "$tpuf_file"
    echo ""
  fi
}

render_correctness_section() {
  local tier="$1" op_file="$2" tpuf_file="$3" overlap_file="${4:-}"
  local op_recall tpuf_recall op_gate tpuf_gate
  local overlap_note overlap_mean overlap_min overlap_qcount overlap_mode
  op_recall="$(jq_field "$op_file" '.recall_at_10')"
  tpuf_recall="$(jq_field "$tpuf_file" '.recall_at_10')"
  op_gate="≥0.85 (large-tier gate)"
  tpuf_gate="≥0.85 (tpuf driver gate)"

  if [[ -n "$overlap_file" && -f "$overlap_file" ]]; then
    overlap_mean="$(jq_field "$overlap_file" '.summary.mean_overlap_at_k')"
    overlap_min="$(jq_field "$overlap_file" '.summary.min_overlap_at_k')"
    overlap_qcount="$(jq_field "$overlap_file" '.summary.query_count')"
    overlap_mode="$(jq_field "$overlap_file" '.mode')"
    overlap_note="mean intersection@10 = $(fmt_recall "$overlap_mean") (${overlap_qcount} vector queries, mode=${overlap_mode}; min=$(fmt_recall "$overlap_min"))"
  else
    overlap_note="_not run — \`./scripts/run-id-overlap-spotcheck.sh --tier ${tier}\` after ingest_"
  fi

  cat <<EOF
## Correctness @ tier ${tier}

| Check | openpuffer | turbopuffer |
|-------|------------|-------------|
| recall@10 | $(fmt_recall "$op_recall") (${op_gate}) | $(fmt_recall "$tpuf_recall") (${tpuf_gate}) |
| Index completeness | preferred_ann_version=3, cursor caught up | index.status up-to-date |
| Spot-check overlap (10× ANN top_k=10) | ${overlap_note} | same query vectors |

Both sides use the same \`queries.json\` recall defaults (num=20, top_k=10). Synthetic embeddings may overstate absolute recall; treat as a regression gate, not a product claim. **Overlap@k** compares result ids only (not rank order); expect divergence from different ANN graphs.

EOF
}

render_static_sections() {
  cat <<'EOF'
## Debugging notes

- _Record failures, timeouts, and config tweaks here for the next operator._
- openpuffer: check `index_cursor` vs `wal_commit_seq`, S3 prefix listing, `storage_roundtrips` on last cold run.
- turbopuffer: check `index.status`, billing alerts, namespace delete cleanup.

## Limitations

- openpuffer ingest throughput is WAL-limited (~1 commit/s); write benchmarks are not apples-to-apples with tpuf batch ingest.
- turbopuffer JSON has no `storage_roundtrips` / `cold_s3_keys_fetched` (managed storage).
- Billing / $/query not included unless explicitly measured.
- Synthetic `bench_sin_v1` data; real embeddings may differ.
- API keys and secrets are redacted in this report; raw JSON in `benchmarks/results/` must be scrubbed before commit if copied from live runs.

## Appendix — JSON artifacts

| Tier | openpuffer | turbopuffer |
|------|------------|-------------|
EOF
}

render_appendix_row() {
  local tier="$1" op_path="$2" tpuf_path="$3"
  printf '| %s | `%s` | `%s` |\n' "$tier" "${op_path#$ROOT/}" "${tpuf_path#$ROOT/}"
}

run_dry_run_banner() {
  echo "render-report dry-run: date=${REPORT_DATE} output=${OUTPUT}" >&2
  echo "  commit=${COMMIT_SHORT} fixtures=${FIXTURES_DIR}" >&2
}

main() {
  local tiers tier op_path tpuf_path missing=0
  tiers="$(collect_tiers)"
  for tier in $tiers; do
    validate_tier "$tier"
  done

  if [[ "$DRY_RUN" == "1" ]]; then
    run_dry_run_banner
    ALLOW_PARTIAL=1
  fi

  for tier in $tiers; do
    op_path="$(resolve_input_json op "$tier")"
    tpuf_path="$(resolve_input_json tpuf "$tier")"
    if [[ -z "$op_path" || ! -f "$op_path" ]]; then
      echo "missing openpuffer JSON for tier ${tier} (expected large-aws-${tier}.json)" >&2
      missing=1
      continue
    fi
    if [[ -z "$tpuf_path" || ! -f "$tpuf_path" ]]; then
      echo "missing turbopuffer JSON for tier ${tier} (expected tpuf-${tier}.json)" >&2
      missing=1
      continue
    fi
    echo "  tier=${tier} op=${op_path} tpuf=${tpuf_path}" >&2
  done

  if [[ "$missing" == "1" && "$ALLOW_PARTIAL" == "0" ]]; then
    echo "aborting: provide both JSON files per tier or use --dry-run / --allow-partial" >&2
    exit 1
  fi
  if [[ "$missing" == "1" && "$ALLOW_PARTIAL" == "1" ]]; then
    echo "warning: partial tiers skipped" >&2
  fi

  mkdir -p "$(dirname "$OUTPUT")"
  {
    cat <<EOF
# openpuffer vs turbopuffer — large synthetic benchmark

_Generated by \`scripts/render-report.sh\` on ${REPORT_DATE} (UTC). Commit \`${COMMIT_SHORT}\`._

EOF
    if [[ "$DRY_RUN" == "1" ]]; then
      cat <<EOF
> **NOT MEASURED** — dry-run report using fixtures under \`benchmarks/report/fixtures/\` and (for L1) \`benchmarks/cross_check/fixtures/overlap-l1-mock.json\`. Numbers are placeholders for layout review only; do not cite in COMPARISON.md until live \`large-aws-*.json\` and \`tpuf-*.json\` exist.

EOF
    fi
    for tier in $tiers; do
      op_path="$(resolve_input_json op "$tier")"
      tpuf_path="$(resolve_input_json tpuf "$tier")"
      [[ -f "$op_path" && -f "$tpuf_path" ]] || continue
      render_executive_summary "$tier" "$op_path" "$tpuf_path"
      render_methodology_skeleton "$tier" "$op_path" "$tpuf_path"
      render_setup_summary "$tier" "$op_path" "$tpuf_path"
      render_results_table "$tier" "$op_path" "$tpuf_path"
      render_secondary_query_table "$tier" "$tpuf_path"
      overlap_path="$(resolve_overlap_json "$tier")"
      render_correctness_section "$tier" "$op_path" "$tpuf_path" "$overlap_path"
      echo ""
    done
    render_static_sections
    for tier in $tiers; do
      op_path="$(resolve_input_json op "$tier")"
      tpuf_path="$(resolve_input_json tpuf "$tier")"
      [[ -f "$op_path" && -f "$tpuf_path" ]] || continue
      render_appendix_row "$tier" "$op_path" "$tpuf_path"
    done
    echo ""
    echo "_End of report._"
  } | redact_text >"$OUTPUT"

  echo "Wrote ${OUTPUT}"
  if [[ "$DRY_RUN" == "1" ]]; then
    echo "Dry-run complete (fixtures allowed). Review skeleton sections marked TODO/_fill_."
  fi
}

main
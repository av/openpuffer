#!/usr/bin/env bash
# Merge openpuffer + turbopuffer large-tier JSON into a comparison report (Phase 1 / A5).
#
# Usage:
#   ./scripts/render-report.sh                    # l1, today's date
#   ./scripts/render-report.sh --tier l2
#   ./scripts/render-report.sh --all-tiers        # l1,l2,l3 where JSON exists
#   ./scripts/render-report.sh --dry-run          # fixtures, no required live artifacts
#   ./scripts/render-report.sh --allow-partial    # warn + single-side tables when one JSON missing
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
    (r"tpuf_(?!driver)[A-Za-z0-9_-]{8,}", "[REDACTED_TPUF_KEY]"),
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
  appendix_json_compact "$path" | redact_text
}

# Compact JSON for appendix: drop large run arrays, keep summary metrics.
appendix_json_compact() {
  local path="$1"
  jq -c '
    del(.cold_runs, .warm_runs)
    | if .ingest_timing then .ingest_timing |= del(.batch_runs) else . end
  ' "$path"
}

scan_artifact_secrets() {
  local path="$1"
  if ! python3 - "$path" <<'PY'; then
import re, sys
from pathlib import Path

path = Path(sys.argv[1])
text = path.read_text()
if re.search(r"tpuf_(?!driver)[A-Za-z0-9_-]{8,}", text):
    sys.exit(1)
if "TURBOPUFFER_API_KEY=" in text or "OPENPUFFER_S3_SECRET_KEY=" in text:
    sys.exit(1)
PY
    echo "render-report: possible secret in ${path} — scrub before report/commit" >&2
    return 1
  fi
  return 0
}

validate_measured_json_pair() {
  local tier="$1" op_file="$2" tpuf_file="$3"
  python3 - "$tier" "$op_file" "$tpuf_file" <<'PY'
import json, sys
from pathlib import Path

tier, op_path, tpuf_path = sys.argv[1:4]

OP_REQUIRED = (
    "benchmark", "tier", "environment", "workload_dir", "namespace",
    "namespace_docs", "dimensions", "seed", "embedding_fn",
    "p50_query_latency_ms", "p95_query_latency_ms", "recall_at_10",
    "index_cursor_eq_wal_commit_seq",
)
TPUF_REQUIRED = (
    "benchmark", "tier", "environment", "workload_dir", "namespace",
    "namespace_docs", "dimensions", "seed", "embedding_fn",
    "p50_query_latency_ms", "p95_query_latency_ms", "recall_at_10",
    "index_up_to_date",
)
MATCH_KEYS = ("tier", "namespace_docs", "dimensions", "seed", "embedding_fn")

def load(path: str) -> dict:
    p = Path(path)
    if not p.is_file():
        raise SystemExit(f"render-report: missing JSON: {path}")
    try:
        return json.loads(p.read_text())
    except json.JSONDecodeError as exc:
        raise SystemExit(f"render-report: invalid JSON {path}: {exc}") from exc

def require(data: dict, keys: tuple, label: str, path: str) -> None:
    missing = [k for k in keys if k not in data or data[k] is None]
    if missing:
        raise SystemExit(
            f"render-report: {label} schema missing fields in {path}: {', '.join(missing)}"
        )

op = load(op_path)
tpuf = load(tpuf_path)
require(op, OP_REQUIRED, "openpuffer", op_path)
require(tpuf, TPUF_REQUIRED, "turbopuffer", tpuf_path)

if str(op.get("tier")) != tier:
    raise SystemExit(
        f"render-report: openpuffer tier={op.get('tier')!r} does not match --tier {tier}"
    )
if str(tpuf.get("tier")) != tier:
    raise SystemExit(
        f"render-report: turbopuffer tier={tpuf.get('tier')!r} does not match --tier {tier}"
    )

for key in MATCH_KEYS:
    if op.get(key) != tpuf.get(key):
        raise SystemExit(
            f"render-report: workload mismatch on {key}: openpuffer={op.get(key)!r} "
            f"turbopuffer={tpuf.get(key)!r}"
        )

op_env = str(op.get("environment", ""))
if "minio" in op_env.lower():
    print(
        f"render-report: warning openpuffer environment={op_env!r} "
        "(not aws-s3; measured COMPARISON rows expect live AWS JSON)",
        file=sys.stderr,
    )

print(f"render-report: schema OK tier={tier} op={op_path} tpuf={tpuf_path}", file=sys.stderr)
PY
}

validate_measured_json_single() {
  local side="$1" tier="$2" path="$3"
  python3 - "$side" "$tier" "$path" <<'PY'
import json, sys
from pathlib import Path

side, tier, path = sys.argv[1:4]

OP_REQUIRED = (
    "benchmark", "tier", "environment", "workload_dir", "namespace",
    "namespace_docs", "dimensions", "seed", "embedding_fn",
    "p50_query_latency_ms", "p95_query_latency_ms", "recall_at_10",
    "index_cursor_eq_wal_commit_seq",
)
TPUF_REQUIRED = (
    "benchmark", "tier", "environment", "workload_dir", "namespace",
    "namespace_docs", "dimensions", "seed", "embedding_fn",
    "p50_query_latency_ms", "p95_query_latency_ms", "recall_at_10",
    "index_up_to_date",
)
REQUIRED = OP_REQUIRED if side == "op" else TPUF_REQUIRED
LABEL = "openpuffer" if side == "op" else "turbopuffer"

p = Path(path)
if not p.is_file():
    raise SystemExit(f"render-report: missing JSON: {path}")
try:
    data = json.loads(p.read_text())
except json.JSONDecodeError as exc:
    raise SystemExit(f"render-report: invalid JSON {path}: {exc}") from exc

missing = [k for k in REQUIRED if k not in data or data[k] is None]
if missing:
    raise SystemExit(
        f"render-report: {LABEL} schema missing fields in {path}: {', '.join(missing)}"
    )
if str(data.get("tier")) != tier:
    raise SystemExit(
        f"render-report: {LABEL} tier={data.get('tier')!r} does not match --tier {tier}"
    )

if side == "op" and "minio" in str(data.get("environment", "")).lower():
    print(
        f"render-report: warning openpuffer environment={data.get('environment')!r} "
        "(not aws-s3; measured COMPARISON rows expect live AWS JSON)",
        file=sys.stderr,
    )

print(f"render-report: schema OK tier={tier} {LABEL}={path} (partial, single side)", file=sys.stderr)
PY
}

json_file_exists() {
  [[ -n "${1:-}" && -f "$1" ]]
}

primary_json_file() {
  if json_file_exists "$1"; then
    echo "$1"
  elif json_file_exists "$2"; then
    echo "$2"
  else
    echo ""
  fi
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

explicit_json_requested() {
  local side="$1" tier="$2"
  if [[ "$tier" != "$TIER" || "$ALL_TIERS" == "1" ]]; then
    return 1
  fi
  if [[ "$side" == "op" && -n "$OP_JSON" ]]; then
    return 0
  fi
  if [[ "$side" == "tpuf" && -n "$TPUF_JSON" ]]; then
    return 0
  fi
  return 1
}

resolve_input_json() {
  local side="$1" tier="$2"
  local primary fallback
  primary="$(json_path_for_tier "$side" "$tier")"
  if [[ -f "$primary" ]]; then
    echo "$primary"
    return 0
  fi
  if explicit_json_requested "$side" "$tier"; then
    echo ""
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
  local example="$ROOT/benchmarks/results/id-overlap-${tier}.example.json"
  if [[ -f "$example" ]]; then
    echo "$example"
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
  if ! json_file_exists "$file"; then
    echo "null"
    return 0
  fi
  jq -r "$filter // \"null\"" "$file" 2>/dev/null || echo "null"
}

render_partial_notice() {
  local tier="$1" have_op="$2" have_tpuf="$3"
  local missing_side missing_path present_side
  if [[ "$have_op" == "1" && "$have_tpuf" == "1" ]]; then
    return 0
  fi
  if [[ "$have_op" == "1" ]]; then
    present_side="openpuffer"
    missing_side="turbopuffer"
    missing_path="benchmarks/results/tpuf-${tier}.json"
  else
    present_side="turbopuffer"
    missing_side="openpuffer"
    missing_path="benchmarks/results/large-aws-${tier}.json"
  fi
  cat <<EOF
> **PARTIAL REPORT (tier ${tier})** — only **${present_side}** benchmark JSON is present. Missing: \`${missing_path}\` (run the other engine or pass \`--openpuffer-json\` / \`--tpuf-json\`). Comparison ratios, auto-interpretation, and ${missing_side} columns show **—**. Do not publish partial rows to [COMPARISON.md](../COMPARISON.md) until both sides exist.

EOF
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

render_executive_summary_partial() {
  local tier="$1" op_file="$2" tpuf_file="$3" overlap_file="${4:-}"
  local have_op="$5" have_tpuf="$6"
  local warm_line="" overlap_line="" status_line=""
  local op_p50 tpuf_p50 op_recall tpuf_recall op_warm_p50 tpuf_warm_p50
  local overlap_mean overlap_mode
  primary="$(primary_json_file "$op_file" "$tpuf_file")"
  op_p50="$(jq_field "$op_file" '.p50_query_latency_ms')"
  tpuf_p50="$(jq_field "$tpuf_file" '.p50_query_latency_ms')"
  op_recall="$(jq_field "$op_file" '.recall_at_10')"
  tpuf_recall="$(jq_field "$tpuf_file" '.recall_at_10')"
  op_warm_p50="$(jq_field "$op_file" '.p50_warm_query_latency_ms')"
  tpuf_warm_p50="$(jq_field "$tpuf_file" '.p50_warm_query_latency_ms')"
  if [[ "$op_warm_p50" != "null" || "$tpuf_warm_p50" != "null" ]]; then
    warm_line="- **Warm query p50:** openpuffer $(fmt_num "$op_warm_p50") ms vs turbopuffer $(fmt_num "$tpuf_warm_p50") ms (ratio —; partial merge).
"
  fi
  if [[ -n "$overlap_file" && -f "$overlap_file" ]]; then
    overlap_mean="$(jq_field "$overlap_file" '.summary.mean_overlap_at_k')"
    overlap_mode="$(jq_field "$overlap_file" '.mode')"
    overlap_line="- **Spot-check overlap@10 (mean):** $(fmt_recall "$overlap_mean") (10 vector queries, mode=${overlap_mode}; Phase 3.3 \`id-overlap-${tier}.json\`).
"
  fi
  if [[ "$have_op" == "1" ]]; then
    status_line="- **Merge status:** partial — turbopuffer JSON missing; ratios and interpretation omitted."
  else
    status_line="- **Merge status:** partial — openpuffer JSON missing; ratios and interpretation omitted."
  fi

  cat <<EOF
## Executive summary

- **Workload:** synthetic-128, tier **${tier}** ($(tier_docs_label "$tier") docs × 128-dim cosine, seed from manifest).
${status_line}
- **Cold query p50:** openpuffer $(fmt_num "$op_p50") ms vs turbopuffer $(fmt_num "$tpuf_p50") ms (ratio —).
${warm_line}${overlap_line}- **Recall@10 (num=20):** openpuffer $(fmt_recall "$op_recall") vs turbopuffer $(fmt_recall "$tpuf_recall").
- **Self-host vs managed:** openpuffer exposes S3 cold-path metrics when present; turbopuffer is managed (those fields n/a).
- _Partial merge:_ comparison interpretation skipped until both JSON files exist. [COMPARISON.md](../COMPARISON.md) § measured maturity.

EOF
}

render_executive_summary() {
  local tier="$1" op_file="$2" tpuf_file="$3" overlap_file="${4:-}"
  local have_op=0 have_tpuf=0
  json_file_exists "$op_file" && have_op=1
  json_file_exists "$tpuf_file" && have_tpuf=1
  if [[ "$have_op" == "0" || "$have_tpuf" == "0" ]]; then
    render_executive_summary_partial "$tier" "$op_file" "$tpuf_file" "$overlap_file" "$have_op" "$have_tpuf"
    return 0
  fi
  local op_p50 tpuf_p50 op_recall tpuf_recall ratio
  local op_warm_p50 tpuf_warm_p50 warm_line="" overlap_line=""
  local overlap_mean overlap_mode
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
  if [[ -n "$overlap_file" && -f "$overlap_file" ]]; then
    overlap_mean="$(jq_field "$overlap_file" '.summary.mean_overlap_at_k')"
    overlap_mode="$(jq_field "$overlap_file" '.mode')"
    overlap_line="- **Spot-check overlap@10 (mean):** $(fmt_recall "$overlap_mean") (10 vector queries, mode=${overlap_mode}; Phase 3.3 \`id-overlap-${tier}.json\`).
"
  fi

  cat <<EOF
## Executive summary

- **Workload:** synthetic-128, tier **${tier}** ($(tier_docs_label "$tier") docs × 128-dim cosine, seed from manifest).
- **Cold query p50:** openpuffer $(fmt_num "$op_p50") ms vs turbopuffer $(fmt_num "$tpuf_p50") ms (ratio ${ratio}).
${warm_line}${overlap_line}- **Recall@10 (num=20):** openpuffer $(fmt_recall "$op_recall") vs turbopuffer $(fmt_recall "$tpuf_recall").
- **Self-host vs managed:** openpuffer exposes S3 cold-path metrics (\`storage_roundtrips\`, \`cold_s3_keys_fetched\`); turbopuffer is managed (those fields n/a).
- _Auto-interpretation:_ see [Comparison interpretation (tier ${tier})](#comparison-interpretation-tier-${tier}) below; edit after operator review. [COMPARISON.md](../COMPARISON.md) § measured maturity.

EOF
}

render_comparison_interpretation() {
  local tier="$1" op_file="$2" tpuf_file="$3"
  python3 - "$tier" "$op_file" "$tpuf_file" <<'PY'
import json, sys
from pathlib import Path

tier, op_path, tpuf_path = sys.argv[1:4]
op = json.loads(Path(op_path).read_text())
tpuf = json.loads(Path(tpuf_path).read_text())
RECALL_GATE = 0.85


def fnum(data, key):
    v = data.get(key)
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def latency_line(label, op_ms, tpuf_ms):
    if op_ms is None or tpuf_ms is None or tpuf_ms <= 0:
        return None
    ratio = op_ms / tpuf_ms
    if ratio < 0.95:
        verdict = f"openpuffer **{tpuf_ms / op_ms:.2f}× faster** than turbopuffer"
    elif ratio <= 2.0:
        verdict = (
            f"openpuffer **{ratio:.2f}× slower** than turbopuffer "
            "(within ~2× — competitive for self-hosted per plan §6.2)"
        )
    else:
        verdict = (
            f"openpuffer **{ratio:.2f}× slower** than turbopuffer "
            "(**>2×** — investigate S3 RTT, probe clamp, rerank, indexer lag per plan §6.2)"
        )
    return (
        f"- **{label}:** {verdict} "
        f"({op_ms:.0f} ms openpuffer vs {tpuf_ms:.0f} ms turbopuffer; ratio {ratio:.2f}× op/tpuf)."
    )


def ingest_line():
    op_ingest = fnum(op, "ingest_elapsed_secs")
    if op_ingest is None and op.get("ingest_timing"):
        op_ingest = fnum(op["ingest_timing"], "upsert_wall_sec")
    tpuf_ingest = fnum(tpuf, "ingest_elapsed_secs")
    if op_ingest is None or tpuf_ingest is None or tpuf_ingest <= 0:
        return None
    ratio = op_ingest / tpuf_ingest
    return (
        f"- **Ingest upsert wall:** openpuffer {op_ingest:.1f}s vs turbopuffer {tpuf_ingest:.1f}s "
        f"(ratio {ratio:.2f}× op/tpuf). openpuffer WAL-limited (~1 commit/s) — "
        "not apples-to-apples with tpuf batch ingest (plan §6.2)."
    )


def recall_lines():
    op_r = fnum(op, "recall_at_10")
    tpuf_r = fnum(tpuf, "recall_at_10")
    if op_r is None or tpuf_r is None:
        return ["- **Recall@10:** missing in one or both JSON files."]
    lines = [
        f"- **Recall@10:** openpuffer {op_r:.3f} vs turbopuffer {tpuf_r:.3f} "
        f"(delta {op_r - tpuf_r:+.3f})."
    ]
    if op_r < RECALL_GATE:
        lines.append(
            f"  - **Warning:** openpuffer recall {op_r:.3f} below large-tier gate (≥{RECALL_GATE})."
        )
    if tpuf_r < RECALL_GATE:
        lines.append(
            f"  - **Warning:** turbopuffer recall {tpuf_r:.3f} below driver gate (≥{RECALL_GATE})."
        )
    delta = op_r - tpuf_r
    if delta < -0.02:
        lines.append(
            "  - **Warning:** openpuffer recall materially below turbopuffer "
            "(>0.02) — tune probes/rerank; simpler v3 ANN vs managed SPFresh is expected on some workloads (plan §6.2)."
        )
    elif abs(delta) <= 0.02:
        lines.append(
            "  - Recall within ±0.02 on synthetic data — strong signal; validate on real embeddings before product claims."
        )
    elif delta > 0.02:
        lines.append(
            "  - openpuffer recall above turbopuffer on this run — treat as synthetic-workload signal only."
        )
    return lines


print(f"## Comparison interpretation (tier {tier})")
print()
print("Auto-generated from merged JSON (measured mode). Ratios use **op/tpuf** (>1 means openpuffer slower or higher).")
print()
for line in (
    latency_line("Cold query p50", fnum(op, "p50_query_latency_ms"), fnum(tpuf, "p50_query_latency_ms")),
    latency_line("Cold query p95", fnum(op, "p95_query_latency_ms"), fnum(tpuf, "p95_query_latency_ms")),
    latency_line(
        "Warm query p50",
        fnum(op, "p50_warm_query_latency_ms"),
        fnum(tpuf, "p50_warm_query_latency_ms"),
    ),
    latency_line(
        "Warm query p95",
        fnum(op, "p95_warm_query_latency_ms"),
        fnum(tpuf, "p95_warm_query_latency_ms"),
    ),
    ingest_line(),
):
    if line:
        print(line)
for line in recall_lines():
    print(line)
print()
PY
}

render_methodology_skeleton() {
  local tier="$1" op_file="$2" tpuf_file="$3" overlap_file="${4:-}"
  local overlap_artifact=""
  local meta_file docs dim seed emb op_env tpuf_env tpuf_region
  meta_file="$(primary_json_file "$op_file" "$tpuf_file")"
  docs="$(jq_field "$meta_file" '.namespace_docs')"
  dim="$(jq_field "$meta_file" '.dimensions')"
  seed="$(jq_field "$meta_file" '.seed')"
  emb="$(jq_field "$meta_file" '.embedding_fn')"
  op_env="$(jq_field "$op_file" '.environment')"
  tpuf_env="$(jq_field "$tpuf_file" '.environment')"
  tpuf_region="$(jq_field "$tpuf_file" '.tpuf_region // .environment')"
  if [[ "$op_env" == "null" ]]; then op_env="_pending (large-aws-${tier}.json)_"; fi
  if [[ "$tpuf_env" == "null" ]]; then tpuf_env="_pending (tpuf-${tier}.json)_"; fi
  if [[ "$tpuf_region" == "null" ]]; then tpuf_region="_pending_"; fi
  if [[ -n "$overlap_file" && -f "$overlap_file" ]]; then
    overlap_artifact=", \`benchmarks/results/id-overlap-${tier}.json\`"
  fi

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

**Artifacts:** \`benchmarks/results/large-aws-${tier}.json\`, \`benchmarks/results/tpuf-${tier}.json\`${overlap_artifact}.

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
  if [[ "$op_ns" == "null" ]]; then op_ns="_pending_"; fi
  if [[ "$tpuf_ns" == "null" ]]; then tpuf_ns="_pending_"; fi
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
  local tier="$1" op_file="$2" tpuf_file="$3" overlap_file="${4:-}"
  local meta_file docs seed overlap_mean overlap_min overlap_mode overlap_row=""
  meta_file="$(primary_json_file "$op_file" "$tpuf_file")"
  docs="$(jq_field "$meta_file" '.namespace_docs')"
  seed="$(jq_field "$meta_file" '.seed')"

  local op_p50 op_p95 tpuf_p50 tpuf_p95
  local op_warm_p50 op_warm_p95 tpuf_warm_p50 tpuf_warm_p95
  local op_recall tpuf_recall op_rt op_cold op_ratio tpuf_ratio op_idx
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
  op_cold="$(jq_field "$op_file" '.cold_s3_keys_fetched')"
  op_ratio="$(jq_field "$op_file" '.candidates_ratio')"
  tpuf_ratio="$(jq_field "$tpuf_file" '.candidates_ratio')"
  op_idx="$(jq_field "$op_file" '.index_object_count')"
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
  if [[ -n "$overlap_file" && -f "$overlap_file" ]]; then
    overlap_mean="$(jq_field "$overlap_file" '.summary.mean_overlap_at_k')"
    overlap_min="$(jq_field "$overlap_file" '.summary.min_overlap_at_k')"
    overlap_mode="$(jq_field "$overlap_file" '.mode')"
    overlap_row="| Spot-check overlap@10 (mean / min) | $(fmt_recall "$overlap_mean") / $(fmt_recall "$overlap_min") | same query vectors (mode=${overlap_mode}) | — |
"
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
${overlap_row}| storage_roundtrips | $(fmt_num "$op_rt") | n/a | — |
| cold_s3_keys_fetched | $(fmt_num "$op_cold") | n/a | — |
| candidates_ratio | $(fmt_num "$op_ratio") | $(fmt_num "$tpuf_ratio") | $(fmt_ratio "$op_ratio" "$tpuf_ratio") |
| index_object_count | $(fmt_num "$op_idx") | n/a | — |

**Query protocol:** strong consistency, vector-only ANN, top_k=10, cache cold (openpuffer: POST cache reset each run), ${docs} docs.${warm_protocol_note}

**Client:** EC2 localhost (openpuffer \`serve\`) vs turbopuffer SDK from same host (_document instance type in Methodology_).

EOF
}

resolve_jq_input() {
  local path="$1"
  if json_file_exists "$path"; then
    echo "$path"
  else
    echo "${EMPTY_JSON:?EMPTY_JSON unset}"
  fi
}

render_secondary_query_table() {
  local tier="$1" op_file="$2" tpuf_file="$3"
  local op_jq tpuf_jq
  local op_nf op_nh tpuf_nf tpuf_nh
  op_jq="$(resolve_jq_input "$op_file")"
  tpuf_jq="$(resolve_jq_input "$tpuf_file")"
  op_nf="$(jq_field "$op_file" '.filter_query_runs | length')"
  op_nh="$(jq_field "$op_file" '.hybrid_query_runs | length')"
  tpuf_nf="$(jq_field "$tpuf_file" '.filter_query_runs | length')"
  tpuf_nh="$(jq_field "$tpuf_file" '.hybrid_query_runs | length')"

  if [[ ("$op_nf" == "0" || "$op_nf" == "null") && ("$op_nh" == "0" || "$op_nh" == "null") \
    && ("$tpuf_nf" == "0" || "$tpuf_nf" == "null") && ("$tpuf_nh" == "0" || "$tpuf_nh" == "null") ]]; then
    return 0
  fi

  cat <<EOF
### Secondary queries (filter + hybrid, tier ${tier})

Per-query latency from \`filter_query_runs\` / \`hybrid_query_runs\` (1× each). Cold path uses \`consistency: strong\`; openpuffer hybrid queries reset segment cache before each run (G2 pattern). When \`bench-large.sh --warm\` ran, openpuffer may also include \`warm_filter_query_runs\` / \`warm_hybrid_query_runs\` @ eventual.

EOF

  if [[ "$op_nf" != "0" && "$op_nf" != "null" || "$tpuf_nf" != "0" && "$tpuf_nf" != "null" ]]; then
    echo "#### Filter queries"
    echo ""
    echo "| Query | openpuffer (ms) | turbopuffer (ms) | Ratio (op/tpuf) |"
    echo "|-------|----------------:|-----------------:|----------------:|"
    jq -s -r '
      (.[0].filter_query_runs // []) as $op
      | (.[1].filter_query_runs // []) as $tpuf
      | ([$op[] | {name: .query_name, op: .latency_ms}]
         + [$tpuf[] | {name: .query_name, tpuf: .latency_ms}])
      | group_by(.name)
      | map({
          name: .[0].name,
          op: (map(.op) | map(select(. != null)) | .[0] // null),
          tpuf: (map(.tpuf) | map(select(. != null)) | .[0] // null)
        })
      | sort_by(.name)
      | .[]
      | "| \(.name) | \(.op // "—") | \(.tpuf // "—") | "
        + (if (.op != null and .tpuf != null and .tpuf != 0)
           then ((.op / .tpuf * 100 | round / 100 | tostring) + "×")
           else "—" end)
        + " |"
    ' "$op_jq" "$tpuf_jq"
    echo ""
  fi

  if [[ "$op_nh" != "0" && "$op_nh" != "null" || "$tpuf_nh" != "0" && "$tpuf_nh" != "null" ]]; then
    echo "#### Hybrid queries"
    echo ""
    echo "| Query | openpuffer (ms) | turbopuffer (ms) | Ratio (op/tpuf) |"
    echo "|-------|----------------:|-----------------:|----------------:|"
    jq -s -r '
      (.[0].hybrid_query_runs // []) as $op
      | (.[1].hybrid_query_runs // []) as $tpuf
      | ([$op[] | {name: .query_name, op: .latency_ms}]
         + [$tpuf[] | {name: .query_name, tpuf: .latency_ms}])
      | group_by(.name)
      | map({
          name: .[0].name,
          op: (map(.op) | map(select(. != null)) | .[0] // null),
          tpuf: (map(.tpuf) | map(select(. != null)) | .[0] // null)
        })
      | sort_by(.name)
      | .[]
      | "| \(.name) | \(.op // "—") | \(.tpuf // "—") | "
        + (if (.op != null and .tpuf != null and .tpuf != 0)
           then ((.op / .tpuf * 100 | round / 100 | tostring) + "×")
           else "—" end)
        + " |"
    ' "$op_jq" "$tpuf_jq"
    echo ""
  fi

  local op_wf op_wh
  op_wf="$(jq_field "$op_file" '.warm_filter_query_runs | length')"
  op_wh="$(jq_field "$op_file" '.warm_hybrid_query_runs | length')"
  if [[ "$op_wf" != "0" && "$op_wf" != "null" || "$op_wh" != "0" && "$op_wh" != "null" ]]; then
    echo "#### openpuffer warm secondary (eventual)"
    echo ""
    if [[ "$op_wf" != "0" && "$op_wf" != "null" ]]; then
      echo "| Filter query | Latency (ms) |"
      echo "|--------------|-------------:|"
      jq -r '.warm_filter_query_runs[]? | "| \(.query_name) | \(.latency_ms) |"' "$op_file"
      echo ""
    fi
    if [[ "$op_wh" != "0" && "$op_wh" != "null" ]]; then
      echo "| Hybrid query | Latency (ms) |"
      echo "|--------------|-------------:|"
      jq -r '.warm_hybrid_query_runs[]? | "| \(.query_name) | \(.latency_ms) |"' "$op_file"
      echo ""
    fi
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
  local op_cell tpuf_cell
  if json_file_exists "$op_path"; then
    op_cell="\`${op_path#"$ROOT"/}\`"
  else
    op_cell="_missing (large-aws-${tier}.json)_"
  fi
  if json_file_exists "$tpuf_path"; then
    tpuf_cell="\`${tpuf_path#"$ROOT"/}\`"
  else
    tpuf_cell="_missing (tpuf-${tier}.json)_"
  fi
  printf "| %s | %s | %s |\n" "$tier" "$op_cell" "$tpuf_cell"
}

render_appendix_json_blocks() {
  local tier="$1" op_path="$2" tpuf_path="$3"
  local op_blob tpuf_blob have_op=0 have_tpuf=0
  json_file_exists "$op_path" && have_op=1
  json_file_exists "$tpuf_path" && have_tpuf=1
  [[ "$have_op" == "1" || "$have_tpuf" == "1" ]] || return 0

  cat <<EOF
### Redacted JSON snapshot @ tier ${tier}

Embedded copies for audit (secrets redacted; large \`cold_runs\` / \`warm_runs\` arrays omitted). Canonical files remain under \`benchmarks/results/\`.

EOF
  if [[ "$have_op" == "1" ]]; then
    op_blob="$(redact_json_file "$op_path")"
    cat <<EOF
#### openpuffer — \`${op_path#"$ROOT"/}\`

\`\`\`json
${op_blob}
\`\`\`

EOF
  fi
  if [[ "$have_tpuf" == "1" ]]; then
    tpuf_blob="$(redact_json_file "$tpuf_path")"
    cat <<EOF
#### turbopuffer — \`${tpuf_path#"$ROOT"/}\`

\`\`\`json
${tpuf_blob}
\`\`\`

EOF
  fi
}

run_dry_run_banner() {
  echo "render-report dry-run: date=${REPORT_DATE} output=${OUTPUT}" >&2
  echo "  commit=${COMMIT_SHORT} fixtures=${FIXTURES_DIR}" >&2
}

main() {
  local tiers tier op_path tpuf_path
  local missing=0 partial=0 renderable=0
  local have_op=0 have_tpuf=0
  tiers="$(collect_tiers)"
  for tier in $tiers; do
    validate_tier "$tier"
  done

  EMPTY_JSON="$(mktemp)"
  trap 'rm -f "${EMPTY_JSON:-}"' EXIT
  echo '{}' >"$EMPTY_JSON"

  if [[ "$DRY_RUN" == "1" ]]; then
    run_dry_run_banner
    ALLOW_PARTIAL=1
  fi

  for tier in $tiers; do
    op_path="$(resolve_input_json op "$tier")"
    tpuf_path="$(resolve_input_json tpuf "$tier")"
    have_op=0
    have_tpuf=0
    json_file_exists "$op_path" && have_op=1
    json_file_exists "$tpuf_path" && have_tpuf=1

    if [[ "$have_op" == "0" && "$have_tpuf" == "0" ]]; then
      echo "render-report: missing both JSON for tier ${tier} (large-aws-${tier}.json and tpuf-${tier}.json)" >&2
      missing=1
      continue
    fi

    if [[ "$have_op" == "1" && "$have_tpuf" == "1" ]]; then
      if [[ "$DRY_RUN" == "0" ]]; then
        validate_measured_json_pair "$tier" "$op_path" "$tpuf_path"
        scan_artifact_secrets "$op_path" || exit 1
        scan_artifact_secrets "$tpuf_path" || exit 1
      fi
      echo "  tier=${tier} op=${op_path} tpuf=${tpuf_path} (full)" >&2
      renderable=1
      continue
    fi

    if [[ "$ALLOW_PARTIAL" == "0" ]]; then
      if [[ "$have_op" == "0" ]]; then
        echo "render-report: missing openpuffer JSON for tier ${tier} (expected large-aws-${tier}.json)" >&2
      fi
      if [[ "$have_tpuf" == "0" ]]; then
        echo "render-report: missing turbopuffer JSON for tier ${tier} (expected tpuf-${tier}.json)" >&2
      fi
      missing=1
      continue
    fi

    partial=1
    if [[ "$have_op" == "0" ]]; then
      echo "render-report: warning tier=${tier} missing openpuffer JSON — partial report (turbopuffer only)" >&2
    fi
    if [[ "$have_tpuf" == "0" ]]; then
      echo "render-report: warning tier=${tier} missing turbopuffer JSON — partial report (openpuffer only)" >&2
    fi
    if [[ "$DRY_RUN" == "0" ]]; then
      if [[ "$have_op" == "1" ]]; then
        validate_measured_json_single op "$tier" "$op_path"
        scan_artifact_secrets "$op_path" || exit 1
      fi
      if [[ "$have_tpuf" == "1" ]]; then
        validate_measured_json_single tpuf "$tier" "$tpuf_path"
        scan_artifact_secrets "$tpuf_path" || exit 1
      fi
    fi
    echo "  tier=${tier} op=${op_path:-—} tpuf=${tpuf_path:-—} (partial)" >&2
    renderable=1
  done

  if [[ "$renderable" == "0" ]]; then
    echo "render-report: aborting — no tier had any JSON (use --dry-run or produce at least one side)" >&2
    exit 1
  fi
  if [[ "$missing" == "1" && "$ALLOW_PARTIAL" == "0" ]]; then
    echo "render-report: aborting — provide both JSON files per tier or use --dry-run / --allow-partial" >&2
    exit 1
  fi
  if [[ "$partial" == "1" ]]; then
    echo "render-report: warning — partial report (one side missing for some tiers)" >&2
  fi

  mkdir -p "$(dirname "$OUTPUT")"
  {
    cat <<EOF
# openpuffer vs turbopuffer — large synthetic benchmark

_Generated by \`scripts/render-report.sh\` on ${REPORT_DATE} (UTC). Commit \`${COMMIT_SHORT}\`._

EOF
    if [[ "$DRY_RUN" == "1" ]]; then
      cat <<EOF
> **NOT MEASURED** — dry-run report using fixtures under \`benchmarks/report/fixtures/\` and (when present) \`benchmarks/results/id-overlap-*.example.json\` or \`benchmarks/cross_check/fixtures/overlap-*-mock.json\`. Numbers are placeholders for layout review only; do not cite in COMPARISON.md until live \`large-aws-*.json\` and \`tpuf-*.json\` exist.

EOF
    fi
    for tier in $tiers; do
      op_path="$(resolve_input_json op "$tier")"
      tpuf_path="$(resolve_input_json tpuf "$tier")"
      have_op=0
      have_tpuf=0
      json_file_exists "$op_path" && have_op=1
      json_file_exists "$tpuf_path" && have_tpuf=1
      [[ "$have_op" == "1" || "$have_tpuf" == "1" ]] || continue
      if [[ "$have_op" == "0" || "$have_tpuf" == "0" ]]; then
        render_partial_notice "$tier" "$have_op" "$have_tpuf"
      fi
      overlap_path="$(resolve_overlap_json "$tier")"
      render_executive_summary "$tier" "$op_path" "$tpuf_path" "$overlap_path"
      render_methodology_skeleton "$tier" "$op_path" "$tpuf_path" "$overlap_path"
      render_setup_summary "$tier" "$op_path" "$tpuf_path"
      render_results_table "$tier" "$op_path" "$tpuf_path" "$overlap_path"
      render_secondary_query_table "$tier" "$op_path" "$tpuf_path"
      if [[ "$DRY_RUN" == "0" && "$have_op" == "1" && "$have_tpuf" == "1" ]]; then
        render_comparison_interpretation "$tier" "$op_path" "$tpuf_path"
        echo ""
      fi
      render_correctness_section "$tier" "$op_path" "$tpuf_path" "$overlap_path"
      echo ""
    done
    render_static_sections
    for tier in $tiers; do
      op_path="$(resolve_input_json op "$tier")"
      tpuf_path="$(resolve_input_json tpuf "$tier")"
      have_op=0
      have_tpuf=0
      json_file_exists "$op_path" && have_op=1
      json_file_exists "$tpuf_path" && have_tpuf=1
      [[ "$have_op" == "1" || "$have_tpuf" == "1" ]] || continue
      render_appendix_row "$tier" "$op_path" "$tpuf_path"
    done
    echo ""
    for tier in $tiers; do
      op_path="$(resolve_input_json op "$tier")"
      tpuf_path="$(resolve_input_json tpuf "$tier")"
      have_op=0
      have_tpuf=0
      json_file_exists "$op_path" && have_op=1
      json_file_exists "$tpuf_path" && have_tpuf=1
      [[ "$have_op" == "1" || "$have_tpuf" == "1" ]] || continue
      render_appendix_json_blocks "$tier" "$op_path" "$tpuf_path"
    done
    echo "_End of report._"
  } | redact_text >"$OUTPUT"

  echo "Wrote ${OUTPUT}"
  if [[ "$DRY_RUN" == "1" ]]; then
    echo "Dry-run complete (fixtures allowed). Review skeleton sections marked TODO/_fill_."
  elif [[ "$partial" == "1" ]]; then
    echo "Partial report complete — merge remaining JSON before publishing to COMPARISON.md."
  fi
}

main
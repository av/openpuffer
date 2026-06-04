#!/usr/bin/env bash
# Copy L1 measured rows from a Phase 7 report into docs/COMPARISON.md (plan §7.3).
#
# Usage:
#   ./scripts/fill-comparison-from-report.sh --report docs/reports/BENCHMARK_VS_TURBOPUFFER_<date>.md
#   ./scripts/fill-comparison-from-report.sh --dry-run --allow-fixture \
#     --report docs/reports/BENCHMARK_VS_TURBOPUFFER_EXEMPLAR.md
#
# Refuses reports marked NOT MEASURED unless --allow-fixture (harness / exemplar only).
# See docs/PLAN_LARGE_DATASET_BENCHMARK.md §7.3 and docs/COMPARISON.md § L1 measured rows.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

REPORT=""
COMPARISON="$ROOT/docs/COMPARISON.md"
TIER="l1"
DRY_RUN=0
ALLOW_FIXTURE=0
IN_PLACE=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --report=*) REPORT="${1#*=}" ;;
    --report)
      shift
      REPORT="${1:?--report requires path}"
      ;;
    --comparison=*) COMPARISON="${1#*=}" ;;
    --comparison)
      shift
      COMPARISON="${1:?--comparison requires path}"
      ;;
    --tier=*) TIER="${1#*=}" ;;
    --tier)
      shift
      TIER="${1:?--tier requires l1|l2|l3}"
      ;;
    --dry-run|-n) DRY_RUN=1; IN_PLACE=0 ;;
    --allow-fixture) ALLOW_FIXTURE=1 ;;
    --no-write) IN_PLACE=0 ;;
    -h|--help)
      sed -n '2,14p' "$0"
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 1
      ;;
  esac
  shift
done

[[ -n "$REPORT" ]] || {
  echo "fill-comparison-from-report: --report is required" >&2
  exit 1
}

if [[ "$REPORT" != /* ]]; then
  REPORT="$ROOT/$REPORT"
fi
if [[ "$COMPARISON" != /* ]]; then
  COMPARISON="$ROOT/$COMPARISON"
fi

[[ -f "$REPORT" ]] || {
  echo "fill-comparison-from-report: report not found: $REPORT" >&2
  exit 1
}
[[ -f "$COMPARISON" ]] || {
  echo "fill-comparison-from-report: comparison doc not found: $COMPARISON" >&2
  exit 1
}

if [[ "$TIER" != "l1" ]]; then
  echo "fill-comparison-from-report: only --tier l1 is supported (COMPARISON § L1 rows)" >&2
  exit 1
fi

command -v python3 >/dev/null 2>&1 || {
  echo "fill-comparison-from-report: python3 required" >&2
  exit 1
}

export ROOT REPORT COMPARISON TIER ALLOW_FIXTURE

BLOCK="$(python3 <<'PY'
import os
import re
import sys
from pathlib import Path

root = Path(os.environ["ROOT"])
report_path = Path(os.environ["REPORT"])
comparison_path = Path(os.environ["COMPARISON"])
tier = os.environ["TIER"]
allow_fixture = os.environ.get("ALLOW_FIXTURE") == "1"

text = report_path.read_text()
rel_report = report_path.relative_to(root).as_posix()
link_report = f"[`{rel_report}`]({rel_report})"

if "NOT MEASURED" in text and not allow_fixture:
    print(
        "fill-comparison-from-report: report is NOT MEASURED; "
        "use live JSON + measured render-report, or pass --allow-fixture for exemplar only",
        file=sys.stderr,
    )
    sys.exit(2)

tier_docs = {"l1": "100k", "l2": "500k", "l3": "1M"}
docs_label = tier_docs.get(tier)
if not docs_label:
    print(f"fill-comparison-from-report: unknown tier {tier!r}", file=sys.stderr)
    sys.exit(1)

results_hdr = re.compile(
    rf"^## Results @ {re.escape(docs_label)} × 128-dim cosine.*$",
    re.MULTILINE,
)
m_res = results_hdr.search(text)
if not m_res:
    print(
        f"fill-comparison-from-report: missing ## Results @ {docs_label} section in {report_path}",
        file=sys.stderr,
    )
    sys.exit(1)

rest = text[m_res.end() :]
end = re.search(r"\n\*\*Query protocol:|\n## ", rest)
table_block = rest[: end.start()] if end else rest

rows: dict[str, tuple[str, str, str]] = {}
for line in table_block.splitlines():
    line = line.strip()
    if not line.startswith("|") or line.startswith("| Metric") or line.startswith("|--------"):
        continue
    parts = [p.strip() for p in line.strip("|").split("|")]
    if len(parts) < 4:
        continue
    metric, op_val, tpuf_val, ratio = parts[0], parts[1], parts[2], parts[3]
    rows[metric] = (op_val, tpuf_val, ratio)

# Overlap: Results row (when overlap JSON merged) or Correctness § Spot-check row (exemplar).
overlap_op = overlap_tpuf = None
overlap_key = "Spot-check overlap@10 (mean / min)"
if overlap_key in rows:
    op_cell, tp_cell, _ = rows[overlap_key]
    overlap_op = op_cell
    overlap_tpuf = tp_cell
else:
    corr = re.search(
        rf"^## Correctness @ tier {re.escape(tier)}\s*\n\n(.*?)(?=\n## |\Z)",
        text,
        re.MULTILINE | re.DOTALL,
    )
    if corr:
        for line in corr.group(1).splitlines():
            if "Spot-check overlap" not in line:
                continue
            parts = [p.strip() for p in line.strip("|").split("|")]
            if len(parts) >= 3:
                overlap_op = parts[1]
                overlap_tpuf = parts[2]
            break

def pick(*names: str) -> tuple[str, str, str] | None:
    for name in names:
        if name in rows:
            return rows[name]
    return None

def dash_if_empty(val: str) -> str:
    v = val.strip()
    if not v or v in ("null", "_pending_"):
        return "—"
    return v

# COMPARISON metric -> report metric name(s)
COMPARISON_METRICS = [
    ("Ingest wall time (s)", ("Ingest upsert wall (s)", "Ingest wall time (s)", "Ingest wall time")),
    ("Time to indexed", ("Time to indexed",)),
    ("Cold p50 query (ms)", ("Cold p50 query (ms)",)),
    ("Cold p95 query (ms)", ("Cold p95 query (ms)",)),
    ("Warm p50 query (ms)", ("Warm p50 query (ms)",)),
    ("recall@10 (num=20)", ("recall@10 (num=20)",)),
    ("`storage_roundtrips`", ("storage_roundtrips", "`storage_roundtrips`")),
    ("`cold_s3_keys_fetched`", ("cold_s3_keys_fetched", "`cold_s3_keys_fetched`")),
    ("`candidates_ratio`", ("candidates_ratio", "`candidates_ratio`")),
    ("`index_object_count`", ("index_object_count", "`index_object_count`")),
]

table_lines = [
    "| Metric | openpuffer (AWS) | turbopuffer | Ratio (op/tpuf) | Source |",
    "|--------|------------------|-------------|-------------------|--------|",
]

for comp_metric, report_names in COMPARISON_METRICS:
    row = pick(*report_names)
    if row:
        op_v, tpuf_v, ratio = row
        source = f"{rel_report} § Results"
    else:
        op_v = tpuf_v = ratio = "_pending_"
        source = "`large-aws-l1.json` / `tpuf-l1.json`"
    table_lines.append(
        f"| {comp_metric} | {dash_if_empty(op_v)} | {dash_if_empty(tpuf_v)} | "
        f"{dash_if_empty(ratio) if ratio not in ('—', '') else '—'} | {source} |"
    )

if overlap_op:
    table_lines.append(
        f"| Spot-check overlap@10 (10 queries) | {overlap_op} | {overlap_tpuf or 'same query vectors'} | — | "
        f"`id-overlap-l1.json` via {rel_report} § Correctness |"
    )
else:
    table_lines.append(
        "| Spot-check overlap@10 (10 queries) | _pending_ | _pending_ | — | "
        "`id-overlap-l1.json` (Phase 3.3) |"
    )

gen_m = re.search(
    r"_Generated by `scripts/render-report\.sh` on (\d{4}-\d{2}-\d{2}) \(UTC\)\. Commit `([0-9a-f]+)`\._",
    text,
)
report_date = gen_m.group(1) if gen_m else "unknown"
commit_short = gen_m.group(2) if gen_m else "unknown"

if "NOT MEASURED" in text:
    status = (
        "**Status:** **fixture dry-run** — rows copied from "
        f"{link_report} via [`fill-comparison-from-report.sh`](../scripts/fill-comparison-from-report.sh) "
        f"(`--allow-fixture`). **Not** live AWS/tpuf comparison numbers."
    )
else:
    status = (
        "**Status:** **measured** — rows copied from "
        f"{link_report} ({report_date} UTC, commit `{commit_short}`) via "
        f"[`fill-comparison-from-report.sh`](../scripts/fill-comparison-from-report.sh). "
        "**Do not** substitute MinIO CI numbers for this matrix."
    )

accepted = (
    f"**Accepted report:** {link_report} — generated {report_date} UTC @ `{commit_short}` "
    f"([Phase 7](PLAN_LARGE_DATASET_BENCHMARK.md#phase-7--comparison-report-deliverable))."
)

print("<!-- comparison-l1-status:start -->")
print(status)
print("<!-- comparison-l1-status:end -->")
print()
print("<!-- comparison-l1-rows:start -->")
print("\n".join(table_lines))
print("<!-- comparison-l1-rows:end -->")
print()
print("<!-- comparison-l1-report:start -->")
print(accepted)
print("<!-- comparison-l1-report:end -->")
PY
)"

if [[ -z "$BLOCK" ]]; then
  echo "fill-comparison-from-report: parser produced no output" >&2
  exit 1
fi

replace_block() {
  local file="$1" start_marker="$2" end_marker="$3"
  python3 -c '
import sys
from pathlib import Path

path = Path(sys.argv[1])
start = sys.argv[2]
end = sys.argv[3]
new_body = sys.stdin.read().rstrip("\n")
if not new_body:
    print(
        f"fill-comparison-from-report: empty replacement for {start!r} in {path}",
        file=sys.stderr,
    )
    sys.exit(1)

text = path.read_text()
s_idx = text.find(start)
e_idx = text.find(end)
if s_idx < 0 or e_idx < 0 or e_idx <= s_idx:
    print(
        f"fill-comparison-from-report: markers {start!r}/{end!r} missing in {path}",
        file=sys.stderr,
    )
    sys.exit(1)

before = text[: s_idx + len(start)]
after = text[e_idx:]
path.write_text(before + "\n" + new_body + "\n" + after)
' "$file" "$start_marker" "$end_marker"
}

extract_section() {
  local marker="$1"
  echo "$BLOCK" | awk -v start="<!-- ${marker}:start -->" -v end="<!-- ${marker}:end -->" '
    $0 == start { on=1; next }
    $0 == end { on=0; next }
    on { print }
  '
}

STATUS_BODY="$(extract_section comparison-l1-status)"
ROWS_BODY="$(extract_section comparison-l1-rows)"
REPORT_BODY="$(extract_section comparison-l1-report)"

if [[ "$DRY_RUN" == "1" ]]; then
  echo "$BLOCK"
  echo ""
  echo "fill-comparison-from-report: dry-run OK (no write to $COMPARISON)" >&2
  exit 0
fi

if [[ "$IN_PLACE" != "1" ]]; then
  echo "$BLOCK"
  exit 0
fi

printf '%s\n' "$STATUS_BODY" | replace_block "$COMPARISON" "<!-- comparison-l1-status:start -->" "<!-- comparison-l1-status:end -->"
printf '%s\n' "$ROWS_BODY" | replace_block "$COMPARISON" "<!-- comparison-l1-rows:start -->" "<!-- comparison-l1-rows:end -->"
printf '%s\n' "$REPORT_BODY" | replace_block "$COMPARISON" "<!-- comparison-l1-report:start -->" "<!-- comparison-l1-report:end -->"

echo "fill-comparison-from-report: updated $COMPARISON from $REPORT" >&2
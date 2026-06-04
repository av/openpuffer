#!/usr/bin/env bash
# Offline tests for scripts/estimate-large-benchmark-cost.sh (JSON ranges vs docs assumptions).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

EST="$ROOT/scripts/estimate-large-benchmark-cost.sh"
chmod +x "$EST"

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

in_range() {
  local val="$1"
  local lo="$2"
  local hi="$3"
  python3 -c "v=float('${val}'); lo=float('${lo}'); hi=float('${hi}');
assert lo <= v <= hi, f'{v} not in [{lo},{hi}]'"
}

json_field() {
  local tier="$1"
  local warm="${2:-0}"
  local jq_expr="$3"
  local warm_args=()
  [[ "$warm" == "1" ]] && warm_args=(--warm)
  "$EST" --tier "$tier" "${warm_args[@]}" --json | jq -r "$jq_expr"
}

echo "==> L1 JSON ranges"
in_range "$(json_field l1 0 '.ec2_hours_g3.min')" 0.5 0.5
in_range "$(json_field l1 0 '.ec2_hours_g3.max')" 1.5 1.5
in_range "$(json_field l1 0 '.s3_puts.min')" 350 450
in_range "$(json_field l1 0 '.s3_puts.max')" 450 550
in_range "$(json_field l1 0 '.s3_gets.min')" 400 500
in_range "$(json_field l1 0 '.s3_gets.max')" 800 900
[[ "$(json_field l1 0 '.tpuf_recall_billed')" == "20" ]] || fail "L1 recall billed"
[[ "$(json_field l1 0 '.tpuf_queries.total')" == "38" ]] || fail "L1 tpuf query total"
[[ "$(json_field l1 0 '.tpuf_write_batches')" == "10" ]] || fail "L1 write batches"

echo "==> L2 JSON ranges"
in_range "$(json_field l2 0 '.ec2_hours_g3.min')" 2.0 2.0
in_range "$(json_field l2 0 '.ec2_hours_g3.max')" 4.0 4.0
in_range "$(json_field l2 0 '.s3_puts.min')" 1950 2050
in_range "$(json_field l2 0 '.s3_puts.max')" 2600 2700
in_range "$(json_field l2 0 '.s3_gets.min')" 1550 1650
in_range "$(json_field l2 0 '.s3_gets.max')" 3200 3400
[[ "$(json_field l2 0 '.tpuf_recall_billed')" == "100" ]] || fail "L2 recall billed"
[[ "$(json_field l2 0 '.tpuf_queries.total')" == "118" ]] || fail "L2 tpuf query total"

echo "==> L3 JSON ranges"
in_range "$(json_field l3 0 '.ec2_hours_g3.min')" 3.0 3.0
in_range "$(json_field l3 0 '.ec2_hours_g3.max')" 6.0 6.0
in_range "$(json_field l3 0 '.s3_puts.min')" 3950 4050
in_range "$(json_field l3 0 '.s3_puts.max')" 5250 5350
in_range "$(json_field l3 0 '.s3_gets.min')" 3050 3150
in_range "$(json_field l3 0 '.s3_gets.max')" 6300 6400
[[ "$(json_field l3 0 '.tpuf_recall_billed')" == "200" ]] || fail "L3 recall billed"
[[ "$(json_field l3 0 '.tpuf_queries.total')" == "218" ]] || fail "L3 tpuf query total"

echo "==> L1 --warm increases tpuf warm line"
[[ "$(json_field l1 1 '.tpuf_queries.warm')" == "20" ]] || fail "warm queries"
[[ "$(json_field l1 1 '.tpuf_queries.total')" == "58" ]] || fail "warm total"

echo "==> text output scopes"
out="$("$EST" --tier l1 --scope aws)"
echo "$out" | grep -q 'EC2 hours G3' || fail 'aws scope'
out="$("$EST" --tier l1 --scope tpuf)"
echo "$out" | grep -q 'write batches' || fail 'tpuf scope'
out="$("$EST" --tier l1)"
echo "$out" | grep -q 'cost-estimate AWS' || fail 'all scope aws'
echo "$out" | grep -q 'cost-estimate turbopuffer' || fail 'all scope tpuf'

echo "==> preflight-aws-ec2 dry-run"
out="$(./scripts/preflight-aws-ec2.sh --dry-run --tier l1)"
echo "$out" | grep -q 'cost-estimate AWS' || fail 'preflight-aws dry-run'

echo "==> run-aws / run-tpuf dry-run include cost lines"
out="$(./scripts/run-aws-large-benchmark.sh --tier l1 --dry-run)"
echo "$out" | grep -q 'cost-estimate AWS' || fail 'run-aws dry-run'
out="$(./scripts/run-tpuf-large-benchmark.sh --tier l1 --dry-run --skip-g2)"
echo "$out" | grep -q 'cost-estimate turbopuffer' || fail 'run-tpuf dry-run'

echo "test_estimate-large-benchmark-cost: OK"
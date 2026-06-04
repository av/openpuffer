# shellcheck shell=bash
# Order-of-magnitude cost / API volume estimates for large-dataset benchmarks (G3 AWS + G4 tpuf).
# Source from preflight and dry-run scripts — do not execute directly.
# Assumptions: docs/BENCHMARKS.md § L2/L3 tiers, § Billing and cost guardrails, § G3 EC2;
#   PLAN_LARGE_DATASET_BENCHMARK.md unresolved-assumptions table; MinIO nightly index_object_count ~275 @ 100k.
#
# Not a quote — use AWS Cost Explorer and the turbopuffer dashboard before spend.

large_benchmark_cost_compute() {
  local tier="$1"
  local warm="${2:-0}"
  python3 - "$tier" "$warm" <<'PY'
import json, math, sys

tier = sys.argv[1].lower()
warm = sys.argv[2] == "1"

TIERS = {
    "l1": {"docs": 100_000, "batches": 10},
    "l2": {"docs": 500_000, "batches": 50},
    "l3": {"docs": 1_000_000, "batches": 100},
}
if tier not in TIERS:
    print(f"unknown tier: {tier}", file=sys.stderr)
    sys.exit(1)

docs = TIERS[tier]["docs"]
batches = TIERS[tier]["batches"]

# m7i.xlarge full G3 wall-clock (hours) — BENCHMARKS.md L2/L3 table / large_preflight_aws_time_estimate
EC2_G3_HOURS = {"l1": (0.5, 1.5), "l2": (2.0, 4.0), "l3": (3.0, 6.0)}
# G4 driver on same host (ingest poll + queries; not USD)
EC2_G4_EXTRA_HOURS = {"l1": (0.25, 0.75), "l2": (0.5, 1.5), "l3": (1.0, 2.5)}

WAL_PUTS_PER_BATCH = 10
INDEX_OBJECTS_PER_1K = 3
INDEXER_GET_PER_PUT = (0.8, 1.5)

COLD_RUNS = 7
COLD_KEYS = (12, 25)
FILTER_HYBRID = 10
RECALL_NUM = 20
SECONDARY_KEYS = (3, 8)

WARM_VECTOR_RUNS = 20

def recall_billed(num=RECALL_NUM):
    chunks = (docs + 99_999) // 100_000
    return max(num, num * chunks)

def s3_puts():
    wal_lo = batches * WAL_PUTS_PER_BATCH
    wal_hi = math.ceil(wal_lo * 1.25)
    idx_lo = (docs // 1000) * INDEX_OBJECTS_PER_1K
    idx_hi = math.ceil(idx_lo * 1.35)
    return wal_lo + idx_lo, wal_hi + idx_hi

def s3_gets(rb):
    idx_puts = (docs // 1000) * INDEX_OBJECTS_PER_1K
    indexer_lo = math.floor(idx_puts * INDEXER_GET_PER_PUT[0])
    indexer_hi = math.ceil(idx_puts * INDEXER_GET_PER_PUT[1])
    cold_lo = COLD_RUNS * COLD_KEYS[0]
    cold_hi = COLD_RUNS * COLD_KEYS[1]
    sec_q = FILTER_HYBRID + rb
    sec_lo = sec_q * SECONDARY_KEYS[0]
    sec_hi = sec_q * SECONDARY_KEYS[1]
    return indexer_lo + cold_lo + sec_lo, indexer_hi + cold_hi + sec_hi

def tpuf_queries(rb, warm_runs):
    cold = COLD_RUNS
    filt = FILTER_HYBRID
    warm = warm_runs
    meta_poll = 1
    return cold + filt + rb + warm + meta_poll

rb = recall_billed()
put_lo, put_hi = s3_puts()
get_lo, get_hi = s3_gets(rb)
g3_lo, g3_hi = EC2_G3_HOURS[tier]
g4_lo, g4_hi = EC2_G4_EXTRA_HOURS[tier]
prog_lo, prog_hi = g3_lo + g4_lo, g3_hi + g4_hi
warm_runs = WARM_VECTOR_RUNS if warm else 0
tq = tpuf_queries(rb, warm_runs)

# Optional USD hints (on-demand us-east-1 list; not a commitment)
EC2_USD_PER_HR = 0.19208  # m7i.xlarge Linux us-east-1 (order-of-magnitude)
S3_PUT_PER_1K = 0.005
S3_GET_PER_1K = 0.0004

out = {
    "tier": tier,
    "docs": docs,
    "batches": batches,
    "warm": warm,
    "ec2_instance": "m7i.xlarge",
    "ec2_hours_g3": {"min": g3_lo, "max": g3_hi},
    "ec2_hours_g4_extra": {"min": g4_lo, "max": g4_hi},
    "ec2_hours_g3_g4": {"min": prog_lo, "max": prog_hi},
    "s3_puts": {"min": put_lo, "max": put_hi},
    "s3_gets": {"min": get_lo, "max": get_hi},
    "tpuf_write_batches": batches,
    "tpuf_recall_billed": rb,
    "tpuf_queries": {
        "cold": COLD_RUNS,
        "filter_hybrid": FILTER_HYBRID,
        "recall_billed": rb,
        "warm": warm_runs,
        "meta_poll": 1,
        "total": tq,
    },
    "usd_hints": {
        "ec2_g3_g4_mid": round(((prog_lo + prog_hi) / 2) * EC2_USD_PER_HR, 2),
        "s3_puts_mid": round(((put_lo + put_hi) / 2) / 1000 * S3_PUT_PER_1K, 4),
        "s3_gets_mid": round(((get_lo + get_hi) / 2) / 1000 * S3_GET_PER_1K, 4),
        "note": "EC2+S3 list prices only; excludes tpuf dashboard billing",
    },
}
print(json.dumps(out))
PY
}

large_benchmark_cost_print() {
  local tier="$1"
  local warm="${2:-0}"
  local scope="${3:-all}"
  local data
  data="$(large_benchmark_cost_compute "$tier" "$warm")"

  case "$scope" in
    aws|tpuf|all) ;;
    *)
      echo "estimate-large-benchmark-cost: unknown scope ${scope} (use aws|tpuf|all)" >&2
      return 1
      ;;
  esac

  python3 - "$data" "$scope" <<'PY'
import json, sys

d = json.loads(sys.argv[1])
scope = sys.argv[2]
tier = d["tier"]
docs = d["docs"]

def line(title):
    print(title)

if scope in ("aws", "all"):
    line(f"cost-estimate AWS (tier={tier}, {docs:,} docs, {d['ec2_instance']}; order-of-magnitude):")
    g3 = d["ec2_hours_g3"]
    g4 = d["ec2_hours_g4_extra"]
    both = d["ec2_hours_g3_g4"]
    puts = d["s3_puts"]
    gets = d["s3_gets"]
    usd = d["usd_hints"]
    print(f"  EC2 hours G3 (ingest+index+bench): {g3['min']:.2f}–{g3['max']:.2f} h")
    print(f"  EC2 hours G4 add-on (same host):    {g4['min']:.2f}–{g4['max']:.2f} h")
    print(f"  EC2 hours G3+G4 combined:         {both['min']:.2f}–{both['max']:.2f} h")
    print(f"  S3 PUT requests (WAL+index build): {puts['min']:,}–{puts['max']:,}")
    print(f"  S3 GET requests (indexer+bench):   {gets['min']:,}–{gets['max']:,}")
    print(
        f"  USD hints (list, us-east-1): EC2 ~${usd['ec2_g3_g4_mid']:.2f}, "
        f"S3 PUT ~${usd['s3_puts_mid']:.4f}, GET ~${usd['s3_gets_mid']:.4f} — {usd['note']}"
    )

if scope in ("tpuf", "all"):
    tq = d["tpuf_queries"]
    line(f"cost-estimate turbopuffer (tier={tier}, docs={docs:,}; API volume not USD):")
    print(f"  write batches (~10k rows): ~{d['tpuf_write_batches']} namespace.write calls")
    print(f"  cold vector queries: {tq['cold']}")
    print(f"  filter + hybrid queries: {tq['filter_hybrid']} (1× each)")
    print(f"  recall billed queries (num=20): ~{d['tpuf_recall_billed']}")
    print(f"  warm vector queries: {tq['warm']}")
    print(f"  order-of-magnitude query calls (excl. ingest index polls): ~{tq['total']}")
    print("  guardrails: start L1; TURBOPUFFER_BENCH_DELETE_FIRST=1; avoid SKIP_DELETE unless debugging")

if scope == "all":
    print("  see docs/BENCHMARKS.md § Billing and cost guardrails")
PY
}

# Back-compat: tpuf-only block for preflight-tpuf.sh
large_preflight_tpuf_cost_estimate() {
  local tier="$1"
  local warm="${2:-0}"
  large_benchmark_cost_print "$tier" "$warm" tpuf
}
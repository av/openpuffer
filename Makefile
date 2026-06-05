# Large-dataset benchmark program — convenience targets (see benchmarks/README.md).
.PHONY: help bench-verify bench-verify-op-scaling bench-dry-run bench-g2-minio bench-preflight \
	bench-op-scaling bench-compare-tpuf bench-scaling-full print-scaling-verdict

# Extra flags for verify, e.g. make bench-verify VERIFY_FLAGS="--skip-l2-l3 --skip-facts"
VERIFY_FLAGS ?=

# Extra flags for preflight, e.g. make bench-preflight PREFLIGHT_FLAGS="--live --tier l1"
PREFLIGHT_FLAGS ?=

# Optional tier subset for bench-op-scaling, e.g. OP_SCALING_TIERS="10k 50k 100k warm 100k-warm"
OP_SCALING_TIERS ?=

help:
	@echo "Large-dataset benchmark targets:"
	@echo "  make bench-verify     Offline harness gate (pytest, schemas, dry-runs, facts)"
	@echo "  make bench-verify-op-scaling  op-vs-tpuf scaling smoke (committed JSON only)"
	@echo "  make bench-dry-run    Harness dry-run only (L1–L3; no pytest/cargo/facts)"
	@echo "  make bench-g2-minio   G2 MinIO correctness gates (Docker; slow)"
	@echo "  make bench-preflight  G3+G4+overlap preflights (offline default; see PREFLIGHT_FLAGS)"
	@echo "  make bench-op-scaling MinIO op-scaling tiers (10k/50k/100k/warm; slow)"
	@echo "  make bench-compare-tpuf  Extrapolate op-scaling vs tpuf official 10M ref"
	@echo "  make bench-scaling-full  Full op-vs-tpuf sweep + compare + verify + verdict (~25–35 min)"
	@echo "  make print-scaling-verdict  One-paragraph operator verdict (offline JSON)"
	@echo ""
	@echo "Options:"
	@echo "  VERIFY_FLAGS='--skip-l2-l3 --skip-op-scaling'   Passed to verify-large-benchmark-program.sh"
	@echo "  PREFLIGHT_FLAGS='--live --tier l1'   Passed to preflight-large-benchmark-all.sh"

bench-verify:
	./scripts/verify-large-benchmark-program.sh $(VERIFY_FLAGS)

bench-verify-op-scaling:
	./scripts/verify-op-scaling-comparison.sh

bench-dry-run:
	@echo "==> L1 harness dry-run (ingest, bench, G3, G4, program, overlap, tpuf driver)"
	@./scripts/ingest-large.sh --tier l1 --dry-run >/dev/null
	@./scripts/bench-large.sh --tier l1 --dry-run >/dev/null
	@./scripts/run-aws-large-benchmark.sh --tier l1 --dry-run >/dev/null
	@./scripts/run-tpuf-large-benchmark.sh --tier l1 --dry-run --skip-g2 >/dev/null
	@python3 benchmarks/tpuf_driver/run_benchmark.py --tier l1 --dry-run >/dev/null
	@./scripts/run-id-overlap-spotcheck.sh --tier l1 --dry-run >/dev/null
	@OPENPUFFER_REPORT_OUTPUT="/tmp/openpuffer-make-dry-run-l1.md" \
		OPENPUFFER_REPORT_DATE=2099-06-04 \
		./scripts/run-large-benchmark-program.sh --tier l1 --dry-run --skip-g2 >/dev/null
	@echo "==> L2/L3 harness dry-run"
	@./scripts/test_l2-l3-harness-dry-run.sh
	@echo "bench-dry-run: OK"

bench-g2-minio:
	./scripts/run-minio-correctness-gates.sh

bench-preflight:
	./scripts/preflight-large-benchmark-all.sh $(PREFLIGHT_FLAGS)

bench-op-scaling:
ifdef OP_SCALING_TIERS
	./scripts/run-op-scaling-benchmark.sh $(OP_SCALING_TIERS)
else
	./scripts/run-op-scaling-benchmark.sh
endif

bench-compare-tpuf:
	./scripts/compare-op-scaling-to-tpuf.sh

# Full openpuffer vs turbopuffer scaling pipeline (Docker + release build).
# Wall time ~25–35 min on a quiet host (10k/50k/100k cold + 10k/100k warm; excludes synthetic128).
bench-scaling-full:
	$(MAKE) bench-op-scaling OP_SCALING_TIERS="10k 50k 100k warm 100k-warm"
	$(MAKE) bench-compare-tpuf
	$(MAKE) bench-verify-op-scaling
	$(MAKE) print-scaling-verdict

print-scaling-verdict:
	./scripts/print-scaling-verdict.sh
//! S3 round-trip integration tests against MinIO via testcontainers (or docker-compose).
//!
//! Validates: upsert_rows → vector top-k + BM25 FTS + hybrid query; data survives
//! serve process restart with only S3 backing.

#![cfg(feature = "integration")]

#[test]
#[ignore = "requires MinIO via testcontainers; run: cargo test -F integration -- --ignored"]
fn roundtrip_write_vector_fts_hybrid_minio() {
    // Placeholder — implemented in build phase.
    unimplemented!("MinIO integration test");
}
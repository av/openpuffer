//! Per-namespace mutex serializing `meta.json` CAS commits (WAL + indexer).
//!
//! Without this, concurrent `append_wal` calls can assign the same WAL sequence and
//! overwrite each other's segments; indexer CAS can also clobber `wal_commit_seq` if
//! it applies index updates to a stale meta snapshot.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{Mutex, RwLock};

static LOCKS: std::sync::OnceLock<RwLock<HashMap<String, Arc<Mutex<()>>>>> = std::sync::OnceLock::new();

fn locks() -> &'static RwLock<HashMap<String, Arc<Mutex<()>>>> {
    LOCKS.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Returns the commit mutex for `namespace` (created on first use).
pub async fn namespace_commit_lock(namespace: &str) -> Arc<Mutex<()>> {
    {
        let guard = locks().read().await;
        if let Some(m) = guard.get(namespace) {
            return m.clone();
        }
    }
    let mut guard = locks().write().await;
    guard
        .entry(namespace.to_string())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}
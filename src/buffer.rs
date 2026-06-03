//! Per-namespace write buffer with turbopuffer-style group commit.
//!
//! Writes are batched in memory and flushed as a single [`WalEntry`] when either:
//! - [`WriteBufferConfig::max_delay`] elapses (default 1s), or
//! - [`WriteBufferConfig::max_batch_ops`] upserts+deletes is reached.
//!
//! **Strong consistency:** HTTP ACK waits until [`crate::namespace::append_wal`] completes —
//! the WAL object is on S3 and `meta.json` CAS succeeded before waiters are released.

use crate::indexer::BackgroundIndexer;
use crate::models::{Document, WriteStats};
use crate::namespace::append_wal;
use crate::wal::WalEntry;
use anyhow::Result;
use serde_json::Value;
use aws_sdk_s3::Client;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, Mutex, RwLock};
use tokio::task::AbortHandle;
use tokio::time::sleep;

/// Group-commit tuning (turbopuffer ~1s batching for v1).
#[derive(Debug, Clone)]
pub struct WriteBufferConfig {
    pub max_delay: Duration,
    pub max_batch_ops: usize,
}

impl Default for WriteBufferConfig {
    fn default() -> Self {
        Self {
            max_delay: Duration::from_secs(1),
            max_batch_ops: 512,
        }
    }
}

/// Result of a durable group-commit flush.
#[derive(Debug, Clone)]
pub struct CommittedBatch {
    pub seq: u64,
    pub entry: WalEntry,
    pub stats: WriteStats,
}

struct WriteWaiter {
    stats: WriteStats,
    tx: oneshot::Sender<Result<CommittedBatch>>,
}

struct BufferState {
    upserts: Vec<Document>,
    patches: Vec<Document>,
    deletes: Vec<String>,
    schema_patch: Option<Value>,
    waiters: Vec<WriteWaiter>,
    timer: Option<AbortHandle>,
}

struct NamespaceBuffer {
    state: Mutex<BufferState>,
    /// Serializes `flush_namespace` so empty flushes do not race in-flight commits.
    flush_lock: Mutex<()>,
}

/// Shared write buffers keyed by namespace.
pub struct WriteBufferManager {
    client: Client,
    bucket: String,
    config: WriteBufferConfig,
    buffers: Arc<RwLock<HashMap<String, Arc<NamespaceBuffer>>>>,
    background_indexer: Option<Arc<BackgroundIndexer>>,
}

impl WriteBufferManager {
    pub fn new(
        client: Client,
        bucket: String,
        config: WriteBufferConfig,
        background_indexer: Option<Arc<BackgroundIndexer>>,
    ) -> Self {
        Self {
            client,
            bucket,
            config,
            buffers: Arc::new(RwLock::new(HashMap::new())),
            background_indexer,
        }
    }

    /// Enqueue upserts/deletes; returns after the batch containing this write is durable on S3.
    pub async fn write(
        &self,
        namespace: &str,
        upserts: Vec<Document>,
        patches: Vec<Document>,
        deletes: Vec<String>,
        schema_patch: Option<Value>,
    ) -> Result<CommittedBatch> {
        let buf = self.buffer_for(namespace).await;
        let (tx, rx) = oneshot::channel();
        let req_stats = WriteStats {
            rows_upserted: upserts.len() as u64,
            rows_patched: patches.len() as u64,
            rows_deleted: deletes.len() as u64,
        };

        let flush_now = {
            let mut st = buf.state.lock().await;
            st.upserts.extend(upserts);
            st.patches.extend(patches);
            st.deletes.extend(deletes);
            if let Some(patch) = schema_patch {
                st.schema_patch = Some(match st.schema_patch.take() {
                    Some(existing) => crate::schema::merge_schema(&existing, &patch),
                    None => patch,
                });
            }
            st.waiters.push(WriteWaiter {
                stats: req_stats,
                tx,
            });
            let ops = st.upserts.len() + st.patches.len() + st.deletes.len();
            let schema_only = ops == 0 && st.schema_patch.is_some();
            if ops >= self.config.max_batch_ops || schema_only {
                true
            } else if st.timer.is_none() {
                let mgr = Arc::new(self.clone_inner());
                let ns = namespace.to_string();
                let handle = tokio::spawn(async move {
                    sleep(mgr.config.max_delay).await;
                    if let Err(e) = mgr.flush_namespace(&ns).await {
                        tracing::error!("group-commit flush {ns}: {e:#}");
                    }
                });
                st.timer = Some(handle.abort_handle());
                false
            } else {
                false
            }
        };

        if flush_now {
            {
                let mut st = buf.state.lock().await;
                if let Some(t) = st.timer.take() {
                    t.abort();
                }
            }
            self.flush_namespace(namespace).await?;
        }

        rx.await
            .map_err(|_| anyhow::anyhow!("write buffer flush cancelled"))?
    }

    async fn buffer_for(&self, namespace: &str) -> Arc<NamespaceBuffer> {
        {
            let guard = self.buffers.read().await;
            if let Some(b) = guard.get(namespace) {
                return b.clone();
            }
        }
        let mut guard = self.buffers.write().await;
        guard
            .entry(namespace.to_string())
            .or_insert_with(|| {
                Arc::new(NamespaceBuffer {
                    state: Mutex::new(BufferState {
                        upserts: Vec::new(),
                        patches: Vec::new(),
                        deletes: Vec::new(),
                        schema_patch: None,
                        waiters: Vec::new(),
                        timer: None,
                    }),
                    flush_lock: Mutex::new(()),
                })
            })
            .clone()
    }

    fn clone_inner(&self) -> Self {
        Self {
            client: self.client.clone(),
            bucket: self.bucket.clone(),
            config: self.config.clone(),
            buffers: self.buffers.clone(),
            background_indexer: self.background_indexer.clone(),
        }
    }

    /// Flush all namespaces (e.g. graceful shutdown).
    pub async fn flush_all(&self) -> Result<()> {
        let names: Vec<String> = self.buffers.read().await.keys().cloned().collect();
        for ns in names {
            self.flush_namespace(&ns).await?;
        }
        Ok(())
    }

    async fn flush_namespace(&self, namespace: &str) -> Result<()> {
        let buf = {
            let guard = self.buffers.read().await;
            guard.get(namespace).cloned()
        };
        let Some(buf) = buf else {
            return Ok(());
        };

        let _flush_guard = buf.flush_lock.lock().await;

        let (upserts, patches, deletes, schema_patch, waiters) = {
            let mut st = buf.state.lock().await;
            if st.upserts.is_empty()
                && st.patches.is_empty()
                && st.deletes.is_empty()
                && st.schema_patch.is_none()
            {
                return Ok(());
            }
            let _ = st.timer.take();
            (
                std::mem::take(&mut st.upserts),
                std::mem::take(&mut st.patches),
                std::mem::take(&mut st.deletes),
                st.schema_patch.take(),
                std::mem::take(&mut st.waiters),
            )
        };

        let result: Result<(u64, WalEntry)> = async {
            let entry = WalEntry::from_write(upserts, patches, deletes)?;
            let seq = append_wal(
                &self.client,
                &self.bucket,
                namespace,
                entry.clone(),
                schema_patch.as_ref(),
            )
            .await?;
            Ok((seq, entry))
        }
        .await;

        match result {
            Ok((seq, entry)) => {
                for w in waiters {
                    let batch = CommittedBatch {
                        seq,
                        entry: entry.clone(),
                        stats: w.stats.clone(),
                    };
                    let _ = w.tx.send(Ok(batch));
                }
                if let Some(indexer) = &self.background_indexer {
                    indexer.wake(namespace).await;
                }
                Ok(())
            }
            Err(err) => {
                let msg = format!("{err:#}");
                for w in waiters {
                    let _ = w.tx.send(Err(anyhow::anyhow!("{msg}")));
                }
                Err(err)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::Document;

    /// Simulates group-commit: three logical writes merged into one `WalEntry`.
    #[test]
    fn buffer_batches_multiple_writes_into_one_wal_entry() {
        let mut pending_upserts = Vec::new();
        let pending_deletes: Vec<String> = Vec::new();

        for i in 0..3 {
            pending_upserts.push(Document {
                id: format!("doc-{i}"),
                attributes: Default::default(),
            });
        }

        let entry = WalEntry::from_write(pending_upserts, vec![], pending_deletes).unwrap();
        assert_eq!(entry.upserts.len(), 3);
        assert!(entry.deletes.is_empty());
    }

    #[test]
    fn default_config_one_second_delay() {
        let cfg = WriteBufferConfig::default();
        assert_eq!(cfg.max_delay, Duration::from_secs(1));
        assert_eq!(cfg.max_batch_ops, 512);
    }
}
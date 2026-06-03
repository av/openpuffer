//! Per-namespace write buffer with turbopuffer-style group commit.
//!
//! Writes are batched in memory and flushed as a single [`WalEntry`] when either:
//! - [`WriteBufferConfig::max_delay`] elapses (default 1s), or
//! - [`WriteBufferConfig::max_batch_ops`] upserts+deletes is reached.
//!
//! **Strong consistency:** HTTP ACK waits until [`crate::namespace::append_wal`] completes —
//! the WAL object is on S3 and `meta.json` CAS succeeded before waiters are released.

use crate::models::Document;
use crate::namespace::append_wal;
use crate::wal::WalEntry;
use anyhow::Result;
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
}

struct BufferState {
    upserts: Vec<Document>,
    deletes: Vec<String>,
    waiters: Vec<oneshot::Sender<Result<CommittedBatch>>>,
    timer: Option<AbortHandle>,
}

struct NamespaceBuffer {
    state: Mutex<BufferState>,
}

/// Shared write buffers keyed by namespace.
pub struct WriteBufferManager {
    client: Client,
    bucket: String,
    config: WriteBufferConfig,
    buffers: Arc<RwLock<HashMap<String, Arc<NamespaceBuffer>>>>,
}

impl WriteBufferManager {
    pub fn new(client: Client, bucket: String, config: WriteBufferConfig) -> Self {
        Self {
            client,
            bucket,
            config,
            buffers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Enqueue upserts/deletes; returns after the batch containing this write is durable on S3.
    pub async fn write(
        &self,
        namespace: &str,
        upserts: Vec<Document>,
        deletes: Vec<String>,
    ) -> Result<CommittedBatch> {
        let buf = self.buffer_for(namespace).await;
        let (tx, rx) = oneshot::channel();
        let flush_now = {
            let mut st = buf.state.lock().await;
            st.upserts.extend(upserts);
            st.deletes.extend(deletes);
            st.waiters.push(tx);
            let ops = st.upserts.len() + st.deletes.len();
            if ops >= self.config.max_batch_ops {
                true
            } else if st.timer.is_none() {
                let mgr = Arc::new(self.clone_inner());
                let ns = namespace.to_string();
                let handle = tokio::spawn(async move {
                    sleep(mgr.config.max_delay).await;
                    let _ = mgr.flush_namespace(&ns).await;
                });
                st.timer = Some(handle.abort_handle());
                false
            } else {
                false
            }
        };

        if flush_now {
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
                        deletes: Vec::new(),
                        waiters: Vec::new(),
                        timer: None,
                    }),
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

        let (upserts, deletes, waiters) = {
            let mut st = buf.state.lock().await;
            if st.upserts.is_empty() && st.deletes.is_empty() {
                return Ok(());
            }
            if let Some(t) = st.timer.take() {
                t.abort();
            }
            (
                std::mem::take(&mut st.upserts),
                std::mem::take(&mut st.deletes),
                std::mem::take(&mut st.waiters),
            )
        };

        let entry = WalEntry::from_write(upserts, deletes)?;
        let docs = entry.clone().into_documents()?;
        let seq = append_wal(
            &self.client,
            &self.bucket,
            namespace,
            docs,
            entry.deletes.clone(),
        )
        .await?;

        // Index WAL tail synchronously so queries can use FTS + unindexed tail (v1).
        if let Err(e) =
            crate::indexer::index_namespace(&self.client, &self.bucket, namespace).await
        {
            tracing::warn!("indexer after flush for {namespace}: {e:#}");
        }

        let committed = CommittedBatch {
            seq,
            entry: entry.clone(),
        };
        for w in waiters {
            let _ = w.send(Ok(committed.clone()));
        }
        Ok(())
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

        let entry = WalEntry::from_write(pending_upserts, pending_deletes).unwrap();
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
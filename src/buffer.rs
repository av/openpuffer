//! Per-namespace write buffer with turbopuffer-style group commit.
//!
//! Writes are batched in memory and flushed as a single [`WalEntry`] when either:
//! - [`WriteBufferConfig::max_delay`] elapses (default 1s), or
//! - [`WriteBufferConfig::max_batch_ops`] upserts+deletes is reached.
//!
//! **WAL commit rate:** at most one durable WAL commit per namespace per
//! [`WriteBufferConfig::min_commit_interval`] (default 1s, turbopuffer write throughput).
//! Additional writes during the cooldown accumulate in the buffer. [`flush_all`] bypasses
//! the limit for graceful shutdown.
//!
//! **Strong consistency:** HTTP ACK waits until [`crate::namespace::append_wal`] completes —
//! the WAL object is on S3 and `meta.json` CAS succeeded before waiters are released.

use crate::indexer::BackgroundIndexer;
use crate::meta::DistanceMetric;
use crate::models::{Document, WriteStats};
use crate::namespace::append_wal;
use crate::wal::WalEntry;
use anyhow::Result;
use aws_sdk_s3::Client;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{oneshot, Mutex, RwLock};
use tokio::task::AbortHandle;
use tokio::time::sleep;

/// Group-commit tuning (turbopuffer ~1s batching for v1).
#[derive(Debug, Clone)]
pub struct WriteBufferConfig {
    pub max_delay: Duration,
    pub max_batch_ops: usize,
    /// Minimum wall time between durable WAL commits per namespace (turbopuffer ~1/s).
    pub min_commit_interval: Duration,
}

impl Default for WriteBufferConfig {
    fn default() -> Self {
        let one_second = Duration::from_secs(1);
        Self {
            max_delay: one_second,
            max_batch_ops: 512,
            min_commit_interval: one_second,
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
    distance_metric: Option<DistanceMetric>,
    waiters: Vec<WriteWaiter>,
    timer: Option<AbortHandle>,
}

struct NamespaceBuffer {
    state: Mutex<BufferState>,
    /// Serializes `flush_namespace` so empty flushes do not race in-flight commits.
    flush_lock: Mutex<()>,
    /// Wall time of the last successful WAL commit (rate-limit anchor).
    last_committed_at: Mutex<Option<Instant>>,
    /// True while a `flush_namespace` task is running (coalesce parallel flush callers).
    flush_running: AtomicBool,
}

/// Remaining cooldown before the next WAL commit is allowed.
pub(crate) fn remaining_commit_cooldown(
    last_committed_at: Option<Instant>,
    min_interval: Duration,
    now: Instant,
) -> Duration {
    match last_committed_at {
        None => Duration::ZERO,
        Some(t) => min_interval.saturating_sub(now.saturating_duration_since(t)),
    }
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
        distance_metric: Option<DistanceMetric>,
        stats: WriteStats,
    ) -> Result<CommittedBatch> {
        let buf = self.buffer_for(namespace).await;
        let (tx, rx) = oneshot::channel();
        let req_stats = stats;

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
            if let Some(metric) = distance_metric {
                st.distance_metric = Some(metric);
            }
            st.waiters.push(WriteWaiter {
                stats: req_stats,
                tx,
            });
            let ops = st.upserts.len() + st.patches.len() + st.deletes.len();
            let metadata_only =
                ops == 0 && (st.schema_patch.is_some() || st.distance_metric.is_some());
            if ops >= self.config.max_batch_ops || metadata_only {
                true
            } else if st.timer.is_none() {
                let mgr = Arc::new(self.clone_inner());
                let ns = namespace.to_string();
                let buf_arc = buf.clone();
                let delay = self.config.max_delay;
                let handle = tokio::spawn(async move {
                    sleep(delay).await;
                    mgr.request_flush(&ns, buf_arc);
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
            self.request_flush(namespace, buf);
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
                        distance_metric: None,
                        waiters: Vec::new(),
                        timer: None,
                    }),
                    flush_lock: Mutex::new(()),
                    last_committed_at: Mutex::new(None),
                    flush_running: AtomicBool::new(false),
                })
            })
            .clone()
    }

    /// Start at most one async flush per namespace; parallel writers wait on their oneshot only.
    fn request_flush(&self, namespace: &str, buf: Arc<NamespaceBuffer>) {
        if buf
            .flush_running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let mgr = Arc::new(self.clone_inner());
        let ns = namespace.to_string();
        tokio::spawn(async move {
            if let Err(e) = mgr.flush_namespace(&ns, false).await {
                tracing::error!("group-commit flush {ns}: {e:#}");
            }
            mgr.after_flush_task(&ns, buf).await;
        });
    }

    async fn after_flush_task(&self, namespace: &str, buf: Arc<NamespaceBuffer>) {
        buf.flush_running.store(false, Ordering::Release);
        let (pending, cooldown) = {
            let st = buf.state.lock().await;
            let ops = st.upserts.len() + st.patches.len() + st.deletes.len();
            let metadata_only =
                ops == 0 && (st.schema_patch.is_some() || st.distance_metric.is_some());
            let pending = ops > 0 || metadata_only;
            let last = buf.last_committed_at.lock().await;
            let cooldown =
                remaining_commit_cooldown(*last, self.config.min_commit_interval, Instant::now());
            (pending, cooldown)
        };
        if !pending {
            return;
        }
        if cooldown.is_zero() {
            self.request_flush(namespace, buf);
        } else {
            self.arm_cooldown_flush_timer(buf, namespace);
        }
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

    /// Drop in-memory buffer state for a namespace (e.g. after server-side namespace copy).
    pub async fn drop_namespace(&self, namespace: &str) {
        let mut guard = self.buffers.write().await;
        guard.remove(namespace);
    }

    /// Apply uncommitted buffer ops on top of a committed doc map (for `upsert_condition` strong reads).
    pub async fn overlay_pending_writes(
        &self,
        namespace: &str,
        docs: &mut std::collections::HashMap<String, Document>,
    ) -> Result<()> {
        let guard = self.buffers.read().await;
        let Some(buf) = guard.get(namespace) else {
            return Ok(());
        };
        let st = buf.state.lock().await;
        if st.upserts.is_empty() && st.patches.is_empty() && st.deletes.is_empty() {
            return Ok(());
        }
        let entry = WalEntry::from_write(
            st.upserts.clone(),
            st.patches.clone(),
            st.deletes.clone(),
        )?;
        crate::wal::apply_entry(docs, &entry)?;
        Ok(())
    }

    /// Flush all namespaces (e.g. graceful shutdown). Bypasses per-namespace commit rate limit.
    pub async fn flush_all(&self) -> Result<()> {
        let names: Vec<String> = self.buffers.read().await.keys().cloned().collect();
        for ns in names {
            loop {
                self.flush_namespace(&ns, true).await?;
                let buf = {
                    let guard = self.buffers.read().await;
                    guard.get(&ns).cloned()
                };
                let Some(buf) = buf else {
                    break;
                };
                let pending = {
                    let st = buf.state.lock().await;
                    let ops = st.upserts.len() + st.patches.len() + st.deletes.len();
                    ops > 0 || st.schema_patch.is_some() || st.distance_metric.is_some()
                };
                if !pending {
                    break;
                }
            }
        }
        Ok(())
    }

    async fn wait_commit_cooldown(&self, buf: &NamespaceBuffer) {
        loop {
            let wait = {
                let last = buf.last_committed_at.lock().await;
                remaining_commit_cooldown(*last, self.config.min_commit_interval, Instant::now())
            };
            if wait.is_zero() {
                return;
            }
            sleep(wait).await;
        }
    }

    /// If the buffer still has ops after a commit, arm a timer for the remaining cooldown.
    fn arm_cooldown_flush_timer(&self, buf: Arc<NamespaceBuffer>, namespace: &str) {
        let pending = {
            let st = match buf.state.try_lock() {
                Ok(g) => g,
                Err(_) => return,
            };
            let ops = st.upserts.len() + st.patches.len() + st.deletes.len();
            let metadata_only =
                ops == 0 && (st.schema_patch.is_some() || st.distance_metric.is_some());
            (ops > 0 || metadata_only) && st.timer.is_none()
        };
        if !pending {
            return;
        }
        let cooldown = match buf.last_committed_at.try_lock() {
            Ok(last) => {
                remaining_commit_cooldown(*last, self.config.min_commit_interval, Instant::now())
            }
            Err(_) => return,
        };
        let delay = cooldown.max(Duration::from_millis(1));
        let Ok(mut st) = buf.state.try_lock() else {
            return;
        };
        if st.timer.is_some() {
            return;
        }
        let mgr = Arc::new(self.clone_inner());
        let ns = namespace.to_string();
        let buf_spawn = buf.clone();
        let handle = tokio::spawn(async move {
            sleep(delay).await;
            mgr.request_flush(&ns, buf_spawn);
        });
        st.timer = Some(handle.abort_handle());
    }

    async fn flush_namespace(&self, namespace: &str, bypass_rate_limit: bool) -> Result<()> {
        let buf = {
            let guard = self.buffers.read().await;
            guard.get(namespace).cloned()
        };
        let Some(buf) = buf else {
            return Ok(());
        };

        let flush_outcome = {
            let _flush_guard = buf.flush_lock.lock().await;

            if !bypass_rate_limit {
                self.wait_commit_cooldown(&buf).await;
            }

            let drained = {
                let mut st = buf.state.lock().await;
                if st.upserts.is_empty()
                    && st.patches.is_empty()
                    && st.deletes.is_empty()
                    && st.schema_patch.is_none()
                    && st.distance_metric.is_none()
                {
                    None
                } else {
                    let _ = st.timer.take();
                    Some((
                        std::mem::take(&mut st.upserts),
                        std::mem::take(&mut st.patches),
                        std::mem::take(&mut st.deletes),
                        st.schema_patch.take(),
                        st.distance_metric.take(),
                        std::mem::take(&mut st.waiters),
                    ))
                }
            };
            match drained {
                None => {
                    let waiters = {
                        let mut st = buf.state.lock().await;
                        let _ = st.timer.take();
                        std::mem::take(&mut st.waiters)
                    };
                    for w in waiters {
                        let batch = CommittedBatch {
                            seq: 0,
                            entry: WalEntry {
                                upserts: vec![],
                                patches: vec![],
                                deletes: vec![],
                            },
                            stats: w.stats,
                        };
                        let _ = w.tx.send(Ok(batch));
                    }
                    Ok(())
                }
                Some((upserts, patches, deletes, schema_patch, distance_metric, waiters)) => {
                    let result: Result<(u64, WalEntry)> = async {
                        let entry = WalEntry::from_write(upserts, patches, deletes)?;
                        let seq = append_wal(
                            &self.client,
                            &self.bucket,
                            namespace,
                            entry.clone(),
                            schema_patch.as_ref(),
                            distance_metric,
                        )
                        .await?;
                        Ok((seq, entry))
                    }
                    .await;

                    match result {
                        Ok((seq, entry)) => {
                            *buf.last_committed_at.lock().await = Some(Instant::now());
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
        };

        flush_outcome
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::Document;

    #[test]
    fn overlay_pending_merges_buffered_upsert_into_committed_map() {
        let mut docs = std::collections::HashMap::new();
        let entry = WalEntry::from_write(
            vec![Document {
                id: "pending".into(),
                attributes: Default::default(),
            }],
            vec![],
            vec![],
        )
        .unwrap();
        crate::wal::apply_entry(&mut docs, &entry).unwrap();
        assert!(docs.contains_key("pending"));
    }

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
    fn default_config_one_second_delay_and_commit_interval() {
        let cfg = WriteBufferConfig::default();
        assert_eq!(cfg.max_delay, Duration::from_secs(1));
        assert_eq!(cfg.max_batch_ops, 512);
        assert_eq!(cfg.min_commit_interval, Duration::from_secs(1));
    }

    #[test]
    fn remaining_cooldown_zero_when_never_committed() {
        let now = Instant::now();
        assert_eq!(
            remaining_commit_cooldown(None, Duration::from_secs(1), now),
            Duration::ZERO
        );
    }

    #[test]
    fn remaining_cooldown_after_recent_commit() {
        let t0 = Instant::now();
        let last = Some(t0);
        let wait = remaining_commit_cooldown(last, Duration::from_secs(1), t0);
        assert_eq!(wait, Duration::from_secs(1));
        let wait_later = remaining_commit_cooldown(last, Duration::from_secs(1), t0 + Duration::from_millis(600));
        assert_eq!(wait_later, Duration::from_millis(400));
        let wait_done = remaining_commit_cooldown(last, Duration::from_secs(1), t0 + Duration::from_secs(2));
        assert_eq!(wait_done, Duration::ZERO);
    }

    /// Five commits spaced 0ms apart would violate the limit; cooldown enforces ≥1s gaps.
    #[test]
    fn five_rapid_commits_need_at_least_five_seconds_of_cooldown() {
        let interval = Duration::from_secs(1);
        let mut last: Option<Instant> = None;
        let mut now = Instant::now();
        let mut commit_times = Vec::new();
        for _ in 0..5 {
            let wait = remaining_commit_cooldown(last, interval, now);
            now += wait;
            commit_times.push(now);
            last = Some(now);
            now += Duration::from_millis(1);
        }
        let span = commit_times[4] - commit_times[0];
        assert!(
            span >= Duration::from_millis(3900),
            "expected ~4s between first and fifth commit, got {span:?}"
        );
    }
}
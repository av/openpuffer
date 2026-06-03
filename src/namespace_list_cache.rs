//! In-process cache for `ListObjectsV2` namespace listing (common prefixes under `openpuffer/`).

use std::time::{Duration, Instant};

/// Default TTL for namespace list results (turbopuffer list is relatively stable).
pub const DEFAULT_NAMESPACE_LIST_TTL: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
struct CachedList {
    namespaces: Vec<String>,
    cached_at: Instant,
}

/// TTL cache for sorted namespace names returned by `Storage::list_namespaces`.
#[derive(Debug)]
pub struct NamespaceListCache {
    ttl: Duration,
    entry: Option<CachedList>,
}

impl NamespaceListCache {
    pub fn new(ttl: Duration) -> Self {
        Self { ttl, entry: None }
    }

    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    /// Returns a clone of the cached list when present and not expired.
    pub fn get(&self) -> Option<Vec<String>> {
        let entry = self.entry.as_ref()?;
        if entry.cached_at.elapsed() >= self.ttl {
            return None;
        }
        Some(entry.namespaces.clone())
    }

    pub fn set(&mut self, namespaces: Vec<String>) {
        self.entry = Some(CachedList {
            namespaces,
            cached_at: Instant::now(),
        });
    }

    pub fn invalidate(&mut self) {
        self.entry = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn cache_hit_returns_list_without_expiry() {
        let mut cache = NamespaceListCache::new(Duration::from_secs(30));
        cache.set(vec!["a".into(), "b".into()]);
        let hit = cache.get().expect("cache hit");
        assert_eq!(hit, vec!["a", "b"]);
        assert_eq!(cache.get().expect("second hit"), hit);
    }

    #[test]
    fn cache_miss_after_invalidate() {
        let mut cache = NamespaceListCache::new(Duration::from_secs(30));
        cache.set(vec!["ns".into()]);
        assert!(cache.get().is_some());
        cache.invalidate();
        assert!(cache.get().is_none());
    }

    #[test]
    fn cache_miss_after_ttl_expires() {
        let mut cache = NamespaceListCache::new(Duration::from_millis(5));
        cache.set(vec!["x".into()]);
        assert!(cache.get().is_some());
        thread::sleep(Duration::from_millis(10));
        assert!(cache.get().is_none());
    }
}
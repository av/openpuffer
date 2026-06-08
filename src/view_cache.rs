//! LRU cache of in-process [`NamespaceView`] instances (turbopuffer warm / pin analogue).

use crate::view::NamespaceView;
use std::collections::{HashMap, VecDeque};

/// Default maximum namespaces kept hot in memory after warm or query.
pub const DEFAULT_MAX_PINNED: usize = 32;

/// In-memory namespace views with LRU eviction when over capacity.
#[derive(Debug)]
pub struct ViewCache {
    max: usize,
    views: HashMap<String, NamespaceView>,
    /// Front = least recently used; back = most recently used.
    order: VecDeque<String>,
}

impl ViewCache {
    pub fn new(max: usize) -> Self {
        Self {
            max: max.max(1),
            views: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    pub fn max(&self) -> usize {
        self.max
    }

    pub fn len(&self) -> usize {
        self.views.len()
    }

    pub fn is_empty(&self) -> bool {
        self.views.is_empty()
    }

    pub fn contains(&self, name: &str) -> bool {
        self.views.contains_key(name)
    }

    pub fn get_mut(&mut self, name: &str) -> Option<&mut NamespaceView> {
        if self.views.contains_key(name) {
            self.touch(name);
            self.views.get_mut(name)
        } else {
            None
        }
    }

    /// Insert or replace a view and mark it most-recently-used (pin after warm).
    pub fn insert(&mut self, name: String, view: NamespaceView) {
        if self.views.contains_key(&name) {
            self.views.insert(name.clone(), view);
            self.touch(&name);
            return;
        }
        self.evict_if_needed();
        self.order.push_back(name.clone());
        self.views.insert(name, view);
    }

    pub fn remove(&mut self, name: &str) -> Option<NamespaceView> {
        if let Some(pos) = self.order.iter().position(|n| n == name) {
            self.order.remove(pos);
        }
        self.views.remove(name)
    }

    fn touch(&mut self, name: &str) {
        if let Some(pos) = self.order.iter().position(|n| n == name) {
            let n = self.order.remove(pos).expect("position valid");
            self.order.push_back(n);
        }
    }

    fn evict_if_needed(&mut self) {
        while self.views.len() >= self.max {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            self.views.remove(&oldest);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::NamespaceMeta;

    fn dummy_view(ns: &str) -> NamespaceView {
        let mut v = NamespaceView::empty();
        v.meta.wal_commit_seq = ns.len() as u64;
        v
    }

    #[test]
    fn lru_evicts_oldest_when_over_capacity() {
        let mut cache = ViewCache::new(2);
        cache.insert("a".into(), dummy_view("a"));
        cache.insert("b".into(), dummy_view("b"));
        assert_eq!(cache.len(), 2);
        assert!(cache.get_mut("a").is_some());
        cache.insert("c".into(), dummy_view("c"));
        assert_eq!(cache.len(), 2);
        assert!(!cache.contains("b"));
        assert!(cache.contains("a"));
        assert!(cache.contains("c"));
    }

    #[test]
    fn replace_updates_view_without_extra_eviction() {
        let mut cache = ViewCache::new(2);
        cache.insert("a".into(), dummy_view("a"));
        let mut updated = NamespaceView::empty();
        updated.meta = NamespaceMeta {
            wal_commit_seq: 99,
            ..Default::default()
        };
        cache.insert("a".into(), updated);
        assert_eq!(cache.get_mut("a").unwrap().meta.wal_commit_seq, 99);
        assert_eq!(cache.len(), 1);
    }
}
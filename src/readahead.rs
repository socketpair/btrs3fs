//! Read-ahead for stored zone data (SPEC.md §5.4): a read smaller than the
//! window fetches the whole aligned window around it with one GET and keeps it
//! in a small LRU cache, so neighbouring small reads (files written together
//! lie together in a zone) need no GET of their own.
//!
//! Only windows lying entirely in the stored part of a zone are cached: that
//! data never changes until the zone is reset, and a reset bumps the zone's
//! generation, which is part of the key, so stale windows are never served.

use bytes::Bytes;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tokio::sync::OnceCell;

/// Read-ahead settings of a running device (command line, not disk.json).
#[derive(Clone, Copy, Debug)]
pub struct ReadAhead {
    /// Window size in bytes; 0 disables read-ahead.
    pub window: u64,
    /// Cache size in bytes; 0 disables read-ahead.
    pub cache: u64,
}

impl ReadAhead {
    pub const DISABLED: ReadAhead = ReadAhead {
        window: 0,
        cache: 0,
    };

    fn enabled(&self) -> bool {
        self.window > 0 && self.cache > 0
    }

    pub fn validate(&self, zone_size: u64) -> anyhow::Result<()> {
        if !self.enabled() {
            return Ok(());
        }
        let w = self.window;
        anyhow::ensure!(
            w.is_multiple_of(crate::config::BLOCK_SIZE) && zone_size.is_multiple_of(w),
            "read window {w} must be a multiple of {} that divides the zone size {zone_size}",
            crate::config::BLOCK_SIZE
        );
        anyhow::ensure!(
            self.cache >= w,
            "read cache {} must hold at least one window of {w}",
            self.cache
        );
        Ok(())
    }
}

/// Zone index, zone generation, window index inside the zone.
pub type Key = (usize, u64, u64);

/// A window being fetched or fetched: concurrent readers of the same window
/// share one cell, so one GET serves them all.
pub type Slot = Arc<OnceCell<Bytes>>;

struct Entry {
    slot: Slot,
    stamp: u64,
}

#[derive(Default)]
struct Lru {
    entries: BTreeMap<Key, Entry>,
    /// Last use stamp → key; the first one is evicted.
    order: BTreeMap<u64, Key>,
    next_stamp: u64,
}

pub struct ReadCache {
    window: u64,
    /// Windows the cache holds.
    capacity: usize,
    lru: Mutex<Lru>,
}

impl ReadCache {
    /// `None` when read-ahead is disabled.
    pub fn new(ra: ReadAhead) -> Option<Self> {
        ra.enabled().then(|| Self {
            window: ra.window,
            capacity: (ra.cache / ra.window).max(1) as usize,
            lru: Mutex::new(Lru::default()),
        })
    }

    pub fn window(&self) -> u64 {
        self.window
    }

    /// The slot of a window, created if missing; marks it most recently used.
    pub fn slot(&self, key: Key) -> Slot {
        let mut lru = self.lru.lock().unwrap();
        let stamp = lru.next_stamp;
        lru.next_stamp += 1;
        if let Some(e) = lru.entries.get_mut(&key) {
            let old = std::mem::replace(&mut e.stamp, stamp);
            let slot = e.slot.clone();
            lru.order.remove(&old);
            lru.order.insert(stamp, key);
            return slot;
        }
        let slot = Slot::default();
        lru.entries.insert(
            key,
            Entry {
                slot: slot.clone(),
                stamp,
            },
        );
        lru.order.insert(stamp, key);
        while lru.entries.len() > self.capacity {
            let (_, oldest) = lru.order.pop_first().expect("order mirrors entries");
            // Readers still holding the slot keep its data until they finish.
            lru.entries.remove(&oldest);
        }
        slot
    }

    /// Drops the windows of a zone (on RESET; the new generation would not
    /// match them anyway, this only frees the memory).
    pub fn forget_zone(&self, zone: usize) {
        let mut lru = self.lru.lock().unwrap();
        let keys: Vec<Key> = lru
            .entries
            .range((zone, 0, 0)..=(zone, u64::MAX, u64::MAX))
            .map(|(k, _)| *k)
            .collect();
        for k in keys {
            let e = lru.entries.remove(&k).unwrap();
            lru.order.remove(&e.stamp);
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.lru.lock().unwrap().entries.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validation() {
        let zs = 16 << 20;
        let ra = |window, cache| ReadAhead { window, cache };
        assert!(ra(256 << 10, 64 << 20).validate(zs).is_ok());
        assert!(ra(0, 0).validate(zs).is_ok()); // disabled
        assert!(ra(256 << 10, 0).validate(zs).is_ok()); // disabled
        assert!(ra(12 << 10, 64 << 20).validate(zs).is_err()); // does not divide the zone
        assert!(ra(1000, 64 << 20).validate(zs).is_err()); // not whole blocks
        assert!(ra(1 << 20, 512 << 10).validate(zs).is_err()); // cache below a window
        assert!(ReadCache::new(ra(256 << 10, 0)).is_none());
    }

    #[tokio::test]
    async fn lru_and_forget() {
        let c = ReadCache::new(ReadAhead {
            window: 4096,
            cache: 2 * 4096,
        })
        .unwrap();
        let filled = |key| c.slot(key).initialized();
        c.slot((0, 0, 0)).set(Bytes::from_static(b"a")).unwrap();
        c.slot((0, 0, 1)).set(Bytes::from_static(b"b")).unwrap();
        assert!(filled((0, 0, 0))); // now the most recent
        c.slot((1, 0, 0)).set(Bytes::from_static(b"c")).unwrap(); // evicts (0, 0, 1)
        assert_eq!(c.len(), 2);
        assert!(!filled((0, 0, 1))); // was evicted: empty again, evicts (0, 0, 0)
        assert!(filled((1, 0, 0)));
        c.forget_zone(0);
        assert_eq!(c.len(), 1);
        assert!(filled((1, 0, 0)));
        assert!(!filled((1, 1, 0))); // another generation is another window
    }
}

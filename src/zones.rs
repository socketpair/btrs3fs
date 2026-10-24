//! Zone engine: zone state machine, write buffers and flushing to the store.
//!
//! Independent of ublk; offsets and lengths are in bytes. Invariant per zone
//! (SPEC.md §6): the store holds `[0, flushed)`, RAM chunks hold
//! `[flushed, data_end)`, everything after `data_end` reads as zeros.
//! Chunks leave RAM only after the store confirmed them.
//!
//! Locking: `Zone::upload` (async, at most one upload per zone) is taken before
//! `Zone::state` (sync, never held across `.await`); `Engine::counters` is a leaf
//! lock taken under `state`. Other zones' `state` are only `try_lock`ed while a
//! zone's `state` is held.

use crate::config::{BLOCK_SIZE, DiskConfig};
use crate::stats::{Reason, STATS, ZoneCounts};
use crate::store::{Body, Layout, Store, StoreError};
use bytes::Bytes;
use futures_util::StreamExt;
use log::{debug, info, warn};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::Notify;

/// Number of zones uploaded in parallel by a FLUSH.
const FLUSH_PARALLELISM: usize = 16;

static ZEROS: [u8; 1 << 20] = [0; 1 << 20];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cond {
    Empty,
    ImpOpen,
    ExpOpen,
    Closed,
    Full,
}

#[derive(Debug, thiserror::Error)]
pub enum ZoneError {
    #[error("{0}")]
    Invalid(String),
    #[error("open zone limit reached")]
    OpenLimit,
    #[error("active zone limit reached")]
    ActiveLimit,
    #[error("store error: {0}")]
    Store(#[from] StoreError),
}

impl ZoneError {
    /// Errno reported to the kernel; the ublk driver maps ETOOMANYREFS and
    /// EOVERFLOW to the zone open/active resource block statuses.
    pub fn errno(&self) -> i32 {
        match self {
            ZoneError::OpenLimit => libc::ETOOMANYREFS,
            ZoneError::ActiveLimit => libc::EOVERFLOW,
            _ => libc::EIO,
        }
    }
}

pub type ZResult<T> = Result<T, ZoneError>;

fn invalid<T>(msg: String) -> ZResult<T> {
    Err(ZoneError::Invalid(msg))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ZoneInfo {
    pub start: u64,
    pub len: u64,
    pub capacity: u64,
    /// Absolute write pointer in bytes.
    pub wp: u64,
    pub cond: Cond,
}

/// A part of a read request.
#[derive(Debug)]
pub enum Piece {
    Store { key: String, offset: u64, len: u64 },
    Mem(Bytes),
    Zero(u64),
}

struct ZoneState {
    cond: Cond,
    /// Reported write pointer, relative to the zone start.
    wp: u64,
    /// End of written data; differs from `wp` only after FINISH.
    data_end: u64,
    /// Bytes confirmed by the store (= object size).
    flushed: u64,
    /// Data `[flushed, data_end)`, one chunk per write request.
    chunks: VecDeque<Bytes>,
}

impl ZoneState {
    fn buffered(&self) -> u64 {
        self.data_end - self.flushed
    }
}

struct Zone {
    start: u64,
    state: Mutex<ZoneState>,
    upload: tokio::sync::Mutex<()>,
}

#[derive(Default, Debug)]
struct Counters {
    imp_open: u32,
    exp_open: u32,
    closed: u32,
}

impl Counters {
    fn slot(&mut self, cond: Cond) -> Option<&mut u32> {
        match cond {
            Cond::ImpOpen => Some(&mut self.imp_open),
            Cond::ExpOpen => Some(&mut self.exp_open),
            Cond::Closed => Some(&mut self.closed),
            Cond::Empty | Cond::Full => None,
        }
    }

    fn open(&self) -> u32 {
        self.imp_open + self.exp_open
    }

    fn active(&self) -> u32 {
        self.open() + self.closed
    }
}

pub struct Engine {
    zone_size: u64,
    disk_size: u64,
    /// Effective limits, 0 = unlimited.
    max_open: u32,
    max_active: u32,
    dirty_limit: u64,
    layout: Layout,
    store: Arc<Store>,
    zones: Vec<Zone>,
    counters: Mutex<Counters>,
    dirty: AtomicU64,
    /// Signalled when buffered data shrinks.
    room: Notify,
    /// Wakes the background flusher.
    kick: Notify,
}

impl Engine {
    /// Restores zone state from the objects present in the store.
    pub async fn open(cfg: &DiskConfig, layout: Layout, store: Arc<Store>) -> anyhow::Result<Self> {
        let nr_zones = cfg.nr_zones() as usize;
        let mut sizes = vec![0u64; nr_zones];
        let config_key = layout.config_key();
        for obj in store.list(&layout.dir()).await? {
            let Some(offset) = layout.parse_zone_key(&obj.key) else {
                if obj.key != config_key {
                    warn!("ignoring unexpected object {}", obj.key);
                }
                continue;
            };
            anyhow::ensure!(
                offset % cfg.zone_size == 0 && offset < cfg.disk_size,
                "object {} does not match any zone",
                obj.key
            );
            anyhow::ensure!(
                obj.size <= cfg.zone_size && obj.size % BLOCK_SIZE == 0,
                "object {} has invalid size {}",
                obj.key,
                obj.size
            );
            sizes[(offset / cfg.zone_size) as usize] = obj.size;
        }

        let mut counters = Counters::default();
        let zones = sizes
            .iter()
            .enumerate()
            .map(|(i, &size)| {
                let cond = if size == 0 {
                    Cond::Empty
                } else if size == cfg.zone_size {
                    Cond::Full
                } else {
                    counters.closed += 1;
                    Cond::Closed
                };
                Zone {
                    start: i as u64 * cfg.zone_size,
                    state: Mutex::new(ZoneState {
                        cond,
                        wp: size,
                        data_end: size,
                        flushed: size,
                        chunks: VecDeque::new(),
                    }),
                    upload: tokio::sync::Mutex::new(()),
                }
            })
            .collect::<Vec<_>>();

        let engine = Self {
            zone_size: cfg.zone_size,
            disk_size: cfg.disk_size,
            max_open: cfg.effective_limit(cfg.max_open_zones),
            max_active: cfg.effective_limit(cfg.max_active_zones),
            dirty_limit: cfg.dirty_limit_bytes,
            layout,
            store,
            zones,
            counters: Mutex::new(Counters::default()),
            dirty: AtomicU64::new(0),
            room: Notify::new(),
            kick: Notify::new(),
        };
        if engine.max_active != 0 && counters.active() > engine.max_active {
            warn!(
                "{} closed zones restored, more than max_active_zones {}",
                counters.active(),
                engine.max_active
            );
        }
        let full = sizes.iter().filter(|&&s| s == cfg.zone_size).count();
        info!(
            "restored {nr_zones} zones: {} closed, {full} full, {} empty",
            counters.closed,
            nr_zones - counters.closed as usize - full
        );
        *engine.counters.lock().unwrap() = counters;
        Ok(engine)
    }

    /// Zone index and offset inside the zone.
    pub fn locate(&self, offset: u64) -> ZResult<(usize, u64)> {
        if offset >= self.disk_size {
            return invalid(format!(
                "offset {offset} beyond device size {}",
                self.disk_size
            ));
        }
        Ok(((offset / self.zone_size) as usize, offset % self.zone_size))
    }

    /// Moves a zone to `cond` keeping the open/active counters in sync.
    fn set_cond(&self, st: &mut ZoneState, cond: Cond) {
        let mut c = self.counters.lock().unwrap();
        if let Some(n) = c.slot(st.cond) {
            *n -= 1;
        }
        if let Some(n) = c.slot(cond) {
            *n += 1;
        }
        st.cond = cond;
    }

    /// Checks that a zone in `cond` (EMPTY or CLOSED) may become open, implicitly
    /// closing another implicitly open zone if the open limit is reached.
    fn check_resources(&self, zone: usize, cond: Cond) -> ZResult<()> {
        let (open, active, imp_open) = {
            let c = self.counters.lock().unwrap();
            (c.open(), c.active(), c.imp_open)
        };
        if cond == Cond::Empty && self.max_active != 0 && active >= self.max_active {
            return Err(ZoneError::ActiveLimit);
        }
        if self.max_open == 0 || open < self.max_open {
            return Ok(());
        }
        if imp_open > 0 && self.close_one_implicit(zone) {
            return Ok(());
        }
        Err(ZoneError::OpenLimit)
    }

    fn close_one_implicit(&self, except: usize) -> bool {
        for (i, z) in self.zones.iter().enumerate() {
            if i == except {
                continue;
            }
            // The caller holds its own zone lock: never block on another one.
            if let Ok(mut st) = z.state.try_lock()
                && st.cond == Cond::ImpOpen
            {
                let to = if st.wp == 0 {
                    Cond::Empty
                } else {
                    Cond::Closed
                };
                self.set_cond(&mut st, to);
                debug!("zone {i} implicitly closed");
                return true;
            }
        }
        false
    }

    /// WRITE (at the write pointer) or ZONE_APPEND (anywhere in the zone, `offset`
    /// is the zone start). Returns the absolute offset the data was written at.
    pub fn write(&self, offset: u64, data: Bytes, append: bool) -> ZResult<u64> {
        let len = data.len() as u64;
        if len == 0 || !len.is_multiple_of(BLOCK_SIZE) {
            return invalid(format!(
                "write length {len} is not a multiple of {BLOCK_SIZE}"
            ));
        }
        let (zi, rel) = self.locate(offset)?;
        if append && rel != 0 {
            return invalid(format!(
                "zone append at {offset} does not target a zone start"
            ));
        }
        let zone = &self.zones[zi];
        let mut st = zone.state.lock().unwrap();
        if st.cond == Cond::Full {
            return invalid(format!("write to full zone {zi}"));
        }
        let pos = st.wp;
        if !append && rel != pos {
            return invalid(format!(
                "write at {rel} in zone {zi}, write pointer is at {pos}"
            ));
        }
        if pos + len > self.zone_size {
            return invalid(format!(
                "write of {len} at {pos} crosses the end of zone {zi}"
            ));
        }
        match st.cond {
            Cond::Empty | Cond::Closed => {
                self.check_resources(zi, st.cond)?;
                self.set_cond(&mut st, Cond::ImpOpen);
            }
            Cond::ImpOpen | Cond::ExpOpen => {}
            Cond::Full => unreachable!(),
        }
        st.chunks.push_back(data);
        st.wp += len;
        st.data_end += len;
        if st.wp == self.zone_size {
            self.set_cond(&mut st, Cond::Full);
        }
        drop(st);

        let dirty = self.dirty.fetch_add(len, Ordering::SeqCst) + len;
        if dirty > self.dirty_limit {
            self.kick.notify_one();
        }
        Ok(zone.start + pos)
    }

    /// Splits a read into store ranges, RAM slices and zeros.
    pub fn read_plan(&self, offset: u64, len: u64) -> ZResult<Vec<Piece>> {
        let (zi, rel) = self.locate(offset)?;
        let end = rel + len;
        if end > self.zone_size {
            return invalid(format!("read of {len} at {offset} crosses a zone boundary"));
        }
        let zone = &self.zones[zi];
        let st = zone.state.lock().unwrap();
        let mut pieces = Vec::new();
        let mut pos = rel;

        if pos < end && pos < st.flushed {
            let n = end.min(st.flushed) - pos;
            pieces.push(Piece::Store {
                key: self.layout.zone_key(zone.start),
                offset: pos,
                len: n,
            });
            pos += n;
        }

        let mem_end = end.min(st.data_end);
        let mut chunk_start = st.flushed;
        for chunk in &st.chunks {
            if pos >= mem_end {
                break;
            }
            let chunk_end = chunk_start + chunk.len() as u64;
            if chunk_end > pos {
                let from = (pos - chunk_start) as usize;
                let to = (chunk_end.min(mem_end) - chunk_start) as usize;
                pieces.push(Piece::Mem(chunk.slice(from..to)));
                pos = chunk_start + to as u64;
            }
            chunk_start = chunk_end;
        }

        if pos < end {
            pieces.push(Piece::Zero(end - pos));
        }
        Ok(pieces)
    }

    /// Resolves a read plan into buffers (fetching store ranges).
    pub async fn fetch(&self, pieces: Vec<Piece>) -> ZResult<Vec<Bytes>> {
        let mut out = Vec::with_capacity(pieces.len());
        for p in pieces {
            match p {
                Piece::Store { key, offset, len } => {
                    let started = Instant::now();
                    out.push(self.store.get_range(&key, offset, len).await?);
                    STATS.gets.inc();
                    STATS.read_from_s3.add(len);
                    STATS
                        .get_latency_ms
                        .record(started.elapsed().as_millis() as u64);
                }
                Piece::Mem(b) => {
                    STATS.read_from_ram.add(b.len() as u64);
                    out.push(b);
                }
                Piece::Zero(len) => {
                    STATS.read_zeros.add(len);
                    out.extend(zeros(len));
                }
            }
        }
        Ok(out)
    }

    /// Uploads buffered data of one zone. Returns once everything that was
    /// buffered when the call started is confirmed by the store.
    pub async fn flush_zone(&self, zi: usize, reason: Reason) -> ZResult<()> {
        let zone = &self.zones[zi];
        let _upload = zone.upload.lock().await;
        let (from, chunks) = {
            let st = zone.state.lock().unwrap();
            if st.buffered() == 0 {
                return Ok(());
            }
            (st.flushed, st.chunks.iter().cloned().collect::<Vec<_>>())
        };
        let nr_chunks = chunks.len();
        let body = Body::new(chunks);
        let len = body.len();
        let key = self.layout.zone_key(zone.start);
        let started = Instant::now();
        let uploaded = {
            let _in_flight = STATS.uploads.enter();
            if from == 0 {
                self.store.put(&key, body).await
            } else {
                self.store.patch(&key, from, body).await
            }
        };
        if let Err(e) = uploaded {
            STATS.upload_errors.inc();
            return Err(e.into());
        }
        STATS
            .upload_latency_ms
            .record(started.elapsed().as_millis() as u64);
        STATS.upload_size.record(len);
        if from == 0 {
            STATS.uploads_put.inc();
        } else {
            STATS.uploads_patch.inc();
        }
        STATS.reason(reason).inc();
        debug!("zone {zi}: uploaded {len} bytes at {from} ({reason:?})");

        {
            let mut st = zone.state.lock().unwrap();
            // Only the holder of `upload` removes chunks, so the front ones are ours.
            st.chunks.drain(..nr_chunks);
            st.flushed = from + len;
        }
        self.dirty.fetch_sub(len, Ordering::SeqCst);
        self.room.notify_waiters();
        Ok(())
    }

    /// Uploads every zone with buffered data; fails if any upload failed.
    pub async fn flush_all(&self, reason: Reason) -> ZResult<()> {
        let dirty: Vec<usize> = (0..self.zones.len())
            .filter(|&i| self.zones[i].state.lock().unwrap().buffered() > 0)
            .collect();
        if matches!(reason, Reason::Flush) {
            STATS.zones_per_flush.record(dirty.len() as u64);
        }
        let results: Vec<ZResult<()>> = futures_util::stream::iter(dirty)
            .map(|zi| self.flush_zone(zi, reason))
            .buffer_unordered(FLUSH_PARALLELISM)
            .collect()
            .await;
        results.into_iter().collect()
    }

    pub fn open_zone(&self, offset: u64) -> ZResult<()> {
        let (zi, _) = self.locate(offset)?;
        let mut st = self.zones[zi].state.lock().unwrap();
        match st.cond {
            Cond::ExpOpen => return Ok(()),
            Cond::Empty | Cond::Closed => self.check_resources(zi, st.cond)?,
            Cond::ImpOpen => {}
            Cond::Full => return invalid(format!("open of full zone {zi}")),
        }
        self.set_cond(&mut st, Cond::ExpOpen);
        Ok(())
    }

    pub fn close_zone(&self, offset: u64) -> ZResult<()> {
        let (zi, _) = self.locate(offset)?;
        let mut st = self.zones[zi].state.lock().unwrap();
        match st.cond {
            Cond::Closed => Ok(()),
            Cond::ImpOpen | Cond::ExpOpen => {
                let to = if st.wp == 0 {
                    Cond::Empty
                } else {
                    Cond::Closed
                };
                self.set_cond(&mut st, to);
                Ok(())
            }
            Cond::Empty | Cond::Full => invalid(format!("close of {:?} zone {zi}", st.cond)),
        }
    }

    /// FINISH: the zone becomes FULL; buffered data stays and is flushed as usual.
    pub fn finish_zone(&self, offset: u64) -> ZResult<()> {
        let (zi, _) = self.locate(offset)?;
        let mut st = self.zones[zi].state.lock().unwrap();
        match st.cond {
            Cond::Full => return Ok(()),
            Cond::Empty | Cond::Closed => self.check_resources(zi, st.cond)?,
            Cond::ImpOpen | Cond::ExpOpen => {}
        }
        self.set_cond(&mut st, Cond::Full);
        st.wp = self.zone_size;
        Ok(())
    }

    /// RESET: deletes the zone object, drops buffered data.
    pub async fn reset_zone(&self, offset: u64) -> ZResult<()> {
        let (zi, _) = self.locate(offset)?;
        self.reset_index(zi).await
    }

    async fn reset_index(&self, zi: usize) -> ZResult<()> {
        let zone = &self.zones[zi];
        // Holding `upload` keeps a concurrent flush from re-creating the object
        // between the delete and the state change.
        let _upload = zone.upload.lock().await;
        let had_object = {
            let st = zone.state.lock().unwrap();
            if st.cond == Cond::Empty && st.data_end == 0 {
                return Ok(());
            }
            st.flushed > 0
        };
        // Delete first: if it fails the zone keeps its state and stays consistent.
        if had_object {
            self.store.delete(&self.layout.zone_key(zone.start)).await?;
        }
        let dropped = {
            let mut st = zone.state.lock().unwrap();
            let dropped = st.buffered();
            self.set_cond(&mut st, Cond::Empty);
            st.wp = 0;
            st.data_end = 0;
            st.flushed = 0;
            st.chunks.clear();
            dropped
        };
        if dropped > 0 {
            self.dirty.fetch_sub(dropped, Ordering::SeqCst);
            self.room.notify_waiters();
        }
        debug!("zone {zi} reset");
        Ok(())
    }

    pub async fn reset_all(&self) -> ZResult<()> {
        let results: Vec<ZResult<()>> = futures_util::stream::iter(0..self.zones.len())
            .map(|zi| self.reset_index(zi))
            .buffer_unordered(FLUSH_PARALLELISM)
            .collect()
            .await;
        results.into_iter().collect()
    }

    /// Zone descriptors for REPORT_ZONES starting at zone `first`.
    pub fn report(&self, first: usize, max: usize) -> Vec<ZoneInfo> {
        self.zones
            .iter()
            .skip(first)
            .take(max)
            .map(|z| {
                let st = z.state.lock().unwrap();
                ZoneInfo {
                    start: z.start,
                    len: self.zone_size,
                    capacity: self.zone_size,
                    wp: z.start + st.wp,
                    cond: st.cond,
                }
            })
            .collect()
    }

    pub fn dirty_bytes(&self) -> u64 {
        self.dirty.load(Ordering::SeqCst)
    }

    /// Writes must wait when buffered data reaches twice the soft limit.
    pub fn over_hard_limit(&self) -> bool {
        self.dirty_bytes() >= 2 * self.dirty_limit
    }

    pub async fn wait_for_room(&self) {
        loop {
            let notified = self.room.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if !self.over_hard_limit() {
                return;
            }
            self.kick.notify_one();
            notified.await;
        }
    }

    /// Uploads the largest buffers whenever buffered data exceeds the soft limit.
    pub async fn background_flusher(self: Arc<Self>) {
        loop {
            if self.dirty_bytes() <= self.dirty_limit {
                self.kick.notified().await;
                continue;
            }
            let Some(zi) = self.largest_buffer() else {
                continue;
            };
            if let Err(e) = self.flush_zone(zi, Reason::Background).await {
                warn!("background flush of zone {zi} failed: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }

    fn largest_buffer(&self) -> Option<usize> {
        (0..self.zones.len())
            .map(|i| (self.zones[i].state.lock().unwrap().buffered(), i))
            .filter(|&(b, _)| b > 0)
            .max()
            .map(|(_, i)| i)
    }

    pub fn dirty_limit(&self) -> u64 {
        self.dirty_limit
    }

    /// Zone conditions and the number of zones with buffered data.
    pub fn zone_summary(&self) -> (ZoneCounts, u64) {
        let mut c = ZoneCounts::default();
        let mut dirty = 0;
        for z in &self.zones {
            let st = z.state.lock().unwrap();
            match st.cond {
                Cond::Empty => c.empty += 1,
                Cond::ImpOpen | Cond::ExpOpen => c.open += 1,
                Cond::Closed => c.closed += 1,
                Cond::Full => c.full += 1,
            }
            if st.buffered() > 0 {
                dirty += 1;
            }
        }
        (c, dirty)
    }

    /// Zones that still hold data not confirmed by the store: (zone, bytes).
    pub fn unflushed(&self) -> Vec<(usize, u64)> {
        (0..self.zones.len())
            .map(|i| (i, self.zones[i].state.lock().unwrap().buffered()))
            .filter(|&(_, b)| b > 0)
            .collect()
    }
}

/// Zero-filled buffers totalling `len` bytes, without allocation.
pub fn zeros(len: u64) -> impl Iterator<Item = Bytes> {
    let mut left = len;
    std::iter::from_fn(move || {
        if left == 0 {
            return None;
        }
        let n = left.min(ZEROS.len() as u64);
        left -= n;
        Some(Bytes::from_static(&ZEROS[..n as usize]))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::MemStore;

    const ZS: u64 = 1 << 20;
    const B: u64 = BLOCK_SIZE;

    async fn engine_with(store: Arc<Store>, max_open: u32, max_active: u32) -> Engine {
        let mut cfg = DiskConfig::new(8 * ZS, ZS);
        cfg.max_open_zones = max_open;
        cfg.max_active_zones = max_active;
        cfg.dirty_limit_bytes = 64 * B;
        // No validate(): the engine handles any limits, while the config
        // rejects ones below the btrfs minimum that these tests need.
        Engine::open(&cfg, Layout::new("t").unwrap(), store)
            .await
            .unwrap()
    }

    fn mem() -> Arc<Store> {
        Arc::new(Store::Mem(MemStore::default()))
    }

    fn mem_store(s: &Store) -> &MemStore {
        match s {
            Store::Mem(m) => m,
            Store::S3(_) => unreachable!(),
        }
    }

    fn blk(byte: u8, blocks: u64) -> Bytes {
        Bytes::from(vec![byte; (blocks * B) as usize])
    }

    async fn read(e: &Engine, offset: u64, len: u64) -> Vec<u8> {
        let pieces = e.read_plan(offset, len).unwrap();
        e.fetch(pieces).await.unwrap().concat()
    }

    #[tokio::test]
    async fn write_flush_read() {
        let store = mem();
        let e = engine_with(store.clone(), 0, 0).await;
        assert_eq!(e.write(ZS, blk(1, 2), false).unwrap(), ZS);
        assert_eq!(e.write(ZS, blk(2, 1), true).unwrap(), ZS + 2 * B);
        assert_eq!(e.dirty_bytes(), 3 * B);

        e.flush_all(Reason::Explicit).await.unwrap();
        assert_eq!(e.dirty_bytes(), 0);
        let obj = store.get("t/0000000000100000").await.unwrap().unwrap();
        assert_eq!(obj.len() as u64, 3 * B);

        e.write(ZS + 3 * B, blk(3, 1), false).unwrap();
        // Read spanning store, RAM and zeros.
        let data = read(&e, ZS + B, 4 * B).await;
        assert_eq!(&data[..B as usize], &blk(1, 1)[..]);
        assert_eq!(&data[B as usize..2 * B as usize], &blk(2, 1)[..]);
        assert_eq!(&data[2 * B as usize..3 * B as usize], &blk(3, 1)[..]);
        assert!(data[3 * B as usize..].iter().all(|&b| b == 0));

        e.flush_zone(1, Reason::Explicit).await.unwrap(); // second upload is a PATCH
        let obj = store.get("t/0000000000100000").await.unwrap().unwrap();
        assert_eq!(obj.len() as u64, 4 * B);
        assert_eq!(&obj[3 * B as usize..], &blk(3, 1)[..]);
    }

    #[tokio::test]
    async fn write_rules() {
        let e = engine_with(mem(), 0, 0).await;
        assert!(e.write(B, blk(1, 1), false).is_err()); // not at wp
        assert!(e.write(B, blk(1, 1), true).is_err()); // append not at zone start
        assert!(e.write(0, Bytes::from(vec![0; 512]), false).is_err()); // partial block
        e.write(0, blk(1, ZS / B - 1), false).unwrap();
        assert!(e.write(0, blk(1, 2), true).is_err()); // crosses zone end
        e.write(0, blk(1, 1), true).unwrap();
        assert_eq!(e.report(0, 1)[0].cond, Cond::Full);
        assert!(e.write(0, blk(1, 1), true).is_err()); // full
    }

    #[tokio::test]
    async fn reset_and_finish() {
        let store = mem();
        let e = engine_with(store.clone(), 0, 0).await;
        e.write(0, blk(7, 2), false).unwrap();
        e.flush_all(Reason::Explicit).await.unwrap();
        e.write(2 * B, blk(8, 1), false).unwrap();

        e.finish_zone(0).unwrap();
        let z = &e.report(0, 1)[0];
        assert_eq!((z.cond, z.wp), (Cond::Full, ZS));
        let data = read(&e, 0, 4 * B).await; // data, data, buffered, zeros
        assert_eq!(&data[2 * B as usize..3 * B as usize], &blk(8, 1)[..]);
        assert!(data[3 * B as usize..].iter().all(|&b| b == 0));

        e.reset_zone(0).await.unwrap();
        assert_eq!(e.report(0, 1)[0].cond, Cond::Empty);
        assert_eq!(e.dirty_bytes(), 0);
        assert!(store.get("t/0000000000000000").await.unwrap().is_none());
        assert!(read(&e, 0, 2 * B).await.iter().all(|&b| b == 0));

        e.write(0, blk(9, 1), false).unwrap(); // fresh object after reset: PUT
        e.flush_all(Reason::Explicit).await.unwrap();
        assert_eq!(
            store
                .get("t/0000000000000000")
                .await
                .unwrap()
                .unwrap()
                .len() as u64,
            B
        );
    }

    #[tokio::test]
    async fn restore_from_store() {
        let store = mem();
        {
            let e = engine_with(store.clone(), 0, 0).await;
            e.write(0, blk(1, 3), false).unwrap();
            e.write(ZS, blk(2, ZS / B), false).unwrap();
            e.write(2 * ZS, blk(3, 1), false).unwrap(); // never flushed
            e.flush_zone(0, Reason::Explicit).await.unwrap();
            e.flush_zone(1, Reason::Explicit).await.unwrap();
        }
        let e = engine_with(store, 0, 0).await;
        let r = e.report(0, 3);
        assert_eq!((r[0].cond, r[0].wp), (Cond::Closed, 3 * B));
        assert_eq!((r[1].cond, r[1].wp), (Cond::Full, 2 * ZS));
        assert_eq!((r[2].cond, r[2].wp), (Cond::Empty, 2 * ZS));
        assert_eq!(&read(&e, 2 * B, B).await[..], &blk(1, 1)[..]);
        e.write(3 * B, blk(4, 1), false).unwrap(); // continues at restored wp
    }

    #[tokio::test]
    async fn open_and_active_limits() {
        let e = engine_with(mem(), 2, 3).await;
        e.write(0, blk(1, 1), false).unwrap();
        e.write(ZS, blk(1, 1), false).unwrap();
        // Third implicit open closes one implicitly open zone.
        e.write(2 * ZS, blk(1, 1), false).unwrap();
        let conds: Vec<Cond> = e.report(0, 3).iter().map(|z| z.cond).collect();
        assert_eq!(conds.iter().filter(|&&c| c == Cond::Closed).count(), 1);
        // Three active zones: a fourth cannot be opened.
        assert!(matches!(
            e.write(3 * ZS, blk(1, 1), false),
            Err(ZoneError::ActiveLimit)
        ));
        // Explicitly open zones cannot be implicitly closed.
        e.close_zone(0).ok();
        e.close_zone(ZS).ok();
        e.close_zone(2 * ZS).ok();
        e.open_zone(0).unwrap();
        e.open_zone(ZS).unwrap();
        assert!(matches!(e.open_zone(2 * ZS), Err(ZoneError::OpenLimit)));
        // Finishing frees an open and an active slot.
        e.finish_zone(0).unwrap();
        e.write(3 * ZS, blk(1, 1), false).unwrap();
        assert!(matches!(
            e.write(4 * ZS, blk(1, 1), false),
            Err(ZoneError::ActiveLimit)
        ));
    }

    #[tokio::test]
    async fn failed_flush_keeps_data() {
        let store = mem();
        let e = engine_with(store.clone(), 0, 0).await;
        e.write(0, blk(5, 2), false).unwrap();
        mem_store(&store).set_fail(true);
        assert!(e.flush_all(Reason::Explicit).await.is_err());
        assert_eq!(e.dirty_bytes(), 2 * B);
        assert_eq!(&read(&e, 0, B).await[..], &blk(5, 1)[..]); // still readable from RAM
        mem_store(&store).set_fail(false);
        e.flush_all(Reason::Explicit).await.unwrap();
        assert_eq!(e.dirty_bytes(), 0);
        assert!(e.unflushed().is_empty());
    }

    #[tokio::test]
    async fn background_flusher_drains_over_limit() {
        let store = mem();
        let e = Arc::new(engine_with(store, 0, 0).await);
        tokio::spawn(e.clone().background_flusher());
        for z in 0..4 {
            e.write(z * ZS, blk(1, 40), false).unwrap(); // 160 blocks > limit of 64
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            while e.dirty_bytes() > 64 * B {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        e.wait_for_room().await;
    }
}

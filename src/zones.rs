//! Zone engine: zone state machine, write buffers and flushing to the store.
//!
//! Independent of ublk; offsets and lengths are in bytes. Invariant per zone
//! (SPEC.md §6): the store holds `[0, flushed)`, RAM chunks hold
//! `[flushed, data_end)`, everything after `data_end` reads as zeros.
//! Chunks leave RAM only after the store confirmed them.
//!
//! The device reports no open or active zone limits (SPEC.md §2): btrfs keeps
//! its own limit and the engine accepts writes to any number of zones.
//!
//! Locking: `Zone::upload` (async, at most one upload per zone) is taken before
//! `Zone::state` (sync, never held across `.await`).

use crate::config::{BLOCK_SIZE, DiskConfig};
use crate::stats::{Reason, STATS, ZoneCounts};
use crate::store::{Body, Layout, Store, StoreError, StoreResult};
use bytes::Bytes;
use futures_util::StreamExt;
use log::{debug, info, warn};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::Notify;

/// S3 requests one bulk operation runs at once: zones uploaded by a FLUSH or
/// the background flusher, tag lookups at start. Judgement call, not
/// measured: enough to keep a link busy with zone-sized bodies, few enough not
/// to hit S3 request rate limits; one FLUSH touches 2–4 zones in practice
/// (`stats`, SPEC.md §10).
const S3_PARALLELISM: usize = 16;

/// Shared zero-filled buffer for reads past the write pointer; a larger read
/// is served as several 1 MiB pieces. Size is a judgement call: 1 MiB of static
/// memory, at most 32 iovecs for the largest (32 MiB) request.
static ZEROS: [u8; 1 << 20] = [0; 1 << 20];

/// Pause of the background flusher after a failed upload. The upload itself
/// already retried for retry_timeout_seconds; this only avoids a busy loop
/// while S3 stays unreachable.
const BACKGROUND_RETRY_PAUSE: Duration = Duration::from_secs(1);

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
    #[error("store error: {0}")]
    Store(#[from] StoreError),
}

impl ZoneError {
    /// Errno reported to the kernel.
    pub fn errno(&self) -> i32 {
        libc::EIO
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
    /// The zone object exists in the store (an empty one for a finished empty zone).
    object: bool,
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

pub struct Engine {
    zone_size: u64,
    disk_size: u64,
    dirty_limit: u64,
    layout: Layout,
    store: Arc<Store>,
    zones: Vec<Zone>,
    dirty: AtomicU64,
    /// Signalled when buffered data shrinks.
    room: Notify,
    /// Wakes the background flusher.
    kick: Notify,
}

impl Engine {
    /// Restores zone state from the objects present in the store: the object
    /// size is the write pointer, the finished tag marks zones closed by FINISH.
    pub async fn open(cfg: &DiskConfig, layout: Layout, store: Arc<Store>) -> anyhow::Result<Self> {
        let nr_zones = cfg.nr_zones() as usize;
        // Per zone: object size and whether it is finished (None: not listed).
        let mut objects: Vec<Option<(u64, Option<bool>)>> = vec![None; nr_zones];
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
            objects[(offset / cfg.zone_size) as usize] = Some((obj.size, obj.finished));
        }

        // Standard S3 does not list tags: ask for them where they matter.
        let unknown: Vec<usize> = (0..nr_zones)
            .filter(|&i| matches!(objects[i], Some((size, None)) if size < cfg.zone_size))
            .collect();
        let results: Vec<(usize, StoreResult<bool>)> = futures_util::stream::iter(unknown)
            .map(|i| {
                let key = layout.zone_key(i as u64 * cfg.zone_size);
                let store = &store;
                async move { (i, store.is_finished(&key).await) }
            })
            .buffer_unordered(S3_PARALLELISM)
            .collect()
            .await;
        for (i, finished) in results {
            if let Some((_, f)) = &mut objects[i] {
                *f = Some(finished?);
            }
        }

        let (mut full, mut empty, mut closed) = (0, 0, 0);
        let zones = objects
            .iter()
            .enumerate()
            .map(|(i, obj)| {
                let (size, finished) = obj.map_or((0, false), |(s, f)| (s, f == Some(true)));
                let (cond, wp) = if finished || size == cfg.zone_size {
                    full += 1;
                    (Cond::Full, cfg.zone_size)
                } else if size == 0 {
                    empty += 1;
                    (Cond::Empty, 0)
                } else {
                    closed += 1;
                    (Cond::Closed, size)
                };
                Zone {
                    start: i as u64 * cfg.zone_size,
                    state: Mutex::new(ZoneState {
                        cond,
                        wp,
                        data_end: size,
                        flushed: size,
                        chunks: VecDeque::new(),
                        object: obj.is_some(),
                    }),
                    upload: tokio::sync::Mutex::new(()),
                }
            })
            .collect::<Vec<_>>();

        let engine = Self {
            zone_size: cfg.zone_size,
            disk_size: cfg.disk_size,
            dirty_limit: cfg.dirty_limit_bytes,
            layout,
            store,
            zones,
            dirty: AtomicU64::new(0),
            room: Notify::new(),
            kick: Notify::new(),
        };
        info!("restored {nr_zones} zones: {closed} closed, {full} full, {empty} empty");
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
                st.cond = Cond::ImpOpen;
            }
            Cond::ImpOpen | Cond::ExpOpen => {}
            Cond::Full => unreachable!(),
        }
        st.chunks.push_back(data);
        st.wp += len;
        st.data_end += len;
        if st.wp == self.zone_size {
            st.cond = Cond::Full;
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
        let _upload = self.zones[zi].upload.lock().await;
        self.flush_locked(zi, reason).await
    }

    /// `flush_zone` for a caller already holding the zone's `upload` lock.
    async fn flush_locked(&self, zi: usize, reason: Reason) -> ZResult<()> {
        let zone = &self.zones[zi];
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
            st.object = true;
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
            .buffer_unordered(S3_PARALLELISM)
            .collect()
            .await;
        results.into_iter().collect()
    }

    pub fn open_zone(&self, offset: u64) -> ZResult<()> {
        let (zi, _) = self.locate(offset)?;
        let mut st = self.zones[zi].state.lock().unwrap();
        match st.cond {
            Cond::ExpOpen => return Ok(()),
            Cond::Empty | Cond::Closed | Cond::ImpOpen => {}
            Cond::Full => return invalid(format!("open of full zone {zi}")),
        }
        st.cond = Cond::ExpOpen;
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
                st.cond = to;
                Ok(())
            }
            Cond::Empty | Cond::Full => invalid(format!("close of {:?} zone {zi}", st.cond)),
        }
    }

    /// FINISH: stores the zone data, tags the zone object as finished
    /// (creating an empty object if there is none) and makes the zone FULL.
    /// Persisting it keeps finished zones FULL after a restart (SPEC.md §4.4).
    pub async fn finish_zone(&self, offset: u64) -> ZResult<()> {
        let (zi, _) = self.locate(offset)?;
        let zone = &self.zones[zi];
        let _upload = zone.upload.lock().await;
        if zone.state.lock().unwrap().cond == Cond::Full {
            return Ok(());
        }
        self.flush_locked(zi, Reason::Finish).await?;
        let key = self.layout.zone_key(zone.start);
        if !zone.state.lock().unwrap().object {
            self.store.put(&key, Body::default()).await?;
            zone.state.lock().unwrap().object = true;
        }
        self.store.mark_finished(&key).await?;

        let mut st = zone.state.lock().unwrap();
        st.cond = Cond::Full;
        st.wp = self.zone_size;
        debug!("zone {zi} finished");
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
            if st.cond == Cond::Empty && st.data_end == 0 && !st.object {
                return Ok(());
            }
            st.object
        };
        // Delete first: if it fails the zone keeps its state and stays consistent.
        if had_object {
            self.store.delete(&self.layout.zone_key(zone.start)).await?;
        }
        self.clear_locked(zi);
        debug!("zone {zi} reset");
        Ok(())
    }

    /// Makes the zone EMPTY and drops its buffer; the caller holds `upload`
    /// and has deleted the zone object.
    fn clear_locked(&self, zi: usize) {
        let dropped = {
            let mut st = self.zones[zi].state.lock().unwrap();
            let dropped = st.buffered();
            st.cond = Cond::Empty;
            st.wp = 0;
            st.data_end = 0;
            st.flushed = 0;
            st.chunks.clear();
            st.object = false;
            dropped
        };
        if dropped > 0 {
            self.dirty.fetch_sub(dropped, Ordering::SeqCst);
            self.room.notify_waiters();
        }
    }

    /// RESET_ALL: all zone objects go in DeleteObjects batches of up to 1000
    /// keys instead of one DELETE per zone (SPEC.md §4.2).
    pub async fn reset_all(&self) -> ZResult<()> {
        // Every `upload` lock, so no flush re-creates an object between the
        // delete and the state change. Taken in zone order; nothing else holds
        // two of them, so this cannot deadlock.
        let mut guards = Vec::with_capacity(self.zones.len());
        let mut keys = Vec::new();
        for zone in &self.zones {
            guards.push(zone.upload.lock().await);
            if zone.state.lock().unwrap().object {
                keys.push(self.layout.zone_key(zone.start));
            }
        }
        // On failure no zone changes state, as with a failed single DELETE,
        // though some objects may be gone: their zones then fail I/O until the
        // next RESET or restart, which restores them from the store as EMPTY.
        self.store.delete_many(&keys).await?;
        for zi in 0..self.zones.len() {
            self.clear_locked(zi);
        }
        debug!("all zones reset, {} objects deleted", keys.len());
        Ok(())
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

    /// Uploads zones whenever buffered data exceeds the soft limit, up to
    /// `S3_PARALLELISM` at once (SPEC.md §5.3). A new upload starts as soon as
    /// one ends, while the data not yet being uploaded is above the limit.
    pub async fn background_flusher(self: Arc<Self>) {
        let mut running = futures_util::stream::FuturesUnordered::new();
        let mut busy = vec![false; self.zones.len()];
        // Bytes the running uploads were picked for (buffered at pick time).
        let mut in_flight = 0u64;
        loop {
            while running.len() < S3_PARALLELISM
                && self.dirty_bytes().saturating_sub(in_flight) > self.dirty_limit
            {
                let Some((zi, bytes)) = self.background_pick(&busy) else {
                    break;
                };
                busy[zi] = true;
                in_flight += bytes;
                let engine: &Engine = &self;
                running.push(async move {
                    (zi, bytes, engine.flush_zone(zi, Reason::Background).await)
                });
            }
            if running.is_empty() {
                // `notify_one` keeps a permit, so a kick sent meanwhile is not lost.
                self.kick.notified().await;
                continue;
            }
            tokio::select! {
                Some((zi, bytes, res)) = running.next() => {
                    busy[zi] = false;
                    in_flight -= bytes;
                    if let Err(e) = res {
                        warn!("background flush of zone {zi} failed: {e}");
                        tokio::time::sleep(BACKGROUND_RETRY_PAUSE).await;
                    }
                }
                // More data arrived: maybe room for another upload.
                _ = self.kick.notified() => {}
            }
        }
    }

    /// Next zone for the background flusher among those not being uploaded:
    /// FULL zones first — nothing more will be written to them, and a zone
    /// filled entirely in RAM goes out as one PUT — then the largest buffer.
    fn background_pick(&self, busy: &[bool]) -> Option<(usize, u64)> {
        (0..self.zones.len())
            .filter(|&i| !busy[i])
            .filter_map(|i| {
                let st = self.zones[i].state.lock().unwrap();
                let b = st.buffered();
                (b > 0).then_some(((st.cond == Cond::Full, b), i))
            })
            .max()
            .map(|((_, b), i)| (i, b))
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

    async fn engine_with(store: Arc<Store>) -> Engine {
        let mut cfg = DiskConfig::new(8 * ZS, ZS);
        cfg.dirty_limit_bytes = 64 * B;
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
        let e = engine_with(store.clone()).await;
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
        let e = engine_with(mem()).await;
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
        let e = engine_with(store.clone()).await;
        e.write(0, blk(7, 2), false).unwrap();
        e.flush_all(Reason::Explicit).await.unwrap();
        e.write(2 * B, blk(8, 1), false).unwrap();

        // FINISH stores buffered data and tags the object.
        e.finish_zone(0).await.unwrap();
        let z = &e.report(0, 1)[0];
        assert_eq!((z.cond, z.wp), (Cond::Full, ZS));
        assert_eq!(e.dirty_bytes(), 0);
        assert!(store.is_finished("t/0000000000000000").await.unwrap());
        let data = read(&e, 0, 4 * B).await; // stored data, then zeros
        assert_eq!(&data[2 * B as usize..3 * B as usize], &blk(8, 1)[..]);
        assert!(data[3 * B as usize..].iter().all(|&b| b == 0));

        e.reset_zone(0).await.unwrap();
        assert_eq!(e.report(0, 1)[0].cond, Cond::Empty);
        assert!(store.get("t/0000000000000000").await.unwrap().is_none());
        assert!(read(&e, 0, 2 * B).await.iter().all(|&b| b == 0));

        e.write(0, blk(9, 1), false).unwrap(); // fresh object after reset: PUT
        e.flush_all(Reason::Explicit).await.unwrap();
        let obj = store.get("t/0000000000000000").await.unwrap().unwrap();
        assert_eq!(obj.len() as u64, B);
        assert!(!store.is_finished("t/0000000000000000").await.unwrap());

        // FINISH of an empty zone creates an empty tagged object; RESET removes it.
        e.finish_zone(ZS).await.unwrap();
        let obj = store.get("t/0000000000100000").await.unwrap().unwrap();
        assert!(obj.is_empty() && store.is_finished("t/0000000000100000").await.unwrap());
        e.reset_zone(ZS).await.unwrap();
        assert!(store.get("t/0000000000100000").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn reset_all_deletes_every_object() {
        let store = mem();
        let e = engine_with(store.clone()).await;
        e.write(0, blk(1, 2), false).unwrap();
        e.write(ZS, blk(2, 2), false).unwrap();
        e.flush_all(Reason::Explicit).await.unwrap();
        e.write(2 * B, blk(3, 1), false).unwrap(); // stored data plus a buffer
        e.write(2 * ZS, blk(4, 1), false).unwrap(); // buffer only
        e.finish_zone(3 * ZS).await.unwrap(); // empty tagged object
        e.reset_all().await.unwrap();
        assert!(e.report(0, 8).iter().all(|z| z.cond == Cond::Empty));
        assert_eq!(e.dirty_bytes(), 0);
        assert!(store.list("t/").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn restore_from_store() {
        let store = mem();
        {
            let e = engine_with(store.clone()).await;
            e.write(0, blk(1, 3), false).unwrap();
            e.write(ZS, blk(2, ZS / B), false).unwrap();
            e.write(2 * ZS, blk(3, 1), false).unwrap(); // never flushed
            e.flush_zone(0, Reason::Explicit).await.unwrap();
            e.flush_zone(1, Reason::Explicit).await.unwrap();
            e.write(3 * ZS, blk(5, 2), false).unwrap();
            e.finish_zone(3 * ZS).await.unwrap(); // partially written, finished
            e.finish_zone(4 * ZS).await.unwrap(); // empty, finished
        }
        let e = engine_with(store).await;
        let r = e.report(0, 5);
        assert_eq!((r[0].cond, r[0].wp), (Cond::Closed, 3 * B));
        assert_eq!((r[1].cond, r[1].wp), (Cond::Full, 2 * ZS));
        assert_eq!((r[2].cond, r[2].wp), (Cond::Empty, 2 * ZS));
        assert_eq!((r[3].cond, r[3].wp), (Cond::Full, 4 * ZS));
        assert_eq!((r[4].cond, r[4].wp), (Cond::Full, 5 * ZS));
        assert_eq!(&read(&e, 2 * B, B).await[..], &blk(1, 1)[..]);
        assert_eq!(&read(&e, 3 * ZS + B, B).await[..], &blk(5, 1)[..]);
        assert!(read(&e, 3 * ZS + 2 * B, B).await.iter().all(|&b| b == 0));
        assert!(read(&e, 4 * ZS, B).await.iter().all(|&b| b == 0));
        e.write(3 * B, blk(4, 1), false).unwrap(); // continues at restored wp
        assert!(e.write(3 * ZS, blk(1, 1), true).is_err()); // finished zone stays full
    }

    #[tokio::test]
    async fn no_zone_limits() {
        let e = engine_with(mem()).await;
        for z in 0..8 {
            e.write(z * ZS, blk(1, 1), false).unwrap(); // every zone open at once
        }
        e.close_zone(0).unwrap();
        e.open_zone(0).unwrap();
        e.open_zone(ZS).unwrap();
        e.finish_zone(2 * ZS).await.unwrap();
        let conds: Vec<Cond> = e.report(0, 3).iter().map(|z| z.cond).collect();
        assert_eq!(conds, vec![Cond::ExpOpen, Cond::ExpOpen, Cond::Full]);
    }

    #[tokio::test]
    async fn failed_flush_keeps_data() {
        let store = mem();
        let e = engine_with(store.clone()).await;
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
        let e = Arc::new(engine_with(store).await);
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

    #[tokio::test]
    async fn background_flusher_prefers_full_zones() {
        let e = Arc::new(engine_with(mem()).await);
        e.write(0, blk(1, ZS / B - 56), false).unwrap();
        e.flush_zone(0, Reason::Explicit).await.unwrap();
        e.write(ZS - 56 * B, blk(1, 56), false).unwrap(); // zone 0: FULL, 56 blocks in RAM
        e.write(ZS, blk(2, 60), false).unwrap(); // zone 1: open, 60 blocks in RAM
        tokio::spawn(e.clone().background_flusher());
        e.kick.notify_one(); // 116 blocks > limit of 64
        tokio::time::timeout(Duration::from_secs(5), async {
            while e.dirty_bytes() > 64 * B {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        // Uploading the FULL zone alone gets under the limit; the larger open
        // buffer stays in RAM for more appends.
        assert_eq!(e.unflushed(), vec![(1, 60 * B)]);
    }
}

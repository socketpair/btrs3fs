//! Zone engine: zone state machine, write buffers and flushing to the store.
//!
//! Independent of ublk; offsets and lengths are in bytes. Invariant per zone
//! (SPEC.md §7): the store holds `[0, flushed)`, RAM chunks hold
//! `[flushed, data_end)`, everything after `data_end` reads as zeros.
//! Chunks leave RAM only after the store confirmed them.
//!
//! The device reports no open or active zone limits (SPEC.md §2): btrfs keeps
//! its own limit and the engine accepts writes to any number of zones.
//!
//! Locking: `Zone::upload` (async, at most one upload per zone) is taken before
//! `Zone::state` (sync, never held across `.await`).

use crate::config::{BLOCK_SIZE, DiskConfig};
use crate::readahead::{ReadAhead, ReadCache};
use crate::stats::{Reason, STATS, ZoneCounts};
use crate::store::{Body, Layout, Store, StoreError};
use bytes::Bytes;
use futures_util::StreamExt;
use log::{debug, info, warn};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{Notify, Semaphore};

/// S3 requests run at once without a link to tune for: uploads of the
/// temporary `create` device and tests. Judgement call: enough to keep a link
/// busy with zone-sized bodies, few enough not to hit S3 request rate limits. With a link, uploads follow it (SPEC.md §6.1).
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
    Store {
        key: String,
        offset: u64,
        len: u64,
        /// Zone index and generation, for the read-ahead cache key.
        zone: usize,
        generation: u64,
        /// Stored bytes of the zone when the plan was made.
        flushed: u64,
    },
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
    /// The zone object exists in the store.
    object: bool,
    /// The FINISH marker `<zone>.finished` exists in the store (SPEC.md §3.3).
    marker: bool,
    /// Bumped on every RESET; part of the read-ahead cache key.
    generation: u64,
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
    /// `None`: read-ahead disabled.
    read_cache: Option<ReadCache>,
    /// Uploads at once, all reasons together; also how many zones FLUSH and
    /// the background flusher start at once.
    uploads: usize,
    /// `uploads` permits shared by every upload, whatever its reason
    /// (SPEC.md §5).
    upload_slots: Semaphore,
    /// Store reads larger than this go out as parallel GETs of this size
    /// (SPEC.md §6.2).
    get_part: u64,
}

impl Engine {
    /// Restores zone state from the objects present in the store: the object
    /// size is the write pointer, a `<zone>.finished` marker a zone closed by
    /// FINISH.
    pub async fn open(cfg: &DiskConfig, layout: Layout, store: Arc<Store>) -> anyhow::Result<Self> {
        let nr_zones = cfg.nr_zones() as usize;
        // Per zone: object size (None: no object) and whether a FINISH marker exists.
        let mut sizes: Vec<Option<u64>> = vec![None; nr_zones];
        let mut finished = vec![false; nr_zones];
        let config_key = layout.config_key();
        let zone_index = |offset: u64, key: &str| -> anyhow::Result<usize> {
            anyhow::ensure!(
                offset.is_multiple_of(cfg.zone_size) && offset < cfg.disk_size,
                "object {key} does not match any zone"
            );
            Ok((offset / cfg.zone_size) as usize)
        };
        for obj in store.list(&layout.dir()).await? {
            if let Some(offset) = layout.parse_finished_key(&obj.key) {
                finished[zone_index(offset, &obj.key)?] = true;
                continue;
            }
            let Some(offset) = layout.parse_zone_key(&obj.key) else {
                if obj.key != config_key {
                    warn!("ignoring unexpected object {}", obj.key);
                }
                continue;
            };
            anyhow::ensure!(
                obj.size <= cfg.zone_size && obj.size % BLOCK_SIZE == 0,
                "object {} has invalid size {}",
                obj.key,
                obj.size
            );
            sizes[zone_index(offset, &obj.key)?] = Some(obj.size);
        }

        let (mut full, mut empty, mut closed) = (0, 0, 0);
        let zones = sizes
            .iter()
            .zip(&finished)
            .enumerate()
            .map(|(i, (&object, &finished))| {
                let size = object.unwrap_or(0);
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
                        object: object.is_some(),
                        marker: finished,
                        generation: 0,
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
            read_cache: None,
            uploads: S3_PARALLELISM,
            upload_slots: Semaphore::new(S3_PARALLELISM),
            get_part: u64::MAX,
        };
        info!("restored {nr_zones} zones: {closed} closed, {full} full, {empty} empty");
        Ok(engine)
    }

    /// Enables read-ahead (SPEC.md §6); without this call it is off.
    pub fn with_read_ahead(mut self, ra: ReadAhead) -> anyhow::Result<Self> {
        ra.validate(self.zone_size)?;
        self.read_cache = ReadCache::new(ra);
        if let Some(c) = &self.read_cache {
            info!(
                "read-ahead: {} byte windows, {} bytes of cache",
                c.window(),
                ra.cache
            );
        }
        Ok(self)
    }

    /// Uploads at once, all reasons together (SPEC.md §6.1).
    pub fn with_uploads(mut self, uploads: usize) -> Self {
        self.uploads = uploads.max(1);
        self.upload_slots = Semaphore::new(self.uploads);
        self
    }

    /// Splits store reads into parallel GETs of `part` bytes (SPEC.md §6.2);
    /// without this call a read is one GET.
    pub fn with_get_part(mut self, part: u64) -> Self {
        self.get_part = part.max(BLOCK_SIZE);
        self
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
        let full = st.wp == self.zone_size;
        if full {
            st.cond = Cond::Full;
        }
        drop(st);

        let dirty = self.dirty.fetch_add(len, Ordering::SeqCst) + len;
        if full || dirty > self.dirty_limit {
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
                zone: zi,
                generation: st.generation,
                flushed: st.flushed,
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
                Piece::Store {
                    key,
                    offset,
                    len,
                    zone,
                    generation,
                    flushed,
                } => match &self.read_cache {
                    // Only small reads, and only windows wholly stored: they never change.
                    Some(c)
                        if len < c.window()
                            && (offset + len).next_multiple_of(c.window()) <= flushed =>
                    {
                        self.read_windows(c, &key, (zone, generation), offset, len, &mut out)
                            .await?
                    }
                    _ => out.extend(self.get_parts(&key, offset, len).await?),
                },
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

    /// `[offset, offset + len)` of an object as parallel GETs of `get_part`.
    async fn get_parts(&self, key: &str, offset: u64, len: u64) -> ZResult<Vec<Bytes>> {
        let end = offset + len;
        let parts = (offset..end)
            .step_by(self.get_part as usize)
            .map(|from| self.get(key, from, self.get_part.min(end - from)));
        futures_util::future::try_join_all(parts).await
    }

    /// One GET, counted in the statistics.
    async fn get(&self, key: &str, offset: u64, len: u64) -> ZResult<Bytes> {
        let started = Instant::now();
        let data = self.store.get_range(key, offset, len).await?;
        STATS.gets.inc();
        STATS.get_size.record(len);
        STATS.read_from_s3.add(len);
        STATS
            .get_latency_ms
            .record(started.elapsed().as_millis() as u64);
        Ok(data)
    }

    /// Serves `[offset, offset + len)` of a zone from read-ahead windows,
    /// fetching the missing ones (one GET per window, shared by concurrent readers).
    async fn read_windows(
        &self,
        cache: &ReadCache,
        key: &str,
        (zone, generation): (usize, u64),
        offset: u64,
        len: u64,
        out: &mut Vec<Bytes>,
    ) -> ZResult<()> {
        let w = cache.window();
        let end = offset + len;
        for wi in offset / w..end.div_ceil(w) {
            let start = wi * w;
            let slot = cache.slot((zone, generation, wi));
            let mut fetched = false;
            let data = slot
                .get_or_try_init(|| {
                    fetched = true;
                    self.get(key, start, w)
                })
                .await?;
            let (from, to) = (offset.max(start) - start, end.min(start + w) - start);
            out.push(data.slice(from as usize..to as usize));
            if fetched {
                STATS.read_ahead_misses.inc();
            } else {
                STATS.read_ahead_hits.inc();
                STATS.read_from_cache.add(to - from);
            }
        }
        Ok(())
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
            // Never closed, so acquiring cannot fail.
            let _slot = self.upload_slots.acquire().await.unwrap();
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
            .buffer_unordered(self.uploads)
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
                if st.buffered() > 0 {
                    self.kick.notify_one();
                }
                Ok(())
            }
            Cond::Empty | Cond::Full => invalid(format!("close of {:?} zone {zi}", st.cond)),
        }
    }

    /// FINISH: stores the zone data, puts the empty marker `<zone>.finished`
    /// and makes the zone FULL. Persisting it keeps finished zones FULL after a
    /// restart (SPEC.md §3.3).
    pub async fn finish_zone(&self, offset: u64) -> ZResult<()> {
        let (zi, _) = self.locate(offset)?;
        let zone = &self.zones[zi];
        let _upload = zone.upload.lock().await;
        if zone.state.lock().unwrap().cond == Cond::Full {
            return Ok(());
        }
        self.flush_locked(zi, Reason::Finish).await?;
        self.store
            .put(&self.layout.finished_key(zone.start), Body::default())
            .await?;

        let mut st = zone.state.lock().unwrap();
        st.marker = true;
        st.cond = Cond::Full;
        st.wp = self.zone_size;
        debug!("zone {zi} finished");
        Ok(())
    }

    /// RESET: deletes the zone object and its FINISH marker, drops buffered data.
    pub async fn reset_zone(&self, offset: u64) -> ZResult<()> {
        let (zi, _) = self.locate(offset)?;
        self.reset_index(zi).await
    }

    async fn reset_index(&self, zi: usize) -> ZResult<()> {
        let zone = &self.zones[zi];
        // Holding `upload` keeps a concurrent flush from re-creating the object
        // between the delete and the state change.
        let _upload = zone.upload.lock().await;
        let keys = {
            let st = zone.state.lock().unwrap();
            if st.cond == Cond::Empty && st.data_end == 0 && !st.object && !st.marker {
                return Ok(());
            }
            self.stored_keys(zone, &st)
        };
        // Delete first: if it fails the zone keeps its state and stays consistent.
        match keys.as_slice() {
            [] => {}
            [key] => self.store.delete(key).await?,
            _ => self.store.delete_many(&keys).await?,
        }
        self.clear_locked(zi);
        debug!("zone {zi} reset");
        Ok(())
    }

    /// Objects of the zone in the store: the zone object and the FINISH marker.
    fn stored_keys(&self, zone: &Zone, st: &ZoneState) -> Vec<String> {
        let mut keys = Vec::new();
        if st.object {
            keys.push(self.layout.zone_key(zone.start));
        }
        if st.marker {
            keys.push(self.layout.finished_key(zone.start));
        }
        keys
    }

    /// Makes the zone EMPTY and drops its buffer; the caller holds `upload`
    /// and has deleted the zone's objects.
    fn clear_locked(&self, zi: usize) {
        let (dropped, had_object) = {
            let mut st = self.zones[zi].state.lock().unwrap();
            let had_object = st.object;
            st.generation += 1;
            let dropped = st.buffered();
            st.cond = Cond::Empty;
            st.wp = 0;
            st.data_end = 0;
            st.flushed = 0;
            st.chunks.clear();
            st.object = false;
            st.marker = false;
            (dropped, had_object)
        };
        // Only stored data is cached.
        if let (Some(c), true) = (&self.read_cache, had_object) {
            c.forget_zone(zi);
        }
        if dropped > 0 {
            self.dirty.fetch_sub(dropped, Ordering::SeqCst);
            self.room.notify_waiters();
        }
    }

    /// RESET_ALL: all zone objects go in DeleteObjects batches of up to 1000
    /// keys instead of one DELETE per zone (SPEC.md §4).
    pub async fn reset_all(&self) -> ZResult<()> {
        // Every `upload` lock, so no flush re-creates an object between the
        // delete and the state change. Taken in zone order; nothing else holds
        // two of them, so this cannot deadlock.
        let mut guards = Vec::with_capacity(self.zones.len());
        let mut keys = Vec::new();
        for zone in &self.zones {
            guards.push(zone.upload.lock().await);
            keys.extend(self.stored_keys(zone, &zone.state.lock().unwrap()));
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

    /// Uploads FULL and CLOSED zones at once, and any zones while buffered
    /// data exceeds the soft limit, up to `uploads` at once (SPEC.md §5).
    /// A new upload starts as soon as one ends.
    pub async fn background_flusher(self: Arc<Self>) {
        let mut running = futures_util::stream::FuturesUnordered::new();
        let mut busy = vec![false; self.zones.len()];
        // Bytes the running uploads were picked for (buffered at pick time).
        let mut in_flight = 0u64;
        loop {
            while running.len() < self.uploads {
                let over = self.dirty_bytes().saturating_sub(in_flight) > self.dirty_limit;
                let Some((zi, bytes, reason)) = self.background_pick(&busy, over) else {
                    break;
                };
                busy[zi] = true;
                in_flight += bytes;
                let engine: &Engine = &self;
                running.push(async move { (zi, bytes, engine.flush_zone(zi, reason).await) });
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

    /// Next zone for the background flusher among those not being uploaded,
    /// with its buffered bytes and the upload reason. FULL and CLOSED zones go
    /// first and always; open ones only when `over_limit`, the largest buffer
    /// first.
    fn background_pick(&self, busy: &[bool], over_limit: bool) -> Option<(usize, u64, Reason)> {
        (0..self.zones.len())
            .filter(|&i| !busy[i])
            .filter_map(|i| {
                let st = self.zones[i].state.lock().unwrap();
                let b = st.buffered();
                let done = match st.cond {
                    Cond::Full => Some(Reason::ZoneFull),
                    Cond::Closed => Some(Reason::ZoneClosed),
                    _ => None,
                };
                (b > 0 && (done.is_some() || over_limit)).then_some((
                    (done.is_some(), b),
                    i,
                    done.unwrap_or(Reason::Background),
                ))
            })
            .max_by_key(|&(key, i, _)| (key, i))
            .map(|((_, b), i, reason)| (i, b, reason))
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

        // FINISH stores buffered data and puts the marker next to the zone.
        e.finish_zone(0).await.unwrap();
        let z = &e.report(0, 1)[0];
        assert_eq!((z.cond, z.wp), (Cond::Full, ZS));
        assert_eq!(e.dirty_bytes(), 0);
        let marker = "t/0000000000000000.finished";
        assert!(store.get(marker).await.unwrap().unwrap().is_empty());
        let data = read(&e, 0, 4 * B).await; // stored data, then zeros
        assert_eq!(&data[2 * B as usize..3 * B as usize], &blk(8, 1)[..]);
        assert!(data[3 * B as usize..].iter().all(|&b| b == 0));

        e.reset_zone(0).await.unwrap(); // object and marker go together
        assert_eq!(e.report(0, 1)[0].cond, Cond::Empty);
        assert!(store.get("t/0000000000000000").await.unwrap().is_none());
        assert!(store.get(marker).await.unwrap().is_none());
        assert!(read(&e, 0, 2 * B).await.iter().all(|&b| b == 0));

        e.write(0, blk(9, 1), false).unwrap(); // fresh object after reset: PUT
        e.flush_all(Reason::Explicit).await.unwrap();
        let obj = store.get("t/0000000000000000").await.unwrap().unwrap();
        assert_eq!(obj.len() as u64, B);
        assert!(store.get(marker).await.unwrap().is_none());

        // FINISH of an empty zone puts only the marker; RESET removes it.
        e.finish_zone(ZS).await.unwrap();
        assert!(store.get("t/0000000000100000").await.unwrap().is_none());
        assert!(
            store
                .get("t/0000000000100000.finished")
                .await
                .unwrap()
                .is_some()
        );
        e.reset_zone(ZS).await.unwrap();
        assert!(store.list("t/0000000000100000").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn read_ahead_windows() {
        let store = mem();
        let ra = ReadAhead {
            window: 4 * B,
            cache: 64 * B,
        };
        let e = engine_with(store.clone())
            .await
            .with_read_ahead(ra)
            .unwrap();
        let gets = || mem_store(&store).range_gets();
        let data: Vec<u8> = (0..18 * B).map(|i| (i / B) as u8).collect();
        let blocks =
            |from: u64, n: u64| data[(from * B) as usize..((from + n) * B) as usize].to_vec();
        e.write(0, Bytes::from(data[..(16 * B) as usize].to_vec()), false)
            .unwrap();
        e.flush_all(Reason::Explicit).await.unwrap();

        assert_eq!(read(&e, B, B).await, blocks(1, 1)); // fetches window 0 (blocks 0–3)
        assert_eq!(gets(), 1);
        assert_eq!(read(&e, 2 * B, B).await, blocks(2, 1)); // from the cache
        assert_eq!(read(&e, 3 * B, 2 * B).await, blocks(3, 2)); // windows 0 and 1
        assert_eq!(gets(), 2);
        assert_eq!(read(&e, 4 * B, 4 * B).await, blocks(4, 4)); // a whole window: plain GET
        assert_eq!(gets(), 3);

        // The window past the stored data is not cached: it may still grow.
        e.write(
            16 * B,
            Bytes::from(data[(16 * B) as usize..].to_vec()),
            false,
        )
        .unwrap();
        e.flush_all(Reason::Explicit).await.unwrap();
        assert_eq!(read(&e, 16 * B, B).await, blocks(16, 1));
        assert_eq!(read(&e, 17 * B, B).await, blocks(17, 1));
        assert_eq!(gets(), 5);

        // After RESET the zone's new data is read, not the cached windows.
        e.reset_zone(0).await.unwrap();
        e.write(0, blk(9, 8), false).unwrap();
        e.flush_all(Reason::Explicit).await.unwrap();
        assert_eq!(read(&e, B, B).await, blk(9, 1).to_vec());
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
        e.finish_zone(3 * ZS).await.unwrap(); // marker only
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
    #[tokio::test]
    async fn background_flusher_uploads_done_zones_at_once() {
        let e = Arc::new(engine_with(mem()).await);
        tokio::spawn(e.clone().background_flusher());
        e.write(0, blk(1, ZS / B), false).unwrap(); // zone 0: FULL
        e.write(ZS, blk(2, 8), false).unwrap(); // zone 1: closed below
        e.close_zone(ZS).unwrap();
        e.write(2 * ZS, blk(3, 8), false).unwrap(); // zone 2: open
        tokio::time::timeout(Duration::from_secs(5), async {
            while e.unflushed().len() > 1 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        // All under the limit: only the open zone waits for more data.
        assert_eq!(e.unflushed(), vec![(2, 8 * B)]);
    }

    #[tokio::test]
    async fn uploads_share_one_limit() {
        let e = Arc::new(engine_with(mem()).await.with_uploads(2));
        e.write(0, blk(1, 8), false).unwrap();
        let taken = e.upload_slots.acquire_many(2).await.unwrap();
        let flush = tokio::spawn({
            let e = e.clone();
            async move { e.flush_all(Reason::Explicit).await }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!flush.is_finished(), "upload ran without a free slot");
        drop(taken);
        flush.await.unwrap().unwrap();
        assert!(e.unflushed().is_empty());
    }
    #[tokio::test]
    async fn large_read_goes_out_in_parts() {
        let store = mem();
        let e = engine_with(store.clone()).await.with_get_part(16 * B);
        for i in 0..5 {
            e.write(0, blk(i, 8), true).unwrap();
        }
        e.flush_all(Reason::Explicit).await.unwrap();
        // 38 blocks from block 2: parts of 16, 16 and 6 blocks.
        let data = read(&e, 2 * B, 38 * B).await;
        assert_eq!(mem_store(&store).range_gets(), 3);
        let want: Vec<u8> = (0..5u8)
            .flat_map(|i| vec![i; (8 * B) as usize])
            .skip((2 * B) as usize)
            .collect();
        assert_eq!(data, want);
    }
}

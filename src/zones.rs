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

use crate::config::{AppendMode, BLOCK_SIZE, DiskConfig};
use crate::readahead::{ReadAhead, ReadCache};
use crate::stats::{InFlight, Reason, STATS, ZoneCounts};
use crate::store::{Body, Layout, PendingConcat, Store, StoreError};
use bytes::Bytes;
use futures_util::StreamExt;
use log::{debug, info, warn};
use std::collections::{BTreeSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{Notify, Semaphore};

/// S3 requests run at once without a link to tune for: uploads of the
/// temporary `create` device and tests. Judgement call: enough to keep a link
/// busy with zone-sized bodies, few enough not to hit S3 request rate limits.
/// With a link, uploads and GETs follow it (SPEC.md §6.1).
const S3_PARALLELISM: usize = 16;

/// Shared zero-filled buffer for reads past the write pointer; a larger read
/// is served as several 1 MiB pieces. Size is a judgement call: 1 MiB of static
/// memory, at most 32 iovecs for the largest (32 MiB) request.
static ZEROS: [u8; 1 << 20] = [0; 1 << 20];

/// When to concatenate a zone (SPEC.md §5). Judgement calls agreed with the
/// user. Segments: more than `COMPACT_READ_FACTOR` × (zone / GET part) — a
/// sequential read of the zone then pays at most ~1/4 extra GETs — within
/// the bounds; metadata zones `COMPACT_META_FACTOR` times as many, since
/// their reads are 16 KiB nodes that never span segments.
const COMPACT_READ_FACTOR: u64 = 4;
const COMPACT_SEGMENTS_MIN: usize = 8;
const COMPACT_SEGMENTS_MAX: usize = 1024;
const COMPACT_META_FACTOR: usize = 4;
/// Count-triggered concatenation waits until at least 1/this of what is
/// already concatenated was added: no copying a whole zone for a crumb.
const COMPACT_GROWTH: u64 = 8;
/// Idle: no write for k × the zone's average write interval, within bounds.
const IDLE_DATA: (f64, u64, u64) = (4.0, 30, 600);
const IDLE_META: (f64, u64, u64) = (8.0, 300, 3600);
/// Weight of a new interval in the zone's average (EWMA).
const IDLE_EWMA_WEIGHT: f64 = 0.2;
/// How often the compactor looks for idle fragmented zones.
const IDLE_SCAN: Duration = Duration::from_secs(5);

/// Seconds since boot, sleep included (`CLOCK_BOOTTIME`): a laptop that
/// slept leaves its zones idle for that long too.
fn boottime() -> f64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: plain syscall filling a timespec we own.
    unsafe { libc::clock_gettime(libc::CLOCK_BOOTTIME, &mut ts) };
    ts.tv_sec as f64 + ts.tv_nsec as f64 / 1e9
}

/// What a zone holds, as its requests tell (SPEC.md §5).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// No request since the daemon started.
    Unknown,
    Data,
    /// btrfs metadata: a request flagged META, or a plain WRITE.
    Meta,
}

/// Why a zone was queued for concatenation.
#[derive(Clone, Copy, Debug)]
pub enum CompactWhy {
    Full,
    ManySegments,
    Idle,
    Button,
}

/// Concatenations running at once. They copy on the server, so this only
/// bounds the S3 request rate. Judgement call.
const COMPACT_PARALLELISM: usize = 4;

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
    /// A change to a disk served read-only (SPEC.md §9.2).
    #[error("the disk is read-only")]
    ReadOnly,
}

pub type ZResult<T> = Result<T, ZoneError>;

fn invalid<T>(msg: String) -> ZResult<T> {
    Err(ZoneError::Invalid(msg))
}

fn broken<T>(zi: usize) -> ZResult<T> {
    invalid(format!("zone {zi} needs a RESET: the previous one failed"))
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

/// `/zones.json`: per zone, in zone order. Sizes in blocks keep it small.
#[derive(serde::Serialize)]
pub struct ZoneMap {
    pub zone_size: u64,
    pub block_size: u64,
    /// One letter per zone: `e`mpty, `o`pen, `c`losed, `f`ull.
    pub cond: String,
    /// One letter per zone: `d`ata, `m`etadata, `u`nknown (no request since start).
    pub kind: String,
    /// Blocks holding data (stored or in RAM).
    pub used: Vec<u32>,
    /// S3 objects serving the zone.
    pub objects: Vec<u32>,
    /// Blocks in RAM, not stored yet.
    pub buffered: Vec<u32>,
}

/// `/zone-<n>.json`, the block level part: one zone in full.
#[derive(serde::Serialize)]
pub struct ZoneDetail {
    pub index: usize,
    /// Byte offset on the device.
    pub start: u64,
    pub size: u64,
    pub cond: &'static str,
    /// Bytes written by the kernel (stored or in RAM).
    pub written: u64,
    /// Bytes confirmed by S3.
    pub stored: u64,
    /// Bytes in RAM, not in S3 yet.
    pub buffered: u64,
    pub kind: &'static str,
    /// S3 objects serving the zone and its markers (`from = to = 0`).
    pub objects: Vec<ObjectDetail>,
    /// Since the last write; `None`: not written since the daemon started.
    pub secs_since_write: Option<f64>,
    /// Average interval between writes.
    pub write_interval_secs: Option<f64>,
    pub compaction_queued: bool,
    /// A RESET failed: I/O is refused until one succeeds.
    pub broken: bool,
    /// GETs of the zone's objects in progress.
    pub gets: u64,
    /// Writing S3 requests on the zone in progress, by what they do.
    pub writing: Vec<&'static str>,
}

#[derive(serde::Serialize)]
pub struct ObjectDetail {
    pub key: String,
    /// Object size; 0 for markers.
    pub size: u64,
    /// Zone bytes `[from, to)` the object serves.
    pub from: u64,
    pub to: u64,
}

/// A part of a read request.
#[derive(Debug)]
pub enum Piece {
    Store {
        key: String,
        /// Offset in the object.
        offset: u64,
        len: u64,
        /// Offset in the zone the piece starts at.
        zone_offset: u64,
        /// Zone index and generation, for the read-ahead cache key.
        zone: usize,
        generation: u64,
        /// Stored bytes of the zone when the plan was made.
        flushed: u64,
    },
    Mem(Bytes),
    Zero(u64),
}

/// A store object serving `[from, to)` of its zone. It begins at `start`
/// (in its key on a SEGMENTS disk), before `from` when an earlier object
/// already serves that part.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Stored {
    start: u64,
    from: u64,
    to: u64,
}

/// Picks the objects serving a zone from the `(start, size)` the store lists
/// (SPEC.md §5): from offset 0 on, each step takes the object covering the
/// current end that reaches furthest. The rest are returned to delete:
/// objects within the covered part are leftovers of a concatenation, objects
/// past a gap leftovers of an interrupted RESET (DeleteObjects is not atomic;
/// listings are strongly consistent, so nothing else leaves a gap) — kept,
/// they would join the zone once it grew up to them.
fn cover(mut found: Vec<(u64, u64)>) -> (Vec<Stored>, Vec<u64>) {
    found.sort_unstable();
    let mut chosen: Vec<Stored> = Vec::new();
    let mut end = 0;
    while let Some(&(start, size)) = found
        .iter()
        .filter(|&&(start, size)| start <= end && start + size > end)
        .max_by_key(|&&(start, size)| start + size)
    {
        chosen.push(Stored {
            start,
            from: end,
            to: start + size,
        });
        end = start + size;
    }
    let leftovers = found
        .iter()
        .filter(|&&(start, _)| !chosen.iter().any(|c| c.start == start))
        .map(|&(start, _)| start)
        .collect();
    (chosen, leftovers)
}

/// Zones waiting for concatenation: urgent (FULL) ones first.
struct CompactQueue {
    urgent: VecDeque<usize>,
    normal: VecDeque<usize>,
    queued: Vec<bool>,
}

/// A concatenation whose parts are copied, waiting to complete.
struct CompactJob {
    generation: u64,
    parts: Vec<Stored>,
    pending: PendingConcat,
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
    /// Objects holding `[0, flushed)`, by offset: one zone object, or the
    /// segments of a SEGMENTS disk (SPEC.md §5).
    objects: Vec<Stored>,
    /// The FINISH marker `<zone>.finished` may exist in the store (SPEC.md
    /// §3.3): set before its PUT, so a PUT that failed after reaching the
    /// store still has its marker deleted by RESET.
    marker: bool,
    /// Bumped on every RESET; part of the read-ahead cache key.
    generation: u64,
    kind: Kind,
    /// The metadata marker `<zone>.meta` exists in the store (SEGMENTS only).
    meta_marker: bool,
    /// Last write (`boottime()`); the daemon start for zones not written since.
    last_write: f64,
    /// Average interval between writes, seconds; `None` before the second write.
    write_interval: Option<f64>,
    /// A write since the daemon started (`last_write` is a write, not the start).
    written: bool,
    /// A RESET failed with some of the zone's objects maybe deleted: I/O is
    /// refused until a RESET succeeds (SPEC.md §5).
    broken: bool,
}

impl ZoneState {
    fn note_write(&mut self, now: f64) {
        if self.written {
            let gap = (now - self.last_write).max(0.0);
            self.write_interval = Some(match self.write_interval {
                Some(avg) => avg + IDLE_EWMA_WEIGHT * (gap - avg),
                None => gap,
            });
        }
        self.last_write = now;
        self.written = true;
    }

    fn buffered(&self) -> u64 {
        self.data_end - self.flushed
    }

    /// EMPTY with nothing stored or buffered: no object of the zone exists
    /// (startup deleted any leftovers), so RESET has nothing to do.
    fn is_clean(&self) -> bool {
        !self.broken
            && self.cond == Cond::Empty
            && self.data_end == 0
            && self.objects.is_empty()
            && !self.marker
            && !self.meta_marker
    }
}

struct Zone {
    start: u64,
    state: Mutex<ZoneState>,
    upload: tokio::sync::Mutex<()>,
    requests: ZoneRequests,
}

/// S3 requests on one zone in progress, for the zone page (SPEC.md §9.3).
#[derive(Default)]
struct ZoneRequests {
    gets: InFlight,
    uploads: InFlight,
    concats: InFlight,
    markers: InFlight,
    deletes: InFlight,
}

pub struct Engine {
    zone_size: u64,
    disk_size: u64,
    dirty_limit: u64,
    layout: Layout,
    store: Arc<Store>,
    zones: Vec<Zone>,
    /// Zones with buffered data, so uploads need not scan every zone.
    /// Changed only under the zone's `state` lock, taken after it.
    buffered_zones: Mutex<BTreeSet<usize>>,
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
    /// GETs at once, every read together (SPEC.md §6.1).
    reads: usize,
    get_slots: Semaphore,
    /// Store reads larger than this go out as parallel GETs of this size
    /// (SPEC.md §6.2).
    get_part: u64,
    /// A SEGMENTS disk: every upload is a new object (SPEC.md §5).
    segments: bool,
    compact_queue: Mutex<CompactQueue>,
    /// Wakes the compactor.
    compact_kick: Notify,
    /// A zone became metadata and needs its marker at once.
    meta_pending: std::sync::atomic::AtomicBool,
    /// Wakes the marker loop of the compactor.
    meta_kick: Notify,
    /// Served read-only: changes are refused, the store only read (SPEC.md §9.2).
    read_only: bool,
}

impl Engine {
    /// Restores zone state from the objects present in the store: the object
    /// size is the write pointer, a `<zone>.finished` marker a zone closed by
    /// FINISH.
    pub async fn open(cfg: &DiskConfig, layout: Layout, store: Arc<Store>) -> anyhow::Result<Self> {
        Self::restore(cfg, layout, store, false).await
    }

    /// `open` for a disk served read-only (SPEC.md §9.2): the store is only
    /// read, leftovers are left in place, every change is refused.
    pub async fn open_read_only(
        cfg: &DiskConfig,
        layout: Layout,
        store: Arc<Store>,
    ) -> anyhow::Result<Self> {
        Self::restore(cfg, layout, store, true).await
    }

    /// Refuses a change to a disk served read-only.
    fn writable(&self) -> ZResult<()> {
        if self.read_only {
            return Err(ZoneError::ReadOnly);
        }
        Ok(())
    }

    async fn restore(
        cfg: &DiskConfig,
        layout: Layout,
        store: Arc<Store>,
        read_only: bool,
    ) -> anyhow::Result<Self> {
        let started = boottime();
        let nr_zones = cfg.nr_zones() as usize;
        let segments = cfg.append == AppendMode::Segments;
        // Per zone: (start, size) of its objects and whether a FINISH marker exists.
        let mut found: Vec<Vec<(u64, u64)>> = vec![Vec::new(); nr_zones];
        let mut finished = vec![false; nr_zones];
        let mut meta = vec![false; nr_zones];
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
            if let Some(offset) = layout.parse_meta_key(&obj.key) {
                meta[zone_index(offset, &obj.key)?] = true;
                continue;
            }
            let parsed = if segments {
                layout.parse_segment_key(&obj.key)
            } else {
                layout.parse_zone_key(&obj.key).map(|zone| (zone, 0))
            };
            let Some((zone, start)) = parsed else {
                if obj.key != config_key {
                    warn!("ignoring unexpected object {}", obj.key);
                }
                continue;
            };
            anyhow::ensure!(
                start
                    .checked_add(obj.size)
                    .is_some_and(|end| end <= cfg.zone_size)
                    && start.is_multiple_of(BLOCK_SIZE)
                    && obj.size.is_multiple_of(BLOCK_SIZE),
                "object {} has invalid size {}",
                obj.key,
                obj.size
            );
            found[zone_index(zone, &obj.key)?].push((start, obj.size));
        }

        let mut leftovers = Vec::new();
        let (mut full, mut empty, mut closed) = (0, 0, 0);
        let mut zones = Vec::with_capacity(nr_zones);
        let finished_found = finished;
        for (i, (objects, &finished)) in found.into_iter().zip(&finished_found).enumerate() {
            let start = i as u64 * cfg.zone_size;
            let (objects, garbage) = cover(objects);
            let end = objects.last().map_or(0, |o| o.to);
            for g in garbage {
                // PATCH: an empty zone object, the only object of its zone.
                let key = if segments {
                    layout.segment_key(start, g)
                } else {
                    layout.zone_key(start)
                };
                if g >= end {
                    warn!(
                        "zone {i}: {key} lies past the zone's data at {end:#x}, \
                         left by an interrupted RESET; deleting it"
                    );
                }
                leftovers.push(key);
            }
            let size = objects.last().map_or(0, |o| o.to);
            // RESET deletes the object at the zone start first, so markers
            // without data are what an interrupted RESET left (SPEC.md §5).
            let is_meta = meta[i] && size > 0;
            if meta[i] && !is_meta {
                leftovers.push(layout.meta_key(start));
            }
            let finished = finished && size > 0;
            if finished_found[i] && !finished {
                warn!(
                    "zone {i}: FINISH marker without data, left by an interrupted RESET; deleting it"
                );
                leftovers.push(layout.finished_key(start));
            }
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
            zones.push(Zone {
                start,
                state: Mutex::new(ZoneState {
                    cond,
                    wp,
                    data_end: size,
                    flushed: size,
                    chunks: VecDeque::new(),
                    objects,
                    marker: finished,
                    generation: 0,
                    kind: if is_meta { Kind::Meta } else { Kind::Unknown },
                    meta_marker: is_meta,
                    last_write: started,
                    write_interval: None,
                    written: false,
                    broken: false,
                }),
                upload: tokio::sync::Mutex::new(()),
                requests: ZoneRequests::default(),
            });
        }
        if read_only {
            // Not part of any zone above; nothing can join them on a disk
            // that is never written.
            if !leftovers.is_empty() {
                info!(
                    "read-only: {} leftover objects left in place",
                    leftovers.len()
                );
            }
        } else {
            // Leftovers of interrupted concatenations and RESETs (SPEC.md §5),
            // gone before any new data can join them.
            if !leftovers.is_empty() {
                store.delete_many(&leftovers).await?;
                info!("deleted {} leftover objects", leftovers.len());
            }
            // In every append mode: whatever wrote the disk before, no
            // unfinished upload of its zones outlives a start.
            let aborted = store.abort_stale_concats(&layout).await?;
            if aborted > 0 {
                info!("aborted {aborted} unfinished concatenations");
            }
        }

        let engine = Self {
            zone_size: cfg.zone_size,
            disk_size: cfg.disk_size,
            dirty_limit: cfg.dirty_limit_bytes,
            layout,
            store,
            zones,
            buffered_zones: Mutex::new(BTreeSet::new()),
            dirty: AtomicU64::new(0),
            room: Notify::new(),
            kick: Notify::new(),
            read_cache: None,
            uploads: S3_PARALLELISM,
            upload_slots: Semaphore::new(S3_PARALLELISM),
            reads: S3_PARALLELISM,
            get_slots: Semaphore::new(S3_PARALLELISM),
            get_part: u64::MAX,
            segments,
            compact_queue: Mutex::new(CompactQueue {
                urgent: VecDeque::new(),
                normal: VecDeque::new(),
                queued: vec![false; nr_zones],
            }),
            compact_kick: Notify::new(),
            meta_pending: std::sync::atomic::AtomicBool::new(false),
            meta_kick: Notify::new(),
            read_only,
        };
        // Zones restored with several segments: concatenate them.
        for (zi, zone) in engine.zones.iter().enumerate() {
            let st = zone.state.lock().unwrap();
            if let Some((urgent, why)) = engine.compaction_due(&st) {
                drop(st);
                engine.queue_compaction(zi, urgent, why);
            }
        }
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

    /// GETs at once, every read together.
    pub fn with_reads(mut self, reads: usize) -> Self {
        self.reads = reads.max(1);
        self.get_slots = Semaphore::new(self.reads);
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
    /// Counts the data as buffered without waiting for room, as file data
    /// (tests).
    #[cfg(test)]
    pub fn write(&self, offset: u64, data: Bytes, append: bool) -> ZResult<u64> {
        let len = data.len() as u64;
        self.dirty.fetch_add(len, Ordering::SeqCst);
        self.write_reserved(offset, data, append, false)
            .inspect_err(|_| self.unreserve(len))
    }

    /// `write` for data whose room `reserve` has taken: on error the caller
    /// gives the room back with `unreserve`. `meta`: the request carries
    /// btrfs metadata (META flag or a plain WRITE).
    pub fn write_reserved(
        &self,
        offset: u64,
        data: Bytes,
        append: bool,
        meta: bool,
    ) -> ZResult<u64> {
        self.writable()?;
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
        if st.broken {
            return broken(zi);
        }
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
        if st.buffered() == 0 {
            self.buffered_zones.lock().unwrap().insert(zi);
        }
        st.chunks.push_back(data);
        st.wp += len;
        st.data_end += len;
        st.note_write(boottime());
        let became_meta = meta && st.kind != Kind::Meta;
        if meta {
            st.kind = Kind::Meta;
        } else if st.kind == Kind::Unknown {
            st.kind = Kind::Data;
        }
        let full = st.wp == self.zone_size;
        if full {
            st.cond = Cond::Full;
        }
        drop(st);
        if became_meta {
            self.mark_meta_soon();
        }

        if full || self.dirty_bytes() > self.dirty_limit {
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
        if st.broken {
            return broken(zi);
        }
        let mut pieces = Vec::new();
        let mut pos = rel;

        // Stored data: a piece per object it spans.
        let stop = end.min(st.flushed);
        for o in &st.objects {
            if o.to <= pos || o.from >= stop {
                continue;
            }
            let to = stop.min(o.to);
            pieces.push(Piece::Store {
                key: self.object_key(zone.start, o),
                offset: pos - o.start,
                len: to - pos,
                zone_offset: pos,
                zone: zi,
                generation: st.generation,
                flushed: st.flushed,
            });
            pos = to;
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

    /// Resolves a read plan into buffers (fetching store ranges), pieces in
    /// parallel.
    pub async fn fetch(&self, pieces: Vec<Piece>) -> ZResult<Vec<Bytes>> {
        let futures: Vec<_> = pieces.into_iter().map(|p| self.fetch_piece(p)).collect();
        in_order(futures).await
    }

    async fn fetch_piece(&self, piece: Piece) -> ZResult<Vec<Bytes>> {
        match piece {
            Piece::Store {
                key,
                offset,
                len,
                zone_offset,
                zone,
                generation,
                flushed,
            } => match &self.read_cache {
                // Only small reads, and only windows wholly stored: they never change.
                Some(c)
                    if len < c.window()
                        && (zone_offset + len).next_multiple_of(c.window()) <= flushed =>
                {
                    self.read_windows(c, (zone, generation), zone_offset, len)
                        .await
                }
                _ => match self.get_parts(zone, &key, offset, len).await {
                    // A segment concatenated and deleted meanwhile: plan anew.
                    Err(ZoneError::Store(StoreError::NotFound(_))) if self.segments => {
                        self.read_stored(zone, zone_offset, len).await
                    }
                    got => got,
                },
            },
            Piece::Mem(b) => {
                STATS.read_from_ram.add(b.len() as u64);
                Ok(vec![b])
            }
            Piece::Zero(len) => {
                STATS.read_zeros.add(len);
                Ok(zeros(len).collect())
            }
        }
    }

    /// `[offset, offset + len)` of an object as parallel GETs of `get_part`.
    async fn get_parts(&self, zi: usize, key: &str, offset: u64, len: u64) -> ZResult<Vec<Bytes>> {
        let end = offset + len;
        let parts = (offset..end)
            .step_by(self.get_part as usize)
            .map(|from| self.get(zi, key, from, self.get_part.min(end - from)));
        futures_util::future::try_join_all(parts).await
    }

    /// `[from, from + len)` of a zone's stored data, planned from its objects
    /// as they are now; planned anew when a segment is concatenated and
    /// deleted during the read.
    async fn read_stored(&self, zi: usize, from: u64, len: u64) -> ZResult<Vec<Bytes>> {
        let zone = &self.zones[zi];
        let mut previous = None;
        loop {
            let parts: Vec<(String, u64, u64)> = {
                let st = zone.state.lock().unwrap();
                let end = from + len;
                if end > st.flushed {
                    return invalid(format!("zone {zi} was reset during a read"));
                }
                st.objects
                    .iter()
                    .filter(|o| o.to > from && o.from < end)
                    .map(|o| {
                        let (a, b) = (from.max(o.from), end.min(o.to));
                        (self.object_key(zone.start, o), a - o.start, b - a)
                    })
                    .collect()
            };
            let futures: Vec<_> = parts
                .iter()
                .map(|(key, offset, n)| self.get_parts(zi, key, *offset, *n))
                .collect();
            match in_order(futures).await {
                Ok(out) => return Ok(out),
                // Concatenated meanwhile: the objects changed, plan anew. The
                // same plan failing twice is a real error.
                Err(ZoneError::Store(StoreError::NotFound(_)))
                    if self.segments && previous.as_ref() != Some(&parts) =>
                {
                    previous = Some(parts)
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Key of a stored object of the zone starting at `zone_start`.
    fn object_key(&self, zone_start: u64, o: &Stored) -> String {
        if self.segments {
            self.layout.segment_key(zone_start, o.start)
        } else {
            self.layout.zone_key(zone_start)
        }
    }

    /// One GET of zone `zi`, counted in the statistics; waits for one of `reads` slots.
    async fn get(&self, zi: usize, key: &str, offset: u64, len: u64) -> ZResult<Bytes> {
        // Never closed, so acquiring cannot fail.
        let _slot = self.get_slots.acquire().await.unwrap();
        let _in_flight = self.zones[zi].requests.gets.enter();
        let started = Instant::now();
        let data = self
            .store
            .get_range(key, offset, len)
            .await
            // 404 is no error on a SEGMENTS disk: a concatenated segment, read anew.
            .inspect_err(|e| {
                if !matches!(e, StoreError::NotFound(_)) {
                    STATS.err_read.record(e)
                }
            })?;
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
        (zone, generation): (usize, u64),
        offset: u64,
        len: u64,
    ) -> ZResult<Vec<Bytes>> {
        let w = cache.window();
        let end = offset + len;
        let windows: Vec<_> = (offset / w..end.div_ceil(w))
            .map(|wi| async move {
                let start = wi * w;
                let slot = cache.slot((zone, generation, wi));
                let mut fetched = false;
                let data = slot
                    .get_or_try_init(|| {
                        fetched = true;
                        async move {
                            let parts = self.read_stored(zone, start, w).await?;
                            Ok::<_, ZoneError>(match parts.as_slice() {
                                [one] => one.clone(),
                                _ => Bytes::from(parts.concat()),
                            })
                        }
                    })
                    .await?;
                let (from, to) = (offset.max(start) - start, end.min(start + w) - start);
                if fetched {
                    STATS.read_ahead_misses.inc();
                } else {
                    STATS.read_ahead_hits.inc();
                    STATS.read_from_cache.add(to - from);
                }
                Ok(vec![data.slice(from as usize..to as usize)])
            })
            .collect();
        in_order(windows).await
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
        let started = Instant::now();
        let uploaded = {
            // Never closed, so acquiring cannot fail.
            let _slot = self.upload_slots.acquire().await.unwrap();
            let _in_flight = (STATS.uploads.enter(), zone.requests.uploads.enter());
            if self.segments {
                let key = self.layout.segment_key(zone.start, from);
                self.store.put(&key, body).await
            } else {
                let key = self.layout.zone_key(zone.start);
                self.store.append(&key, from, body).await
            }
        };
        if let Err(e) = uploaded {
            STATS.upload_errors.inc();
            STATS.err_upload.record(&e);
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

        let compaction = {
            let mut st = zone.state.lock().unwrap();
            // Only the holder of `upload` removes chunks, so the front ones are ours.
            st.chunks.drain(..nr_chunks);
            st.flushed = from + len;
            if st.buffered() == 0 {
                self.buffered_zones.lock().unwrap().remove(&zi);
            }
            let stored = Stored {
                start: if self.segments { from } else { 0 },
                from,
                to: from + len,
            };
            if self.segments || st.objects.is_empty() {
                st.objects.push(stored);
            } else {
                st.objects[0].to = stored.to;
            }
            self.compaction_due(&st)
        };
        if let Some((urgent, why)) = compaction {
            self.queue_compaction(zi, urgent, why);
        }
        self.dirty.fetch_sub(len, Ordering::SeqCst);
        self.room.notify_waiters();
        Ok(())
    }

    /// Uploads every zone with buffered data; fails if any upload failed.
    pub async fn flush_all(&self, reason: Reason) -> ZResult<()> {
        let dirty = self.buffered_zone_list();
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
        self.writable()?;
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
        self.writable()?;
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
    /// restart (SPEC.md §3.3). A zone without data gets no marker: a marker
    /// without data is a leftover of a RESET, and the zone is EMPTY again
    /// after a restart.
    pub async fn finish_zone(&self, offset: u64) -> ZResult<()> {
        self.writable()?;
        let (zi, _) = self.locate(offset)?;
        let zone = &self.zones[zi];
        let _upload = zone.upload.lock().await;
        {
            let mut st = zone.state.lock().unwrap();
            if st.broken {
                return broken(zi);
            }
            if st.cond == Cond::Full {
                return Ok(());
            }
            if st.data_end == 0 {
                st.cond = Cond::Full;
                st.wp = self.zone_size;
                return Ok(());
            }
        }
        self.flush_locked(zi, Reason::Finish).await?;
        zone.state.lock().unwrap().marker = true;
        {
            let _in_flight = zone.requests.markers.enter();
            self.store
                .put(&self.layout.finished_key(zone.start), Body::default())
                .await?;
        }

        let compaction = {
            let mut st = zone.state.lock().unwrap();
            st.cond = Cond::Full;
            st.wp = self.zone_size;
            self.compaction_due(&st)
        };
        if let Some((urgent, why)) = compaction {
            self.queue_compaction(zi, urgent, why);
        }
        debug!("zone {zi} finished");
        Ok(())
    }

    /// RESET: deletes the zone's objects and markers, drops buffered data.
    pub async fn reset_zone(&self, offset: u64) -> ZResult<()> {
        self.writable()?;
        let (zi, _) = self.locate(offset)?;
        self.reset_index(zi).await
    }

    async fn reset_index(&self, zi: usize) -> ZResult<()> {
        let zone = &self.zones[zi];
        // Holding `upload` keeps flushes, marker PUTs and concatenations of
        // the zone out until the state change.
        let _upload = zone.upload.lock().await;
        let marker = {
            let mut st = zone.state.lock().unwrap();
            if st.is_clean() {
                return Ok(());
            }
            // A concatenation under way never completes now, even if the
            // RESET fails and leaves the rest of the state as it is.
            st.generation += 1;
            st.marker
        };
        let deleted = {
            let _in_flight = zone.requests.deletes.enter();
            self.delete_zone_objects(zone.start, marker).await
        };
        if let Err(e) = deleted {
            self.break_locked(zi);
            warn!("zone {zi}: RESET failed; I/O to the zone is refused until a RESET succeeds");
            return Err(e);
        }
        self.clear_locked(zi);
        debug!("zone {zi} reset");
        Ok(())
    }

    /// Deletes a zone's objects, the one at the zone start first and the
    /// rest only after it is gone: a RESET interrupted at any point leaves
    /// the zone either untouched or without its start, which restores as
    /// EMPTY with the rest deleted (SPEC.md §5).
    async fn delete_zone_objects(&self, zone_start: u64, marker: bool) -> ZResult<()> {
        if !self.segments {
            self.store.delete(&self.layout.zone_key(zone_start)).await?;
            if marker {
                self.store
                    .delete(&self.layout.finished_key(zone_start))
                    .await?;
            }
            return Ok(());
        }
        // Segments: everything under the zone's prefix, leftovers included;
        // the listing is complete, as everything that creates objects of the
        // zone waits for its `upload` lock.
        let first = self.layout.segment_key(zone_start, 0);
        let prefix = self.layout.zone_key(zone_start);
        let (deleted, listed) = tokio::join!(self.store.delete(&first), self.store.list(&prefix));
        deleted?;
        let rest: Vec<String> = listed?
            .into_iter()
            .map(|o| o.key)
            .filter(|k| *k != first && self.layout.zone_of(k) == Some(zone_start))
            .collect();
        match rest.as_slice() {
            [] => {}
            [key] => self.store.delete(key).await?,
            _ => self.store.delete_many(&rest).await?,
        }
        Ok(())
    }

    /// Makes the zone EMPTY and drops its buffer; the caller holds `upload`
    /// and has deleted the zone's objects.
    fn clear_locked(&self, zi: usize) {
        let (dropped, had_object) = {
            let mut st = self.zones[zi].state.lock().unwrap();
            let had_object = !st.objects.is_empty();
            st.generation += 1;
            let dropped = st.buffered();
            st.cond = Cond::Empty;
            st.wp = 0;
            st.data_end = 0;
            st.flushed = 0;
            st.chunks.clear();
            st.objects.clear();
            self.buffered_zones.lock().unwrap().remove(&zi);
            st.marker = false;
            st.kind = Kind::Unknown;
            st.meta_marker = false;
            st.last_write = boottime();
            st.write_interval = None;
            st.written = false;
            st.broken = false;
            (dropped, had_object)
        };
        self.forget_data(zi, dropped, had_object);
    }

    /// After a failed RESET, with the caller holding `upload`: some objects
    /// of the zone may be gone, so its data is dropped and I/O refused until
    /// a RESET succeeds; the condition and write pointer stay, so the kernel
    /// sees the zone not empty and resets it again before using it. Markers
    /// stay flagged for that RESET to delete.
    fn break_locked(&self, zi: usize) {
        let (dropped, had_object) = {
            let mut st = self.zones[zi].state.lock().unwrap();
            let had_object = !st.objects.is_empty();
            let dropped = st.buffered();
            st.chunks.clear();
            st.data_end = st.flushed;
            st.objects.clear();
            self.buffered_zones.lock().unwrap().remove(&zi);
            st.broken = true;
            (dropped, had_object)
        };
        self.forget_data(zi, dropped, had_object);
    }

    /// Releases a dropped buffer and cached data of a zone.
    fn forget_data(&self, zi: usize, dropped: u64, had_object: bool) {
        // Only stored data is cached.
        if let (Some(c), true) = (&self.read_cache, had_object) {
            c.forget_zone(zi);
        }
        if dropped > 0 {
            self.dirty.fetch_sub(dropped, Ordering::SeqCst);
            self.room.notify_waiters();
        }
    }

    /// RESET_ALL: every zone object of the disk (not its config, not disks
    /// nested under its prefix), the objects at zone starts first as in a RESET, in DeleteObjects batches of up to 1000
    /// keys instead of one DELETE per zone (SPEC.md §4).
    pub async fn reset_all(&self) -> ZResult<()> {
        self.writable()?;
        // Every `upload` lock, so nothing creates an object between the
        // delete and the state change. Taken in zone order; nothing else holds
        // two of them, so this cannot deadlock.
        let mut guards = Vec::with_capacity(self.zones.len());
        for zone in &self.zones {
            guards.push(zone.upload.lock().await);
            zone.state.lock().unwrap().generation += 1;
        }
        let deleted = self.delete_all_objects().await;
        for zi in 0..self.zones.len() {
            match deleted {
                Ok(_) => self.clear_locked(zi),
                // A clean zone had nothing to delete and stays usable: the
                // kernel would not reset a zone it sees EMPTY.
                Err(_) if self.zones[zi].state.lock().unwrap().is_clean() => {}
                Err(_) => self.break_locked(zi),
            }
        }
        if deleted.is_err() {
            warn!("RESET_ALL failed; I/O to every zone is refused until it is reset");
        }
        // Not inside `debug!`: its arguments are not evaluated when debug is off.
        let deleted = deleted?;
        debug!("all zones reset, {deleted} objects deleted");
        Ok(())
    }

    async fn delete_all_objects(&self) -> ZResult<usize> {
        let listed = self.store.list(&self.layout.dir()).await?;
        let (first, rest): (Vec<String>, Vec<String>) = listed
            .into_iter()
            .map(|o| o.key)
            .filter(|k| self.layout.zone_of(k).is_some())
            .partition(|k| {
                if self.segments {
                    self.layout
                        .parse_segment_key(k)
                        .is_some_and(|(_, at)| at == 0)
                } else {
                    self.layout.parse_zone_key(k).is_some()
                }
            });
        self.store.delete_many(&first).await?;
        self.store.delete_many(&rest).await?;
        Ok(first.len() + rest.len())
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

    /// Buffered data writes may not take beyond: twice the soft limit.
    pub fn hard_limit(&self) -> u64 {
        self.dirty_limit.saturating_mul(2)
    }

    /// Takes `len` bytes of buffer room for a write if it stays within the
    /// hard limit, or if nothing is buffered (a single request larger than
    /// the room must pass too). Atomic: concurrent writes cannot all pass on
    /// the same free room (SPEC.md §5).
    pub fn try_reserve(&self, len: u64) -> bool {
        self.dirty
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |cur| {
                (cur == 0 || cur + len <= self.hard_limit()).then_some(cur + len)
            })
            .is_ok()
    }

    /// `try_reserve`, waiting for uploads to free room.
    pub async fn reserve(&self, len: u64) {
        loop {
            let notified = self.room.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.try_reserve(len) {
                return;
            }
            self.kick.notify_one();
            notified.await;
        }
    }

    /// Gives back room taken for a write that failed.
    pub fn unreserve(&self, len: u64) {
        self.dirty.fetch_sub(len, Ordering::SeqCst);
        self.room.notify_waiters();
    }

    /// Uploads FULL and CLOSED zones at once, and any zones while buffered
    /// data exceeds the soft limit, up to `uploads` at once (SPEC.md §5).
    /// A new upload starts as soon as one ends.
    pub async fn background_flusher(self: Arc<Self>) {
        let mut running = futures_util::stream::FuturesUnordered::new();
        let mut busy = vec![false; self.zones.len()];
        // Bytes the running uploads were picked for (buffered at pick time).
        let mut in_flight = 0u64;
        // After a failure no upload starts before this; running ones go on.
        let mut paused_until: Option<tokio::time::Instant> = None;
        loop {
            if paused_until.is_some_and(|t| t <= tokio::time::Instant::now()) {
                paused_until = None;
            }
            while paused_until.is_none() && running.len() < self.uploads {
                let over = self.dirty_bytes().saturating_sub(in_flight) > self.dirty_limit;
                let Some((zi, bytes, reason)) = self.background_pick(&busy, over) else {
                    break;
                };
                busy[zi] = true;
                in_flight += bytes;
                let engine: &Engine = &self;
                running.push(async move { (zi, bytes, engine.flush_zone(zi, reason).await) });
            }
            tokio::select! {
                Some((zi, bytes, res)) = running.next(), if !running.is_empty() => {
                    busy[zi] = false;
                    in_flight -= bytes;
                    if let Err(e) = res {
                        warn!("background flush of zone {zi} failed: {e}");
                        paused_until = Some(tokio::time::Instant::now() + BACKGROUND_RETRY_PAUSE);
                    }
                }
                // More data arrived: maybe room for another upload. `notify_one`
                // keeps a permit, so a kick sent meanwhile is not lost.
                _ = self.kick.notified() => {}
                _ = sleep_until_some(paused_until) => {}
            }
        }
    }

    /// Next zone for the background flusher among those not being uploaded,
    /// with its buffered bytes and the upload reason. FULL and CLOSED zones go
    /// first and always; open ones only when `over_limit`, the largest buffer
    /// first.
    fn background_pick(&self, busy: &[bool], over_limit: bool) -> Option<(usize, u64, Reason)> {
        self.buffered_zone_list()
            .into_iter()
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

    /// Segments above which a zone is concatenated (SPEC.md §5).
    fn segment_limit(&self, kind: Kind) -> usize {
        let data = (COMPACT_READ_FACTOR * self.zone_size / self.get_part)
            .clamp(COMPACT_SEGMENTS_MIN as u64, COMPACT_SEGMENTS_MAX as u64)
            as usize;
        if kind == Kind::Meta {
            data * COMPACT_META_FACTOR
        } else {
            data
        }
    }

    /// Whether a SEGMENTS zone needs concatenation now, how soon (`true`:
    /// at once) and why. Idleness is checked by `idle_due`.
    fn compaction_due(&self, st: &ZoneState) -> Option<(bool, CompactWhy)> {
        if !self.segments || st.objects.len() < 2 {
            return None;
        }
        let meta = st.kind == Kind::Meta;
        if st.cond == Cond::Full && !meta {
            return Some((true, CompactWhy::Full));
        }
        // What the first object, the last concatenation, already covers.
        let base = st.objects[0].to;
        let grown = (st.flushed - base) * COMPACT_GROWTH >= base;
        (st.objects.len() > self.segment_limit(st.kind) && grown)
            .then_some((false, CompactWhy::ManySegments))
    }

    /// Whether a fragmented zone has been left alone long enough (SPEC.md §5).
    fn idle_due(&self, st: &ZoneState, now: f64) -> bool {
        if !self.segments || st.objects.len() < 2 {
            return false;
        }
        let (k, min, max) = if st.kind == Kind::Meta {
            IDLE_META
        } else {
            IDLE_DATA
        };
        let wait = st
            .write_interval
            .map_or(min as f64, |avg| (k * avg).clamp(min as f64, max as f64));
        now - st.last_write >= wait
    }

    /// Puts `<zone>.meta` for metadata zones not marked yet (SEGMENTS; the
    /// compactor calls it periodically). Under the zone's `upload` lock, so a
    /// RESET, which deletes everything under the zone prefix, cannot run
    /// between the check and the PUT and leave a marker on an empty zone.
    async fn store_meta_markers(&self) {
        if !self.segments || self.read_only {
            return;
        }
        for zi in 0..self.zones.len() {
            let zone = &self.zones[zi];
            let unmarked = |st: &ZoneState| st.kind == Kind::Meta && !st.meta_marker && !st.broken;
            if !unmarked(&zone.state.lock().unwrap()) {
                continue;
            }
            let _upload = zone.upload.lock().await;
            if !unmarked(&zone.state.lock().unwrap()) {
                continue;
            }
            let _in_flight = zone.requests.markers.enter();
            match self
                .store
                .put(&self.layout.meta_key(zone.start), Body::default())
                .await
            {
                Ok(()) => zone.state.lock().unwrap().meta_marker = true,
                Err(e) => {
                    warn!("cannot mark zone {zi} as metadata: {e}");
                    return;
                }
            }
        }
    }

    /// Queues the idle fragmented zones (the compactor calls it periodically).
    fn queue_idle(&self) {
        let now = boottime();
        for zi in 0..self.zones.len() {
            let idle = self.idle_due(&self.zones[zi].state.lock().unwrap(), now);
            if idle {
                self.queue_compaction(zi, false, CompactWhy::Idle);
            }
        }
    }

    /// Queues every zone served by more than one object for concatenation
    /// at once, below the threshold too (the statistics page button).
    /// Returns how many were queued; none on a disk without segments or
    /// served read-only.
    pub fn compact_all(&self) -> usize {
        if !self.segments || self.read_only {
            return 0;
        }
        let fragmented: Vec<usize> = (0..self.zones.len())
            .filter(|&i| self.zones[i].state.lock().unwrap().objects.len() > 1)
            .collect();
        for &zi in &fragmented {
            self.queue_compaction(zi, true, CompactWhy::Button);
        }
        fragmented.len()
    }

    fn queue_compaction(&self, zi: usize, urgent: bool, why: CompactWhy) {
        let mut q = self.compact_queue.lock().unwrap();
        if q.queued[zi] {
            if !urgent || q.urgent.contains(&zi) {
                return;
            }
            q.normal.retain(|&z| z != zi);
        } else {
            STATS.compact_why(why).inc();
        }
        q.queued[zi] = true;
        if urgent {
            q.urgent.push_back(zi);
        } else {
            q.normal.push_back(zi);
        }
        drop(q);
        self.compact_kick.notify_one();
    }

    /// Next queued zone not being concatenated already.
    fn next_compaction(&self, busy: &[bool]) -> Option<usize> {
        let mut q = self.compact_queue.lock().unwrap();
        let pick = |list: &mut VecDeque<usize>| {
            let i = list.iter().position(|&z| !busy[z])?;
            list.remove(i)
        };
        let zi = pick(&mut q.urgent).or_else(|| pick(&mut q.normal))?;
        q.queued[zi] = false;
        Some(zi)
    }

    /// Concatenates segments of SEGMENTS zones on the server (SPEC.md §5), up
    /// to `COMPACT_PARALLELISM` zones at once, never one zone twice at once,
    /// and puts metadata markers. The two loops run side by side in this one
    /// task: a marker PUT waits for the zone lock a concatenation may hold,
    /// so one loop must never wait for it with the other's work unpolled.
    pub async fn compactor(self: Arc<Self>) {
        if !self.segments {
            return;
        }
        tokio::join!(self.compact_loop(), self.marker_loop());
    }

    async fn compact_loop(&self) {
        let mut running = futures_util::stream::FuturesUnordered::new();
        let mut busy = vec![false; self.zones.len()];
        // One timer for the loop: a sleep made anew each turn would never
        // fire while concatenations keep finishing.
        let mut scan = tokio::time::interval(IDLE_SCAN);
        scan.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // After a failure no concatenation starts before this; running ones go on.
        let mut paused_until: Option<tokio::time::Instant> = None;
        loop {
            if paused_until.is_some_and(|t| t <= tokio::time::Instant::now()) {
                paused_until = None;
            }
            while paused_until.is_none() && running.len() < COMPACT_PARALLELISM {
                let Some(zi) = self.next_compaction(&busy) else {
                    break;
                };
                busy[zi] = true;
                running.push(async move { (zi, self.compact_zone(zi).await) });
            }
            tokio::select! {
                _ = scan.tick() => self.queue_idle(),
                Some((zi, res)) = running.next(), if !running.is_empty() => {
                    busy[zi] = false;
                    if let Err(e) = res {
                        STATS.compact_errors.inc();
                        STATS.err_compact.record(&e);
                        warn!("concatenation of zone {zi} failed: {e}");
                        paused_until = Some(tokio::time::Instant::now() + BACKGROUND_RETRY_PAUSE);
                    }
                }
                _ = self.compact_kick.notified() => {}
                _ = sleep_until_some(paused_until) => {}
            }
        }
    }

    /// Puts metadata markers when a zone becomes metadata and every
    /// `IDLE_SCAN` (retrying failed PUTs).
    async fn marker_loop(&self) {
        let mut scan = tokio::time::interval(IDLE_SCAN);
        scan.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = self.meta_kick.notified() => {}
                _ = scan.tick() => {}
            }
            self.meta_pending.store(false, Ordering::SeqCst);
            self.store_meta_markers().await;
        }
    }

    /// Concatenates the segments of a zone into its first object
    /// `<zone>-00000000` and deletes the others.
    async fn compact_zone(&self, zi: usize) -> ZResult<()> {
        let _in_flight = STATS.compacting.enter();
        let started = Instant::now();
        let Some(job) = self.compact_begin(zi).await? else {
            return Ok(());
        };
        let (segments, bytes) = (
            job.parts.len() as u64,
            job.parts.iter().map(|o| o.to - o.from).sum(),
        );
        if self.compact_end(zi, job).await? {
            STATS.compactions.inc();
            STATS.compacted_segments.add(segments);
            STATS.compacted_bytes.add(bytes);
            STATS.segments_per_compaction.record(segments);
            STATS
                .compact_time_ms
                .record(started.elapsed().as_millis() as u64);
        } else {
            STATS.compact_aborted.inc();
        }
        Ok(())
    }

    /// Takes the segments under the zone's `upload` lock, then copies them
    /// on the server without it: flushes and RESET of the zone do not wait.
    async fn compact_begin(&self, zi: usize) -> ZResult<Option<CompactJob>> {
        let zone = &self.zones[zi];
        let (generation, parts) = {
            let _upload = zone.upload.lock().await;
            let st = zone.state.lock().unwrap();
            if st.objects.len() < 2 {
                return Ok(None);
            }
            (st.generation, st.objects.clone())
        };
        let sources: Vec<_> = parts
            .iter()
            .map(|o| {
                let key = self.layout.segment_key(zone.start, o.start);
                (key, o.from - o.start, o.to - o.from)
            })
            .collect();
        let base = self.layout.segment_key(zone.start, 0);
        let _in_flight = zone.requests.concats.enter();
        let pending = match self.store.begin_concat(&base, &sources).await {
            Ok(p) => p,
            // A RESET meanwhile deleted the parts (the upload is aborted):
            // no failure of the concatenation, nothing to concatenate.
            Err(e) if zone.state.lock().unwrap().generation != generation => {
                debug!("zone {zi} was reset during its concatenation ({e}); dropped it");
                STATS.compact_aborted.inc();
                return Ok(None);
            }
            Err(e) => return Err(e.into()),
        };
        Ok(Some(CompactJob {
            generation,
            parts,
            pending,
        }))
    }

    /// Completes the concatenation under the zone's `upload` lock, unless the
    /// zone changed meanwhile: after a RESET (another generation) a new first
    /// segment `-00000000` may hold new data, and completing would overwrite
    /// it with old data; after another concatenation the parts are stale.
    /// Then deletes the concatenated segments, still under the lock: after a
    /// RESET new data may be stored under the same keys. Returns whether it
    /// completed.
    async fn compact_end(&self, zi: usize, job: CompactJob) -> ZResult<bool> {
        let zone = &self.zones[zi];
        let end = job.parts.last().expect("at least two parts").to;
        let _upload = zone.upload.lock().await;
        let unchanged = {
            let st = zone.state.lock().unwrap();
            st.generation == job.generation && st.objects.starts_with(&job.parts)
        };
        if !unchanged {
            self.store.abort_concat(job.pending).await;
            debug!("zone {zi} changed during its concatenation; dropped it");
            return Ok(false);
        }
        let _in_flight = zone.requests.concats.enter();
        self.store.finish_concat(job.pending).await?;
        {
            let mut st = zone.state.lock().unwrap();
            let rest = st.objects.split_off(job.parts.len());
            st.objects = std::iter::once(Stored {
                start: 0,
                from: 0,
                to: end,
            })
            .chain(rest)
            .collect();
        }
        let merged: Vec<String> = job
            .parts
            .iter()
            .filter(|o| o.start != 0)
            .map(|o| self.layout.segment_key(zone.start, o.start))
            .collect();
        // Readers still holding a deleted segment plan anew on 404. Segments
        // left by a failure lie within the base: deleted at restart or by RESET.
        self.store.delete_many(&merged).await?;
        debug!("zone {zi}: {} segments concatenated", job.parts.len());
        Ok(true)
    }

    pub fn dirty_limit(&self) -> u64 {
        self.dirty_limit
    }

    /// A read flagged META marks its zone as metadata (zones restored at
    /// start learn their kind so).
    pub fn note_meta(&self, offset: u64) {
        if let Ok((zi, _)) = self.locate(offset) {
            let mut st = self.zones[zi].state.lock().unwrap();
            // Only zones holding data are marked; an empty one learns from its writes.
            if st.kind != Kind::Meta && st.data_end > 0 {
                st.kind = Kind::Meta;
                drop(st);
                self.mark_meta_soon();
            }
        }
    }

    /// Has the compactor put the `.meta` marker of a zone that just became
    /// metadata, without waiting for its periodic scan.
    fn mark_meta_soon(&self) {
        if self.segments {
            self.meta_pending.store(true, Ordering::SeqCst);
            self.meta_kick.notify_one();
        }
    }

    /// Uploads allowed at once.
    pub fn uploads_limit(&self) -> usize {
        self.uploads
    }

    /// GETs allowed at once.
    pub fn reads_limit(&self) -> usize {
        self.reads
    }

    /// Part size large reads are split into for parallel GETs; `None` when
    /// they are not split (no link given).
    pub fn get_part(&self) -> Option<u64> {
        (self.get_part != u64::MAX).then_some(self.get_part)
    }

    /// Everything the daemon knows of zone `zi`, for the zone map of the
    /// page (SPEC.md §9.3); `None` past the last zone.
    pub fn zone_detail(&self, zi: usize) -> Option<ZoneDetail> {
        let zone = self.zones.get(zi)?;
        let queued = self.compact_queue.lock().unwrap().queued[zi];
        let st = zone.state.lock().unwrap();
        let mut objects: Vec<ObjectDetail> = st
            .objects
            .iter()
            .map(|o| ObjectDetail {
                key: self.object_key(zone.start, o),
                size: o.to - o.start,
                from: o.from,
                to: o.to,
            })
            .collect();
        let mut marker = |present: bool, key: String| {
            if present {
                objects.push(ObjectDetail {
                    key,
                    size: 0,
                    from: 0,
                    to: 0,
                });
            }
        };
        marker(st.marker, self.layout.finished_key(zone.start));
        marker(st.meta_marker, self.layout.meta_key(zone.start));
        Some(ZoneDetail {
            index: zi,
            start: zone.start,
            size: self.zone_size,
            cond: match st.cond {
                Cond::Empty => "empty",
                Cond::ImpOpen => "open (implicitly)",
                Cond::ExpOpen => "open (explicitly)",
                Cond::Closed => "closed",
                Cond::Full => "full",
            },
            written: st.data_end,
            stored: st.flushed,
            buffered: st.buffered(),
            kind: match st.kind {
                Kind::Unknown => "unknown (no request since the start)",
                Kind::Data => "data",
                Kind::Meta => "metadata",
            },
            objects,
            secs_since_write: st.written.then(|| (boottime() - st.last_write).max(0.0)),
            write_interval_secs: st.write_interval,
            compaction_queued: queued,
            broken: st.broken,
            gets: zone.requests.gets.current(),
            writing: [
                (&zone.requests.uploads, "upload of buffered data"),
                (&zone.requests.concats, "concatenation"),
                (&zone.requests.markers, "marker"),
                (&zone.requests.deletes, "deletion (RESET)"),
            ]
            .into_iter()
            .filter(|(n, _)| n.current() > 0)
            .map(|(_, what)| what)
            .collect(),
        })
    }

    /// Every zone for the zone map of the page (SPEC.md §9.3).
    pub fn zone_map(&self) -> ZoneMap {
        let n = self.zones.len();
        let mut m = ZoneMap {
            zone_size: self.zone_size,
            block_size: BLOCK_SIZE,
            cond: String::with_capacity(n),
            kind: String::with_capacity(n),
            used: Vec::with_capacity(n),
            objects: Vec::with_capacity(n),
            buffered: Vec::with_capacity(n),
        };
        for z in &self.zones {
            let st = z.state.lock().unwrap();
            m.cond.push(match st.cond {
                Cond::Empty => 'e',
                Cond::ImpOpen | Cond::ExpOpen => 'o',
                Cond::Closed => 'c',
                Cond::Full => 'f',
            });
            m.kind.push(match st.kind {
                Kind::Unknown => 'u',
                Kind::Data => 'd',
                Kind::Meta => 'm',
            });
            m.used.push((st.data_end / BLOCK_SIZE) as u32);
            m.objects.push(st.objects.len() as u32);
            m.buffered.push((st.buffered() / BLOCK_SIZE) as u32);
        }
        m
    }

    /// Zone conditions, filling, fragmentation and the concatenation queue
    /// (SPEC.md §9.3), and the number of zones with buffered data.
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
            if st.data_end > 0 && st.data_end < self.zone_size {
                c.partial += 1;
            }
            if st.cond == Cond::Full && st.data_end < self.zone_size {
                c.finished_early += 1;
            }
            if st.data_end > 0 {
                match st.kind {
                    Kind::Data => c.data_zones += 1,
                    Kind::Meta => c.meta_zones += 1,
                    Kind::Unknown => c.unknown_zones += 1,
                }
            }
            let objects = st.objects.len() as u64;
            if objects > 1 {
                c.fragmented += 1;
            }
            c.objects += objects;
            c.max_objects = c.max_objects.max(objects);
            if st.buffered() > 0 {
                dirty += 1;
            }
        }
        let q = self.compact_queue.lock().unwrap();
        c.compact_urgent = q.urgent.len() as u64;
        c.compact_queued = c.compact_urgent + q.normal.len() as u64;
        (c, dirty)
    }

    /// Zones that still hold data not confirmed by the store: (zone, bytes).
    /// Zones with buffered data now (a copy: the set is not held while
    /// zones are locked).
    fn buffered_zone_list(&self) -> Vec<usize> {
        self.buffered_zones
            .lock()
            .unwrap()
            .iter()
            .copied()
            .collect()
    }

    pub fn unflushed(&self) -> Vec<(usize, u64)> {
        (0..self.zones.len())
            .map(|i| (i, self.zones[i].state.lock().unwrap().buffered()))
            .filter(|&(_, b)| b > 0)
            .collect()
    }
}

/// Zero-filled buffers totalling `len` bytes, without allocation.
/// Runs `futures` at once (each GET waits for a slot of `reads`) and joins
/// their buffers in order; the first error wins.
async fn in_order<F>(futures: Vec<F>) -> ZResult<Vec<Bytes>>
where
    F: std::future::Future<Output = ZResult<Vec<Bytes>>>,
{
    let parts = futures_util::future::try_join_all(futures).await?;
    Ok(parts.into_iter().flatten().collect())
}

/// Sleeps until `t`; never returns for `None`.
async fn sleep_until_some(t: Option<tokio::time::Instant>) {
    match t {
        Some(t) => tokio::time::sleep_until(t).await,
        None => std::future::pending().await,
    }
}

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

    async fn segments_engine(store: Arc<Store>) -> Engine {
        let mut cfg = DiskConfig::new(8 * ZS, ZS);
        cfg.dirty_limit_bytes = 64 * B;
        cfg.append = AppendMode::Segments;
        Engine::open(&cfg, Layout::new("t").unwrap(), store)
            .await
            .unwrap()
    }

    async fn keys(store: &Store, prefix: &str) -> Vec<String> {
        let listed = store.list(prefix).await.unwrap();
        listed.into_iter().map(|o| o.key).collect()
    }

    /// Writes and flushes `n` single-block segments at the zone's pointer,
    /// block i filled with `first + i`.
    async fn segment_writes(e: &Engine, zone: u64, first: u8, n: u8) {
        for i in 0..n {
            let wp = e.report(zone as usize, 1)[0].wp;
            e.write(wp, blk(first + i, 1), false).unwrap();
            e.flush_zone(zone as usize, Reason::Explicit).await.unwrap();
        }
    }

    #[tokio::test]
    async fn zone_detail_gives_object_sizes_and_requests() {
        let e = segments_engine(mem()).await;
        segment_writes(&e, 1, 1, 2).await;
        e.write(ZS + 2 * B, blk(3, 1), false).unwrap();
        e.finish_zone(ZS).await.unwrap();
        read(&e, ZS, 3 * B).await;
        let d = e.zone_detail(1).unwrap();
        let sizes: Vec<_> = d.objects.iter().map(|o| (o.key.as_str(), o.size)).collect();
        assert_eq!(
            sizes,
            [
                ("t/0000000000100000-00000000", B),
                ("t/0000000000100000-00001000", B),
                ("t/0000000000100000-00002000", B),
                ("t/0000000000100000.finished", 0),
            ]
        );
        // Every request is over: its count went back.
        assert_eq!(d.gets, 0);
        assert!(d.writing.is_empty());
        e.compact_zone(1).await.unwrap();
        assert_eq!(e.zone_detail(1).unwrap().objects[0].size, 3 * B);
    }

    #[tokio::test]
    async fn segments_write_read_concatenate() {
        let store = mem();
        let e = segments_engine(store.clone()).await;
        segment_writes(&e, 1, 1, 3).await;
        assert_eq!(
            keys(&store, "t/").await,
            [
                "t/0000000000100000-00000000",
                "t/0000000000100000-00001000",
                "t/0000000000100000-00002000"
            ]
        );
        let want: Vec<u8> = [blk(1, 1), blk(2, 1), blk(3, 1)].concat();
        assert_eq!(read(&e, ZS, 3 * B).await, want); // across segments

        e.compact_zone(1).await.unwrap();
        assert_eq!(keys(&store, "t/").await, ["t/0000000000100000-00000000"]);
        assert_eq!(read(&e, ZS, 3 * B).await, want);
        segment_writes(&e, 1, 4, 1).await; // a new segment after the base
        assert_eq!(keys(&store, "t/").await.len(), 2);
        assert_eq!(
            &read(&e, ZS + 2 * B, 2 * B).await[B as usize..],
            &blk(4, 1)[..]
        );
    }

    #[tokio::test]
    async fn compact_all_queues_every_fragmented_zone() {
        let e = segments_engine(mem()).await;
        segment_writes(&e, 1, 1, 2).await; // below the threshold
        segment_writes(&e, 3, 1, 3).await;
        segment_writes(&e, 5, 1, 1).await; // one object: nothing to do
        assert_eq!(e.next_compaction(&[false; 8]), None);
        assert_eq!(e.compact_all(), 2);
        assert_eq!(e.next_compaction(&[false; 8]), Some(1));
        assert_eq!(e.next_compaction(&[false; 8]), Some(3));
        assert_eq!(e.next_compaction(&[false; 8]), None);
        // A disk without segments has nothing to concatenate.
        assert_eq!(engine_with(mem()).await.compact_all(), 0);
    }

    #[tokio::test]
    async fn compaction_rules_for_data_and_metadata() {
        // Zone 1 MiB, no GET part: data limit 8 segments, metadata 32.
        let e = segments_engine(mem()).await;
        let none = &[false; 8];
        let meta_write = |zone: u64| {
            let wp = e.report(zone as usize, 1)[0].wp;
            e.dirty.fetch_add(B, Ordering::SeqCst);
            e.write_reserved(wp, blk(7, 1), false, true).unwrap();
        };
        // Data: 9 segments of growing size pass both the limit and the growth guard.
        segment_writes(&e, 1, 1, 9).await;
        assert_eq!(e.next_compaction(none), Some(1));
        // Metadata: 9 segments are fine, 33 are many.
        for _ in 0..9 {
            meta_write(2);
            e.flush_zone(2, Reason::Explicit).await.unwrap();
        }
        assert_eq!(e.next_compaction(none), None);
        for _ in 0..24 {
            meta_write(2);
            e.flush_zone(2, Reason::Explicit).await.unwrap();
        }
        assert_eq!(e.next_compaction(none), Some(2));
        assert_eq!(e.zones[2].state.lock().unwrap().kind, Kind::Meta);
        e.note_meta(4 * ZS); // empty zone: learns from its writes
        assert_eq!(e.zones[4].state.lock().unwrap().kind, Kind::Unknown);
        segment_writes(&e, 3, 1, 1).await;
        assert_eq!(e.zones[3].state.lock().unwrap().kind, Kind::Data);
        e.note_meta(3 * ZS); // a META read of a data-looking zone
        assert_eq!(e.zones[3].state.lock().unwrap().kind, Kind::Meta);
    }

    #[tokio::test]
    async fn meta_marker_survives_restart_and_goes_with_reset() {
        let store = mem();
        let e = segments_engine(store.clone()).await;
        e.dirty.fetch_add(B, Ordering::SeqCst);
        e.write_reserved(2 * ZS, blk(1, 1), false, true).unwrap();
        e.flush_zone(2, Reason::Explicit).await.unwrap();
        assert!(e.meta_pending.load(Ordering::SeqCst));
        e.store_meta_markers().await;
        let marker = "t/0000000000200000.meta";
        assert!(store.get(marker).await.unwrap().is_some());
        drop(e);

        let e = segments_engine(store.clone()).await; // restart
        {
            let st = e.zones[2].state.lock().unwrap();
            assert_eq!((st.kind, st.meta_marker), (Kind::Meta, true));
        }
        e.reset_zone(2 * ZS).await.unwrap();
        assert!(store.get(marker).await.unwrap().is_none());
        assert_eq!(e.zones[2].state.lock().unwrap().kind, Kind::Unknown);

        // A marker left without data (interrupted RESET) is deleted at start.
        store
            .put(marker, crate::store::Body::default())
            .await
            .unwrap();
        let e = segments_engine(store.clone()).await;
        assert_eq!(e.zones[2].state.lock().unwrap().kind, Kind::Unknown);
        assert!(store.get(marker).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn marker_waiting_for_a_zone_lock_does_not_stop_concatenation() {
        let store = mem();
        let e = Arc::new(segments_engine(store.clone()).await);
        segment_writes(&e, 1, 1, 3).await;
        // Zone 2 becomes metadata while its lock is held (a long upload).
        e.dirty.fetch_add(B, Ordering::SeqCst);
        e.write_reserved(2 * ZS, blk(1, 1), false, true).unwrap();
        let held = e.zones[2].upload.lock().await;
        let task = tokio::spawn(e.clone().compactor());
        assert_eq!(e.compact_all(), 1);
        wait_for("concatenation", || {
            e.zones[1].state.lock().unwrap().objects.len() == 1
        })
        .await;
        drop(held);
        wait_for("metadata marker", || {
            e.zones[2].state.lock().unwrap().meta_marker
        })
        .await;
        task.abort();
    }

    /// Waits up to 5 s for `done`.
    async fn wait_for(what: &str, done: impl Fn() -> bool) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !done() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{what} stalled"));
    }

    #[tokio::test]
    async fn idle_zones_are_concatenated_late_for_metadata() {
        let e = segments_engine(mem()).await;
        segment_writes(&e, 1, 1, 2).await;
        let now = boottime();
        {
            let mut st = e.zones[1].state.lock().unwrap();
            assert!(!e.idle_due(&st, now)); // just written
            // A round time: `last_write + x - last_write` is exactly `x` then.
            let written_at = std::mem::replace(&mut st.last_write, 1000.0);
            st.write_interval = Some(1.0); // 4 × 1 s is below the 30 s floor
            assert!(!e.idle_due(&st, st.last_write + 29.0));
            assert!(e.idle_due(&st, st.last_write + 30.0));
            st.write_interval = Some(60.0); // 4 × 60 s
            assert!(!e.idle_due(&st, st.last_write + 239.0));
            assert!(e.idle_due(&st, st.last_write + 240.0));
            st.kind = Kind::Meta; // 8 × 60 s
            assert!(!e.idle_due(&st, st.last_write + 479.0));
            assert!(e.idle_due(&st, st.last_write + 480.0));
            st.write_interval = Some(3600.0); // capped at an hour
            assert!(e.idle_due(&st, st.last_write + 3600.0));
            st.last_write = written_at;
        }
        e.queue_idle();
        assert_eq!(e.next_compaction(&[false; 8]), None); // not idle yet by the clock
    }

    #[tokio::test]
    async fn segments_full_zone_is_urgent() {
        let e = segments_engine(mem()).await;
        segment_writes(&e, 2, 1, 2).await;
        assert_eq!(e.next_compaction(&[false; 8]), None); // two segments: not yet
        e.write(2 * ZS + 2 * B, blk(9, ZS / B - 2), false).unwrap(); // fills the zone
        e.flush_zone(2, Reason::Explicit).await.unwrap();
        assert_eq!(e.next_compaction(&[false; 8]), Some(2));
    }

    #[tokio::test]
    async fn segments_restore_cover_and_leftovers() {
        let store = mem();
        let put = |k: &str, b: Bytes| {
            let store = store.clone();
            let k = k.to_string();
            async move { store.put(&k, Body::new(vec![b])).await.unwrap() }
        };
        // A concatenated base of 3 blocks, a segment it covers (a leftover)
        // and one after it.
        put(
            "t/0000000000000000-00000000",
            [blk(1, 1), blk(2, 1), blk(3, 1)].concat().into(),
        )
        .await;
        put("t/0000000000000000-00001000", blk(2, 1)).await;
        put("t/0000000000000000-00003000", blk(4, 1)).await;
        let e = segments_engine(store.clone()).await;
        assert_eq!(e.report(0, 1)[0].wp, 4 * B);
        assert_eq!(
            read(&e, 0, 4 * B).await,
            [blk(1, 1), blk(2, 1), blk(3, 1), blk(4, 1)].concat()
        );
        assert_eq!(
            keys(&store, "t/").await,
            ["t/0000000000000000-00000000", "t/0000000000000000-00003000"]
        );

        // Past a gap: unreachable, deleted.
        put("t/0000000000100000-00000000", blk(5, 1)).await;
        put("t/0000000000100000-00002000", blk(6, 1)).await;
        let e = segments_engine(store.clone()).await;
        assert_eq!(e.report(1, 1)[0].wp, ZS + B);
        assert_eq!(
            keys(&store, "t/0000000000100000").await,
            ["t/0000000000100000-00000000"]
        );
    }

    #[tokio::test]
    async fn segments_concatenation_racing_reset() {
        let store = mem();
        let e = segments_engine(store.clone()).await;
        segment_writes(&e, 0, 1, 3).await;
        // Parts copied, then the zone is reset and written anew.
        let job = e.compact_begin(0).await.unwrap().unwrap();
        e.reset_zone(0).await.unwrap();
        segment_writes(&e, 0, 7, 1).await; // new data in -00000000
        e.compact_end(0, job).await.unwrap();
        // The new first segment survived: the stale concatenation was dropped.
        assert_eq!(keys(&store, "t/").await, ["t/0000000000000000-00000000"]);
        assert_eq!(read(&e, 0, B).await, blk(7, 1).to_vec());
        assert_eq!(e.report(0, 1)[0].wp, B);
    }

    #[tokio::test]
    async fn read_only_disk_reads_and_changes_nothing() {
        let store = mem();
        let e = segments_engine(store.clone()).await;
        segment_writes(&e, 1, 1, 2).await;
        // A leftover: a FINISH marker without data, as an interrupted RESET leaves.
        store
            .put("t/0000000000300000.finished", Body::default())
            .await
            .unwrap();
        drop(e);
        let before = keys(&store, "t/").await;

        let mut cfg = DiskConfig::new(8 * ZS, ZS);
        cfg.append = AppendMode::Segments;
        let e = Engine::open_read_only(&cfg, Layout::new("t").unwrap(), store.clone())
            .await
            .unwrap();
        assert_eq!(read(&e, ZS, 2 * B).await, [blk(1, 1), blk(2, 1)].concat());
        // The leftover is not taken for data: zone 3 is empty.
        assert_eq!(e.report(3, 1)[0].cond, Cond::Empty);
        let refused = [
            e.write(ZS + 2 * B, blk(3, 1), false).err(),
            e.open_zone(2 * ZS).err(),
            e.close_zone(ZS).err(),
            e.finish_zone(ZS).await.err(),
            e.reset_zone(ZS).await.err(),
            e.reset_all().await.err(),
        ];
        assert!(
            refused
                .iter()
                .all(|r| matches!(r, Some(ZoneError::ReadOnly)))
        );
        assert_eq!(e.compact_all(), 0);
        assert_eq!(e.dirty_bytes(), 0);
        assert_eq!(keys(&store, "t/").await, before);
    }

    #[tokio::test]
    async fn concatenation_broken_by_a_reset_is_no_failure() {
        let store = mem();
        let e = segments_engine(store.clone()).await;
        segment_writes(&e, 0, 1, 3).await;
        // While the parts are copied, a RESET bumps the generation and the
        // copies fail (their sources are gone).
        let (done, ()) = tokio::join!(e.compact_zone(0), async {
            e.zones[0].state.lock().unwrap().generation += 1;
            mem_store(&store).set_fail(true);
        });
        mem_store(&store).set_fail(false);
        done.unwrap();
        // The same failure without a RESET is one.
        mem_store(&store).set_fail(true);
        let failed = e.compact_zone(0).await;
        mem_store(&store).set_fail(false);
        assert!(failed.is_err());
    }

    #[tokio::test]
    async fn segments_reset_by_prefix() {
        let store = mem();
        let e = segments_engine(store.clone()).await;
        segment_writes(&e, 3, 1, 2).await;
        e.finish_zone(3 * ZS).await.unwrap();
        // A leftover the engine does not know about.
        store
            .put("t/0000000000300000-00008000", Body::default())
            .await
            .unwrap();
        e.reset_zone(3 * ZS).await.unwrap();
        assert!(keys(&store, "t/").await.is_empty());
    }

    #[tokio::test]
    async fn segments_read_planned_before_concatenation() {
        let store = mem();
        let e = segments_engine(store.clone()).await;
        segment_writes(&e, 0, 1, 3).await;
        let pieces = e.read_plan(0, 3 * B).unwrap(); // three segment pieces
        e.compact_zone(0).await.unwrap(); // deletes two of them
        let got = e.fetch(pieces).await.unwrap().concat();
        assert_eq!(got, [blk(1, 1), blk(2, 1), blk(3, 1)].concat());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stored_reads_survive_concurrent_concatenation() {
        let store = mem();
        let e = Arc::new(segments_engine(store.clone()).await);
        segment_writes(&e, 0, 0, 2).await;
        let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let reader = {
            let (e, done) = (e.clone(), done.clone());
            tokio::spawn(async move {
                let mut reads = 0;
                while !done.load(Ordering::SeqCst) {
                    let n = e.zones[0].state.lock().unwrap().flushed;
                    let got = e.read_stored(0, 0, n).await.unwrap().concat();
                    let want: Vec<u8> = (0..n / B).flat_map(|i| blk(i as u8, 1).to_vec()).collect();
                    assert_eq!(got, want);
                    reads += 1;
                }
                reads
            })
        };
        for i in 2..60 {
            segment_writes(&e, 0, i, 1).await;
            e.compact_zone(0).await.unwrap();
        }
        done.store(true, Ordering::SeqCst);
        assert!(reader.await.unwrap() > 0);
    }

    // Crash points (SPEC.md §5): the daemon dies at a given step, a new
    // engine restores the zone from what the store holds.

    #[tokio::test]
    async fn crash_after_segment_upload_before_state() {
        let store = mem();
        let e = segments_engine(store.clone()).await;
        segment_writes(&e, 0, 1, 2).await;
        // The third upload reached the store, then the daemon died before
        // recording it (and before acknowledging the FLUSH).
        store
            .put("t/0000000000000000-00002000", Body::new(vec![blk(3, 1)]))
            .await
            .unwrap();
        drop(e);
        let e = segments_engine(store).await;
        assert_eq!(e.report(0, 1)[0].wp, 3 * B);
        assert_eq!(
            read(&e, 0, 3 * B).await,
            [blk(1, 1), blk(2, 1), blk(3, 1)].concat()
        );
    }

    #[tokio::test]
    async fn crash_during_concatenation_before_complete() {
        let store = mem();
        let e = segments_engine(store.clone()).await;
        segment_writes(&e, 0, 1, 3).await;
        let job = e.compact_begin(0).await.unwrap().unwrap();
        drop(job); // parts copied, never completed
        drop(e);
        let e = segments_engine(store.clone()).await;
        assert_eq!(keys(&store, "t/").await.len(), 3); // segments untouched
        assert_eq!(
            read(&e, 0, 3 * B).await,
            [blk(1, 1), blk(2, 1), blk(3, 1)].concat()
        );
        e.compact_zone(0).await.unwrap(); // and concatenate fine later
        assert_eq!(keys(&store, "t/").await, ["t/0000000000000000-00000000"]);
    }

    #[tokio::test]
    async fn crash_after_complete_before_deleting_segments() {
        let store = mem();
        let e = segments_engine(store.clone()).await;
        segment_writes(&e, 0, 1, 3).await;
        let job = e.compact_begin(0).await.unwrap().unwrap();
        store.finish_concat(job.pending).await.unwrap(); // the base is replaced
        drop(e); // died before updating the map and deleting the segments
        let e = segments_engine(store.clone()).await;
        assert_eq!(keys(&store, "t/").await, ["t/0000000000000000-00000000"]);
        assert_eq!(e.report(0, 1)[0].wp, 3 * B);
        assert_eq!(
            read(&e, 0, 3 * B).await,
            [blk(1, 1), blk(2, 1), blk(3, 1)].concat()
        );
    }

    /// A zone with three segments, a FINISH marker and a metadata marker.
    async fn finished_meta_zone(e: &Engine, store: &Store, zone: u64) {
        e.dirty.fetch_add(B, Ordering::SeqCst);
        e.write_reserved(zone * ZS, blk(1, 1), false, true).unwrap();
        e.flush_zone(zone as usize, Reason::Explicit).await.unwrap();
        segment_writes(e, zone, 2, 2).await;
        e.store_meta_markers().await;
        e.finish_zone(zone * ZS).await.unwrap();
        assert_eq!(keys(store, &e.layout.zone_key(zone * ZS)).await.len(), 5);
    }

    #[tokio::test]
    async fn crash_during_reset_leaves_zone_untouched_or_empty() {
        // Segments: deleting the zone start failed: nothing else is gone.
        let store = mem();
        let e = segments_engine(store.clone()).await;
        finished_meta_zone(&e, &store, 1).await;
        mem_store(&store).fail_deletes_of(Some("0000000000100000-00000000"));
        assert!(e.reset_zone(ZS).await.is_err());
        mem_store(&store).fail_deletes_of(None);
        assert_eq!(keys(&store, "t/").await.len(), 5);
        drop(e);
        let e = segments_engine(store.clone()).await;
        assert_eq!(e.report(1, 1)[0].cond, Cond::Full); // untouched
        assert_eq!(
            read(&e, ZS, 3 * B).await,
            [blk(1, 1), blk(2, 1), blk(3, 1)].concat()
        );
        assert_eq!(keys(&store, "t/").await.len(), 5);

        // The daemon died after deleting the zone start, during the rest.
        mem_store(&store).fail_deletes_of(Some("0000000000100000-00001000"));
        let deleting = e.reset_zone(ZS).await;
        assert!(deleting.is_err());
        mem_store(&store).fail_deletes_of(None);
        // The start went first; some of the rest is left.
        let left = keys(&store, "t/").await;
        assert!(!left.is_empty() && !left.contains(&"t/0000000000100000-00000000".to_string()));
        drop(e);
        let e = segments_engine(store.clone()).await;
        assert_eq!(e.report(1, 1)[0].cond, Cond::Empty);
        assert!(keys(&store, "t/").await.is_empty());

        // PATCH: the zone object gone, the FINISH marker left.
        let store = mem();
        let e = engine_with(store.clone()).await;
        e.write(ZS, blk(1, 2), false).unwrap();
        e.finish_zone(ZS).await.unwrap();
        mem_store(&store).fail_deletes_of(Some(".finished"));
        assert!(e.reset_zone(ZS).await.is_err());
        mem_store(&store).fail_deletes_of(None);
        assert_eq!(keys(&store, "t/").await, ["t/0000000000100000.finished"]);
        drop(e);
        let e = engine_with(store.clone()).await;
        assert_eq!(e.report(1, 1)[0].cond, Cond::Empty);
        assert!(keys(&store, "t/").await.is_empty());
    }

    #[tokio::test]
    async fn failed_reset_refuses_io_until_reset() {
        let store = mem();
        let e = segments_engine(store.clone()).await;
        finished_meta_zone(&e, &store, 2).await;
        segment_writes(&e, 3, 1, 3).await;
        let job = e.compact_begin(3).await.unwrap().unwrap();
        mem_store(&store).fail_deletes_of(Some("-00001000"));
        assert!(e.reset_zone(2 * ZS).await.is_err());
        assert!(e.reset_zone(3 * ZS).await.is_err());
        mem_store(&store).fail_deletes_of(None);
        // The concatenation started before the RESET never completes.
        assert!(!e.compact_end(3, job).await.unwrap());
        let r = e.report(2, 2);
        assert_eq!((r[0].cond, r[1].cond), (Cond::Full, Cond::ImpOpen)); // not empty: reset again
        assert!(e.read_plan(3 * ZS, B).is_err());
        assert!(e.write(3 * ZS + 3 * B, blk(9, 1), false).is_err());
        assert!(e.finish_zone(3 * ZS).await.is_err());
        e.reset_zone(2 * ZS).await.unwrap();
        e.reset_zone(3 * ZS).await.unwrap();
        assert!(keys(&store, "t/").await.is_empty());
        segment_writes(&e, 3, 7, 1).await;
        assert_eq!(read(&e, 3 * ZS, B).await, blk(7, 1).to_vec());
    }

    #[tokio::test]
    async fn resets_leave_disks_nested_under_the_prefix() {
        let store = mem();
        let e = segments_engine(store.clone()).await;
        let nested = [
            "t/x/disk.json",
            "t/x/0000000000000000-00000000",
            "t/0000000000100000x/0000000000000000",
        ];
        for k in nested {
            store.put(k, Body::default()).await.unwrap();
        }
        segment_writes(&e, 1, 1, 2).await;
        e.reset_zone(ZS).await.unwrap();
        segment_writes(&e, 0, 1, 2).await;
        e.reset_all().await.unwrap();
        assert_eq!(keys(&store, "t/").await, {
            let mut k = nested.to_vec();
            k.sort();
            k
        });
    }

    #[tokio::test]
    async fn failed_reset_all_breaks_only_zones_with_something() {
        let store = mem();
        let e = segments_engine(store.clone()).await;
        segment_writes(&e, 1, 1, 2).await;
        mem_store(&store).set_fail(true);
        assert!(e.reset_all().await.is_err());
        mem_store(&store).set_fail(false);
        assert_eq!(e.report(1, 1)[0].cond, Cond::ImpOpen); // reset again by the kernel
        assert!(e.write(ZS + 2 * B, blk(9, 1), false).is_err());
        // An EMPTY zone the kernel will not reset stays usable.
        assert_eq!(e.report(2, 1)[0].cond, Cond::Empty);
        e.write(2 * ZS, blk(7, 1), false).unwrap();
        assert_eq!(read(&e, 2 * ZS, B).await, blk(7, 1).to_vec());
    }

    #[tokio::test]
    async fn finish_marker_goes_with_reset_even_if_its_put_failed() {
        let store = mem();
        let e = engine_with(store.clone()).await;
        e.write(0, blk(1, 2), false).unwrap();
        e.flush_zone(0, Reason::Explicit).await.unwrap();
        // The marker PUT reached the store but its answer was lost.
        mem_store(&store).set_fail(true);
        assert!(e.finish_zone(0).await.is_err());
        mem_store(&store).set_fail(false);
        store
            .put("t/0000000000000000.finished", Body::default())
            .await
            .unwrap();
        e.reset_zone(0).await.unwrap();
        assert!(keys(&store, "t/").await.is_empty());
        // New data in the zone is not taken for a finished zone after a restart.
        e.write(0, blk(2, 1), false).unwrap();
        e.flush_zone(0, Reason::Explicit).await.unwrap();
        drop(e);
        let e = engine_with(store).await;
        assert_eq!(
            (e.report(0, 1)[0].cond, e.report(0, 1)[0].wp),
            (Cond::Closed, B)
        );
    }

    #[tokio::test]
    async fn finish_of_empty_zone_stores_nothing() {
        let store = mem();
        let e = segments_engine(store.clone()).await;
        e.finish_zone(4 * ZS).await.unwrap();
        assert_eq!(e.report(4, 1)[0].cond, Cond::Full);
        assert!(keys(&store, "t/").await.is_empty());
        drop(e);
        let e = segments_engine(store).await;
        assert_eq!(e.report(4, 1)[0].cond, Cond::Empty);
    }

    #[tokio::test]
    async fn crash_during_finish() {
        let store = mem();
        let e = segments_engine(store.clone()).await;
        segment_writes(&e, 0, 1, 2).await; // FINISH flushed, died before the marker
        e.write(ZS, blk(5, 1), false).unwrap();
        e.finish_zone(ZS).await.unwrap(); // zone 1: fully finished
        drop(e);
        let e = segments_engine(store).await;
        let r = e.report(0, 2);
        assert_eq!((r[0].cond, r[0].wp), (Cond::Closed, 2 * B)); // FINISH lost: closed
        assert_eq!((r[1].cond, r[1].wp), (Cond::Full, 2 * ZS)); // marker made it
    }

    #[tokio::test]
    async fn store_failure_during_concatenation() {
        let store = mem();
        let e = segments_engine(store.clone()).await;
        segment_writes(&e, 0, 1, 3).await;
        mem_store(&store).set_fail(true);
        assert!(e.compact_zone(0).await.is_err());
        mem_store(&store).set_fail(false);
        assert_eq!(keys(&store, "t/").await.len(), 3);
        assert_eq!(
            read(&e, 0, 3 * B).await,
            [blk(1, 1), blk(2, 1), blk(3, 1)].concat()
        );
        e.compact_zone(0).await.unwrap();
        assert_eq!(keys(&store, "t/").await, ["t/0000000000000000-00000000"]);
    }

    /// What the stress test did, to check that it exercised every path.
    static STRESS_OPS: [AtomicU64; 4] = [const { AtomicU64::new(0) }; 4];
    const OP_FLUSH: usize = 0;
    const OP_READ: usize = 1;
    const OP_FINISH: usize = 2;
    const OP_RESET: usize = 3;

    /// Deterministic generator for the stress test (xorshift64*).
    struct Rng(u64);

    impl Rng {
        fn below(&mut self, n: u64) -> u64 {
            self.0 ^= self.0 >> 12;
            self.0 ^= self.0 << 25;
            self.0 ^= self.0 >> 27;
            self.0.wrapping_mul(0x2545_f491_4f6c_dd1d) % n
        }
    }

    /// What a zone task knows of its zone: bytes written, how many of them a
    /// FLUSH or FINISH acknowledged, whether it is finished, whether a RESET
    /// was started and not acknowledged.
    #[derive(Clone, Default)]
    struct Model {
        written: Vec<u8>,
        acked: usize,
        finished: bool,
        resetting: bool,
    }

    /// Random writes, flushes, reads, FINISH and RESET on one zone, checking
    /// every read against the model. Store errors (injected) are allowed:
    /// they only leave an operation unacknowledged.
    async fn zone_task(e: Arc<Engine>, zi: usize, model: Arc<Mutex<Model>>, seed: u64) {
        let mut rng = Rng(seed | 1);
        let zstart = zi as u64 * ZS;
        loop {
            let op = rng.below(100);
            let (len, finished, resetting) = {
                let m = model.lock().unwrap();
                (m.written.len() as u64, m.finished, m.resetting)
            };
            if resetting {
                // A failed RESET: the zone refuses I/O until one succeeds.
                if e.reset_zone(zstart).await.is_ok() {
                    STRESS_OPS[OP_RESET].fetch_add(1, Ordering::Relaxed);
                    *model.lock().unwrap() = Model::default();
                }
            } else if op < 50 && !finished && len < ZS {
                let n = (1 + rng.below(4)).min((ZS - len) / B);
                let data = blk(rng.below(256) as u8, n);
                e.write(zstart + len, data.clone(), false).unwrap();
                model.lock().unwrap().written.extend_from_slice(&data);
            } else if op < 75 {
                if e.flush_zone(zi, Reason::Explicit).await.is_ok() {
                    STRESS_OPS[OP_FLUSH].fetch_add(1, Ordering::Relaxed);
                    let mut m = model.lock().unwrap();
                    m.acked = m.acked.max(len as usize);
                }
            } else if op < 90 && len > 0 {
                let off = rng.below(len / B) * B;
                let n = (1 + rng.below((len - off) / B)) * B;
                let want = model.lock().unwrap().written[off as usize..(off + n) as usize].to_vec();
                let pieces = e.read_plan(zstart + off, n).unwrap();
                if let Ok(got) = e.fetch(pieces).await {
                    STRESS_OPS[OP_READ].fetch_add(1, Ordering::Relaxed);
                    assert_eq!(got.concat(), want, "zone {zi} read at {off}+{n}");
                }
            } else if op < 94 && !finished {
                if e.finish_zone(zstart).await.is_ok() {
                    STRESS_OPS[OP_FINISH].fetch_add(1, Ordering::Relaxed);
                    let mut m = model.lock().unwrap();
                    m.acked = len as usize;
                    m.finished = true;
                }
            } else if op >= 94 || finished || len == ZS {
                model.lock().unwrap().resetting = true;
                if e.reset_zone(zstart).await.is_ok() {
                    STRESS_OPS[OP_RESET].fetch_add(1, Ordering::Relaxed);
                    *model.lock().unwrap() = Model::default();
                }
            }
            tokio::task::yield_now().await;
        }
    }

    /// After a restart: every acknowledged byte is there, or the zone is
    /// empty if a RESET was under way; nothing beyond what was written, and
    /// the zone is a prefix of what was written. Returns the model of the
    /// restored zone.
    async fn check_restored(e: &Engine, zi: usize, m: &Model) -> Model {
        let (len, cond) = {
            let st = e.zones[zi].state.lock().unwrap();
            (st.data_end as usize, st.cond)
        };
        assert!(
            len >= m.acked || (m.resetting && len == 0),
            "zone {zi}: {len} bytes restored, {} acknowledged{}",
            m.acked,
            if m.resetting { ", RESET under way" } else { "" }
        );
        assert!(
            len <= m.written.len(),
            "zone {zi}: {len} restored, {} written",
            m.written.len()
        );
        let got = read(e, zi as u64 * ZS, len as u64).await;
        assert_eq!(got, m.written[..len], "zone {zi}: restored data");
        Model {
            written: got,
            acked: len,
            finished: cond == Cond::Full,
            resetting: false,
        }
    }

    /// Zones written, flushed, read, finished and reset concurrently with the
    /// compactor and injected store failures, then the "daemon" dies at a
    /// random moment (every task aborted mid-operation) and a new engine
    /// restores the disk from the store, round after round.
    async fn stress(append: AppendMode) {
        const ZONES: usize = 6;
        const ROUNDS: u64 = 150;
        let store = mem();
        let mut cfg = DiskConfig::new(8 * ZS, ZS);
        cfg.dirty_limit_bytes = 64 * B;
        cfg.append = append;
        let mut models = vec![Model::default(); ZONES];
        let mut rng = Rng(0x5eed);
        for round in 0..ROUNDS {
            mem_store(&store).set_fail(false);
            let e = Arc::new(
                Engine::open(&cfg, Layout::new("t").unwrap(), store.clone())
                    .await
                    .unwrap(),
            );
            for (zi, m) in models.iter_mut().enumerate() {
                *m = check_restored(&e, zi, m).await;
            }
            let shared: Vec<_> = models
                .iter()
                .map(|m| Arc::new(Mutex::new(m.clone())))
                .collect();
            let mut tasks: Vec<tokio::task::JoinHandle<()>> = (0..ZONES)
                .map(|zi| {
                    let seed = round * 1000 + zi as u64;
                    tokio::spawn(zone_task(e.clone(), zi, shared[zi].clone(), seed))
                })
                .collect();
            tasks.push(tokio::spawn(e.clone().compactor()));
            tasks.push(tokio::spawn(e.clone().background_flusher()));
            let chaos_store = store.clone();
            tasks.push(tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(3)).await;
                    mem_store(&chaos_store).set_fail(true);
                    tokio::time::sleep(Duration::from_micros(300)).await;
                    mem_store(&chaos_store).set_fail(false);
                }
            }));
            tokio::time::sleep(Duration::from_millis(5 + rng.below(25))).await;
            for t in &tasks {
                t.abort(); // the daemon dies here
            }
            for t in tasks {
                if let Err(err) = t.await
                    && err.is_panic()
                {
                    std::panic::resume_unwind(err.into_panic());
                }
            }
            models = shared.iter().map(|m| m.lock().unwrap().clone()).collect();
            let with_data: Vec<usize> = (0..e.zones.len())
                .filter(|&i| e.zones[i].state.lock().unwrap().buffered() > 0)
                .collect();
            assert_eq!(
                e.buffered_zone_list(),
                with_data,
                "set of zones with buffered data"
            );
            drop(e);
        }
        let ops: Vec<u64> = STRESS_OPS
            .iter()
            .map(|o| o.load(Ordering::Relaxed))
            .collect();
        eprintln!(
            "stress {append:?}: {ROUNDS} crashes; acked flushes {}, reads {}, finishes {}, resets {}; \
             concatenations so far {} ({} failed)",
            ops[OP_FLUSH],
            ops[OP_READ],
            ops[OP_FINISH],
            ops[OP_RESET],
            STATS.compactions.get(),
            STATS.compact_errors.get()
        );
        assert!(ops.iter().all(|&n| n > 0), "every operation ran: {ops:?}");
        if append == AppendMode::Segments {
            assert!(STATS.compactions.get() > 0, "the compactor ran");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn stress_crashes_segments() {
        stress(AppendMode::Segments).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn stress_crashes_patch() {
        stress(AppendMode::Patch).await;
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

        // FINISH of an empty zone stores nothing (`finish_of_empty_zone_stores_nothing`).
        e.finish_zone(ZS).await.unwrap();
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
            e.finish_zone(4 * ZS).await.unwrap(); // empty, finished: nothing stored
        }
        let e = engine_with(store).await;
        let r = e.report(0, 5);
        assert_eq!((r[0].cond, r[0].wp), (Cond::Closed, 3 * B));
        assert_eq!((r[1].cond, r[1].wp), (Cond::Full, 2 * ZS));
        assert_eq!((r[2].cond, r[2].wp), (Cond::Empty, 2 * ZS));
        assert_eq!((r[3].cond, r[3].wp), (Cond::Full, 4 * ZS));
        assert_eq!((r[4].cond, r[4].wp), (Cond::Empty, 4 * ZS)); // no data, no marker
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
        e.reserve(B).await;
        e.unreserve(B);
    }

    #[tokio::test]
    async fn writers_never_pass_the_hard_limit() {
        // Soft limit 64 blocks, hard 128.
        let e = Arc::new(engine_with(mem()).await);
        assert!(e.try_reserve(120 * B));
        assert!(e.try_reserve(8 * B)); // exactly at the hard limit
        assert!(!e.try_reserve(B));
        // Waiting writers: room for only two of them once 16 blocks are freed.
        let waiters: Vec<_> = (0..5)
            .map(|_| {
                let e = e.clone();
                tokio::spawn(async move { e.reserve(8 * B).await })
            })
            .collect();
        tokio::time::sleep(Duration::from_millis(20)).await;
        e.unreserve(16 * B);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(waiters.iter().filter(|w| w.is_finished()).count(), 2);
        assert_eq!(e.dirty_bytes(), 128 * B);
        // Nothing buffered: a request larger than the whole room still passes.
        e.unreserve(128 * B);
        for w in waiters {
            w.abort();
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        let rest = e.dirty_bytes();
        e.unreserve(rest);
        assert!(e.try_reserve(1000 * B));
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

//! Runtime statistics of the device server and the `stats` command.
//!
//! Counters live in a process-wide `STATS` (one server process serves one
//! disk) and are plain relaxed atomics. The server answers every connection to
//! `/run/btrs3fs/<dev-id>.sock` with a JSON `Snapshot` and closes it.

use anyhow::Context;
use log::{debug, warn};
use serde::{Deserialize, Serialize};
use std::io::Read;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

pub const SOCKET_DIR: &str = "/run/btrs3fs";

const KIB: u64 = 1 << 10;
const MIB: u64 = 1 << 20;
const SIZE_BOUNDS: &[u64] = &[
    4 * KIB,
    16 * KIB,
    64 * KIB,
    256 * KIB,
    MIB,
    4 * MIB,
    16 * MIB,
];
const LATENCY_BOUNDS_MS: &[u64] = &[10, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000];
const ZONES_BOUNDS: &[u64] = &[0, 1, 2, 4, 8, 16, 32, 64];
const MAX_BUCKETS: usize = 12;

pub struct Counter(AtomicU64);

impl Counter {
    const fn new() -> Self {
        Self(AtomicU64::new(0))
    }

    pub fn add(&self, n: u64) {
        self.0.fetch_add(n, Relaxed);
    }

    pub fn inc(&self) {
        self.add(1);
    }

    fn get(&self) -> u64 {
        self.0.load(Relaxed)
    }
}

/// Values distributed over fixed buckets: bucket i counts values `<= bounds[i]`
/// (and above the previous bound); the last bucket counts larger values.
pub struct Histogram {
    bounds: &'static [u64],
    counts: [AtomicU64; MAX_BUCKETS],
    sum: AtomicU64,
}

impl Histogram {
    const fn new(bounds: &'static [u64]) -> Self {
        assert!(bounds.len() < MAX_BUCKETS);
        Self {
            bounds,
            counts: [const { AtomicU64::new(0) }; MAX_BUCKETS],
            sum: AtomicU64::new(0),
        }
    }

    pub fn record(&self, value: u64) {
        let i = self
            .bounds
            .iter()
            .position(|&b| value <= b)
            .unwrap_or(self.bounds.len());
        self.counts[i].fetch_add(1, Relaxed);
        self.sum.fetch_add(value, Relaxed);
    }

    fn snapshot(&self) -> Hist {
        Hist {
            bounds: self.bounds.to_vec(),
            counts: self.counts[..=self.bounds.len()]
                .iter()
                .map(|c| c.load(Relaxed))
                .collect(),
            sum: self.sum.load(Relaxed),
        }
    }
}

/// Current and highest number of operations in progress.
pub struct InFlight {
    current: AtomicU64,
    max: AtomicU64,
}

pub struct InFlightGuard<'a>(&'a InFlight);

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        self.0.current.fetch_sub(1, Relaxed);
    }
}

impl InFlight {
    const fn new() -> Self {
        Self {
            current: AtomicU64::new(0),
            max: AtomicU64::new(0),
        }
    }

    pub fn enter(&self) -> InFlightGuard<'_> {
        let now = self.current.fetch_add(1, Relaxed) + 1;
        self.max.fetch_max(now, Relaxed);
        InFlightGuard(self)
    }
}

/// Why buffered zone data was uploaded.
#[derive(Clone, Copy, Debug)]
pub enum Reason {
    /// FLUSH request from the kernel (fsync, sync, btrfs commit).
    Flush,
    /// Write with FUA.
    Fua,
    /// Buffered data above `dirty_limit_bytes`.
    Background,
    /// Explicit flush by btrs3fs itself (`create`).
    Explicit,
}

pub struct Stats {
    pub upload_size: Histogram,
    pub upload_latency_ms: Histogram,
    pub uploads_put: Counter,
    pub uploads_patch: Counter,
    pub upload_reason: [Counter; 4],
    pub uploads: InFlight,
    pub upload_errors: Counter,
    pub retries: Counter,
    pub zones_per_flush: Histogram,
    pub gets: Counter,
    pub get_latency_ms: Histogram,
    pub read_from_s3: Counter,
    pub read_from_ram: Counter,
    pub read_zeros: Counter,
    pub req_read: Counter,
    pub req_read_bytes: Counter,
    pub req_write: Counter,
    pub req_write_bytes: Counter,
    pub req_append: Counter,
    pub req_append_bytes: Counter,
    pub req_flush: Counter,
    pub req_fua: Counter,
    pub req_zone_mgmt: Counter,
    pub req_zone_reset: Counter,
    pub req_report: Counter,
    pub req_errors: Counter,
    pub requests: InFlight,
}

pub static STATS: Stats = Stats {
    upload_size: Histogram::new(SIZE_BOUNDS),
    upload_latency_ms: Histogram::new(LATENCY_BOUNDS_MS),
    uploads_put: Counter::new(),
    uploads_patch: Counter::new(),
    upload_reason: [
        Counter::new(),
        Counter::new(),
        Counter::new(),
        Counter::new(),
    ],
    uploads: InFlight::new(),
    upload_errors: Counter::new(),
    retries: Counter::new(),
    zones_per_flush: Histogram::new(ZONES_BOUNDS),
    gets: Counter::new(),
    get_latency_ms: Histogram::new(LATENCY_BOUNDS_MS),
    read_from_s3: Counter::new(),
    read_from_ram: Counter::new(),
    read_zeros: Counter::new(),
    req_read: Counter::new(),
    req_read_bytes: Counter::new(),
    req_write: Counter::new(),
    req_write_bytes: Counter::new(),
    req_append: Counter::new(),
    req_append_bytes: Counter::new(),
    req_flush: Counter::new(),
    req_fua: Counter::new(),
    req_zone_mgmt: Counter::new(),
    req_zone_reset: Counter::new(),
    req_report: Counter::new(),
    req_errors: Counter::new(),
    requests: InFlight::new(),
};

impl Stats {
    pub fn reason(&self, r: Reason) -> &Counter {
        &self.upload_reason[r as usize]
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Hist {
    pub bounds: Vec<u64>,
    pub counts: Vec<u64>,
    pub sum: u64,
}

impl Hist {
    pub fn count(&self) -> u64 {
        self.counts.iter().sum()
    }

    fn delta(&self, prev: &Hist) -> Hist {
        Hist {
            bounds: self.bounds.clone(),
            counts: self
                .counts
                .iter()
                .zip(&prev.counts)
                .map(|(a, b)| a - b)
                .collect(),
            sum: self.sum - prev.sum,
        }
    }

    /// Upper bound of the bucket holding the q-quantile; `None` if it is the
    /// open-ended last bucket or there are no values.
    pub fn quantile(&self, q: f64) -> Option<u64> {
        let total = self.count();
        if total == 0 {
            return None;
        }
        let want = (q * total as f64).ceil().max(1.0) as u64;
        let mut seen = 0;
        for (i, &c) in self.counts.iter().enumerate() {
            seen += c;
            if seen >= want {
                return self.bounds.get(i).copied();
            }
        }
        None
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct ZoneCounts {
    pub empty: u64,
    pub open: u64,
    pub closed: u64,
    pub full: u64,
}

/// Everything `btrs3fs stats` shows. Counters are totals since the server started.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Snapshot {
    pub disk: String,
    pub device: String,
    pub uptime_secs: u64,
    pub dirty_bytes: u64,
    pub dirty_limit: u64,
    pub dirty_zones: u64,
    pub zones: ZoneCounts,
    pub upload_size: Hist,
    pub upload_latency_ms: Hist,
    pub uploads_put: u64,
    pub uploads_patch: u64,
    pub uploads_flush: u64,
    pub uploads_fua: u64,
    pub uploads_background: u64,
    pub uploads_explicit: u64,
    pub uploads_in_flight: u64,
    pub uploads_in_flight_max: u64,
    pub upload_errors: u64,
    pub retries: u64,
    pub zones_per_flush: Hist,
    pub gets: u64,
    pub get_latency_ms: Hist,
    pub read_from_s3: u64,
    pub read_from_ram: u64,
    pub read_zeros: u64,
    pub req_read: u64,
    pub req_read_bytes: u64,
    pub req_write: u64,
    pub req_write_bytes: u64,
    pub req_append: u64,
    pub req_append_bytes: u64,
    pub req_flush: u64,
    pub req_fua: u64,
    pub req_zone_mgmt: u64,
    pub req_zone_reset: u64,
    pub req_report: u64,
    pub req_errors: u64,
    pub requests_in_flight: u64,
    pub requests_in_flight_max: u64,
}

/// State of the disk that is not a counter, filled in by the caller.
pub struct DiskState {
    pub disk: String,
    pub device: String,
    pub uptime_secs: u64,
    pub dirty_bytes: u64,
    pub dirty_limit: u64,
    pub dirty_zones: u64,
    pub zones: ZoneCounts,
}

impl Stats {
    pub fn snapshot(&self, d: DiskState) -> Snapshot {
        Snapshot {
            disk: d.disk,
            device: d.device,
            uptime_secs: d.uptime_secs,
            dirty_bytes: d.dirty_bytes,
            dirty_limit: d.dirty_limit,
            dirty_zones: d.dirty_zones,
            zones: d.zones,
            upload_size: self.upload_size.snapshot(),
            upload_latency_ms: self.upload_latency_ms.snapshot(),
            uploads_put: self.uploads_put.get(),
            uploads_patch: self.uploads_patch.get(),
            uploads_flush: self.reason(Reason::Flush).get(),
            uploads_fua: self.reason(Reason::Fua).get(),
            uploads_background: self.reason(Reason::Background).get(),
            uploads_explicit: self.reason(Reason::Explicit).get(),
            uploads_in_flight: self.uploads.current.load(Relaxed),
            uploads_in_flight_max: self.uploads.max.load(Relaxed),
            upload_errors: self.upload_errors.get(),
            retries: self.retries.get(),
            zones_per_flush: self.zones_per_flush.snapshot(),
            gets: self.gets.get(),
            get_latency_ms: self.get_latency_ms.snapshot(),
            read_from_s3: self.read_from_s3.get(),
            read_from_ram: self.read_from_ram.get(),
            read_zeros: self.read_zeros.get(),
            req_read: self.req_read.get(),
            req_read_bytes: self.req_read_bytes.get(),
            req_write: self.req_write.get(),
            req_write_bytes: self.req_write_bytes.get(),
            req_append: self.req_append.get(),
            req_append_bytes: self.req_append_bytes.get(),
            req_flush: self.req_flush.get(),
            req_fua: self.req_fua.get(),
            req_zone_mgmt: self.req_zone_mgmt.get(),
            req_zone_reset: self.req_zone_reset.get(),
            req_report: self.req_report.get(),
            req_errors: self.req_errors.get(),
            requests_in_flight: self.requests.current.load(Relaxed),
            requests_in_flight_max: self.requests.max.load(Relaxed),
        }
    }
}

impl Snapshot {
    /// Counters accumulated since `prev`; gauges and maxima stay current.
    pub fn delta(&self, prev: &Snapshot) -> Snapshot {
        let d = |a: u64, b: u64| a.saturating_sub(b);
        Snapshot {
            upload_size: self.upload_size.delta(&prev.upload_size),
            upload_latency_ms: self.upload_latency_ms.delta(&prev.upload_latency_ms),
            uploads_put: d(self.uploads_put, prev.uploads_put),
            uploads_patch: d(self.uploads_patch, prev.uploads_patch),
            uploads_flush: d(self.uploads_flush, prev.uploads_flush),
            uploads_fua: d(self.uploads_fua, prev.uploads_fua),
            uploads_background: d(self.uploads_background, prev.uploads_background),
            uploads_explicit: d(self.uploads_explicit, prev.uploads_explicit),
            upload_errors: d(self.upload_errors, prev.upload_errors),
            retries: d(self.retries, prev.retries),
            zones_per_flush: self.zones_per_flush.delta(&prev.zones_per_flush),
            gets: d(self.gets, prev.gets),
            get_latency_ms: self.get_latency_ms.delta(&prev.get_latency_ms),
            read_from_s3: d(self.read_from_s3, prev.read_from_s3),
            read_from_ram: d(self.read_from_ram, prev.read_from_ram),
            read_zeros: d(self.read_zeros, prev.read_zeros),
            req_read: d(self.req_read, prev.req_read),
            req_read_bytes: d(self.req_read_bytes, prev.req_read_bytes),
            req_write: d(self.req_write, prev.req_write),
            req_write_bytes: d(self.req_write_bytes, prev.req_write_bytes),
            req_append: d(self.req_append, prev.req_append),
            req_append_bytes: d(self.req_append_bytes, prev.req_append_bytes),
            req_flush: d(self.req_flush, prev.req_flush),
            req_fua: d(self.req_fua, prev.req_fua),
            req_zone_mgmt: d(self.req_zone_mgmt, prev.req_zone_mgmt),
            req_zone_reset: d(self.req_zone_reset, prev.req_zone_reset),
            req_report: d(self.req_report, prev.req_report),
            req_errors: d(self.req_errors, prev.req_errors),
            ..self.clone()
        }
    }
}

pub fn socket_path(dev_id: u32) -> PathBuf {
    PathBuf::from(format!("{SOCKET_DIR}/{dev_id}.sock"))
}

/// Starts answering `stats` requests in a background thread.
pub fn serve(dev_id: u32, snapshot: impl Fn() -> Snapshot + Send + 'static) -> anyhow::Result<()> {
    std::fs::create_dir_all(SOCKET_DIR)?;
    std::fs::set_permissions(SOCKET_DIR, std::fs::Permissions::from_mode(0o700))?;
    let path = socket_path(dev_id);
    // A socket left behind by a crashed server of the same device id.
    let _ = std::fs::remove_file(&path);
    let listener = UnixListener::bind(&path)
        .with_context(|| format!("cannot listen on {}", path.display()))?;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    std::thread::Builder::new()
        .name("btrs3fs-stats".into())
        .spawn(move || {
            for conn in listener.incoming() {
                let res = conn.map_err(anyhow::Error::from).and_then(|stream| {
                    serde_json::to_writer(stream, &snapshot()).map_err(anyhow::Error::from)
                });
                if let Err(e) = res {
                    warn!("stats request failed: {e:#}");
                }
            }
        })?;
    debug!("statistics available on {}", path.display());
    Ok(())
}

pub fn remove_socket(dev_id: u32) {
    let _ = std::fs::remove_file(socket_path(dev_id));
}

/// Client side of `btrs3fs stats`.
pub fn fetch(dev_id: u32) -> anyhow::Result<Snapshot> {
    let path = socket_path(dev_id);
    let mut stream = UnixStream::connect(&path).with_context(|| {
        format!(
            "cannot connect to {} (is device {dev_id} served by btrs3fs?)",
            path.display()
        )
    })?;
    let mut data = Vec::new();
    stream.read_to_end(&mut data)?;
    serde_json::from_slice(&data).context("invalid statistics response")
}

/// Device ids that have a statistics socket.
pub fn served_devices() -> Vec<u32> {
    let mut ids: Vec<u32> = std::fs::read_dir(SOCKET_DIR)
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|e| e.file_name().to_str()?.strip_suffix(".sock")?.parse().ok())
        .collect();
    ids.sort();
    ids
}

fn bytes(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = n as f64;
    let mut u = 0;
    while v >= 1024.0 && u + 1 < UNITS.len() {
        v /= 1024.0;
        u += 1;
    }
    if u == 0 {
        format!("{n} B")
    } else {
        format!("{v:.1} {}", UNITS[u])
    }
}

fn size_label(b: u64) -> String {
    if b >= MIB {
        format!("{}M", b / MIB)
    } else {
        format!("{}K", b / KIB)
    }
}

/// Histogram rows with non-zero counts: (label, count, sum is not per bucket).
fn rows(h: &Hist, label: impl Fn(u64) -> String) -> Vec<(String, u64)> {
    h.counts
        .iter()
        .enumerate()
        .filter(|&(_, &c)| c > 0)
        .map(|(i, &c)| {
            let name = match h.bounds.get(i) {
                Some(&b) => format!("<={}", label(b)),
                None => format!(">{}", label(*h.bounds.last().unwrap_or(&0))),
            };
            (name, c)
        })
        .collect()
}

fn hist_line(h: &Hist, label: impl Fn(u64) -> String + Copy) -> String {
    if h.count() == 0 {
        return "-".into();
    }
    rows(h, label)
        .into_iter()
        .map(|(n, c)| format!("{n}: {c}"))
        .collect::<Vec<_>>()
        .join("  ")
}

/// Integer histogram as exact ranges: `0: 3  1: 5  3-4: 2  >64: 1`.
fn count_line(h: &Hist) -> String {
    if h.count() == 0 {
        return "-".into();
    }
    let mut parts = Vec::new();
    let mut lo = 0;
    for (i, &c) in h.counts.iter().enumerate() {
        let name = match h.bounds.get(i) {
            Some(&hi) if hi == lo => hi.to_string(),
            Some(&hi) => format!("{lo}-{hi}"),
            None => format!(">{}", lo.saturating_sub(1)),
        };
        if c > 0 {
            parts.push(format!("{name}: {c}"));
        }
        lo = h.bounds.get(i).map_or(lo, |&hi| hi + 1);
    }
    parts.join("  ")
}

fn quantiles(h: &Hist) -> String {
    let q = |p: f64| match h.quantile(p) {
        Some(v) => format!("<={v}ms"),
        None if h.count() > 0 => format!(">{}ms", h.bounds.last().unwrap_or(&0)),
        None => "-".into(),
    };
    format!("p50 {}, p90 {}, p99 {}", q(0.5), q(0.9), q(0.99))
}

/// Human-readable report; `interval` is set for `--watch` deltas.
pub fn render(s: &Snapshot, interval: Option<u64>) -> String {
    let mut out = String::new();
    let mut line = |l: String| {
        out.push_str(&l);
        out.push('\n');
    };
    let period = match interval {
        Some(secs) => format!("last {secs}s"),
        None => format!("since start, uptime {}s", s.uptime_secs),
    };
    line(format!("{} on {} ({period})", s.disk, s.device));
    line(format!(
        "RAM buffers: {} in {} zones (limit {}); zones: {} open, {} closed, {} full, {} empty",
        bytes(s.dirty_bytes),
        s.dirty_zones,
        bytes(s.dirty_limit),
        s.zones.open,
        s.zones.closed,
        s.zones.full,
        s.zones.empty
    ));
    line(String::new());

    let uploads = s.uploads_put + s.uploads_patch;
    line(format!(
        "Uploads to S3: {uploads} ({} PUT, {} PATCH), {}, avg {}",
        s.uploads_put,
        s.uploads_patch,
        bytes(s.upload_size.sum),
        s.upload_size
            .sum
            .checked_div(uploads)
            .map_or_else(|| "-".into(), bytes)
    ));
    line(format!(
        "  reason:      FLUSH {}, FUA {}, buffer limit {}, explicit {}",
        s.uploads_flush, s.uploads_fua, s.uploads_background, s.uploads_explicit
    ));
    line(format!(
        "  size:        {}",
        hist_line(&s.upload_size, size_label)
    ));
    line(format!(
        "  latency:     {}",
        quantiles(&s.upload_latency_ms)
    ));
    line(format!(
        "  in parallel: {} now, {} max; retries {}, failed {}",
        s.uploads_in_flight, s.uploads_in_flight_max, s.retries, s.upload_errors
    ));
    line(format!(
        "  zones per FLUSH: {}",
        count_line(&s.zones_per_flush)
    ));
    line(String::new());

    line(format!(
        "Reads: {} requests, {}: from S3 {} ({} GETs, {}), RAM {}, zeros {}",
        s.req_read,
        bytes(s.req_read_bytes),
        bytes(s.read_from_s3),
        s.gets,
        quantiles(&s.get_latency_ms),
        bytes(s.read_from_ram),
        bytes(s.read_zeros)
    ));
    line(format!(
        "Kernel requests: WRITE {} ({}), ZONE_APPEND {} ({}), FLUSH {}, FUA {}, \
         zone reset {}, other zone ops {}, reports {}, errors {}",
        s.req_write,
        bytes(s.req_write_bytes),
        s.req_append,
        bytes(s.req_append_bytes),
        s.req_flush,
        s.req_fua,
        s.req_zone_reset,
        s.req_zone_mgmt,
        s.req_report,
        s.req_errors
    ));
    line(format!(
        "  in parallel: {} now, {} max",
        s.requests_in_flight, s.requests_in_flight_max
    ));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_buckets_and_quantiles() {
        static H: Histogram = Histogram::new(&[10, 100]);
        for v in [1, 10, 11, 50, 100, 1000] {
            H.record(v);
        }
        let s = H.snapshot();
        assert_eq!(s.counts, vec![2, 3, 1]);
        assert_eq!(s.sum, 1172);
        assert_eq!(s.quantile(0.3), Some(10));
        assert_eq!(s.quantile(0.8), Some(100));
        assert_eq!(s.quantile(1.0), None);
        let d = s.delta(&Hist {
            bounds: vec![10, 100],
            counts: vec![1, 1, 0],
            sum: 100,
        });
        assert_eq!(d.counts, vec![1, 2, 1]);
    }

    #[test]
    fn in_flight_tracks_max() {
        static F: InFlight = InFlight::new();
        let a = F.enter();
        let b = F.enter();
        drop(a);
        drop(b);
        let _c = F.enter();
        assert_eq!(F.current.load(Relaxed), 1);
        assert_eq!(F.max.load(Relaxed), 2);
    }

    #[test]
    fn count_ranges() {
        let h = Hist {
            bounds: vec![0, 1, 2, 4, 8],
            counts: vec![3, 5, 0, 2, 0, 1],
            sum: 0,
        };
        assert_eq!(count_line(&h), "0: 3  1: 5  3-4: 2  >8: 1");
    }

    #[test]
    fn render_has_sections() {
        let text = render(&Snapshot::default(), Some(5));
        assert!(text.contains("Uploads to S3"));
        assert!(text.contains("last 5s"));
    }
}

//! Runtime statistics of the device server and its web page.
//!
//! Counters live in a process-wide `STATS` (one server process serves one
//! disk) and are plain relaxed atomics. The server exposes them on
//! `http://127.0.0.1:<port>/`: `/` is a page that polls `/stats.json` every
//! second and computes rates and charts in the browser.

use anyhow::anyhow;
use log::{info, warn};
use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

/// Default port is `BASE_PORT + device number`, so several disks on one host
/// get different ports. 9470 is an arbitrary port outside the well-known
/// range with no common service on it; `--stats-port` overrides it.
pub const BASE_PORT: u16 = 9470;

const PAGE: &str = include_str!("stats.html");

/// Bucket slots of a histogram: enough for every `u64` on every scale (bit
/// length 0..=64 for powers of two; 1-2-5 steps up to 5·10^19).
const SLOTS: usize = 65;

/// How a histogram cuts values into buckets (SPEC.md §9.3). The scale, not a
/// list of bounds, decides: no upper bound is written down anywhere.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Scale {
    /// Bytes, powers of two from one block to the zone size (no request is
    /// larger); every bucket of that range is shown.
    Bytes,
    /// Milliseconds, steps 1, 2, 5, 10, 20, 50, …; no upper bound.
    #[serde(rename = "ms")]
    Millis,
    /// Counts: 0, then powers of two; no upper bound.
    #[default]
    Count,
}

impl Scale {
    /// Upper bound of bucket `i` (the bucket holds values up to it).
    fn bound(self, i: usize) -> u64 {
        match self {
            Scale::Bytes | Scale::Count => {
                if i == 0 {
                    0
                } else {
                    1u64.checked_shl(i as u32 - 1).unwrap_or(u64::MAX)
                }
            }
            Scale::Millis => {
                let step = [1u64, 2, 5][i % 3];
                10u64
                    .checked_pow((i / 3) as u32)
                    .and_then(|d| d.checked_mul(step))
                    .unwrap_or(u64::MAX)
            }
        }
    }

    /// Bucket of `value`: the first one whose bound is not below it.
    fn bucket(self, value: u64) -> usize {
        match self {
            // Bit length of value - 1: 1 → 1, 2 → 2, 3..=4 → 3, …
            Scale::Bytes | Scale::Count => {
                if value == 0 {
                    0
                } else {
                    (u64::BITS - (value - 1).leading_zeros()) as usize + 1
                }
            }
            Scale::Millis => (0..SLOTS)
                .find(|&i| value <= self.bound(i))
                .unwrap_or(SLOTS - 1),
        }
    }
}

/// Largest request, for the range of `Scale::Bytes` histograms: the zone
/// size of the disk, set once the disk is opened.
static ZONE_SIZE: AtomicU64 = AtomicU64::new(0);

pub fn set_zone_size(bytes: u64) {
    ZONE_SIZE.store(bytes, Relaxed);
}

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

    pub fn get(&self) -> u64 {
        self.0.load(Relaxed)
    }
}

/// Values counted in the buckets of a `Scale`.
pub struct Histogram {
    scale: Scale,
    counts: [AtomicU64; SLOTS],
    sum: AtomicU64,
}

impl Histogram {
    const fn new(scale: Scale) -> Self {
        Self {
            scale,
            counts: [const { AtomicU64::new(0) }; SLOTS],
            sum: AtomicU64::new(0),
        }
    }

    pub fn record(&self, value: u64) {
        self.counts[self.scale.bucket(value)].fetch_add(1, Relaxed);
        self.sum.fetch_add(value, Relaxed);
    }

    fn snapshot(&self) -> Hist {
        let counts: Vec<u64> = self.counts.iter().map(|c| c.load(Relaxed)).collect();
        let used = || {
            counts
                .iter()
                .enumerate()
                .filter(|&(_, &c)| c > 0)
                .map(|(i, _)| i)
        };
        let (first, last) = match self.scale {
            Scale::Bytes => {
                let block = self.scale.bucket(crate::config::BLOCK_SIZE);
                let zone = self.scale.bucket(ZONE_SIZE.load(Relaxed).max(1));
                (
                    used().min().map_or(block, |u| u.min(block)),
                    used().max().map_or(zone, |u| u.max(zone)),
                )
            }
            Scale::Millis | Scale::Count => match (used().min(), used().max()) {
                (Some(a), Some(b)) => (a, b),
                _ => (0, 0),
            },
        };
        let total: u64 = counts.iter().sum();
        let sum = self.sum.load(Relaxed);
        // Upper bound of the bucket holding the q-th value.
        let quantile = |q: f64| {
            let rank = ((q * total as f64).ceil() as u64).max(1);
            let mut seen = 0;
            counts.iter().enumerate().find_map(|(i, &c)| {
                seen += c;
                (seen >= rank).then(|| self.scale.bound(i))
            })
        };
        Hist {
            unit: self.scale,
            buckets: (first..=last)
                .map(|i| Bucket {
                    lo: if i == first {
                        None
                    } else {
                        Some(self.scale.bound(i - 1))
                    },
                    hi: self.scale.bound(i),
                    count: counts[i],
                })
                .collect(),
            count: total,
            sum,
            avg: (total > 0).then(|| sum as f64 / total as f64),
            p50: (total > 0).then(|| quantile(0.5)).flatten(),
            p90: (total > 0).then(|| quantile(0.9)).flatten(),
            p99: (total > 0).then(|| quantile(0.99)).flatten(),
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
    /// Zone became FULL: uploaded at once.
    ZoneFull,
    /// Zone was CLOSED: uploaded at once.
    ZoneClosed,
    /// Explicit flush by btrs3fs itself (`create`).
    Explicit,
    /// Zone FINISH: data is stored before the zone is tagged as finished.
    Finish,
    /// The button on the statistics page (`POST /flush`).
    Manual,
}

impl Reason {
    /// Number of reasons: `Manual` is the last one.
    const COUNT: usize = Reason::Manual as usize + 1;
}

pub struct Stats {
    pub upload_size: Histogram,
    pub upload_latency_ms: Histogram,
    pub uploads_put: Counter,
    pub uploads_patch: Counter,
    pub upload_reason: [Counter; Reason::COUNT],
    pub uploads: InFlight,
    pub upload_errors: Counter,
    pub retries: Counter,
    pub zones_per_flush: Histogram,
    pub gets: Counter,
    pub get_size: Histogram,
    pub get_latency_ms: Histogram,
    pub read_from_s3: Counter,
    pub read_from_cache: Counter,
    pub read_ahead_hits: Counter,
    pub read_ahead_misses: Counter,
    pub compactions: Counter,
    pub compacted_segments: Counter,
    pub compacted_bytes: Counter,
    pub compact_aborted: Counter,
    pub compact_errors: Counter,
    pub compacting: InFlight,
    pub compact_time_ms: Histogram,
    pub segments_per_compaction: Histogram,
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
    upload_size: Histogram::new(Scale::Bytes),
    upload_latency_ms: Histogram::new(Scale::Millis),
    uploads_put: Counter::new(),
    uploads_patch: Counter::new(),
    upload_reason: [const { Counter::new() }; Reason::COUNT],
    uploads: InFlight::new(),
    upload_errors: Counter::new(),
    retries: Counter::new(),
    zones_per_flush: Histogram::new(Scale::Count),
    gets: Counter::new(),
    get_size: Histogram::new(Scale::Bytes),
    get_latency_ms: Histogram::new(Scale::Millis),
    read_from_s3: Counter::new(),
    read_from_cache: Counter::new(),
    read_ahead_hits: Counter::new(),
    read_ahead_misses: Counter::new(),
    compactions: Counter::new(),
    compacted_segments: Counter::new(),
    compacted_bytes: Counter::new(),
    compact_aborted: Counter::new(),
    compact_errors: Counter::new(),
    compacting: InFlight::new(),
    compact_time_ms: Histogram::new(Scale::Millis),
    segments_per_compaction: Histogram::new(Scale::Count),
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

/// One bucket: values in `(lo, hi]`; `lo` is absent for the first bucket shown.
#[derive(Serialize, Clone, Debug, Default)]
pub struct Bucket {
    pub lo: Option<u64>,
    pub hi: u64,
    pub count: u64,
}

/// A histogram ready to print: the page formats `lo`/`hi` by `unit` only.
#[derive(Serialize, Clone, Debug, Default)]
pub struct Hist {
    pub unit: Scale,
    pub buckets: Vec<Bucket>,
    pub count: u64,
    pub sum: u64,
    pub avg: Option<f64>,
    /// Upper bounds of the buckets holding the 50th, 90th and 99th percentile.
    pub p50: Option<u64>,
    pub p90: Option<u64>,
    pub p99: Option<u64>,
}

#[derive(Serialize, Clone, Debug, Default)]
pub struct ZoneCounts {
    pub empty: u64,
    pub open: u64,
    pub closed: u64,
    pub full: u64,
    /// Zones with data, but less than the zone: open, closed, or FULL after
    /// an early FINISH.
    pub partial: u64,
    /// Zones served by more than one object (SEGMENTS, not concatenated yet).
    pub fragmented: u64,
    /// Objects of all zones, and the most in one zone.
    pub objects: u64,
    pub max_objects: u64,
    /// Zones waiting for concatenation, of them FULL (urgent).
    pub compact_queued: u64,
    pub compact_urgent: u64,
}

/// `/stats.json`. Counters are totals since the server started; the page
/// derives rates from consecutive snapshots.
#[derive(Serialize, Clone, Debug, Default)]
pub struct Snapshot {
    pub disk: String,
    pub device: String,
    /// How zone objects grow; concatenation exists only with `SEGMENTS`.
    pub append: crate::config::AppendMode,
    pub uptime_secs: u64,
    pub dirty_bytes: u64,
    /// Background uploads start above `dirty_limit`; writes wait at `dirty_hard_limit`.
    pub dirty_limit: u64,
    pub dirty_hard_limit: u64,
    pub dirty_zones: u64,
    pub zones: ZoneCounts,
    pub upload_size: Hist,
    pub upload_latency_ms: Hist,
    pub uploads_put: u64,
    pub uploads_patch: u64,
    pub uploads_flush: u64,
    pub uploads_fua: u64,
    pub uploads_background: u64,
    pub uploads_zone_full: u64,
    pub uploads_zone_closed: u64,
    pub uploads_explicit: u64,
    pub uploads_finish: u64,
    pub uploads_manual: u64,
    pub uploads_in_flight: u64,
    pub uploads_in_flight_max: u64,
    pub upload_errors: u64,
    pub retries: u64,
    pub zones_per_flush: Hist,
    pub gets: u64,
    pub get_size: Hist,
    pub get_latency_ms: Hist,
    pub read_from_s3: u64,
    pub read_from_cache: u64,
    pub read_ahead_hits: u64,
    pub read_ahead_misses: u64,
    pub compactions: u64,
    pub compacted_segments: u64,
    pub compacted_bytes: u64,
    pub compact_aborted: u64,
    pub compact_errors: u64,
    pub compacting: u64,
    pub compacting_max: u64,
    pub compact_time_ms: Hist,
    pub segments_per_compaction: Hist,
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
    pub append: crate::config::AppendMode,
    pub uptime_secs: u64,
    pub dirty_bytes: u64,
    pub dirty_limit: u64,
    pub dirty_hard_limit: u64,
    pub dirty_zones: u64,
    pub zones: ZoneCounts,
}

impl Stats {
    pub fn snapshot(&self, d: DiskState) -> Snapshot {
        Snapshot {
            disk: d.disk,
            device: d.device,
            append: d.append,
            uptime_secs: d.uptime_secs,
            dirty_bytes: d.dirty_bytes,
            dirty_limit: d.dirty_limit,
            dirty_hard_limit: d.dirty_hard_limit,
            dirty_zones: d.dirty_zones,
            zones: d.zones,
            upload_size: self.upload_size.snapshot(),
            upload_latency_ms: self.upload_latency_ms.snapshot(),
            uploads_put: self.uploads_put.get(),
            uploads_patch: self.uploads_patch.get(),
            uploads_flush: self.reason(Reason::Flush).get(),
            uploads_fua: self.reason(Reason::Fua).get(),
            uploads_background: self.reason(Reason::Background).get(),
            uploads_zone_full: self.reason(Reason::ZoneFull).get(),
            uploads_zone_closed: self.reason(Reason::ZoneClosed).get(),
            uploads_explicit: self.reason(Reason::Explicit).get(),
            uploads_finish: self.reason(Reason::Finish).get(),
            uploads_manual: self.reason(Reason::Manual).get(),
            uploads_in_flight: self.uploads.current.load(Relaxed),
            uploads_in_flight_max: self.uploads.max.load(Relaxed),
            upload_errors: self.upload_errors.get(),
            retries: self.retries.get(),
            zones_per_flush: self.zones_per_flush.snapshot(),
            gets: self.gets.get(),
            get_size: self.get_size.snapshot(),
            get_latency_ms: self.get_latency_ms.snapshot(),
            read_from_s3: self.read_from_s3.get(),
            read_from_cache: self.read_from_cache.get(),
            read_ahead_hits: self.read_ahead_hits.get(),
            read_ahead_misses: self.read_ahead_misses.get(),
            compactions: self.compactions.get(),
            compacted_segments: self.compacted_segments.get(),
            compacted_bytes: self.compacted_bytes.get(),
            compact_aborted: self.compact_aborted.get(),
            compact_errors: self.compact_errors.get(),
            compacting: self.compacting.current.load(Relaxed),
            compacting_max: self.compacting.max.load(Relaxed),
            compact_time_ms: self.compact_time_ms.snapshot(),
            segments_per_compaction: self.segments_per_compaction.snapshot(),
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

/// Serves the statistics page on 127.0.0.1:`port` in a background thread.
/// Serves the page, `/stats.json` and the actions `POST /<name>`: `action`
/// gets the name and answers with a JSON body, or `None` for an unknown one.
pub fn serve_http(
    port: u16,
    snapshot: impl Fn() -> Snapshot + Send + 'static,
    action: impl Fn(&str) -> Option<String> + Send + 'static,
) -> anyhow::Result<()> {
    let server = tiny_http::Server::http(("127.0.0.1", port))
        .map_err(|e| anyhow!("cannot listen on 127.0.0.1:{port}: {e}"))?;
    std::thread::Builder::new()
        .name("btrs3fs-stats".into())
        .spawn(move || {
            for req in server.incoming_requests() {
                let path = req.url().split('?').next().unwrap_or_default();
                let post = *req.method() == tiny_http::Method::Post;
                let (status, body, content_type) = match path {
                    // POST only: links and browser prefetch must not start actions.
                    p if post => match action(p.trim_start_matches('/')) {
                        Some(json) => (200, json, "application/json"),
                        None => (404, "no such action\n".into(), "text/plain; charset=utf-8"),
                    },
                    "/compact" | "/flush" => {
                        (405, "use POST\n".into(), "text/plain; charset=utf-8")
                    }
                    "/" | "/index.html" => (200, PAGE.to_string(), "text/html; charset=utf-8"),
                    "/stats.json" => match serde_json::to_string(&snapshot()) {
                        Ok(json) => (200, json, "application/json"),
                        Err(e) => (500, e.to_string(), "text/plain; charset=utf-8"),
                    },
                    _ => (404, "not found\n".into(), "text/plain; charset=utf-8"),
                };
                let response = tiny_http::Response::from_string(body)
                    .with_status_code(status)
                    .with_header(header("Content-Type", content_type))
                    .with_header(header("Cache-Control", "no-store"));
                if let Err(e) = req.respond(response) {
                    warn!("statistics request failed: {e}");
                }
            }
        })?;
    info!("statistics on http://127.0.0.1:{port}/");
    Ok(())
}

fn header(name: &str, value: &str) -> tiny_http::Header {
    tiny_http::Header::from_bytes(name.as_bytes(), value.as_bytes())
        .expect("static header is valid")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scales() {
        let bounds = |sc: Scale| (0..8).map(|i| sc.bound(i)).collect::<Vec<_>>();
        assert_eq!(bounds(Scale::Count), [0, 1, 2, 4, 8, 16, 32, 64]);
        assert_eq!(bounds(Scale::Millis), [1, 2, 5, 10, 20, 50, 100, 200]);
        for sc in [Scale::Bytes, Scale::Millis, Scale::Count] {
            for v in [0, 1, 2, 3, 4, 5, 99, 4096, 4097, 1 << 40, u64::MAX] {
                let b = sc.bucket(v);
                assert!(v <= sc.bound(b), "{sc:?} {v}");
                assert!(b == 0 || v > sc.bound(b - 1), "{sc:?} {v}");
            }
        }
    }

    #[test]
    fn histogram_snapshot() {
        static T: Histogram = Histogram::new(Scale::Millis);
        for v in [3, 3, 3, 40, 900] {
            T.record(v);
        }
        let s = T.snapshot();
        // From the first to the last used bucket: 5, 10, 20, 50, …, 1000 ms.
        assert_eq!(s.buckets.first().map(|b| (b.lo, b.hi)), Some((None, 5)));
        assert_eq!(
            s.buckets.last().map(|b| (b.lo, b.hi)),
            Some((Some(500), 1000))
        );
        assert_eq!(
            (s.count, s.sum, s.p50, s.p90, s.p99),
            (5, 949, Some(5), Some(1000), Some(1000))
        );

        // Bytes: always from one block to the zone size, even when empty there.
        static B: Histogram = Histogram::new(Scale::Bytes);
        set_zone_size(16 << 20);
        B.record(2 << 20);
        let s = B.snapshot();
        let his: Vec<u64> = s.buckets.iter().map(|b| b.hi).collect();
        assert_eq!(his.first(), Some(&4096));
        assert_eq!(his.last(), Some(&(16 << 20)));
        assert_eq!(his.len(), 13);
        assert_eq!(s.p50, Some(2 << 20));

        let empty = Histogram::new(Scale::Count).snapshot();
        assert_eq!((empty.count, empty.avg, empty.p50), (0, None, None));
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
    fn serves_page_and_json() {
        // Port 0 cannot be used with a fixed URL; pick a high fixed test port.
        let port = 39470;
        serve_http(
            port,
            || Snapshot {
                disk: "t/".into(),
                ..Default::default()
            },
            |name| (name == "compact").then(|| "{\"queued\":3}".to_string()),
        )
        .unwrap();
        let send = |method: &str, path: &str| {
            use std::io::{Read, Write};
            let mut s = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
            write!(
                s,
                "{method} {path} HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            )
            .unwrap();
            let mut out = String::new();
            s.read_to_string(&mut out).unwrap();
            out
        };
        let get = |path: &str| send("GET", path);
        let page = get("/");
        assert!(page.starts_with("HTTP/1.1 200") && page.contains("<html"));
        let json = get("/stats.json");
        assert!(json.contains("\"disk\":\"t/\""));
        assert!(get("/nope").starts_with("HTTP/1.1 404"));
        assert!(get("/compact").starts_with("HTTP/1.1 405"));
        let posted = send("POST", "/compact");
        assert!(posted.starts_with("HTTP/1.1 200") && posted.ends_with("{\"queued\":3}"));
        assert!(send("POST", "/nope").starts_with("HTTP/1.1 404"));
    }
}

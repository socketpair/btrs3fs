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

const KIB: u64 = 1 << 10;
const MIB: u64 = 1 << 20;

/// Buckets of the upload and GET size histograms: ×4 steps from one block (4 KiB) to
/// the largest zone (32 MiB, config::MAX_ZONE_SIZE) — an append is never
/// smaller than a block nor larger than a zone.
const SIZE_BOUNDS: &[u64] = &[
    4 * KIB,
    16 * KIB,
    64 * KIB,
    256 * KIB,
    MIB,
    4 * MIB,
    16 * MIB,
    32 * MIB, // largest zone
];
/// Buckets of S3 request time, ms: from in-datacenter latency (10 ms) up to
/// the slow PATCHes measured on large appended objects (~30 s, SPEC.md §11).
const LATENCY_BOUNDS_MS: &[u64] = &[10, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000];

/// Buckets of zones uploaded per FLUSH: 0 (nothing to do), then powers of two;
/// up to 64 upload at once (tuning::UPLOADS_MAX), the rest queue up.
const ZONES_BOUNDS: &[u64] = &[0, 1, 2, 4, 8, 16, 32, 64];

/// Room for the longest bounds list plus its open-ended last bucket.
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
    /// Zone became FULL: uploaded at once.
    ZoneFull,
    /// Zone was CLOSED: uploaded at once.
    ZoneClosed,
    /// Explicit flush by btrs3fs itself (`create`).
    Explicit,
    /// Zone FINISH: data is stored before the zone is tagged as finished.
    Finish,
}

impl Reason {
    /// Number of reasons: `Finish` is the last one.
    const COUNT: usize = Reason::Finish as usize + 1;
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
    upload_reason: [const { Counter::new() }; Reason::COUNT],
    uploads: InFlight::new(),
    upload_errors: Counter::new(),
    retries: Counter::new(),
    zones_per_flush: Histogram::new(ZONES_BOUNDS),
    gets: Counter::new(),
    get_size: Histogram::new(SIZE_BOUNDS),
    get_latency_ms: Histogram::new(LATENCY_BOUNDS_MS),
    read_from_s3: Counter::new(),
    read_from_cache: Counter::new(),
    read_ahead_hits: Counter::new(),
    read_ahead_misses: Counter::new(),
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

#[derive(Serialize, Clone, Debug, Default)]
pub struct Hist {
    pub bounds: Vec<u64>,
    pub counts: Vec<u64>,
    pub sum: u64,
}

#[derive(Serialize, Clone, Debug, Default)]
pub struct ZoneCounts {
    pub empty: u64,
    pub open: u64,
    pub closed: u64,
    pub full: u64,
}

/// `/stats.json`. Counters are totals since the server started; the page
/// derives rates from consecutive snapshots.
#[derive(Serialize, Clone, Debug, Default)]
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
    pub uploads_zone_full: u64,
    pub uploads_zone_closed: u64,
    pub uploads_explicit: u64,
    pub uploads_finish: u64,
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
            uploads_zone_full: self.reason(Reason::ZoneFull).get(),
            uploads_zone_closed: self.reason(Reason::ZoneClosed).get(),
            uploads_explicit: self.reason(Reason::Explicit).get(),
            uploads_finish: self.reason(Reason::Finish).get(),
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
pub fn serve_http(
    port: u16,
    snapshot: impl Fn() -> Snapshot + Send + 'static,
) -> anyhow::Result<()> {
    let server = tiny_http::Server::http(("127.0.0.1", port))
        .map_err(|e| anyhow!("cannot listen on 127.0.0.1:{port}: {e}"))?;
    std::thread::Builder::new()
        .name("btrs3fs-stats".into())
        .spawn(move || {
            for req in server.incoming_requests() {
                let path = req.url().split('?').next().unwrap_or_default();
                let (status, body, content_type) = match path {
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
    fn histogram_buckets() {
        static H: Histogram = Histogram::new(&[10, 100]);
        for v in [1, 10, 11, 50, 100, 1000] {
            H.record(v);
        }
        let s = H.snapshot();
        assert_eq!(s.counts, vec![2, 3, 1]);
        assert_eq!(s.sum, 1172);
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
        serve_http(port, || Snapshot {
            disk: "t/".into(),
            ..Default::default()
        })
        .unwrap();
        let get = |path: &str| {
            use std::io::{Read, Write};
            let mut s = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
            write!(
                s,
                "GET {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"
            )
            .unwrap();
            let mut out = String::new();
            s.read_to_string(&mut out).unwrap();
            out
        };
        let page = get("/");
        assert!(page.starts_with("HTTP/1.1 200") && page.contains("<html"));
        let json = get("/stats.json");
        assert!(json.contains("\"disk\":\"t/\""));
        assert!(get("/nope").starts_with("HTTP/1.1 404"));
    }
}

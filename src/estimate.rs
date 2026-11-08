//! Passive estimates of the link to S3 from the daemon's own requests
//! (SPEC.md §6.1): latency from GET time-to-headers, the rate of one stream
//! from large GETs and uploads. The totals come from the connection
//! regulator, which sees the whole link.

use crate::tuning::Chars;
use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Samples kept per estimate: the last ones only, so a change of the link
/// (VPN on or off) shows within a few dozen requests.
const SAMPLES: usize = 64;
/// Requests smaller than this say little about the rate of a stream. It is
/// the smallest read window: the daemon's GETs are never larger than the
/// window, so a higher floor would keep the window from ever growing.
const STREAM_MIN: u64 = 128 << 10;
/// Uploads up to this size are mostly latency (an estimate of it before any GET).
const SMALL_PUT: u64 = 64 << 10;

/// Direction of a request, for the regulators.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Dir {
    Read,
    Write,
}

impl Dir {
    /// The direction of a request named as in the retry log (`GET <key>`).
    pub fn of(what: &str) -> Option<Dir> {
        match what.split(' ').next()? {
            "GET" => Some(Dir::Read),
            "PUT" | "PATCH" | "APPEND" => Some(Dir::Write),
            _ => None,
        }
    }
}

struct Samples {
    /// GET time to the response headers.
    ttfb: VecDeque<Duration>,
    /// Bytes per second of large GET bodies.
    read_rates: VecDeque<f64>,
    /// (bytes, whole time) of uploads.
    puts: VecDeque<(u64, Duration)>,
    /// The last measurement: stands for what has no samples since.
    seeded: Chars,
}

pub struct Observations {
    samples: Mutex<Samples>,
    retries: [AtomicU64; 2],
}

fn push<T>(ring: &mut VecDeque<T>, v: T) {
    if ring.len() == SAMPLES {
        ring.pop_front();
    }
    ring.push_back(v);
}

/// The value at quantile `q` (0..=1) of the samples, `None` without any.
fn quantile<T: Copy + PartialOrd>(v: impl Iterator<Item = T>, q: f64) -> Option<T> {
    let mut v: Vec<T> = v.collect();
    if v.is_empty() {
        return None;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let i = ((q * v.len() as f64).ceil() as usize).clamp(1, v.len()) - 1;
    Some(v[i])
}

pub static OBS: Observations = Observations::new();

impl Observations {
    pub const fn new() -> Self {
        Self {
            samples: Mutex::new(Samples {
                ttfb: VecDeque::new(),
                read_rates: VecDeque::new(),
                puts: VecDeque::new(),
                seeded: Chars {
                    latency: None,
                    read_stream: None,
                    read_total: None,
                    write_stream: None,
                    write_total: None,
                },
            }),
            retries: [AtomicU64::new(0), AtomicU64::new(0)],
        }
    }

    /// A GET that succeeded: time to its headers, then its body.
    pub fn get(&self, ttfb: Duration, bytes: u64, body: Duration) {
        let mut s = self.samples.lock().unwrap();
        push(&mut s.ttfb, ttfb);
        if bytes >= STREAM_MIN && !body.is_zero() {
            push(&mut s.read_rates, bytes as f64 / body.as_secs_f64());
        }
    }

    /// An upload that succeeded (its answer comes only after the body).
    pub fn put(&self, bytes: u64, total: Duration) {
        push(&mut self.samples.lock().unwrap().puts, (bytes, total));
    }

    pub fn retry(&self, dir: Dir) {
        self.retries[dir as usize].fetch_add(1, Ordering::SeqCst);
    }

    /// Retries in `dir` since the last call.
    pub fn take_retries(&self, dir: Dir) -> u64 {
        self.retries[dir as usize].swap(0, Ordering::SeqCst)
    }

    /// A measurement: its values stand until new samples arrive.
    pub fn seed(&self, c: Chars) {
        let mut s = self.samples.lock().unwrap();
        s.ttfb.clear();
        s.read_rates.clear();
        s.puts.clear();
        s.seeded = c;
    }

    /// Latency and the rates of one stream (SPEC.md §6.1); what has no
    /// samples keeps the seeded value, else `prev`. Totals are `prev`'s.
    pub fn estimate(&self, prev: Chars) -> Chars {
        let s = self.samples.lock().unwrap();
        // The median: one slow answer does not move it.
        let latency = quantile(s.ttfb.iter().copied(), 0.5)
            .or(s.seeded.latency)
            .or(prev.latency);
        // p90: concurrent requests share the link and understate one stream.
        let read_stream = quantile(s.read_rates.iter().copied(), 0.9)
            .map(|r| r as u64)
            .or(s.seeded.read_stream)
            .or(prev.read_stream);
        // time = L + size / W₁, L from GETs or the quickest small upload.
        let l = latency.or_else(|| {
            s.puts
                .iter()
                .filter(|(b, _)| *b <= SMALL_PUT)
                .map(|(_, t)| *t)
                .min()
        });
        let write_rates = s.puts.iter().filter_map(|&(bytes, t)| {
            let body = t.checked_sub(l?)?;
            (bytes >= STREAM_MIN && !body.is_zero()).then(|| bytes as f64 / body.as_secs_f64())
        });
        let write_stream = quantile(write_rates, 0.9)
            .map(|r| r as u64)
            .or(s.seeded.write_stream)
            .or(prev.write_stream);
        Chars {
            latency,
            read_stream,
            write_stream,
            ..prev
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = 1 << 20;
    const KIB: u64 = 1 << 10;
    const MS: fn(u64) -> Duration = Duration::from_millis;

    fn near(got: Option<u64>, want: f64, tolerance: f64) -> bool {
        got.is_some_and(|g| (g as f64 - want).abs() <= want * tolerance)
    }

    #[test]
    fn latency_is_median_ttfb() {
        let o = Observations::new();
        for t in [10, 20, 30, 1000] {
            o.get(MS(t), 4 * KIB, MS(1));
        }
        let l = o.estimate(Chars::default()).latency.unwrap();
        assert!((MS(20)..=MS(30)).contains(&l), "{l:?}");
    }

    #[test]
    fn read_stream_ignores_small_gets() {
        let o = Observations::new();
        let prev = Chars {
            read_stream: Some(7),
            ..Chars::default()
        };
        for _ in 0..10 {
            o.get(MS(30), 4 * KIB, MS(1));
        }
        assert_eq!(o.estimate(prev).read_stream, Some(7));
    }

    /// GETs of the smallest read window still feed the estimate: it must be
    /// able to grow the window from its start value.
    #[test]
    fn read_stream_counts_unit_sized_gets() {
        let o = Observations::new();
        for _ in 0..8 {
            o.get(MS(30), 128 * KIB, MS(10));
        }
        let r = o.estimate(Chars::default()).read_stream;
        assert!(near(r, 128.0 * KIB as f64 / 0.010, 0.01), "{r:?}");
    }

    #[test]
    fn read_stream_p90() {
        let o = Observations::new();
        for i in 1..=64u64 {
            o.get(MS(30), MIB, MS(10 * i));
        }
        // Rates 1 MiB / (10..640 ms); p90 is the 7th fastest: 1 MiB / 70 ms.
        let r = o.estimate(Chars::default()).read_stream;
        assert!(near(r, MIB as f64 / 0.070, 0.15), "{r:?}");
    }

    #[test]
    fn write_stream_from_line() {
        let o = Observations::new();
        for _ in 0..8 {
            o.get(MS(30), 4 * KIB, MS(1));
            o.put(4 * MIB, MS(130));
        }
        let w = o.estimate(Chars::default()).write_stream;
        assert!(near(w, 40.0 * MIB as f64, 0.05), "{w:?}");
    }

    #[test]
    fn write_stream_without_gets_uses_small_puts() {
        let o = Observations::new();
        o.put(4 * KIB, MS(30));
        o.put(4 * MIB, MS(130));
        let w = o.estimate(Chars::default()).write_stream;
        assert!(near(w, 40.0 * MIB as f64, 0.05), "{w:?}");
    }

    #[test]
    fn seed_replaces() {
        let o = Observations::new();
        for _ in 0..8 {
            o.get(MS(30), MIB, MS(100));
        }
        let seeded = Chars {
            latency: Some(MS(50)),
            read_stream: Some(3 * MIB),
            write_stream: Some(2 * MIB),
            ..Chars::default()
        };
        o.seed(seeded);
        let e = o.estimate(Chars::default());
        assert_eq!(
            (e.latency, e.read_stream, e.write_stream),
            (Some(MS(50)), Some(3 * MIB), Some(2 * MIB))
        );
        // New samples count again.
        o.get(MS(10), 4 * KIB, MS(1));
        assert_eq!(o.estimate(Chars::default()).latency, Some(MS(10)));
    }

    #[test]
    fn retries_are_counted_per_direction() {
        let o = Observations::new();
        o.retry(Dir::Read);
        o.retry(Dir::Write);
        o.retry(Dir::Write);
        assert_eq!(o.take_retries(Dir::Write), 2);
        assert_eq!(o.take_retries(Dir::Write), 0);
        assert_eq!(o.take_retries(Dir::Read), 1);
        assert_eq!(Dir::of("GET t/x bytes=0-1"), Some(Dir::Read));
        assert_eq!(Dir::of("PATCH t/x"), Some(Dir::Write));
        assert_eq!(Dir::of("PUT t/x"), Some(Dir::Write));
        assert_eq!(Dir::of("DELETE t/x"), None);
    }
}

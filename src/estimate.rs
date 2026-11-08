//! Passive estimates of the link to S3 from the daemon's own requests
//! (SPEC.md §6.1): latency from GET time-to-headers, the rate of one stream
//! from GETs and uploads whose body took long enough to show it. The totals
//! come from the connection regulator, which sees the whole link.

use crate::tuning::Chars;
use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Samples kept per estimate: the last ones only, so a change of the link
/// (VPN on or off) shows within a few dozen requests.
const SAMPLES: usize = 64;
/// A body shorter than this many latencies measures socket buffers and the
/// start of TCP, not the link: such requests never count for a stream rate
/// (user decision). The read window is sized so its body takes one latency;
/// the probe GETs of 4 windows (SPEC.md §6.1) pass this.
const BODY_LATENCIES: u32 = 2;
/// Stream rates need at least this many long requests.
pub const STREAM_SAMPLES_MIN: usize = 8;
/// Before any latency is known, uploads at least this large are kept, to be
/// judged once it is.
const PUT_KEEP: u64 = 1 << 20;

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

/// What an estimate stands on, for the statistics page.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Source {
    /// Requests that count, and their bytes; fewer than the minimum: not
    /// enough yet.
    Samples { n: usize, bytes: u64 },
    /// The last measurement, no newer samples.
    Measured,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Sources {
    pub latency: Source,
    pub read_stream: Source,
    pub write_stream: Source,
}

struct Samples {
    /// GET time to the response headers, and its bytes.
    ttfb: VecDeque<(Duration, u64)>,
    /// (bytes, body time) of GETs whose body was long.
    reads: VecDeque<(u64, Duration)>,
    /// (bytes, whole time) of uploads that may count.
    puts: VecDeque<(u64, Duration)>,
    /// The last measurement: stands for what has no samples since.
    seeded: Chars,
}

impl Samples {
    fn latency(&self) -> Option<Duration> {
        quantile(self.ttfb.iter().map(|&(t, _)| t), 0.5).or(self.seeded.latency)
    }

    fn long_reads(&self, l: Duration) -> Vec<(u64, Duration)> {
        self.reads
            .iter()
            .copied()
            .filter(|&(_, body)| body >= l * BODY_LATENCIES)
            .collect()
    }

    /// (bytes, body time) of uploads whose body (time − L) was long.
    fn long_puts(&self, l: Option<Duration>) -> Vec<(u64, Duration)> {
        let Some(l) = l else { return Vec::new() };
        self.puts
            .iter()
            .filter_map(|&(bytes, t)| {
                let body = t.checked_sub(l)?;
                (body >= l * BODY_LATENCIES).then_some((bytes, body))
            })
            .collect()
    }
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

/// p90 of the rates of long requests, if there are enough: concurrent
/// requests share the link and understate one stream, so not the mean.
fn stream_rate(long: &[(u64, Duration)]) -> Option<u64> {
    if long.len() < STREAM_SAMPLES_MIN {
        return None;
    }
    let rates = long
        .iter()
        .map(|&(bytes, t)| bytes as f64 / t.as_secs_f64());
    quantile(rates, 0.9).map(|r| r as u64)
}

fn source(long: &[(u64, Duration)], seeded: bool) -> Source {
    if long.len() < STREAM_SAMPLES_MIN && seeded {
        return Source::Measured;
    }
    Source::Samples {
        n: long.len(),
        bytes: long.iter().map(|&(b, _)| b).sum(),
    }
}

pub static OBS: Observations = Observations::new();

impl Observations {
    pub const fn new() -> Self {
        Self {
            samples: Mutex::new(Samples {
                ttfb: VecDeque::new(),
                reads: VecDeque::new(),
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

    /// A GET that succeeded: time to its headers, then its body. Kept for a
    /// stream rate only if its body was long, so the many short GETs do not
    /// push the few long ones out.
    pub fn get(&self, ttfb: Duration, bytes: u64, body: Duration) {
        let mut s = self.samples.lock().unwrap();
        push(&mut s.ttfb, (ttfb, bytes));
        let l = s.latency().unwrap_or(ttfb);
        if body >= l * BODY_LATENCIES {
            push(&mut s.reads, (bytes, body));
        }
    }

    /// An upload that succeeded (its answer comes only after the body).
    pub fn put(&self, bytes: u64, total: Duration) {
        let mut s = self.samples.lock().unwrap();
        let keep = match s.latency() {
            Some(l) => total >= l * (BODY_LATENCIES + 1),
            None => bytes >= PUT_KEEP,
        };
        if keep {
            push(&mut s.puts, (bytes, total));
        }
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
        s.reads.clear();
        s.puts.clear();
        s.seeded = c;
    }

    /// Latency and the rates of one stream (SPEC.md §6.1); what has not
    /// enough samples keeps the measured value, else `prev`. Totals are
    /// `prev`'s.
    pub fn estimate(&self, prev: Chars) -> Chars {
        let s = self.samples.lock().unwrap();
        let latency = s.latency().or(prev.latency);
        let read_stream = latency
            .and_then(|l| stream_rate(&s.long_reads(l)))
            .or(s.seeded.read_stream)
            .or(prev.read_stream);
        let write_stream = stream_rate(&s.long_puts(latency))
            .or(s.seeded.write_stream)
            .or(prev.write_stream);
        Chars {
            latency,
            read_stream,
            write_stream,
            ..prev
        }
    }

    /// What each estimate stands on.
    pub fn sources(&self) -> Sources {
        let s = self.samples.lock().unwrap();
        let l = s.latency();
        let latency = if s.ttfb.is_empty() && s.seeded.latency.is_some() {
            Source::Measured
        } else {
            Source::Samples {
                n: s.ttfb.len(),
                bytes: s.ttfb.iter().map(|&(_, b)| b).sum(),
            }
        };
        let reads = l.map_or_else(Vec::new, |l| s.long_reads(l));
        Sources {
            latency,
            read_stream: source(&reads, s.seeded.read_stream.is_some()),
            write_stream: source(&s.long_puts(l), s.seeded.write_stream.is_some()),
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

    /// Small GETs: latency 30 ms.
    fn with_latency(o: &Observations, n: usize) {
        for _ in 0..n {
            o.get(MS(30), 4 * KIB, MS(1));
        }
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

    /// A body shorter than two latencies measures buffers and the start of
    /// TCP, not the link: such GETs never count, however many.
    #[test]
    fn short_bodies_never_count() {
        let o = Observations::new();
        with_latency(&o, 8);
        for _ in 0..50 {
            o.get(MS(30), 128 * KIB, MS(10));
        }
        let prev = Chars {
            read_stream: Some(7),
            ..Chars::default()
        };
        assert_eq!(o.estimate(prev).read_stream, Some(7));
        assert_eq!(o.sources().read_stream, Source::Samples { n: 0, bytes: 0 });
    }

    #[test]
    fn long_bodies_count_from_eight() {
        let o = Observations::new();
        with_latency(&o, 8);
        for _ in 0..7 {
            o.get(MS(30), 512 * KIB, MS(100));
        }
        assert_eq!(o.estimate(Chars::default()).read_stream, None);
        o.get(MS(30), 512 * KIB, MS(100));
        let r = o.estimate(Chars::default()).read_stream;
        assert!(near(r, 512.0 * KIB as f64 / 0.100, 0.01), "{r:?}");
        assert_eq!(
            o.sources().read_stream,
            Source::Samples {
                n: 8,
                bytes: 8 * 512 * KIB
            }
        );
    }

    /// Probes come once in 10 s among many small GETs: these must not push
    /// them out of the samples.
    #[test]
    fn small_gets_do_not_push_out_long_ones() {
        let o = Observations::new();
        with_latency(&o, 8);
        for _ in 0..8 {
            o.get(MS(30), 512 * KIB, MS(100));
        }
        with_latency(&o, 500);
        assert!(o.estimate(Chars::default()).read_stream.is_some());
    }

    #[test]
    fn read_stream_p90() {
        let o = Observations::new();
        with_latency(&o, 8);
        for i in 1..=64u64 {
            o.get(MS(30), 4 * MIB, MS(60 + 10 * i));
        }
        // Rates 4 MiB / (70..700 ms); p90 is the 7th fastest: 4 MiB / 130 ms.
        let r = o.estimate(Chars::default()).read_stream;
        assert!(near(r, 4.0 * MIB as f64 / 0.130, 0.15), "{r:?}");
    }

    #[test]
    fn write_stream_from_long_uploads() {
        let o = Observations::new();
        with_latency(&o, 8);
        // FUA-sized uploads: all latency, they never count.
        for _ in 0..50 {
            o.put(4 * KIB, MS(35));
        }
        for _ in 0..7 {
            o.put(4 * MIB, MS(130));
        }
        assert_eq!(o.estimate(Chars::default()).write_stream, None);
        o.put(4 * MIB, MS(130));
        let w = o.estimate(Chars::default()).write_stream;
        assert!(near(w, 40.0 * MIB as f64, 0.05), "{w:?}");
    }

    #[test]
    fn write_stream_waits_for_a_latency() {
        let o = Observations::new();
        for _ in 0..8 {
            o.put(16 * MIB, MS(700));
        }
        // Without a latency the line has no start: nothing yet.
        assert_eq!(o.estimate(Chars::default()).write_stream, None);
        with_latency(&o, 8);
        let w = o.estimate(Chars::default()).write_stream;
        assert!(near(w, 16.0 * MIB as f64 / 0.670, 0.02), "{w:?}");
    }

    #[test]
    fn seed_replaces() {
        let o = Observations::new();
        with_latency(&o, 8);
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
        assert_eq!(o.sources().read_stream, Source::Measured);
        // New samples count again.
        o.get(MS(10), 4 * KIB, MS(1));
        assert_eq!(o.estimate(Chars::default()).latency, Some(MS(10)));
        assert_eq!(
            o.sources().latency,
            Source::Samples {
                n: 1,
                bytes: 4 * KIB
            }
        );
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

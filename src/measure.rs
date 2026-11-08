//! Measuring the link to S3 (SPEC.md §6.1), for `btrs3fs measure` and the
//! button of the statistics page. Reads only existing objects of the disk
//! (zones stored in one object, picked at random); writes temporary objects
//! under `<prefix>/measure-<random>/` and deletes them. In the daemon the
//! whole run fits a time budget; each phase stops at its deadline with what
//! it has, and a phase without a single answer is skipped.

use crate::config::DiskConfig;
use crate::s3::S3Store;
use crate::store::{Body, Layout, ObjectInfo, StoreResult};
use crate::tuning::{Auto, Chars, Manual};
use bytes::Bytes;
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use std::future::Future;
use std::time::{Duration, Instant, SystemTime};

const KIB: u64 = 1 << 10;
const MIB: u64 = 1 << 20;

/// The daemon's budget for one measurement (user decision).
pub const BUDGET: Duration = Duration::from_secs(20);
/// Shares of the budget: latency, read stream, read total, write stream,
/// write total; the rest is left for the cleanup.
const PHASES: [Duration; 5] = [
    Duration::from_secs(3),
    Duration::from_secs(4),
    Duration::from_secs(4),
    Duration::from_secs(4),
    Duration::from_secs(4),
];
const PHASE_NAMES: [&str; 5] = [
    "latency",
    "read_stream",
    "read_total",
    "write_stream",
    "write_total",
];

/// Large request of a stream measurement: a whole zone, as uploads are; within
/// a budget 4 MiB, which a slow link still moves in a phase.
const STREAM_SIZE: u64 = 16 * MIB;
const STREAM_SIZE_BUDGET: u64 = 4 * MIB;
/// One-stream measurements, the median is taken. Each is a pair of requests
/// one after the other — a small one and a large one — and the rate is taken
/// from the difference, so the latency of that connection cancels out.
const STREAM_RUNS: usize = 5;
/// Small GETs for the latency: rounds of `LATENCY_PARALLEL` at once, so the
/// sample spreads over several connections (single connections were seen to
/// differ: 15 ms on one, 37 ms on another), as the daemon's requests do.
const LATENCY_ROUNDS: usize = 4;
const LATENCY_PARALLEL: usize = 16;
const SMALL_SIZE: u64 = 4 * KIB;
/// Requests of the total-rate measurements, at once, and their size; the
/// daemon never runs more than 64 (tuning::CONNECTIONS_MAX). Within a budget
/// fewer and smaller, so a slow link answers some of them in a phase.
const TOTAL_PARALLEL: usize = 64;
const TOTAL_SIZE: u64 = 4 * MIB;
const TOTAL_PARALLEL_BUDGET: usize = 32;
const TOTAL_SIZE_BUDGET: u64 = MIB;
/// How long new requests start in a total-rate measurement without a budget;
/// long enough to leave the start of TCP behind, short enough for a slow link.
const TOTAL_TIME: Duration = Duration::from_secs(5);
/// Temporary objects the total write measurement writes in turn.
const WRITE_KEYS: usize = 128;

/// What to measure with.
pub struct Plan {
    /// Objects to read: (key, size); empty — reads are not measured.
    pub reads: Vec<(String, u64)>,
    /// Where temporary objects go; `None` — writes are not measured.
    pub write_dir: Option<String>,
    pub budget: Option<Duration>,
}

pub struct Measured {
    /// `None` where not measured.
    pub chars: Chars,
    pub latency_p90: Option<Duration>,
    pub elapsed: Duration,
    /// Phases not measured (no objects to read, writes not allowed, no
    /// answer within the phase).
    pub skipped: Vec<&'static str>,
}

/// A directory for temporary objects of a measurement of the disk `layout`.
pub fn temp_dir(layout: &Layout) -> String {
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    format!("{}{nanos:x}", layout.measure_prefix())
}

/// Objects worth reading from a listing of the disk: zones stored in one
/// object; on a SEGMENTS disk only zones whose single segment starts at 0.
pub fn candidates_from_listing(
    layout: &Layout,
    objects: &[ObjectInfo],
    segments: bool,
) -> Vec<(String, u64)> {
    use std::collections::BTreeMap;
    let mut zones: BTreeMap<u64, Vec<&ObjectInfo>> = BTreeMap::new();
    for o in objects {
        let zone = if segments {
            layout.parse_segment_key(&o.key).map(|(z, _)| z)
        } else {
            layout.parse_zone_key(&o.key)
        };
        if let Some(z) = zone {
            zones.entry(z).or_default().push(o);
        }
    }
    zones
        .into_iter()
        .filter_map(|(z, objs)| match objs[..] {
            [o] if o.size > 0
                && (!segments || layout.parse_segment_key(&o.key) == Some((z, 0))) =>
            {
                Some((o.key.clone(), o.size))
            }
            _ => None,
        })
        .collect()
}

/// A cheap random number (no cryptographic use): which object and range to read.
fn random() -> u64 {
    use std::hash::BuildHasher;
    std::collections::hash_map::RandomState::new().hash_one(Instant::now())
}

/// A random block-aligned range of `len` bytes (at most the object) of a
/// random object.
fn pick(reads: &[(String, u64)], len: u64) -> Option<(&str, u64, u64)> {
    let (key, size) = reads.get(random() as usize % reads.len().max(1))?;
    let len = len.min(*size);
    let blocks = (size - len) / crate::config::BLOCK_SIZE + 1;
    let offset = random() % blocks * crate::config::BLOCK_SIZE;
    Some((key, offset, len))
}

/// Deletes the temporary objects of a measurement under `dir`.
pub async fn cleanup(s3: &S3Store, dir: &str) -> anyhow::Result<()> {
    let keys: Vec<String> = s3
        .list(&format!("{dir}/"))
        .await?
        .into_iter()
        .map(|o| o.key)
        .collect();
    s3.delete_many(&keys).await?;
    Ok(())
}

/// Measures the link; the caller deletes the temporary objects (`cleanup`),
/// also when this fails.
pub async fn run(s3: &S3Store, plan: Plan) -> anyhow::Result<Measured> {
    let started = Instant::now();
    let (chars, latency_p90) = phases(s3, &plan, started).await?;
    let skipped = PHASE_NAMES
        .iter()
        .zip([
            chars.latency.is_none(),
            chars.read_stream.is_none(),
            chars.read_total.is_none(),
            chars.write_stream.is_none(),
            chars.write_total.is_none(),
        ])
        .filter_map(|(name, none)| none.then_some(*name))
        .collect();
    Ok(Measured {
        chars,
        latency_p90,
        elapsed: started.elapsed(),
        skipped,
    })
}

/// Waits for `f` until `deadline`; `None` past it (the request is dropped).
async fn by<T>(deadline: Option<Instant>, f: impl Future<Output = T>) -> Option<T> {
    match deadline {
        Some(d) => tokio::time::timeout_at(d.into(), f).await.ok(),
        None => Some(f.await),
    }
}

async fn phases(
    s3: &S3Store,
    plan: &Plan,
    started: Instant,
) -> anyhow::Result<(Chars, Option<Duration>)> {
    let budgeted = plan.budget.is_some();
    // Deadline of phase i: the sum of the shares up to it.
    let deadline = |i: usize| {
        plan.budget
            .map(|_| started + PHASES[..=i].iter().sum::<Duration>())
    };
    let stream_size = if budgeted {
        STREAM_SIZE_BUDGET
    } else {
        STREAM_SIZE
    };
    let (total_parallel, total_size) = if budgeted {
        (TOTAL_PARALLEL_BUDGET, TOTAL_SIZE_BUDGET)
    } else {
        (TOTAL_PARALLEL, TOTAL_SIZE)
    };
    let reads = &plan.reads;
    let mut c = Chars::default();
    let mut latency_p90 = None;

    if !reads.is_empty() {
        let mut lat = Vec::new();
        'rounds: for _ in 0..LATENCY_ROUNDS {
            let round = (0..LATENCY_PARALLEL).filter_map(|_| {
                let (key, offset, len) = pick(reads, SMALL_SIZE)?;
                Some(timed(s3.get_range(key, offset, len)))
            });
            let all = futures_util::future::join_all(round);
            match by(deadline(0), all).await {
                Some(times) => lat.extend(times.into_iter().flatten()),
                None => break 'rounds,
            }
        }
        if !lat.is_empty() {
            lat.sort();
            c.latency = Some(lat[lat.len() / 2]);
            latency_p90 = Some(lat[lat.len() * 9 / 10]);
        }

        let large: Vec<(String, u64)> = reads
            .iter()
            .filter(|(_, size)| *size >= stream_size.min(MIB))
            .cloned()
            .collect();
        let mut rates = Vec::new();
        for _ in 0..STREAM_RUNS {
            let Some((key, offset, len)) = pick(&large, stream_size) else {
                break;
            };
            let pair = async {
                let small = timed(s3.get_range(key, offset, SMALL_SIZE)).await?;
                let big = timed(s3.get_range(key, offset, len)).await?;
                StoreResult::Ok(pair_rate(len, small, big))
            };
            match by(deadline(1), pair).await {
                Some(r) => rates.push(r?),
                None => break,
            }
        }
        if !rates.is_empty() {
            c.read_stream = Some(median(&mut rates));
        }

        c.read_total = total_rate(total_parallel, deadline(2), |_| {
            let picked = pick(reads, total_size);
            async move {
                let (key, offset, len) = picked.expect("objects to read");
                s3.get_range(key, offset, len).await.map(|_| len)
            }
        })
        .await?;
    }

    if let Some(dir) = &plan.write_dir {
        let zone = Bytes::from(vec![0x5a; stream_size as usize]);
        let small = zone.slice(..SMALL_SIZE as usize);
        let mut rates = Vec::new();
        for i in 0..STREAM_RUNS {
            let pair = async {
                let t_small =
                    timed(s3.put(&format!("{dir}/small"), Body::new(vec![small.clone()]))).await?;
                let t_big =
                    timed(s3.put(&format!("{dir}/stream-{i}"), Body::new(vec![zone.clone()])))
                        .await?;
                StoreResult::Ok(pair_rate(stream_size, t_small, t_big))
            };
            match by(deadline(3), pair).await {
                Some(r) => rates.push(r?),
                None => break,
            }
        }
        if !rates.is_empty() {
            c.write_stream = Some(median(&mut rates));
        }

        let part = Bytes::from(vec![0x5a; total_size as usize]);
        c.write_total = total_rate(total_parallel, deadline(4), |n| {
            let key = format!("{dir}/w-{}", n % WRITE_KEYS);
            let body = Body::new(vec![part.clone()]);
            async move { s3.put(&key, body).await.map(|_| total_size) }
        })
        .await?;
    }
    Ok((c, latency_p90))
}

async fn timed<T>(f: impl Future<Output = StoreResult<T>>) -> StoreResult<Duration> {
    let started = Instant::now();
    f.await?;
    Ok(started.elapsed())
}

/// Rate after the first byte from a small request and a `big`-byte one made
/// one after the other: the extra bytes over the extra time.
fn pair_rate(big: u64, t_small: Duration, t_big: Duration) -> u64 {
    let t = t_big.saturating_sub(t_small).max(Duration::from_millis(1));
    (big.saturating_sub(SMALL_SIZE) as f64 / t.as_secs_f64()) as u64
}

fn median(v: &mut [u64]) -> u64 {
    v.sort();
    v[v.len() / 2]
}

/// Keeps `parallel` requests in flight, starting new ones for `TOTAL_TIME`
/// (without a deadline) or until the deadline; bytes per second of the
/// answers. Requests still running at the deadline are dropped. `None`: no
/// answer at all.
async fn total_rate<F, Fut>(
    parallel: usize,
    deadline: Option<Instant>,
    request: F,
) -> StoreResult<Option<u64>>
where
    F: Fn(usize) -> Fut,
    Fut: Future<Output = StoreResult<u64>>,
{
    let started = Instant::now();
    let start_until = deadline.unwrap_or(started + TOTAL_TIME);
    let mut running = FuturesUnordered::new();
    let mut n = 0;
    let mut bytes = 0;
    loop {
        while running.len() < parallel && Instant::now() < start_until {
            running.push(request(n));
            n += 1;
        }
        match by(deadline, running.next()).await {
            Some(Some(r)) => bytes += r?,
            Some(None) | None => break,
        }
    }
    let secs = started.elapsed().as_secs_f64();
    Ok((bytes > 0).then(|| (bytes as f64 / secs) as u64))
}

/// A rate as `parse_size` reads it back: whole MiB, or KiB below 10 MiB.
pub fn rate_flag(bytes: u64) -> String {
    if bytes >= 10 * MIB {
        format!("{}M", bytes / MIB)
    } else {
        format!("{}K", (bytes / KIB).max(1))
    }
}

/// Report for the admin: what was measured, what the daemon would derive,
/// and a `--link-config` file with the measured characteristics.
pub fn report(m: &Measured) -> String {
    let c = &m.chars;
    let zone = DiskConfig::DEFAULT_ZONE_SIZE;
    let derived = crate::tuning::resolve(
        &Manual {
            chars: *c,
            ..Manual::default()
        },
        &Auto {
            chars: Chars::default(),
            uploads: 1,
            reads: 1,
        },
        zone,
    )
    .derived;
    let rate = |r: Option<u64>| r.map_or("not measured".into(), |r| format!("{}/s", rate_flag(r)));
    let skipped = if m.skipped.is_empty() {
        String::new()
    } else {
        format!("\nNot measured: {}.", m.skipped.join(", "))
    };
    let yaml = crate::linkconf::render(&Manual {
        chars: *c,
        ..Manual::default()
    });
    format!(
        "# S3 link from this host ({:.0} s):\n\
         #   latency (first byte of a small GET): {}, p90 {}\n\
         #   read:  one stream {}, all together {}\n\
         #   write: one stream {}, all together {}\n\
         #   for a {} MiB zone the daemon would derive: {}{}\n\
         # Save as a file for `mount --link-config`:\n{yaml}",
        m.elapsed.as_secs_f64(),
        c.latency
            .map_or("not measured".into(), |l| format!("{} ms", l.as_millis())),
        m.latency_p90
            .map_or("–".into(), |l| format!("{} ms", l.as_millis())),
        rate(c.read_stream),
        rate(c.read_total),
        rate(c.write_stream),
        rate(c.write_total),
        zone / MIB,
        derived,
        skipped.replace('\n', "\n# "),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::parse_size;

    #[tokio::test]
    async fn no_candidates_skips_reads() {
        let creds: crate::config::Creds = serde_json::from_str(
            r#"{"endpoint":"http://127.0.0.1:9","bucket":"b","access_key":"a","secret_key":"s","region":"r","allow_http":true}"#,
        )
        .unwrap();
        let transport = crate::s3::Transport { proxy: None };
        let s3 = S3Store::new(&creds, &transport, Default::default()).unwrap();
        let m = run(
            &s3,
            Plan {
                reads: Vec::new(),
                write_dir: None,
                budget: Some(BUDGET),
            },
        )
        .await
        .unwrap();
        assert_eq!(m.chars, Chars::default());
        assert_eq!(
            m.skipped,
            [
                "latency",
                "read_stream",
                "read_total",
                "write_stream",
                "write_total"
            ]
        );
    }

    #[test]
    fn rate_flags_parse_back() {
        for (bytes, flag) in [(55 * MIB, "55M"), (3 * MIB + 7, "3072K"), (100, "1K")] {
            assert_eq!(rate_flag(bytes), flag);
            assert!(parse_size(flag).unwrap() <= bytes.max(KIB));
        }
    }

    #[test]
    fn pair_rate_cancels_latency() {
        // 36 ms for 4 KiB, 1036 ms for 16 MiB: (16 MiB − 4 KiB) in one second.
        let r = pair_rate(
            16 * MIB,
            Duration::from_millis(36),
            Duration::from_millis(1036),
        );
        assert_eq!(r, 16 * MIB - SMALL_SIZE);
        assert_eq!(median(&mut [5, 1, 3]), 3);
    }
}

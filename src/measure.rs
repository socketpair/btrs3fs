//! `btrs3fs measure`: characteristics of the link to S3 from this host for the
//! `--s3-*` flags of `mount`/`run` (SPEC.md §6.1). Works with temporary
//! objects under `<prefix>/measure-<random>/` and deletes them.

use crate::config::DiskConfig;
use crate::s3::S3Store;
use crate::store::{Body, StoreResult};
use crate::tuning::Link;
use bytes::Bytes;
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use std::future::Future;
use std::time::{Duration, Instant, SystemTime};

const KIB: u64 = 1 << 10;
const MIB: u64 = 1 << 20;

/// Body of one stream measurement: a whole zone, as uploads are.
const STREAM_SIZE: u64 = 16 * MIB;
/// One-stream measurements, the median is taken. Each is a pair of requests
/// one after the other — a small one and a 16 MiB one — and the rate is taken
/// from the difference, so the latency of that connection cancels out.
const STREAM_RUNS: usize = 5;
/// Small GETs for the latency: rounds of `LATENCY_PARALLEL` at once, so the
/// sample spreads over several connections (single connections were seen to
/// differ: 15 ms on one, 37 ms on another), as the daemon's requests do.
const LATENCY_ROUNDS: usize = 4;
const LATENCY_PARALLEL: usize = 16;
const SMALL_SIZE: u64 = 4 * KIB;
/// Requests of the total-rate measurements, at once; the daemon never runs
/// more (tuning::UPLOADS_MAX).
const TOTAL_PARALLEL: usize = 64;
const TOTAL_SIZE: u64 = 4 * MIB;
/// How long new requests start in a total-rate measurement; long enough to
/// leave the start of TCP behind, short enough for a slow link.
const TOTAL_TIME: Duration = Duration::from_secs(5);
/// Objects the total-rate measurements read and write in turn.
const SEEDS: usize = 8;
const WRITE_KEYS: usize = 128;

pub struct Measured {
    pub link: Link,
    pub latency_p90: Duration,
    pub elapsed: Duration,
    pub dir: String,
}

/// Measures the link; temporary objects go under `prefix` and are deleted
/// even if a measurement fails.
pub async fn measure(s3: &S3Store, prefix: &str) -> anyhow::Result<Measured> {
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)?
        .as_nanos();
    let dir = format!("{}/measure-{nanos:x}", prefix.trim_end_matches('/'));
    let started = Instant::now();
    let result = run(s3, &dir).await;
    let keys: Vec<String> = s3
        .list(&format!("{dir}/"))
        .await?
        .into_iter()
        .map(|o| o.key)
        .collect();
    s3.delete_many(&keys).await?;
    let (link, latency_p90) = result?;
    Ok(Measured {
        link,
        latency_p90,
        elapsed: started.elapsed(),
        dir,
    })
}

async fn run(s3: &S3Store, dir: &str) -> anyhow::Result<(Link, Duration)> {
    let zone = Bytes::from(vec![0x5a; STREAM_SIZE as usize]);
    let small = zone.slice(..SMALL_SIZE as usize);
    let seed = |i: usize| format!("{dir}/seed-{i}");

    // Write stream: the 16 MiB PUTs also create the first seeds.
    let mut write_rates = Vec::with_capacity(STREAM_RUNS);
    for i in 0..STREAM_RUNS {
        let t_small =
            timed(s3.put(&format!("{dir}/small"), Body::new(vec![small.clone()]))).await?;
        let t_big = timed(s3.put(&seed(i), Body::new(vec![zone.clone()]))).await?;
        write_rates.push(pair_rate(t_small, t_big));
    }
    let write_stream = median(&mut write_rates);

    let mut lat = Vec::with_capacity(LATENCY_ROUNDS * LATENCY_PARALLEL);
    for round in 0..LATENCY_ROUNDS {
        let keys: Vec<String> = (0..LATENCY_PARALLEL)
            .map(|i| seed(i % STREAM_RUNS))
            .collect();
        let futs: Vec<_> = keys
            .iter()
            .enumerate()
            .map(|(i, k)| {
                let offset =
                    ((round * LATENCY_PARALLEL + i) as u64 * 7919 * SMALL_SIZE) % STREAM_SIZE;
                timed(s3.get_range(k, offset, SMALL_SIZE))
            })
            .collect();
        for t in futures_util::stream::iter(futs)
            .buffer_unordered(LATENCY_PARALLEL)
            .collect::<Vec<_>>()
            .await
        {
            lat.push(t?);
        }
    }
    lat.sort();
    let latency = lat[lat.len() / 2];
    let latency_p90 = lat[lat.len() * 9 / 10];

    let mut read_rates = Vec::with_capacity(STREAM_RUNS);
    for i in 0..STREAM_RUNS {
        let t_small = timed(s3.get_range(&seed(i), 0, SMALL_SIZE)).await?;
        let t_big = timed(s3.get_range(&seed(i), 0, STREAM_SIZE)).await?;
        read_rates.push(pair_rate(t_small, t_big));
    }
    let read_stream = median(&mut read_rates);

    let rest_keys: Vec<String> = (STREAM_RUNS.min(SEEDS)..SEEDS).map(seed).collect();
    let rest: Vec<_> = rest_keys
        .iter()
        .map(|k| s3.put(k, Body::new(vec![zone.clone()])))
        .collect();
    for r in futures_util::stream::iter(rest)
        .buffer_unordered(SEEDS)
        .collect::<Vec<_>>()
        .await
    {
        r?;
    }

    let ranges = (STREAM_SIZE / TOTAL_SIZE) as usize;
    let read_total = total_rate(|n| {
        let key = seed(n % SEEDS);
        let offset = ((n / SEEDS) % ranges) as u64 * TOTAL_SIZE;
        async move {
            s3.get_range(&key, offset, TOTAL_SIZE)
                .await
                .map(|_| TOTAL_SIZE)
        }
    })
    .await?;

    let part = zone.slice(..TOTAL_SIZE as usize);
    let write_total = total_rate(|n| {
        let key = format!("{dir}/w-{}", n % WRITE_KEYS);
        let body = Body::new(vec![part.clone()]);
        async move { s3.put(&key, body).await.map(|_| TOTAL_SIZE) }
    })
    .await?;

    let link = Link {
        latency,
        read_stream,
        read_total,
        write_stream,
        write_total,
    };
    Ok((link, latency_p90))
}

async fn timed<T>(f: impl Future<Output = StoreResult<T>>) -> StoreResult<Duration> {
    let started = Instant::now();
    f.await?;
    Ok(started.elapsed())
}

/// Rate after the first byte from a small request and a 16 MiB one made one
/// after the other: the extra bytes over the extra time.
fn pair_rate(t_small: Duration, t_big: Duration) -> u64 {
    let t = t_big.saturating_sub(t_small).max(Duration::from_millis(1));
    ((STREAM_SIZE - SMALL_SIZE) as f64 / t.as_secs_f64()) as u64
}

fn median(v: &mut [u64]) -> u64 {
    v.sort();
    v[v.len() / 2]
}

/// Keeps `TOTAL_PARALLEL` requests in flight for `TOTAL_TIME`, then waits for
/// the rest; bytes per second over the whole run.
async fn total_rate<F, Fut>(request: F) -> StoreResult<u64>
where
    F: Fn(usize) -> Fut,
    Fut: Future<Output = StoreResult<u64>>,
{
    let started = Instant::now();
    let mut running = FuturesUnordered::new();
    let mut n = 0;
    let mut bytes = 0;
    loop {
        while running.len() < TOTAL_PARALLEL && started.elapsed() < TOTAL_TIME {
            running.push(request(n));
            n += 1;
        }
        match running.next().await {
            Some(r) => bytes += r?,
            None => break,
        }
    }
    Ok((bytes as f64 / started.elapsed().as_secs_f64()) as u64)
}

/// A rate as `parse_size` reads it back: whole MiB, or KiB below 10 MiB.
pub fn rate_flag(bytes: u64) -> String {
    if bytes >= 10 * MIB {
        format!("{}M", bytes / MIB)
    } else {
        format!("{}K", (bytes / KIB).max(1))
    }
}

/// Report for the admin: what was measured and the flags to pass.
pub fn report(m: &Measured) -> String {
    let l = &m.link;
    let ms = l.latency.as_millis().max(1);
    let zone = DiskConfig::DEFAULT_ZONE_SIZE;
    format!(
        "S3 link from this host ({:.0} s; temporary objects under {}/ deleted):\n\
         \x20 latency (first byte of a small GET): {ms} ms, p90 {} ms\n\
         \x20 read:  one stream {}/s, all together {}/s\n\
         \x20 write: one stream {}/s, all together {}/s\n\
         For a {} MiB zone the daemon derives: {}\n\
         Pass to mount/run:\n\
         \x20 --s3-latency {ms}ms --s3-read-stream {} --s3-read-total {} --s3-write-stream {} --s3-write-total {}",
        m.elapsed.as_secs_f64(),
        m.dir,
        m.latency_p90.as_millis(),
        rate_flag(l.read_stream),
        rate_flag(l.read_total),
        rate_flag(l.write_stream),
        rate_flag(l.write_total),
        zone / MIB,
        l.tuning(zone),
        rate_flag(l.read_stream),
        rate_flag(l.read_total),
        rate_flag(l.write_stream),
        rate_flag(l.write_total),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::parse_size;

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
        let r = pair_rate(Duration::from_millis(36), Duration::from_millis(1036));
        assert_eq!(r, STREAM_SIZE - SMALL_SIZE);
        assert_eq!(median(&mut [5, 1, 3]), 3);
    }
}

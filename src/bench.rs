//! `btrs3fs s3bench`: how zone objects grown by small appends read, right at
//! the S3 level (no disk, no btrfs). Each object grows the way a disk zone
//! grows under `write(4 KiB) + fsync`: PATCH appends (Yandex), or one new
//! object per append (SEGMENTS) and a server-side concatenation. Reads are
//! compared with an object of the same size written by one PUT. Everything
//! goes under `<prefix>/s3bench-<random>/` and is deleted at the end.

use crate::s3::S3Store;
use crate::store::{Body, ConcatPart};
use bytes::Bytes;
use futures_util::StreamExt;
use std::fmt::{self, Write as _};
use std::future::Future;
use std::time::{Duration, Instant, SystemTime};

pub struct Options {
    /// Object sizes to test, in pieces.
    pub pieces: Vec<u64>,
    /// Bytes per append: one `write(4096) + fsync` of btrfs.
    pub piece: u64,
    /// Reads of each object.
    pub repeat: usize,
    /// GETs at once for reads in parts and for segments.
    pub parallel: usize,
    /// Part size of reads in parts (the daemon's read window).
    pub part: u64,
}

/// Times of one kind of request.
pub struct Summary {
    pub n: usize,
    pub min: Duration,
    pub p50: Duration,
    pub p90: Duration,
    pub max: Duration,
    pub total: Duration,
}

impl Summary {
    pub fn of(times: &[Duration]) -> Self {
        let mut t = times.to_vec();
        t.sort();
        let at = |q: f64| {
            t.get(((q * t.len() as f64).ceil() as usize).clamp(1, t.len().max(1)) - 1)
                .copied()
                .unwrap_or_default()
        };
        Self {
            n: t.len(),
            min: t.first().copied().unwrap_or_default(),
            p50: at(0.5),
            p90: at(0.9),
            max: t.last().copied().unwrap_or_default(),
            total: t.iter().sum(),
        }
    }
}

fn ms(d: Duration) -> String {
    if d >= Duration::from_secs(1) {
        format!("{:.2} s", d.as_secs_f64())
    } else {
        format!("{} ms", d.as_millis())
    }
}

impl fmt::Display for Summary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.n == 1 {
            return write!(f, "1 × {}", ms(self.min));
        }
        write!(
            f,
            "{}: p50 {}, p90 {}, min {}, max {}, total {}",
            self.n,
            ms(self.p50),
            ms(self.p90),
            ms(self.min),
            ms(self.max),
            ms(self.total)
        )
    }
}

/// A segment as a disk names it: `<zone>-<offset>`, here under `seg-<n>/`.
fn segment_key(dir: &str, n: u64, offset: u64) -> String {
    format!("{dir}/seg-{n}/{:016x}-{offset:08x}", 0)
}

async fn timed<T, E>(f: impl Future<Output = Result<T, E>>) -> Result<(T, Duration), E> {
    let started = Instant::now();
    let v = f.await?;
    Ok((v, started.elapsed()))
}

/// Data that differs per piece, so a misplaced append shows.
fn data(len: u64) -> Bytes {
    (0..len)
        .map(|i| (i / 4096 % 251) as u8 ^ (i % 7) as u8)
        .collect::<Vec<u8>>()
        .into()
}

fn body(b: Bytes) -> Body {
    Body::new(vec![b])
}

/// Reads `key` whole with one GET, then in parts; checks the bytes once.
async fn reads(
    s3: &S3Store,
    o: &Options,
    key: &str,
    want: &Bytes,
    out: &mut String,
    what: &str,
) -> anyhow::Result<()> {
    let len = want.len() as u64;
    let mut whole = Vec::new();
    let mut parts = Vec::new();
    for _ in 0..o.repeat {
        let (got, t) = timed(s3.get(key)).await?;
        anyhow::ensure!(got == *want, "{key}: the object read back differs");
        whole.push(t);
        let ranges: Vec<(u64, u64)> = (0..len)
            .step_by(o.part as usize)
            .map(|from| (from, o.part.min(len - from)))
            .collect();
        let (got, t) = timed(async {
            let pieces: Vec<Bytes> = futures_util::stream::iter(ranges)
                .map(|(from, n)| s3.get_range(key, from, n))
                .buffered(o.parallel)
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<Result<_, _>>()?;
            anyhow::Ok(pieces.concat())
        })
        .await?;
        anyhow::ensure!(got == want.as_ref(), "{key}: the parts read back differ");
        parts.push(t);
    }
    let _ = writeln!(out, "    read {what}, one GET:  {}", Summary::of(&whole));
    let _ = writeln!(
        out,
        "    read {what}, GETs of {} KiB by {}: {}",
        o.part >> 10,
        o.parallel,
        Summary::of(&parts)
    );
    Ok(())
}

/// One object size: control (one PUT), PATCH appends, segments.
async fn one_size(
    s3: &S3Store,
    o: &Options,
    dir: &str,
    n: u64,
    out: &mut String,
) -> anyhow::Result<()> {
    let len = n * o.piece;
    let want = data(len);
    let piece = |i: u64| want.slice((i * o.piece) as usize..((i + 1) * o.piece) as usize);
    let _ = writeln!(out, "\n{n} appends of {} B = {} KiB:", o.piece, len >> 10);

    let ctl = format!("{dir}/put-{n}");
    let ((), t) = timed(s3.put(&ctl, body(want.clone()))).await?;
    let _ = writeln!(out, "  one PUT: {}", ms(t));
    reads(s3, o, &ctl, &want, out, "the PUT object").await?;

    let patched = format!("{dir}/patch-{n}");
    let mut times = Vec::new();
    let ((), t) = timed(s3.put(&patched, body(piece(0)))).await?;
    times.push(t);
    let mut patch_ok = true;
    for i in 1..n {
        match timed(s3.patch(&patched, i * o.piece, body(piece(i)))).await {
            Ok(((), t)) => times.push(t),
            Err(e) => {
                let _ = writeln!(out, "  PATCH: not available here ({e})");
                patch_ok = false;
                break;
            }
        }
    }
    if patch_ok {
        let _ = writeln!(out, "  PUT + PATCH appends: {}", Summary::of(&times));
        reads(s3, o, &patched, &want, out, "the patched object").await?;
    }

    let keys: Vec<String> = (0..n).map(|i| segment_key(dir, n, i * o.piece)).collect();
    let mut times = Vec::new();
    for (i, key) in keys.iter().enumerate() {
        let ((), t) = timed(s3.put(key, body(piece(i as u64)))).await?;
        times.push(t);
    }
    let _ = writeln!(out, "  segments, one PUT each: {}", Summary::of(&times));
    let mut reads_seg = Vec::new();
    for _ in 0..o.repeat {
        let (got, t) = timed(async {
            let pieces: Vec<Bytes> = futures_util::stream::iter(keys.iter())
                .map(|k| s3.get(k))
                .buffered(o.parallel)
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<Result<_, _>>()?;
            anyhow::Ok(pieces.concat())
        })
        .await?;
        anyhow::ensure!(got == want.as_ref(), "segments of {n}: read back differs");
        reads_seg.push(t);
    }
    let _ = writeln!(
        out,
        "    read the segments, a GET each by {}: {}",
        o.parallel,
        Summary::of(&reads_seg)
    );
    let merged = format!("{dir}/seg-{n}/merged");
    let parts: Vec<ConcatPart> = keys.iter().map(|k| (k.clone(), 0, o.piece)).collect();
    let concat = async {
        let (pending, t_copy) = timed(s3.begin_concat(&merged, &parts)).await?;
        let ((), t_done) = timed(s3.finish_concat(pending)).await?;
        anyhow::Ok((t_copy, t_done))
    };
    match concat.await {
        Ok((t_copy, t_done)) => {
            let _ = writeln!(
                out,
                "  concatenation on the server: copies {}, completion {}, total {}",
                ms(t_copy),
                ms(t_done),
                ms(t_copy + t_done)
            );
            reads(s3, o, &merged, &want, out, "the concatenated object").await?;
        }
        Err(e) => {
            let _ = writeln!(out, "  concatenation on the server: failed: {e:#}");
        }
    }
    Ok(())
}

/// Runs the benchmark; its objects are deleted whatever happens.
pub async fn run(s3: &S3Store, prefix: &str, o: &Options) -> anyhow::Result<String> {
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let dir = format!("{}/s3bench-{nanos:x}", prefix.trim_matches('/'));
    let mut out = format!(
        "S3 append benchmark under {dir}/ ({} reads of each object)\n",
        o.repeat
    );
    let mut result = Ok(());
    for &n in &o.pieces {
        eprintln!("{n} appends…");
        if let Err(e) = one_size(s3, o, &dir, n, &mut out).await {
            result = Err(e);
            break;
        }
    }
    let keys: Vec<String> = s3
        .list(&format!("{dir}/"))
        .await?
        .into_iter()
        .map(|o| o.key)
        .collect();
    s3.delete_many(&keys).await?;
    result.map(|()| out)
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: fn(u64) -> Duration = Duration::from_millis;

    #[test]
    fn summary_of_times() {
        let s = Summary::of(&[MS(30), MS(10), MS(20), MS(40), MS(100)]);
        assert_eq!(
            (s.n, s.min, s.p50, s.p90, s.max),
            (5, MS(10), MS(30), MS(100), MS(100))
        );
        assert_eq!(s.total, MS(200));
        assert_eq!(Summary::of(&[]).n, 0);
        assert_eq!(format!("{}", Summary::of(&[MS(1500)])), "1 × 1.50 s");
        let text = format!("{}", Summary::of(&[MS(10), MS(20), MS(30)]));
        assert_eq!(
            text,
            "3: p50 20 ms, p90 30 ms, min 10 ms, max 30 ms, total 60 ms"
        );
    }

    #[test]
    fn segment_keys_follow_the_disk_layout() {
        assert_eq!(
            segment_key("p/s3bench-1", 3, 0),
            "p/s3bench-1/seg-3/0000000000000000-00000000"
        );
        assert_eq!(
            segment_key("p/s3bench-1", 3, 8192),
            "p/s3bench-1/seg-3/0000000000000000-00002000"
        );
    }
}

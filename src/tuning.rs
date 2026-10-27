//! Settings derived from the link to S3 (SPEC.md §6.1). The admin measures the
//! link with `btrs3fs measure` and passes it to `mount`/`run`; the read window,
//! the part size of large GETs, the btrfs readahead window and the numbers of
//! parallel uploads and GETs all follow from it.

use std::fmt;
use std::time::Duration;

const KIB: u64 = 1 << 10;
const MIB: u64 = 1 << 20;

/// Bounds of the transfer unit (read window and GET part).
/// Judgement calls: below 128 KiB a window glues hardly any neighbouring
/// small files; above 4 MiB a random small read downloads too much even on a
/// fast link.
const UNIT_MIN: u64 = 128 * KIB;
const UNIT_MAX: u64 = 4 * MIB;

/// Bytes in flight for sequential reads = factor × latency × total read rate.
/// Fitted to the cloud measurement (SPEC.md §12): 64 MiB was the best window
/// there, which is ~4 × 32 ms × 564 MiB/s with `measure`'s latency.
const READAHEAD_FACTOR: f64 = 4.0;
/// btrfs never uses less than 4 MiB (`disk-io.c`); 256 MiB bounds the page
/// cache one sequential reader may fill ahead — a judgement call.
const READAHEAD_MIN: u64 = 4 * MIB;
const READAHEAD_MAX: u64 = 256 * MIB;

/// Bounds of parallel uploads. Judgement calls: fewer than 4 leaves FLUSH of
/// a few zones serial; S3 was measured up to 64 at once (SPEC.md §12).
const UPLOADS_MIN: usize = 4;
const UPLOADS_MAX: usize = 64;

/// Characteristics of the link to S3; rates are bytes per second.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Link {
    /// Time to the first byte of a small GET.
    pub latency: Duration,
    /// One GET after its first byte.
    pub read_stream: u64,
    /// All GETs together.
    pub read_total: u64,
    /// One PUT after its first byte.
    pub write_stream: u64,
    /// All PUTs together.
    pub write_total: u64,
}

impl Link {
    /// Medians of three `btrs3fs measure` runs on a VM in Yandex Cloud next
    /// to the bucket (SPEC.md §12); used when the admin gives none.
    pub const YANDEX_CLOUD: Link = Link {
        latency: Duration::from_millis(32),
        read_stream: 57 * MIB,
        read_total: 564 * MIB,
        write_stream: 36 * MIB,
        write_total: 992 * MIB,
    };

    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.latency.is_zero()
                && self.read_stream > 0
                && self.read_total > 0
                && self.write_stream > 0
                && self.write_total > 0,
            "S3 link characteristics must all be positive"
        );
        Ok(())
    }

    /// What the daemon tunes for this link (SPEC.md §6.1).
    pub fn tuning(&self, zone_size: u64) -> Tuning {
        let latency = self.latency.as_secs_f64();
        // Fetching one unit more costs as much as one more request.
        let unit = pow2_round(latency * self.read_stream as f64)
            .clamp(UNIT_MIN, UNIT_MAX)
            .min(zone_size);
        let fs_readahead = pow2_round(READAHEAD_FACTOR * latency * self.read_total as f64)
            .clamp(READAHEAD_MIN, READAHEAD_MAX);
        // Rate of one upload of a whole zone, its first byte included.
        let one_upload = zone_size as f64 / (latency + zone_size as f64 / self.write_stream as f64);
        let uploads = ((self.write_total as f64 / one_upload).ceil() as usize)
            .clamp(UPLOADS_MIN, UPLOADS_MAX);
        // The same for GETs of one unit (user decision).
        let one_get = unit as f64 / (latency + unit as f64 / self.read_stream as f64);
        let reads = ((self.read_total as f64 / one_get).ceil() as usize).max(1);
        Tuning {
            unit,
            fs_readahead,
            uploads,
            reads,
        }
    }
}

impl fmt::Display for Link {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "latency {} ms, read {} MiB/s per stream / {} MiB/s total, write {} / {} MiB/s",
            self.latency.as_millis(),
            self.read_stream / MIB,
            self.read_total / MIB,
            self.write_stream / MIB,
            self.write_total / MIB
        )
    }
}

/// Settings derived from a `Link`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Tuning {
    /// Read-ahead window of the daemon and the part size large reads are
    /// split into for parallel GETs: one GET of this size costs about two
    /// latencies.
    pub unit: u64,
    /// btrfs readahead window (`bdi/read_ahead_kb`): bytes in flight for a
    /// sequential reader.
    pub fs_readahead: u64,
    /// Zones uploaded at once by FLUSH and the background flusher.
    pub uploads: usize,
    /// GETs at once, every read together.
    pub reads: usize,
}

impl fmt::Display for Tuning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "read window and GET part {} KiB (latency × read stream), \
             btrfs readahead {} MiB ({READAHEAD_FACTOR} × latency × total read), \
             {} parallel uploads (total write / one zone upload), \
             {} parallel GETs (total read / one GET of the window)",
            self.unit / KIB,
            self.fs_readahead / MIB,
            self.uploads,
            self.reads
        )
    }
}

/// Nearest power of two (at least 1).
fn pow2_round(x: f64) -> u64 {
    if x <= 1.0 {
        return 1;
    }
    2f64.powf(x.log2().round()) as u64
}

/// `36ms`, `0.036s`, `1s`: a duration with a unit.
pub fn parse_duration(s: &str) -> anyhow::Result<Duration> {
    let s = s.trim();
    let (num, scale) = if let Some(n) = s.strip_suffix("ms") {
        (n, 1e-3)
    } else if let Some(n) = s.strip_suffix('s') {
        (n, 1.0)
    } else {
        anyhow::bail!("duration {s:?} needs a unit: ms or s");
    };
    let v: f64 = num
        .trim()
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid duration {s:?}"))?;
    anyhow::ensure!(v > 0.0 && v.is_finite(), "duration {s:?} must be positive");
    Duration::try_from_secs_f64(v * scale)
        .map_err(|_| anyhow::anyhow!("duration {s:?} is too large"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const ZONE: u64 = 16 * MIB;

    #[test]
    fn cloud_and_internet() {
        // Matches the best settings measured in the cloud (SPEC.md §12).
        let t = Link::YANDEX_CLOUD.tuning(ZONE);
        assert_eq!((t.unit, t.fs_readahead), (2 * MIB, 64 * MIB));
        assert!((16..=64).contains(&t.uploads), "{t:?}");
        // Alone 2 MiB take 32 ms + 35 ms, ~30 MiB/s: 564 MiB/s want 19 GETs.
        assert_eq!(t.reads, 19);

        // The laptop over the internet.
        let laptop = Link {
            latency: Duration::from_millis(50),
            read_stream: 2 * MIB,
            read_total: 20 * MIB,
            write_stream: 3 * MIB,
            write_total: 36 * MIB,
        };
        let t = laptop.tuning(ZONE);
        assert_eq!((t.unit, t.fs_readahead), (128 * KIB, 4 * MIB));
        assert!((4..=16).contains(&t.uploads), "{t:?}");
        assert!((5..=20).contains(&t.reads), "{t:?}");
    }

    #[test]
    fn bounds() {
        let slow = Link {
            latency: Duration::from_secs(2),
            read_stream: 100 * MIB,
            read_total: 10_000 * MIB,
            write_stream: 1,
            write_total: 1,
        };
        let t = slow.tuning(4 * MIB);
        assert_eq!(t.unit, 4 * MIB); // never above the zone
        assert_eq!(t.fs_readahead, READAHEAD_MAX);
        assert_eq!(t.uploads, UPLOADS_MIN);
        assert_eq!(pow2_round(3.0 * MIB as f64), 4 * MIB);
        assert_eq!(pow2_round(1.4 * MIB as f64), MIB);
    }

    #[test]
    fn durations() {
        assert_eq!(parse_duration("36ms").unwrap(), Duration::from_millis(36));
        assert_eq!(parse_duration("0.5s").unwrap(), Duration::from_millis(500));
        assert!(parse_duration("36").is_err());
        assert!(parse_duration("0ms").is_err());
        assert!(parse_duration("1e30s").is_err());
    }
}

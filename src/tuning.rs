//! Settings derived from the link to S3 (SPEC.md §6.1): the read window and
//! part size of large GETs, the btrfs readahead window and the numbers of
//! parallel uploads and GETs follow from the link characteristics. Each of
//! the nine values is manual or auto; `resolve` picks what applies.

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
/// Upper bound of parallel GETs, as measured for uploads; it also keeps
/// absurd link values from asking for thousands of connections.
const READS_MAX: usize = 64;
/// Bound of any connection count, manual or regulated.
pub const CONNECTIONS_MAX: usize = 64;

/// Characteristics of the link to S3; rates are bytes per second. `None`:
/// not known (no estimate yet, nothing hardcoded; SPEC.md §6.1).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Chars {
    /// Time to the first byte of a small GET.
    pub latency: Option<Duration>,
    /// One GET after its first byte.
    pub read_stream: Option<u64>,
    /// All GETs together.
    pub read_total: Option<u64>,
    /// One PUT after its first byte.
    pub write_stream: Option<u64>,
    /// All PUTs together.
    pub write_total: Option<u64>,
}

impl Chars {
    /// `self` where set, else `other`.
    pub fn or(self, other: Chars) -> Chars {
        Chars {
            latency: self.latency.or(other.latency),
            read_stream: self.read_stream.or(other.read_stream),
            read_total: self.read_total.or(other.read_total),
            write_stream: self.write_stream.or(other.write_stream),
            write_total: self.write_total.or(other.write_total),
        }
    }
}

impl fmt::Display for Chars {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let rate = |r: Option<u64>| r.map_or("?".to_string(), |r| format!("{}", r / MIB));
        write!(
            f,
            "latency {} ms, read {} MiB/s per stream / {} MiB/s total, write {} / {} MiB/s",
            self.latency
                .map_or("?".to_string(), |l| l.as_millis().to_string()),
            rate(self.read_stream),
            rate(self.read_total),
            rate(self.write_stream),
            rate(self.write_total)
        )
    }
}

/// Settings derived from the characteristics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Derived {
    /// Read-ahead window of the daemon and the part size large reads are
    /// split into for parallel GETs: one GET of this size costs about two
    /// latencies.
    pub read_unit: u64,
    /// btrfs readahead window (`bdi/read_ahead_kb`): bytes in flight for a
    /// sequential reader.
    pub fs_readahead: u64,
    /// Zones uploaded at once by FLUSH and the background flusher.
    pub uploads: usize,
    /// GETs at once, every read together.
    pub reads: usize,
}

impl fmt::Display for Derived {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "read window and GET part {} KiB, btrfs readahead {} MiB, \
             {} parallel uploads, {} parallel GETs",
            self.read_unit / KIB,
            self.fs_readahead / MIB,
            self.uploads,
            self.reads
        )
    }
}

/// Values set by hand (`--link-config`, the statistics page); `None`: auto.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Manual {
    pub chars: Chars,
    pub read_unit: Option<u64>,
    pub fs_readahead: Option<u64>,
    pub uploads: Option<usize>,
    pub reads: Option<usize>,
    /// RAM for read-ahead windows; `None`: `config::DEFAULT_READ_CACHE`.
    pub read_cache: Option<u64>,
}

/// What the daemon found itself: estimates and the regulated connections.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Auto {
    pub chars: Chars,
    pub uploads: usize,
    pub reads: usize,
}

/// What applies: manual where set, auto elsewhere.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Effective {
    pub chars: Chars,
    pub derived: Derived,
    /// What auto would give for each derived value (shown next to manual ones).
    pub auto_derived: Derived,
}

/// Manual values where set, auto elsewhere (SPEC.md §6.1). Derived values
/// in auto follow the effective characteristics; connections in auto come
/// from the regulator unless every input of their formula is manual (the
/// regulator's own estimate of the total depends on the connections).
pub fn resolve(manual: &Manual, auto: &Auto, zone_size: u64) -> Effective {
    let c = manual.chars.or(auto.chars);
    let m = &manual.chars;
    let read_unit = match (c.latency, c.read_stream) {
        (Some(l), Some(r)) => unit_for(l, r, zone_size),
        _ => UNIT_MIN.min(zone_size),
    };
    let unit = manual.read_unit.unwrap_or(read_unit);
    let fs_readahead = match (c.latency, c.read_total) {
        (Some(l), Some(r)) => readahead_for(l, r),
        _ => READAHEAD_MIN,
    };
    let uploads = match (m.latency, m.write_stream, m.write_total) {
        (Some(l), Some(s), Some(t)) => uploads_for(l, s, t, zone_size),
        _ => auto.uploads,
    };
    let reads = match (m.latency, m.read_stream, m.read_total) {
        (Some(l), Some(s), Some(t)) => reads_for(l, s, t, unit),
        _ => auto.reads,
    };
    // The auto column shows what the daemon found itself: the regulator.
    let auto_derived = Derived {
        read_unit,
        fs_readahead,
        uploads: auto.uploads,
        reads: auto.reads,
    };
    Effective {
        chars: c,
        derived: Derived {
            read_unit: unit,
            fs_readahead: manual.fs_readahead.unwrap_or(fs_readahead),
            uploads: manual.uploads.unwrap_or(uploads),
            reads: manual.reads.unwrap_or(reads),
        },
        auto_derived,
    }
}

/// Checks manual values against the bounds of SPEC.md §6.1.
pub fn validate(m: &Manual, zone_size: u64) -> anyhow::Result<()> {
    let c = &m.chars;
    anyhow::ensure!(
        c.latency.is_none_or(|l| !l.is_zero()),
        "latency must be positive"
    );
    for (name, v) in [
        ("read_stream", c.read_stream),
        ("read_total", c.read_total),
        ("write_stream", c.write_stream),
        ("write_total", c.write_total),
    ] {
        anyhow::ensure!(v.is_none_or(|v| v > 0), "{name} must be positive");
    }
    if let Some(u) = m.read_unit {
        anyhow::ensure!(
            u.is_power_of_two() && (UNIT_MIN..=UNIT_MAX).contains(&u) && u <= zone_size,
            "read_unit {u} must be a power of two from {UNIT_MIN} to {} bytes",
            UNIT_MAX.min(zone_size)
        );
    }
    if let Some(r) = m.fs_readahead {
        anyhow::ensure!(
            (READAHEAD_MIN..=READAHEAD_MAX).contains(&r),
            "fs_readahead {r} must be from {READAHEAD_MIN} to {READAHEAD_MAX} bytes"
        );
    }
    for (name, v) in [("uploads", m.uploads), ("reads", m.reads)] {
        if let Some(n) = v {
            anyhow::ensure!(
                (1..=CONNECTIONS_MAX).contains(&n),
                "{name} {n} must be from 1 to {CONNECTIONS_MAX}"
            );
        }
    }
    Ok(())
}

/// Fetching one unit more costs as much as one more request.
fn unit_for(latency: Duration, read_stream: u64, zone_size: u64) -> u64 {
    pow2_round(latency.as_secs_f64() * read_stream as f64)
        .clamp(UNIT_MIN, UNIT_MAX)
        .min(zone_size)
}

fn readahead_for(latency: Duration, read_total: u64) -> u64 {
    pow2_round(READAHEAD_FACTOR * latency.as_secs_f64() * read_total as f64)
        .clamp(READAHEAD_MIN, READAHEAD_MAX)
}

/// Total write / rate of one upload of a whole zone, its first byte included.
fn uploads_for(latency: Duration, stream: u64, total: u64, zone_size: u64) -> usize {
    let one = zone_size as f64 / (latency.as_secs_f64() + zone_size as f64 / stream as f64);
    ((total as f64 / one).ceil() as usize).clamp(UPLOADS_MIN, UPLOADS_MAX)
}

/// The same for GETs of one unit (user decision).
fn reads_for(latency: Duration, stream: u64, total: u64, unit: u64) -> usize {
    let one = unit as f64 / (latency.as_secs_f64() + unit as f64 / stream as f64);
    ((total as f64 / one).ceil() as usize).clamp(1, READS_MAX)
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

    /// Measured in the cloud (SPEC.md §12): the best settings there.
    fn cloud() -> Chars {
        Chars {
            latency: Some(Duration::from_millis(32)),
            read_stream: Some(57 * MIB),
            read_total: Some(564 * MIB),
            write_stream: Some(36 * MIB),
            write_total: Some(992 * MIB),
        }
    }

    fn manual(chars: Chars) -> Manual {
        Manual {
            chars,
            ..Manual::default()
        }
    }

    fn regulated(n: usize) -> Auto {
        Auto {
            chars: Chars::default(),
            uploads: n,
            reads: n,
        }
    }

    #[test]
    fn resolve_all_auto_without_data() {
        let e = resolve(&Manual::default(), &regulated(1), ZONE);
        assert_eq!(
            e.derived,
            Derived {
                read_unit: 128 * KIB,
                fs_readahead: 4 * MIB,
                uploads: 1,
                reads: 1
            }
        );
        assert_eq!(e.chars, Chars::default());
    }

    #[test]
    fn resolve_auto_derived_follow_effective_chars() {
        let m = manual(Chars {
            latency: Some(Duration::from_millis(32)),
            ..Chars::default()
        });
        let auto = Auto {
            chars: Chars {
                read_stream: Some(57 * MIB),
                ..Chars::default()
            },
            ..regulated(1)
        };
        let e = resolve(&m, &auto, ZONE);
        assert_eq!(e.derived.read_unit, 2 * MIB);
        assert_eq!(e.chars.latency, Some(Duration::from_millis(32)));
        assert_eq!(e.chars.read_stream, Some(57 * MIB));
        // Total read unknown: the readahead window stays at its minimum.
        assert_eq!(e.derived.fs_readahead, 4 * MIB);
    }

    #[test]
    fn manual_chars_use_formula_for_connections() {
        let e = resolve(&manual(cloud()), &regulated(3), ZONE);
        assert_eq!(
            (e.derived.read_unit, e.derived.fs_readahead),
            (2 * MIB, 64 * MIB)
        );
        assert!((21..=36).contains(&e.derived.uploads), "{e:?}");
        // Alone 2 MiB take 32 ms + 35 ms, ~30 MiB/s: 564 MiB/s want 19 GETs.
        assert_eq!(e.derived.reads, 19);
        // What auto would give: the regulator's.
        assert_eq!((e.auto_derived.uploads, e.auto_derived.reads), (3, 3));

        // The laptop over the internet.
        let laptop = Chars {
            latency: Some(Duration::from_millis(50)),
            read_stream: Some(2 * MIB),
            read_total: Some(20 * MIB),
            write_stream: Some(3 * MIB),
            write_total: Some(36 * MIB),
        };
        let d = resolve(&manual(laptop), &regulated(1), ZONE).derived;
        assert_eq!((d.read_unit, d.fs_readahead), (128 * KIB, 4 * MIB));
        assert!((4..=16).contains(&d.uploads), "{d:?}");
        assert!((5..=20).contains(&d.reads), "{d:?}");
    }

    #[test]
    fn manual_derived_wins() {
        let m = Manual {
            uploads: Some(8),
            ..manual(cloud())
        };
        let e = resolve(&m, &regulated(5), ZONE);
        assert_eq!(e.derived.uploads, 8);
        assert_eq!(e.auto_derived.uploads, 5);
    }

    #[test]
    fn formula_bounds() {
        let slow = Chars {
            latency: Some(Duration::from_secs(2)),
            read_stream: Some(100 * MIB),
            read_total: Some(10_000 * MIB),
            write_stream: Some(1),
            write_total: Some(1),
        };
        let d = resolve(&manual(slow), &regulated(1), 4 * MIB).derived;
        assert_eq!(d.read_unit, 4 * MIB); // never above the zone
        assert_eq!(d.fs_readahead, READAHEAD_MAX);
        assert_eq!(d.uploads, UPLOADS_MIN);
        let absurd = Chars {
            latency: Some(Duration::from_secs(1000)),
            read_total: Some(u64::MAX),
            ..slow
        };
        assert_eq!(
            resolve(&manual(absurd), &regulated(1), 4096).derived.reads,
            READS_MAX
        );
        assert_eq!(pow2_round(3.0 * MIB as f64), 4 * MIB);
        assert_eq!(pow2_round(1.4 * MIB as f64), MIB);
    }

    #[test]
    fn validate_bounds() {
        let ok = |m: Manual| validate(&m, ZONE).is_ok();
        assert!(ok(Manual::default()));
        assert!(ok(manual(cloud())));
        assert!(ok(Manual {
            read_unit: Some(MIB),
            uploads: Some(64),
            ..Manual::default()
        }));
        assert!(!ok(Manual {
            read_unit: Some(3 * MIB),
            ..Manual::default()
        }));
        assert!(!ok(Manual {
            read_unit: Some(8 * MIB),
            ..Manual::default()
        }));
        assert!(!ok(Manual {
            uploads: Some(0),
            ..Manual::default()
        }));
        assert!(!ok(Manual {
            uploads: Some(65),
            ..Manual::default()
        }));
        assert!(!ok(Manual {
            reads: Some(65),
            ..Manual::default()
        }));
        assert!(!ok(Manual {
            fs_readahead: Some(MIB),
            ..Manual::default()
        }));
        let zero_rate = Chars {
            read_stream: Some(0),
            ..Chars::default()
        };
        assert!(!ok(manual(zero_rate)));
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

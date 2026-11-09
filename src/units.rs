//! Units of measure (SPEC.md §9.5): parsing what people write and
//! formatting what they read. Inside the program a size is bytes, a rate
//! bytes per second, a time a `Duration`.
//!
//! Input needs an explicit unit: `16MiB`, `5 MB`, `300Mbit/s`, `40MB/s`,
//! `52ms`. A bare letter (`38M`, `10G`) is an error: it used to mean MiB
//! in one place and MiB/s in another.

use anyhow::{bail, ensure};
use std::time::Duration;

const SIZE_UNITS: &str = "B, KiB, MiB, GiB, TiB (binary) or KB, MB, GB, TB (decimal)";
const RATE_UNITS: &str = "Kbit/s, Mbit/s, Gbit/s (bits) or KiB/s, MiB/s, MB/s, ... (bytes)";

/// Splits `16MiB` / `16 MiB` into the number and the unit.
fn split(s: &str) -> (&str, &str) {
    let s = s.trim();
    let at = s
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(s.len());
    (s[..at].trim(), s[at..].trim())
}

fn number(num: &str, whole: &str, what: &str) -> anyhow::Result<f64> {
    let v: f64 = num
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid {what} {whole:?}"))?;
    ensure!(v.is_finite(), "invalid {what} {whole:?}");
    Ok(v)
}

/// Bytes in one byte unit: `B`, `KB`…`TB`, `KiB`…`TiB`; `k` for kilo too.
fn byte_unit(unit: &str) -> Option<f64> {
    let (prefix, binary) = match unit.strip_suffix("iB") {
        Some(p) => (p, true),
        None => (unit.strip_suffix('B')?, false),
    };
    let step: f64 = if binary { 1024.0 } else { 1000.0 };
    let power = match prefix {
        "" if !binary => 0,
        "K" | "k" => 1,
        "M" => 2,
        "G" => 3,
        "T" => 4,
        _ => return None,
    };
    Some(step.powi(power))
}

/// Bytes per second in one bit unit: `bit`, `Kbit`…`Tbit` (decimal).
fn bit_unit(unit: &str) -> Option<f64> {
    let power = match unit.strip_suffix("bit")? {
        "" => 0,
        "K" | "k" => 1,
        "M" => 2,
        "G" => 3,
        "T" => 4,
        _ => return None,
    };
    Some(1000f64.powi(power) / 8.0)
}

fn whole(v: f64, what: &str, s: &str) -> anyhow::Result<u64> {
    ensure!(v >= 0.0, "{what} must not be negative: {s:?}");
    // 2^64 exactly: `u64::MAX as f64` rounds up to it.
    ensure!(
        v < 18_446_744_073_709_551_616.0,
        "{what} {s:?} is too large"
    );
    Ok(v as u64)
}

/// A size: `16MiB`, `5 MB`, `4096B`; a bare `0` too.
pub fn parse_size(s: &str) -> anyhow::Result<u64> {
    let (num, unit) = split(s);
    let v = number(num, s, "size")?;
    let mult = match unit {
        "" if v == 0.0 => 1.0,
        "" => bail!("size {s:?} needs a unit: {SIZE_UNITS}"),
        u => match byte_unit(u) {
            Some(m) => m,
            None => bail!("unknown size unit in {s:?}: use {SIZE_UNITS}"),
        },
    };
    let bytes = v * mult;
    ensure!(
        bytes.fract() == 0.0,
        "size {s:?} is not a whole number of bytes"
    );
    whole(bytes, "size", s)
}

/// A rate, as bytes per second: `300Mbit/s`, `40MB/s`, `37 MiB/s`.
pub fn parse_rate(s: &str) -> anyhow::Result<u64> {
    let (num, unit) = split(s);
    let v = number(num, s, "rate")?;
    let Some(per) = unit.strip_suffix("/s") else {
        bail!("rate {s:?} needs a unit per second: {RATE_UNITS}");
    };
    let Some(mult) = bit_unit(per.trim()).or_else(|| byte_unit(per.trim())) else {
        bail!("unknown rate unit in {s:?}: use {RATE_UNITS}");
    };
    ensure!(v > 0.0, "rate {s:?} must be positive");
    // Bits rarely make whole bytes; a rate is an estimate anyway.
    Ok(whole((v * mult).round(), "rate", s)?.max(1))
}

/// A duration: `36ms`, `0.036s`, `1 s`.
pub fn parse_duration(s: &str) -> anyhow::Result<Duration> {
    let (num, unit) = split(s);
    let v = number(num, s, "duration")?;
    let scale = match unit {
        "ms" => 1e-3,
        "s" => 1.0,
        _ => bail!("duration {s:?} needs a unit: ms or s"),
    };
    ensure!(v > 0.0, "duration {s:?} must be positive");
    Duration::try_from_secs_f64(v * scale)
        .map_err(|_| anyhow::anyhow!("duration {s:?} is too large"))
}

/// Three significant digits, trailing zeros dropped: `319`, `85.3`, `1.25`.
fn three_digits(v: f64) -> String {
    let decimals = if v >= 100.0 {
        0
    } else if v >= 10.0 {
        1
    } else {
        2
    };
    let text = format!("{v:.decimals$}");
    if text.contains('.') {
        text.trim_end_matches('0').trim_end_matches('.').to_string()
    } else {
        text
    }
}

const BINARY: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];

/// An exact size for files people edit: `16 MiB`, `512 KiB`, `1234 B`, `0`.
pub fn size_exact(bytes: u64) -> String {
    if bytes == 0 {
        return "0".into();
    }
    let mut i = 0;
    let mut v = bytes;
    while i < BINARY.len() - 1 && v.is_multiple_of(1024) {
        v /= 1024;
        i += 1;
    }
    format!("{v} {}", BINARY[i])
}

/// A size to read: `1.5 MiB`, `16 MiB`, `700 B`.
pub fn size(bytes: u64) -> String {
    let (mut v, mut i) = (bytes as f64, 0);
    while v >= 1024.0 && i < BINARY.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    format!("{} {}", three_digits(v), BINARY[i])
}

/// Traffic to or from S3, from bytes per second: `319 Mbit/s`, `1.25 Gbit/s`.
pub fn bitrate(bytes_per_sec: u64) -> String {
    const UNITS: [&str; 5] = ["bit/s", "Kbit/s", "Mbit/s", "Gbit/s", "Tbit/s"];
    let (mut v, mut i) = (bytes_per_sec as f64 * 8.0, 0);
    while v >= 1000.0 && i < UNITS.len() - 1 {
        v /= 1000.0;
        i += 1;
    }
    format!("{} {}", three_digits(v), UNITS[i])
}

/// `51.8 ms`, `2 s`: whole seconds, else milliseconds to one decimal.
pub fn duration(d: Duration) -> String {
    if d.subsec_nanos() == 0 {
        format!("{} s", d.as_secs())
    } else {
        let ms = (d.as_secs_f64() * 1e4).round() / 10.0;
        format!("{ms} ms")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = 1 << 20;

    #[test]
    fn sizes_need_a_unit() {
        assert_eq!(parse_size("16MiB").unwrap(), 16 * MIB);
        assert_eq!(parse_size("16 MiB").unwrap(), 16 * MIB);
        assert_eq!(parse_size("5MB").unwrap(), 5_000_000);
        assert_eq!(parse_size("1.5KiB").unwrap(), 1536);
        assert_eq!(parse_size("4096B").unwrap(), 4096);
        assert_eq!(parse_size("2kB").unwrap(), 2000);
        assert_eq!(parse_size("10GiB").unwrap(), 10 << 30);
        assert_eq!(parse_size("0").unwrap(), 0);
        for bad in [
            "38M", "10G", "128K", "4096", "12X", "1.5B", "-1MiB", "5Mb", "MiB",
        ] {
            assert!(parse_size(bad).is_err(), "{bad}");
        }
        assert!(parse_size("20000000TiB").is_err()); // past 2^64
        let e = format!("{:#}", parse_size("38M").unwrap_err());
        assert!(e.contains("MiB") && e.contains("MB"), "{e}");
    }

    #[test]
    fn rates_need_per_second() {
        assert_eq!(parse_rate("8bit/s").unwrap(), 1);
        assert_eq!(parse_rate("300Mbit/s").unwrap(), 37_500_000);
        assert_eq!(parse_rate("1.2 Gbit/s").unwrap(), 150_000_000);
        assert_eq!(parse_rate("40MB/s").unwrap(), 40_000_000);
        assert_eq!(parse_rate("37MiB/s").unwrap(), 37 * MIB);
        assert_eq!(parse_rate("512 KiB/s").unwrap(), 512 << 10);
        for bad in ["38M", "38MiB", "300Mbit", "300Mb/s", "0Mbit/s", "fast"] {
            assert!(parse_rate(bad).is_err(), "{bad}");
        }
        let e = format!("{:#}", parse_rate("38M").unwrap_err());
        assert!(e.contains("Mbit/s") && e.contains("MiB/s"), "{e}");
    }

    #[test]
    fn durations_need_a_unit() {
        assert_eq!(parse_duration("52ms").unwrap(), Duration::from_millis(52));
        assert_eq!(
            parse_duration("51.8 ms").unwrap(),
            Duration::from_micros(51_800)
        );
        assert_eq!(parse_duration("2s").unwrap(), Duration::from_secs(2));
        for bad in ["52", "0ms", "fast", "5min"] {
            assert!(parse_duration(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn output_names_its_unit() {
        assert_eq!(size_exact(16 * MIB), "16 MiB");
        assert_eq!(size_exact(512 << 10), "512 KiB");
        assert_eq!(size_exact(3 * MIB + 7), "3145735 B");
        assert_eq!(size_exact(0), "0");
        assert_eq!(size(512 * MIB), "512 MiB");
        assert_eq!(size(1536), "1.5 KiB");
        assert_eq!(size(700), "700 B");
        assert_eq!(bitrate(38 * MIB), "319 Mbit/s");
        assert_eq!(bitrate(150_000_000), "1.2 Gbit/s");
        assert_eq!(bitrate(10_662_500), "85.3 Mbit/s");
        assert_eq!(duration(Duration::from_nanos(51_812_345)), "51.8 ms");
        assert_eq!(duration(Duration::from_secs(2)), "2 s");
    }

    #[test]
    fn output_parses_back() {
        for b in [0, 4096, 16 * MIB, 3 * MIB + 7, 10 << 30] {
            assert_eq!(parse_size(&size_exact(b)).unwrap(), b);
        }
        // A rate keeps three digits: within half a percent.
        for r in [1000, 7_429_454, 38 * MIB, 992 * MIB, 5_000_000_000] {
            let back = parse_rate(&bitrate(r)).unwrap() as f64;
            assert!(
                (back / r as f64 - 1.0).abs() < 0.005,
                "{r} -> {}",
                bitrate(r)
            );
        }
        let d = Duration::from_nanos(108_142_468);
        assert_eq!(
            parse_duration(&duration(d)).unwrap(),
            Duration::from_micros(108_100)
        );
    }
}

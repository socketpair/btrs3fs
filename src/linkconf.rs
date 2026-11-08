//! The `--link-config` file (SPEC.md §6.1): YAML with the manual link
//! values; a key that is `null` or absent is auto. Parsed with
//! `serde_norway`, written by hand so the output carries comments.

use crate::tuning::Manual;
use anyhow::Context;
use serde::Deserialize;
use std::path::Path;
use std::time::Duration;

/// A value as written: YAML gives counts as numbers and sizes, rates and
/// durations as strings (`24M`, `38ms`); a bare number is bytes.
#[derive(Deserialize)]
#[serde(untagged)]
enum Value {
    Number(u64),
    Text(String),
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct File {
    latency: Option<Value>,
    read_stream: Option<Value>,
    read_total: Option<Value>,
    write_stream: Option<Value>,
    write_total: Option<Value>,
    read_unit: Option<Value>,
    fs_readahead: Option<Value>,
    uploads: Option<Value>,
    reads: Option<Value>,
    read_cache: Option<Value>,
}

pub fn load(path: &Path) -> anyhow::Result<Manual> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("cannot read {}", path.display()))?;
    parse(&text).with_context(|| format!("link config {}", path.display()))
}

pub fn parse(text: &str) -> anyhow::Result<Manual> {
    // An empty document (or comments only) is null to YAML: all auto.
    let file: Option<File> = serde_norway::from_str(text)?;
    let f = file.unwrap_or_default();
    let size = |key: &str, v: Option<Value>| -> anyhow::Result<Option<u64>> {
        v.map(|v| match v {
            Value::Number(n) => Ok(n),
            Value::Text(t) => crate::config::parse_size(&t),
        })
        .transpose()
        .with_context(|| key.to_string())
    };
    let count = |key: &str, v: Option<Value>| -> anyhow::Result<Option<usize>> {
        v.map(|v| match v {
            Value::Number(n) => usize::try_from(n).map_err(anyhow::Error::from),
            Value::Text(t) => Err(anyhow::anyhow!("{t:?} is not a number")),
        })
        .transpose()
        .with_context(|| key.to_string())
    };
    let latency = f
        .latency
        .map(|v| match v {
            Value::Text(t) => crate::tuning::parse_duration(&t),
            Value::Number(_) => Err(anyhow::anyhow!("a duration needs a unit: ms or s")),
        })
        .transpose()
        .context("latency")?;
    Ok(Manual {
        chars: crate::tuning::Chars {
            latency,
            read_stream: size("read_stream", f.read_stream)?,
            read_total: size("read_total", f.read_total)?,
            write_stream: size("write_stream", f.write_stream)?,
            write_total: size("write_total", f.write_total)?,
        },
        read_unit: size("read_unit", f.read_unit)?,
        fs_readahead: size("fs_readahead", f.fs_readahead)?,
        uploads: count("uploads", f.uploads)?,
        reads: count("reads", f.reads)?,
        read_cache: size("read_cache", f.read_cache)?,
    })
}

/// `24M`, `512K`, or bytes when not a whole number of KiB.
pub fn size_text(bytes: u64) -> String {
    const KIB: u64 = 1 << 10;
    const MIB: u64 = 1 << 20;
    const GIB: u64 = 1 << 30;
    match bytes {
        0 => "0".into(),
        b if b.is_multiple_of(GIB) => format!("{}G", b / GIB),
        b if b.is_multiple_of(MIB) => format!("{}M", b / MIB),
        b if b.is_multiple_of(KIB) => format!("{}K", b / KIB),
        b => b.to_string(),
    }
}

/// Whole seconds, else milliseconds to one decimal: a latency needs no more.
pub fn duration_text(d: Duration) -> String {
    if d.subsec_nanos() == 0 {
        format!("{}s", d.as_secs())
    } else {
        let ms = (d.as_secs_f64() * 1e4).round() / 10.0;
        format!("{ms}ms")
    }
}

/// A rate as an estimate: whole MiB/s from 10 MiB/s, else whole KiB/s.
pub fn rate_text(bytes: u64) -> String {
    const KIB: u64 = 1 << 10;
    const MIB: u64 = 1 << 20;
    if bytes >= 10 * MIB {
        format!("{}M", (bytes + MIB / 2) / MIB)
    } else {
        format!("{}K", ((bytes + KIB / 2) / KIB).max(1))
    }
}

/// The manual values as a `--link-config` file; auto keys are `null`.
pub fn render(m: &Manual) -> String {
    fn or_null<T>(v: Option<T>, f: impl Fn(T) -> String) -> String {
        v.map_or("null".into(), f)
    }
    let c = &m.chars;
    format!(
        "# btrs3fs link settings for `mount --link-config` (SPEC.md §6.1).\n\
         # null = auto: estimated from the daemon's own S3 traffic.\n\
         # Characteristics of the link to S3; rates are per second.\n\
         latency: {}\n\
         read_stream: {}\n\
         read_total: {}\n\
         write_stream: {}\n\
         write_total: {}\n\
         # Derived from the above when null.\n\
         read_unit: {}\n\
         fs_readahead: {}\n\
         uploads: {}\n\
         reads: {}\n\
         # RAM for read-ahead windows (null = 64M, 0 = none).\n\
         read_cache: {}\n",
        or_null(c.latency, duration_text),
        or_null(c.read_stream, rate_text),
        or_null(c.read_total, rate_text),
        or_null(c.write_stream, rate_text),
        or_null(c.write_total, rate_text),
        or_null(m.read_unit, size_text),
        or_null(m.fs_readahead, size_text),
        or_null(m.uploads, |n| n.to_string()),
        or_null(m.reads, |n| n.to_string()),
        or_null(m.read_cache, size_text),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = 1 << 20;

    #[test]
    fn example_parses() {
        let m = parse(include_str!("../examples/link.yaml")).unwrap();
        assert_eq!(m.chars.latency, Some(Duration::from_millis(38)));
        assert_eq!(m.chars.read_stream, Some(33 * MIB));
        assert_eq!(m.chars.read_total, None);
        assert_eq!(m.chars.write_stream, Some(24 * MIB));
        assert_eq!(m.chars.write_total, None);
        assert_eq!(m.uploads, Some(8));
        assert_eq!(m.reads, None);
        assert_eq!(m.read_unit, None);
        assert_eq!(m.read_cache, Some(64 * MIB));
    }

    #[test]
    fn empty_is_auto() {
        assert_eq!(parse("").unwrap(), Manual::default());
        assert_eq!(parse("{}").unwrap(), Manual::default());
        assert_eq!(parse("# only a comment\n").unwrap(), Manual::default());
    }

    #[test]
    fn unknown_key_fails() {
        let e = format!("{:#}", parse("upload: 8").unwrap_err());
        assert!(e.contains("upload"), "{e}");
    }

    #[test]
    fn bad_value_fails() {
        let e = format!("{:#}", parse("latency: fast").unwrap_err());
        assert!(e.contains("latency"), "{e}");
        assert!(parse("uploads: many").is_err());
        assert!(parse("uploads: -1").is_err());
    }

    #[test]
    fn render_roundtrip() {
        let example = parse(include_str!("../examples/link.yaml")).unwrap();
        for m in [example, Manual::default()] {
            assert_eq!(parse(&render(&m)).unwrap(), m, "{}", render(&m));
        }
        // An inexact size survives too.
        let odd = Manual {
            read_cache: Some(3 * MIB + 7),
            ..Manual::default()
        };
        assert_eq!(parse(&render(&odd)).unwrap(), odd);
    }

    #[test]
    fn measured_values_render_round() {
        let m = Manual {
            chars: crate::tuning::Chars {
                latency: Some(Duration::from_nanos(108_142_468)),
                read_stream: Some(7_429_454),
                read_total: Some(34_867_198),
                ..Default::default()
            },
            ..Manual::default()
        };
        let text = render(&m);
        assert!(text.contains("latency: 108.1ms"), "{text}");
        assert!(text.contains("read_stream: 7255K"), "{text}");
        assert!(text.contains("read_total: 33M"), "{text}");
        assert!(parse(&text).is_ok());
    }

    #[test]
    fn load_reports_the_file() {
        let e = format!(
            "{:#}",
            load(Path::new("/nonexistent/link.yaml")).unwrap_err()
        );
        assert!(e.contains("/nonexistent/link.yaml"), "{e}");
    }
}

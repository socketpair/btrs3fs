//! The `--link-config` file (SPEC.md §6.1): YAML with the manual link
//! values; a key that is `null` or absent is auto. Parsed with
//! `serde_norway`, written by hand so the output carries comments.

use crate::tuning::Manual;
use crate::units;
use anyhow::Context;
use serde::Deserialize;
use std::path::Path;

/// A value as written: YAML gives counts as numbers and sizes, rates and
/// durations as strings with a unit (`16 MiB`, `300 Mbit/s`, `38 ms`,
/// SPEC.md §9.5); a bare number is a count, or a zero size.
#[derive(Deserialize)]
#[serde(untagged)]
enum Value {
    Number(u64),
    Text(String),
}

impl Value {
    fn text(self) -> String {
        match self {
            Value::Number(n) => n.to_string(),
            Value::Text(t) => t,
        }
    }
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
    fn each<T>(
        key: &str,
        v: Option<Value>,
        parse: impl Fn(&str) -> anyhow::Result<T>,
    ) -> anyhow::Result<Option<T>> {
        v.map(|v| parse(&v.text()))
            .transpose()
            .with_context(|| key.to_string())
    }
    let size = |key, v| each(key, v, units::parse_size);
    let rate = |key, v| each(key, v, units::parse_rate);
    let count = |key: &str, v: Option<Value>| -> anyhow::Result<Option<usize>> {
        v.map(|v| match v {
            Value::Number(n) => usize::try_from(n).map_err(anyhow::Error::from),
            Value::Text(t) => Err(anyhow::anyhow!("{t:?} is not a number")),
        })
        .transpose()
        .with_context(|| key.to_string())
    };
    let latency = each("latency", f.latency, units::parse_duration)?;
    Ok(Manual {
        chars: crate::tuning::Chars {
            latency,
            read_stream: rate("read_stream", f.read_stream)?,
            read_total: rate("read_total", f.read_total)?,
            write_stream: rate("write_stream", f.write_stream)?,
            write_total: rate("write_total", f.write_total)?,
        },
        read_unit: size("read_unit", f.read_unit)?,
        fs_readahead: size("fs_readahead", f.fs_readahead)?,
        uploads: count("uploads", f.uploads)?,
        reads: count("reads", f.reads)?,
        read_cache: size("read_cache", f.read_cache)?,
    })
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
         # Characteristics of the link to S3; units as in SPEC.md §9.5.\n\
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
         # RAM for read-ahead windows (null = 64 MiB, 0 = none).\n\
         read_cache: {}\n",
        or_null(c.latency, units::duration),
        or_null(c.read_stream, units::bitrate),
        or_null(c.read_total, units::bitrate),
        or_null(c.write_stream, units::bitrate),
        or_null(c.write_total, units::bitrate),
        or_null(m.read_unit, units::size_exact),
        or_null(m.fs_readahead, units::size_exact),
        or_null(m.uploads, |n| n.to_string()),
        or_null(m.reads, |n| n.to_string()),
        or_null(m.read_cache, units::size_exact),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    const MIB: u64 = 1 << 20;

    #[test]
    fn example_parses() {
        let m = parse(include_str!("../examples/link.yaml")).unwrap();
        assert_eq!(m.chars.latency, Some(Duration::from_millis(38)));
        assert_eq!(m.chars.read_stream, Some(34_625_000)); // 277 Mbit/s
        assert_eq!(m.chars.read_total, None);
        assert_eq!(m.chars.write_stream, Some(25_125_000)); // 201 Mbit/s
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
        // The old format without units (SPEC.md §9.5).
        for old in [
            "read_stream: 38M",
            "read_unit: 128K",
            "read_cache: 64M",
            "latency: 38",
        ] {
            let e = format!("{:#}", parse(old).unwrap_err());
            assert!(e.contains("unit"), "{old}: {e}");
        }
        assert!(parse("read_stream: 38MiB").is_err()); // a rate needs /s
        assert_eq!(parse("read_cache: 0").unwrap().read_cache, Some(0));
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
        assert!(text.contains("latency: 108.1 ms"), "{text}");
        assert!(text.contains("read_stream: 59.4 Mbit/s"), "{text}");
        assert!(text.contains("read_total: 279 Mbit/s"), "{text}");
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

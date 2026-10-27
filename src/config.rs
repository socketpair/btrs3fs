//! Disk configuration (`<prefix>/disk.json`) and S3 credentials (`creds.json`).

use anyhow::{Context, bail, ensure};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::path::Path;

// Where every number comes from: a hard external limit (with its source), a
// measurement (with a SPEC.md reference) or a judgement call (said so).

/// Logical and physical block size of the device. Requirement of the task
/// ("the smallest write is 4 KiB, sectors are strictly 4 KiB, never 512");
/// also the btrfs sector size and the page size on x86-64.
pub const BLOCK_SIZE: u64 = 4096;

/// Zones of one disk: every zone takes a few hundred bytes of RAM, and
/// `disk.json` comes from the bucket — a bad one must not make the daemon
/// allocate tens of GiB. 2^20 zones are 16 TiB with 16 MiB zones.
pub const MAX_ZONES: u64 = 1 << 20;

/// Most appends one S3 object may receive. Requirement of the task (taken
/// from the Yandex Object Storage append limit as the user knows it; 5228
/// appends in a row were verified, the real limit was not reached, SPEC.md
/// §11). Every append carries at least one block, so a zone holds at most
/// this many blocks: 10 000 × 4 KiB = 40 960 000 bytes, i.e. at most 32 MiB
/// for a power-of-two zone size.
pub const MAX_APPENDS_PER_OBJECT: u64 = 10_000;

/// Largest zone. One ublk request covers up to a whole zone (SPEC.md §2), and
/// ublk user copy encodes the byte offset inside a request in
/// `UBLK_IO_BUF_BITS` = 25 bits (`linux/ublk_cmd.h`), so a request is at most
/// 32 MiB. Today MAX_APPENDS_PER_OBJECT gives the same bound, but for an
/// unrelated reason; this check keeps the ublk limit if that rule changes.
pub const MAX_ZONE_SIZE: u64 = 1 << libublk::sys::UBLK_IO_BUF_BITS;

/// Deepest queue the ublk driver accepts (`UBLK_MAX_QUEUE_DEPTH`, `linux/ublk_cmd.h`).
const UBLK_MAX_QUEUE_DEPTH: u16 = libublk::sys::UBLK_MAX_QUEUE_DEPTH as u16;

/// Version of the disk.json format; bump on incompatible changes.
pub const CONFIG_VERSION: u32 = 1;

/// Zone size of new disks. Measured (SPEC.md §11): Yandex PATCH to an object
/// grown by appends takes 9–30 s once it passes ~20 MB and 0.2–0.6 s below
/// 16 MB, so the largest power of two under that threshold. User decision
/// 2026-10-24 (instead of the originally chosen 32 MiB).
pub const DEFAULT_ZONE_SIZE: u64 = 16 << 20;

/// Buffered (not yet uploaded) data above which zones are uploaded in the
/// background without waiting for a FLUSH; writes wait at twice this amount
/// (SPEC.md §5). Judgement call proposed at the start and accepted by the
/// user: room for one commit=300 interval of moderate writes while bounding the
/// daemon's RAM; not measured. Per disk, `create --dirty-limit`.
pub const DEFAULT_DIRTY_LIMIT: u64 = 256 << 20;

/// RAM for read-ahead windows (SPEC.md §6): a memory budget, not a property
/// of the link. Judgement call: 64 MiB holds 32 windows of 2 MiB (the cloud
/// window), enough for a cold scan of small files to find its neighbours
/// (SPEC.md §12). Per mount, `--read-cache`.
pub const DEFAULT_READ_CACHE: u64 = 64 << 20;

/// Total time one S3 operation may retry before the request fails with EIO.
/// Judgement call accepted by the user: long enough to ride out a short network
/// or S3 hiccup, short enough that a FLUSH does not hang for minutes.
pub const DEFAULT_RETRY_TIMEOUT_SECS: u64 = 60;

/// First pause between retries; doubles each time (DEFAULT_RETRY_BACKOFF), so a
/// 60 s budget gives about 9 attempts. Judgement call accepted by the user.
pub const DEFAULT_RETRY_INITIAL_DELAY_MS: u64 = 100;

/// Growth of the pause between retries (classic exponential backoff).
pub const DEFAULT_RETRY_BACKOFF: f64 = 2.0;

/// ublk queues (threads serving requests). User decision ("4 by default"); the
/// bottleneck is S3, not CPU, so more rarely helps.
pub const DEFAULT_QUEUES: u16 = 4;

/// Requests in flight per ublk queue. Taken from the libublk/rublk examples;
/// each tag costs only an eventfd and a small task, and 128 × 4 queues is more
/// concurrency than the kernel sends to one disk in practice.
pub const DEFAULT_QUEUE_DEPTH: u16 = 128;

/// How zone objects grow (SPEC.md §5). `create` picks it from what the bucket
/// accepts; disks created before the field existed use PATCH.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum AppendMode {
    /// Yandex Object Storage PATCH with `Content-Range`: one request.
    #[default]
    #[serde(rename = "PATCH", alias = "patch")]
    Patch,
    /// Ceph RGW Append Object, `PUT ?append&position=N`: one request.
    #[serde(rename = "PUT_APPEND")]
    PutAppend,
    /// S3 without append: every upload is a new object `<zone>-<offset>`;
    /// the segments of a zone are concatenated on the server in the
    /// background (SPEC.md §5). Needs server-side copy of small parts.
    #[serde(rename = "SEGMENTS")]
    Segments,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DiskConfig {
    pub version: u32,
    pub disk_size: u64,
    pub zone_size: u64,
    /// Obsolete: the device reports no zone limits (SPEC.md §2). Accepted in
    /// old disk.json files, never written.
    #[serde(default, skip_serializing)]
    pub max_open_zones: Option<u32>,
    /// Obsolete, see `max_open_zones`.
    #[serde(default, skip_serializing)]
    pub max_active_zones: Option<u32>,
    pub dirty_limit_bytes: u64,
    pub retry_timeout_seconds: u64,
    pub retry_initial_delay_ms: u64,
    pub retry_backoff_factor: f64,
    pub nr_queues: u16,
    pub queue_depth: u16,
    #[serde(default)]
    pub append: AppendMode,
}

impl DiskConfig {
    pub const DEFAULT_ZONE_SIZE: u64 = DEFAULT_ZONE_SIZE;

    /// Configuration with default tunables; call `validate()` before use.
    pub fn new(disk_size: u64, zone_size: u64) -> Self {
        Self {
            version: CONFIG_VERSION,
            disk_size,
            zone_size,
            max_open_zones: None,
            max_active_zones: None,
            dirty_limit_bytes: DEFAULT_DIRTY_LIMIT,
            retry_timeout_seconds: DEFAULT_RETRY_TIMEOUT_SECS,
            retry_initial_delay_ms: DEFAULT_RETRY_INITIAL_DELAY_MS,
            retry_backoff_factor: DEFAULT_RETRY_BACKOFF,
            nr_queues: DEFAULT_QUEUES,
            queue_depth: DEFAULT_QUEUE_DEPTH,
            append: AppendMode::Patch,
        }
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            self.version == CONFIG_VERSION,
            "unsupported disk config version {} (expected {CONFIG_VERSION})",
            self.version
        );
        ensure!(
            self.zone_size.is_power_of_two() && self.zone_size >= BLOCK_SIZE,
            "zone_size {} must be a power of two and at least {BLOCK_SIZE}",
            self.zone_size
        );
        ensure!(
            self.zone_size <= MAX_ZONE_SIZE,
            "zone_size {} is larger than {MAX_ZONE_SIZE} (largest ublk request)",
            self.zone_size
        );
        ensure!(
            self.zone_size / BLOCK_SIZE <= MAX_APPENDS_PER_OBJECT,
            "zone_size {} holds more than {MAX_APPENDS_PER_OBJECT} blocks of {BLOCK_SIZE} bytes",
            self.zone_size
        );
        ensure!(
            self.disk_size > 0 && self.disk_size.is_multiple_of(self.zone_size),
            "disk_size {} must be a positive multiple of zone_size {}",
            self.disk_size,
            self.zone_size
        );
        ensure!(
            self.nr_zones() <= MAX_ZONES,
            "too many zones: {} (at most {MAX_ZONES}; larger zones make room)",
            self.nr_zones()
        );
        // A write as large as a zone (the largest request) must fit under
        // twice the limit while the soft limit alone does not start an upload
        // of open zones; a smaller limit hangs such writes (SPEC.md §5).
        ensure!(
            self.dirty_limit_bytes >= self.zone_size,
            "dirty_limit_bytes {} is less than the zone size {}",
            self.dirty_limit_bytes,
            self.zone_size
        );
        ensure!(
            self.retry_backoff_factor.is_finite() && self.retry_backoff_factor >= 1.0,
            "retry_backoff_factor {} must be a number >= 1",
            self.retry_backoff_factor
        );
        ensure!(
            self.retry_initial_delay_ms >= 1,
            "retry_initial_delay_ms must be at least 1"
        );
        ensure!(self.nr_queues >= 1, "nr_queues must be at least 1");
        ensure!(
            (1..=UBLK_MAX_QUEUE_DEPTH).contains(&self.queue_depth),
            "queue_depth {} must be in 1..={UBLK_MAX_QUEUE_DEPTH}",
            self.queue_depth
        );
        Ok(())
    }

    pub fn nr_zones(&self) -> u64 {
        self.disk_size / self.zone_size
    }

    pub fn from_json(data: &[u8]) -> anyhow::Result<Self> {
        let cfg: Self = serde_json::from_slice(data).context("cannot parse disk config")?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn to_json(&self) -> Vec<u8> {
        let mut out = serde_json::to_vec_pretty(self).expect("disk config is serializable");
        out.push(b'\n');
        out
    }
}

/// S3 access parameters. Secrets are never printed.
#[derive(Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct Creds {
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    /// A plain `http://` endpoint must be allowed explicitly: without TLS
    /// anyone on the path reads the data and can alter the disk image.
    #[serde(default)]
    pub allow_http: bool,
}

impl Creds {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let data = std::fs::read(path)
            .with_context(|| format!("cannot read credentials file {}", path.display()))?;
        let creds: Self = serde_json::from_slice(&data)
            .with_context(|| format!("cannot parse credentials file {}", path.display()))?;
        creds.check_endpoint()?;
        if let Ok(meta) = std::fs::metadata(path) {
            use std::os::unix::fs::PermissionsExt;
            if meta.permissions().mode() & 0o077 != 0 {
                log::warn!(
                    "credentials file {} is readable by other users; chmod 600 it",
                    path.display()
                );
            }
        }
        Ok(creds)
    }

    /// The endpoint is a bare `https://host[:port]` (or `http://` if allowed).
    fn check_endpoint(&self) -> anyhow::Result<()> {
        let url = reqwest::Url::parse(self.endpoint.trim_end_matches('/'))
            .with_context(|| format!("invalid endpoint {:?}", self.endpoint))?;
        match url.scheme() {
            "https" => {}
            "http" if self.allow_http => {}
            "http" => bail!(
                "endpoint {} is plain http: anyone on the way can read and alter the disk; \
                 use https or set \"allow_http\": true in the credentials file",
                self.endpoint
            ),
            other => bail!("endpoint scheme must be https, not {other}"),
        }
        ensure!(
            url.host_str().is_some_and(|h| !h.is_empty())
                && url.path() == "/"
                && url.query().is_none()
                && url.username().is_empty(),
            "endpoint must be just https://host[:port], got {:?}",
            self.endpoint
        );
        Ok(())
    }
}

impl fmt::Debug for Creds {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Creds")
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("bucket", &self.bucket)
            .field("access_key", &"<hidden>")
            .field("secret_key", &"<hidden>")
            .finish()
    }
}

/// Parses sizes like `10G`, `32M`, `4096`, `1.5T` (binary units).
pub fn parse_size(s: &str) -> anyhow::Result<u64> {
    let s = s.trim();
    let (num, mult) = match s.char_indices().find(|(_, c)| c.is_ascii_alphabetic()) {
        None => (s, 1u64),
        Some((i, _)) => {
            let unit = s[i..].to_ascii_uppercase();
            let mult = match unit.trim_end_matches("IB").trim_end_matches('B') {
                "" => 1,
                "K" => 1 << 10,
                "M" => 1 << 20,
                "G" => 1 << 30,
                "T" => 1 << 40,
                _ => bail!("unknown size unit in {s:?}"),
            };
            (&s[..i], mult)
        }
    };
    let value: f64 = num
        .trim()
        .parse()
        .with_context(|| format!("invalid size {s:?}"))?;
    ensure!(value >= 0.0, "size must not be negative: {s:?}");
    let bytes = value * mult as f64;
    ensure!(
        bytes.fract() == 0.0,
        "size {s:?} is not a whole number of bytes"
    );
    // 2^64 exactly: `u64::MAX as f64` rounds up to it.
    ensure!(
        bytes < 18_446_744_073_709_551_616.0,
        "size {s:?} is too large"
    );
    Ok(bytes as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limits_and_retries_are_bounded() {
        let zone = 16 << 20;
        let ok = DiskConfig::new(1 << 30, zone);
        assert!(ok.validate().is_ok());
        let with = |f: &dyn Fn(&mut DiskConfig)| {
            let mut c = ok.clone();
            f(&mut c);
            c.validate()
        };
        assert!(with(&|c| c.dirty_limit_bytes = zone - 4096).is_err()); // a zone-sized write would hang
        assert!(with(&|c| c.retry_backoff_factor = f64::INFINITY).is_err());
        assert!(with(&|c| c.retry_backoff_factor = f64::NAN).is_err());
        assert!(with(&|c| c.retry_initial_delay_ms = 0).is_err());
    }

    #[test]
    fn zone_count_is_bounded() {
        let zone = 4 << 20;
        assert!(DiskConfig::new(MAX_ZONES * zone, zone).validate().is_ok());
        assert!(
            DiskConfig::new((MAX_ZONES + 1) * zone, zone)
                .validate()
                .is_err()
        );
    }

    #[test]
    fn endpoints() {
        let check = |endpoint: &str, allow_http: bool| {
            Creds {
                endpoint: endpoint.into(),
                region: "r".into(),
                bucket: "b".into(),
                access_key: "a".into(),
                secret_key: "s".into(),
                allow_http,
            }
            .check_endpoint()
        };
        assert!(check("https://storage.yandexcloud.net", false).is_ok());
        assert!(check("https://s3.example:8443/", false).is_ok());
        assert!(check("http://minio:9000", false).is_err()); // must be allowed
        assert!(check("http://minio:9000", true).is_ok());
        for bad in [
            "storage.yandexcloud.net",
            "ftp://host",
            "https://",
            "https://host/bucket",
            "https://user:pw@host",
            "https://ho st",
        ] {
            assert!(check(bad, true).is_err(), "{bad}");
        }
    }

    #[test]
    fn default_config_is_valid() {
        let cfg = DiskConfig::new(10 << 30, DiskConfig::DEFAULT_ZONE_SIZE);
        cfg.validate().unwrap();
        assert_eq!(cfg.nr_zones(), 640);
    }

    #[test]
    fn rejects_bad_geometry() {
        assert!(DiskConfig::new(10 << 30, 3 << 20).validate().is_err()); // not a power of two
        assert!(DiskConfig::new(10 << 30, 64 << 20).validate().is_err()); // > 10000 blocks
        assert!(
            DiskConfig::new((10 << 30) + 4096, 32 << 20)
                .validate()
                .is_err()
        ); // not a multiple
    }

    #[test]
    fn old_config_with_zone_limits() {
        let mut json: serde_json::Value =
            serde_json::from_slice(&DiskConfig::new(1 << 30, 16 << 20).to_json()).unwrap();
        assert!(json.get("max_open_zones").is_none()); // not written any more
        assert!(json.get("max_active_zones").is_none());
        json["max_open_zones"] = 1024.into();
        json["max_active_zones"] = 1024.into();
        DiskConfig::from_json(json.to_string().as_bytes()).unwrap(); // still accepted
    }

    #[test]
    fn append_mode() {
        let mut cfg = DiskConfig::new(1 << 30, 16 << 20);
        cfg.append = AppendMode::Segments;
        let mut json: serde_json::Value = serde_json::from_slice(&cfg.to_json()).unwrap();
        assert_eq!(json["append"], "SEGMENTS");
        json["append"] = "patch".into(); // written so before the names were fixed
        let old = DiskConfig::from_json(json.to_string().as_bytes()).unwrap();
        assert_eq!(old.append, AppendMode::Patch);
        json["append"] = "MULTIPART".into(); // removed mode
        assert!(DiskConfig::from_json(json.to_string().as_bytes()).is_err());
        json.as_object_mut().unwrap().remove("append"); // disks created before the field
        let old = DiskConfig::from_json(json.to_string().as_bytes()).unwrap();
        assert_eq!(old.append, AppendMode::Patch);
    }

    #[test]
    fn json_roundtrip() {
        let cfg = DiskConfig::new(1 << 30, 32 << 20);
        assert_eq!(DiskConfig::from_json(&cfg.to_json()).unwrap(), cfg);
        assert!(DiskConfig::from_json(br#"{"version":1}"#).is_err());
    }

    #[test]
    fn sizes() {
        assert_eq!(parse_size("4096").unwrap(), 4096);
        assert_eq!(parse_size("32M").unwrap(), 32 << 20);
        assert_eq!(parse_size("10GiB").unwrap(), 10 << 30);
        assert_eq!(parse_size("1.5k").unwrap(), 1536);
        assert!(parse_size("12X").is_err());
        assert!(parse_size("20000000T").is_err()); // past 2^64
        assert!(parse_size("99999999999999999999").is_err());
    }
}

//! Disk configuration (`<prefix>/disk.json`) and S3 credentials (`creds.json`).

use anyhow::{Context, bail, ensure};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::path::Path;

/// Logical and physical block size of the device. Fixed, never 512.
pub const BLOCK_SIZE: u64 = 4096;

/// Limit of appends into one S3 object; every append is at least one block,
/// so a zone may hold at most this many blocks.
pub const MAX_APPENDS_PER_OBJECT: u64 = 10_000;

/// Largest zone: one ublk request covers up to a zone, and ublk user copy
/// addresses at most 32 MiB per request.
pub const MAX_ZONE_SIZE: u64 = 32 << 20;

/// Smallest usable active zone limit. btrfs accepts 8 (`BTRFS_MIN_ACTIVE_ZONES`),
/// but with DUP metadata it keeps 6 active zones reserved for metadata and
/// system block groups; at 8 it deadlocked waiting for data space (SPEC.md §2).
const MIN_ACTIVE_ZONES: u32 = 16;

/// Maximum queue depth accepted by the ublk driver.
const UBLK_MAX_QUEUE_DEPTH: u16 = 4096;

pub const CONFIG_VERSION: u32 = 1;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DiskConfig {
    pub version: u32,
    pub disk_size: u64,
    pub zone_size: u64,
    /// Obsolete: open zones are never limited (SPEC.md §2). Accepted in old
    /// disk.json files, never written.
    #[serde(default, skip_serializing)]
    pub max_open_zones: Option<u32>,
    pub max_active_zones: u32,
    pub dirty_limit_bytes: u64,
    pub retry_timeout_seconds: u64,
    pub retry_initial_delay_ms: u64,
    pub retry_backoff_factor: f64,
    pub nr_queues: u16,
    pub queue_depth: u16,
}

impl DiskConfig {
    pub const DEFAULT_ZONE_SIZE: u64 = 16 << 20;

    /// Configuration with default tunables; call `validate()` before use.
    pub fn new(disk_size: u64, zone_size: u64) -> Self {
        Self {
            version: CONFIG_VERSION,
            disk_size,
            zone_size,
            max_open_zones: None,
            max_active_zones: 1024,
            dirty_limit_bytes: 256 << 20,
            retry_timeout_seconds: 60,
            retry_initial_delay_ms: 100,
            retry_backoff_factor: 2.0,
            nr_queues: 4,
            queue_depth: 128,
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
            self.nr_zones() <= u32::MAX as u64,
            "too many zones: {}",
            self.nr_zones()
        );
        let active = self.effective_limit(self.max_active_zones);
        ensure!(
            active == 0 || active >= MIN_ACTIVE_ZONES,
            "max_active_zones {active} is below the btrfs minimum of {MIN_ACTIVE_ZONES}"
        );
        ensure!(
            self.dirty_limit_bytes >= BLOCK_SIZE,
            "dirty_limit_bytes {} is less than one block",
            self.dirty_limit_bytes
        );
        ensure!(
            self.retry_backoff_factor >= 1.0,
            "retry_backoff_factor {} must be >= 1.0",
            self.retry_backoff_factor
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

    /// Zone limit as reported to the kernel: 0 means "no limit".
    pub fn effective_limit(&self, limit: u32) -> u32 {
        if limit as u64 >= self.nr_zones() {
            0
        } else {
            limit
        }
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
}

impl Creds {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let data = std::fs::read(path)
            .with_context(|| format!("cannot read credentials file {}", path.display()))?;
        let creds: Self = serde_json::from_slice(&data)
            .with_context(|| format!("cannot parse credentials file {}", path.display()))?;
        if !creds.endpoint.starts_with("https://") && !creds.endpoint.starts_with("http://") {
            bail!("endpoint must start with https:// or http://");
        }
        Ok(creds)
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
    Ok(bytes as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        let cfg = DiskConfig::new(10 << 30, DiskConfig::DEFAULT_ZONE_SIZE);
        cfg.validate().unwrap();
        assert_eq!(cfg.nr_zones(), 640);
        assert_eq!(cfg.effective_limit(1024), 0);
        assert_eq!(cfg.effective_limit(16), 16);
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
    fn zone_limits() {
        let mut cfg = DiskConfig::new(1 << 30, 16 << 20); // 64 zones
        cfg.max_active_zones = 16;
        cfg.validate().unwrap();
        cfg.max_active_zones = 8;
        assert!(cfg.validate().is_err()); // btrfs deadlocks this low
        cfg.max_active_zones = 1024; // >= zone count: no limit
        cfg.validate().unwrap();
    }

    #[test]
    fn old_config_with_open_limit() {
        let mut json: serde_json::Value =
            serde_json::from_slice(&DiskConfig::new(1 << 30, 16 << 20).to_json()).unwrap();
        assert!(json.get("max_open_zones").is_none()); // not written any more
        json["max_open_zones"] = 1024.into();
        DiskConfig::from_json(json.to_string().as_bytes()).unwrap(); // still accepted
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
    }
}

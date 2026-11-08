//! Automatic removal of empty block groups (SPEC.md §9.3): btrfs deletes a
//! block group by itself only once its whole zone is written and freed, so a
//! zone written in part and then freed keeps its objects in the bucket until
//! `balance -dusage=0` runs. The daemon watches the filesystem's own
//! accounting in sysfs and runs that balance when an emptied group holds a
//! zone that is not empty in S3; a button pauses and resumes it.

use crate::btrfs::Btrfs;
use crate::zones::Engine;
use log::{debug, info, warn};
use serde::Serialize;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// How often the sysfs figures are read: two small files per type.
const POLL: Duration = Duration::from_secs(5);
/// Least time between two looks for empty groups, and so between two
/// automatic balances (user decision): a balance costs a transaction commit
/// per group, and space freed meanwhile is caught by the next look.
const COOLDOWN: Duration = Duration::from_secs(60);

/// What btrfs counts over data, metadata and system block groups.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Totals {
    /// Live bytes (`bytes_used`).
    pub used: u64,
    /// Written and freed, not writable again before a reset (`bytes_zone_unusable`).
    pub unusable: u64,
}

/// The sums from `/sys/fs/btrfs/<fsid>/allocation/*`.
fn read_totals(sysfs: &Path) -> Option<Totals> {
    let mut t = Totals::default();
    for kind in ["data", "metadata", "system"] {
        let d = sysfs.join("allocation").join(kind);
        let v = |name: &str| -> Option<u64> {
            std::fs::read_to_string(d.join(name))
                .ok()?
                .trim()
                .parse()
                .ok()
        };
        t.used += v("bytes_used")?;
        t.unusable += v("bytes_zone_unusable").unwrap_or(0);
    }
    Some(t)
}

/// Whether at least a zone's worth was freed since `base`: less live data,
/// or more garbage in zones.
fn due(base: Totals, now: Totals, zone: u64) -> bool {
    base.used.saturating_sub(now.used) >= zone || now.unusable.saturating_sub(base.unusable) >= zone
}

/// How many of the empty block groups (by device offset) lie in zones that
/// hold something in S3.
fn worth(empty: &[u64], zone: u64, written: impl Fn(usize) -> bool) -> usize {
    empty
        .iter()
        .filter(|&&offset| written((offset / zone) as usize))
        .count()
}

#[derive(Serialize, Clone, Debug, Default)]
pub struct AutoInfo {
    pub paused: bool,
    /// Seconds since the last automatic balance and how many groups it found.
    pub last_run_secs_ago: Option<f64>,
    pub last_groups: Option<usize>,
}

#[derive(Default)]
pub struct AutoBalance {
    paused: AtomicBool,
    last_run: Mutex<Option<(Instant, usize)>>,
    last_check: Mutex<Option<Instant>>,
}

impl AutoBalance {
    pub fn set_paused(&self, paused: bool) {
        self.paused.store(paused, Ordering::SeqCst);
        info!(
            "automatic removal of empty block groups {}",
            if paused { "paused" } else { "resumed" }
        );
    }

    pub fn info(&self) -> AutoInfo {
        let last = *self.last_run.lock().unwrap();
        AutoInfo {
            paused: self.paused.load(Ordering::SeqCst),
            last_run_secs_ago: last.map(|(t, _)| t.elapsed().as_secs_f64()),
            last_groups: last.map(|(_, n)| n),
        }
    }

    /// Watches the filesystem on `device` (`ublkbN`) until the process ends.
    pub fn run(self: Arc<Self>, device: String, fs: Arc<Btrfs>, engine: Arc<Engine>, zone: u64) {
        let mut base: Option<Totals> = None;
        loop {
            std::thread::sleep(POLL);
            let Some(sysfs) = crate::btrfs::sysfs_dirs(&device).into_iter().next() else {
                base = None; // not mounted
                continue;
            };
            let Some(now) = read_totals(&sysfs) else {
                continue;
            };
            let start = *base.get_or_insert(now);
            if self.paused.load(Ordering::SeqCst) || !due(start, now, zone) {
                continue;
            }
            let recent = self
                .last_check
                .lock()
                .unwrap()
                .is_some_and(|t| t.elapsed() < COOLDOWN);
            if recent {
                continue; // the freed space waits for the next look
            }
            *self.last_check.lock().unwrap() = Some(Instant::now());
            base = Some(now);
            let Some(mountpoint) = fs.mountpoint() else {
                continue;
            };
            let groups = match crate::btrfs_zone::empty_groups(&mountpoint) {
                Ok(Some(empty)) => worth(&empty, zone, |zi| engine.zone_in_use(zi)),
                // No block group tree: whether a group is empty is not known
                // cheaply; balance finds out.
                Ok(None) => 1,
                Err(e) => {
                    warn!("cannot look for empty block groups: {e}");
                    continue;
                }
            };
            if groups == 0 {
                debug!("space was freed, but no empty block group holds a stored zone");
                continue;
            }
            match fs.start("autobalance") {
                Ok(()) => *self.last_run.lock().unwrap() = Some((Instant::now(), groups)),
                Err(e) => debug!("automatic balance not started: {e}"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const Z: u64 = 16 << 20;

    #[test]
    fn checks_after_a_zone_was_freed() {
        let t = |used, unusable| Totals { used, unusable };
        assert!(!due(t(100 * Z, 0), t(100 * Z, 0), Z));
        assert!(
            !due(t(100 * Z, 0), t(100 * Z - Z / 2, Z / 2), Z),
            "half a zone"
        );
        assert!(due(t(100 * Z, 0), t(99 * Z, 0), Z), "a zone less used");
        assert!(due(t(100 * Z, 0), t(100 * Z, Z), Z), "a zone more garbage");
        assert!(!due(t(100 * Z, 0), t(120 * Z, 0), Z), "more data only");
    }

    /// Only empty groups whose zones hold something in S3 are worth a
    /// balance: a fresh empty group costs nothing and would come right back.
    #[test]
    fn worth_counts_groups_on_written_zones() {
        let written = |zi: usize| zi == 3 || zi == 7;
        assert_eq!(worth(&[3 * Z, 5 * Z], Z, written), 1);
        assert_eq!(worth(&[5 * Z, 6 * Z], Z, written), 0);
        assert_eq!(worth(&[3 * Z, 7 * Z + 4096], Z, written), 2);
        assert_eq!(worth(&[], Z, written), 0);
    }
}

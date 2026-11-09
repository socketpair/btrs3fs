//! The btrfs on the device, for the statistics page (SPEC.md §9.3): the
//! operation running in it (sysfs, balance progress ioctl) and the actions
//! `balance -dusage=0 -musage=0` and `syncfs()`.
//! No external programs are run.

use log::{info, warn};
use serde::Serialize;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// `struct btrfs_balance_args` (`linux/btrfs.h`, packed, 136 bytes).
#[repr(C, packed)]
#[derive(Clone, Copy, Default)]
struct BalanceArgs {
    profiles: u64,
    /// `usage`, or `usage_min`/`usage_max`.
    usage: u64,
    devid: u64,
    pstart: u64,
    pend: u64,
    vstart: u64,
    vend: u64,
    target: u64,
    flags: u64,
    limit: u64,
    stripes_min: u32,
    stripes_max: u32,
    unused: [u64; 6],
}

/// `struct btrfs_ioctl_balance_args` (`linux/btrfs.h`, 1024 bytes).
#[repr(C)]
struct IoctlBalanceArgs {
    flags: u64,
    state: u64,
    data: BalanceArgs,
    meta: BalanceArgs,
    sys: BalanceArgs,
    /// `struct btrfs_balance_progress`: expected, considered, completed.
    stat: [u64; 3],
    unused: [u64; 72],
}

const _: () = assert!(std::mem::size_of::<BalanceArgs>() == 136);
const _: () = assert!(std::mem::size_of::<IoctlBalanceArgs>() == 1024);

/// `_IOWR(BTRFS_IOCTL_MAGIC, 32, struct btrfs_ioctl_balance_args)`.
const BTRFS_IOC_BALANCE_V2: libc::c_ulong = 0xc400_9420;
/// `_IOR(BTRFS_IOCTL_MAGIC, 34, struct btrfs_ioctl_balance_args)`.
const BTRFS_IOC_BALANCE_PROGRESS: libc::c_ulong = 0x8400_9422;
/// `BTRFS_BALANCE_DATA`: balance data block groups.
const BALANCE_DATA: u64 = 1 << 0;
/// `BTRFS_BALANCE_SYSTEM`: balance system block groups.
const BALANCE_SYSTEM: u64 = 1 << 1;
/// `BTRFS_BALANCE_METADATA`: balance metadata block groups.
const BALANCE_METADATA: u64 = 1 << 2;
/// `BTRFS_BALANCE_ARGS_USAGE`: only block groups used up to `usage` percent.
const BALANCE_ARGS_USAGE: u64 = 1 << 1;
/// `BTRFS_BALANCE_STATE_RUNNING`.
const BALANCE_STATE_RUNNING: u64 = 1 << 0;

impl IoctlBalanceArgs {
    fn zeroed() -> Self {
        // SAFETY: a plain C struct of integers; all-zero is a valid value.
        unsafe { std::mem::zeroed() }
    }
}

#[derive(Serialize, Default)]
pub struct Balance {
    pub running: bool,
    pub expected: u64,
    pub considered: u64,
    pub completed: u64,
}

#[derive(Serialize, Default, Clone)]
pub struct ActionResult {
    pub name: String,
    pub ok: bool,
    pub message: String,
    pub secs: f64,
}

/// `/btrfs.json`: the operation running in the filesystem and the actions
/// of the statistics page (zones tab).
#[derive(Serialize, Default)]
pub struct Info {
    /// The filesystem on the device is mounted (found in sysfs and mountinfo).
    pub mounted: bool,
    pub device: String,
    pub exclusive_operation: String,
    #[serde(rename = "bg_reclaim_threshold_pct")]
    pub bg_reclaim_threshold: Option<u64>,
    #[serde(rename = "global_rsv_size_bytes")]
    pub global_rsv_size: Option<u64>,
    #[serde(rename = "global_rsv_reserved_bytes")]
    pub global_rsv_reserved: Option<u64>,
    pub balance: Balance,
    /// The automatic removal of empty block groups.
    pub auto: crate::autobalance::AutoInfo,
    /// The action in progress and how long it runs, seconds.
    pub running: Option<String>,
    pub running_secs: f64,
    pub last: Option<ActionResult>,
}

/// The btrfs of one device (`ublkbN`) and the actions on it.
pub struct Btrfs {
    device: String,
    action: Mutex<ActionState>,
    auto: Arc<crate::autobalance::AutoBalance>,
}

#[derive(Default)]
struct ActionState {
    running: Option<(String, Instant)>,
    last: Option<ActionResult>,
}

/// The filesystem as found: sysfs directory and mount point.
struct Found {
    sysfs: PathBuf,
    mountpoint: PathBuf,
}

impl Btrfs {
    /// `device`: `ublkbN`.
    pub fn new(device: &str) -> Self {
        Self {
            device: device.to_string(),
            action: Mutex::default(),
            auto: Arc::default(),
        }
    }

    fn find(&self) -> Option<Found> {
        let sysfs = sysfs_dirs(&self.device).into_iter().next()?;
        let mountpoint = self.mountpoint()?;
        Some(Found { sysfs, mountpoint })
    }

    /// What btrfs keeps in the zone at device bytes `[start, start + size)`.
    pub fn zone(&self, start: u64, size: u64) -> crate::btrfs_zone::ZoneFs {
        match self.find() {
            Some(fs) => crate::btrfs_zone::zone_contents(&fs.mountpoint, start, size),
            None => Default::default(),
        }
    }

    /// The automatic removal of empty block groups on this filesystem.
    pub fn auto(&self) -> Arc<crate::autobalance::AutoBalance> {
        self.auto.clone()
    }

    /// Where the filesystem on the device is mounted (the top-most mount).
    pub fn mountpoint(&self) -> Option<PathBuf> {
        mount_of(&format!("/dev/{}", self.device))
    }

    pub fn info(&self) -> Info {
        let mut info = Info {
            device: format!("/dev/{}", self.device),
            auto: self.auto.info(),
            ..Default::default()
        };
        {
            let a = self.action.lock().unwrap();
            if let Some((name, started)) = &a.running {
                info.running = Some(name.clone());
                info.running_secs = started.elapsed().as_secs_f64();
            }
            info.last = a.last.clone();
        }
        let Some(fs) = self.find() else {
            return info;
        };
        let s = &fs.sysfs;
        info.mounted = true;
        info.exclusive_operation = read_text(&s.join("exclusive_operation")).unwrap_or_default();
        info.bg_reclaim_threshold = read_u64(&s.join("bg_reclaim_threshold"));
        info.global_rsv_size = read_u64(&s.join("allocation/global_rsv_size"));
        info.global_rsv_reserved = read_u64(&s.join("allocation/global_rsv_reserved"));
        info.balance = balance_progress(&fs.mountpoint);
        info
    }

    /// Starts `balance` or `syncfs` in a thread of its own; refuses while
    /// another action runs or when the filesystem is not mounted.
    pub fn start(self: &Arc<Self>, name: &str) -> Result<(), String> {
        let run: fn(&Path) -> Result<String, String> = match name {
            // The same as the button, started by the daemon (SPEC.md §9.3).
            "balance" | "autobalance" => balance_empty,
            "syncfs" => sync_fs,
            _ => return Err(format!("unknown action {name}")),
        };
        let fs = self.find().ok_or("the filesystem is not mounted")?;
        {
            let mut a = self.action.lock().unwrap();
            if let Some((busy, _)) = &a.running {
                return Err(format!("{busy} is still running"));
            }
            a.running = Some((name.to_string(), Instant::now()));
        }
        let name = name.to_string();
        let this = self.clone();
        let spawned = std::thread::Builder::new()
            .name(format!("btrs3fs-{name}"))
            .spawn(move || {
                let by = if name == "autobalance" {
                    "daemon"
                } else {
                    "statistics page"
                };
                info!("{by}: {name} on {} started", fs.mountpoint.display());
                let started = Instant::now();
                let res = run(&fs.mountpoint);
                let secs = started.elapsed().as_secs_f64();
                match &res {
                    Ok(m) => info!("{name} finished in {secs:.1} s: {m}"),
                    Err(m) => warn!("{name} failed after {secs:.1} s: {m}"),
                }
                let mut a = this.action.lock().unwrap();
                a.running = None;
                a.last = Some(ActionResult {
                    name,
                    ok: res.is_ok(),
                    message: res.unwrap_or_else(|e| e),
                    secs,
                });
            });
        if let Err(e) = spawned {
            self.action.lock().unwrap().running = None;
            return Err(format!("cannot start a thread: {e}"));
        }
        Ok(())
    }
}

/// Mount point of the top-most mount of `source`.
fn mount_of(source: &str) -> Option<PathBuf> {
    crate::mountinfo::mounts()
        .ok()?
        .into_iter()
        .rfind(|m| m.source == source)
        .map(|m| m.point)
}

/// The sysfs directories (`/sys/fs/btrfs/<fsid>`) of the mounted btrfs
/// filesystems on the device `name` (`ublkbN`).
pub fn sysfs_dirs(name: &str) -> Vec<PathBuf> {
    std::fs::read_dir("/sys/fs/btrfs")
        .into_iter()
        .flatten()
        .flatten()
        .map(|e| e.path())
        .filter(|fs| fs.join("devices").join(name).exists())
        .collect()
}

fn open_dir(path: &Path) -> Result<std::fs::File, String> {
    std::fs::File::open(path).map_err(|e| format!("cannot open {}: {e}", path.display()))
}

/// `btrfs balance start -dusage=0 -musage=0`: relocates (that is, deletes)
/// the block groups nothing lives in. As in `btrfs`, system block groups go
/// with metadata under the same filter.
fn balance_empty(mountpoint: &Path) -> Result<String, String> {
    let dir = open_dir(mountpoint)?;
    let mut args = IoctlBalanceArgs::zeroed();
    args.flags = BALANCE_DATA | BALANCE_METADATA | BALANCE_SYSTEM;
    args.data.flags = BALANCE_ARGS_USAGE;
    args.data.usage = 0;
    args.meta = args.data;
    args.sys = args.data;
    // SAFETY: the ioctl reads and writes one btrfs_ioctl_balance_args we own.
    let r = unsafe { libc::ioctl(dir.as_raw_fd(), BTRFS_IOC_BALANCE_V2, &mut args) };
    if r < 0 {
        return Err(format!("balance: {}", std::io::Error::last_os_error()));
    }
    let [_, considered, completed] = args.stat;
    Ok(format!(
        "{completed} empty block groups relocated of {considered} considered"
    ))
}

fn balance_progress(mountpoint: &Path) -> Balance {
    let Ok(dir) = open_dir(mountpoint) else {
        return Balance::default();
    };
    let mut args = IoctlBalanceArgs::zeroed();
    // SAFETY: the ioctl writes one btrfs_ioctl_balance_args we own; ENOTCONN
    // (no balance running) leaves it untouched.
    let r = unsafe { libc::ioctl(dir.as_raw_fd(), BTRFS_IOC_BALANCE_PROGRESS, &mut args) };
    if r < 0 {
        return Balance::default();
    }
    let [expected, considered, completed] = args.stat;
    Balance {
        running: args.state & BALANCE_STATE_RUNNING != 0,
        expected,
        considered,
        completed,
    }
}

/// `syncfs()`: commits the btrfs transaction, so everything written is in S3.
fn sync_fs(mountpoint: &Path) -> Result<String, String> {
    let dir = open_dir(mountpoint)?;
    // SAFETY: plain syscall on a descriptor we own.
    if unsafe { libc::syncfs(dir.as_raw_fd()) } < 0 {
        return Err(format!("syncfs: {}", std::io::Error::last_os_error()));
    }
    Ok("the filesystem is committed and stored".into())
}

fn read_text(path: &Path) -> Option<String> {
    std::fs::read_to_string(path)
        .ok()
        .map(|s| s.trim().to_string())
}

fn read_u64(path: &Path) -> Option<u64> {
    read_text(path)?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn not_mounted() {
        let b = Btrfs::new("ublkb-none");
        let i = b.info();
        assert!(!i.mounted);
        assert_eq!(i.device, "/dev/ublkb-none");
        assert_eq!(b.mountpoint(), None);
    }

    /// Only what the zones tab shows: the operation running and the actions.
    #[test]
    fn info_has_operations_only() {
        let json = serde_json::to_value(Btrfs::new("ublkb-none").info()).unwrap();
        let mut keys: Vec<&str> = json
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort();
        assert_eq!(
            keys,
            [
                "auto",
                "balance",
                "bg_reclaim_threshold_pct",
                "device",
                "exclusive_operation",
                "global_rsv_reserved_bytes",
                "global_rsv_size_bytes",
                "last",
                "mounted",
                "running",
                "running_secs"
            ]
        );
    }
}

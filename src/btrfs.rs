//! The btrfs on the device, for the statistics page (SPEC.md §9.3): figures
//! from sysfs and ioctls, and the actions `balance -dusage=0` and `syncfs()`.
//! No external programs are run.

use log::{info, warn};
use serde::Serialize;
use std::collections::BTreeMap;
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

/// Space of one kind of block groups (`allocation/<kind>/`), bytes.
#[derive(Serialize, Default)]
pub struct Space {
    pub kind: String,
    pub total_bytes: Option<u64>,
    pub bytes_used: Option<u64>,
    pub bytes_zone_unusable: Option<u64>,
    pub bytes_pinned: Option<u64>,
    pub bytes_reserved: Option<u64>,
    pub bytes_may_use: Option<u64>,
    pub bytes_readonly: Option<u64>,
    pub disk_total: Option<u64>,
    pub disk_used: Option<u64>,
    pub bg_reclaim_threshold: Option<u64>,
    pub dynamic_reclaim: Option<u64>,
    pub periodic_reclaim: Option<u64>,
    pub reclaim_count: Option<u64>,
    pub reclaim_bytes: Option<u64>,
    pub reclaim_errors: Option<u64>,
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

/// `/btrfs.json`.
#[derive(Serialize, Default)]
pub struct Info {
    /// The filesystem on the device is mounted (found in sysfs and mountinfo).
    pub mounted: bool,
    pub device: String,
    pub fsid: String,
    pub label: String,
    pub mountpoint: String,
    pub read_only: bool,
    pub generation: Option<u64>,
    pub nodesize: Option<u64>,
    pub sectorsize: Option<u64>,
    pub exclusive_operation: String,
    pub bg_reclaim_threshold: Option<u64>,
    /// `commit_stats`: commits, last/max/total commit time in ms.
    pub commits: BTreeMap<String, u64>,
    pub global_rsv_size: Option<u64>,
    pub global_rsv_reserved: Option<u64>,
    pub spaces: Vec<Space>,
    /// `devinfo/*/error_stats`, summed over devices.
    pub errors: BTreeMap<String, u64>,
    pub balance: Balance,
    /// The action in progress and how long it runs, seconds.
    pub running: Option<String>,
    pub running_secs: f64,
    pub last: Option<ActionResult>,
}

/// The btrfs of one device (`ublkbN`) and the actions on it.
pub struct Btrfs {
    device: String,
    action: Mutex<ActionState>,
}

#[derive(Default)]
struct ActionState {
    running: Option<(String, Instant)>,
    last: Option<ActionResult>,
}

/// The filesystem as found: sysfs directory and mount.
struct Found {
    sysfs: PathBuf,
    mountpoint: PathBuf,
    read_only: bool,
}

impl Btrfs {
    /// `device`: `ublkbN`.
    pub fn new(device: &str) -> Self {
        Self {
            device: device.to_string(),
            action: Mutex::default(),
        }
    }

    fn find(&self) -> Option<Found> {
        let sysfs = std::fs::read_dir("/sys/fs/btrfs")
            .ok()?
            .flatten()
            .map(|e| e.path())
            .find(|fs| fs.join("devices").join(&self.device).exists())?;
        let (mountpoint, read_only) = mount_of(&format!("/dev/{}", self.device))?;
        Some(Found {
            sysfs,
            mountpoint,
            read_only,
        })
    }

    /// What btrfs keeps in the zone at device bytes `[start, start + size)`.
    pub fn zone(&self, start: u64, size: u64) -> crate::btrfs_zone::ZoneFs {
        match self.find() {
            Some(fs) => crate::btrfs_zone::zone_contents(&fs.mountpoint, start, size),
            None => Default::default(),
        }
    }

    /// Whether the filesystem is mounted read-only; `None` when not mounted.
    pub fn read_only(&self) -> Option<bool> {
        mount_of(&format!("/dev/{}", self.device)).map(|(_, ro)| ro)
    }

    pub fn info(&self) -> Info {
        let mut info = Info {
            device: format!("/dev/{}", self.device),
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
        info.fsid = s
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        info.label = read_text(&s.join("label")).unwrap_or_default();
        info.mountpoint = fs.mountpoint.display().to_string();
        info.read_only = fs.read_only;
        info.generation = read_u64(&s.join("generation"));
        info.nodesize = read_u64(&s.join("nodesize"));
        info.sectorsize = read_u64(&s.join("sectorsize"));
        info.exclusive_operation = read_text(&s.join("exclusive_operation")).unwrap_or_default();
        info.bg_reclaim_threshold = read_u64(&s.join("bg_reclaim_threshold"));
        info.commits = read_pairs(&s.join("commit_stats"));
        info.global_rsv_size = read_u64(&s.join("allocation/global_rsv_size"));
        info.global_rsv_reserved = read_u64(&s.join("allocation/global_rsv_reserved"));
        for kind in ["data", "metadata", "system"] {
            let d = s.join("allocation").join(kind);
            if !d.is_dir() {
                continue;
            }
            let v = |name: &str| read_u64(&d.join(name));
            info.spaces.push(Space {
                kind: kind.to_string(),
                total_bytes: v("total_bytes"),
                bytes_used: v("bytes_used"),
                bytes_zone_unusable: v("bytes_zone_unusable"),
                bytes_pinned: v("bytes_pinned"),
                bytes_reserved: v("bytes_reserved"),
                bytes_may_use: v("bytes_may_use"),
                bytes_readonly: v("bytes_readonly"),
                disk_total: v("disk_total"),
                disk_used: v("disk_used"),
                bg_reclaim_threshold: v("bg_reclaim_threshold"),
                dynamic_reclaim: v("dynamic_reclaim"),
                periodic_reclaim: v("periodic_reclaim"),
                reclaim_count: v("reclaim_count"),
                reclaim_bytes: v("reclaim_bytes"),
                reclaim_errors: v("reclaim_errors"),
            });
        }
        for dev in std::fs::read_dir(s.join("devinfo"))
            .into_iter()
            .flatten()
            .flatten()
        {
            for (k, v) in read_pairs(&dev.path().join("error_stats")) {
                *info.errors.entry(k).or_default() += v;
            }
        }
        info.balance = balance_progress(&fs.mountpoint);
        info
    }

    /// Starts `balance` or `syncfs` in a thread of its own; refuses while
    /// another action runs or when the filesystem is not mounted.
    pub fn start(self: &Arc<Self>, name: &str) -> Result<(), String> {
        let run: fn(&Path) -> Result<String, String> = match name {
            "balance" => balance_empty_data,
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
                info!(
                    "statistics page: {name} on {} started",
                    fs.mountpoint.display()
                );
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

/// Mount point and read-only flag of the top-most mount of `source`.
fn mount_of(source: &str) -> Option<(PathBuf, bool)> {
    let text = std::fs::read_to_string("/proc/self/mountinfo").ok()?;
    let mut found = None;
    for line in text.lines() {
        let Some((left, right)) = line.split_once(" - ") else {
            continue;
        };
        let left: Vec<&str> = left.split(' ').collect();
        let right: Vec<&str> = right.split(' ').collect();
        if right
            .get(1)
            .map(|s| crate::unescape_mountinfo(s))
            .as_deref()
            == Some(source)
            && let (Some(point), Some(opts)) = (left.get(4), left.get(5))
        {
            let ro = opts.split(',').any(|o| o == "ro");
            found = Some((PathBuf::from(crate::unescape_mountinfo(point)), ro));
        }
    }
    found
}

fn open_dir(path: &Path) -> Result<std::fs::File, String> {
    std::fs::File::open(path).map_err(|e| format!("cannot open {}: {e}", path.display()))
}

/// `btrfs balance start -dusage=0`: relocates (that is, deletes) the data
/// block groups nothing lives in.
fn balance_empty_data(mountpoint: &Path) -> Result<String, String> {
    let dir = open_dir(mountpoint)?;
    let mut args = IoctlBalanceArgs::zeroed();
    args.flags = BALANCE_DATA;
    args.data.flags = BALANCE_ARGS_USAGE;
    args.data.usage = 0;
    // SAFETY: the ioctl reads and writes one btrfs_ioctl_balance_args we own.
    let r = unsafe { libc::ioctl(dir.as_raw_fd(), BTRFS_IOC_BALANCE_V2, &mut args) };
    if r < 0 {
        return Err(format!("balance: {}", std::io::Error::last_os_error()));
    }
    let [_, considered, completed] = args.stat;
    Ok(format!(
        "{completed} empty data block groups relocated of {considered} considered"
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

/// `key value` lines (`commit_stats`, `error_stats`).
fn read_pairs(path: &Path) -> BTreeMap<String, u64> {
    read_text(path)
        .unwrap_or_default()
        .lines()
        .filter_map(|l| {
            let (k, v) = l.split_once(' ')?;
            Some((k.to_string(), v.trim().parse().ok()?))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pairs() {
        let dir = std::env::temp_dir().join(format!("btrs3fs-pairs-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let f = dir.join("commit_stats");
        std::fs::write(&f, "commits 6\ncur_commit_ms 0\nlast_commit_ms 79\nbad\n").unwrap();
        let p = read_pairs(&f);
        assert_eq!(p.get("commits"), Some(&6));
        assert_eq!(p.get("last_commit_ms"), Some(&79));
        assert_eq!(p.len(), 3);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn not_mounted() {
        let b = Btrfs::new("ublkb-none");
        let i = b.info();
        assert!(!i.mounted);
        assert_eq!(i.device, "/dev/ublkb-none");
    }
}

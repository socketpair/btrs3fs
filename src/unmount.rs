//! Stopping the device when its filesystem is unmounted by hand (SPEC.md
//! §9.2): a device of `btrs3fs mount` has no use without its mount. An
//! inotify watch on the root of the mounted filesystem does not keep it
//! busy; when the filesystem goes away the kernel removes the watch and
//! reports `IN_UNMOUNT`. Other mounts never wake it, and a filesystem still
//! mounted elsewhere (a bind mount) sends nothing.

use log::{info, warn};
use std::ffi::CString;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// How often the daemon looks for its filesystem until it is first mounted
/// (`btrs3fs mount` mounts it after the device is up).
const FIND_POLL: Duration = Duration::from_secs(1);

/// What a batch of inotify events says.
#[derive(Debug, PartialEq)]
enum Seen {
    Nothing,
    /// The filesystem of the watched root went away.
    Unmounted,
    /// The watch was removed for another reason.
    WatchGone,
}

/// Looks through `struct inotify_event`s (16 bytes and a name each).
fn scan(buf: &[u8]) -> Seen {
    let mut seen = Seen::Nothing;
    let mut pos = 0;
    while let Some(head) = buf.get(pos..pos + 16) {
        let mask = u32::from_ne_bytes(head[4..8].try_into().unwrap());
        let len = u32::from_ne_bytes(head[12..16].try_into().unwrap()) as usize;
        if mask & libc::IN_UNMOUNT != 0 {
            return Seen::Unmounted;
        }
        if mask & libc::IN_IGNORED != 0 {
            seen = Seen::WatchGone;
        }
        pos += 16 + len;
    }
    seen
}

/// Where `/dev/<device>` is mounted now (the top-most mount), if anywhere.
fn mount_point(device: &str) -> Option<PathBuf> {
    let source = format!("/dev/{device}");
    crate::mountinfo::mounts()
        .ok()?
        .into_iter()
        .rfind(|m| m.source == source)
        .map(|m| m.point)
}

/// An inotify watch on `dir`; `IN_DELETE_SELF` only, which a mount root
/// never sees: the watch is there for `IN_UNMOUNT`, sent to every watch.
fn watch(dir: &Path) -> std::io::Result<OwnedFd> {
    // SAFETY: plain syscall; the descriptor is owned right away.
    let fd = unsafe { libc::inotify_init1(libc::IN_CLOEXEC) };
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: a fresh descriptor nothing else owns.
    let fd = unsafe { OwnedFd::from_raw_fd(fd) };
    let path = CString::new(dir.as_os_str().as_bytes())?;
    // SAFETY: valid descriptor and NUL-terminated path.
    let wd = unsafe {
        libc::inotify_add_watch(
            fd.as_raw_fd(),
            path.as_ptr(),
            libc::IN_DELETE_SELF | libc::IN_ONLYDIR,
        )
    };
    if wd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(fd)
}

/// Blocks until the watch reports something that matters.
fn wait(fd: &OwnedFd) -> std::io::Result<Seen> {
    let mut buf = [0u8; 4096];
    loop {
        // SAFETY: reads into a buffer we own, at most its length.
        let n = unsafe { libc::read(fd.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len()) };
        if n < 0 {
            let e = std::io::Error::last_os_error();
            if e.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Err(e);
        }
        match scan(&buf[..n as usize]) {
            Seen::Nothing => continue,
            seen => return Ok(seen),
        }
    }
}

/// Sets `stop` once the filesystem on `device` (`ublkbN`), first seen
/// mounted, is unmounted; runs in a thread of its own.
pub fn stop_on_unmount(device: &str, stop: Arc<AtomicBool>) {
    let mut point = loop {
        if let Some(p) = mount_point(device) {
            break p;
        }
        std::thread::sleep(FIND_POLL);
    };
    loop {
        let watched = watch(&point);
        // Unmounted between the look and the watch: the watch would sit on
        // the directory under the mount.
        let Some(now) = mount_point(device) else {
            break;
        };
        if now != point {
            point = now; // moved: watch the new place
            continue;
        }
        let fd = match watched {
            Ok(fd) => fd,
            Err(e) => {
                warn!("cannot watch {} for its unmount: {e}", point.display());
                return;
            }
        };
        match wait(&fd) {
            Ok(Seen::Unmounted) => break,
            Ok(_) => {} // the watch went otherwise: look again
            Err(e) => {
                warn!("watching {} for its unmount failed: {e}", point.display());
                return;
            }
        }
        match mount_point(device) {
            Some(p) => point = p,
            None => break,
        }
    }
    info!(
        "/dev/{device} was unmounted from {}: stopping the device",
        point.display()
    );
    stop.store(true, Ordering::SeqCst);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `struct inotify_event` (wd, mask, cookie, len) and its name.
    fn event(mask: u32, name: &[u8]) -> Vec<u8> {
        let mut e = Vec::new();
        e.extend_from_slice(&1i32.to_ne_bytes());
        e.extend_from_slice(&mask.to_ne_bytes());
        e.extend_from_slice(&0u32.to_ne_bytes());
        e.extend_from_slice(&(name.len() as u32).to_ne_bytes());
        e.extend_from_slice(name);
        e
    }

    #[test]
    fn unmount_among_events() {
        let mut buf = event(libc::IN_DELETE_SELF, b"");
        assert_eq!(scan(&buf), Seen::Nothing);
        buf.extend(event(libc::IN_UNMOUNT, b"\0\0\0\0"));
        buf.extend(event(libc::IN_IGNORED, b""));
        assert_eq!(scan(&buf), Seen::Unmounted);
        assert_eq!(scan(&event(libc::IN_IGNORED, b"")), Seen::WatchGone);
        assert_eq!(scan(&[1, 2, 3]), Seen::Nothing); // cut short: ignored
    }
}

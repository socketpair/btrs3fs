//! Mounts of this process's namespace from `/proc/self/mountinfo`, for
//! `mount`/`umount` and the statistics page alike.

use std::path::PathBuf;

pub struct Mount {
    pub point: PathBuf,
    pub source: String,
    /// Mounted read-only (the per-mount `ro` option).
    pub read_only: bool,
}

/// Every mount, in mount order: of several on one point, the last is on top.
pub fn mounts() -> std::io::Result<Vec<Mount>> {
    Ok(parse(&std::fs::read_to_string("/proc/self/mountinfo")?))
}

/// Fields: `id parent maj:min root point options [optional…] - type source super-options`.
fn parse(text: &str) -> Vec<Mount> {
    text.lines()
        .filter_map(|line| {
            let (left, right) = line.split_once(" - ")?;
            let left: Vec<&str> = left.split(' ').collect();
            let (point, options) = (left.get(4)?, left.get(5)?);
            let source = right.split(' ').nth(1)?;
            Some(Mount {
                point: PathBuf::from(unescape(point)),
                source: unescape(source),
                read_only: options.split(',').any(|o| o == "ro"),
            })
        })
        .collect()
}

/// Decodes `\040`-style escapes used in mountinfo: a backslash and three
/// octal digits (value = d1·64 + d2·8 + d3), e.g. `\040` is a space.
fn unescape(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'\\'
            && i + 3 < b.len()
            && b[i + 1..i + 4].iter().all(|c| (b'0'..=b'7').contains(c))
        {
            out.push((b[i + 1] - b'0') * 64 + (b[i + 2] - b'0') * 8 + (b[i + 3] - b'0'));
            i += 4;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escapes() {
        assert_eq!(unescape(r"/mnt/my\040disk"), "/mnt/my disk");
        assert_eq!(unescape(r"/mnt/plain"), "/mnt/plain");
        assert_eq!(unescape(r"tail\04"), r"tail\04");
    }

    #[test]
    fn lines() {
        let m = parse(
            "36 35 98:0 / /mnt/my\\040disk rw,noatime master:1 - btrfs /dev/ublkb0 rw\n\
             37 35 98:0 / /ro ro,relatime - ext4 /dev/sda1 rw\n\
             broken line\n",
        );
        assert_eq!(m.len(), 2);
        assert_eq!(m[0].point, PathBuf::from("/mnt/my disk"));
        assert_eq!(m[0].source, "/dev/ublkb0");
        assert!(!m[0].read_only);
        assert!(m[1].read_only);
    }
}

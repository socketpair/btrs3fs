//! Device identity for lsblk and /dev/disk/by-id (SPEC.md §2). ublk has no
//! such fields, so the daemon sends a synthetic uevent with the udev
//! properties hex-encoded (the kernel accepts only letters and digits in
//! values); the udev rule from setup-permissions.sh decodes them with
//! `basenc --base16 -d` from coreutils.

use log::{debug, warn};
use std::fmt::Write as _;

/// Longest property text before hex encoding. All variables of a uevent
/// share a 2 KiB buffer (an overflow prints a kernel WARNING); the standard
/// ones take ~300 bytes, hex doubles the length.
const MAX_PAYLOAD: usize = 768;

/// Any UUID will do: it only tags the event (`SYNTH_UUID`).
const EVENT_UUID: &str = "00000000-0000-0000-0000-000000000000";

/// udev properties `KEY=VALUE`, one per line. `serial` is `<bucket>/<prefix>`;
/// without it (a RAM disk) only the model and the revision.
pub fn properties(serial: Option<&str>) -> String {
    let base = format!(
        "ID_MODEL=btrs3fs\nID_REVISION={}\n",
        env!("BTRS3FS_VERSION")
    );
    let Some(serial) = serial else {
        return base;
    };
    // A line break would end the property early.
    let value: String = serial
        .chars()
        .map(|c| if c.is_control() { '-' } else { c })
        .collect();
    let link: String = serial
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || "._-".contains(c) {
                c
            } else {
                '-'
            }
        })
        .collect();
    let full = format!("{base}ID_SERIAL_SHORT={value}\nBTRS3FS_LINK={link}\n");
    if full.len() > MAX_PAYLOAD {
        warn!(
            "serial {value:?} is too long for a uevent ({} > {MAX_PAYLOAD} bytes): lsblk shows no serial",
            full.len()
        );
        return base;
    }
    full
}

/// RFC 4648 base16 (upper-case hex), what `basenc --base16 -d` reads.
fn base16(text: &str) -> String {
    let mut hex = String::with_capacity(2 * text.len());
    for b in text.bytes() {
        let _ = write!(hex, "{b:02X}");
    }
    hex
}

/// Sends the properties to udev as a synthetic `change` event of `bdev`.
pub fn announce(bdev: &str, serial: Option<&str>) {
    let Some(name) = bdev.strip_prefix("/dev/") else {
        return;
    };
    let hex = base16(&properties(serial));
    let path = format!("/sys/block/{name}/uevent");
    match crate::device::write_sysfs(&path, format!("change {EVENT_UUID} BTRS3FS={hex}")) {
        Ok(()) => debug!("{path}: identity sent to udev"),
        Err(e) => warn!("cannot send the device identity to udev via {path}: {e}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serial_and_link() {
        let p = properties(Some("ideco-bucket-test/test/play x\n"));
        assert!(p.starts_with("ID_MODEL=btrs3fs\nID_REVISION="));
        assert!(p.ends_with(
            "ID_SERIAL_SHORT=ideco-bucket-test/test/play x-\n\
             BTRS3FS_LINK=ideco-bucket-test-test-play-x-\n"
        ));
        assert_eq!(properties(None).lines().count(), 2);
        assert_eq!(properties(Some(&"x".repeat(MAX_PAYLOAD))), properties(None));
    }

    #[test]
    fn base16_is_upper_hex() {
        assert_eq!(base16("ID=a/b\n"), "49443D612F620A");
    }
}

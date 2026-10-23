#!/usr/bin/env bash
# One-time host setup for btrs3fs so that no sudo is needed during development:
#   - group "ublk" gets rw access to /dev/ublk-control, /dev/ublkcN, /dev/ublkbN
#     (the daemon uses unprivileged ublk mode: UBLK_F_UNPRIVILEGED_DEV);
#   - ublk_drv is loaded at boot;
#   - /dev/ublkb0 can be mounted/unmounted by a regular user via fstab "user" option
#     ("user" implies nosuid,nodev,noexec).
#
# Usage:
#   sudo ./setup-permissions.sh install   [user]   # user defaults to $SUDO_USER
#   sudo ./setup-permissions.sh uninstall [user]
#
# After install the user must start a new login session to get the group,
# or run commands via: sg ublk -c '<command>'
set -euo pipefail

GROUP=ublk
DEVICE=/dev/ublkb0
MOUNTPOINT=/mnt/btrs3fs
UDEV_RULES=/etc/udev/rules.d/99-ublk.rules
MODULES_LOAD=/etc/modules-load.d/ublk.conf
FSTAB_LINE="$DEVICE $MOUNTPOINT btrfs noauto,user 0 0"

die() { echo "error: $*" >&2; exit 1; }

[[ $EUID -eq 0 ]] || die "must be run as root: sudo $0 install|uninstall [user]"

ACTION="${1:-}"
TARGET_USER="${2:-${SUDO_USER:-}}"
[[ -n $TARGET_USER ]] || die "cannot determine target user, pass it as the second argument"
id "$TARGET_USER" >/dev/null 2>&1 || die "user '$TARGET_USER' does not exist"

install() {
    getent group "$GROUP" >/dev/null || groupadd --system "$GROUP"
    usermod --append --groups "$GROUP" "$TARGET_USER"

    echo ublk_drv > "$MODULES_LOAD"

    cat > "$UDEV_RULES" <<EOF
KERNEL=="ublk-control", GROUP="$GROUP", MODE="0660"
KERNEL=="ublkc[0-9]*",  GROUP="$GROUP", MODE="0660"
KERNEL=="ublkb[0-9]*",  GROUP="$GROUP", MODE="0660"
EOF
    udevadm control --reload
    modprobe ublk_drv
    # Re-apply rules to nodes that may already exist (module loaded earlier).
    udevadm trigger --action=change --sysname-match='ublk*'
    udevadm settle

    mkdir -p "$MOUNTPOINT"
    if ! grep -qF "$FSTAB_LINE" /etc/fstab; then
        echo "$FSTAB_LINE" >> /etc/fstab
        systemctl daemon-reload
    fi

    echo "installed for user '$TARGET_USER':"
    ls -l /dev/ublk-control
    grep -F "$FSTAB_LINE" /etc/fstab
}

uninstall() {
    if grep -qF "$FSTAB_LINE" /etc/fstab; then
        grep -vF "$FSTAB_LINE" /etc/fstab > /etc/fstab.btrs3fs.tmp
        cat /etc/fstab.btrs3fs.tmp > /etc/fstab   # keep inode, owner and SELinux label
        rm -f /etc/fstab.btrs3fs.tmp
        systemctl daemon-reload
    fi
    rmdir "$MOUNTPOINT" 2>/dev/null || true

    rm -f "$UDEV_RULES" "$MODULES_LOAD"
    udevadm control --reload
    udevadm trigger --action=change --sysname-match='ublk*'

    gpasswd --delete "$TARGET_USER" "$GROUP" 2>/dev/null || true
    echo "uninstalled; group '$GROUP' kept (remove with: groupdel $GROUP)"
}

case "$ACTION" in
    install)   install ;;
    uninstall) uninstall ;;
    *)         die "usage: sudo $0 install|uninstall [user]" ;;
esac

#!/usr/bin/env bash
# One-time host setup for btrs3fs so that no sudo is needed during development.
#
#   install     runtime packages + permissions:
#               - group "btrs3fs" gets rw access to /dev/ublk-control, /dev/ublkcN, /dev/ublkbN
#                 (note: this covers ALL ublk devices on the host, not only btrs3fs ones);
#                 (the daemon uses unprivileged ublk mode: UBLK_F_UNPRIVILEGED_DEV);
#               - ublk_drv is loaded at boot;
#               - /dev/ublkb0 can be mounted/unmounted by a regular user via fstab "user"
#                 option ("user" implies nosuid,nodev,noexec).
#   uninstall   revert permissions and delete the group (packages are kept).
#   build-deps  packages needed to build btrs3fs from source.
#   diag-tools  packages for testing and diagnostics.
#
# Usage:
#   sudo ./setup-permissions.sh install    [user]   # user defaults to $SUDO_USER
#   sudo ./setup-permissions.sh uninstall  [user]
#   sudo ./setup-permissions.sh build-deps
#   sudo ./setup-permissions.sh diag-tools
#
# After install the user must start a new login session to get the group,
# or run commands via: sg btrs3fs -c '<command>'
set -euo pipefail

GROUP=btrs3fs
DEVICE=/dev/ublkb0
MOUNTPOINT=/mnt/btrs3fs
UDEV_RULES=/etc/udev/rules.d/99-btrs3fs-ublk.rules
MODULES_LOAD=/etc/modules-load.d/btrs3fs-ublk.conf
FSTAB_LINE="$DEVICE $MOUNTPOINT btrfs noauto,user 0 0"

# Strictly required to run a ready-made btrs3fs binary.
RUNTIME_PACKAGES=(
    kernel-modules-core   # ublk_drv.ko
    systemd-udev          # udevadm, device node permissions
    shadow-utils          # groupadd, usermod, gpasswd, sg
    util-linux            # mount/umount with fstab "user" option
    btrfs-progs           # mkfs.btrfs
    ca-certificates       # TLS to the S3 endpoint
)

# Required to build btrs3fs from source.
BUILD_PACKAGES=(
    rust cargo            # compiler and build tool
    gcc                   # linker used by rustc
    clang-devel           # libclang for bindgen (libublk-rs-sys generates ublk bindings)
    kernel-headers        # linux/ublk_cmd.h, linux/blkzoned.h
    glibc-devel
    git
)

# Testing and diagnostics.
DIAG_PACKAGES=(
    fio                   # load tests with data verification
    blktrace              # block layer request tracing (blktrace/blkparse)
    sysstat               # iostat
    strace
    gdb
    perf
    jq                    # inspect disk.json and JSON logs
    awscli2               # inspect bucket contents independently of btrs3fs
)

die() { echo "error: $*" >&2; exit 1; }

[[ $EUID -eq 0 ]] || die "must be run as root: sudo $0 install|uninstall|build-deps|diag-tools [user]"

ACTION="${1:-}"

target_user() {
    local user="${1:-${SUDO_USER:-}}"
    [[ -n $user ]] || die "cannot determine target user, pass it as the second argument"
    id "$user" >/dev/null 2>&1 || die "user '$user' does not exist"
    echo "$user"
}

install_packages() {
    dnf install -y "$@"
}

install() {
    local user
    user="$(target_user "${1:-}")"

    install_packages "${RUNTIME_PACKAGES[@]}"

    getent group "$GROUP" >/dev/null || groupadd --system "$GROUP"
    usermod --append --groups "$GROUP" "$user"

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

    echo "installed for user '$user':"
    ls -l /dev/ublk-control
    grep -F "$FSTAB_LINE" /etc/fstab
}

uninstall() {
    local user
    user="$(target_user "${1:-}")"

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

    if getent group "$GROUP" >/dev/null; then
        gpasswd --delete "$user" "$GROUP" 2>/dev/null || true
        groupdel "$GROUP"
    fi
    echo "uninstalled; packages kept"
}

case "$ACTION" in
    install)    install "${2:-}" ;;
    uninstall)  uninstall "${2:-}" ;;
    build-deps) install_packages "${BUILD_PACKAGES[@]}" ;;
    diag-tools) install_packages "${DIAG_PACKAGES[@]}" ;;
    *)          die "usage: sudo $0 install|uninstall|build-deps|diag-tools [user]" ;;
esac

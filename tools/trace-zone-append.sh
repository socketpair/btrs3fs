#!/usr/bin/env bash
# Diagnoses how btrfs data writes reach a btrs3fs device: sizes of btrfs
# ordered extents, reserved extents, btrfs bios and bios queued to the device,
# plus kernel stacks of 4 KiB btrfs bios. Writes a test file, removes it after.
#
# Usage: sudo tools/trace-zone-append.sh [mountpoint] [size]
#        (defaults: /mnt/btrs3fs 16M; needs bpftrace: setup-permissions.sh diag-tools)
set -euo pipefail

MNT="${1:-/mnt/btrs3fs}"
SIZE="${2:-16M}"
[[ $EUID -eq 0 ]] || { echo "error: run as root" >&2; exit 1; }
command -v bpftrace >/dev/null || { echo "error: bpftrace is not installed" >&2; exit 1; }

DEV="$(findmnt -no SOURCE "$MNT")"
[[ "$DEV" == /dev/ublkb* ]] || { echo "error: $MNT is not mounted from a ublk device" >&2; exit 1; }
read -r MAJ MIN < <(stat -Lc '%Hr %Lr' "$DEV")
KDEV=$(( (MAJ << 20) | MIN ))   # dev_t as stored in block tracepoints
OUT="$(mktemp)"
TESTFILE="$MNT/zz-trace-zone-append.bin"
trap 'rm -f "$OUT" "$TESTFILE"' EXIT

echo "tracing $DEV ($MAJ:$MIN) mounted on $MNT, writing $SIZE"
bpftrace -q -o "$OUT" -e "
tracepoint:btrfs:btrfs_ordered_extent_add { @ordered_extent_len = hist(args.len); }
tracepoint:btrfs:btrfs_reserve_extent { @reserved_extent_len = hist(args.len); }
kprobe:btrfs_submit_bbio {
  \$size = ((struct btrfs_bio *)arg0)->bio.bi_iter.bi_size;
  @btrfs_submit_bbio_size = hist(\$size);
  if (\$size == 4096) { @stack_of_4k_bbio[kstack(12)] = count(); }
}
tracepoint:block:block_bio_queue /args.dev == $KDEV/ { @bio_queued_bytes[args.rwbs] = hist(args.nr_sector * 512); }
tracepoint:block:block_split /args.dev == $KDEV/ { @bio_split[args.rwbs] = count(); }
" &
BPF=$!
sleep 3   # let bpftrace attach

dd if=/dev/urandom of="$TESTFILE" bs=1M count="${SIZE%M}" status=none
sync -f "$MNT"
filefrag "$TESTFILE"
sleep 1

kill -INT "$BPF"
wait "$BPF" || true
echo "================ bpftrace ================"
# Keep the output short: at most 3 stacks.
awk '/^@stack_of_4k_bbio/ {n++} n<=3 || !/^@stack_of_4k_bbio/' "$OUT"

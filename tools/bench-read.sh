#!/bin/bash
# Cold sequential read throughput of a mounted btrs3fs disk against the kernel
# request size (queue/max_sectors_kb) and the btrfs readahead window
# (bdi read_ahead_kb). Needs root for sysfs and drop_caches; run as yourself:
#   bash tools/bench-read.sh [--remount PREFIX]
# Results go to target/bench-read.txt. Restores both settings at the end.
set -u
export LC_ALL=C   # printf and bc agree on the decimal point
cd "$(dirname "$0")/.."
M=/mnt/btrs3fs
FILE=$M/big1g.bin
OUT=target/bench-read.txt
CHUNK_MIB=96
: > "$OUT"
log() { echo "$*" | tee -a "$OUT"; }

if [ "${1:-}" = "--remount" ]; then
    sudo target/debug/btrs3fs umount $M || exit 1
    sudo target/debug/btrs3fs mount --creds creds.json --prefix "$2" $M || exit 1
    sleep 3
fi
DEV=$(basename "$(findmnt -no SOURCE $M)")
Q=/sys/block/$DEV/queue
BDI=/sys/fs/btrfs/$(findmnt -no UUID $M)/bdi
log "device $DEV: wbt_lat_usec=$(cat $Q/wbt_lat_usec) scheduler=$(cat $Q/scheduler)"
[ -f "$FILE" ] || { log "no $FILE"; exit 1; }
MSK0=$(cat $Q/max_sectors_kb); RA0=$(cat $BDI/read_ahead_kb)
log "defaults: max_sectors_kb=$MSK0 read_ahead_kb=$RA0"

snap() { curl -s 127.0.0.1:9470/stats.json | python3 -c 'import json,sys; s=json.load(sys.stdin); print(s["gets"], s["read_from_s3"])'; }
i=0
for msk in 16384 4096 1024 512; do
    for ra in 4096 16384; do
        echo $msk | sudo tee $Q/max_sectors_kb > /dev/null
        echo $ra | sudo tee $BDI/read_ahead_kb > /dev/null
        sync; echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
        read g0 b0 < <(snap)
        t0=$(date +%s.%N)
        dd if=$FILE of=/dev/null bs=1M count=$CHUNK_MIB skip=$((i * CHUNK_MIB)) status=none
        t1=$(date +%s.%N)
        read g1 b1 < <(snap)
        secs=$(echo "$t1 - $t0" | bc)
        log "$(printf 'max_sectors_kb %5s read_ahead_kb %5s: %3s MiB in %6.1f s = %5.1f MiB/s, %4s GET of ~%s KiB' \
            $msk $ra $CHUNK_MIB $secs $(echo "$CHUNK_MIB / $secs" | bc -l) $((g1 - g0)) $(( (b1 - b0) / 1024 / ((g1 - g0) > 0 ? (g1 - g0) : 1) )))"
        i=$((i + 1))
    done
done
echo $MSK0 | sudo tee $Q/max_sectors_kb > /dev/null
echo $RA0 | sudo tee $BDI/read_ahead_kb > /dev/null
log "restored max_sectors_kb=$MSK0 read_ahead_kb=$RA0"

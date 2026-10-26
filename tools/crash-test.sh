#!/bin/bash
# tools/crash-test.sh CREDS PREFIX ROUNDS "LINK FLAGS": parallel fsync writers, kill -9 of
# the daemon under load, remount, check every file whose fsync returned.
# Disk experiments run on the test VM (CLAUDE.md); needs sudo for btrs3fs.
# The disk must hold nothing of value: files are written and deleted at will.
set -u
export LC_ALL=C
CREDS=$1 PREFIX=$2 ROUNDS=$3 LINK=$4
B=target/debug/btrs3fs M=/mnt/btrs3fs
SP=target/crash-test
mkdir -p $SP; : > $SP/acked; : > $SP/deleted
mnt() { sudo -n $B mount --creds $CREDS --prefix $PREFIX $LINK $M >/dev/null || { echo "MOUNT FAILED"; exit 1; }; sudo -n chown mmarkk $M; }
writer() { # writer ID ROUND
    local i=0 f
    mkdir -p $M/w$1 2>/dev/null || return
    while :; do
        i=$((i + 1)); f=$M/w$1/r$2-$i
        head -c $(( (1 + RANDOM % 64) * 4096 )) /dev/urandom > $f 2>/dev/null || return
        sync -f $f 2>/dev/null || return
        echo "$(sha256sum $f | cut -d' ' -f1) $f" >> $SP/acked
    done
}
deleter() { # deletes files of earlier rounds, logged before deleting
    while :; do
        local f=$(grep -v "r$1-" $SP/acked 2>/dev/null | shuf -n1 | cut -d' ' -f2)
        [ -n "$f" ] && { echo "$f" >> $SP/deleted; rm -f "$f" 2>/dev/null || return; }
        sleep 0.2
    done
}
findmnt $M >/dev/null || mnt
for r in $(seq 1 $ROUNDS); do
    pids=()
    for w in 1 2 3 4; do writer $w $r & pids+=($!); done
    deleter $r & pids+=($!)
    t=$((8 + RANDOM % 13)); sleep $t
    sudo -n $B detach --force --dev-id $(basename $(findmnt -no SOURCE $M) | tr -dc 0-9) >/dev/null 2>&1   # kill -9 the daemon
    sleep 1; kill ${pids[@]} 2>/dev/null; wait 2>/dev/null
    sudo -n $B umount $M >/dev/null 2>&1 || { echo "umount after crash failed"; exit 1; }
    mnt
    bad=0 ok=0
    while read sum f; do
        grep -qxF "$f" $SP/deleted && continue
        if [ "$(sha256sum "$f" 2>/dev/null | cut -d' ' -f1)" = "$sum" ]; then ok=$((ok+1)); else bad=$((bad+1)); echo "  BAD: $f"; fi
    done < $SP/acked
    restore=$(journalctl -t btrs3fs --since "-40s" --no-pager | grep -E "restored|deleted .* segments|aborted|past the zone" | sed 's/^.*btrs3fs\[[0-9]*\]: //' | tr '\n' ';')
    echo "round $r: killed after ${t}s; fsynced files intact $ok, damaged $bad; $restore"
done

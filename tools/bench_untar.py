#!/usr/bin/env python3
"""Write benchmark: btrs3fs, geesefs and ZeroFS on the same bucket.

Each round writes on the chosen filesystems (--fs) in turn and
measures the time until every byte is in S3. The workload (--workload) is
either unpacking the archive (untar: many small files) or decompressing it
into one tar file (tar: one large file):

  btrs3fs  write -> syncfs()               (syncfs commits btrfs; the device
                                             answers FLUSH after S3 confirms)
  geesefs  write -> syncfs() -> unmount    (geesefs uploads what is left after
                                             the unmount and only then exits;
                                             the clock stops at its exit)
  zerofs   write -> syncfs() -> unmount -> `zerofs flush`
                                            (SIGTERM to the FUSE client: it unmounts
                                             and hands its write-back cache to the
                                             server; the flush returns once the
                                             server has put everything into S3)

Not measured: deleting the previous run, the syncfs after it and the
background work of btrs3fs (balance of empty block groups, concatenation
of zones, upload of buffers), which is all done before the clock starts.

The link is measured once with `btrs3fs measure`; btrs3fs is remounted with
that file, and geesefs gets the same number of parallel uploads. ZeroFS
uploads at most 4 segments at once (fixed in its code) and runs uncompressed
(`compression = "none"`, a local patch: it has no such mode upstream).

Runs as the user; root only through `sudo <btrs3fs>` for umount/mount.
"""

import argparse
import signal
import csv
import ctypes
import json
import os
import shutil
import statistics
import subprocess
import sys
import time
import urllib.request
from datetime import datetime
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
BENCH_DIR = "bench-untar"  # only this directory is cleaned on the tested filesystems
WORKLOADS = ("untar", "tar")

libc = ctypes.CDLL(None, use_errno=True)


def log(msg):
    print(f"{datetime.now():%H:%M:%S} {msg}", flush=True)


def run(cmd, **kw):
    log("$ " + " ".join(str(c) for c in cmd))
    return subprocess.run(cmd, check=True, **kw)


def syncfs(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        if libc.syncfs(fd) != 0:
            e = ctypes.get_errno()
            raise OSError(e, f"syncfs({path}): {os.strerror(e)}")
    finally:
        os.close(fd)


def warm_page_cache(archive):
    with open(archive, "rb") as f:
        while f.read(16 << 20):
            pass


def wipe(directory):
    """rm -rf of the benchmark directory, then an empty one in its place."""
    if directory.exists():
        run(["rm", "-rf", "--one-file-system", str(directory)])
    directory.mkdir()


def write(workload, archive, directory):
    if workload == "untar":
        run(["tar", "-I", "zstd", "-xf", str(archive), "-C", str(directory)])
    else:
        tar = directory / Path(archive.name).with_suffix("").name  # drivers.tar.zstd -> drivers.tar
        with open(tar, "wb") as out:
            run(["zstd", "-d", "-q", "-c", str(archive)], stdout=out)


class Btrs3fs:
    name = "btrs3fs"

    def __init__(self, a, link_yaml):
        self.a = a
        self.link_yaml = link_yaml
        self.url = f"http://127.0.0.1:{a.stats_port}/"
        self.dir = a.btrs3fs_mnt / BENCH_DIR
        self.was_paused = None

    def bin(self, *args):
        return ["sudo", str(self.a.btrs3fs_bin), *args]

    def get(self, name):
        with urllib.request.urlopen(self.url + name, timeout=30) as r:
            return json.load(r)

    def post(self, name):
        req = urllib.request.Request(self.url + name, method="POST", data=b"")
        with urllib.request.urlopen(req, timeout=30) as r:
            j = json.load(r)
        if "error" in j:
            raise RuntimeError(f"{name}: {j['error']}")
        return j

    def remount(self):
        """umount (it uploads everything) and mount with the measured link file."""
        mnt = self.a.btrs3fs_mnt
        if os.path.ismount(mnt):
            busy = subprocess.run(["fuser", "-m", str(mnt)], capture_output=True, text=True)
            if busy.stdout.strip():
                sys.exit(f"{mnt} is in use by {busy.stdout.strip()}; not remounting")
            run(self.bin("umount", str(mnt)))
        run(self.bin("mount", "--creds", str(self.a.creds), "--prefix", self.a.btrs3fs_prefix,
                     "--link-config", str(self.link_yaml),
                     "--stats-port", str(self.a.stats_port), str(mnt)))
        for _ in range(100):
            try:
                self.get("stats.json")
                return
            except OSError:
                time.sleep(0.2)
        sys.exit(f"statistics page {self.url} does not answer")

    def uploads(self):
        for v in self.get("link.json")["values"]:
            if v["key"] == "uploads":
                return int(v["effective"])
        raise RuntimeError("link.json has no uploads value")

    def pause_auto(self):
        self.was_paused = self.get("btrfs.json")["auto"]["paused"]
        self.post("autobalance-pause")

    def restore_auto(self):
        if self.was_paused is False:
            self.post("autobalance-resume")

    def wait(self, what, done, timeout):
        start = time.monotonic()
        while not done():
            if time.monotonic() - start > timeout:
                raise TimeoutError(f"btrs3fs: {what} did not finish in {timeout} s")
            time.sleep(0.5)

    def idle(self):
        s = self.get("stats.json")
        return (s["dirty_bytes"] == 0 and s["uploads_in_flight"] == 0
                and s["compacting"] == 0 and s["zones"]["compact_queued"] == 0)

    def prepare(self):
        """Empty benchmark directory and a disk with no background work left."""
        wipe(self.dir)
        syncfs(self.a.btrs3fs_mnt)
        self.post("balance")
        self.wait("balance", lambda: self.get("btrfs.json")["running"] is None, 600)
        last = self.get("btrfs.json")["last"]
        if not last or not last["ok"]:
            raise RuntimeError(f"btrs3fs balance failed: {last}")
        log(f"btrs3fs: {last['message']}")
        syncfs(self.a.btrs3fs_mnt)
        self.post("compact")
        self.post("flush")
        self.wait("concatenation and uploads", self.idle, 1800)

    def measure(self, workload, archive):
        t0 = time.monotonic()
        write(workload, archive, self.dir)
        t1 = time.monotonic()
        syncfs(self.dir)
        t2 = time.monotonic()
        return {"write_s": t1 - t0, "syncfs_s": t2 - t1, "umount_s": None, "total_s": t2 - t0}

    def finish(self):
        self.restore_auto()


class Geesefs:
    name = "geesefs"

    def __init__(self, a, uploads):
        self.a = a
        self.uploads = uploads
        self.dir = a.geesefs_mnt / BENCH_DIR
        with open(a.creds) as f:
            c = json.load(f)
        ep = c["endpoint"]
        self.endpoint = ep if ep.startswith("http") else "https://" + ep
        self.region = c["region"]
        self.source = f"{c['bucket']}:{a.geesefs_prefix}"
        self.env = dict(os.environ, AWS_ACCESS_KEY_ID=c["access_key"],
                        AWS_SECRET_ACCESS_KEY=c["secret_key"])

    def pid(self):
        """PID of the geesefs serving our mount point, or None."""
        mnt = str(self.a.geesefs_mnt)
        for p in Path("/proc").iterdir():
            if not p.name.isdigit():
                continue
            try:
                argv = (p / "cmdline").read_bytes().split(b"\0")
            except OSError:
                continue
            if argv and argv[0].endswith(b"geesefs") and mnt.encode() in argv:
                return int(p.name)
        return None

    def mount(self):
        n = str(self.uploads)
        cmd = [str(self.a.geesefs_bin), "--endpoint", self.endpoint, "--region", self.region,
               "--max-flushers", n, "--max-parallel-parts", n,
               "--log-file", str(self.a.out / "geesefs.log"),
               self.source, str(self.a.geesefs_mnt)]
        # The keys stay in the environment and out of the printed command.
        log("$ " + " ".join(cmd))
        subprocess.run(cmd, check=True, env=self.env)  # returns once mounted

    def unmount(self, timeout=3600):
        """fusermount3 -u and wait for geesefs to finish uploading and exit."""
        pid = self.pid()
        subprocess.run(["fusermount3", "-u", str(self.a.geesefs_mnt)], check=True)
        if pid is None:
            return
        start = time.monotonic()
        while Path(f"/proc/{pid}").exists():
            if time.monotonic() - start > timeout:
                raise TimeoutError(f"geesefs {pid} still running {timeout} s after unmount")
            time.sleep(0.05)

    def prepare(self):
        if not os.path.ismount(self.a.geesefs_mnt):
            self.mount()
        wipe(self.dir)
        syncfs(self.a.geesefs_mnt)
        self.unmount()
        self.mount()

    def measure(self, workload, archive):
        t0 = time.monotonic()
        write(workload, archive, self.dir)
        t1 = time.monotonic()
        syncfs(self.dir)
        t2 = time.monotonic()
        self.unmount()
        t3 = time.monotonic()
        return {"write_s": t1 - t0, "syncfs_s": t2 - t1, "umount_s": t3 - t2, "total_s": t3 - t0}

    def finish(self):
        if not os.path.ismount(self.a.geesefs_mnt):
            self.mount()


class Zerofs:
    """ZeroFS server (`zerofs run`) and its FUSE client (`zerofs mount`), both
    started here in their own sessions so they outlive the script."""

    name = "zerofs"

    def __init__(self, a):
        self.a = a
        self.dir = a.zerofs_mnt / BENCH_DIR
        with open(a.creds) as f:
            c = json.load(f)
        ep = c["endpoint"]
        self.sock = Path(os.environ.get("XDG_RUNTIME_DIR", "/tmp")) / "zerofs-bench.9p.sock"
        self.rpc = self.sock.with_name("zerofs-bench.rpc.sock")
        self.config = a.out / "zerofs.toml"
        # Keys stay in the environment: the file names the variables only.
        self.config.write_text(f"""[cache]
dir = "${{HOME}}/.cache/zerofs-bench"
disk_size_gb = 10.0
memory_size_gb = 1.0

[storage]
url = "s3://{c['bucket']}/{a.zerofs_prefix}"
encryption_password = "{a.zerofs_password}"

[servers.ninep]
unix_socket = "{self.sock}"

[servers.rpc]
unix_socket = "{self.rpc}"

[filesystem]
compression = "none"

[aws]
access_key_id = "${{AWS_ACCESS_KEY_ID}}"
secret_access_key = "${{AWS_SECRET_ACCESS_KEY}}"
endpoint = "{ep if ep.startswith("http") else "https://" + ep}"
default_region = "{c['region']}"

[telemetry]
enabled = false
""")
        self.env = dict(os.environ, AWS_ACCESS_KEY_ID=c["access_key"],
                        AWS_SECRET_ACCESS_KEY=c["secret_key"])
        self.server = None
        self.client = None

    def start(self, cmd, log_name, ready, what, timeout=120):
        log("$ " + " ".join(cmd))
        out = open(self.a.out / log_name, "a")
        proc = subprocess.Popen(cmd, env=self.env, stdout=out, stderr=subprocess.STDOUT,
                                stdin=subprocess.DEVNULL, start_new_session=True)
        start = time.monotonic()
        while not ready():
            if proc.poll() is not None:
                sys.exit(f"{what} exited with {proc.returncode}, see {self.a.out / log_name}")
            if time.monotonic() - start > timeout:
                sys.exit(f"{what} not ready in {timeout} s, see {self.a.out / log_name}")
            time.sleep(0.1)
        return proc

    def up(self):
        """Server, then the FUSE mount."""
        for s in (self.sock, self.rpc):
            s.unlink(missing_ok=True)
        self.server = self.start([str(self.a.zerofs_bin), "run", "-c", str(self.config)],
                                 "zerofs-server.log", lambda: self.sock.exists() and self.rpc.exists(),
                                 "zerofs server")
        self.client = self.start([str(self.a.zerofs_bin), "mount", f"unix:{self.sock}",
                                  str(self.a.zerofs_mnt)],
                                 "zerofs-mount.log", lambda: os.path.ismount(self.a.zerofs_mnt),
                                 "zerofs mount")

    @staticmethod
    def wait(proc, what, timeout=3600):
        try:
            proc.wait(timeout)
        except subprocess.TimeoutExpired:
            raise TimeoutError(f"{what} still running {timeout} s later")

    def unmount(self):
        """SIGTERM to the client and wait for it to exit: it unmounts and serves
        the kernel's write-back of dirty pages until done. An unmount from
        outside (fusermount3 -u) leaves `zerofs mount` running: it ends only
        on a signal, as its CSI plugin does it."""
        self.client.send_signal(signal.SIGTERM)
        self.wait(self.client, "zerofs mount")
        if os.path.ismount(self.a.zerofs_mnt):
            raise RuntimeError(f"{self.a.zerofs_mnt} is still mounted after zerofs mount exited")

    def down(self):
        """Unmount, then stop the server gracefully (it flushes everything)."""
        if self.client and self.client.poll() is None:
            self.unmount()
        if self.server and self.server.poll() is None:
            self.server.send_signal(signal.SIGTERM)
            self.wait(self.server, "zerofs server")

    def flush(self):
        run([str(self.a.zerofs_bin), "flush", "-c", str(self.config)], env=self.env,
            stdout=subprocess.DEVNULL)

    def prepare(self):
        """Empty benchmark directory and a freshly started server."""
        if self.server is None:
            if os.path.ismount(self.a.zerofs_mnt):
                sys.exit(f"{self.a.zerofs_mnt} is mounted already: unmount it first")
            self.up()
        wipe(self.dir)
        syncfs(self.a.zerofs_mnt)
        self.down()
        self.up()

    def measure(self, workload, archive):
        t0 = time.monotonic()
        write(workload, archive, self.dir)
        t1 = time.monotonic()
        syncfs(self.dir)
        t2 = time.monotonic()
        self.unmount()
        t3 = time.monotonic()
        self.flush()
        t4 = time.monotonic()
        return {"write_s": t1 - t0, "syncfs_s": t2 - t1, "umount_s": t3 - t2,
                "flush_s": t4 - t3, "total_s": t4 - t0}

    def finish(self):
        if self.server is not None and (self.client is None or self.client.poll() is not None):
            # Mounted again for the user, like geesefs.
            self.client = self.start([str(self.a.zerofs_bin), "mount", f"unix:{self.sock}",
                                      str(self.a.zerofs_mnt)],
                                     "zerofs-mount.log", lambda: os.path.ismount(self.a.zerofs_mnt),
                                     "zerofs mount")


def summary(rows):
    print("\nseconds: median (min..max) of", len({r["round"] for r in rows}), "rounds")
    for fs in dict.fromkeys(r["fs"] for r in rows):
        mine = [r for r in rows if r["fs"] == fs]
        parts = []
        for k in ("write_s", "syncfs_s", "umount_s", "flush_s", "total_s"):
            v = [r[k] for r in mine if r[k] is not None]
            if v:
                parts.append(f"{k[:-2]} {statistics.median(v):.1f} ({min(v):.1f}..{max(v):.1f})")
        print(f"  {fs:8} " + "  ".join(parts))


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--archive", type=Path, default=Path.home() / "drivers.tar.zstd")
    p.add_argument("--workload", choices=WORKLOADS, default="untar",
                   help="untar: unpack the archive (many small files); tar: decompress it into one tar file")
    p.add_argument("--rounds", type=int, default=3)
    p.add_argument("--fs", default="btrs3fs,geesefs",
                   help="filesystems to test in this order, of btrs3fs, geesefs, zerofs")
    p.add_argument("--uploads", type=int,
                   help="parallel uploads of geesefs when btrs3fs is not tested (else from its link)")
    p.add_argument("--creds", type=Path, default=REPO / "creds2.json")
    p.add_argument("--btrs3fs-bin", type=Path, default=REPO / "target/debug/btrs3fs")
    p.add_argument("--btrs3fs-mnt", type=Path, default=Path("/mnt/btrs3fs"))
    p.add_argument("--btrs3fs-prefix", default="lenovo")
    p.add_argument("--stats-port", type=int, default=9470)
    p.add_argument("--link-config", type=Path,
                   help="use this link file instead of running `btrs3fs measure`")
    p.add_argument("--geesefs-bin", type=Path, default=Path.home() / "src/GH/geesefs/geesefs")
    p.add_argument("--geesefs-mnt", type=Path, default=Path("/mnt/geesefs"))
    p.add_argument("--geesefs-prefix", default="geesefs/")
    p.add_argument("--zerofs-bin", type=Path,
                   default=Path.home() / "src/GH/ZeroFS/zerofs/target/release/zerofs")
    p.add_argument("--zerofs-mnt", type=Path, default=Path("/mnt/zerofs"))
    p.add_argument("--zerofs-prefix", default="zerofs")
    p.add_argument("--zerofs-password", default="somepass",
                   help="encryption password: a test value, not a secret (user decision)")
    p.add_argument("--out", type=Path,
                   default=None, help="default: ~/bench-untar/<time>-<workload>")
    a = p.parse_args()
    if a.out is None:
        a.out = Path.home() / "bench-untar" / f"{datetime.now():%Y%m%d-%H%M%S}-{a.workload}"
    chosen = [x.strip() for x in a.fs.split(",") if x.strip()]
    unknown = set(chosen) - {"btrs3fs", "geesefs", "zerofs"}
    if unknown or not chosen:
        sys.exit(f"--fs: unknown filesystems {sorted(unknown)}" if unknown else "--fs: none chosen")
    if "geesefs" in chosen and "btrs3fs" not in chosen and not a.uploads:
        sys.exit("--uploads is needed for geesefs without btrs3fs")
    a.out.mkdir(parents=True)

    fss = {}
    uploads = a.uploads
    if "btrs3fs" in chosen:
        link_yaml = a.out / "link.yaml"
        if a.link_config:
            shutil.copy(a.link_config, link_yaml)
        else:
            with open(link_yaml, "w") as f:
                run([str(a.btrs3fs_bin), "measure", "--creds", str(a.creds),
                     "--prefix", a.btrs3fs_prefix], stdout=f)
        log("link:\n" + link_yaml.read_text())
        b = Btrs3fs(a, link_yaml)
        b.remount()
        uploads = b.uploads()
        b.pause_auto()
        fss["btrs3fs"] = b
    if "geesefs" in chosen:
        log(f"parallel uploads of geesefs: {uploads}")
        fss["geesefs"] = Geesefs(a, uploads)
    if "zerofs" in chosen:
        fss["zerofs"] = Zerofs(a)
    order = [fss[x] for x in chosen]

    rows = []
    csv_path = a.out / "results.csv"
    fields = ["round", "fs", "write_s", "syncfs_s", "umount_s", "flush_s", "total_s"]
    try:
        with open(csv_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            for rnd in range(1, a.rounds + 1):
                for fs in order:
                    log(f"round {rnd}/{a.rounds} {fs.name}: preparing")
                    warm_page_cache(a.archive)
                    fs.prepare()
                    log(f"round {rnd}/{a.rounds} {fs.name}: measuring {a.workload}")
                    r = {"round": rnd, "fs": fs.name, "flush_s": None,
                         **fs.measure(a.workload, a.archive)}
                    log("  " + "  ".join(f"{k} {v:.2f}" for k, v in r.items()
                                         if isinstance(v, float)))
                    rows.append(r)
                    w.writerow({k: (f"{v:.3f}" if isinstance(v, float) else v)
                                for k, v in r.items()})
                    f.flush()
    finally:
        for fs in order:
            fs.finish()
    summary(rows)
    log(f"results: {csv_path}")


if __name__ == "__main__":
    main()

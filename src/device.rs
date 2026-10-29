//! ublk zoned block device on top of the zone engine.
//!
//! Each ublk queue runs in its own thread with a local executor driven by the
//! queue's io_uring. Requests that only touch RAM are served in place; requests
//! that need the store (FLUSH, FUA, reads of flushed data, resets, waiting for
//! buffer room) run on the tokio runtime. The queue thread sleeps in io_uring,
//! so a tokio completion wakes it through a per-tag eventfd that the IO task
//! reads with an io_uring READ (SPEC.md §9.1).

use crate::config::{BLOCK_SIZE, DiskConfig};
use crate::logging::REQUESTS_TARGET;
use crate::stats::{Reason, STATS};
use crate::zones::{Cond, Engine, Piece, ZResult, ZoneError};
use anyhow::{Context, bail};
use bytes::Bytes;
use libublk::ctrl::{UblkCtrl, UblkCtrlBuilder};
use libublk::io::{BufDesc, UblkDev, UblkIOCtx, UblkQueue};
use libublk::io_uring::{opcode, types};
use libublk::{UblkError, UblkFlags, UblkRuntime, ops, sys};
use log::{debug, error, info, trace, warn};
use std::future::Future;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::runtime::Handle;

/// ublk passes positions and lengths in 512-byte sectors (`ublksrv_io_desc`),
/// whatever the logical block size of the device.
const SECTOR_SHIFT: u32 = 9;
const BLOCK_SHIFT: u8 = BLOCK_SIZE.trailing_zeros() as u8;

/// How often the main thread checks for SIGTERM/SIGINT/SIGHUP and for queues
/// that stopped on their own. Judgement call: the reaction delay is invisible
/// next to the S3 work of stopping, and polling costs nothing.
const STOP_POLL: Duration = Duration::from_millis(100);

/// ublk features the device cannot work without; there is no fallback
/// (SPEC.md §10). USER_COPY (6.5): zones require it; ZONED: 6.6;
/// FAIL_IO: if we die, the kernel keeps the disk and fails its I/O instead of
/// removing it with del_gendisk() under a mounted filesystem (SPEC.md §9.4),
/// 6.13; USER_RECOVERY only enables FAIL_IO, we never recover.
const REQUIRED_FEATURES: [(u32, &str); 4] = [
    (sys::UBLK_F_USER_COPY, "UBLK_F_USER_COPY"),
    (sys::UBLK_F_ZONED, "UBLK_F_ZONED"),
    (sys::UBLK_F_USER_RECOVERY, "UBLK_F_USER_RECOVERY"),
    (
        sys::UBLK_F_USER_RECOVERY_FAIL_IO,
        "UBLK_F_USER_RECOVERY_FAIL_IO",
    ),
];

/// Oldest kernel with every feature in `REQUIRED_FEATURES` (checked in the
/// kernel's `include/uapi/linux/ublk_cmd.h` history).
const MIN_KERNEL: &str = "6.13";

pub struct DeviceOptions {
    pub dev_id: i32,
    pub nr_queues: u16,
    pub queue_depth: u16,
    /// btrfs readahead window to set once the filesystem on the device is
    /// mounted (`bdi/read_ahead_kb`), bytes; `None` leaves btrfs's own.
    pub fs_readahead: Option<u64>,
    /// `<bucket>/<prefix>`, the serial number lsblk shows (SPEC.md §2);
    /// `None` for a RAM disk.
    pub serial: Option<String>,
    /// Read-only device (SPEC.md §9.2): the kernel refuses writes to it.
    pub read_only: bool,
}

struct Ctx {
    engine: Arc<Engine>,
    rt: Handle,
}

/// Called once the device is live with its path (`/dev/ublkbN`); returns
/// whether to keep serving until a signal (`true`) or to stop right away.
pub type OnReady = Box<dyn FnOnce(&str) -> bool + Send + Sync>;

/// Creates the device and serves it until `on_ready` asks to stop, a
/// SIGTERM/SIGINT/SIGHUP arrives or the device is removed externally. Returns
/// after all queues stopped; the caller then calls `delete()`.
pub fn serve(
    cfg: &DiskConfig,
    engine: Arc<Engine>,
    rt: Handle,
    opts: &DeviceOptions,
    on_ready: OnReady,
) -> anyhow::Result<UblkCtrl> {
    let features = UblkCtrl::get_features().with_context(|| {
        format!("ublk driver is not available (needs Linux >= {MIN_KERNEL} with ublk_drv loaded)")
    })?;
    let missing: Vec<&str> = REQUIRED_FEATURES
        .iter()
        .filter(|(flag, _)| features & *flag as u64 == 0)
        .map(|(_, name)| *name)
        .collect();
    if !missing.is_empty() {
        bail!(
            "ublk driver lacks {} (features {features:#x}); btrs3fs needs Linux >= {MIN_KERNEL}",
            missing.join(", ")
        );
    }
    let needed = REQUIRED_FEATURES
        .iter()
        .fold(0u64, |acc, (flag, _)| acc | *flag as u64);

    // Register before the device appears so a signal never kills us mid-setup.
    let mut signals = signal_hook::iterator::Signals::new([
        signal_hook::consts::SIGTERM,
        signal_hook::consts::SIGINT,
        signal_hook::consts::SIGHUP,
    ])?;

    let ctrl = UblkCtrlBuilder::default()
        .name("btrs3fs")
        .id(opts.dev_id)
        .nr_queues(opts.nr_queues)
        .depth(opts.queue_depth)
        // Largest request = zone size (zones are at most 32 MiB, the user-copy
        // limit of ublk is 32 MiB per request).
        .io_buf_bytes(cfg.zone_size as u32)
        .ctrl_flags(needed)
        .dev_flags(UblkFlags::UBLK_DEV_F_ADD_DEV)
        .build()
        .context("cannot create ublk device")?;

    let disk_size = cfg.disk_size;
    let zone_sectors = (cfg.zone_size >> SECTOR_SHIFT) as u32;
    let depth = opts.queue_depth;
    let read_only = opts.read_only;
    let tgt_init = move |dev: &mut UblkDev| {
        dev.set_default_params(disk_size);
        let p = &mut dev.tgt.params;
        p.types |= sys::UBLK_PARAM_TYPE_ZONED;
        p.basic.attrs = sys::UBLK_ATTR_VOLATILE_CACHE | sys::UBLK_ATTR_FUA;
        if read_only {
            p.basic.attrs |= sys::UBLK_ATTR_READ_ONLY;
        }
        p.basic.logical_bs_shift = BLOCK_SHIFT;
        p.basic.physical_bs_shift = BLOCK_SHIFT;
        p.basic.io_min_shift = BLOCK_SHIFT;
        // The kernel sizes writeback I/O by io_opt: with io_opt = 4 KiB btrfs
        // sent its data as 4 KiB zone appends (SPEC.md §2). Zone size is a
        // power of two, so the shift is exact.
        p.basic.io_opt_shift = zone_sectors.trailing_zeros() as u8 + SECTOR_SHIFT as u8;
        p.basic.chunk_sectors = zone_sectors;
        p.zoned = sys::ublk_param_zoned {
            max_open_zones: 0,
            // No zone limits (SPEC.md §2): btrfs keeps its own active limit.
            max_active_zones: 0,
            max_zone_append_sectors: zone_sectors.min(p.basic.max_sectors),
            ..Default::default()
        };
        // Each tag may have two operations in flight at once: its ublk command
        // and the eventfd read that waits for tokio (SPEC.md §9.1).
        dev.tgt.sq_depth = 2 * depth;
        dev.tgt.cq_depth = 2 * depth;
        Ok(())
    };

    let live_queues = Arc::new(AtomicUsize::new(opts.nr_queues as usize));
    let ctx = Arc::new(Ctx { engine, rt });
    let q_live = live_queues.clone();
    let q_handler = move |qid: u16, dev: &Arc<UblkDev>| {
        run_queue(qid, dev, &ctx);
        q_live.fetch_sub(1, Ordering::SeqCst);
    };

    let zone_size = cfg.zone_size;
    let fs_readahead = opts.fs_readahead;
    let serial = opts.serial.clone();
    let device_fn = move |ctrl: &UblkCtrl| {
        let bdev = ctrl.get_bdev_path();
        info!(
            "device {bdev} is ready ({} queues, depth {})",
            ctrl.dev_info().nr_hw_queues,
            ctrl.dev_info().queue_depth
        );
        crate::ident::announce(&bdev, serial.as_deref());
        disable_wbt(&bdev);
        set_max_request(&bdev, zone_size);
        if let Some(name) = bdev.strip_prefix("/dev/") {
            let name = name.to_string();
            // Lives until the process exits, like the device.
            let _ = std::thread::Builder::new()
                .name("btrs3fs-fs-tune".into())
                .spawn(move || keep_fs_tuned(&name, fs_readahead, !read_only));
        }
        if on_ready(&bdev) {
            loop {
                if let Some(sig) = signals.pending().next() {
                    info!("signal {sig} received, stopping device");
                    break;
                }
                if live_queues.load(Ordering::SeqCst) == 0 {
                    warn!("all queues stopped, device was removed externally");
                    break;
                }
                std::thread::sleep(STOP_POLL);
            }
        }
        if let Err(e) = ctrl.kill_dev() {
            warn!("stopping device failed: {e:?}");
        }
    };

    ctrl.run_target(tgt_init, q_handler, device_fn)
        .context("ublk device failed")?;
    Ok(ctrl)
}

/// Turns off the kernel's writeback throttling (wbt) for the device
/// (SPEC.md §2). It slows buffered writes down when reads are slower than
/// 2 ms, to keep writes from queueing ahead of reads; here writes complete in
/// RAM and reads are slow because of S3, so it only gets in the way. Set
/// manually, it stays off when udev switches the I/O scheduler.
fn disable_wbt(bdev: &str) {
    let Some(name) = bdev.strip_prefix("/dev/") else {
        return;
    };
    let path = format!("/sys/block/{name}/queue/wbt_lat_usec");
    match write_sysfs(&path, "0") {
        Ok(()) => debug!("{path}: writeback throttling disabled"),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            info!("{path} is missing: the kernel has no writeback throttling to disable")
        }
        Err(e) => warn!("cannot disable writeback throttling in {path}: {e}"),
    }
}

/// Writes a sysfs attribute. Like `std::fs::write`, but opens it without
/// `O_CREAT`: sysfs cannot create files and answers `EACCES` to `O_CREAT` on
/// a missing one, while a plain open tells it apart as `NotFound`.
pub fn write_sysfs(
    path: impl AsRef<std::path::Path>,
    value: impl AsRef<[u8]>,
) -> std::io::Result<()> {
    use std::io::Write;
    std::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(path)?
        .write_all(value.as_ref())
}

/// Raises the largest request the kernel sends to the device to the zone
/// size (SPEC.md §2): by default the kernel caps it below the hardware limit.
/// btrfs mounted afterwards sizes its data appends by it, so a smaller cap
/// would cut files into extents of that size. Large reads are split into
/// parallel GETs by the daemon instead (SPEC.md §6.2).
fn set_max_request(bdev: &str, bytes: u64) {
    let Some(name) = bdev.strip_prefix("/dev/") else {
        return;
    };
    let path = format!("/sys/block/{name}/queue/max_sectors_kb");
    match write_sysfs(&path, (bytes >> 10).to_string()) {
        Ok(()) => info!("{path}: requests up to {bytes} bytes"),
        Err(e) => warn!("cannot set request size in {path}: {e}"),
    }
}

/// How often the daemon looks for its filesystem being mounted. Judgement
/// call: until then btrfs reads ahead its own 4 MiB and reclaims as it does
/// by default, which a second costs next to nothing; the check is a
/// directory listing.
const FS_TUNE_POLL: Duration = Duration::from_secs(1);

/// Tunes a btrfs filesystem on the device `name` whenever it gets mounted —
/// by `mount` or by fstab alike: the readahead window
/// (`/sys/fs/btrfs/<fsid>/bdi/read_ahead_kb`, SPEC.md §6.1), when given, and
/// periodic reclaim when `reclaim` (not on a read-only disk; SPEC.md §11).
/// Both belong to the mounted filesystem, not
/// to the device, so they cannot be set before. Each mount is set once:
/// values the admin changes afterwards stay.
fn keep_fs_tuned(name: &str, readahead: Option<u64>, reclaim: bool) {
    let mut done: std::collections::HashSet<std::path::PathBuf> = Default::default();
    loop {
        let mounted: Vec<std::path::PathBuf> = std::fs::read_dir("/sys/fs/btrfs")
            .into_iter()
            .flatten()
            .flatten()
            .map(|e| e.path())
            .filter(|fs| fs.join("devices").join(name).exists())
            .collect();
        done.retain(|fs| mounted.contains(fs));
        for fs in mounted {
            if !done.insert(fs.clone()) {
                continue;
            }
            if let Some(bytes) = readahead {
                let path = fs.join("bdi/read_ahead_kb");
                match write_sysfs(&path, (bytes >> 10).to_string()) {
                    Ok(()) => info!("{}: btrfs readahead set to {bytes} bytes", path.display()),
                    Err(e) => warn!("cannot set btrfs readahead in {}: {e}", path.display()),
                }
            }
            if reclaim {
                enable_reclaim(&fs);
            }
        }
        std::thread::sleep(FS_TUNE_POLL);
    }
}

/// Turns on periodic reclaim for data and metadata (SPEC.md §11): btrfs
/// looks for block groups to reclaim periodically, among those not written
/// to since its previous look, not only when space is freed. Dynamic reclaim
/// stays off (user's decision): it waits until fewer than 10 zones are left.
fn enable_reclaim(fs: &std::path::Path) {
    for kind in ["data", "metadata"] {
        let path = fs.join("allocation").join(kind).join("periodic_reclaim");
        match write_sysfs(&path, "1") {
            Ok(()) => info!("{}: enabled", path.display()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => info!(
                "{} is missing: the kernel has no periodic reclaim (6.12+)",
                path.display()
            ),
            Err(e) => warn!("cannot enable periodic reclaim in {}: {e}", path.display()),
        }
    }
}

/// Tells systemd (Type=notify) about state changes; no-op outside systemd.
pub fn sd_notify(state: &str) {
    use std::os::linux::net::SocketAddrExt;
    use std::os::unix::net::{SocketAddr, UnixDatagram};

    let Some(path) = std::env::var_os("NOTIFY_SOCKET") else {
        return;
    };
    let path = path.to_string_lossy();
    let addr = match path.strip_prefix('@') {
        Some(name) => SocketAddr::from_abstract_name(name.as_bytes()),
        None => SocketAddr::from_pathname(path.as_ref()),
    };
    let sent = addr.and_then(|a| UnixDatagram::unbound()?.send_to_addr(state.as_bytes(), &a));
    if let Err(e) = sent {
        warn!("cannot notify systemd: {e}");
    }
}

/// Removes the stopped device.
pub fn delete(ctrl: &UblkCtrl) {
    if let Err(e) = ctrl.del_dev() {
        warn!("removing device failed: {e:?}");
    }
}

/// Serves one queue: a task per tag on libublk's per-thread runtime, which
/// logs task errors and ignores the normal stop (`QueueIsDown`).
fn run_queue(qid: u16, dev: &Arc<UblkDev>, ctx: &Arc<Ctx>) {
    let ctx = ctx.clone();
    let res = UblkRuntime::run_io_tasks(dev, qid, move |q, tag| {
        let ctx = ctx.clone();
        async move { io_task(&q, tag, &ctx).await }
    });
    if let Err(e) = res {
        error!("queue {qid} failed: {e:?}");
    }
    debug!("queue {qid} stopped");
}

async fn io_task(q: &UblkQueue, tag: u16, ctx: &Ctx) -> Result<(), UblkError> {
    let efd = Arc::new(
        new_eventfd().map_err(|e| UblkError::OtherError(-e.raw_os_error().unwrap_or(libc::EIO)))?,
    );
    q.submit_io_prep_cmd(tag, BufDesc::ZonedAppendLba(0), 0, None)
        .await?;
    loop {
        let (res, lba) = match handle_io(q, tag, ctx, &efd).await {
            Ok(r) => r,
            Err(e) => {
                let iod = q.get_iod(tag);
                debug!(
                    "op {} at sector {} ({} sectors) failed: {e}",
                    iod.op_flags & 0xff,
                    iod.start_sector,
                    iod.nr_sectors
                );
                match e {
                    ZoneError::ReadOnly => (-libc::EROFS, 0),
                    ZoneError::Invalid(_) => (-libc::EIO, 0),
                    ZoneError::Store(_) => {
                        error!("I/O error: {e}");
                        STATS.req_errors.inc();
                        STATS.err_io.record(&e);
                        (-libc::EIO, 0)
                    }
                }
            }
        };
        q.submit_io_commit_cmd(tag, BufDesc::ZonedAppendLba(lba), res)
            .await?;
    }
}

/// Returns (result, zone append LBA in sectors).
async fn handle_io(q: &UblkQueue, tag: u16, ctx: &Ctx, efd: &Arc<OwnedFd>) -> ZResult<(i32, u64)> {
    let iod = *q.get_iod(tag);
    let op = iod.op_flags & 0xff;
    let offset = iod.start_sector << SECTOR_SHIFT;
    let len = (iod.nr_sectors as u64) << SECTOR_SHIFT;
    let cdev = q.dev().tgt.fds[0];
    let copy_pos = UblkIOCtx::ublk_user_copy_pos(q.get_qid(), tag, 0);
    let engine = &ctx.engine;
    let _in_flight = STATS.requests.enter();
    trace!(
        target: REQUESTS_TARGET,
        "op={op} zone={} offset={offset} len={len} flags={:#x}",
        engine.locate(offset).map_or(-1, |(zi, _)| zi as i64),
        iod.op_flags & !0xff
    );

    match op {
        sys::UBLK_IO_OP_READ => {
            STATS.req_read.inc();
            STATS.req_read_bytes.add(len);
            STATS.req_read_size.record(len);
            if iod.op_flags & sys::UBLK_IO_F_META != 0 {
                engine.note_meta(offset);
            }
            let pieces = engine.read_plan(offset, len)?;
            let bufs = if pieces.iter().any(|p| matches!(p, Piece::Store { .. })) {
                let e = engine.clone();
                on_tokio(ctx, efd, async move { e.fetch(pieces).await }).await??
            } else {
                // RAM and zeros only: completes without awaiting anything.
                engine.fetch(pieces).await?
            };
            copy_to_request(cdev, &bufs, copy_pos, len)?;
            Ok((len as i32, 0))
        }
        sys::UBLK_IO_OP_WRITE | sys::UBLK_IO_OP_ZONE_APPEND => {
            if op == sys::UBLK_IO_OP_ZONE_APPEND {
                STATS.req_append.inc();
                STATS.req_append_bytes.add(len);
                STATS.req_append_size.record(len);
            } else {
                STATS.req_write.inc();
                STATS.req_write_bytes.add(len);
                STATS.req_write_size.record(len);
            }
            // Room first, then the copy: a request waiting for room holds no
            // data (SPEC.md §5).
            if !engine.try_reserve(len) {
                let e = engine.clone();
                let _waiting = STATS.writes_waiting.enter();
                on_tokio(ctx, efd, async move { e.reserve(len).await }).await?;
            }
            let append = op == sys::UBLK_IO_OP_ZONE_APPEND;
            // btrfs writes metadata with plain WRITE and flags it META (SPEC.md §5).
            let meta = !append || iod.op_flags & sys::UBLK_IO_F_META != 0;
            let at = copy_from_request(cdev, copy_pos, len)
                .and_then(|data| engine.write_reserved(offset, data, append, meta))
                .inspect_err(|_| engine.unreserve(len))?;
            if iod.op_flags & sys::UBLK_IO_F_FUA != 0 {
                STATS.req_fua.inc();
                let (zi, _) = engine.locate(at)?;
                let e = engine.clone();
                on_tokio(ctx, efd, async move { e.flush_zone(zi, Reason::Fua).await }).await??;
            }
            Ok((len as i32, if append { at >> SECTOR_SHIFT } else { 0 }))
        }
        sys::UBLK_IO_OP_FLUSH => {
            STATS.req_flush.inc();
            let e = engine.clone();
            on_tokio(ctx, efd, async move { e.flush_all(Reason::Flush).await }).await??;
            Ok((0, 0))
        }
        sys::UBLK_IO_OP_REPORT_ZONES => {
            STATS.req_report.inc();
            let n = report_zones(engine, cdev, copy_pos, offset, iod.nr_sectors as usize)?;
            Ok((n as i32, 0))
        }
        sys::UBLK_IO_OP_ZONE_OPEN => {
            STATS.req_zone_mgmt.inc();
            engine.open_zone(offset).map(|_| (0, 0))
        }
        sys::UBLK_IO_OP_ZONE_CLOSE => {
            STATS.req_zone_mgmt.inc();
            engine.close_zone(offset).map(|_| (0, 0))
        }
        sys::UBLK_IO_OP_ZONE_FINISH => {
            STATS.req_zone_mgmt.inc();
            let e = engine.clone();
            on_tokio(ctx, efd, async move { e.finish_zone(offset).await }).await??;
            Ok((0, 0))
        }
        sys::UBLK_IO_OP_ZONE_RESET => {
            STATS.req_zone_reset.inc();
            let e = engine.clone();
            on_tokio(ctx, efd, async move { e.reset_zone(offset).await }).await??;
            Ok((0, 0))
        }
        sys::UBLK_IO_OP_ZONE_RESET_ALL => {
            STATS.req_zone_reset.inc();
            let e = engine.clone();
            on_tokio(ctx, efd, async move { e.reset_all().await }).await??;
            Ok((0, 0))
        }
        _ => Ok((-libc::EOPNOTSUPP, 0)),
    }
}

/// Runs `fut` on the tokio runtime and waits for it from the queue thread.
async fn on_tokio<T, F>(ctx: &Ctx, efd: &Arc<OwnedFd>, fut: F) -> ZResult<T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    /// Signals the eventfd when dropped, i.e. also if the task panics.
    struct Signal(Arc<OwnedFd>);
    impl Drop for Signal {
        fn drop(&mut self) {
            let one: u64 = 1;
            // SAFETY: writes 8 bytes from a valid u64 to an eventfd we keep open.
            unsafe { libc::write(self.0.as_raw_fd(), (&one as *const u64).cast(), 8) };
        }
    }

    let slot = Arc::new(Mutex::new(None));
    let (task_slot, signal) = (slot.clone(), Signal(efd.clone()));
    ctx.rt.spawn(async move {
        let _signal = signal;
        let value = fut.await;
        *task_slot.lock().unwrap() = Some(value);
    });

    let mut counter = [0u8; 8];
    loop {
        let sqe = opcode::Read::new(types::Fd(efd.as_raw_fd()), counter.as_mut_ptr(), 8).build();
        // SAFETY: `counter` lives until the read completes: the op is awaited
        // right here.
        let res = match unsafe { ops::submit_sqe(sqe) } {
            Ok(op) => op.await,
            Err(e) => return Err(ZoneError::Invalid(format!("eventfd read failed: {e:?}"))),
        };
        if res == -libc::EINTR {
            continue;
        }
        if res < 0 {
            return Err(ZoneError::Invalid(format!("eventfd read failed: {res}")));
        }
        break;
    }
    let value = slot.lock().unwrap().take();
    value.ok_or_else(|| ZoneError::Invalid("store task panicked".into()))
}

fn new_eventfd() -> std::io::Result<OwnedFd> {
    // SAFETY: plain syscall; the returned descriptor is owned by OwnedFd.
    let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC) };
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: fd is a fresh descriptor not owned by anything else.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// Reads the write payload of a request from /dev/ublkcN.
fn copy_from_request(cdev: RawFd, pos: u64, len: u64) -> ZResult<Bytes> {
    let mut buf = Vec::<u8>::with_capacity(len as usize);
    // SAFETY: the buffer has `len` bytes of capacity; only the bytes the kernel
    // reported as copied are exposed through set_len.
    let n = unsafe {
        libc::pread(
            cdev,
            buf.as_mut_ptr().cast(),
            len as usize,
            pos as libc::off_t,
        )
    };
    if n != len as isize {
        return Err(ZoneError::Invalid(format!(
            "user copy from request failed: {n} of {len} bytes, {}",
            std::io::Error::last_os_error()
        )));
    }
    unsafe { buf.set_len(len as usize) };
    Ok(Bytes::from(buf))
}

/// Writes read data into the request through /dev/ublkcN.
fn copy_to_request(cdev: RawFd, bufs: &[Bytes], pos: u64, len: u64) -> ZResult<()> {
    let n = pwritev_all(cdev, bufs, pos)
        .map_err(|e| ZoneError::Invalid(format!("user copy to request failed: {e}")))?;
    if n != len {
        return Err(ZoneError::Invalid(format!(
            "user copy to request: {n} of {len} bytes"
        )));
    }
    Ok(())
}

/// Most iovecs one `pwritev` accepts: `UIO_MAXIOV` in `linux/uio.h`, also
/// `getconf IOV_MAX` (not exported by the libc crate for Linux).
const MAX_IOVECS: usize = 1024;

/// Writes all buffers at `pos`, in batches of at most `MAX_IOVECS`: a request
/// as large as a zone may be served from thousands of small RAM chunks.
fn pwritev_all(fd: RawFd, bufs: &[Bytes], pos: u64) -> std::io::Result<u64> {
    let mut written = 0u64;
    for batch in bufs.chunks(MAX_IOVECS) {
        let iov: Vec<libc::iovec> = batch
            .iter()
            .map(|b| libc::iovec {
                iov_base: b.as_ptr() as *mut _,
                iov_len: b.len(),
            })
            .collect();
        let want: u64 = batch.iter().map(|b| b.len() as u64).sum();
        // SAFETY: iovecs point into live Bytes borrowed for the duration of the call.
        let n = unsafe {
            libc::pwritev(
                fd,
                iov.as_ptr(),
                iov.len() as i32,
                (pos + written) as libc::off_t,
            )
        };
        if n < 0 {
            return Err(std::io::Error::last_os_error());
        }
        written += n as u64;
        if n as u64 != want {
            break;
        }
    }
    Ok(written)
}

fn report_zones(engine: &Engine, cdev: RawFd, pos: u64, offset: u64, max: usize) -> ZResult<usize> {
    let (first, _) = engine.locate(offset)?;
    let zones: Vec<sys::blk_zone> = engine
        .report(first, max)
        .into_iter()
        .map(|z| {
            // SAFETY: blk_zone is a plain C struct; all-zero is a valid value.
            let mut b: sys::blk_zone = unsafe { std::mem::zeroed() };
            b.start = z.start >> SECTOR_SHIFT;
            b.len = z.len >> SECTOR_SHIFT;
            b.capacity = z.capacity >> SECTOR_SHIFT;
            b.wp = z.wp >> SECTOR_SHIFT;
            b.type_ = sys::BLK_ZONE_TYPE_SEQWRITE_REQ as u8;
            b.cond = match z.cond {
                Cond::Empty => sys::BLK_ZONE_COND_EMPTY,
                Cond::ImpOpen => sys::BLK_ZONE_COND_IMP_OPEN,
                Cond::ExpOpen => sys::BLK_ZONE_COND_EXP_OPEN,
                Cond::Closed => sys::BLK_ZONE_COND_CLOSED,
                Cond::Full => sys::BLK_ZONE_COND_FULL,
            } as u8;
            b
        })
        .collect();
    let bytes = std::mem::size_of_val(zones.as_slice());
    // SAFETY: writes the initialized zone array to the request buffer.
    let n = unsafe { libc::pwrite(cdev, zones.as_ptr().cast(), bytes, pos as libc::off_t) };
    if n != bytes as isize {
        return Err(ZoneError::Invalid(format!(
            "user copy of zone report failed: {n} of {bytes} bytes, {}",
            std::io::Error::last_os_error()
        )));
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::net::UnixDatagram;

    #[test]
    fn sysfs_writes_never_create_files() {
        let dir = std::env::temp_dir().join(format!("btrs3fs-sysfs-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let missing = dir.join("wbt_lat_usec");
        let err = write_sysfs(&missing, "0").unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
        assert!(!missing.exists());
        std::fs::write(&missing, "2000").unwrap();
        write_sysfs(&missing, "0").unwrap();
        assert_eq!(std::fs::read_to_string(&missing).unwrap(), "0");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn pwritev_more_than_iov_max() {
        use std::io::Read;
        use std::os::fd::AsRawFd;
        let path = std::env::temp_dir().join(format!("btrs3fs-pwritev-{}", std::process::id()));
        let file = std::fs::File::options()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let bufs: Vec<Bytes> = (0..3000u32)
            .map(|i| Bytes::from(vec![(i % 251) as u8; 16]))
            .collect();
        assert_eq!(
            pwritev_all(file.as_raw_fd(), &bufs, 100).unwrap(),
            3000 * 16
        );
        let mut data = Vec::new();
        std::fs::File::open(&path)
            .unwrap()
            .read_to_end(&mut data)
            .unwrap();
        std::fs::remove_file(&path).unwrap();
        assert_eq!(data.len(), 100 + 3000 * 16);
        assert_eq!(&data[100 + 2999 * 16..], &[(2999 % 251) as u8; 16]);
    }

    #[test]
    fn notifies_systemd_socket() {
        let dir = std::env::temp_dir().join(format!("btrs3fs-notify-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("notify.sock");
        let _ = std::fs::remove_file(&path);
        let sock = UnixDatagram::bind(&path).unwrap();
        // SAFETY: no other test reads or writes NOTIFY_SOCKET.
        unsafe { std::env::set_var("NOTIFY_SOCKET", &path) };
        sd_notify("READY=1");
        unsafe { std::env::remove_var("NOTIFY_SOCKET") };
        let mut buf = [0u8; 64];
        let n = sock.recv(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"READY=1");
        std::fs::remove_dir_all(&dir).unwrap();
    }
}

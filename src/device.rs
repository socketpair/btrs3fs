//! ublk zoned block device on top of the zone engine.
//!
//! Each ublk queue runs in its own thread with a local executor driven by the
//! queue's io_uring. Requests that only touch RAM are served in place; requests
//! that need the store (FLUSH, FUA, reads of flushed data, resets, waiting for
//! buffer room) run on the tokio runtime. The queue thread sleeps in io_uring,
//! so a tokio completion wakes it through a per-tag eventfd that the IO task
//! reads with an io_uring READ (SPEC.md §9).

use crate::config::{BLOCK_SIZE, DiskConfig};
use crate::zones::{Cond, Engine, Piece, ZResult, ZoneError};
use anyhow::{Context, bail};
use bytes::Bytes;
use io_uring::{opcode, types};
use libublk::ctrl::{UblkCtrl, UblkCtrlBuilder};
use libublk::io::{BufDesc, UblkDev, UblkIOCtx, UblkQueue};
use libublk::{UblkError, UblkFlags, sys, wait_and_handle_io_events};
use log::{debug, error, info, warn};
use std::future::Future;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::runtime::Handle;

/// Largest single request; also the zone append limit.
const IO_BUF_BYTES: u32 = 512 << 10;
const SECTOR_SHIFT: u32 = 9;
const BLOCK_SHIFT: u8 = BLOCK_SIZE.trailing_zeros() as u8;

pub struct DeviceOptions {
    pub dev_id: i32,
    pub nr_queues: u16,
    pub queue_depth: u16,
}

struct Ctx {
    engine: Arc<Engine>,
    rt: Handle,
}

/// Creates the device and serves it until SIGTERM/SIGINT/SIGHUP or until the
/// device is removed externally. Returns after all queues stopped; the caller
/// flushes remaining data and then calls `delete()`.
pub fn serve(cfg: &DiskConfig, engine: Arc<Engine>, rt: Handle, opts: &DeviceOptions) -> anyhow::Result<UblkCtrl> {
    let features = UblkCtrl::get_features().context("ublk driver is not available")?;
    let needed = (sys::UBLK_F_USER_COPY | sys::UBLK_F_ZONED) as u64;
    if features & needed != needed {
        bail!("ublk driver lacks zoned/user-copy support (features {features:#x})");
    }

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
        .io_buf_bytes(IO_BUF_BYTES)
        .ctrl_flags(needed)
        .dev_flags(UblkFlags::UBLK_DEV_F_ADD_DEV)
        .build()
        .context("cannot create ublk device")?;

    let disk_size = cfg.disk_size;
    let zone_sectors = (cfg.zone_size >> SECTOR_SHIFT) as u32;
    let max_open = cfg.effective_limit(cfg.max_open_zones);
    let max_active = cfg.effective_limit(cfg.max_active_zones);
    let depth = opts.queue_depth;
    let tgt_init = move |dev: &mut UblkDev| {
        dev.set_default_params(disk_size);
        let p = &mut dev.tgt.params;
        p.types |= sys::UBLK_PARAM_TYPE_ZONED;
        p.basic.attrs = sys::UBLK_ATTR_VOLATILE_CACHE | sys::UBLK_ATTR_FUA;
        p.basic.logical_bs_shift = BLOCK_SHIFT;
        p.basic.physical_bs_shift = BLOCK_SHIFT;
        p.basic.io_min_shift = BLOCK_SHIFT;
        p.basic.io_opt_shift = BLOCK_SHIFT;
        p.basic.chunk_sectors = zone_sectors;
        p.zoned = sys::ublk_param_zoned {
            max_open_zones: max_open,
            max_active_zones: max_active,
            max_zone_append_sectors: zone_sectors.min(p.basic.max_sectors),
            ..Default::default()
        };
        // Each tag may have its ublk command and an eventfd read in flight.
        dev.tgt.sq_depth = 2 * depth;
        dev.tgt.cq_depth = 2 * depth;
        Ok(())
    };

    let live_queues = Arc::new(AtomicUsize::new(opts.nr_queues as usize));
    let ctx = Arc::new(Ctx { engine, rt });
    let q_live = live_queues.clone();
    let q_handler = move |qid: u16, dev: &UblkDev| {
        run_queue(qid, dev, &ctx);
        q_live.fetch_sub(1, Ordering::SeqCst);
    };

    let device_fn = move |ctrl: &UblkCtrl| {
        info!(
            "device {} is ready ({} queues, depth {})",
            ctrl.get_bdev_path(),
            ctrl.dev_info().nr_hw_queues,
            ctrl.dev_info().queue_depth
        );
        loop {
            if let Some(sig) = signals.pending().next() {
                info!("signal {sig} received, stopping device");
                break;
            }
            if live_queues.load(Ordering::SeqCst) == 0 {
                warn!("all queues stopped, device was removed externally");
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        if let Err(e) = ctrl.kill_dev() {
            warn!("stopping device failed: {e:?}");
        }
    };

    ctrl.run_target(tgt_init, q_handler, device_fn)
        .context("ublk device failed")?;
    Ok(ctrl)
}

/// Removes the stopped device.
pub fn delete(ctrl: &UblkCtrl) {
    if let Err(e) = ctrl.del_dev() {
        warn!("removing device failed: {e:?}");
    }
}

fn run_queue(qid: u16, dev: &UblkDev, ctx: &Arc<Ctx>) {
    let q = match UblkQueue::new(qid, dev) {
        Ok(q) => Rc::new(q),
        Err(e) => {
            error!("queue {qid}: setup failed: {e:?}");
            return;
        }
    };
    let exe = Rc::new(smol::LocalExecutor::new());
    let tasks: Vec<_> = (0..dev.dev_info.queue_depth)
        .map(|tag| {
            let (q, ctx) = (q.clone(), ctx.clone());
            exe.spawn(async move {
                match io_task(&q, tag, &ctx).await {
                    Ok(()) | Err(UblkError::QueueIsDown) => {}
                    Err(e) => error!("queue {qid} tag {tag}: {e:?}"),
                }
            })
        })
        .collect();

    let exe2 = exe.clone();
    smol::block_on(exe.run(async {
        let run_ops = || while exe2.try_tick() {};
        let done = || tasks.iter().all(|t| t.is_finished());
        if let Err(e) = wait_and_handle_io_events(&q, Some(20), run_ops, done).await {
            error!("queue {qid}: event loop failed: {e:?}");
        }
    }));
    debug!("queue {qid} stopped");
}

async fn io_task(q: &UblkQueue<'_>, tag: u16, ctx: &Ctx) -> Result<(), UblkError> {
    let efd = Arc::new(new_eventfd().map_err(|e| UblkError::OtherError(-e.raw_os_error().unwrap_or(libc::EIO)))?);
    q.submit_io_prep_cmd(tag, BufDesc::ZonedAppendLba(0), 0, None).await?;
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
                if !matches!(e, ZoneError::Invalid(_) | ZoneError::OpenLimit | ZoneError::ActiveLimit) {
                    error!("I/O error: {e}");
                }
                (-e.errno(), 0)
            }
        };
        q.submit_io_commit_cmd(tag, BufDesc::ZonedAppendLba(lba), res).await?;
    }
}

/// Returns (result, zone append LBA in sectors).
async fn handle_io(q: &UblkQueue<'_>, tag: u16, ctx: &Ctx, efd: &Arc<OwnedFd>) -> ZResult<(i32, u64)> {
    let iod = *q.get_iod(tag);
    let op = iod.op_flags & 0xff;
    let offset = iod.start_sector << SECTOR_SHIFT;
    let len = (iod.nr_sectors as u64) << SECTOR_SHIFT;
    let cdev = q.dev.tgt.fds[0];
    let copy_pos = UblkIOCtx::ublk_user_copy_pos(q.get_qid(), tag, 0);
    let engine = &ctx.engine;

    match op {
        sys::UBLK_IO_OP_READ => {
            let pieces = engine.read_plan(offset, len)?;
            let bufs = if pieces.iter().any(|p| matches!(p, Piece::Store { .. })) {
                let e = engine.clone();
                on_tokio(q, ctx, efd, async move { e.fetch(pieces).await }).await??
            } else {
                // RAM and zeros only: completes without awaiting anything.
                engine.fetch(pieces).await?
            };
            copy_to_request(cdev, &bufs, copy_pos, len)?;
            Ok((len as i32, 0))
        }
        sys::UBLK_IO_OP_WRITE | sys::UBLK_IO_OP_ZONE_APPEND => {
            let data = copy_from_request(cdev, copy_pos, len)?;
            if engine.over_hard_limit() {
                let e = engine.clone();
                on_tokio(q, ctx, efd, async move { e.wait_for_room().await }).await?;
            }
            let append = op == sys::UBLK_IO_OP_ZONE_APPEND;
            let at = engine.write(offset, data, append)?;
            if iod.op_flags & sys::UBLK_IO_F_FUA != 0 {
                let (zi, _) = engine.locate(at)?;
                let e = engine.clone();
                on_tokio(q, ctx, efd, async move { e.flush_zone(zi).await }).await??;
            }
            Ok((len as i32, if append { at >> SECTOR_SHIFT } else { 0 }))
        }
        sys::UBLK_IO_OP_FLUSH => {
            let e = engine.clone();
            on_tokio(q, ctx, efd, async move { e.flush_all().await }).await??;
            Ok((0, 0))
        }
        sys::UBLK_IO_OP_REPORT_ZONES => {
            let n = report_zones(engine, cdev, copy_pos, offset, iod.nr_sectors as usize)?;
            Ok((n as i32, 0))
        }
        sys::UBLK_IO_OP_ZONE_OPEN => engine.open_zone(offset).map(|_| (0, 0)),
        sys::UBLK_IO_OP_ZONE_CLOSE => engine.close_zone(offset).map(|_| (0, 0)),
        sys::UBLK_IO_OP_ZONE_FINISH => engine.finish_zone(offset).map(|_| (0, 0)),
        sys::UBLK_IO_OP_ZONE_RESET => {
            let e = engine.clone();
            on_tokio(q, ctx, efd, async move { e.reset_zone(offset).await }).await??;
            Ok((0, 0))
        }
        sys::UBLK_IO_OP_ZONE_RESET_ALL => {
            let e = engine.clone();
            on_tokio(q, ctx, efd, async move { e.reset_all().await }).await??;
            Ok((0, 0))
        }
        _ => Ok((-libc::EOPNOTSUPP, 0)),
    }
}

/// Runs `fut` on the tokio runtime and waits for it from the queue thread.
async fn on_tokio<T, F>(q: &UblkQueue<'_>, ctx: &Ctx, efd: &Arc<OwnedFd>, fut: F) -> ZResult<T>
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
        let res = q.ublk_submit_sqe(sqe).await;
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
    let n = unsafe { libc::pread(cdev, buf.as_mut_ptr().cast(), len as usize, pos as libc::off_t) };
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
    let iov: Vec<libc::iovec> = bufs
        .iter()
        .map(|b| libc::iovec { iov_base: b.as_ptr() as *mut _, iov_len: b.len() })
        .collect();
    // SAFETY: iovecs point into live Bytes borrowed for the duration of the call.
    let n = unsafe { libc::pwritev(cdev, iov.as_ptr(), iov.len() as i32, pos as libc::off_t) };
    if n != len as isize {
        return Err(ZoneError::Invalid(format!(
            "user copy to request failed: {n} of {len} bytes, {}",
            std::io::Error::last_os_error()
        )));
    }
    Ok(())
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

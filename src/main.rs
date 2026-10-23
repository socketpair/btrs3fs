mod config;
mod device;
mod store;
mod zones;

use anyhow::{Context, bail};
use clap::{Args, Parser, Subcommand};
use config::{DiskConfig, parse_size};
use log::{error, info, warn};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use store::{Layout, MemStore, Store};
use zones::Engine;

/// Zoned ublk block device backed by S3, one object per zone.
#[derive(Parser)]
#[command(version)]
struct Cli {
    /// Log filter: error, warn, info, debug, trace (env_logger syntax).
    #[arg(long, global = true, default_value = "info")]
    log_level: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Create a new disk: write <prefix>/disk.json.
    Create(CreateArgs),
    /// Serve a disk as /dev/ublkb<dev-id> (needs root).
    Run(RunArgs),
    /// Stop a running device: flush data and remove it (needs root).
    Detach(DetachArgs),
    /// Delete all objects of a disk.
    Destroy(DestroyArgs),
}

#[derive(Args)]
struct DiskArgs {
    /// Credentials file (JSON, see SPEC.md §3.3).
    #[arg(long)]
    creds: PathBuf,
    /// Object key prefix of the disk inside the bucket.
    #[arg(long)]
    prefix: String,
}

#[derive(Args)]
struct CreateArgs {
    #[command(flatten)]
    disk: DiskArgs,
    /// Disk size, e.g. 10G.
    #[arg(long, value_parser = parse_size)]
    size: u64,
    #[arg(long, value_parser = parse_size, default_value = "32M")]
    zone_size: u64,
    #[arg(long)]
    max_open_zones: Option<u32>,
    #[arg(long)]
    max_active_zones: Option<u32>,
    #[arg(long, value_parser = parse_size)]
    dirty_limit: Option<u64>,
    #[arg(long)]
    retry_timeout_seconds: Option<u64>,
    #[arg(long)]
    retry_initial_delay_ms: Option<u64>,
    #[arg(long)]
    retry_backoff_factor: Option<f64>,
    #[arg(long)]
    queues: Option<u16>,
    #[arg(long)]
    queue_depth: Option<u16>,
}

#[derive(Args)]
struct RunArgs {
    #[arg(long, required_unless_present = "memory")]
    creds: Option<PathBuf>,
    #[arg(long, required_unless_present = "memory")]
    prefix: Option<String>,
    /// Keep everything in RAM instead of S3 (testing only; data is lost on exit).
    #[arg(long, conflicts_with_all = ["creds", "prefix"], requires = "size")]
    memory: bool,
    /// Disk size for --memory.
    #[arg(long, value_parser = parse_size)]
    size: Option<u64>,
    /// Zone size for --memory.
    #[arg(long, value_parser = parse_size, default_value = "32M")]
    zone_size: u64,
    /// ublk device id: the device is /dev/ublkb<dev-id>.
    #[arg(long, default_value_t = 0)]
    dev_id: i32,
    /// Override nr_queues from disk.json.
    #[arg(long)]
    queues: Option<u16>,
}

#[derive(Args)]
struct DetachArgs {
    #[arg(long, default_value_t = 0)]
    dev_id: i32,
}

#[derive(Args)]
struct DestroyArgs {
    #[command(flatten)]
    disk: DiskArgs,
    /// Confirm deletion of all disk data.
    #[arg(long)]
    yes: bool,
}

fn main() {
    let cli = Cli::parse();
    env_logger::Builder::new()
        .parse_filters(&cli.log_level)
        .format_target(false)
        .init();

    let res = match cli.command {
        Command::Create(_) | Command::Destroy(_) => Err(anyhow::anyhow!("S3 support is not implemented yet")),
        Command::Run(a) => cmd_run(a),
        Command::Detach(a) => cmd_detach(a),
    };
    match res {
        Ok(code) => std::process::exit(code),
        Err(e) => {
            error!("{e:#}");
            std::process::exit(1);
        }
    }
}

fn runtime() -> anyhow::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("btrs3fs-store")
        .build()
        .context("cannot start async runtime")
}

fn cmd_run(a: RunArgs) -> anyhow::Result<i32> {
    let rt = runtime()?;
    let (mut cfg, layout, store) = if a.memory {
        let cfg = DiskConfig::new(a.size.unwrap(), a.zone_size);
        cfg.validate()?;
        (cfg, Layout::new("memory")?, Store::Mem(MemStore::default()))
    } else {
        bail!("S3 support is not implemented yet");
    };
    if let Some(q) = a.queues {
        cfg.nr_queues = q;
    }
    cfg.validate()?;
    info!(
        "disk: {} bytes, {} zones of {} bytes",
        cfg.disk_size,
        cfg.nr_zones(),
        cfg.zone_size
    );

    let engine = Arc::new(rt.block_on(Engine::open(&cfg, layout, Arc::new(store)))?);
    rt.spawn(engine.clone().background_flusher());

    let opts = device::DeviceOptions {
        dev_id: a.dev_id,
        nr_queues: cfg.nr_queues,
        queue_depth: cfg.queue_depth,
    };
    let ctrl = device::serve(&cfg, engine.clone(), rt.handle().clone(), &opts)?;

    // Queues are stopped: no new writes can arrive, push out what is left.
    let code = final_flush(&rt, &engine);
    device::delete(&ctrl);
    info!("device removed");
    Ok(code)
}

fn final_flush(rt: &tokio::runtime::Runtime, engine: &Engine) -> i32 {
    let pending = engine.dirty_bytes();
    if pending == 0 {
        return 0;
    }
    info!("flushing {pending} buffered bytes");
    match rt.block_on(engine.flush_all()) {
        Ok(()) => {
            info!("all data flushed");
            0
        }
        Err(e) => {
            error!("final flush failed: {e}");
            for (zone, bytes) in engine.unflushed() {
                error!("data lost: zone {zone}, {bytes} bytes not stored");
            }
            2
        }
    }
}

fn cmd_detach(a: DetachArgs) -> anyhow::Result<i32> {
    use std::os::unix::fs::OpenOptionsExt;

    let ctrl = libublk::ctrl::UblkCtrl::new_simple(a.dev_id)
        .with_context(|| format!("ublk device {} not found", a.dev_id))?;
    let bdev = ctrl.get_bdev_path();
    match std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_EXCL)
        .open(&bdev)
    {
        Err(e) if e.raw_os_error() == Some(libc::EBUSY) => {
            bail!("{bdev} is in use (mounted?), unmount it first")
        }
        Err(e) => warn!("cannot check whether {bdev} is in use: {e}"),
        Ok(_) => {}
    }

    let pid = ctrl.dev_info().ublksrv_pid;
    if pid <= 0 {
        bail!("device {} has no server process", a.dev_id);
    }
    // SAFETY: plain syscall.
    if unsafe { libc::kill(pid, libc::SIGTERM) } != 0 {
        bail!("cannot signal server process {pid}: {}", std::io::Error::last_os_error());
    }
    info!("sent SIGTERM to server process {pid}, waiting for it to flush and exit");
    let started = Instant::now();
    let mut reported = Instant::now();
    // SAFETY: signal 0 only checks that the process exists.
    while unsafe { libc::kill(pid, 0) } == 0 {
        std::thread::sleep(Duration::from_millis(100));
        if reported.elapsed() >= Duration::from_secs(5) {
            info!("still waiting for process {pid} ({}s)", started.elapsed().as_secs());
            reported = Instant::now();
        }
    }
    info!("device {} detached", a.dev_id);
    Ok(0)
}

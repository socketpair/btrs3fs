mod config;
mod device;
mod s3;
mod store;
mod zones;

use anyhow::{Context, bail};
use bytes::Bytes;
use clap::{Args, Parser, Subcommand};
use config::{DiskConfig, parse_size};
use futures_util::StreamExt;
use log::{error, info, warn};
use s3::{RetryPolicy, S3Store, Transport};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use store::{Body, Layout, MemStore, Store};
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
    #[command(flatten)]
    transport: Transport,
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
    #[command(flatten)]
    transport: Transport,
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
        Command::Create(a) => cmd_create(a),
        Command::Destroy(a) => cmd_destroy(a),
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
        let creds = config::Creds::load(a.creds.as_ref().unwrap())?;
        let layout = Layout::new(a.prefix.as_ref().unwrap())?;
        let defaults = DiskConfig::new(0, DiskConfig::DEFAULT_ZONE_SIZE);
        let s3 = S3Store::new(&creds, &a.transport, RetryPolicy::from_config(&defaults))?;
        let cfg = rt
            .block_on(load_config(&s3, &layout))?
            .with_context(|| format!("disk {} not found, create it first", layout.config_key()))?;
        let s3 = s3.with_retry(RetryPolicy::from_config(&cfg));
        (cfg, layout, Store::S3(s3))
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
        bail!(
            "cannot signal server process {pid}: {}",
            std::io::Error::last_os_error()
        );
    }
    info!("sent SIGTERM to server process {pid}, waiting for it to flush and exit");
    let started = Instant::now();
    let mut reported = Instant::now();
    // SAFETY: signal 0 only checks that the process exists.
    while unsafe { libc::kill(pid, 0) } == 0 {
        std::thread::sleep(Duration::from_millis(100));
        if reported.elapsed() >= Duration::from_secs(5) {
            info!(
                "still waiting for process {pid} ({}s)",
                started.elapsed().as_secs()
            );
            reported = Instant::now();
        }
    }
    info!("device {} detached", a.dev_id);
    Ok(0)
}

async fn load_config(s3: &S3Store, layout: &Layout) -> anyhow::Result<Option<DiskConfig>> {
    let key = layout.config_key();
    match s3.get(&key).await {
        Ok(data) => Ok(Some(
            DiskConfig::from_json(&data).with_context(|| format!("invalid {key}"))?,
        )),
        Err(store::StoreError::NotFound(_)) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

fn cmd_create(a: CreateArgs) -> anyhow::Result<i32> {
    let mut cfg = DiskConfig::new(a.size, a.zone_size);
    if let Some(v) = a.max_open_zones {
        cfg.max_open_zones = v;
    }
    if let Some(v) = a.max_active_zones {
        cfg.max_active_zones = v;
    }
    if let Some(v) = a.dirty_limit {
        cfg.dirty_limit_bytes = v;
    }
    if let Some(v) = a.retry_timeout_seconds {
        cfg.retry_timeout_seconds = v;
    }
    if let Some(v) = a.retry_initial_delay_ms {
        cfg.retry_initial_delay_ms = v;
    }
    if let Some(v) = a.retry_backoff_factor {
        cfg.retry_backoff_factor = v;
    }
    if let Some(v) = a.queues {
        cfg.nr_queues = v;
    }
    if let Some(v) = a.queue_depth {
        cfg.queue_depth = v;
    }
    cfg.validate()?;

    let creds = config::Creds::load(&a.disk.creds)?;
    let layout = Layout::new(&a.disk.prefix)?;
    let s3 = S3Store::new(&creds, &a.disk.transport, RetryPolicy::from_config(&cfg))?;
    runtime()?.block_on(async {
        let existing = s3.list(&layout.dir()).await?;
        if !existing.is_empty() {
            bail!(
                "prefix {} already holds {} objects; destroy the old disk first",
                layout.dir(),
                existing.len()
            );
        }
        let json = Bytes::from(cfg.to_json());
        s3.put(&layout.config_key(), Body::new(vec![json])).await?;
        Ok(())
    })?;
    info!(
        "created {}: {} bytes, {} zones of {} bytes",
        layout.config_key(),
        cfg.disk_size,
        cfg.nr_zones(),
        cfg.zone_size
    );
    Ok(0)
}

fn cmd_destroy(a: DestroyArgs) -> anyhow::Result<i32> {
    if !a.yes {
        bail!("this deletes all data of the disk; pass --yes to confirm");
    }
    let creds = config::Creds::load(&a.disk.creds)?;
    let layout = Layout::new(&a.disk.prefix)?;
    let defaults = DiskConfig::new(0, DiskConfig::DEFAULT_ZONE_SIZE);
    let s3 = S3Store::new(
        &creds,
        &a.disk.transport,
        RetryPolicy::from_config(&defaults),
    )?;
    let deleted = runtime()?.block_on(async {
        let config_key = layout.config_key();
        let keys: Vec<String> = s3
            .list(&layout.dir())
            .await?
            .into_iter()
            .map(|o| o.key)
            .filter(|k| *k != config_key)
            .collect();
        let n = keys.len();
        let results: Vec<_> = futures_util::stream::iter(keys)
            .map(|k| {
                let s3 = &s3;
                async move { s3.delete(&k).await }
            })
            .buffer_unordered(16)
            .collect()
            .await;
        for r in results {
            r?;
        }
        // The config goes last so an interrupted destroy can be repeated.
        s3.delete(&config_key).await?;
        anyhow::Ok(n)
    })?;
    info!("destroyed {}: {deleted} zone objects deleted", layout.dir());
    Ok(0)
}

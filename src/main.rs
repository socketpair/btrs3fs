mod autobalance;
mod autotune;
mod bench;
mod btrfs;
mod btrfs_zone;
mod config;
mod device;
mod estimate;
mod ident;
mod limiter;
mod linkconf;
mod logging;
mod measure;
mod mountinfo;
mod readahead;
mod regulator;
mod s3;
mod stats;
mod store;
mod tuning;
mod units;
mod zones;

use anyhow::{Context, bail, ensure};
use bytes::Bytes;
use clap::{Args, Parser, Subcommand};
use config::DiskConfig;
use log::{error, info, warn};
use s3::{RetryPolicy, S3Store, Transport};
use std::fs::File;
use std::io::{Read, Write};
use std::os::fd::{FromRawFd, OwnedFd};
use std::path::{Path, PathBuf};
use std::process::Command as Process;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use store::{Backend, Body, Layout, MemStore, Store};
use tokio::runtime::Runtime;
use tuning::Manual;
use units::parse_size;
use zones::Engine;

/// btrfs mount options always used by `mount`: user decision (SPEC.md §9.2);
/// commit=300 makes btrfs commit (and FLUSH) every 5 minutes, so writes merge
/// into fewer, larger S3 appends. nosuid,nodev: the image comes from the
/// bucket, so whoever can write there must not get setuid files or device
/// nodes onto this host.
const MOUNT_OPTIONS: &str = "nosuid,nodev,noatime,lazytime,commit=300";

/// How long `mount` waits for udev to create /dev/ublkbN after the device
/// server reported it ready. Judgement call: udev needs milliseconds.
const DEVICE_NODE_TIMEOUT: Duration = Duration::from_secs(10);

/// Polling interval while waiting for the device node.
const DEVICE_NODE_POLL: Duration = Duration::from_millis(50);

/// Polling interval while waiting for a device server process to exit, and
/// how often the wait is reported in the log. Judgement calls.
const EXIT_POLL: Duration = Duration::from_millis(100);
const EXIT_REPORT_EVERY: Duration = Duration::from_secs(5);

/// Zoned ublk block device backed by S3, one object per zone.
#[derive(Parser)]
#[command(version = env!("BTRS3FS_VERSION"))]
struct Cli {
    /// Log filter: error, warn, info, debug, trace (env_logger syntax).
    #[arg(long, global = true, default_value = "info,libublk=warn")]
    log_level: String,

    /// Also log to stderr (colored on a terminal); logs always go to journald.
    #[arg(long, global = true)]
    log_to_stderr: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Create a new disk: write <prefix>/disk.yaml and format it with zoned btrfs (needs root).
    Create(CreateArgs),
    /// Start the device in the background and mount its btrfs (needs root).
    Mount(MountArgs),
    /// Unmount a btrfs mounted by `mount` and stop its device (needs root).
    Umount(UmountArgs),
    /// Serve a disk as /dev/ublkb<dev-id> in the foreground, without mounting (needs root).
    Run(RunArgs),
    /// Stop a running device that is not mounted (needs root).
    Detach(DetachArgs),
    /// Delete all objects of a disk.
    Destroy(DestroyArgs),
    /// Measure the link to S3 from this host and print a --link-config file for mount/run.
    Measure(DiskArgs),
    /// Benchmark reads of objects grown by small appends (PATCH, segments) at the S3 level.
    S3bench(BenchArgs),
}

#[derive(Args)]
struct BenchArgs {
    #[command(flatten)]
    disk: DiskArgs,
    /// Object sizes in appends, comma-separated.
    #[arg(long, value_delimiter = ',', default_values_t = [256u64, 1024])]
    appends: Vec<u64>,
    /// Bytes per append (one write + fsync of btrfs).
    #[arg(long, value_parser = parse_size, default_value = "4KiB")]
    piece: u64,
    /// Reads of each object.
    #[arg(long, default_value_t = 3)]
    repeat: usize,
    /// GETs at once for reads in parts and of segments.
    #[arg(long, default_value_t = 8)]
    parallel: usize,
    /// Part size of reads in parts (the daemon's read window).
    #[arg(long, value_parser = parse_size, default_value = "128KiB")]
    part: u64,
}

#[derive(Args)]
struct DiskArgs {
    /// Credentials file (YAML or JSON, see SPEC.md §3.2).
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
    /// Disk size with a unit, e.g. 10GiB or 100GB.
    #[arg(long, value_parser = parse_size)]
    size: u64,
    /// Zone size, a power of two up to 32MiB [default: 16MiB].
    #[arg(long, value_parser = parse_size)]
    zone_size: Option<u64>,
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
struct MountArgs {
    #[command(flatten)]
    disk: DiskArgs,
    /// Directory to mount the filesystem on.
    mountpoint: PathBuf,
    /// Extra mount options, appended to nosuid,nodev,noatime,lazytime,commit=300.
    #[arg(short = 'o', long)]
    options: Option<String>,
    /// Serve the disk read-only and mount it ro,rescue=nologreplay: works with
    /// credentials that may only read the bucket.
    #[arg(short = 'r', long)]
    read_only: bool,
    /// ublk device id; -1 lets the kernel choose.
    #[arg(long, default_value_t = -1, allow_hyphen_values = true)]
    dev_id: i32,
    /// Override nr_queues from disk.yaml.
    #[arg(long)]
    queues: Option<u16>,
    #[command(flatten)]
    stats: StatsArgs,
    #[command(flatten)]
    link: LinkArgs,
}

/// Link settings (SPEC.md §6.1): per run, not in disk.yaml.
#[derive(Args, Clone)]
struct LinkArgs {
    /// YAML file with manual link values (see examples/link.yaml); a key that
    /// is null or absent is auto: estimated from the daemon's own traffic.
    #[arg(long, value_name = "FILE")]
    link_config: Option<PathBuf>,
}

impl LinkArgs {
    /// The manual values; all auto without a file.
    fn manual(&self) -> anyhow::Result<Manual> {
        match &self.link_config {
            Some(path) => linkconf::load(path),
            None => Ok(Manual::default()),
        }
    }
}

/// Statistics web page of the device server.
#[derive(Args, Clone, Copy)]
struct StatsArgs {
    /// Port of the statistics page on 127.0.0.1 [default: 9470 + device number].
    #[arg(long)]
    stats_port: Option<u16>,
    /// Do not serve the statistics page.
    #[arg(long, conflicts_with = "stats_port")]
    no_stats: bool,
}

#[derive(Args)]
struct UmountArgs {
    mountpoint: PathBuf,
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
    /// Zone size for --memory [default: 16MiB].
    #[arg(long, value_parser = parse_size)]
    zone_size: Option<u64>,
    /// ublk device id: the device is /dev/ublkb<dev-id>.
    #[arg(long, default_value_t = 0)]
    dev_id: i32,
    /// Override nr_queues from disk.yaml.
    #[arg(long)]
    queues: Option<u16>,
    /// Serve the disk read-only (mount it with -o ro,rescue=nologreplay):
    /// works with credentials that may only read the bucket.
    #[arg(short = 'r', long, conflicts_with = "memory")]
    read_only: bool,
    #[command(flatten)]
    stats: StatsArgs,
    #[command(flatten)]
    link: LinkArgs,
    #[command(flatten)]
    transport: Transport,
}

#[derive(Args)]
struct DetachArgs {
    #[arg(long, default_value_t = 0)]
    dev_id: i32,
    /// Kill the server with SIGKILL at once, even if the device is mounted
    /// (like pulling the power cord: unflushed data is lost, the filesystem
    /// gets I/O errors). For hung servers and crash tests.
    #[arg(long)]
    force: bool,
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
    logging::init(&cli.log_level, cli.log_to_stderr);

    let res = match cli.command {
        Command::Create(a) => cmd_create(a),
        Command::Mount(a) => cmd_mount(a, cli.log_to_stderr),
        Command::Umount(a) => cmd_umount(a),
        Command::Run(a) => cmd_run(a),
        Command::Detach(a) if a.force => kill_server(a.dev_id),
        Command::Detach(a) => detach(a.dev_id),
        Command::Destroy(a) => cmd_destroy(a),
        Command::Measure(a) => cmd_measure(a),
        Command::S3bench(a) => cmd_s3bench(a),
    };
    let code = match res {
        Ok(code) => code,
        Err(e) => {
            error!("{e:#}");
            if !cli.log_to_stderr {
                eprintln!("btrs3fs: {e:#}");
            }
            1
        }
    };
    log::logger().flush();
    std::process::exit(code);
}

fn runtime() -> anyhow::Result<Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("btrs3fs-store")
        .build()
        .context("cannot start async runtime")
}

fn ensure_root(what: &str) -> anyhow::Result<()> {
    // SAFETY: plain syscall without arguments.
    ensure!(unsafe { libc::geteuid() } == 0, "{what} needs root");
    Ok(())
}

/// The disk's `disk.yaml`; for a disk created before it, its `disk.json`,
/// moved to `disk.yaml` when `migrate` (served writable; SPEC.md §3.2).
async fn load_config(
    s3: &S3Store,
    layout: &Layout,
    migrate: bool,
) -> anyhow::Result<Option<DiskConfig>> {
    for key in [layout.config_key(), layout.legacy_config_key()] {
        let data = match s3.get(&key).await {
            Ok(data) => data,
            Err(store::StoreError::NotFound(_)) => continue,
            Err(e) => return Err(e.into()),
        };
        let cfg = DiskConfig::parse(&data).with_context(|| format!("invalid {key}"))?;
        if migrate && key == layout.legacy_config_key() {
            // The new one first: an interruption leaves both, never neither.
            let yaml = Bytes::from(cfg.to_yaml());
            s3.put(&layout.config_key(), Body::new(vec![yaml])).await?;
            s3.delete(&key).await?;
            info!("{key} moved to {}", layout.config_key());
        }
        return Ok(Some(cfg));
    }
    Ok(None)
}

/// Loads the disk config and returns an S3 store using the disk's retry
/// policy; a writable disk gets its old `disk.json` moved to `disk.yaml`.
fn open_s3_disk(
    rt: &Runtime,
    creds: &Path,
    prefix: &str,
    transport: &Transport,
    read_only: bool,
) -> anyhow::Result<(DiskConfig, Layout, Store)> {
    let creds = config::Creds::load(creds)?;
    let layout = Layout::new(prefix)?;
    let s3 = S3Store::new(&creds, transport, RetryPolicy::default())?;
    let cfg = rt
        .block_on(load_config(&s3, &layout, !read_only))?
        .with_context(|| format!("disk {} not found, create it first", layout.config_key()))?;
    let s3 = s3
        .with_retry(RetryPolicy::from_config(&cfg))
        .with_append(cfg.append);
    Ok((cfg, layout, Store::s3(s3)))
}

/// Restores the zones and serves the device until it stops.
fn serve_disk(
    rt: &Runtime,
    mut cfg: DiskConfig,
    layout: Layout,
    store: Store,
    serve: ServeOptions,
    on_ready: device::OnReady,
) -> anyhow::Result<(Arc<Engine>, libublk::ctrl::UblkCtrl)> {
    let ServeOptions {
        dev_id,
        queues,
        stats: stats_opt,
        link,
        read_only,
    } = serve;
    if let Some(q) = queues {
        cfg.nr_queues = q;
    }
    cfg.validate()?;
    let serial = match store.backend() {
        Backend::S3(s) => Some(format!("{}/{}", s.bucket(), layout.prefix())),
        Backend::Mem(_) => None,
    };
    // Where the disk lives, for the statistics page.
    let (endpoint, bucket) = match store.backend() {
        Backend::S3(s) => (Some(s.endpoint().to_string()), Some(s.bucket().to_string())),
        Backend::Mem(_) => (None, None),
    };
    let prefix = layout.prefix().to_string();
    if let Some(m) = &link {
        tuning::validate(m, cfg.zone_size)?;
    }
    info!(
        "disk {}: {}, {} zones of {}",
        layout.dir(),
        units::size(cfg.disk_size),
        cfg.nr_zones(),
        units::size(cfg.zone_size)
    );
    let disk = layout.dir();
    let store = if read_only {
        info!("serving the disk read-only");
        store.read_only()
    } else {
        store
    };
    let engine = Arc::new(rt.block_on(Engine::open(&cfg, layout, Arc::new(store)))?);
    // Without link settings (the temporary device of `create`) nothing is
    // tuned: no read-ahead, default uploads, whole-object GETs.
    let fs_target = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let tune = match link {
        Some(manual) => {
            let t = autotune::Autotune::new(
                engine.clone(),
                manual,
                cfg.zone_size,
                fs_target.clone(),
                read_only,
                rt.handle().clone(),
            )?;
            rt.spawn(t.clone().run());
            Some(t)
        }
        None => None,
    };
    // A read-only disk has nothing to upload, concatenate or mark.
    if !read_only {
        rt.spawn(engine.clone().background_flusher());
        rt.spawn(engine.clone().compactor());
    }
    let opts = device::DeviceOptions {
        dev_id,
        nr_queues: cfg.nr_queues,
        queue_depth: cfg.queue_depth,
        fs_readahead: tune.as_ref().map(|_| fs_target.clone()),
        serial,
        read_only,
    };

    // The statistics page starts once the device number is known.
    let started = Instant::now();
    let stats_engine = engine.clone();
    let act_rt = rt.handle().clone();
    let (append, copy_min_part) = (cfg.append, cfg.copy_min_part);
    stats::set_zone_size(cfg.zone_size);
    let auto_engine = engine.clone();
    let zone_size = cfg.zone_size;
    let on_ready: device::OnReady = Box::new(move |bdev| {
        let name = bdev.trim_start_matches("/dev/").to_string();
        let fs = Arc::new(btrfs::Btrfs::new(&name));
        // A device of `run` or `mount` (not the temporary one of `create`)
        // that may write: empty block groups go by themselves (SPEC.md §9.3).
        if stats_opt.is_some() && !read_only {
            let auto = fs.auto();
            let auto_fs = fs.clone();
            let e = auto_engine.clone();
            let spawned = std::thread::Builder::new()
                .name("btrs3fs-autobalance".into())
                .spawn(move || auto.run(name, auto_fs, e, zone_size));
            if let Err(e) = spawned {
                warn!("cannot start the automatic removal of empty block groups: {e}");
            }
        }
        let port = stats_opt.filter(|o| !o.no_stats).and_then(|o| {
            o.stats_port.or_else(|| {
                ublk_dev_id(bdev).and_then(|id| stats::BASE_PORT.checked_add(id.try_into().ok()?))
            })
        });
        if let Some(port) = port {
            let device = bdev.to_string();
            let json_fs = fs.clone();
            let snap_fs = fs.clone();
            let act_engine = stats_engine.clone();
            let map_engine = stats_engine.clone();
            let snapshot = move || {
                let (zones, dirty_zones) = stats_engine.zone_summary();
                stats::STATS.snapshot(stats::DiskState {
                    disk: disk.clone(),
                    device: device.clone(),
                    append,
                    copy_min_part,
                    endpoint: endpoint.clone(),
                    bucket: bucket.clone(),
                    prefix: prefix.clone(),
                    mountpoint: snap_fs.mountpoint().map(|p| p.display().to_string()),
                    uptime_secs: started.elapsed().as_secs(),
                    dirty_bytes: stats_engine.dirty_bytes(),
                    dirty_limit: stats_engine.dirty_limit(),
                    dirty_hard_limit: stats_engine.hard_limit(),
                    dirty_zones,
                    zones,
                    uploads_limit: stats_engine.uploads_limit() as u64,
                    get_part: stats_engine.get_part(),
                    reads_limit: stats_engine.reads_limit() as u64,
                    disk_read_only: read_only,
                })
            };
            let act_rt = act_rt.clone();
            let act_tune = tune.clone();
            let json_tune = tune.clone();
            let action =
                move |name: &str, query: &str| match name {
                    // Link settings and measurements: a read-only disk only reads.
                    "link" => Some(match &act_tune {
                        Some(t) => link_settings(t, query),
                        None => serde_json::json!({ "error": "no link tuning on this device" })
                            .to_string(),
                    }),
                    "measure" => Some(match act_tune.as_ref().map(|t| t.measure()) {
                        Some(Ok(())) => r#"{"started":true}"#.to_string(),
                        Some(Err(e)) => serde_json::json!({ "error": e }).to_string(),
                        None => serde_json::json!({ "error": "no link tuning on this device" })
                            .to_string(),
                    }),
                    // Every other action changes the disk.
                    _ if read_only => Some(
                        serde_json::json!({ "error": "the disk is served read-only" }).to_string(),
                    ),
                    "balance" | "syncfs" => Some(match fs.start(name) {
                        Ok(()) => r#"{"started":true}"#.to_string(),
                        Err(e) => serde_json::json!({ "error": e }).to_string(),
                    }),
                    "autobalance-pause" | "autobalance-resume" => {
                        fs.auto().set_paused(name == "autobalance-pause");
                        Some(r#"{"ok":true}"#.to_string())
                    }
                    "compact" => Some(format!("{{\"queued\":{}}}", act_engine.compact_all())),
                    "flush" => {
                        let pending = act_engine.unflushed();
                        let bytes: u64 = pending.iter().map(|&(_, b)| b).sum();
                        info!(
                            "statistics page: uploading {} buffered in {} zones",
                            units::size(bytes),
                            pending.len()
                        );
                        let e = act_engine.clone();
                        act_rt.spawn(async move {
                            if let Err(e) = e.flush_all(stats::Reason::Manual).await {
                                warn!("upload asked from the statistics page failed: {e}");
                            }
                        });
                        Some(format!("{{\"zones\":{},\"bytes\":{bytes}}}", pending.len()))
                    }
                    _ => None,
                };
            let json = move |name: &str| match name {
                "stats.json" => serde_json::to_string(&snapshot()).ok(),
                "btrfs.json" => serde_json::to_string(&json_fs.info()).ok(),
                "zones.json" => serde_json::to_string(&map_engine.zone_map()).ok(),
                "link.json" => json_tune
                    .as_ref()
                    .and_then(|t| serde_json::to_string(&t.snapshot()).ok()),
                // One zone in full, on a click on the zone map; its block
                // level part alone, refreshed while the zone is shown.
                name => {
                    let name = name.strip_prefix("zone-")?;
                    if let Some(zi) = name.strip_suffix("-block.json") {
                        let block = map_engine.zone_detail(zi.parse().ok()?)?;
                        return serde_json::to_string(&block).ok();
                    }
                    let zi: usize = name.strip_suffix(".json")?.parse().ok()?;
                    let block = map_engine.zone_detail(zi)?;
                    let fs = json_fs.zone(block.start, block.size);
                    serde_json::to_string(&serde_json::json!({ "block": block, "fs": fs })).ok()
                }
            };
            match stats::serve_http(port, json, action) {
                Ok(()) => {
                    let _ = STATS_PORT.set(port);
                }
                Err(e) => warn!("statistics page is not available: {e:#}"),
            }
        }
        on_ready(bdev)
    });
    let ctrl = device::serve(&cfg, engine.clone(), rt.handle().clone(), &opts, on_ready)?;
    Ok((engine, ctrl))
}

/// `POST /link?<key>=<value|auto>&…`: every pair checked first, then all
/// applied in order; nothing applies when one is refused.
fn link_settings(t: &autotune::Autotune, query: &str) -> String {
    let pairs = stats::query_pairs(query);
    if pairs.is_empty() {
        return serde_json::json!({ "error": "nothing to set" }).to_string();
    }
    if let Err(e) = t.set_all(&pairs) {
        return serde_json::json!({ "error": e }).to_string();
    }
    r#"{"ok":true}"#.to_string()
}

/// How `serve_disk` exposes the device.
struct ServeOptions {
    /// ublk device id; -1 lets the kernel choose.
    dev_id: i32,
    /// Overrides `nr_queues` from disk.yaml.
    queues: Option<u16>,
    /// `None` — no statistics page (temporary device of `create`).
    stats: Option<StatsArgs>,
    /// `None` — no tuning (temporary device of `create`): no read-ahead,
    /// default uploads, whole-object GETs, btrfs readahead untouched.
    link: Option<Manual>,
    /// Serve the disk read-only (SPEC.md §9.2).
    read_only: bool,
}

impl ServeOptions {
    /// A device of `run` or `mount`, tuned for the given link.
    fn device(
        dev_id: i32,
        queues: Option<u16>,
        stats: StatsArgs,
        manual: Manual,
        read_only: bool,
    ) -> Self {
        Self {
            dev_id,
            queues,
            stats: Some(stats),
            link: Some(manual),
            read_only,
        }
    }
}

/// `serve_disk` for `run` and `mount`: serves until stopped, then removes
/// the device.
fn serve_until_stopped(
    rt: &Runtime,
    cfg: DiskConfig,
    layout: Layout,
    store: Store,
    serve: ServeOptions,
    on_ready: device::OnReady,
) -> anyhow::Result<()> {
    let (engine, ctrl) = serve_disk(rt, cfg, layout, store, serve, on_ready)?;
    report_discarded(&engine);
    device::delete(&ctrl);
    info!("device removed");
    Ok(())
}

/// Port the statistics page actually listens on, once started.
static STATS_PORT: std::sync::OnceLock<u16> = std::sync::OnceLock::new();

/// `N` of `/dev/ublkbN`.
fn ublk_dev_id(bdev: &str) -> Option<u32> {
    bdev.strip_prefix("/dev/ublkb")?.parse().ok()
}

/// Data not covered by a FLUSH is not stored on shutdown (SPEC.md §5).
fn report_discarded(engine: &Engine) {
    let pending = engine.unflushed();
    if pending.is_empty() {
        return;
    }
    let bytes: u64 = pending.iter().map(|&(_, b)| b).sum();
    info!(
        "not storing {} in {} zones: they were never flushed from userspace (no fsync/FLUSH)",
        units::size(bytes),
        pending.len()
    );
}

fn cmd_run(a: RunArgs) -> anyhow::Result<i32> {
    let rt = runtime()?;
    let (cfg, layout, store) = if a.memory {
        let cfg = DiskConfig::new(
            a.size.unwrap(),
            a.zone_size.unwrap_or(DiskConfig::DEFAULT_ZONE_SIZE),
        );
        cfg.validate()?;
        (cfg, Layout::new("memory")?, Store::mem(MemStore::default()))
    } else {
        open_s3_disk(
            &rt,
            a.creds.as_ref().unwrap(),
            a.prefix.as_ref().unwrap(),
            &a.transport,
            a.read_only,
        )?
    };
    let on_ready: device::OnReady = Box::new(|_| {
        device::sd_notify("READY=1");
        true
    });
    let serve = ServeOptions::device(a.dev_id, a.queues, a.stats, a.link.manual()?, a.read_only);
    serve_until_stopped(&rt, cfg, layout, store, serve, on_ready)?;
    Ok(0)
}

/// Options of `mount`: ours, read-only ones for a read-only disk (SPEC.md
/// §9.2: a fsync log cannot be replayed on a read-only device), then `extra`.
fn mount_options(extra: Option<&str>, read_only: bool) -> String {
    let mut options = MOUNT_OPTIONS.to_string();
    if read_only {
        options.push_str(",ro,rescue=nologreplay");
    }
    if let Some(e) = extra.map(str::trim).filter(|e| !e.is_empty()) {
        options.push(',');
        options.push_str(e);
    }
    options
}

fn pipe() -> anyhow::Result<(File, File)> {
    let mut fds = [0; 2];
    // SAFETY: pipe2 fills two fresh descriptors that we take ownership of.
    if unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) } != 0 {
        bail!("pipe: {}", std::io::Error::last_os_error());
    }
    let (r, w) = unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) };
    Ok((File::from(r), File::from(w)))
}

fn cmd_mount(a: MountArgs, keep_stderr: bool) -> anyhow::Result<i32> {
    ensure_root("mount")?;
    let mountpoint = std::fs::canonicalize(&a.mountpoint)
        .with_context(|| format!("mount point {}", a.mountpoint.display()))?;
    ensure!(
        mountpoint.is_dir(),
        "{} is not a directory",
        mountpoint.display()
    );
    if let Some(source) = find_mount(&mountpoint)? {
        bail!("{} is already mounted ({source})", mountpoint.display());
    }
    let creds = std::fs::canonicalize(&a.disk.creds)
        .with_context(|| format!("credentials file {}", a.disk.creds.display()))?;
    let options = mount_options(a.options.as_deref(), a.read_only);
    // Read before the fork: a bad file fails here, with its message.
    let manual = a.link.manual()?;

    // Fork before any thread exists (tokio and ublk queues start in the child).
    let (mut status_rx, status_tx) = pipe()?;
    // SAFETY: the process is single-threaded here.
    let pid = unsafe { libc::fork() };
    if pid < 0 {
        bail!("fork: {}", std::io::Error::last_os_error());
    }
    if pid == 0 {
        drop(status_rx);
        let code = mount_daemon(&a, &creds, manual, status_tx, keep_stderr);
        log::logger().flush();
        // SAFETY: terminate the child without running the parent's exit handlers.
        unsafe { libc::_exit(code) };
    }
    drop(status_tx);

    let mut status = String::new();
    status_rx.read_to_string(&mut status).ok();
    let line = status.lines().next().unwrap_or_default();
    let (bdev, stats_port) = match (line.strip_prefix("READY "), line.strip_prefix("ERROR ")) {
        (Some(rest), _) => {
            let mut parts = rest.split(' ');
            let bdev = parts.next().unwrap_or_default().to_string();
            (bdev, parts.next().and_then(|p| p.parse::<u16>().ok()))
        }
        (_, Some(err)) => bail!("device server failed: {err}"),
        _ => bail!("device server exited unexpectedly, see `journalctl -t btrs3fs`"),
    };
    // From here on a failure must not leave the device server running.
    let mounted = wait_for_path(Path::new(&bdev), DEVICE_NODE_TIMEOUT).and_then(|()| {
        let status = Process::new("mount")
            .args(["-t", "btrfs", "-o", &options, &bdev])
            .arg(&mountpoint)
            .status()
            .context("cannot run mount")?;
        ensure!(
            status.success(),
            "mount of {bdev} on {} failed",
            mountpoint.display()
        );
        Ok(())
    });
    if let Err(e) = mounted {
        warn!("mounting {bdev} failed, stopping its server process {pid}");
        stop_process(pid);
        return Err(e);
    }
    info!(
        "{bdev} mounted on {} with {options}; server process {pid}",
        mountpoint.display()
    );
    if let Some(port) = stats_port {
        info!("statistics: http://127.0.0.1:{port}/");
    }
    Ok(0)
}

/// Background device server of `mount`. Reports `READY <bdev> <stats port|->` or `ERROR <msg>`
/// to the parent through `status`.
fn mount_daemon(
    a: &MountArgs,
    creds: &Path,
    manual: Manual,
    status: File,
    keep_stderr: bool,
) -> i32 {
    detach_from_terminal(keep_stderr);
    let status = Arc::new(Mutex::new(Some(status)));
    let res = (|| -> anyhow::Result<()> {
        let rt = runtime()?;
        let (cfg, layout, store) =
            open_s3_disk(&rt, creds, &a.disk.prefix, &a.disk.transport, a.read_only)?;
        let ready_status = status.clone();
        let on_ready: device::OnReady = Box::new(move |bdev| {
            if let Some(mut f) = ready_status.lock().unwrap().take() {
                let port = STATS_PORT.get().map_or("-".into(), |p| p.to_string());
                let _ = writeln!(f, "READY {bdev} {port}");
            }
            true
        });
        let serve = ServeOptions::device(a.dev_id, a.queues, a.stats, manual, a.read_only);
        serve_until_stopped(&rt, cfg, layout, store, serve, on_ready)
    })();
    match res {
        Ok(()) => 0,
        Err(e) => {
            error!("{e:#}");
            if let Some(mut f) = status.lock().unwrap().take() {
                let _ = writeln!(f, "ERROR {e:#}");
            }
            1
        }
    }
}

fn detach_from_terminal(keep_stderr: bool) {
    // SAFETY: plain syscalls on descriptors owned by this process.
    unsafe {
        libc::setsid();
        libc::chdir(c"/".as_ptr());
        let null = libc::open(c"/dev/null".as_ptr(), libc::O_RDWR);
        if null >= 0 {
            libc::dup2(null, 0);
            libc::dup2(null, 1);
            if !keep_stderr {
                libc::dup2(null, 2);
            }
            if null > 2 {
                libc::close(null);
            }
        }
    }
}

fn wait_for_path(path: &Path, timeout: Duration) -> anyhow::Result<()> {
    let started = Instant::now();
    while !path.exists() {
        ensure!(
            started.elapsed() < timeout,
            "{} did not appear",
            path.display()
        );
        std::thread::sleep(DEVICE_NODE_POLL);
    }
    Ok(())
}

/// Source of the top-most mount on `mountpoint`, from /proc/self/mountinfo.
fn find_mount(mountpoint: &Path) -> anyhow::Result<Option<String>> {
    Ok(mountinfo::mounts()?
        .into_iter()
        .rfind(|m| m.point == mountpoint)
        .map(|m| m.source))
}

fn cmd_umount(a: UmountArgs) -> anyhow::Result<i32> {
    ensure_root("umount")?;
    let mountpoint = std::fs::canonicalize(&a.mountpoint)
        .with_context(|| format!("mount point {}", a.mountpoint.display()))?;
    let source = find_mount(&mountpoint)?
        .with_context(|| format!("{} is not mounted", mountpoint.display()))?;
    let dev_id = ublk_dev_id(&source).map(|id| id as i32).with_context(|| {
        format!(
            "{} is mounted from {}, not from a btrs3fs device",
            mountpoint.display(),
            source
        )
    })?;
    let unmounted = Process::new("umount")
        .arg(&mountpoint)
        .status()
        .context("cannot run umount")?;
    ensure!(
        unmounted.success(),
        "umount of {} failed",
        mountpoint.display()
    );
    info!("unmounted {} ({source})", mountpoint.display());
    detach(dev_id)
}

/// Stops the server of an unmounted device and waits for it to exit.
fn detach(dev_id: i32) -> anyhow::Result<i32> {
    use std::os::unix::fs::OpenOptionsExt;

    let ctrl = libublk::ctrl::UblkCtrl::new_simple(dev_id)
        .with_context(|| format!("ublk device {dev_id} not found"))?;
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
    let server = live_server(dev_id, pid);
    if let Some(pid) = server {
        // SAFETY: plain syscall.
        if unsafe { libc::kill(pid, libc::SIGTERM) } != 0 {
            bail!(
                "cannot signal server process {pid}: {}",
                std::io::Error::last_os_error()
            );
        }
    } else {
        // The server crashed or was killed: the device is dead, only remove it.
        warn!("server process {pid} of device {dev_id} is gone, removing the device");
        ctrl.del_dev()
            .map_err(|e| anyhow::anyhow!("cannot remove device {dev_id}: {e:?}"))?;
        info!("device {dev_id} removed");
        return Ok(0);
    }
    info!("sent SIGTERM to server process {pid}, waiting for it to exit");
    wait_for_exit(pid);
    info!("device {dev_id} detached");
    Ok(0)
}

/// The server process of a device: `pid` as the kernel recorded it, if that
/// process still holds the device's `/dev/ublkcN` open. The recorded pid
/// outlives the server (a crashed or killed server leaves it behind) and may
/// belong to an unrelated process by now: such a process is never signalled.
fn live_server(dev_id: i32, pid: i32) -> Option<i32> {
    if pid <= 0 {
        return None;
    }
    let cdev = PathBuf::from(format!("/dev/ublkc{dev_id}"));
    let fds = std::fs::read_dir(format!("/proc/{pid}/fd")).ok()?;
    fds.flatten()
        .any(|fd| std::fs::read_link(fd.path()).is_ok_and(|target| target == cdev))
        .then_some(pid)
}

/// `detach --force`: SIGKILL the device server; the kernel then fails all I/O.
fn kill_server(dev_id: i32) -> anyhow::Result<i32> {
    let ctrl = libublk::ctrl::UblkCtrl::new_simple(dev_id)
        .with_context(|| format!("ublk device {dev_id} not found"))?;
    let Some(pid) = live_server(dev_id, ctrl.dev_info().ublksrv_pid) else {
        bail!("server of device {dev_id} is already dead; unmount the filesystem and run detach");
    };
    warn!("killing server process {pid} of device {dev_id} with SIGKILL");
    // SAFETY: plain syscall.
    if unsafe { libc::kill(pid, libc::SIGKILL) } != 0 {
        bail!(
            "cannot kill process {pid}: {}",
            std::io::Error::last_os_error()
        );
    }
    wait_for_exit(pid);
    info!(
        "server process {pid} killed; unmount the filesystem and run detach to remove the device"
    );
    Ok(0)
}

fn stop_process(pid: i32) {
    // SAFETY: plain syscall.
    if unsafe { libc::kill(pid, libc::SIGTERM) } == 0 {
        wait_for_exit(pid);
    }
}

fn wait_for_exit(pid: i32) {
    let started = Instant::now();
    let mut reported = Instant::now();
    // SAFETY: signal 0 only checks that the process exists.
    while unsafe { libc::kill(pid, 0) } == 0 {
        // Reap it if it is our child; ignore errors for foreign processes.
        unsafe { libc::waitpid(pid, std::ptr::null_mut(), libc::WNOHANG) };
        std::thread::sleep(EXIT_POLL);
        if reported.elapsed() >= EXIT_REPORT_EVERY {
            info!(
                "still waiting for process {pid} ({}s)",
                started.elapsed().as_secs()
            );
            reported = Instant::now();
        }
    }
}

fn cmd_create(a: CreateArgs) -> anyhow::Result<i32> {
    ensure_root("create (it formats the disk)")?;
    let mut cfg = DiskConfig::new(a.size, a.zone_size.unwrap_or(DiskConfig::DEFAULT_ZONE_SIZE));
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
    let rt = runtime()?;
    rt.block_on(async {
        // Deleting by prefix would reach into a disk nested under another.
        for parent in layout.ancestors() {
            let parent = Layout::new(parent)?;
            if load_config(&s3, &parent, false).await?.is_some() {
                bail!(
                    "{} is a disk: a disk cannot lie inside another disk's prefix",
                    parent.dir()
                );
            }
        }
        let listed = s3.list(&layout.dir()).await?;
        let (existing, leftovers) = existing_objects(&layout, &listed);
        if !existing.is_empty() {
            bail!(
                "prefix {} already holds {} objects; destroy the old disk first",
                layout.dir(),
                existing.len()
            );
        }
        if !leftovers.is_empty() {
            info!(
                "deleting {} objects of an interrupted link measurement",
                leftovers.len()
            );
            s3.delete_many(&leftovers).await?;
        }
        (cfg.append, cfg.copy_min_part) = s3.detect_append(&layout.dir()).await?;
        info!(
            "appending to zone objects with {:?}{}",
            cfg.append,
            if cfg.copy_min_part > 0 {
                format!(
                    "; concatenation reads pieces under {} MiB",
                    cfg.copy_min_part >> 20
                )
            } else {
                String::new()
            }
        );
        let yaml = Bytes::from(cfg.to_yaml());
        s3.put(&layout.config_key(), Body::new(vec![yaml])).await?;
        Ok(())
    })?;
    let s3 = s3.with_append(cfg.append);
    info!(
        "created {}: {}, {} zones of {}",
        layout.config_key(),
        units::size(cfg.disk_size),
        cfg.nr_zones(),
        units::size(cfg.zone_size)
    );

    if let Err(e) = format_disk(&cfg, &layout, &s3) {
        warn!("formatting failed, removing the new disk {}", layout.dir());
        if let Err(re) = destroy_prefix(&rt, &s3, &layout) {
            error!("cleanup of {} failed: {re:#}", layout.dir());
        }
        return Err(e);
    }
    info!("disk {} is ready", layout.dir());
    Ok(0)
}

/// Objects under a prefix `create` must not overwrite, and leftovers of an
/// interrupted `btrs3fs measure` there, which it deletes.
fn existing_objects(layout: &Layout, listed: &[store::ObjectInfo]) -> (Vec<String>, Vec<String>) {
    listed
        .iter()
        .map(|o| o.key.clone())
        .partition(|k| !layout.is_measure_key(k))
}

/// Brings up a temporary device, runs mkfs.btrfs on it and stores the result.
/// On a runtime of its own: when it returns, the engine's background tasks
/// (uploads, concatenation) are gone, so a cleanup after a failure lists
/// everything the disk will ever hold.
fn format_disk(cfg: &DiskConfig, layout: &Layout, s3: &S3Store) -> anyhow::Result<()> {
    let rt = runtime()?;
    let rt = &rt;
    let mkfs_result = Arc::new(Mutex::new(None));
    let result = mkfs_result.clone();
    let on_ready: device::OnReady = Box::new(move |bdev| {
        info!("formatting {bdev} with zoned btrfs");
        let res = Process::new("mkfs.btrfs")
            // No DUP anywhere (SPEC.md §1): system follows the metadata profile.
            .args(["-O", "zoned", "-d", "single", "-m", "single", bdev])
            .output()
            .context("cannot run mkfs.btrfs")
            .and_then(|out| {
                ensure!(
                    out.status.success(),
                    "mkfs.btrfs failed: {}",
                    String::from_utf8_lossy(&out.stderr).trim()
                );
                Ok(())
            });
        *result.lock().unwrap() = Some(res);
        false
    });
    let (engine, ctrl) = serve_disk(
        rt,
        cfg.clone(),
        layout.clone(),
        Store::s3(s3.clone()),
        ServeOptions {
            dev_id: -1,
            queues: None,
            stats: None,
            // mkfs reads next to nothing.
            link: None,
            read_only: false,
        },
        on_ready,
    )?;
    // Formatting is part of creating the disk: store it even without a FLUSH.
    let flushed = rt.block_on(engine.flush_all(stats::Reason::Explicit));
    device::delete(&ctrl);
    drop(engine);
    mkfs_result
        .lock()
        .unwrap()
        .take()
        .unwrap_or_else(|| Err(anyhow::anyhow!("device stopped before formatting")))?;
    flushed.context("storing the new filesystem failed")?;
    Ok(())
}

fn cmd_measure(a: DiskArgs) -> anyhow::Result<i32> {
    let creds = config::Creds::load(&a.creds)?;
    let s3 = S3Store::new(&creds, &a.transport, RetryPolicy::default())?;
    let layout = Layout::new(&a.prefix)?;
    let rt = runtime()?;
    // Reads only the disk's own objects; without a disk, writes only.
    // Only reads the disk: its config stays as it is.
    let cfg = rt.block_on(load_config(&s3, &layout, false))?;
    let reads = match &cfg {
        Some(cfg) => {
            let listed = rt.block_on(s3.list(&layout.dir()))?;
            measure::candidates_from_listing(
                &layout,
                &listed,
                cfg.append == config::AppendMode::Segments,
            )
        }
        None => Vec::new(),
    };
    eprintln!(
        "measuring the link to S3 (reading {} objects of the disk)…",
        reads.len()
    );
    let dir = measure::temp_dir(&layout);
    let plan = measure::Plan {
        reads,
        write_dir: Some(dir.clone()),
        budget: None,
    };
    let measured = rt.block_on(measure::run(&s3, plan));
    rt.block_on(measure::cleanup(&s3, &dir))?;
    let measured = measured?;
    print!("{}", measure::report(&measured));
    Ok(0)
}

fn cmd_s3bench(a: BenchArgs) -> anyhow::Result<i32> {
    ensure!(
        a.piece > 0 && a.part > 0 && a.parallel > 0 && a.repeat > 0 && !a.appends.contains(&0),
        "sizes, counts and parallelism must be positive"
    );
    let creds = config::Creds::load(&a.disk.creds)?;
    let s3 = S3Store::new(&creds, &a.disk.transport, RetryPolicy::default())?;
    let layout = Layout::new(&a.disk.prefix)?;
    let opts = bench::Options {
        pieces: a.appends,
        piece: a.piece,
        repeat: a.repeat,
        parallel: a.parallel,
        part: a.part,
    };
    runtime()?.block_on(bench::run(&s3, layout.prefix(), &opts))?;
    Ok(0)
}

fn cmd_destroy(a: DestroyArgs) -> anyhow::Result<i32> {
    if !a.yes {
        bail!("this deletes all data of the disk; pass --yes to confirm");
    }
    let creds = config::Creds::load(&a.disk.creds)?;
    let layout = Layout::new(&a.disk.prefix)?;
    let s3 = S3Store::new(&creds, &a.disk.transport, RetryPolicy::default())?;
    let rt = runtime()?;
    if rt.block_on(load_config(&s3, &layout, false))?.is_none() {
        bail!(
            "no disk at {}: {} not found",
            layout.dir(),
            layout.config_key()
        );
    }
    let deleted = destroy_prefix(&rt, &s3, &layout)?;
    info!("destroyed {}: {deleted} zone objects deleted", layout.dir());
    Ok(0)
}

/// Deletes the disk's zone objects, unfinished concatenations and config;
/// disks nested under its prefix stay.
fn destroy_prefix(rt: &Runtime, s3: &S3Store, layout: &Layout) -> anyhow::Result<usize> {
    rt.block_on(async {
        let keys: Vec<String> = s3
            .list(&layout.dir())
            .await?
            .into_iter()
            .map(|o| o.key)
            .filter(|k| layout.zone_of(k).is_some() || layout.is_measure_key(k))
            .collect();
        s3.abort_stale_concats(layout).await?;
        s3.delete_many(&keys).await?;
        // The config goes last so an interrupted destroy can be repeated.
        s3.delete(&layout.legacy_config_key()).await?;
        s3.delete(&layout.config_key()).await?;
        anyhow::Ok(keys.len())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn measurement_leftovers_do_not_block_create() {
        let layout = Layout::new("d").unwrap();
        let obj = |key: &str| store::ObjectInfo {
            key: key.into(),
            size: 1,
        };
        let listed = [obj("d/measure-1f/w-3"), obj("d/measure-1f/small")];
        let (blocking, leftovers) = existing_objects(&layout, &listed);
        assert!(blocking.is_empty());
        assert_eq!(leftovers, ["d/measure-1f/w-3", "d/measure-1f/small"]);
        let listed = [obj("d/measure-1f/w-3"), obj("d/0000000000000000")];
        assert_eq!(existing_objects(&layout, &listed).0, ["d/0000000000000000"]);
    }

    #[test]
    fn options() {
        assert_eq!(
            mount_options(None, false),
            "nosuid,nodev,noatime,lazytime,commit=300"
        );
        assert_eq!(
            mount_options(Some(" "), false),
            "nosuid,nodev,noatime,lazytime,commit=300"
        );
        assert_eq!(
            mount_options(Some("compress=zstd"), false),
            "nosuid,nodev,noatime,lazytime,commit=300,compress=zstd"
        );
        assert_eq!(
            mount_options(Some("subvol=a"), true),
            "nosuid,nodev,noatime,lazytime,commit=300,ro,rescue=nologreplay,subvol=a"
        );
    }
}

mod btrfs;
mod config;
mod device;
mod ident;
mod logging;
mod measure;
mod readahead;
mod s3;
mod stats;
mod store;
mod tuning;
mod zones;

use anyhow::{Context, bail, ensure};
use bytes::Bytes;
use clap::{Args, Parser, Subcommand};
use config::{DiskConfig, parse_size};
use log::{error, info, warn};
use readahead::ReadAhead;
use s3::{RetryPolicy, S3Store, Transport};
use std::fs::File;
use std::io::{Read, Write};
use std::os::fd::{FromRawFd, OwnedFd};
use std::path::{Path, PathBuf};
use std::process::Command as Process;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use store::{Body, Layout, MemStore, Store};
use tokio::runtime::Runtime;
use tuning::Link;
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
    /// Create a new disk: write <prefix>/disk.json and format it with zoned btrfs (needs root).
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
    /// Measure the link to S3 from this host and print the --s3-* flags for mount/run.
    Measure(DiskArgs),
}

#[derive(Args)]
struct DiskArgs {
    /// Credentials file (JSON, see SPEC.md §3.2).
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
    /// Zone size, a power of two up to 32M [default: 16M].
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
    /// ublk device id; -1 lets the kernel choose.
    #[arg(long, default_value_t = -1, allow_hyphen_values = true)]
    dev_id: i32,
    /// Override nr_queues from disk.json.
    #[arg(long)]
    queues: Option<u16>,
    #[command(flatten)]
    stats: StatsArgs,
    #[command(flatten)]
    link: LinkArgs,
}

/// The link to S3 as `btrs3fs measure` reports it (SPEC.md §6.1): the daemon
/// derives its read and upload settings from it. Per run, not in disk.json;
/// values not given are those measured in Yandex Cloud next to the bucket.
#[derive(Args, Clone, Copy)]
struct LinkArgs {
    /// Time to the first byte of a small GET, e.g. 36ms.
    #[arg(long, value_parser = tuning::parse_duration)]
    s3_latency: Option<Duration>,
    /// Rate of one GET after its first byte, per second, e.g. 55M.
    #[arg(long, value_parser = parse_size)]
    s3_read_stream: Option<u64>,
    /// Rate of all GETs together, per second.
    #[arg(long, value_parser = parse_size)]
    s3_read_total: Option<u64>,
    /// Rate of one PUT after its first byte, per second.
    #[arg(long, value_parser = parse_size)]
    s3_write_stream: Option<u64>,
    /// Rate of all PUTs together, per second.
    #[arg(long, value_parser = parse_size)]
    s3_write_total: Option<u64>,
    /// RAM for read-ahead windows; 0 disables read-ahead [default: 64M].
    #[arg(long, value_parser = parse_size, default_value_t = config::DEFAULT_READ_CACHE, hide_default_value = true)]
    read_cache: u64,
}

impl LinkArgs {
    /// The link given on the command line, Yandex Cloud values for the rest;
    /// says in the log what was not given.
    fn link(&self) -> Link {
        let d = Link::YANDEX_CLOUD;
        let missing: Vec<&str> = [
            ("--s3-latency", self.s3_latency.is_none()),
            ("--s3-read-stream", self.s3_read_stream.is_none()),
            ("--s3-read-total", self.s3_read_total.is_none()),
            ("--s3-write-stream", self.s3_write_stream.is_none()),
            ("--s3-write-total", self.s3_write_total.is_none()),
        ]
        .into_iter()
        .filter_map(|(flag, none)| none.then_some(flag))
        .collect();
        if !missing.is_empty() {
            warn!(
                "S3 link not given ({}): assuming values measured in Yandex Cloud; \
                 measure yours with `btrs3fs measure`",
                missing.join(", ")
            );
        }
        Link {
            latency: self.s3_latency.unwrap_or(d.latency),
            read_stream: self.s3_read_stream.unwrap_or(d.read_stream),
            read_total: self.s3_read_total.unwrap_or(d.read_total),
            write_stream: self.s3_write_stream.unwrap_or(d.write_stream),
            write_total: self.s3_write_total.unwrap_or(d.write_total),
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
    /// Zone size for --memory [default: 16M].
    #[arg(long, value_parser = parse_size)]
    zone_size: Option<u64>,
    /// ublk device id: the device is /dev/ublkb<dev-id>.
    #[arg(long, default_value_t = 0)]
    dev_id: i32,
    /// Override nr_queues from disk.json.
    #[arg(long)]
    queues: Option<u16>,
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

/// Loads `disk.json` and returns an S3 store using the disk's retry policy.
fn open_s3_disk(
    rt: &Runtime,
    creds: &Path,
    prefix: &str,
    transport: &Transport,
) -> anyhow::Result<(DiskConfig, Layout, Store)> {
    let creds = config::Creds::load(creds)?;
    let layout = Layout::new(prefix)?;
    let s3 = S3Store::new(&creds, transport, RetryPolicy::default())?;
    let cfg = rt
        .block_on(load_config(&s3, &layout))?
        .with_context(|| format!("disk {} not found, create it first", layout.config_key()))?;
    let s3 = s3
        .with_retry(RetryPolicy::from_config(&cfg))
        .with_append(cfg.append);
    Ok((cfg, layout, Store::S3(s3)))
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
        read_cache,
    } = serve;
    if let Some(q) = queues {
        cfg.nr_queues = q;
    }
    cfg.validate()?;
    let serial = match &store {
        Store::S3(s) => Some(format!("{}/{}", s.bucket(), layout.prefix())),
        Store::Mem(_) => None,
    };
    let tuning = match link {
        Some(l) => {
            l.validate()?;
            let t = l.tuning(cfg.zone_size);
            info!("S3 link: {l}; derived: {t}");
            Some(t)
        }
        None => None,
    };
    info!(
        "disk {}: {} bytes, {} zones of {} bytes",
        layout.dir(),
        cfg.disk_size,
        cfg.nr_zones(),
        cfg.zone_size
    );
    let disk = layout.dir();
    let engine = rt.block_on(Engine::open(&cfg, layout, Arc::new(store)))?;
    let read_ahead = tuning.map_or(ReadAhead::DISABLED, |t| ReadAhead {
        window: t.unit,
        cache: read_cache,
    });
    let mut engine = engine.with_read_ahead(read_ahead)?;
    if let Some(t) = tuning {
        engine = engine.with_uploads(t.uploads).with_get_part(t.unit);
    }
    let engine = Arc::new(engine);
    rt.spawn(engine.clone().background_flusher());
    rt.spawn(engine.clone().compactor());
    let opts = device::DeviceOptions {
        dev_id,
        nr_queues: cfg.nr_queues,
        queue_depth: cfg.queue_depth,
        fs_readahead: tuning.map(|t| t.fs_readahead),
        serial,
    };

    // The statistics page starts once the device number is known.
    let started = Instant::now();
    let stats_engine = engine.clone();
    let act_rt = rt.handle().clone();
    let append = cfg.append;
    stats::set_zone_size(cfg.zone_size);
    let on_ready: device::OnReady = Box::new(move |bdev| {
        let port = stats_opt.filter(|o| !o.no_stats).and_then(|o| {
            o.stats_port.or_else(|| {
                ublk_dev_id(bdev).and_then(|id| stats::BASE_PORT.checked_add(id.try_into().ok()?))
            })
        });
        if let Some(port) = port {
            let device = bdev.to_string();
            let fs = Arc::new(btrfs::Btrfs::new(device.trim_start_matches("/dev/")));
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
                    uptime_secs: started.elapsed().as_secs(),
                    dirty_bytes: stats_engine.dirty_bytes(),
                    dirty_limit: stats_engine.dirty_limit(),
                    dirty_hard_limit: stats_engine.hard_limit(),
                    dirty_zones,
                    zones,
                    uploads_limit: stats_engine.uploads_limit() as u64,
                    get_part: stats_engine.get_part(),
                    read_only: snap_fs.read_only(),
                })
            };
            let act_rt = act_rt.clone();
            let action = move |name: &str| match name {
                "balance" | "syncfs" => Some(match fs.start(name) {
                    Ok(()) => r#"{"started":true}"#.to_string(),
                    Err(e) => serde_json::json!({ "error": e }).to_string(),
                }),
                "compact" => Some(format!("{{\"queued\":{}}}", act_engine.compact_all())),
                "flush" => {
                    let pending = act_engine.unflushed();
                    let bytes: u64 = pending.iter().map(|&(_, b)| b).sum();
                    info!(
                        "statistics page: uploading {bytes} buffered bytes of {} zones",
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
                _ => None,
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

/// How `serve_disk` exposes the device.
struct ServeOptions {
    /// ublk device id; -1 lets the kernel choose.
    dev_id: i32,
    /// Overrides `nr_queues` from disk.json.
    queues: Option<u16>,
    /// `None` — no statistics page (temporary device of `create`).
    stats: Option<StatsArgs>,
    /// `None` — no tuning (temporary device of `create`): no read-ahead,
    /// default uploads, whole-object GETs, btrfs readahead untouched.
    link: Option<Link>,
    read_cache: u64,
}

impl ServeOptions {
    /// A device of `run` or `mount`, tuned for the given link.
    fn device(dev_id: i32, queues: Option<u16>, stats: StatsArgs, link: &LinkArgs) -> Self {
        Self {
            dev_id,
            queues,
            stats: Some(stats),
            link: Some(link.link()),
            read_cache: link.read_cache,
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
        "not storing {bytes} bytes in {} zones: they were never flushed from userspace (no fsync/FLUSH)",
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
        (cfg, Layout::new("memory")?, Store::Mem(MemStore::default()))
    } else {
        open_s3_disk(
            &rt,
            a.creds.as_ref().unwrap(),
            a.prefix.as_ref().unwrap(),
            &a.transport,
        )?
    };
    let on_ready: device::OnReady = Box::new(|_| {
        device::sd_notify("READY=1");
        true
    });
    let serve = ServeOptions::device(a.dev_id, a.queues, a.stats, &a.link);
    serve_until_stopped(&rt, cfg, layout, store, serve, on_ready)?;
    Ok(0)
}

fn mount_options(extra: Option<&str>) -> String {
    match extra.map(str::trim).filter(|e| !e.is_empty()) {
        Some(e) => format!("{MOUNT_OPTIONS},{e}"),
        None => MOUNT_OPTIONS.to_string(),
    }
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
    let options = mount_options(a.options.as_deref());

    // Fork before any thread exists (tokio and ublk queues start in the child).
    let (mut status_rx, status_tx) = pipe()?;
    // SAFETY: the process is single-threaded here.
    let pid = unsafe { libc::fork() };
    if pid < 0 {
        bail!("fork: {}", std::io::Error::last_os_error());
    }
    if pid == 0 {
        drop(status_rx);
        let code = mount_daemon(&a, &creds, status_tx, keep_stderr);
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
fn mount_daemon(a: &MountArgs, creds: &Path, status: File, keep_stderr: bool) -> i32 {
    detach_from_terminal(keep_stderr);
    let status = Arc::new(Mutex::new(Some(status)));
    let res = (|| -> anyhow::Result<()> {
        let rt = runtime()?;
        let (cfg, layout, store) = open_s3_disk(&rt, creds, &a.disk.prefix, &a.disk.transport)?;
        let ready_status = status.clone();
        let on_ready: device::OnReady = Box::new(move |bdev| {
            if let Some(mut f) = ready_status.lock().unwrap().take() {
                let port = STATS_PORT.get().map_or("-".into(), |p| p.to_string());
                let _ = writeln!(f, "READY {bdev} {port}");
            }
            true
        });
        let serve = ServeOptions::device(a.dev_id, a.queues, a.stats, &a.link);
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
    let info = std::fs::read_to_string("/proc/self/mountinfo")?;
    let target = mountpoint.to_string_lossy();
    let mut found = None;
    for line in info.lines() {
        let Some((left, right)) = line.split_once(" - ") else {
            continue;
        };
        let point = left.split(' ').nth(4).map(unescape_mountinfo);
        let source = right.split(' ').nth(1);
        if let (Some(point), Some(source)) = (point, source)
            && point == target
        {
            found = Some(unescape_mountinfo(source));
        }
    }
    Ok(found)
}

/// Decodes `\040`-style escapes used in mountinfo: a backslash and three
/// octal digits (value = d1·64 + d2·8 + d3), e.g. `\040` is a space.
fn unescape_mountinfo(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'\\'
            && i + 3 < b.len()
            && b[i + 1..i + 4].iter().all(|c| (b'0'..=b'7').contains(c))
        {
            out.push((b[i + 1] - b'0') * 64 + (b[i + 2] - b'0') * 8 + (b[i + 3] - b'0'));
            i += 4;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
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

    let info = ctrl.dev_info();
    let pid = info.ublksrv_pid;
    // In FAIL_IO the server is dead and its pid is stale: do not signal it.
    let server_dead = info.state == libublk::sys::UBLK_S_DEV_FAIL_IO as u16;
    // SAFETY: plain syscall.
    if server_dead || pid <= 0 || unsafe { libc::kill(pid, libc::SIGTERM) } != 0 {
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

/// `detach --force`: SIGKILL the device server; the kernel then fails all I/O.
fn kill_server(dev_id: i32) -> anyhow::Result<i32> {
    let ctrl = libublk::ctrl::UblkCtrl::new_simple(dev_id)
        .with_context(|| format!("ublk device {dev_id} not found"))?;
    let info = ctrl.dev_info();
    let pid = info.ublksrv_pid;
    ensure!(
        info.state != libublk::sys::UBLK_S_DEV_FAIL_IO as u16,
        "server of device {dev_id} is already dead; unmount the filesystem and run detach"
    );
    ensure!(pid > 0, "device {dev_id} has no server process");
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
            let key = Layout::new(parent)?.config_key();
            match s3.get(&key).await {
                Ok(_) => bail!("{key} exists: a disk cannot lie inside another disk's prefix"),
                Err(store::StoreError::NotFound(_)) => {}
                Err(e) => return Err(e.into()),
            }
        }
        let existing = s3.list(&layout.dir()).await?;
        if !existing.is_empty() {
            bail!(
                "prefix {} already holds {} objects; destroy the old disk first",
                layout.dir(),
                existing.len()
            );
        }
        cfg.append = s3.detect_append(&layout.dir()).await?;
        info!("appending to zone objects with {:?}", cfg.append);
        let json = Bytes::from(cfg.to_json());
        s3.put(&layout.config_key(), Body::new(vec![json])).await?;
        Ok(())
    })?;
    let s3 = s3.with_append(cfg.append);
    info!(
        "created {}: {} bytes, {} zones of {} bytes",
        layout.config_key(),
        cfg.disk_size,
        cfg.nr_zones(),
        cfg.zone_size
    );

    if let Err(e) = format_disk(&rt, &cfg, &layout, &s3) {
        warn!("formatting failed, removing the new disk {}", layout.dir());
        if let Err(re) = destroy_prefix(&rt, &s3, &layout) {
            error!("cleanup of {} failed: {re:#}", layout.dir());
        }
        return Err(e);
    }
    info!("disk {} is ready", layout.dir());
    Ok(0)
}

/// Brings up a temporary device, runs mkfs.btrfs on it and stores the result.
fn format_disk(
    rt: &Runtime,
    cfg: &DiskConfig,
    layout: &Layout,
    s3: &S3Store,
) -> anyhow::Result<()> {
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
        Store::S3(s3.clone()),
        ServeOptions {
            dev_id: -1,
            queues: None,
            stats: None,
            // mkfs reads next to nothing.
            link: None,
            read_cache: 0,
        },
        on_ready,
    )?;
    // Formatting is part of creating the disk: store it even without a FLUSH.
    let flushed = rt.block_on(engine.flush_all(stats::Reason::Explicit));
    device::delete(&ctrl);
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
    eprintln!("measuring the link to S3, about 20 s…");
    let measured = runtime()?.block_on(measure::measure(&s3, &a.prefix))?;
    println!("{}", measure::report(&measured));
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
    match rt.block_on(s3.get(&layout.config_key())) {
        Ok(_) => {}
        Err(store::StoreError::NotFound(_)) => bail!(
            "no disk at {}: {} not found",
            layout.dir(),
            layout.config_key()
        ),
        Err(e) => return Err(e.into()),
    }
    let deleted = destroy_prefix(&rt, &s3, &layout)?;
    info!("destroyed {}: {deleted} zone objects deleted", layout.dir());
    Ok(0)
}

/// Deletes the disk's zone objects, unfinished concatenations and config;
/// disks nested under its prefix stay.
fn destroy_prefix(rt: &Runtime, s3: &S3Store, layout: &Layout) -> anyhow::Result<usize> {
    rt.block_on(async {
        let config_key = layout.config_key();
        let keys: Vec<String> = s3
            .list(&layout.dir())
            .await?
            .into_iter()
            .map(|o| o.key)
            .filter(|k| layout.zone_of(k).is_some())
            .collect();
        s3.abort_stale_concats(layout).await?;
        s3.delete_many(&keys).await?;
        // The config goes last so an interrupted destroy can be repeated.
        s3.delete(&config_key).await?;
        anyhow::Ok(keys.len())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mountinfo_unescape() {
        assert_eq!(unescape_mountinfo(r"/mnt/my\040disk"), "/mnt/my disk");
        assert_eq!(unescape_mountinfo(r"/mnt/plain"), "/mnt/plain");
        assert_eq!(unescape_mountinfo(r"tail\04"), r"tail\04");
    }

    #[test]
    fn options() {
        assert_eq!(mount_options(None), "nosuid,nodev,noatime,lazytime,commit=300");
        assert_eq!(mount_options(Some(" ")), "nosuid,nodev,noatime,lazytime,commit=300");
        assert_eq!(
            mount_options(Some("compress=zstd")),
            "nosuid,nodev,noatime,lazytime,commit=300,compress=zstd"
        );
    }
}

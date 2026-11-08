//! Live link tuning (SPEC.md §6.1): estimates the link from the daemon's own
//! traffic, regulates the parallel uploads and GETs, applies whatever the
//! nine values resolve to (manual where set, auto elsewhere), and runs the
//! measurement of the statistics page button.

use crate::estimate::{Dir, OBS};
use crate::linkconf;
use crate::measure;
use crate::readahead::ReadAhead;
use crate::regulator::{Regulator, Sample};
use crate::stats::STATS;
use crate::tuning::{self, Auto, Chars, Derived, Effective, Manual};
use crate::zones::Engine;
use log::{info, warn};
use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Saturation is sampled this often; a step is `TICKS_PER_STEP` of them.
const TICK: Duration = Duration::from_millis(100);
const TICKS_PER_STEP: u32 = 10;
/// A step is saturated when all slots were busy at this share of its ticks.
const SATURATED_SHARE: f64 = 0.8;
/// How long a measurement waits for the S3 operations in flight: new ones
/// queue behind it meanwhile, so it gives up rather than stall the disk.
const PAUSE_WAIT: Duration = Duration::from_secs(10);
/// Deleting the temporary objects runs after the pause, but not forever:
/// what is left is deleted at the next start.
const CLEANUP_TIME: Duration = Duration::from_secs(30);

/// Waits until no S3 operation is in flight and holds new ones back;
/// `waiting` hears how many are still in flight. Gives up after `limit`,
/// which lets the queued operations go on.
async fn pause_within<'a>(
    store: &'a crate::store::Store,
    limit: Duration,
    mut waiting: impl FnMut(usize),
) -> Result<tokio::sync::RwLockWriteGuard<'a, ()>, String> {
    let deadline = tokio::time::Instant::now() + limit;
    let pause = store.pause();
    tokio::pin!(pause);
    loop {
        tokio::select! {
            guard = &mut pause => return Ok(guard),
            _ = tokio::time::sleep(Duration::from_millis(200)) => {
                waiting(store.in_flight());
                if tokio::time::Instant::now() >= deadline {
                    return Err(format!(
                        "S3 is busy: {} S3 operations still in flight after {} s",
                        store.in_flight(),
                        limit.as_secs_f64()
                    ));
                }
            }
        }
    }
}

/// Byte and request counters of S3 traffic since the start.
#[derive(Clone, Copy, Debug, Default)]
pub struct Totals {
    pub read: u64,
    pub written: u64,
    pub gets: u64,
    pub uploads: u64,
}

#[derive(Default)]
struct Ticks {
    n: u32,
    uploads_busy: u32,
    reads_busy: u32,
}

#[derive(Serialize, Clone, Debug, Default)]
pub struct MeasureState {
    /// idle, waiting (for operations in flight), measuring, done, failed.
    pub state: &'static str,
    pub waiting_ops: usize,
    /// Seconds since it started or ended.
    pub secs: f64,
    pub elapsed_secs: Option<f64>,
    pub skipped: Vec<&'static str>,
    pub error: Option<String>,
    #[serde(skip)]
    at: Option<Instant>,
}

struct State {
    manual: Manual,
    /// The daemon's own estimates of the five characteristics.
    chars: Chars,
    uploads: Regulator,
    reads: Regulator,
    applied: Option<(Derived, u64)>,
    effective: Option<Effective>,
    ticks: Ticks,
    last: Option<(f64, Totals)>,
    measure: MeasureState,
}

pub struct Autotune {
    engine: Arc<Engine>,
    zone_size: u64,
    fs_target: Arc<AtomicU64>,
    read_only: bool,
    rt: tokio::runtime::Handle,
    st: Mutex<State>,
}

/// One of the values on the page: as file text, and raw for charts.
#[derive(Serialize, Clone, Debug)]
pub struct Value {
    pub key: &'static str,
    /// link, derived or memory.
    pub group: &'static str,
    /// ms, bytes, bytes/s or count.
    pub unit: &'static str,
    pub auto: Option<String>,
    pub manual: Option<String>,
    pub effective: Option<String>,
    pub auto_raw: Option<f64>,
    pub effective_raw: Option<f64>,
}

#[derive(Serialize, Clone, Debug)]
pub struct RegulatorState {
    /// Regulated now: neither set by hand nor following manual characteristics.
    pub active: bool,
    pub phase: &'static str,
    pub limit: usize,
    pub change_secs_ago: Option<f64>,
    pub change: Option<String>,
}

#[derive(Serialize, Clone, Debug)]
pub struct LinkState {
    pub values: Vec<Value>,
    pub uploads: RegulatorState,
    pub reads: RegulatorState,
    pub measure: MeasureState,
    /// The manual values as a `--link-config` file.
    pub yaml: String,
}

/// Seconds on a clock that keeps running through suspend, like the rest of
/// the daemon's idle logic.
fn now_secs() -> f64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: valid pointer to a timespec on our stack.
    unsafe { libc::clock_gettime(libc::CLOCK_BOOTTIME, &mut ts) };
    ts.tv_sec as f64 + ts.tv_nsec as f64 * 1e-9
}

impl Autotune {
    /// Applies the start values at once (manual, or 1 connection and the
    /// minimal windows).
    pub fn new(
        engine: Arc<Engine>,
        manual: Manual,
        zone_size: u64,
        fs_target: Arc<AtomicU64>,
        read_only: bool,
        rt: tokio::runtime::Handle,
    ) -> anyhow::Result<Arc<Self>> {
        tuning::validate(&manual, zone_size)?;
        let me = Arc::new(Self {
            engine,
            zone_size,
            fs_target,
            read_only,
            rt,
            st: Mutex::new(State {
                manual,
                chars: Chars::default(),
                uploads: Regulator::new(),
                reads: Regulator::new(),
                applied: None,
                effective: None,
                ticks: Ticks::default(),
                last: None,
                measure: MeasureState {
                    state: "idle",
                    ..MeasureState::default()
                },
            }),
        });
        me.apply(&mut me.st.lock().unwrap(), "start");
        Ok(me)
    }

    /// Ticks and steps until the process ends.
    pub async fn run(self: Arc<Self>) {
        let mut tick = tokio::time::interval(TICK);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut n = 0;
        loop {
            tick.tick().await;
            self.tick();
            n += 1;
            if n % TICKS_PER_STEP == 0 {
                self.step(
                    now_secs(),
                    Totals {
                        read: STATS.read_from_s3.get(),
                        written: STATS.upload_size.sum(),
                        gets: STATS.gets.get(),
                        uploads: STATS.upload_size.count(),
                    },
                );
            }
        }
    }

    /// Samples whether all slots are busy.
    pub fn tick(&self) {
        let busy = |l: &crate::limiter::Limiter| l.in_use() >= l.limit();
        let mut st = self.st.lock().unwrap();
        st.ticks.n += 1;
        st.ticks.uploads_busy += busy(self.engine.upload_limiter()) as u32;
        st.ticks.reads_busy += busy(self.engine.get_limiter()) as u32;
    }

    /// One step, with the counters since the start.
    pub fn step(&self, now: f64, t: Totals) {
        let mut st = self.st.lock().unwrap();
        let ticks = std::mem::take(&mut st.ticks);
        let Some((then, t0)) = st.last.replace((now, t)) else {
            return;
        };
        let dt = (now - then).max(1e-3);
        let saturated = |busy: u32| ticks.n > 0 && busy as f64 >= SATURATED_SHARE * ticks.n as f64;
        let manual = st.manual;
        let e = st.effective.expect("applied at start");
        // A regulator steps only while it decides: its samples mean nothing
        // under a limit set otherwise.
        if e.derived.uploads == st.uploads.limit() && manual.uploads.is_none() {
            st.uploads.step(Sample {
                bytes_per_sec: t.written.saturating_sub(t0.written) as f64 / dt,
                saturated: saturated(ticks.uploads_busy),
                completions: t.uploads.saturating_sub(t0.uploads),
                retries: OBS.take_retries(Dir::Write),
                now_secs: now,
            });
        }
        if e.derived.reads == st.reads.limit() && manual.reads.is_none() {
            st.reads.step(Sample {
                bytes_per_sec: t.read.saturating_sub(t0.read) as f64 / dt,
                saturated: saturated(ticks.reads_busy),
                completions: t.gets.saturating_sub(t0.gets),
                retries: OBS.take_retries(Dir::Read),
                now_secs: now,
            });
        }
        let mut chars = OBS.estimate(st.chars);
        chars.read_total = st.reads.best_rate().map(|r| r as u64).or(chars.read_total);
        chars.write_total = st
            .uploads
            .best_rate()
            .map(|r| r as u64)
            .or(chars.write_total);
        st.chars = chars;
        self.apply(&mut st, "estimate");
    }

    fn auto(st: &State) -> Auto {
        Auto {
            chars: st.chars,
            uploads: st.uploads.limit(),
            reads: st.reads.limit(),
        }
    }

    /// Resolves the values and applies what changed.
    fn apply(&self, st: &mut State, why: &str) {
        let e = tuning::resolve(&st.manual, &Self::auto(st), self.zone_size);
        st.effective = Some(e);
        let d = e.derived;
        let cache = st
            .manual
            .read_cache
            .unwrap_or(crate::config::DEFAULT_READ_CACHE);
        let old = st.applied.replace((d, cache));
        if old == Some((d, cache)) {
            return;
        }
        let (od, ocache) = old.map_or((None, None), |(d, c)| (Some(d), Some(c)));
        if od.map(|o| o.uploads) != Some(d.uploads) {
            self.engine.set_uploads(d.uploads);
        }
        if od.map(|o| o.reads) != Some(d.reads) {
            self.engine.set_reads(d.reads);
        }
        if od.map(|o| o.read_unit) != Some(d.read_unit) || ocache != Some(cache) {
            self.engine.set_get_part(d.read_unit);
            let ra = ReadAhead {
                window: d.read_unit,
                cache,
            };
            if let Err(e) = self.engine.set_read_ahead(ra) {
                warn!("cannot set read-ahead: {e:#}");
            }
        }
        self.fs_target.store(d.fs_readahead, Ordering::SeqCst);
        info!("S3 link ({why}): {}; applied: {d}", e.chars);
    }

    /// Sets one value by hand; `auto` gives it back to the daemon.
    #[cfg(test)]
    pub fn set(&self, key: &str, value: &str) -> Result<(), String> {
        self.set_all(&[(key.to_string(), value.to_string())])
    }

    /// Sets values by hand, in order; nothing applies if one is refused.
    pub fn set_all(&self, pairs: &[(String, String)]) -> Result<(), String> {
        let mut st = self.st.lock().unwrap();
        let mut manual = st.manual;
        for (key, value) in pairs {
            manual = Self::with(manual, key, value)?;
        }
        tuning::validate(&manual, self.zone_size).map_err(|e| format!("{e:#}"))?;
        if manual.read_cache.is_some() {
            let d = tuning::resolve(&manual, &Self::auto(&st), self.zone_size).derived;
            let ra = ReadAhead {
                window: d.read_unit,
                cache: manual.read_cache.unwrap_or(0),
            };
            ra.validate(self.zone_size).map_err(|e| format!("{e:#}"))?;
        }
        for (key, value) in pairs {
            info!("statistics page: {key} = {value}");
        }
        st.manual = manual;
        self.apply(&mut st, "set by hand");
        Ok(())
    }

    /// `manual` with `key` set to `value` (file syntax) or `auto`.
    fn with(mut manual: Manual, key: &str, value: &str) -> Result<Manual, String> {
        let one = if value == "auto" {
            linkconf::parse(&format!("{key}: null"))
        } else if value.is_empty() || !value.chars().all(|c| c.is_ascii_alphanumeric() || c == '.')
        {
            return Err(format!("{key}: invalid value {value:?}"));
        } else {
            linkconf::parse(&format!("{key}: {value}"))
        }
        .map_err(|e| format!("{e:#}"))?;
        let c = &mut manual.chars;
        match key {
            "latency" => c.latency = one.chars.latency,
            "read_stream" => c.read_stream = one.chars.read_stream,
            "read_total" => c.read_total = one.chars.read_total,
            "write_stream" => c.write_stream = one.chars.write_stream,
            "write_total" => c.write_total = one.chars.write_total,
            "read_unit" => manual.read_unit = one.read_unit,
            "fs_readahead" => manual.fs_readahead = one.fs_readahead,
            "uploads" => manual.uploads = one.uploads,
            "reads" => manual.reads = one.reads,
            "read_cache" => manual.read_cache = one.read_cache,
            _ => return Err(format!("unknown key {key}")),
        }
        Ok(manual)
    }

    pub fn snapshot(&self) -> LinkState {
        let st = self.st.lock().unwrap();
        let e = st.effective.expect("applied at start");
        let m = &st.manual;
        let ms = |d: Option<Duration>| d.map(|d| d.as_secs_f64() * 1e3);
        let f = |v: Option<u64>| v.map(|v| v as f64);
        let dur = |d: Option<Duration>| d.map(linkconf::duration_text);
        let rate = |v: Option<u64>| v.map(linkconf::rate_text);
        let size = |v: Option<u64>| v.map(linkconf::size_text);
        let count = |v: Option<usize>| v.map(|n| n.to_string());
        let a = st.chars;
        let mut values = vec![Value {
            key: "latency",
            group: "link",
            unit: "ms",
            auto: dur(a.latency),
            manual: dur(m.chars.latency),
            effective: dur(e.chars.latency),
            auto_raw: ms(a.latency),
            effective_raw: ms(e.chars.latency),
        }];
        for (key, auto, manual, eff) in [
            (
                "read_stream",
                a.read_stream,
                m.chars.read_stream,
                e.chars.read_stream,
            ),
            (
                "read_total",
                a.read_total,
                m.chars.read_total,
                e.chars.read_total,
            ),
            (
                "write_stream",
                a.write_stream,
                m.chars.write_stream,
                e.chars.write_stream,
            ),
            (
                "write_total",
                a.write_total,
                m.chars.write_total,
                e.chars.write_total,
            ),
        ] {
            values.push(Value {
                key,
                group: "link",
                unit: "bytes/s",
                auto: rate(auto),
                manual: rate(manual),
                effective: rate(eff),
                auto_raw: f(auto),
                effective_raw: f(eff),
            });
        }
        let (ad, d) = (e.auto_derived, e.derived);
        for (key, auto, manual, eff) in [
            ("read_unit", ad.read_unit, m.read_unit, d.read_unit),
            (
                "fs_readahead",
                ad.fs_readahead,
                m.fs_readahead,
                d.fs_readahead,
            ),
        ] {
            values.push(Value {
                key,
                group: "derived",
                unit: "bytes",
                auto: size(Some(auto)),
                manual: size(manual),
                effective: size(Some(eff)),
                auto_raw: f(Some(auto)),
                effective_raw: f(Some(eff)),
            });
        }
        for (key, auto, manual, eff) in [
            ("uploads", ad.uploads, m.uploads, d.uploads),
            ("reads", ad.reads, m.reads, d.reads),
        ] {
            values.push(Value {
                key,
                group: "derived",
                unit: "count",
                auto: count(Some(auto)),
                manual: count(manual),
                effective: count(Some(eff)),
                auto_raw: Some(auto as f64),
                effective_raw: Some(eff as f64),
            });
        }
        let cache = m.read_cache.unwrap_or(crate::config::DEFAULT_READ_CACHE);
        values.push(Value {
            key: "read_cache",
            group: "memory",
            unit: "bytes",
            auto: size(Some(crate::config::DEFAULT_READ_CACHE)),
            manual: size(m.read_cache),
            effective: size(Some(cache)),
            auto_raw: Some(crate::config::DEFAULT_READ_CACHE as f64),
            effective_raw: Some(cache as f64),
        });
        let now = now_secs();
        let reg = |r: &Regulator, active: bool| RegulatorState {
            active,
            phase: r.phase(),
            limit: r.limit(),
            change_secs_ago: r.last_change().map(|(t, _)| (now - t).max(0.0)),
            change: r.last_change().map(|(_, why)| why.clone()),
        };
        let mut measure = st.measure.clone();
        measure.secs = measure.at.map_or(0.0, |t| t.elapsed().as_secs_f64());
        LinkState {
            values,
            uploads: reg(
                &st.uploads,
                m.uploads.is_none() && d.uploads == st.uploads.limit(),
            ),
            reads: reg(&st.reads, m.reads.is_none() && d.reads == st.reads.limit()),
            measure,
            yaml: linkconf::render(m),
        }
    }

    /// Starts a measurement: S3 operations pause while it runs (SPEC.md §6.1).
    pub fn measure(self: &Arc<Self>) -> Result<(), String> {
        {
            let mut st = self.st.lock().unwrap();
            if matches!(st.measure.state, "waiting" | "measuring") {
                return Err("a measurement is already running".into());
            }
            st.measure = MeasureState {
                state: "waiting",
                at: Some(Instant::now()),
                ..MeasureState::default()
            };
        }
        let me = self.clone();
        self.rt.spawn(async move {
            /// Marks the measurement failed if its task ends without a result
            /// (a panic); the pause guard goes with the task's stack.
            struct Unfinished(Arc<Autotune>);
            impl Drop for Unfinished {
                fn drop(&mut self) {
                    let mut st = self.0.st.lock().unwrap();
                    if matches!(st.measure.state, "waiting" | "measuring") {
                        st.measure.state = "failed";
                        st.measure.error = Some("the measurement task panicked".into());
                    }
                }
            }
            let _unfinished = Unfinished(me.clone());
            let result = me.run_measure().await;
            me.finish(result);
        });
        Ok(())
    }

    async fn run_measure(&self) -> anyhow::Result<measure::Measured> {
        let store = self.engine.store().clone();
        let s3 = store
            .s3_store()
            .ok_or_else(|| anyhow::anyhow!("a measurement needs S3"))?
            .clone();
        let paused = pause_within(&store, PAUSE_WAIT, |n| {
            self.st.lock().unwrap().measure.waiting_ops = n;
        })
        .await
        .map_err(|e| anyhow::anyhow!(e))?;
        {
            let mut st = self.st.lock().unwrap();
            st.measure.state = "measuring";
            st.measure.waiting_ops = 0;
        }
        info!("measuring the link to S3; S3 operations wait meanwhile");
        let write_dir = (!self.read_only).then(|| measure::temp_dir(self.engine.layout()));
        let plan = measure::Plan {
            reads: self.engine.measure_candidates(),
            write_dir: write_dir.clone(),
            budget: Some(measure::BUDGET),
        };
        let measured = measure::run(&s3, plan).await;
        drop(paused);
        // Only the measurement's own objects: no need to hold the disk back.
        if let Some(dir) = write_dir {
            match tokio::time::timeout(CLEANUP_TIME, measure::cleanup(&s3, &dir)).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => warn!("cannot delete the measurement objects under {dir}/: {e:#}"),
                Err(_) => warn!("deleting the measurement objects under {dir}/ timed out"),
            }
        }
        measured
    }

    fn finish(&self, result: anyhow::Result<measure::Measured>) {
        let mut st = self.st.lock().unwrap();
        let at = Some(Instant::now());
        match result {
            Ok(m) => {
                info!(
                    "link measured in {:.1} s: {}{}",
                    m.elapsed.as_secs_f64(),
                    m.chars,
                    if m.skipped.is_empty() {
                        String::new()
                    } else {
                        format!("; not measured: {}", m.skipped.join(", "))
                    }
                );
                OBS.seed(m.chars.or(st.chars));
                st.chars = m.chars.or(st.chars);
                // Connections start over from what the measurement implies.
                let measured = Manual {
                    chars: st.chars,
                    ..Manual::default()
                };
                let d = tuning::resolve(&measured, &Self::auto(&st), self.zone_size).derived;
                st.uploads.restart_from(d.uploads);
                st.reads.restart_from(d.reads);
                st.measure = MeasureState {
                    state: "done",
                    elapsed_secs: Some(m.elapsed.as_secs_f64()),
                    skipped: m.skipped,
                    at,
                    ..MeasureState::default()
                };
                self.apply(&mut st, "measured");
            }
            Err(e) => {
                warn!("link measurement failed: {e:#}");
                STATS.err_measure.record(format!("{e:#}"));
                st.measure = MeasureState {
                    state: "failed",
                    error: Some(format!("{e:#}")),
                    at,
                    ..MeasureState::default()
                };
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DiskConfig;
    use crate::store::{Layout, MemStore, Store};
    use std::time::Duration;

    const ZS: u64 = 16 << 20;

    async fn tuner(manual: Manual) -> Arc<Autotune> {
        let cfg = DiskConfig::new(8 * ZS, ZS);
        let store = Arc::new(Store::mem(MemStore::default()));
        let engine = Arc::new(
            Engine::open(&cfg, Layout::new("t").unwrap(), store)
                .await
                .unwrap(),
        );
        Autotune::new(
            engine,
            manual,
            ZS,
            Arc::new(AtomicU64::new(0)),
            false,
            tokio::runtime::Handle::current(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn starts_from_one_connection() {
        let t = tuner(Manual::default()).await;
        assert_eq!(t.engine.uploads_limit(), 1);
        assert_eq!(t.engine.reads_limit(), 1);
        assert_eq!(t.engine.get_part(), Some(128 << 10));
        assert_eq!(t.fs_target.load(Ordering::SeqCst), 4 << 20);
    }

    #[tokio::test]
    async fn manual_value_applies() {
        let t = tuner(Manual {
            uploads: Some(8),
            ..Manual::default()
        })
        .await;
        assert_eq!(t.engine.uploads_limit(), 8);
        let s = t.snapshot();
        let uploads = s.values.iter().find(|v| v.key == "uploads").unwrap();
        assert_eq!(uploads.manual.as_deref(), Some("8"));
        assert_eq!(uploads.auto.as_deref(), Some("1")); // the regulator's
        assert_eq!(uploads.effective.as_deref(), Some("8"));
        assert!(s.yaml.contains("uploads: 8"), "{}", s.yaml);
    }

    #[tokio::test]
    async fn saturated_steps_grow_connections() {
        let t = tuner(Manual::default()).await;
        let (mut totals, mut now) = (Totals::default(), 0.0);
        t.step(now, totals); // the first step only starts the counters
        for _ in 0..9 {
            // All GET slots busy for the whole second, the rate doubling with them.
            let held: Vec<_> = futures_util::future::join_all(
                (0..t.engine.reads_limit()).map(|_| t.engine.get_limiter().acquire()),
            )
            .await;
            for _ in 0..10 {
                t.tick();
            }
            drop(held);
            now += 1.0;
            totals.read += t.engine.reads_limit() as u64 * (1 << 20);
            totals.gets += t.engine.reads_limit() as u64;
            t.step(now, totals);
        }
        assert_eq!(t.engine.reads_limit(), 8);
        assert_eq!(t.engine.uploads_limit(), 1); // not saturated: unchanged
    }

    #[tokio::test]
    async fn set_and_back_to_auto() {
        let t = tuner(Manual::default()).await;
        t.set("uploads", "8").unwrap();
        assert_eq!(t.engine.uploads_limit(), 8);
        t.set("latency", "40ms").unwrap();
        t.set("read_stream", "4M").unwrap();
        assert_eq!(t.engine.get_part(), Some(128 << 10)); // 160 KiB → 128 KiB
        t.set("read_stream", "50M").unwrap();
        assert_eq!(t.engine.get_part(), Some(2 << 20));
        t.set("uploads", "auto").unwrap();
        assert_eq!(t.engine.uploads_limit(), 1);
        t.set("read_cache", "0").unwrap();
        assert_eq!(t.engine.read_ahead_window(), None);
    }

    #[tokio::test]
    async fn set_rejects_bad() {
        let t = tuner(Manual::default()).await;
        assert!(t.set("uploads", "0").is_err());
        assert!(t.set("uploads", "65").is_err());
        assert!(t.set("nope", "1").is_err());
        assert!(t.set("latency", "fast").is_err());
        assert!(t.set("uploads", "8\nreads: 3").is_err());
        assert!(t.set("read_unit", "3M").is_err());
        assert_eq!(t.engine.uploads_limit(), 1); // nothing applied
    }

    #[tokio::test]
    async fn set_all_is_all_or_nothing() {
        let t = tuner(Manual::default()).await;
        let pairs = |p: &[(&str, &str)]| -> Vec<(String, String)> {
            p.iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        };
        assert!(
            t.set_all(&pairs(&[("uploads", "8"), ("reads", "0")]))
                .is_err()
        );
        assert_eq!(t.engine.uploads_limit(), 1);
        t.set_all(&pairs(&[("uploads", "8"), ("reads", "3")]))
            .unwrap();
        assert_eq!((t.engine.uploads_limit(), t.engine.reads_limit()), (8, 3));
    }

    /// Operations in flight that do not finish (long retries) must not keep
    /// every other S3 operation queued behind the measurement.
    #[tokio::test]
    async fn pause_gives_up_on_busy_store() {
        let store = Store::mem(MemStore::default());
        let busy = store.op().await;
        let mut waiting = Vec::new();
        let got = pause_within(&store, Duration::from_millis(100), |n| waiting.push(n)).await;
        assert!(got.is_err());
        assert!(got.unwrap_err().contains("1 S3 operations"));
        // The queued writer is gone: new operations go through at once.
        tokio::time::timeout(Duration::from_millis(100), store.list("t/"))
            .await
            .expect("no writer left in the queue")
            .unwrap();
        drop(busy);
        assert!(
            pause_within(&store, Duration::from_millis(100), |_| {})
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn measure_twice_errors() {
        let t = tuner(Manual::default()).await;
        t.measure().unwrap();
        assert_eq!(t.measure().unwrap_err(), "a measurement is already running");
    }

    #[tokio::test]
    async fn measure_needs_s3_and_releases_the_pause() {
        let t = tuner(Manual::default()).await;
        t.measure().unwrap();
        for _ in 0..100 {
            if t.snapshot().measure.state != "waiting" && t.snapshot().measure.state != "measuring"
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let m = t.snapshot().measure;
        assert_eq!(m.state, "failed");
        assert!(m.error.unwrap().contains("S3"));
        // The store works again.
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            t.engine.store().list("t/"),
        )
        .await
        .expect("no pause left")
        .unwrap();
        // And another measurement may start.
        assert!(t.measure().is_ok());
    }
}

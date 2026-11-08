# Link Autotuning Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** each of the nine link values (five S3 link characteristics and four derived ones) is either auto (estimated from the daemon's own traffic and adapted live) or manual (set in `--link-config` YAML or on the statistics page). A button re-measures the link while S3 operations are paused.

**Architecture:**
- Pure units:
  - `tuning.rs`: formulas and resolution of manual vs auto;
  - `linkconf.rs`: the YAML file;
  - `estimate.rs`: estimates from observations;
  - `regulator.rs`: the connection controller.
- `autotune.rs` is the runtime glue. A 100 ms ticker samples saturation, and once a second it feeds the regulators, resolves the effective values and applies changes to the engine.
- The engine gets resizable limits (`Limiter`), an atomic GET part and a swappable read cache.
- `Store` gets a pause gate for the measurement.

**Tech Stack:** Rust, tokio, serde + `serde_norway` (YAML read), tiny_http, the existing `S3Store`.

**Spec:** `SPEC.md` §6.1 (and the decisions table in §1). Read it first; this plan only adds the decisions the spec leaves open.

## Global Constraints

- **Communication:** answer the user in Russian. Code, logs, error messages and commit messages are in English. Every commit message ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. One logical change is one commit.
- **Hardcoding:** none for link characteristics. Without data a characteristic is `None`, shown as "—".
- **Start values with no estimate:**
  - auto `uploads` = `reads` = 1;
  - `read_unit` = 128 KiB (`UNIT_MIN`);
  - `fs_readahead` = 4 MiB (`READAHEAD_MIN`).
- **Bounds:**
  - `read_unit` is a power of two, 128K..4M and ≤ zone;
  - `fs_readahead` is 4M..256M;
  - `uploads` and `reads` are 1..64 (manual or regulated). The formula from manual characteristics keeps `uploads` 4..64 and `reads` 1..64.
- **Regulator:**
  - step 1 s;
  - slow-start doubling while saturated and the step's throughput grew ≥ 10 %;
  - a doubling without gain reverts to the best value;
  - then, under load, a probe +1 every 30 s, kept if the gain is ≥ 5 %, otherwise reverted;
  - throughput at the same saturation dropped > 1/3 from the best, or a retry happened in the step: halve (min 1), reset the best, restart slow start;
  - cap 64.
- **Measure button:**
  - one at a time: a second request answers `{"error":"a measurement is already running"}`;
  - budget ≤ 20 s total;
  - reads only existing single-object zone objects, chosen at random (never a fragmented SEGMENTS zone);
  - writes temp objects under `<prefix>/measure-<random>/`, deleted afterwards;
  - no writes on a read-only disk.
- **Start-up cleanup:** on start, when not read-only, delete every object under `<prefix>/measure-`. `destroy` deletes them too.
- **YAML keys:** `latency`, `read_stream`, `read_total`, `write_stream`, `write_total`, `read_unit`, `fs_readahead`, `uploads`, `reads`, `read_cache`.
  - `null` or an absent key means auto (`read_cache` absent means 64M);
  - an unknown key is a start-up error.
- **Value syntax:** durations via `tuning::parse_duration` (`38ms`), sizes and rates via `config::parse_size` (`24M` = MiB or MiB/s), counts as YAML integers.
- **Flags:** `--s3-latency`, `--s3-read-stream`, `--s3-read-total`, `--s3-write-stream`, `--s3-write-total` and `--read-cache` are removed and replaced by `--link-config <FILE>` on `mount` and `run`.
- **Persistence:** none. Dashboard changes live in memory only.
- **sudo:** only for `target/debug/btrs3fs` and `chown mmarkk /mnt/btrs3fs`.
- **Laptop:** remount only when the user asks, and only after `fuser -m /mnt/btrs3fs` is free and the buffers are empty.

## Review Focus

1. **`destroy` after a crashed measurement.** `measure-*` leftovers must be deleted too; today `destroy_prefix` filters by `zone_of` and would leave them. Test in Task 7.
2. **Shrinking a limit while all slots are busy.** No request may be cancelled, and the in-use count must converge to the new limit as requests finish. Test in Task 3.
3. **Measurement on a fresh, nearly empty disk.** There are no read candidates: the read part is skipped, the previous estimates are kept and no error is raised. Test in Task 7.
4. **A measurement that hangs or panics.** The pause must be released anyway, otherwise every S3 operation and the filesystem hang. Test in Task 6.
5. **A YAML typo such as `upload: 8`.** It must fail at start, not run silently as auto. Test in Task 2.

---

### Task 1: Link values with optional parts, manual/auto resolution

**Files:**
- Modify: `src/tuning.rs`: remove `Link::YANDEX_CLOUD`; add types; keep the formulas.
- Test: `src/tuning.rs` tests.

**Interfaces:**
- Produces:
  - `pub struct Chars { latency: Option<Duration>, read_stream: Option<u64>, read_total: Option<u64>, write_stream: Option<u64>, write_total: Option<u64> }` (Copy, Default, PartialEq, Debug).
  - `pub struct Derived { read_unit: u64, fs_readahead: u64, uploads: usize, reads: usize }`, the renamed `Tuning`.
  - `pub struct Manual { chars: Chars, read_unit: Option<u64>, fs_readahead: Option<u64>, uploads: Option<usize>, reads: Option<usize>, read_cache: Option<u64> }` (Default = all auto).
  - `pub struct Auto { chars: Chars, uploads: usize, reads: usize }`: estimates plus regulator outputs.
  - `pub fn resolve(manual: &Manual, auto: &Auto, zone_size: u64) -> Effective`, where `pub struct Effective { chars: Chars, derived: Derived, auto_derived: Derived }`. `auto_derived` is what auto would give (shown on the page next to manual values).
  - `pub fn validate(m: &Manual, zone_size: u64) -> anyhow::Result<()>` checks the bounds from Global Constraints.

- [ ] **Step 1: Write failing tests**
  - `resolve_all_auto_without_data`: `resolve(&Manual::default(), &Auto{chars: Chars::default(), uploads: 1, reads: 1}, 16 MiB)` gives `derived == Derived{read_unit: 128 KiB, fs_readahead: 4 MiB, uploads: 1, reads: 1}`.
  - `resolve_auto_derived_follow_effective_chars`: manual `latency` = 32 ms with auto `read_stream` = 57 MiB/s gives `read_unit` = 2 MiB (the same as the existing `cloud_and_internet` numbers).
  - `manual_chars_use_formula_for_connections`: all five chars manual, uploads and reads auto, the cloud numbers from the existing test, gives `uploads` in 21..=36 and `reads` = 19. With an `Auto` whose uploads = 3, `auto_derived.uploads` = 3.
  - `manual_derived_wins`: manual `uploads` = 8 gives `derived.uploads` = 8 while `auto_derived.uploads` stays the regulator's.
  - `validate_bounds`: `read_unit` = 3 MiB is an error (not a power of two), 8 MiB is an error, `uploads` = 0 and 65 are errors.
- [ ] **Step 2: Run** `cargo test tuning`. Expect compile failure or FAIL.
- [ ] **Step 3: Implement.**
  - `read_unit`/`fs_readahead` auto: the formulas when their inputs are `Some`, otherwise the minimum.
  - `uploads`/`reads` auto: the formula only if all its inputs are manual, otherwise `auto.uploads`/`auto.reads`.
  - Delete `Link`'s CLI defaulting; keep the `Display` of `Derived` for the log.
- [ ] **Step 4: Run** `cargo test tuning`. Expect PASS. `cargo build` may break callers in main.rs: leave a temporary adapter there that builds a `Manual` from the old flags. Task 8 removes it.
- [ ] **Step 5: Commit** `tuning: optional link characteristics, manual/auto resolution, no built-in link`.

### Task 2: `--link-config` YAML

**Files:**
- Create: `src/linkconf.rs`.
- Modify: `Cargo.toml` (add `serde_norway`), `src/main.rs` (`mod linkconf;`).
- Test: `src/linkconf.rs` tests, using `examples/link.yaml`.

**Interfaces:**
- Consumes: `tuning::Manual`, `tuning::parse_duration`, `config::parse_size`.
- Produces:
  - `pub fn load(path: &Path) -> anyhow::Result<Manual>`.
  - `pub fn parse(text: &str) -> anyhow::Result<Manual>`.
  - `pub fn render(m: &Manual) -> String`: YAML with a header comment, every key present (`null` for auto), sizes rendered as `<n>M`/`<n>K` when exact, otherwise bytes.

- [ ] **Step 1: Failing tests**
  - `example_parses`: `parse(include_str!("../examples/link.yaml"))` gives `latency` = 38 ms, `read_stream` = 33 MiB, `read_total` = None, `write_stream` = 24 MiB, `write_total` = None, `uploads` = 8, `reads` = None, `read_cache` = 64 MiB.
  - `empty_is_auto`: `parse("")` and `parse("{}")` give `Manual::default()`.
  - `unknown_key_fails`: the error from `parse("upload: 8")` mentions `upload`.
  - `bad_value_fails`: `parse("latency: fast")` is an error naming `latency`.
  - `render_roundtrip`: `parse(&render(&m)) == m` for the example and for the default.
- [ ] **Step 2:** `cargo test linkconf`. Expect FAIL.
- [ ] **Step 3: Implement.** Use a `#[serde(deny_unknown_fields)]` struct of `Option<Value>` fields, where `Value` is an untagged enum of `u64 | String`. Convert each field with its parser; error text is `"{key}: {err}"`.
- [ ] **Step 4:** `cargo test linkconf`. Expect PASS.
- [ ] **Step 5: Commit** `--link-config YAML: parse and render link settings`.

### Task 3: Resizable limits and live engine knobs

**Files:**
- Create: `src/limiter.rs`.
- Modify: `src/zones.rs`: `upload_slots`/`get_slots` become `Limiter`; `get_part: AtomicU64`; `read_cache: Mutex<Option<Arc<ReadCache>>>`.
- Test: `src/limiter.rs`, `src/zones.rs` tests.

**Interfaces:**
- Produces:
  - `pub struct Limiter` with:
    - `new(limit: usize)`;
    - `async fn acquire(&self) -> LimiterPermit`;
    - `fn set_limit(&self, n: usize)`;
    - `fn limit(&self) -> usize`;
    - `fn in_use(&self) -> usize`;
    - `fn waiting(&self) -> usize`.
  - When shrinking, available permits are taken with `forget_permits`; the rest is a debt that dropped permits pay by `forget()` instead of returning.
  - Engine:
    - `pub fn set_uploads(&self, n: usize)`, `pub fn set_reads(&self, n: usize)`;
    - `pub fn set_get_part(&self, bytes: u64)`;
    - `pub fn set_read_ahead(&self, ra: ReadAhead) -> anyhow::Result<()>` (a new `ReadCache` replaces the old one; `None` when disabled);
    - `pub fn upload_limiter(&self) -> &Limiter`, `pub fn get_limiter(&self) -> &Limiter`.
  - The builder methods `with_uploads`/`with_reads`/`with_get_part`/`with_read_ahead` are removed in favour of the setters.

- [ ] **Step 1: Failing tests**
  - `grow_lets_waiters_in`: limit 1, hold one permit, spawn an acquire (pending), `set_limit(2)`: the acquire completes.
  - `shrink_while_busy`: limit 4, hold 4, `set_limit(1)`; `limit()` = 1; drop 3 permits, then a new acquire is still pending; drop the last one: the pending acquire completes, and `in_use()` stays ≤ 1 afterwards. (Review Focus 2.)
  - In zones.rs, `read_cache_swap_on_window_change`: a read fills the cache; `set_read_ahead` with a new window; the next read gives the same data and the old windows are gone (`forget`/len check through a test helper).
  - Adapt `uploads_share_one_limit` and `large_read_goes_out_in_parts` to the setters.
- [ ] **Step 2:** `cargo test limiter zones`. Expect FAIL.
- [ ] **Step 3: Implement.** The read path clones the `Arc<ReadCache>` out of the mutex before awaiting.
- [ ] **Step 4:** `cargo test`. Everything passes.
- [ ] **Step 5: Commit** `Engine: resizable upload/GET limits, live GET part and read cache`.

### Task 4: Passive estimates

**Files:**
- Create: `src/estimate.rs`.
- Modify: `src/s3.rs`: observation calls on success inside the attempt closures of `get_range`, `put`, `patch_request` and `rgw_append`, plus a retry event in `with_retries`.
- Test: `src/estimate.rs` tests.

**Interfaces:**
- Produces:
  - `pub static OBS: Observations`, global like `STATS`.
  - `OBS.get(ttfb: Duration, bytes: u64, body: Duration)`, `OBS.put(bytes: u64, total: Duration)`, `OBS.retry(dir: Dir)`, with `pub enum Dir { Read, Write }`.
  - `OBS.take_retries(dir) -> u64`: the count since the last call.
  - `pub fn estimate(&self, prev: Chars) -> Chars`: latency, read_stream and write_stream only (totals come from the regulator, Task 5); a characteristic without samples keeps `prev`.
  - `OBS.seed(c: Chars)`: the measurement result replaces the sample windows' estimates.
- **Decisions not in the spec** (update the SPEC §6.1 table text in this task's commit):
  - samples live in rings of the last 64;
  - latency = the median of GET time-to-headers;
  - read_stream = p90 of `bytes / body` over GETs of ≥ 256 KiB (p90, because concurrent GETs share bandwidth and understate one stream);
  - write_stream = p90 of `bytes / (total − latency)` over PUTs of ≥ 256 KiB. This is the line "time = L + size / W₁" with L taken from GETs. Before any latency is known, write_stream needs one: use the minimum PUT time of ≤ 64 KiB as L.
  - `retry` direction: `what` starting with `GET` is Read; `PUT`, `PATCH` and `APPEND` are Write; others are ignored.
  - The retry event is recorded wherever `crate::stats::STATS.retries.inc()` is.

- [ ] **Step 1: Failing tests**
  - `latency_is_median_ttfb`: GET ttfb samples 10, 20, 30, 1000 ms give 25 ms ± 5.
  - `read_stream_ignores_small_gets`: 4 KiB GETs only give `read_stream` = prev.
  - `read_stream_p90`: 64 GETs of 1 MiB, body times from 10 ms to 640 ms, give ≈ 1 MiB / 64 ms (p90 of rates, ±10 %).
  - `write_stream_from_line`: latency 30 ms known; PUTs of 4 MiB in 30 + 100 ms give ≈ 40 MiB/s (±5 %).
  - `seed_replaces`: after `seed`, `estimate` returns the seeded values until new samples arrive.
- [ ] **Step 2:** `cargo test estimate`. Expect FAIL.
- [ ] **Step 3: Implement.** The hooks in s3.rs measure `Instant` before `send`, after the headers (`send` returned) and after the body.
- [ ] **Step 4:** `cargo test`. PASS.
- [ ] **Step 5: Commit** `Passive link estimates from S3 request timings`.

### Task 5: Connection regulator

**Files:**
- Create: `src/regulator.rs`.
- Test: `src/regulator.rs` tests.

**Interfaces:**
- Produces:
  - `pub struct Regulator`;
  - `new() -> Self` (limit 1, slow start);
  - `fn step(&mut self, s: Sample) -> usize`, which returns the new limit, with `pub struct Sample { bytes_per_sec: f64, saturated: bool, retries: u64, now_secs: f64 }`;
  - `fn limit(&self) -> usize`;
  - `fn best_rate(&self) -> Option<f64>`: the throughput at the best limit, which is the auto `read_total`/`write_total`;
  - `fn phase(&self) -> &'static str`: one of `"slow start"`, `"probing"`, `"steady"`, `"backed off"`;
  - `fn last_change(&self) -> Option<(f64, String)>`: time and reason, for the page;
  - `fn restart_from(&mut self, n: usize)`: after a measurement.
- Rules are in Global Constraints. Steps that are not saturated change nothing.

- [ ] **Step 1: Failing tests** with a synthetic link `rate(n) = min(n × stream, total)`, stream 3 MiB/s, total 24 MiB/s, one step per simulated second:
  - `grows_to_saturation`: from 1, within 10 steps, the limit is in 8..=16 and `best_rate` is ≈ 24 MiB/s ± 10 %.
  - `doubling_without_gain_reverts`: total 24, stream 12: it settles at 2, not 4.
  - `probe_every_30s`: at the steady state the limit changes at most once per 30 s, and a probe without gain reverts.
  - `drop_halves`: after settling, total drops to 6 MiB/s: within 3 steps the limit is ≤ half of before, then it settles again near 2.
  - `retry_halves`: a sample with `retries > 0` halves the limit.
  - `idle_freezes`: 100 unsaturated steps leave the limit and `best_rate` unchanged.
  - `cap_64`: with an infinite total it never exceeds 64.
- [ ] **Step 2:** `cargo test regulator`. Expect FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** `cargo test regulator`. PASS.
- [ ] **Step 5: Commit** `Connection regulator: slow start, probing, back-off`.

### Task 6: Pause gate in Store

**Files:**
- Modify: `src/store.rs`.
- Test: `src/store.rs` tests.

**Interfaces:**
- Produces:
  - Every public async `Store` operation holds `self.gate.read().await` (a `tokio::sync::RwLock<()>`) for its duration and counts itself in `in_flight: AtomicUsize`.
  - `pub async fn pause(&self) -> tokio::sync::RwLockWriteGuard<'_, ()>`.
  - `pub fn in_flight(&self) -> usize`.
  - `pub fn s3(&self) -> Option<&S3Store>`: the measurement talks to S3 directly, past the gate.

- [ ] **Step 1: Failing tests** on `MemStore`:
  - `pause_waits_for_in_flight`: start an op that blocks; `pause()` is pending until the op completes.
  - `ops_queue_behind_pause`: hold the pause guard; a new `put` is pending; drop the guard; the put completes.
  - `pause_released_on_panic`: a task takes `pause()` and panics; afterwards a `put` completes within 1 s. (Review Focus 4.)
- [ ] **Step 2:** `cargo test store`. Expect FAIL.
- [ ] **Step 3: Implement.** For an op blocked in tests, add a `#[cfg(test)]` hook to `MemStore` or use the `fail` latch pattern already there.
- [ ] **Step 4:** `cargo test`. PASS.
- [ ] **Step 5: Commit** `Store: pause gate for link measurements`.

### Task 7: Measurement on existing objects, with a budget; leftover cleanup

**Files:**
- Modify: `src/measure.rs`, `src/store.rs` (`Layout::measure_dir_prefix()`, `Layout::is_measure_key(&str)`), `src/zones.rs` (restore and candidates), `src/main.rs` (`destroy_prefix`, `cmd_measure`).
- Test: `src/measure.rs`, `src/zones.rs` tests.

**Interfaces:**
- Consumes: `Store::s3`, `tuning::Chars`, `linkconf::render`.
- Produces:
  - `pub struct Plan { reads: Vec<(String, u64)>, write_dir: Option<String>, budget: Option<Duration> }`.
  - `pub async fn run(s3: &S3Store, plan: Plan) -> anyhow::Result<Measured>` with `pub struct Measured { chars: Chars, elapsed: Duration, skipped: Vec<&'static str> }`. A part that is not measured stays `None` and is named in `skipped`.
  - `Engine::measure_candidates(&self) -> Vec<(String, u64)>`: the key and size of every zone stored in exactly one object with `flushed > 0`.
  - `measure::candidates_from_listing(layout, objects, segments: bool) -> Vec<(String, u64)>` for the CLI. For a SEGMENTS disk, zones whose only object is `-00000000`.
- **Budget split** when `Some(20 s)`:
  - latency 3 s;
  - read stream 4 s;
  - read total 4 s;
  - write stream 4 s;
  - write total 4 s;
  - cleanup takes the rest.
- Each phase stops at its deadline with whatever it has; a phase with zero samples is skipped. Ranges are random and block-aligned inside objects. A 404 on a candidate drops it and picks another. The read stream pair uses a GET of min(16 MiB, object size).

- [ ] **Step 1: Failing tests**
  - `candidates_skip_fragmented`: a SEGMENTS engine with zone 0 in one object and zone 1 in two segments: candidates contain zone 0's key only.
  - `candidates_from_listing_matches_engine` on the same objects.
  - `restore_deletes_measure_leftovers`: put `t/measure-x/a` in a MemStore, open the engine: the object is gone and no "unexpected object" is logged (check that the key is gone). The read-only open keeps it.
  - `destroy_deletes_measure_leftovers` in main.rs tests through `destroy_prefix` on an S3-less helper, or by unit-testing the key filter `Layout::is_measure_key` and the filter used by `destroy_prefix`. (Review Focus 1.)
  - `no_candidates_skips_reads`: `run` with empty reads and `write_dir` = None, on an `S3Store` pointed at `http://127.0.0.1:9` (no request may be sent), gives `chars` all None and `skipped` = the five phases, not an error. (Review Focus 3.)
- [ ] **Step 2:** `cargo test measure zones`. Expect FAIL.
- [ ] **Step 3: Implement.** `cmd_measure` lists the disk, builds candidates, writes under a temp dir, runs without a budget and prints `linkconf::render` of a `Manual` holding the measured chars. The old create-objects-to-read code goes away.
- [ ] **Step 4:** `cargo test`. PASS. `cargo test -- --ignored measure` with `creds.json` against `ideco-bucket-test`: the output is YAML, and no `measure-` objects remain afterwards (`aws s3 ls` via the scratchpad helper).
- [ ] **Step 5: Commit** `measure: read existing objects, time budget, YAML output; clean leftovers at start and destroy`.

### Task 8: Autotune runtime and `--link-config` wiring

**Files:**
- Create: `src/autotune.rs`.
- Modify:
  - `src/main.rs`: remove `LinkArgs` s3 flags and `--read-cache`, add `--link-config`; `ServeOptions` carries `Manual`; spawn autotune instead of the one-time tuning;
  - `src/device.rs`: `keep_fs_tuned` follows an `Arc<AtomicU64>` target, 0 meaning don't touch, and rewrites only when the target changes or a new mount appears.
- Test: `src/autotune.rs` tests.

**Interfaces:**
- Consumes: Tasks 1–7.
- Produces:
  - `pub struct Autotune` (in an `Arc`), with:
    - `new(engine, manual: Manual, zone_size, fs_target: Arc<AtomicU64>, read_only: bool)`;
    - `async fn run(self: Arc<Self>)`: the 100 ms ticker and the 1 s step;
    - `fn snapshot(&self) -> LinkState` (Serialize);
    - `fn set(&self, key: &str, value: &str) -> Result<(), String>`, where `"auto"` clears the key;
    - `fn measure(self: &Arc<Self>) -> Result<(), String>`, which starts it or errors if already running.
  - `LinkState` has, per key:
    - `{auto, manual, effective}` as strings in file units, plus raw numbers for the page;
    - `read_cache`;
    - regulator phase, limit and last change per direction;
    - `measure: {state: "idle"|"waiting"|"measuring"|"done"|"failed", waiting_ops, phase, started, result, error}`;
    - `yaml: linkconf::render(&manual)`.
- **Per step:**
  - a saturated tick means `limiter.in_use() >= limiter.limit()`; a second is saturated if ≥ 80 % of its ticks were;
  - bytes per second come from the deltas of `STATS.read_from_s3` and the uploaded-bytes counter;
  - feed the regulators with `OBS.take_retries`;
  - `auto.chars` = `OBS.estimate(prev)` with totals from `best_rate`;
  - `resolve`, then apply the changed values (`set_uploads`, `set_reads`, `set_get_part`, `set_read_ahead`, `fs_target`) and log each change at info with its reason.
- **Measure:**
  - `store.pause()` while reporting `waiting_ops = store.in_flight()`;
  - then `measure::run` with candidates, `write_dir` (None if read-only) and a 20 s budget;
  - then `OBS.seed`, and `restart_from` of both regulators at the formula value;
  - drop the guard in every outcome;
  - errors go to `STATS` last errors as a new `err_measure` (follow `err_upload`).
- **`create`'s temporary device** runs without autotune: fixed 16 slots, as today.

- [ ] **Step 1: Failing tests** with a MemStore engine and a manual clock or `tokio::time::pause`:
  - `manual_value_applies`: `Manual{uploads: Some(8)}` gives `engine.upload_limiter().limit()` = 8 after one step, and `snapshot().uploads.auto` shows the regulator's value.
  - `set_auto_returns_to_regulator`: `set("uploads", "8")` then `set("uploads", "auto")` gives the regulator's limit.
  - `set_rejects_bad`: `set("uploads", "0")` errors; `set("nope", "1")` errors.
  - `measure_twice_errors`: a second `measure()` while the first is waiting gives `Err("a measurement is already running")`.
  - `measure_read_only_no_write`: with a read-only store, the snapshot after the measurement shows writes skipped (Mem backend: `measure` errors "measurement needs S3"; assert the message and that the pause is released).
- [ ] **Step 2:** `cargo test autotune`. Expect FAIL.
- [ ] **Step 3: Implement.** Fix `packaging/btrs3fs@.service`'s `EXTRA_ARGS` comment to say `--link-config`.
- [ ] **Step 4:** `cargo test`, `cargo clippy --all-targets` (0 warnings), `cargo fmt`. `./target/debug/btrs3fs mount --help` lists `--link-config` and no `--s3-*`.
- [ ] **Step 5: Commit** `Autotune: live link estimates, regulated connections, --link-config`.

### Task 9: HTTP API

**Files:**
- Modify: `src/stats.rs` (`serve_http`: the action gets `(name, query)`), `src/main.rs` (actions and json).
- Test: `src/stats.rs` server tests.

**Interfaces:**
- Produces:
  - `serve_http(port, json: Fn(&str)->Option<String>, action: Fn(&str, &str)->Option<String>)`;
  - `GET /link.json` gives `Autotune::snapshot`;
  - `POST /link?<key>=<value|auto>[&...]` applies each pair in order and answers `{"ok":true}` or `{"error":"<key>: <reason>"}`, applying nothing on the first error. Query values are percent-decoded; reuse the decoder if one exists, otherwise a small `percent_decode` in stats.rs with a test;
  - `POST /measure` gives `{"started":true}` or `{"error":...}`;
  - on a read-only disk `/link` and `/measure` are allowed (a measurement reads only there); the other actions stay refused.
- [ ] **Step 1: Failing tests:** `post_link_sets` (the action closure sees name `link`, query `uploads=8`), `get_link_405_free` (GET `/link.json` gives 200), `percent_decode("38%6Ds")` = `"38ms"`.
- [ ] **Step 2–4:** red, implement, green (`cargo test stats`).
- [ ] **Step 5: Commit** `Statistics API: link state, manual settings, measure`.

### Task 10: Page section "S3 link"

**Files:**
- Modify: `src/stats.html`.

**Interfaces:**
- Consumes the Task 9 API.
- Produces:
  - a sidebar tab `link` (`#link`);
  - two tables with the columns name, auto, manual (an auto/manual checkbox plus an input), effective. Rates are shown in Mbit/s like the rest of the page; inputs are in file units;
  - the `read_cache` row;
  - the regulator lines per direction (phase, limit, last change and reason);
  - a "Measure now" button with the live state (`waiting for N operations` → phase → result and elapsed), errors as toast;
  - a YAML `<pre>` with a "Copy" button (`navigator.clipboard.writeText`, with a fallback that selects the text);
  - on the Overview S3 card, the current uploads and GETs with `auto`/`manual`.
- Toggling to manual pre-fills the input with the current effective value; Enter or blur posts `/link?key=value`. Unchecking posts `key=auto`. Errors show as toast and the row reverts.

- [ ] **Step 1:** Implement.
- [ ] **Step 2:** `node --check` on the extracted script (as in the earlier page checks); `cargo test` (the page is embedded).
- [ ] **Step 3:** Render with the scratchpad harness against a canned `link.json` and look at the screenshot: desktop and phone width, light and dark.
- [ ] **Step 4: Commit** `Statistics page: S3 link section`.

### Task 11: Docs and laptop check

**Files:**
- Modify: `manual.md` (§5 command table and flags: `--link-config`, no `--s3-*`; §5.1 measuring and the YAML; §14 the S3 link section), `SPEC.md` (§6.1 decisions from Task 4, §9.3 page section, §12 laptop result), `PLAN.md` (mark done).

- [ ] **Step 1:** Update the docs; no duplication (user-facing in manual, design in SPEC, status in PLAN).
- [ ] **Step 2:** `cargo test`, `cargo clippy --all-targets`. All green.
- [ ] **Step 3: Commit** `docs: link autotuning`.
- [ ] **Step 4:** Ask the user to remount. Then:
  - remount without `--link-config`;
  - copy a 1 GiB file to `/mnt/btrs3fs` and watch `/link.json`: uploads grow from 1 and settle;
  - press "Measure now" during a read and compare with the old laptop flags (38ms/33M/24M);
  - record the result in SPEC §12 and commit.

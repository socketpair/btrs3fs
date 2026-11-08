//! Object storage abstraction: zone objects (one per zone, or segments
//! `<zone>-<offset>` on SEGMENTS disks) plus `disk.json`.
//!
//! `MemStore` mirrors the semantics observed on Yandex Object Storage
//! (see SPEC.md §11): PATCH of a missing object fails with "not found",
//! PATCH starting past the end of the object fails with "invalid range",
//! PATCH may overlap the tail and extend the object.

use bytes::Bytes;
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("object {0} not found")]
    NotFound(String),
    #[error("invalid range for object {key}: {detail}")]
    InvalidRange { key: String, detail: String },
    /// The S3 service does not implement the request (HTTP 405 or 501).
    #[error("{0}")]
    Unsupported(String),
    /// A change asked of a store opened read-only (SPEC.md §9.2).
    #[error("the store is read-only")]
    ReadOnly,
    /// With the whole chain of causes: "giving up" alone says nothing.
    #[error("{0:#}")]
    Other(#[from] anyhow::Error),
}

pub type StoreResult<T> = Result<T, StoreError>;

/// Object body as a list of chunks, sent without concatenation.
#[derive(Clone, Default, Debug)]
pub struct Body {
    pub chunks: Vec<Bytes>,
}

impl Body {
    pub fn new(chunks: Vec<Bytes>) -> Self {
        Self { chunks }
    }

    pub fn len(&self) -> u64 {
        self.chunks.iter().map(|c| c.len() as u64).sum()
    }

    pub fn to_vec(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.len() as usize);
        for c in &self.chunks {
            out.extend_from_slice(c);
        }
        out
    }
}

pub struct ObjectInfo {
    pub key: String,
    pub size: u64,
}

/// A server-side concatenation whose parts are copied but which is not
/// completed yet (SPEC.md §5): finishing makes the object appear, replacing
/// any object of that key atomically; aborting leaves nothing.
pub enum PendingConcat {
    Mem { dst: String, data: Vec<u8> },
    S3(crate::s3::PendingConcat),
}

/// A source range of a concatenation: (key, offset, length).
pub type ConcatPart = (String, u64, u64);

pub enum Backend {
    Mem(MemStore),
    S3(crate::s3::S3Store),
}

/// The object store of a disk. A read-only store refuses every change with
/// `StoreError::ReadOnly`: a disk served read-only only GETs and LISTs
/// (SPEC.md §9.2), whatever path asks for a change.
pub struct Store {
    backend: Backend,
    read_only: bool,
    /// Every operation holds it shared; a link measurement holds it alone
    /// (SPEC.md §6.1). Fair: operations started after the measurement asked
    /// wait behind it.
    gate: tokio::sync::RwLock<()>,
    in_flight: std::sync::atomic::AtomicUsize,
}

/// An operation in flight: lets a measurement wait for it.
pub struct Op<'a> {
    _shared: tokio::sync::RwLockReadGuard<'a, ()>,
    count: &'a std::sync::atomic::AtomicUsize,
}

impl Drop for Op<'_> {
    fn drop(&mut self) {
        self.count.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Store {
    pub fn new(backend: Backend) -> Self {
        Self {
            backend,
            read_only: false,
            gate: tokio::sync::RwLock::new(()),
            in_flight: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    pub fn mem(m: MemStore) -> Self {
        Self::new(Backend::Mem(m))
    }

    pub fn s3(s: crate::s3::S3Store) -> Self {
        Self::new(Backend::S3(s))
    }

    /// The same store, refusing every change.
    pub fn read_only(self) -> Self {
        Self {
            read_only: true,
            ..self
        }
    }

    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    pub fn backend(&self) -> &Backend {
        &self.backend
    }

    /// Enters an operation: waits while a measurement runs.
    pub async fn op(&self) -> Op<'_> {
        let shared = self.gate.read().await;
        self.in_flight.fetch_add(1, Ordering::SeqCst);
        Op {
            _shared: shared,
            count: &self.in_flight,
        }
    }

    /// Waits for every operation in flight; new ones wait until the guard
    /// is dropped (also when its holder panics).
    pub async fn pause(&self) -> tokio::sync::RwLockWriteGuard<'_, ()> {
        self.gate.write().await
    }

    /// Operations in flight now.
    pub fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::SeqCst)
    }

    /// The S3 store, for the measurement, which talks to it past the gate.
    pub fn s3_store(&self) -> Option<&crate::s3::S3Store> {
        match &self.backend {
            Backend::S3(s) => Some(s),
            Backend::Mem(_) => None,
        }
    }

    fn writable(&self) -> StoreResult<()> {
        if self.read_only {
            return Err(StoreError::ReadOnly);
        }
        Ok(())
    }

    /// Whole object, `None` if it does not exist.
    #[cfg(test)]
    pub async fn get(&self, key: &str) -> StoreResult<Option<Bytes>> {
        let _op = self.op().await;
        self.interleave().await;
        let res = match &self.backend {
            Backend::Mem(m) => m.get(key),
            Backend::S3(s) => s.get(key).await,
        };
        match res {
            Ok(b) => Ok(Some(b)),
            Err(StoreError::NotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// In tests every in-memory operation first yields to the scheduler, so
    /// concurrent tasks interleave at each store call as they do over a
    /// network; a no-op otherwise.
    async fn interleave(&self) {
        #[cfg(test)]
        if matches!(self.backend, Backend::Mem(_)) {
            tokio::task::yield_now().await;
        }
    }

    /// Creates or replaces an object.
    pub async fn put(&self, key: &str, body: Body) -> StoreResult<()> {
        let _op = self.op().await;
        self.writable()?;
        self.interleave().await;
        match &self.backend {
            Backend::Mem(m) => m.put(key, body),
            Backend::S3(s) => s.put(key, body).await,
        }
    }

    /// Grows a zone object: `offset` 0 creates it, else `body` goes at
    /// `offset` = the object's size, the way the disk appends (SPEC.md §5).
    pub async fn append(&self, key: &str, offset: u64, body: Body) -> StoreResult<()> {
        let _op = self.op().await;
        self.writable()?;
        self.interleave().await;
        match &self.backend {
            Backend::Mem(m) if offset == 0 => m.put(key, body),
            Backend::Mem(m) => m.patch(key, offset, body),
            Backend::S3(s) => s.append(key, offset, body).await,
        }
    }

    /// Exactly `len` bytes starting at `offset`.
    pub async fn get_range(&self, key: &str, offset: u64, len: u64) -> StoreResult<Bytes> {
        let _op = self.op().await;
        self.interleave().await;
        match &self.backend {
            Backend::Mem(m) => m.get_range(key, offset, len),
            Backend::S3(s) => s.get_range(key, offset, len).await,
        }
    }

    /// Deletes an object; deleting a missing object is not an error.
    pub async fn delete(&self, key: &str) -> StoreResult<()> {
        let _op = self.op().await;
        self.writable()?;
        self.interleave().await;
        match &self.backend {
            Backend::Mem(m) => m.delete(key),
            Backend::S3(s) => s.delete(key).await,
        }
    }

    /// Deletes many objects with few requests; missing objects are not an error.
    pub async fn delete_many(&self, keys: &[String]) -> StoreResult<()> {
        let _op = self.op().await;
        self.writable()?;
        self.interleave().await;
        match &self.backend {
            // DeleteObjects keeps no order; backwards is the worst case for a
            // zone, its start (the lowest key) going last.
            Backend::Mem(m) => keys.iter().rev().try_for_each(|k| m.delete(k)),
            Backend::S3(s) => s.delete_many(keys).await,
        }
    }

    /// All objects whose key starts with `prefix`.
    pub async fn list(&self, prefix: &str) -> StoreResult<Vec<ObjectInfo>> {
        let _op = self.op().await;
        self.interleave().await;
        match &self.backend {
            Backend::Mem(m) => m.list(prefix),
            Backend::S3(s) => s.list(prefix).await,
        }
    }

    /// Copies `parts` (ranges of existing objects) on the server into a new
    /// object `dst` that appears only on `finish_concat`.
    pub async fn begin_concat(
        &self,
        dst: &str,
        parts: &[ConcatPart],
    ) -> StoreResult<PendingConcat> {
        let _op = self.op().await;
        self.writable()?;
        self.interleave().await;
        match &self.backend {
            Backend::Mem(m) => {
                let mut data = Vec::new();
                for (key, offset, len) in parts {
                    data.extend_from_slice(&m.get_range(key, *offset, *len)?);
                }
                Ok(PendingConcat::Mem {
                    dst: dst.to_string(),
                    data,
                })
            }
            Backend::S3(s) => Ok(PendingConcat::S3(s.begin_concat(dst, parts).await?)),
        }
    }

    pub async fn finish_concat(&self, pending: PendingConcat) -> StoreResult<()> {
        let _op = self.op().await;
        self.writable()?;
        self.interleave().await;
        match (&self.backend, pending) {
            (Backend::Mem(m), PendingConcat::Mem { dst, data }) => {
                m.put(&dst, Body::new(vec![Bytes::from(data)]))
            }
            (Backend::S3(s), PendingConcat::S3(p)) => s.finish_concat(p).await,
            _ => unreachable!("a concatenation belongs to its store"),
        }
    }

    /// Drops a concatenation that must not complete (best effort).
    pub async fn abort_concat(&self, pending: PendingConcat) {
        let _op = self.op().await;
        if let (Backend::S3(s), PendingConcat::S3(p)) = (&self.backend, pending) {
            s.abort_concat(p).await;
        }
    }

    /// Aborts concatenations of the disk a crash left unfinished; how many.
    pub async fn abort_stale_concats(&self, layout: &Layout) -> StoreResult<usize> {
        let _op = self.op().await;
        self.writable()?;
        self.interleave().await;
        match &self.backend {
            Backend::Mem(_) => Ok(0),
            Backend::S3(s) => s.abort_stale_concats(layout).await,
        }
    }
}

#[derive(Default)]
struct MemObject {
    data: Vec<u8>,
}

/// In-memory store for tests and `run --memory`.
#[derive(Default)]
pub struct MemStore {
    objects: Mutex<BTreeMap<String, MemObject>>,
    fail: AtomicBool,
    /// Deletes of keys containing this fail (tests).
    fail_deletes_of: Mutex<Option<String>>,
    range_gets: AtomicU64,
}

impl MemStore {
    /// Makes every subsequent operation fail (failure injection for tests).
    #[cfg(test)]
    pub fn set_fail(&self, fail: bool) {
        self.fail.store(fail, Ordering::SeqCst);
    }

    /// Makes deletes of keys containing `part` fail; `None` stops it. A batch
    /// deletes the keys before the first failing one, as DeleteObjects may.
    #[cfg(test)]
    pub fn fail_deletes_of(&self, part: Option<&str>) {
        *self.fail_deletes_of.lock().unwrap() = part.map(str::to_string);
    }

    /// Ranged GETs served so far (tests count requests with it).
    #[cfg(test)]
    pub fn range_gets(&self) -> u64 {
        self.range_gets.load(Ordering::SeqCst)
    }

    fn check_fail(&self) -> StoreResult<()> {
        if self.fail.load(Ordering::SeqCst) {
            return Err(anyhow::anyhow!("injected store failure").into());
        }
        Ok(())
    }

    #[cfg(test)]
    fn get(&self, key: &str) -> StoreResult<Bytes> {
        self.check_fail()?;
        let objects = self.objects.lock().unwrap();
        let obj = objects
            .get(key)
            .ok_or_else(|| StoreError::NotFound(key.to_string()))?;
        Ok(Bytes::copy_from_slice(&obj.data))
    }

    fn put(&self, key: &str, body: Body) -> StoreResult<()> {
        self.check_fail()?;
        let obj = MemObject {
            data: body.to_vec(),
        };
        self.objects.lock().unwrap().insert(key.to_string(), obj);
        Ok(())
    }

    fn patch(&self, key: &str, offset: u64, body: Body) -> StoreResult<()> {
        self.check_fail()?;
        let mut objects = self.objects.lock().unwrap();
        let data = &mut objects
            .get_mut(key)
            .ok_or_else(|| StoreError::NotFound(key.to_string()))?
            .data;
        if offset > data.len() as u64 {
            return Err(StoreError::InvalidRange {
                key: key.to_string(),
                detail: format!("patch offset {offset} beyond object size {}", data.len()),
            });
        }
        let offset = offset as usize;
        let body = body.to_vec();
        let end = offset + body.len();
        if end > data.len() {
            data.resize(end, 0);
        }
        data[offset..end].copy_from_slice(&body);
        Ok(())
    }

    fn get_range(&self, key: &str, offset: u64, len: u64) -> StoreResult<Bytes> {
        self.check_fail()?;
        self.range_gets.fetch_add(1, Ordering::SeqCst);
        let objects = self.objects.lock().unwrap();
        let data = &objects
            .get(key)
            .ok_or_else(|| StoreError::NotFound(key.to_string()))?
            .data;
        let end = offset
            .checked_add(len)
            .filter(|&e| e <= data.len() as u64)
            .ok_or_else(|| StoreError::InvalidRange {
                key: key.to_string(),
                detail: format!("range {offset}+{len} beyond object size {}", data.len()),
            })?;
        Ok(Bytes::copy_from_slice(&data[offset as usize..end as usize]))
    }

    fn delete(&self, key: &str) -> StoreResult<()> {
        self.check_fail()?;
        if let Some(part) = &*self.fail_deletes_of.lock().unwrap()
            && key.contains(part.as_str())
        {
            return Err(anyhow::anyhow!("injected failure deleting {key}").into());
        }
        self.objects.lock().unwrap().remove(key);
        Ok(())
    }

    fn list(&self, prefix: &str) -> StoreResult<Vec<ObjectInfo>> {
        self.check_fail()?;
        let objects = self.objects.lock().unwrap();
        Ok(objects
            .range(prefix.to_string()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| ObjectInfo {
                key: k.clone(),
                size: v.data.len() as u64,
            })
            .collect())
    }
}

/// Object key layout inside the bucket (SPEC.md §3.1).
#[derive(Clone, Debug)]
pub struct Layout {
    prefix: String,
}

impl Layout {
    pub fn new(prefix: &str) -> anyhow::Result<Self> {
        let prefix = prefix.trim_end_matches('/');
        anyhow::ensure!(!prefix.is_empty(), "prefix must not be empty");
        anyhow::ensure!(!prefix.starts_with('/'), "prefix must not start with '/'");
        // `.` and `..` are folded by URL parsing after signing (the signature
        // then fails); empty parts and control characters make odd keys.
        anyhow::ensure!(
            prefix
                .split('/')
                .all(|part| !matches!(part, "" | "." | ".."))
                && !prefix.chars().any(char::is_control),
            "prefix {prefix:?} must not have empty, `.` or `..` parts or control characters"
        );
        Ok(Self {
            prefix: prefix.to_string(),
        })
    }

    /// The prefix without a trailing `/`.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// `<prefix>/` — every object of the disk starts with it.
    pub fn dir(&self) -> String {
        format!("{}/", self.prefix)
    }

    pub fn config_key(&self) -> String {
        format!("{}/disk.json", self.prefix)
    }

    /// Zone object name: byte offset of the zone in hex with leading zeros
    /// (requirement of the task), 16 digits — the width of a u64 — so names
    /// sort by offset; lowercase is our choice (SPEC.md §3.1).
    pub fn zone_key(&self, zone_offset: u64) -> String {
        format!("{}/{zone_offset:016x}", self.prefix)
    }

    /// Empty object marking a zone closed by FINISH: `<zone key>.finished`,
    /// next to the zone object (SPEC.md §3.3). A marker rather than an object
    /// tag: not every S3 keeps tags, and the listing shows it for free.
    pub fn finished_key(&self, zone_offset: u64) -> String {
        format!("{}{FINISHED_SUFFIX}", self.zone_key(zone_offset))
    }

    /// Empty object marking a metadata zone of a SEGMENTS disk:
    /// `<zone key>.meta`, so its concatenation policy survives a restart
    /// (SPEC.md §5).
    pub fn meta_key(&self, zone_offset: u64) -> String {
        format!("{}{META_SUFFIX}", self.zone_key(zone_offset))
    }

    /// Byte offset of the zone if `key` is a metadata marker of this disk.
    pub fn parse_meta_key(&self, key: &str) -> Option<u64> {
        self.parse_zone_key(key.strip_suffix(META_SUFFIX)?)
    }

    /// Byte offset of the zone if `key` is a zone object of this disk.
    pub fn parse_zone_key(&self, key: &str) -> Option<u64> {
        let name = key.strip_prefix(&self.prefix)?.strip_prefix('/')?;
        // Exactly what zone_key() writes: 16 lowercase hex digits.
        if name.len() != 16 || !name.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
            return None;
        }
        u64::from_str_radix(name, 16).ok()
    }

    /// Segment of a SEGMENTS disk: `<zone key>-<offset in the zone>`, the
    /// offset in 8 lowercase hex digits (a zone is at most 32 MiB). All objects
    /// of a zone share the prefix `zone_key()` (SPEC.md §3.1).
    pub fn segment_key(&self, zone_offset: u64, offset: u64) -> String {
        format!("{}-{offset:08x}", self.zone_key(zone_offset))
    }

    /// (zone offset, offset in the zone) if `key` is a segment of this disk.
    pub fn parse_segment_key(&self, key: &str) -> Option<(u64, u64)> {
        let (zone, offset) = key.rsplit_once('-')?;
        if offset.len() != 8
            || !offset
                .bytes()
                .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
        {
            return None;
        }
        Some((
            self.parse_zone_key(zone)?,
            u64::from_str_radix(offset, 16).ok()?,
        ))
    }

    /// Byte offset of the zone if `key` is a FINISH marker of this disk.
    pub fn parse_finished_key(&self, key: &str) -> Option<u64> {
        self.parse_zone_key(key.strip_suffix(FINISHED_SUFFIX)?)
    }

    /// Byte offset of the zone if `key` is any object of a zone of this
    /// disk: what deleting by prefix may touch. Objects of another disk
    /// nested under this prefix (`<prefix>/x/...`) are not.
    pub fn zone_of(&self, key: &str) -> Option<u64> {
        self.parse_zone_key(key)
            .or_else(|| self.parse_segment_key(key).map(|(zone, _)| zone))
            .or_else(|| self.parse_finished_key(key))
            .or_else(|| self.parse_meta_key(key))
    }

    /// Prefixes of the disks this one would nest in: `a` and `a/b` for `a/b/c`.
    pub fn ancestors(&self) -> Vec<&str> {
        self.prefix
            .match_indices('/')
            .map(|(i, _)| &self.prefix[..i])
            .collect()
    }
}

const FINISHED_SUFFIX: &str = ".finished";
const META_SUFFIX: &str = ".meta";

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    async fn pending<F: std::future::Future>(f: F) -> bool {
        tokio::time::timeout(Duration::from_millis(30), f)
            .await
            .is_err()
    }

    fn one() -> Body {
        Body::new(vec![Bytes::from_static(b"x")])
    }

    #[tokio::test]
    async fn pause_waits_for_in_flight() {
        let store = Store::mem(MemStore::default());
        let op = store.op().await; // an operation in flight
        assert_eq!(store.in_flight(), 1);
        assert!(pending(store.pause()).await);
        drop(op);
        let _paused = store.pause().await;
        assert_eq!(store.in_flight(), 0);
    }

    #[tokio::test]
    async fn ops_queue_behind_pause() {
        let store = Arc::new(Store::mem(MemStore::default()));
        let paused = store.pause().await;
        let put = tokio::spawn({
            let store = store.clone();
            async move { store.put("k", one()).await }
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!put.is_finished(), "an operation ran during the pause");
        drop(paused);
        put.await.unwrap().unwrap();
        assert_eq!(store.list("k").await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn pause_released_on_panic() {
        let store = Arc::new(Store::mem(MemStore::default()));
        let measuring = tokio::spawn({
            let store = store.clone();
            async move {
                let _paused = store.pause().await;
                panic!("measurement failed");
            }
        });
        assert!(measuring.await.is_err());
        tokio::time::timeout(Duration::from_secs(1), store.put("k", one()))
            .await
            .expect("the pause is gone")
            .unwrap();
    }

    use super::*;

    #[test]
    fn prefixes() {
        assert!(Layout::new("disks/data/").is_ok());
        for bad in ["", "/a", "a//b", "a/./b", "a/..", "a\nb"] {
            assert!(Layout::new(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn keys_of_the_disk_and_nested_disks() {
        let l = Layout::new("a/b").unwrap();
        assert_eq!(l.ancestors(), ["a"]);
        assert!(Layout::new("a").unwrap().ancestors().is_empty());
        for key in [
            "a/b/0000000001000000",
            "a/b/0000000001000000-00001000",
            "a/b/0000000001000000.finished",
            "a/b/0000000001000000.meta",
        ] {
            assert_eq!(l.zone_of(key), Some(1 << 24), "{key}");
        }
        for key in [
            "a/b/disk.json",
            "a/b/c/0000000001000000",
            "a/b/c/disk.json",
            "a/b/0000000001000000-x",
        ] {
            assert_eq!(l.zone_of(key), None, "{key}");
        }
    }

    fn body(data: &[u8]) -> Body {
        Body::new(vec![Bytes::copy_from_slice(data)])
    }

    #[test]
    fn mem_patch_semantics() {
        let m = MemStore::default();
        assert!(matches!(
            m.patch("k", 0, body(b"x")),
            Err(StoreError::NotFound(_))
        ));
        m.put("k", body(b"aaaa")).unwrap();
        m.patch("k", 4, body(b"bb")).unwrap();
        m.patch("k", 4, body(b"bb")).unwrap(); // idempotent repeat
        m.patch("k", 5, body(b"cc")).unwrap(); // overlap + extend
        assert!(matches!(
            m.patch("k", 99, body(b"d")),
            Err(StoreError::InvalidRange { .. })
        ));
        assert_eq!(&m.get("k").unwrap()[..], b"aaaabcc");
        assert_eq!(&m.get_range("k", 3, 2).unwrap()[..], b"ab");
        assert!(m.get_range("k", 6, 2).is_err());
    }

    #[test]
    fn layout_keys() {
        let l = Layout::new("disks/test/").unwrap();
        assert_eq!(l.zone_key(32 << 20), "disks/test/0000000002000000");
        assert_eq!(
            l.parse_zone_key("disks/test/0000000002000000"),
            Some(32 << 20)
        );
        assert_eq!(l.parse_zone_key("disks/test/disk.json"), None);
        assert_eq!(l.parse_zone_key("disks/test2/0000000002000000"), None);
        assert_eq!(
            l.finished_key(32 << 20),
            "disks/test/0000000002000000.finished"
        );
        assert_eq!(
            l.parse_finished_key("disks/test/0000000002000000.finished"),
            Some(32 << 20)
        );
        assert_eq!(l.parse_finished_key("disks/test/0000000002000000"), None);
        assert_eq!(
            l.parse_zone_key("disks/test/0000000002000000.finished"),
            None
        );
        assert_eq!(
            l.segment_key(32 << 20, 0x340000),
            "disks/test/0000000002000000-00340000"
        );
        assert_eq!(
            l.parse_segment_key("disks/test/0000000002000000-00340000"),
            Some((32 << 20, 0x340000))
        );
        assert_eq!(l.parse_segment_key("disks/test/0000000002000000"), None);
        assert_eq!(
            l.parse_segment_key("disks/test/0000000002000000-340000"),
            None
        );
        assert_eq!(
            l.parse_zone_key("disks/test/0000000002000000-00340000"),
            None
        );
        assert!(Layout::new("/").is_err());
    }
}

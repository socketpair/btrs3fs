//! Object storage abstraction: one object per zone plus `disk.json`.
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
    #[error(transparent)]
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

pub enum Store {
    Mem(MemStore),
    S3(crate::s3::S3Store),
}

impl Store {
    /// Whole object, `None` if it does not exist.
    #[cfg(test)]
    pub async fn get(&self, key: &str) -> StoreResult<Option<Bytes>> {
        let res = match self {
            Store::Mem(m) => m.get(key),
            Store::S3(s) => s.get(key).await,
        };
        match res {
            Ok(b) => Ok(Some(b)),
            Err(StoreError::NotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Creates or replaces an object.
    pub async fn put(&self, key: &str, body: Body) -> StoreResult<()> {
        match self {
            Store::Mem(m) => m.put(key, body),
            Store::S3(s) => s.put(key, body).await,
        }
    }

    /// Writes `body` at `offset` of an existing object; `offset` must not exceed its size.
    pub async fn patch(&self, key: &str, offset: u64, body: Body) -> StoreResult<()> {
        match self {
            Store::Mem(m) => m.patch(key, offset, body),
            Store::S3(s) => s.patch(key, offset, body).await,
        }
    }

    /// Exactly `len` bytes starting at `offset`.
    pub async fn get_range(&self, key: &str, offset: u64, len: u64) -> StoreResult<Bytes> {
        match self {
            Store::Mem(m) => m.get_range(key, offset, len),
            Store::S3(s) => s.get_range(key, offset, len).await,
        }
    }

    /// Deletes an object; deleting a missing object is not an error.
    pub async fn delete(&self, key: &str) -> StoreResult<()> {
        match self {
            Store::Mem(m) => m.delete(key),
            Store::S3(s) => s.delete(key).await,
        }
    }

    /// Deletes many objects with few requests; missing objects are not an error.
    pub async fn delete_many(&self, keys: &[String]) -> StoreResult<()> {
        match self {
            Store::Mem(m) => keys.iter().try_for_each(|k| m.delete(k)),
            Store::S3(s) => s.delete_many(keys).await,
        }
    }

    /// All objects whose key starts with `prefix`.
    pub async fn list(&self, prefix: &str) -> StoreResult<Vec<ObjectInfo>> {
        match self {
            Store::Mem(m) => m.list(prefix),
            Store::S3(s) => s.list(prefix).await,
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
    range_gets: AtomicU64,
}

impl MemStore {
    /// Makes every subsequent operation fail (failure injection for tests).
    #[cfg(test)]
    pub fn set_fail(&self, fail: bool) {
        self.fail.store(fail, Ordering::SeqCst);
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

    /// Byte offset of the zone if `key` is a zone object of this disk.
    pub fn parse_zone_key(&self, key: &str) -> Option<u64> {
        let name = key.strip_prefix(&self.prefix)?.strip_prefix('/')?;
        // Exactly what zone_key() writes: 16 lowercase hex digits.
        if name.len() != 16 || !name.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
            return None;
        }
        u64::from_str_radix(name, 16).ok()
    }

    /// Byte offset of the zone if `key` is a FINISH marker of this disk.
    pub fn parse_finished_key(&self, key: &str) -> Option<u64> {
        self.parse_zone_key(key.strip_suffix(FINISHED_SUFFIX)?)
    }
}

const FINISHED_SUFFIX: &str = ".finished";

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(Layout::new("/").is_err());
    }
}

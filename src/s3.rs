//! Minimal S3 client: reqwest for HTTP, aws-sigv4 for request signing.
//!
//! Only what btrs3fs needs: PUT, PATCH (Yandex Object Storage extension for
//! partial writes, `Content-Range: bytes a-b/*`), ranged GET, DELETE and
//! ListObjectsV2. Bodies are signed as `UNSIGNED-PAYLOAD` (TLS protects them),
//! so they are streamed from the zone chunks without hashing or concatenation.

use crate::config::{
    AppendMode, Creds, DEFAULT_RETRY_BACKOFF, DEFAULT_RETRY_INITIAL_DELAY_MS,
    DEFAULT_RETRY_TIMEOUT_SECS, DiskConfig,
};
use crate::store::{Body, ConcatPart, Layout, ObjectInfo, StoreError, StoreResult};
use anyhow::{Context, anyhow};
use aws_credential_types::Credentials;
use aws_sigv4::http_request::{
    PayloadChecksumKind, PercentEncodingMode, SignableBody, SignableRequest, SigningSettings,
    UriPathNormalizationMode, sign,
};
use aws_sigv4::sign::v4;
use bytes::Bytes;
use futures_util::StreamExt;
use log::{debug, warn};
use reqwest::StatusCode;
use serde::Deserialize;
use std::future::Future;
use std::time::{Duration, Instant, SystemTime};

/// Network options of the S3 client. HTTP/1.1 only, a TCP connection per
/// request in flight: over HTTP/2 every request shared one connection and
/// parallel GETs topped out 3.5 times lower (SPEC.md §8).
#[derive(Clone, Debug, clap::Args)]
pub struct Transport {
    /// Proxy for S3 requests, e.g. http://proxy:3128 (HTTPS goes through CONNECT).
    #[arg(long)]
    pub proxy: Option<String>,
}

#[derive(Clone, Copy, Debug)]
pub struct RetryPolicy {
    /// Total time budget of one operation including all retries.
    pub timeout: Duration,
    pub initial_delay: Duration,
    pub backoff_factor: f64,
}

impl Default for RetryPolicy {
    /// For requests before disk.json is read, or without a disk at all.
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(DEFAULT_RETRY_TIMEOUT_SECS),
            initial_delay: Duration::from_millis(DEFAULT_RETRY_INITIAL_DELAY_MS),
            backoff_factor: DEFAULT_RETRY_BACKOFF,
        }
    }
}

impl RetryPolicy {
    pub fn from_config(cfg: &DiskConfig) -> Self {
        Self {
            timeout: Duration::from_secs(cfg.retry_timeout_seconds),
            initial_delay: Duration::from_millis(cfg.retry_initial_delay_ms),
            backoff_factor: cfg.retry_backoff_factor,
        }
    }
}

/// Outcome of one attempt.
enum Attempt {
    Retry(anyhow::Error),
    Fatal(StoreError),
}

impl From<reqwest::Error> for Attempt {
    fn from(e: reqwest::Error) -> Self {
        // Network failures, timeouts and broken bodies are worth retrying.
        Attempt::Retry(anyhow!(e))
    }
}

#[derive(Clone)]
pub struct S3Store {
    http: reqwest::Client,
    /// `<endpoint>/<bucket>` (path-style addressing).
    base: String,
    bucket: String,
    region: String,
    credentials: Credentials,
    retry: RetryPolicy,
    append: AppendMode,
}

/// TCP keepalive probes on idle connections: notices connections silently
/// dropped by NAT or Wi-Fi (SPEC.md §8). Judgement call: well below the
/// common 60+ s NAT idle timeouts.
const TCP_KEEPALIVE: Duration = Duration::from_secs(30);

/// Idle connections are kept for reuse this long; the reqwest default.
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(90);

/// Connection setup (TCP + TLS) longer than this counts as a failed attempt
/// and is retried. Judgement call: a healthy setup takes well under a second.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Longest silence while waiting for response bytes before the attempt is
/// retried (an idle timeout, not a limit on the whole transfer). Must exceed
/// the server's own think time: the slowest PATCH measured was ~30 s, on
/// objects grown past 20 MB, which 16 MiB zones avoid (SPEC.md §11).
const READ_TIMEOUT: Duration = Duration::from_secs(60);

impl S3Store {
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub fn new(creds: &Creds, transport: &Transport, retry: RetryPolicy) -> anyhow::Result<Self> {
        let mut builder = reqwest::Client::builder()
            .tcp_nodelay(true)
            .tcp_keepalive(TCP_KEEPALIVE)
            .pool_idle_timeout(POOL_IDLE_TIMEOUT)
            .connect_timeout(CONNECT_TIMEOUT)
            .read_timeout(READ_TIMEOUT)
            .http1_only()
            .user_agent(concat!("btrs3fs/", env!("CARGO_PKG_VERSION")));
        if let Some(proxy) = &transport.proxy {
            builder = builder.proxy(reqwest::Proxy::all(proxy).context("invalid proxy URL")?);
        }
        Ok(Self {
            http: builder.build().context("cannot create HTTP client")?,
            base: format!(
                "{}/{}",
                creds.endpoint.trim_end_matches('/'),
                encode_path(&creds.bucket)
            ),
            bucket: creds.bucket.clone(),
            region: creds.region.clone(),
            credentials: Credentials::new(
                &creds.access_key,
                &creds.secret_key,
                None,
                None,
                "btrs3fs",
            ),
            retry,
            append: AppendMode::Patch,
        })
    }

    /// Same client and connection pool with another way to append (from disk.json).
    pub fn with_append(&self, append: AppendMode) -> Self {
        Self {
            append,
            ..self.clone()
        }
    }

    /// Same client and connection pool with another retry policy.
    pub fn with_retry(&self, retry: RetryPolicy) -> Self {
        Self {
            retry,
            ..self.clone()
        }
    }

    fn object_url(&self, key: &str) -> String {
        format!("{}/{}", self.base, encode_path(key))
    }

    /// Signs and sends one request; `headers` are included in the signature.
    async fn send(
        &self,
        method: reqwest::Method,
        url: &str,
        headers: &[(&str, String)],
        body: Option<&Body>,
    ) -> Result<reqwest::Response, reqwest::Error> {
        let mut req = http::Request::builder().method(method.clone()).uri(url);
        for (name, value) in headers {
            req = req.header(*name, value);
        }
        let reqwest_body = match body {
            Some(b) => {
                req = req.header(http::header::CONTENT_LENGTH, b.len());
                let chunks = b.chunks.clone();
                reqwest::Body::wrap_stream(futures_util::stream::iter(
                    chunks.into_iter().map(Ok::<Bytes, std::io::Error>),
                ))
            }
            None => {
                // Some S3 services (Selectel) refuse a POST or PUT without
                // Content-Length (411), even with nothing to send.
                if matches!(method, reqwest::Method::POST | reqwest::Method::PUT) {
                    req = req.header(http::header::CONTENT_LENGTH, 0);
                }
                reqwest::Body::from(Bytes::new())
            }
        };
        let mut req = req.body(reqwest_body).expect("request parts are valid");
        self.sign(&mut req);
        self.http.execute(reqwest::Request::try_from(req)?).await
    }

    fn sign<B>(&self, req: &mut http::Request<B>) {
        let mut settings = SigningSettings::default();
        settings.percent_encoding_mode = PercentEncodingMode::Single;
        settings.uri_path_normalization_mode = UriPathNormalizationMode::Disabled;
        settings.payload_checksum_kind = PayloadChecksumKind::XAmzSha256;
        let identity = self.credentials.clone().into();
        let params = v4::SigningParams::builder()
            .identity(&identity)
            .region(&self.region)
            .name("s3")
            .time(SystemTime::now())
            .settings(settings)
            .build()
            .expect("signing params are complete")
            .into();
        let uri = req.uri().to_string();
        let headers: Vec<(&str, &str)> = req
            .headers()
            .iter()
            .filter(|(n, _)| *n != http::header::CONTENT_LENGTH)
            .map(|(n, v)| (n.as_str(), v.to_str().unwrap_or("")))
            .collect();
        let signable = SignableRequest::new(
            req.method().as_str(),
            uri,
            headers.into_iter(),
            SignableBody::UnsignedPayload,
        )
        .expect("request URI is valid");
        let (instructions, _) = sign(signable, &params)
            .expect("signing cannot fail")
            .into_parts();
        instructions.apply_to_request_http1x(req);
    }

    /// Runs `attempt` until it succeeds, fails fatally or the budget is spent.
    async fn with_retries<T, F, Fut>(&self, what: &str, mut attempt: F) -> StoreResult<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, Attempt>>,
    {
        let started = Instant::now();
        let mut delay = self.retry.initial_delay;
        let mut tries = 0u32;
        loop {
            tries += 1;
            match attempt().await {
                Ok(v) => return Ok(v),
                Err(Attempt::Fatal(e)) => return Err(e),
                Err(Attempt::Retry(e)) => {
                    let left = self.retry.timeout.saturating_sub(started.elapsed());
                    if left.is_zero() {
                        return Err(StoreError::Other(
                            e.context(format!("{what}: giving up after {tries} attempts")),
                        ));
                    }
                    let wait = delay.min(left);
                    warn!("{what}: attempt {tries} failed ({e:#}), retrying in {wait:?}");
                    crate::stats::STATS.retries.inc();
                    tokio::time::sleep(wait).await;
                    delay = delay.mul_f64(self.retry.backoff_factor);
                }
            }
        }
    }

    pub async fn put(&self, key: &str, body: Body) -> StoreResult<()> {
        let url = self.object_url(key);
        self.with_retries(&format!("PUT {key}"), || async {
            let resp = self
                .send(reqwest::Method::PUT, &url, &[], Some(&body))
                .await?;
            check(resp, key).await.map(drop)
        })
        .await
    }

    /// Grows a zone object the way the disk appends (SPEC.md §5): `offset` 0
    /// creates it, else `body` goes at `offset` = the object's size.
    pub async fn append(&self, key: &str, offset: u64, body: Body) -> StoreResult<()> {
        match (self.append, offset) {
            // RGW appends only to objects an append created.
            (AppendMode::PutAppend, _) => self.rgw_append(key, offset, body).await,
            (_, 0) => self.put(key, body).await,
            (AppendMode::Patch, _) => self.patch_request(key, offset, body).await,
            (AppendMode::Segments, _) => {
                Err(anyhow!("{key}: a SEGMENTS disk writes new objects, it never appends").into())
            }
        }
    }

    /// Yandex PATCH at `offset`, whatever the disk's append mode.
    #[cfg_attr(not(test), allow(dead_code))]
    pub async fn patch(&self, key: &str, offset: u64, body: Body) -> StoreResult<()> {
        self.patch_request(key, offset, body).await
    }

    /// How zone objects can grow in this bucket (SPEC.md §5), tried on probe
    /// objects under `dir`: PATCH; else Ceph RGW Append Object, which must
    /// answer 200 with the right `x-rgw-next-append-position` twice (a service
    /// that ignores `?append` overwrites the object and sends no such header —
    /// Selectel does); else segments, if the bucket concatenates small parts on
    /// the server. Without any of these btrs3fs cannot use the bucket.
    pub async fn detect_append(&self, dir: &str) -> StoreResult<AppendMode> {
        let dir = dir.trim_end_matches('/');
        let block = Bytes::from(vec![0u8; crate::config::BLOCK_SIZE as usize]);
        let len = block.len() as u64;
        let one = || Body::new(vec![block.clone()]);

        let key = format!("{dir}/append-probe");
        self.put(&key, one()).await?;
        let patched = self.patch_request(&key, len, one()).await;
        self.delete(&key).await?;
        match patched {
            Ok(()) => return Ok(AppendMode::Patch),
            Err(StoreError::Unsupported(_)) => {}
            Err(e) => return Err(e),
        }

        let key = format!("{dir}/append-probe-rgw");
        let appended = async {
            Ok::<_, StoreError>(
                self.rgw_append_once(&key, 0, one()).await? == Some(len)
                    && self.rgw_append_once(&key, len, one()).await? == Some(2 * len),
            )
        }
        .await;
        self.delete(&key).await?;
        match appended {
            Ok(true) => return Ok(AppendMode::PutAppend),
            Ok(false) => {}
            Err(e) => debug!("no Append Object: {e}"),
        }

        let (a, b, c) = (
            format!("{dir}/append-probe-a"),
            format!("{dir}/append-probe-b"),
            format!("{dir}/append-probe-c"),
        );
        let first = Bytes::from(vec![0x11u8; block.len()]);
        let second = Bytes::from(vec![0x22u8; block.len()]);
        self.put(&a, Body::new(vec![first.clone()])).await?;
        self.put(&b, Body::new(vec![second.clone()])).await?;
        let concatenated = async {
            let pending = self
                .begin_concat(&c, &[(a.clone(), 0, len), (b.clone(), 0, len)])
                .await?;
            self.finish_concat(pending).await?;
            Ok::<_, StoreError>(self.get(&c).await? == [first, second].concat())
        }
        .await;
        self.delete_many(&[a, b, c]).await?;
        match concatenated {
            Ok(true) => Ok(AppendMode::Segments),
            Ok(false) => Err(anyhow!("server-side concatenation gave wrong data").into()),
            Err(e) => Err(anyhow!(
                "this S3 can neither append (PATCH, Append Object) nor concatenate small \
                 parts on the server ({e}); btrs3fs cannot use it"
            )
            .into()),
        }
    }

    /// Ceph RGW Append Object at `position`, which must be the object's size.
    /// The answer must carry `x-rgw-next-append-position` = position + length;
    /// anything else is an error, not a silent overwrite. A repeat after a lost
    /// answer gets 409 (the position is no longer the size): the earlier try
    /// landed if the object is exactly position + length long.
    async fn rgw_append(&self, key: &str, position: u64, body: Body) -> StoreResult<()> {
        let want = position + body.len();
        match self.rgw_append_once(key, position, body).await {
            Ok(Some(next)) if next == want => Ok(()),
            Ok(next) => Err(anyhow!(
                "{key}: append at {position} answered next position {next:?}, expected {want}"
            )
            .into()),
            Err(StoreError::InvalidRange { .. }) if self.head_size(key).await? == want => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// One `PUT ?append&position=N` (with retries): `x-rgw-next-append-position`.
    async fn rgw_append_once(
        &self,
        key: &str,
        position: u64,
        body: Body,
    ) -> StoreResult<Option<u64>> {
        let url = format!("{}?append&position={position}", self.object_url(key));
        self.with_retries(&format!("APPEND {key} at {position}"), || async {
            let resp = self
                .send(reqwest::Method::PUT, &url, &[], Some(&body))
                .await?;
            let resp = check(resp, key).await?;
            Ok(resp
                .headers()
                .get("x-rgw-next-append-position")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse().ok()))
        })
        .await
    }

    /// HEAD: size of an existing object.
    async fn head_size(&self, key: &str) -> StoreResult<u64> {
        let url = self.object_url(key);
        self.with_retries(&format!("HEAD {key}"), || async {
            let resp = self.send(reqwest::Method::HEAD, &url, &[], None).await?;
            let resp = check(resp, key).await?;
            resp.headers()
                .get(http::header::CONTENT_LENGTH)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse().ok())
                .ok_or_else(|| Attempt::Fatal(anyhow!("{key}: HEAD without size").into()))
        })
        .await
    }

    /// Yandex PATCH: `Content-Range: bytes a-b/*`.
    async fn patch_request(&self, key: &str, offset: u64, body: Body) -> StoreResult<()> {
        let url = self.object_url(key);
        let range = format!("bytes {}-{}/*", offset, offset + body.len() - 1);
        self.with_retries(&format!("PATCH {key} {range}"), || async {
            let headers = [("content-range", range.clone())];
            let resp = self
                .send(reqwest::Method::PATCH, &url, &headers, Some(&body))
                .await?;
            check(resp, key).await.map(drop)
        })
        .await
    }

    /// CreateMultipartUpload: the upload id.
    async fn create_multipart(&self, key: &str, url: &str) -> StoreResult<String> {
        let url = format!("{url}?uploads");
        self.with_retries(&format!("POST {key}?uploads"), || async {
            let resp = self.send(reqwest::Method::POST, &url, &[], None).await?;
            let xml = check(resp, key).await?.text().await?;
            let r: InitiateResult = quick_xml::de::from_str(&xml).map_err(|e| {
                Attempt::Fatal(anyhow!("cannot parse multipart upload answer: {e}").into())
            })?;
            Ok(r.upload_id)
        })
        .await
    }

    /// Concatenation on the server (SPEC.md §5): a multipart upload to `dst`
    /// whose parts are copies of `parts` (UploadPartCopy with a range), up to
    /// `COPY_PARALLELISM` at once. The object appears on `finish_concat`.
    pub async fn begin_concat(
        &self,
        dst: &str,
        parts: &[ConcatPart],
    ) -> StoreResult<PendingConcat> {
        let url = self.object_url(dst);
        let upload_id = self.create_multipart(dst, &url).await?;
        let id = encode_query(&upload_id);
        let copies: Vec<_> = parts
            .iter()
            .enumerate()
            .map(|(i, (src, offset, len))| {
                let part_url = format!("{url}?partNumber={}&uploadId={id}", i + 1);
                let headers = [
                    (
                        "x-amz-copy-source",
                        format!("/{}/{}", self.bucket, encode_path(src)),
                    ),
                    (
                        "x-amz-copy-source-range",
                        format!("bytes={offset}-{}", offset + len - 1),
                    ),
                ];
                async move {
                    self.with_retries(&format!("COPY {src} into {dst}"), || async {
                        let resp = self
                            .send(reqwest::Method::PUT, &part_url, &headers, None)
                            .await?;
                        let xml = check(resp, src).await?.text().await?;
                        body_error(&xml, dst)?;
                        let r: CopyPartResult = quick_xml::de::from_str(&xml).map_err(|e| {
                            Attempt::Fatal(anyhow!("cannot parse copy part answer: {e}").into())
                        })?;
                        Ok(r.e_tag)
                    })
                    .await
                }
            })
            .collect();
        let etags: Vec<StoreResult<String>> = futures_util::stream::iter(copies)
            .buffered(COPY_PARALLELISM)
            .collect()
            .await;
        let pending = PendingConcat {
            dst: dst.to_string(),
            upload_id,
            etags: Vec::new(),
            size: parts.iter().map(|p| p.2).sum(),
        };
        match etags.into_iter().collect::<StoreResult<Vec<String>>>() {
            Ok(etags) => Ok(PendingConcat { etags, ..pending }),
            Err(e) => {
                self.abort_concat(pending).await;
                Err(e)
            }
        }
    }

    /// CompleteMultipartUpload: the concatenated object replaces `dst`. A
    /// repeat after a lost answer finds no upload (404): it completed if `dst`
    /// has the concatenated size.
    pub async fn finish_concat(&self, p: PendingConcat) -> StoreResult<()> {
        let url = format!(
            "{}?uploadId={}",
            self.object_url(&p.dst),
            encode_query(&p.upload_id)
        );
        let xml = Body::new(vec![Bytes::from(complete_xml(&p.etags))]);
        let done = self
            .with_retries(&format!("COMPLETE {}", p.dst), || async {
                let headers = [("content-type", "application/xml".to_string())];
                let resp = self
                    .send(reqwest::Method::POST, &url, &headers, Some(&xml))
                    .await?;
                let answer = check(resp, &p.dst).await?.text().await?;
                body_error(&answer, &p.dst)
            })
            .await;
        match done {
            Err(StoreError::NotFound(_)) if self.head_size(&p.dst).await? == p.size => Ok(()),
            other => other,
        }
    }

    /// AbortMultipartUpload (best effort: a leftover only costs storage and
    /// is aborted at the next start).
    pub async fn abort_concat(&self, p: PendingConcat) {
        let url = self.object_url(&p.dst);
        if let Err(e) = self.abort_multipart(&p.dst, &url, &p.upload_id).await {
            warn!("cannot abort concatenation into {}: {e}", p.dst);
        }
    }

    /// Aborts the multipart uploads under `prefix` (ListMultipartUploads),
    /// which only a crashed concatenation leaves; how many.
    pub async fn abort_stale_concats(&self, layout: &Layout) -> StoreResult<usize> {
        let prefix = &layout.dir();
        let mut markers: Option<(String, String)> = None;
        let mut aborted = 0;
        loop {
            let mut url = format!("{}?uploads&prefix={}", self.base, encode_query(prefix));
            if let Some((key, id)) = &markers {
                url.push_str(&format!(
                    "&key-marker={}&upload-id-marker={}",
                    encode_query(key),
                    encode_query(id)
                ));
            }
            let page: UploadsResult = self
                .with_retries(&format!("LIST UPLOADS {prefix}"), || async {
                    let resp = self.send(reqwest::Method::GET, &url, &[], None).await?;
                    let xml = check(resp, prefix).await?.text().await?;
                    quick_xml::de::from_str(&xml).map_err(|e| {
                        Attempt::Fatal(anyhow!("cannot parse multipart uploads list: {e}").into())
                    })
                })
                .await?;
            // Only this disk's: another disk may be nested under the prefix.
            for u in page
                .uploads
                .iter()
                .filter(|u| layout.zone_of(&u.key).is_some())
            {
                self.abort_multipart(&u.key, &self.object_url(&u.key), &u.upload_id)
                    .await?;
                aborted += 1;
            }
            let next = page.next_key_marker.zip(page.next_upload_id_marker);
            match next_page(page.is_truncated, next, &markers, prefix)? {
                Some(m) => markers = Some(m),
                None => return Ok(aborted),
            }
        }
    }

    async fn abort_multipart(&self, key: &str, url: &str, upload_id: &str) -> StoreResult<()> {
        let url = format!("{url}?uploadId={}", encode_query(upload_id));
        self.with_retries(&format!("ABORT {key}"), || async {
            let resp = self.send(reqwest::Method::DELETE, &url, &[], None).await?;
            match check(resp, key).await {
                Ok(_) | Err(Attempt::Fatal(StoreError::NotFound(_))) => Ok(()),
                Err(e) => Err(e),
            }
        })
        .await
    }

    pub async fn get(&self, key: &str) -> StoreResult<Bytes> {
        let url = self.object_url(key);
        self.with_retries(&format!("GET {key}"), || async {
            let resp = self.send(reqwest::Method::GET, &url, &[], None).await?;
            Ok(check(resp, key).await?.bytes().await?)
        })
        .await
    }

    pub async fn get_range(&self, key: &str, offset: u64, len: u64) -> StoreResult<Bytes> {
        let url = self.object_url(key);
        let range = format!("bytes={}-{}", offset, offset + len - 1);
        self.with_retries(&format!("GET {key} {range}"), || async {
            let headers = [("range", range.clone())];
            let resp = self
                .send(reqwest::Method::GET, &url, &headers, None)
                .await?;
            let data = check(resp, key).await?.bytes().await?;
            if data.len() as u64 != len {
                return Err(Attempt::Retry(anyhow!(
                    "short read: got {} of {len} bytes",
                    data.len()
                )));
            }
            Ok(data)
        })
        .await
    }

    pub async fn delete(&self, key: &str) -> StoreResult<()> {
        let url = self.object_url(key);
        self.with_retries(&format!("DELETE {key}"), || async {
            let resp = self.send(reqwest::Method::DELETE, &url, &[], None).await?;
            match check(resp, key).await {
                Ok(_) | Err(Attempt::Fatal(StoreError::NotFound(_))) => Ok(()),
                Err(e) => Err(e),
            }
        })
        .await
    }

    /// DeleteObjects: deletes `keys` in batches of `DELETE_BATCH`, up to
    /// `DELETE_PARALLELISM` batches at once; missing keys are not an error.
    pub async fn delete_many(&self, keys: &[String]) -> StoreResult<()> {
        // Futures built up front: a borrowing closure inside the stream would
        // make the future not provably `Send` (a known rustc limitation).
        let batches: Vec<_> = keys
            .chunks(DELETE_BATCH)
            .map(|batch| self.delete_batch(batch))
            .collect();
        let results: Vec<StoreResult<()>> = futures_util::stream::iter(batches)
            .buffer_unordered(DELETE_PARALLELISM)
            .collect()
            .await;
        results.into_iter().collect()
    }

    async fn delete_batch(&self, keys: &[String]) -> StoreResult<()> {
        let url = format!("{}?delete", self.base);
        let xml = delete_xml(keys);
        let checksum = crc32c_header(xml.as_bytes());
        let body = Body::new(vec![Bytes::from(xml)]);
        let what = format!("DELETE {} objects from {}", keys.len(), keys[0]);
        self.with_retries(&what, || async {
            let headers = [
                ("content-type", "application/xml".to_string()),
                ("x-amz-checksum-crc32c", checksum.clone()),
            ];
            let resp = self
                .send(reqwest::Method::POST, &url, &headers, Some(&body))
                .await?;
            let xml = check(resp, &keys[0]).await?.text().await?;
            let result: DeleteResult = quick_xml::de::from_str(&xml)
                .map_err(|e| Attempt::Fatal(anyhow!("cannot parse delete response: {e}").into()))?;
            // Quiet mode lists only the keys that failed.
            let Some(e) = result.errors.first() else {
                return Ok(());
            };
            let err = anyhow!(
                "{} of {} keys not deleted, first {}: {}: {}",
                result.errors.len(),
                keys.len(),
                e.key,
                e.code,
                e.message
            );
            // Deleting is idempotent: the whole batch is simply sent again.
            Err(match e.code.as_str() {
                "InternalError" | "ServiceUnavailable" | "SlowDown" => Attempt::Retry(err),
                _ => Attempt::Fatal(err.into()),
            })
        })
        .await
    }

    pub async fn list(&self, prefix: &str) -> StoreResult<Vec<ObjectInfo>> {
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut url = format!("{}?list-type=2&prefix={}", self.base, encode_query(prefix));
            if let Some(t) = &token {
                url.push_str(&format!("&continuation-token={}", encode_query(t)));
            }
            let page: ListResult = self
                .with_retries(&format!("LIST {prefix}"), || async {
                    let resp = self.send(reqwest::Method::GET, &url, &[], None).await?;
                    let xml = check(resp, prefix).await?.text().await?;
                    quick_xml::de::from_str(&xml).map_err(|e| {
                        Attempt::Fatal(anyhow!("cannot parse list response: {e}").into())
                    })
                })
                .await?;
            debug!("LIST {prefix}: {} objects in page", page.contents.len());
            out.extend(page.contents.into_iter().map(|c| ObjectInfo {
                key: c.key,
                size: c.size,
            }));
            match next_page(
                page.is_truncated,
                page.next_continuation_token,
                &token,
                prefix,
            )? {
                Some(t) => token = Some(t),
                None => return Ok(out),
            }
        }
    }
}

/// Where the next page of a listing starts: `None` at the end. A truncated
/// page without a new place to go on from is an error: taking it as the end
/// would lose objects (zones restored EMPTY and overwritten), repeating it
/// would loop forever.
fn next_page<T: PartialEq>(
    truncated: bool,
    next: Option<T>,
    current: &Option<T>,
    prefix: &str,
) -> StoreResult<Option<T>> {
    match next {
        _ if !truncated => Ok(None),
        Some(n) if current.as_ref() != Some(&n) => Ok(Some(n)),
        _ => Err(anyhow!("listing of {prefix} is truncated with no new place to continue").into()),
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ListResult {
    #[serde(default)]
    contents: Vec<ListEntry>,
    #[serde(default)]
    is_truncated: bool,
    next_continuation_token: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ListEntry {
    key: String,
    size: u64,
}

#[derive(Deserialize)]
struct DeleteResult {
    #[serde(rename = "Error", default)]
    errors: Vec<DeleteError>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DeleteError {
    key: String,
    code: String,
    #[serde(default)]
    message: String,
}

/// `x-amz-checksum-crc32c` of a request body: base64 of the big-endian CRC32C.
/// DeleteObjects must carry a checksum of its key list (standard S3; Selectel
/// enforces it, Yandex does not); CRC32C is the fastest the standard allows
/// — one CPU instruction per 8 bytes on x86 (SSE4.2) and ARM (SPEC.md §8).
fn crc32c_header(body: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(crc32c::crc32c(body).to_be_bytes())
}

/// DeleteObjects request body; quiet: the answer lists only failures.
fn delete_xml(keys: &[String]) -> String {
    let mut xml = String::from("<Delete><Quiet>true</Quiet>");
    for k in keys {
        xml.push_str(&format!(
            "<Object><Key>{}</Key></Object>",
            quick_xml::escape::escape(k.as_str())
        ));
    }
    xml.push_str("</Delete>");
    xml
}

/// Maps the HTTP status to success, a retryable or a fatal error.
async fn check(resp: reqwest::Response, key: &str) -> Result<reqwest::Response, Attempt> {
    let status = resp.status();
    if status.is_success() {
        return Ok(resp);
    }
    let body = resp.text().await.unwrap_or_default();
    Err(classify(status, &body, key))
}

/// A failed answer as a retryable or a fatal error, from its status and body.
fn classify(status: StatusCode, body: &str, key: &str) -> Attempt {
    let detail = format!("HTTP {status}: {}", s3_error_summary(body));
    match status {
        StatusCode::NOT_FOUND => Attempt::Fatal(StoreError::NotFound(key.to_string())),
        // 409 OperationAborted: an earlier request to the key (one we gave up
        // on) is still in progress on the server; it says to try again
        // (Selectel, SPEC.md §11).
        StatusCode::CONFLICT if xml_tag(body, "Code").as_deref() == Some("OperationAborted") => {
            Attempt::Retry(anyhow!(detail))
        }
        // 409: Ceph RGW append at a position that is not the object's size.
        StatusCode::RANGE_NOT_SATISFIABLE | StatusCode::CONFLICT => {
            Attempt::Fatal(StoreError::InvalidRange {
                key: key.to_string(),
                detail,
            })
        }
        StatusCode::METHOD_NOT_ALLOWED | StatusCode::NOT_IMPLEMENTED => {
            Attempt::Fatal(StoreError::Unsupported(format!("{key}: {detail}")))
        }
        StatusCode::TOO_MANY_REQUESTS | StatusCode::REQUEST_TIMEOUT => {
            Attempt::Retry(anyhow!(detail))
        }
        // 400 without an S3 error code comes from a balancer or proxy in
        // front of S3, not from S3 judging the request: Selectel answered so
        // to requests cut off when the route changed (SPEC.md §11). Our
        // requests are repeatable, so it is retried like a network error.
        StatusCode::BAD_REQUEST if xml_tag(body, "Code").is_none() => {
            Attempt::Retry(anyhow!(detail))
        }
        s if s.is_server_error() => Attempt::Retry(anyhow!(detail)),
        _ => Attempt::Fatal(anyhow!("{key}: {detail}").into()),
    }
}

/// UploadPartCopy and CompleteMultipartUpload may answer 200 with an
/// `<Error>` body (S3 starts the answer before the work ends).
fn body_error(xml: &str, key: &str) -> Result<(), Attempt> {
    if !xml.contains("<Error>") {
        return Ok(());
    }
    let summary = s3_error_summary(xml);
    let err = anyhow!("{key}: {summary}");
    Err(
        if ["InternalError", "ServiceUnavailable", "SlowDown"]
            .iter()
            .any(|c| summary.starts_with(c))
        {
            Attempt::Retry(err)
        } else {
            Attempt::Fatal(err.into())
        },
    )
}

/// Server-side concatenation in progress: parts copied, not completed.
pub struct PendingConcat {
    dst: String,
    upload_id: String,
    etags: Vec<String>,
    /// Bytes of the concatenated object.
    size: u64,
}

/// Parts of one concatenation copied at once: the copies run on the server,
/// so this only bounds requests in flight. Judgement call, as S3_PARALLELISM.
const COPY_PARALLELISM: usize = 16;

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct UploadsResult {
    #[serde(rename = "Upload", default)]
    uploads: Vec<UploadEntry>,
    #[serde(default)]
    is_truncated: bool,
    next_key_marker: Option<String>,
    next_upload_id_marker: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct UploadEntry {
    key: String,
    upload_id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct InitiateResult {
    upload_id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct CopyPartResult {
    #[serde(rename = "ETag")]
    e_tag: String,
}

/// CompleteMultipartUpload body: parts numbered from 1 in order.
fn complete_xml(etags: &[String]) -> String {
    let mut xml = String::from("<CompleteMultipartUpload>");
    for (i, etag) in etags.iter().enumerate() {
        xml.push_str(&format!(
            "<Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>",
            i + 1,
            quick_xml::escape::escape(etag.as_str())
        ));
    }
    xml.push_str("</CompleteMultipartUpload>");
    xml
}

/// Keys in one DeleteObjects request: the S3 API maximum.
const DELETE_BATCH: usize = 1000;
/// DeleteObjects requests `delete_many` runs at once. Judgement call: the
/// largest disk (65536 zones of 16 MiB, 1 TiB) needs 66 requests; a few in
/// parallel finish it in seconds without bursting the request rate.
const DELETE_PARALLELISM: usize = 4;

/// Characters of a non-XML error body kept in a log line (proxies and load
/// balancers answer with whole HTML pages).
const ERROR_BODY_PREVIEW: usize = 200;

/// `Code: Message` from an S3 XML error body.
fn s3_error_summary(body: &str) -> String {
    match (xml_tag(body, "Code"), xml_tag(body, "Message")) {
        (Some(c), Some(m)) => format!("{c}: {m}"),
        (Some(c), None) => c,
        _ => body.chars().take(ERROR_BODY_PREVIEW).collect(),
    }
}

/// Text of the first `<name>…</name>` in an S3 XML body.
fn xml_tag(body: &str, name: &str) -> Option<String> {
    let open = format!("<{name}>");
    let start = body.find(&open)? + open.len();
    let end = body[start..].find(&format!("</{name}>"))? + start;
    Some(body[start..end].to_string())
}

fn is_unreserved(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~')
}

/// Percent-encodes an object key, keeping `/` separators.
fn encode_path(s: &str) -> String {
    encode(s, |b| is_unreserved(b) || b == b'/')
}

fn encode_query(s: &str) -> String {
    encode(s, is_unreserved)
}

fn encode(s: &str, keep: impl Fn(u8) -> bool) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        if keep(b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoding() {
        assert_eq!(
            encode_path("a b/c+d/0000000002000000"),
            "a%20b/c%2Bd/0000000002000000"
        );
        assert_eq!(encode_query("p/q r"), "p%2Fq%20r");
    }

    #[test]
    fn listing_pages() {
        let none: Option<String> = None;
        assert_eq!(next_page(false, None, &none, "p").unwrap(), None);
        assert_eq!(
            next_page(false, Some("t".to_string()), &none, "p").unwrap(),
            None
        );
        assert_eq!(
            next_page(true, Some("t".to_string()), &none, "p").unwrap(),
            Some("t".to_string())
        );
        assert!(next_page(true, None, &none, "p").is_err()); // V1-style answer: no token
        let at = Some("t".to_string());
        assert!(next_page(true, Some("t".to_string()), &at, "p").is_err()); // the same page again
    }

    #[test]
    fn list_parsing() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>b</Name><Prefix>p/</Prefix>
<KeyCount>2</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>true</IsTruncated>
<Contents><Key>p/0000000000000000</Key><LastModified>2026-10-23T12:00:00.000Z</LastModified><ETag>&#34;x&#34;</ETag><Size>8192</Size><StorageClass>STANDARD</StorageClass></Contents>
<Contents><Key>p/disk.json</Key><Size>300</Size></Contents>
<NextContinuationToken>tok/1</NextContinuationToken></ListBucketResult>"#;
        let r: ListResult = quick_xml::de::from_str(xml).unwrap();
        assert_eq!(r.contents.len(), 2);
        assert_eq!(
            (r.contents[0].key.as_str(), r.contents[0].size),
            ("p/0000000000000000", 8192)
        );
        assert!(r.is_truncated);
        assert_eq!(r.next_continuation_token.as_deref(), Some("tok/1"));

        let empty = r#"<ListBucketResult><KeyCount>0</KeyCount><IsTruncated>false</IsTruncated></ListBucketResult>"#;
        let r: ListResult = quick_xml::de::from_str(empty).unwrap();
        assert!(r.contents.is_empty() && !r.is_truncated);
    }

    #[test]
    fn multipart_bodies() {
        assert_eq!(
            complete_xml(&["\"a1\"".into(), "\"b2\"".into()]),
            "<CompleteMultipartUpload>\
             <Part><PartNumber>1</PartNumber><ETag>&quot;a1&quot;</ETag></Part>\
             <Part><PartNumber>2</PartNumber><ETag>&quot;b2&quot;</ETag></Part>\
             </CompleteMultipartUpload>"
        );
        let r: InitiateResult = quick_xml::de::from_str(
            "<InitiateMultipartUploadResult><Bucket>b</Bucket><Key>k</Key>\
             <UploadId>2~abc</UploadId></InitiateMultipartUploadResult>",
        )
        .unwrap();
        assert_eq!(r.upload_id, "2~abc");
        let c: CopyPartResult = quick_xml::de::from_str(
            "<CopyPartResult><LastModified>x</LastModified><ETag>\"e\"</ETag></CopyPartResult>",
        )
        .unwrap();
        assert_eq!(c.e_tag, "\"e\"");
        // 200 with an error body: retried for transient codes, fatal otherwise.
        assert!(body_error("<CompleteMultipartUploadResult/>", "k").is_ok());
        let slow = "<Error><Code>SlowDown</Code><Message>m</Message></Error>";
        assert!(matches!(body_error(slow, "k"), Err(Attempt::Retry(_))));
        let small = "<Error><Code>EntityTooSmall</Code><Message>m</Message></Error>";
        assert!(matches!(body_error(small, "k"), Err(Attempt::Fatal(_))));
    }

    #[test]
    fn crc32c_checksum() {
        // CRC32C of "123456789" is the standard check value 0xE3069283.
        assert_eq!(crc32c_header(b"123456789"), "4waSgw==");
    }

    #[test]
    fn delete_body_and_errors() {
        assert_eq!(
            delete_xml(&["p/a".into(), "p/<b>".into()]),
            "<Delete><Quiet>true</Quiet><Object><Key>p/a</Key></Object>\
             <Object><Key>p/&lt;b&gt;</Key></Object></Delete>"
        );
        let ok: DeleteResult = quick_xml::de::from_str(
            r#"<?xml version="1.0" encoding="UTF-8"?>
            <DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></DeleteResult>"#,
        )
        .unwrap();
        assert!(ok.errors.is_empty());
        let failed: DeleteResult = quick_xml::de::from_str(
            "<DeleteResult><Error><Key>p/a</Key><Code>AccessDenied</Code>\
             <Message>Access Denied</Message></Error></DeleteResult>",
        )
        .unwrap();
        assert_eq!(
            (
                failed.errors[0].key.as_str(),
                failed.errors[0].code.as_str()
            ),
            ("p/a", "AccessDenied")
        );
    }

    #[test]
    fn error_summary() {
        let body = "<Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message></Error>";
        assert_eq!(
            s3_error_summary(body),
            "NoSuchKey: The specified key does not exist."
        );
        let busy = "<Error><Code>OperationAborted</Code><Message>A conflicting conditional \
                    operation is currently in progress against this resource. Try again.</Message></Error>";
        assert_eq!(xml_tag(busy, "Code").as_deref(), Some("OperationAborted"));
        assert_eq!(xml_tag("<html>408</html>", "Code"), None);
    }

    #[test]
    fn answers_retried_or_fatal() {
        let retried = |status: u16, body: &str| {
            let status = StatusCode::from_u16(status).unwrap();
            matches!(classify(status, body, "k"), Attempt::Retry(_))
        };
        let html_400 = "<html><body><h1>400 Bad request</h1>Your browser sent an invalid request.</body></html>";
        assert!(retried(400, html_400)); // balancer, not S3
        assert!(!retried(400, "<Error><Code>InvalidArgument</Code></Error>"));
        assert!(retried(409, "<Error><Code>OperationAborted</Code></Error>"));
        assert!(!retried(
            409,
            "<Error><Code>PositionNotEqualToLength</Code></Error>"
        ));
        assert!(retried(408, "<html>408 Request Time-out</html>"));
        assert!(retried(503, ""));
        assert!(!retried(403, "<Error><Code>AccessDenied</Code></Error>"));
    }

    #[test]
    fn store_errors_show_their_causes() {
        let e: StoreError = anyhow!("operation timed out")
            .context("PUT k: giving up after 1 attempts")
            .into();
        assert_eq!(
            e.to_string(),
            "PUT k: giving up after 1 attempts: operation timed out"
        );
    }
}

/// Tests against a real bucket. Run explicitly:
/// `BTRS3FS_CREDS=creds.json cargo test -- --ignored`
#[cfg(test)]
mod bucket_tests {
    use super::*;
    use crate::stats::Reason;
    use crate::store::{Layout, Store};
    use crate::zones::Engine;
    use std::sync::Arc;

    fn store() -> S3Store {
        let path = std::env::var("BTRS3FS_CREDS").expect("set BTRS3FS_CREDS to a credentials file");
        let creds = Creds::load(std::path::Path::new(&path)).unwrap();
        let retry = RetryPolicy {
            timeout: Duration::from_secs(30),
            initial_delay: Duration::from_millis(100),
            backoff_factor: 2.0,
        };
        let transport = Transport { proxy: None };
        S3Store::new(&creds, &transport, retry).unwrap()
    }

    fn unique_prefix(name: &str) -> String {
        let nanos = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("test/{name}-{nanos:x}")
    }

    fn body(data: &[u8]) -> Body {
        // Two chunks to exercise streaming of multi-chunk bodies.
        let (a, b) = data.split_at(data.len() / 2);
        Body::new(vec![Bytes::copy_from_slice(a), Bytes::copy_from_slice(b)])
    }

    async fn cleanup(s3: &S3Store, prefix: &str) {
        let keys: Vec<String> = s3
            .list(prefix)
            .await
            .unwrap()
            .into_iter()
            .map(|o| o.key)
            .collect();
        s3.delete_many(&keys).await.unwrap();
        assert!(s3.list(prefix).await.unwrap().is_empty());
    }

    #[tokio::test]
    #[ignore]
    async fn object_operations() {
        let s3 = store();
        let prefix = unique_prefix("ops");
        let key = format!("{prefix}/obj");

        assert!(matches!(
            s3.patch(&key, 0, body(&[1; 8192])).await,
            Err(StoreError::NotFound(_))
        ));
        s3.put(&key, body(&[1; 8192])).await.unwrap();
        s3.patch(&key, 8192, body(&[2; 4096])).await.unwrap();
        s3.patch(&key, 8192, body(&[2; 4096])).await.unwrap(); // idempotent
        assert!(matches!(
            s3.patch(&key, 20480, body(&[3; 4096])).await,
            Err(StoreError::InvalidRange { .. })
        ));
        assert_eq!(s3.get(&key).await.unwrap().len(), 12288);
        let r = s3.get_range(&key, 8190, 4).await.unwrap();
        assert_eq!(&r[..], &[1, 1, 2, 2]);
        assert!(matches!(
            s3.get(&format!("{prefix}/none")).await,
            Err(StoreError::NotFound(_))
        ));

        let listed = s3.list(&format!("{prefix}/")).await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(
            (listed[0].key.as_str(), listed[0].size),
            (key.as_str(), 12288)
        );

        s3.delete(&key).await.unwrap();
        s3.delete(&key).await.unwrap(); // missing object is fine
        assert!(s3.list(&format!("{prefix}/")).await.unwrap().is_empty());

        // Batch delete; a missing key is fine there too.
        let keys: Vec<String> = (0..3).map(|i| format!("{prefix}/k{i}")).collect();
        for k in &keys {
            s3.put(k, body(&[1; 8])).await.unwrap();
        }
        s3.delete_many(&[keys[0].clone(), keys[2].clone(), format!("{prefix}/none")])
            .await
            .unwrap();
        let left: Vec<String> = s3
            .list(&format!("{prefix}/"))
            .await
            .unwrap()
            .into_iter()
            .map(|o| o.key)
            .collect();
        assert_eq!(left, vec![keys[1].clone()]);
        cleanup(&s3, &format!("{prefix}/")).await;
    }

    #[tokio::test]
    #[ignore]
    async fn detect_append_mode() {
        let s3 = store();
        let prefix = unique_prefix("detect");
        let mode = s3.detect_append(&prefix).await.unwrap();
        eprintln!("append mode of this bucket: {mode:?}");
        if let Ok(want) = std::env::var("BTRS3FS_EXPECT_APPEND") {
            assert_eq!(format!("{mode:?}"), want);
        }
        assert!(s3.list(&format!("{prefix}/")).await.unwrap().is_empty()); // probes removed
    }

    #[tokio::test]
    #[ignore]
    async fn engine_roundtrip() {
        let s3 = store();
        let prefix = unique_prefix("engine");
        let layout = Layout::new(&prefix).unwrap();
        let cfg = DiskConfig::new(4 << 20, 1 << 20);
        cfg.validate().unwrap();
        let block = |b: u8, n: usize| Bytes::from(vec![b; n * 4096]);
        {
            let store = Arc::new(Store::S3(s3.clone()));
            let e = Engine::open(&cfg, layout.clone(), store).await.unwrap();
            e.write(0, block(1, 2), false).unwrap();
            e.write(1 << 20, block(2, 3), true).unwrap();
            e.flush_all(Reason::Explicit).await.unwrap(); // PUT both zones
            e.write(2 * 4096, block(3, 1), false).unwrap();
            e.flush_all(Reason::Explicit).await.unwrap(); // PATCH zone 0
        }
        let store = Arc::new(Store::S3(s3.clone()));
        let e = Engine::open(&cfg, layout.clone(), store).await.unwrap();
        let r = e.report(0, 2);
        assert_eq!((r[0].wp, r[1].wp), (3 * 4096, (1 << 20) + 3 * 4096));
        let pieces = e.read_plan(4096, 3 * 4096).unwrap();
        let data = e.fetch(pieces).await.unwrap().concat();
        assert_eq!(&data[..4096], &block(1, 1)[..]);
        assert_eq!(&data[4096..8192], &block(3, 1)[..]);
        assert!(data[8192..].iter().all(|&b| b == 0));
        // FINISH puts markers next to the zones; the listing brings them back.
        e.finish_zone(1 << 20).await.unwrap();
        e.finish_zone(2 << 20).await.unwrap(); // empty zone: marker only
        assert!(
            s3.get(&layout.finished_key(1 << 20))
                .await
                .unwrap()
                .is_empty()
        );
        drop(e);
        let e = Engine::open(&cfg, layout.clone(), Arc::new(Store::S3(s3.clone())))
            .await
            .unwrap();
        let r = e.report(0, 3);
        assert_eq!(
            r.iter().map(|z| z.wp).collect::<Vec<_>>(),
            vec![3 * 4096, 2 << 20, 3 << 20]
        );
        e.reset_zone(0).await.unwrap();
        e.reset_zone(2 << 20).await.unwrap();
        // Zone 1 is left: its object and its FINISH marker.
        assert_eq!(s3.list(&format!("{prefix}/")).await.unwrap().len(), 2);
        cleanup(&s3, &format!("{prefix}/")).await;
    }
}

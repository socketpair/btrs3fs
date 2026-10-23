//! Minimal S3 client: reqwest for HTTP, aws-sigv4 for request signing.
//!
//! Only what btrs3fs needs: PUT, PATCH (Yandex Object Storage extension for
//! partial writes, `Content-Range: bytes a-b/*`), ranged GET, DELETE and
//! ListObjectsV2. Bodies are signed as `UNSIGNED-PAYLOAD` (TLS protects them),
//! so they are streamed from the zone chunks without hashing or concatenation.

use crate::config::{Creds, DiskConfig};
use crate::store::{Body, ObjectInfo, StoreError, StoreResult};
use anyhow::{Context, anyhow};
use aws_credential_types::Credentials;
use aws_sigv4::http_request::{
    PayloadChecksumKind, PercentEncodingMode, SignableBody, SignableRequest, SigningSettings,
    UriPathNormalizationMode, sign,
};
use aws_sigv4::sign::v4;
use bytes::Bytes;
use log::{debug, warn};
use reqwest::StatusCode;
use serde::Deserialize;
use std::future::Future;
use std::time::{Duration, Instant, SystemTime};

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum HttpVersion {
    /// Negotiate with ALPN (HTTP/2 if the server offers it).
    Auto,
    Http1,
    Http2,
}

/// Network options of the S3 client.
#[derive(Clone, Debug, clap::Args)]
pub struct Transport {
    /// HTTP protocol version for S3.
    #[arg(long, value_enum, default_value = "auto")]
    pub http: HttpVersion,
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
    region: String,
    credentials: Credentials,
    retry: RetryPolicy,
}

impl S3Store {
    pub fn new(creds: &Creds, transport: &Transport, retry: RetryPolicy) -> anyhow::Result<Self> {
        let mut builder = reqwest::Client::builder()
            .tcp_nodelay(true)
            .tcp_keepalive(Duration::from_secs(30))
            .pool_idle_timeout(Duration::from_secs(90))
            .connect_timeout(Duration::from_secs(10))
            .read_timeout(Duration::from_secs(60))
            .http2_adaptive_window(true)
            .user_agent(concat!("btrs3fs/", env!("CARGO_PKG_VERSION")));
        if let Some(proxy) = &transport.proxy {
            builder = builder.proxy(reqwest::Proxy::all(proxy).context("invalid proxy URL")?);
        }
        builder = match transport.http {
            HttpVersion::Auto => builder,
            HttpVersion::Http1 => builder.http1_only(),
            HttpVersion::Http2 => builder.http2_prior_knowledge(),
        };
        Ok(Self {
            http: builder.build().context("cannot create HTTP client")?,
            base: format!("{}/{}", creds.endpoint.trim_end_matches('/'), encode_path(&creds.bucket)),
            region: creds.region.clone(),
            credentials: Credentials::new(&creds.access_key, &creds.secret_key, None, None, "btrs3fs"),
            retry,
        })
    }

    /// Same client and connection pool with another retry policy.
    pub fn with_retry(&self, retry: RetryPolicy) -> Self {
        Self { retry, ..self.clone() }
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
            None => reqwest::Body::from(Bytes::new()),
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
        let signable =
            SignableRequest::new(req.method().as_str(), uri, headers.into_iter(), SignableBody::UnsignedPayload)
                .expect("request URI is valid");
        let (instructions, _) = sign(signable, &params).expect("signing cannot fail").into_parts();
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
                    tokio::time::sleep(wait).await;
                    delay = delay.mul_f64(self.retry.backoff_factor);
                }
            }
        }
    }

    pub async fn put(&self, key: &str, body: Body) -> StoreResult<()> {
        let url = self.object_url(key);
        self.with_retries(&format!("PUT {key}"), || async {
            let resp = self.send(reqwest::Method::PUT, &url, &[], Some(&body)).await?;
            check(resp, key).await.map(drop)
        })
        .await
    }

    pub async fn patch(&self, key: &str, offset: u64, body: Body) -> StoreResult<()> {
        let url = self.object_url(key);
        let range = format!("bytes {}-{}/*", offset, offset + body.len() - 1);
        self.with_retries(&format!("PATCH {key} {range}"), || async {
            let headers = [("content-range", range.clone())];
            let resp = self.send(reqwest::Method::PATCH, &url, &headers, Some(&body)).await?;
            check(resp, key).await.map(drop)
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
            let resp = self.send(reqwest::Method::GET, &url, &headers, None).await?;
            let data = check(resp, key).await?.bytes().await?;
            if data.len() as u64 != len {
                return Err(Attempt::Retry(anyhow!("short read: got {} of {len} bytes", data.len())));
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
                    quick_xml::de::from_str(&xml)
                        .map_err(|e| Attempt::Fatal(anyhow!("cannot parse list response: {e}").into()))
                })
                .await?;
            debug!("LIST {prefix}: {} objects in page", page.contents.len());
            out.extend(page.contents.into_iter().map(|c| ObjectInfo { key: c.key, size: c.size }));
            match (page.is_truncated, page.next_continuation_token) {
                (true, Some(t)) => token = Some(t),
                _ => return Ok(out),
            }
        }
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

/// Maps the HTTP status to success, a retryable or a fatal error.
async fn check(resp: reqwest::Response, key: &str) -> Result<reqwest::Response, Attempt> {
    let status = resp.status();
    if status.is_success() {
        return Ok(resp);
    }
    let body = resp.text().await.unwrap_or_default();
    let detail = format!("HTTP {status}: {}", s3_error_summary(&body));
    Err(match status {
        StatusCode::NOT_FOUND => Attempt::Fatal(StoreError::NotFound(key.to_string())),
        StatusCode::RANGE_NOT_SATISFIABLE => {
            Attempt::Fatal(StoreError::InvalidRange { key: key.to_string(), detail })
        }
        StatusCode::TOO_MANY_REQUESTS | StatusCode::REQUEST_TIMEOUT => Attempt::Retry(anyhow!(detail)),
        s if s.is_server_error() => Attempt::Retry(anyhow!(detail)),
        _ => Attempt::Fatal(anyhow!("{key}: {detail}").into()),
    })
}

/// `Code: Message` from an S3 XML error body.
fn s3_error_summary(body: &str) -> String {
    let tag = |name: &str| {
        let open = format!("<{name}>");
        let start = body.find(&open)? + open.len();
        let end = body[start..].find(&format!("</{name}>"))? + start;
        Some(body[start..end].to_string())
    };
    match (tag("Code"), tag("Message")) {
        (Some(c), Some(m)) => format!("{c}: {m}"),
        (Some(c), None) => c,
        _ => body.chars().take(200).collect(),
    }
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
        assert_eq!(encode_path("a b/c+d/0000000002000000"), "a%20b/c%2Bd/0000000002000000");
        assert_eq!(encode_query("p/q r"), "p%2Fq%20r");
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
        assert_eq!((r.contents[0].key.as_str(), r.contents[0].size), ("p/0000000000000000", 8192));
        assert!(r.is_truncated);
        assert_eq!(r.next_continuation_token.as_deref(), Some("tok/1"));

        let empty = r#"<ListBucketResult><KeyCount>0</KeyCount><IsTruncated>false</IsTruncated></ListBucketResult>"#;
        let r: ListResult = quick_xml::de::from_str(empty).unwrap();
        assert!(r.contents.is_empty() && !r.is_truncated);
    }

    #[test]
    fn error_summary() {
        let body = "<Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message></Error>";
        assert_eq!(s3_error_summary(body), "NoSuchKey: The specified key does not exist.");
    }
}

/// Tests against a real bucket. Run explicitly:
/// `BTRS3FS_CREDS=creds.json cargo test -- --ignored`
#[cfg(test)]
mod bucket_tests {
    use super::*;
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
        let transport = Transport { http: HttpVersion::Auto, proxy: None };
        S3Store::new(&creds, &transport, retry).unwrap()
    }

    fn unique_prefix(name: &str) -> String {
        let nanos = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos();
        format!("test/{name}-{nanos:x}")
    }

    fn body(data: &[u8]) -> Body {
        // Two chunks to exercise streaming of multi-chunk bodies.
        let (a, b) = data.split_at(data.len() / 2);
        Body::new(vec![Bytes::copy_from_slice(a), Bytes::copy_from_slice(b)])
    }

    async fn cleanup(s3: &S3Store, prefix: &str) {
        for o in s3.list(prefix).await.unwrap() {
            s3.delete(&o.key).await.unwrap();
        }
    }

    #[tokio::test]
    #[ignore]
    async fn object_operations() {
        let s3 = store();
        let prefix = unique_prefix("ops");
        let key = format!("{prefix}/obj");

        assert!(matches!(s3.patch(&key, 0, body(&[1; 8192])).await, Err(StoreError::NotFound(_))));
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
        assert!(matches!(s3.get(&format!("{prefix}/none")).await, Err(StoreError::NotFound(_))));

        let listed = s3.list(&format!("{prefix}/")).await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!((listed[0].key.as_str(), listed[0].size), (key.as_str(), 12288));

        s3.delete(&key).await.unwrap();
        s3.delete(&key).await.unwrap(); // missing object is fine
        assert!(s3.list(&format!("{prefix}/")).await.unwrap().is_empty());
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
            e.flush_all().await.unwrap(); // PUT both zones
            e.write(2 * 4096, block(3, 1), false).unwrap();
            e.flush_all().await.unwrap(); // PATCH zone 0
        }
        let store = Arc::new(Store::S3(s3.clone()));
        let e = Engine::open(&cfg, layout, store).await.unwrap();
        let r = e.report(0, 2);
        assert_eq!((r[0].wp, r[1].wp), (3 * 4096, (1 << 20) + 3 * 4096));
        let pieces = e.read_plan(4096, 3 * 4096).unwrap();
        let data = e.fetch(pieces).await.unwrap().concat();
        assert_eq!(&data[..4096], &block(1, 1)[..]);
        assert_eq!(&data[4096..8192], &block(3, 1)[..]);
        assert!(data[8192..].iter().all(|&b| b == 0));
        e.reset_zone(0).await.unwrap();
        assert_eq!(s3.list(&format!("{prefix}/")).await.unwrap().len(), 1);
        cleanup(&s3, &format!("{prefix}/")).await;
    }
}

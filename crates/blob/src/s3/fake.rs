//! An **in-process S3-compatible fixture**: the transport a test drives instead
//! of a network.
//!
//! It is the object-store analogue of the local backend's temp directory. It
//! speaks enough of the protocol for the backend to be exercised end to end —
//! `PUT` / `GET` / `DELETE` an object, and a **paged** `ListObjectsV2` — and it
//! can be made to fail in the specific ways that decide the backend's behaviour:
//!
//! * **unreachable** (no response at all), which must classify *transient* and
//!   must never be mistaken for a deleted object;
//! * a bounded number of failures, so a retry that succeeds on the third attempt
//!   is a deterministic assertion rather than a race;
//! * a specific status, so a 403 (transient, not retried) and a 500 (transient,
//!   retried) are distinguishable;
//! * an object **overwritten out-of-band**, which is the case the existence
//!   probe's measured hash exists to catch.
//!
//! It also records the `Authorization` header of every request, so "the backend
//! signs what it sends" is checkable without a server that verifies signatures.
//!
//! Behind the default-on `test-kit` feature, like `dagr-core`'s single-task kit;
//! `dagr-cli` takes this crate with `default-features = false`, so a pipeline
//! binary never links it.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use super::http::{HttpRequest, HttpResponse, HttpTransport, TransportError};

/// An in-process object store behind the [`HttpTransport`] port.
///
/// Cheap to clone: every clone shares one store, so a test holds a handle to
/// inspect and perturb the same objects the backend is talking to.
#[derive(Debug, Clone)]
pub struct FakeS3 {
    inner: Arc<Mutex<State>>,
}

#[derive(Debug)]
struct State {
    bucket: String,
    objects: BTreeMap<String, Vec<u8>>,
    requests: Vec<String>,
    authorizations: Vec<String>,
    unreachable: bool,
    fail_next: usize,
    status_next: Option<(usize, u16)>,
    status_until_cleared: Option<u16>,
    list_page_size: usize,
}

impl FakeS3 {
    /// A fixture serving `bucket`, empty and healthy.
    #[must_use]
    pub fn new(bucket: impl Into<String>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(State {
                bucket: bucket.into(),
                objects: BTreeMap::new(),
                requests: Vec::new(),
                authorizations: Vec::new(),
                unreachable: false,
                fail_next: 0,
                status_next: None,
                status_until_cleared: None,
                list_page_size: 1_000,
            })),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// How many requests the fixture has been given, including failed ones.
    #[must_use]
    pub fn request_count(&self) -> usize {
        self.lock().requests.len()
    }

    /// Every request, as `"<METHOD> <path-and-query>"`, in order.
    #[must_use]
    pub fn request_log(&self) -> Vec<String> {
        self.lock().requests.clone()
    }

    /// The `Authorization` header of every request, in order.
    #[must_use]
    pub fn authorizations(&self) -> Vec<String> {
        self.lock().authorizations.clone()
    }

    /// Make every request fail with no response until cleared — the store is
    /// unreachable.
    pub fn set_unreachable(&self, unreachable: bool) {
        self.lock().unreachable = unreachable;
    }

    /// Make the next `n` requests fail with no response, then behave normally.
    pub fn fail_next(&self, n: usize) {
        self.lock().fail_next = n;
    }

    /// Answer the next `n` requests with `status`, then behave normally.
    pub fn respond_next_with_status(&self, n: usize, status: u16) {
        self.lock().status_next = Some((n, status));
    }

    /// Answer every request with `status` until cleared.
    pub fn respond_with_status_until_cleared(&self, status: u16) {
        self.lock().status_until_cleared = Some(status);
    }

    /// Stop answering with a forced status.
    pub fn clear_forced_status(&self) {
        self.lock().status_until_cleared = None;
    }

    /// How many keys one listing page returns — small values force pagination.
    pub fn set_list_page_size(&self, size: usize) {
        self.lock().list_page_size = size.max(1);
    }

    /// Put an object directly, bypassing the backend. Used to plant something the
    /// backend did not write (another tool's object, a different prefix).
    pub fn insert_object(&self, object_key: &str, bytes: Vec<u8>) {
        self.lock().objects.insert(object_key.to_string(), bytes);
    }

    /// **Overwrite an object out-of-band** — the mutation the existence probe's
    /// measured hash exists to catch.
    pub fn overwrite_object(&self, object_key: &str, bytes: Vec<u8>) {
        self.insert_object(object_key, bytes);
    }

    /// Delete an object directly, bypassing the backend.
    pub fn remove_object(&self, object_key: &str) {
        self.lock().objects.remove(object_key);
    }

    /// Every object key currently held.
    #[must_use]
    pub fn object_keys(&self) -> Vec<String> {
        self.lock().objects.keys().cloned().collect()
    }
}

impl HttpTransport for FakeS3 {
    fn execute(&self, request: &HttpRequest) -> Result<HttpResponse, TransportError> {
        let (path, query) = split_path(request.url());
        let mut state = self.lock();
        state.requests.push(format!("{} {path}", request.method()));
        state.authorizations.push(
            request
                .header("authorization")
                .unwrap_or("<unsigned>")
                .to_string(),
        );

        if state.unreachable {
            return Err(TransportError::new(
                "connection refused (the fixture is set unreachable)",
            ));
        }
        if state.fail_next > 0 {
            state.fail_next -= 1;
            return Err(TransportError::new(
                "connection reset (the fixture is failing a bounded number of requests)",
            ));
        }
        if let Some(status) = state.status_until_cleared {
            return Ok(error_response(status));
        }
        if let Some((remaining, status)) = state.status_next {
            if remaining > 0 {
                state.status_next = if remaining == 1 {
                    None
                } else {
                    Some((remaining - 1, status))
                };
                return Ok(error_response(status));
            }
            state.status_next = None;
        }

        // `/<bucket>` with a query is a listing; `/<bucket>/<object-key>` is an
        // object operation.
        let bucket_path = format!("/{}", state.bucket);
        let Some(rest) = path.strip_prefix(&bucket_path) else {
            return Ok(error_response(404));
        };
        if rest.is_empty() || rest == "/" {
            return Ok(list_objects(&state, &query));
        }
        let Some(object_key) = rest.strip_prefix('/') else {
            return Ok(error_response(404));
        };
        let object_key = object_key.to_string();

        match request.method() {
            "PUT" => {
                state.objects.insert(object_key, request.body().to_vec());
                Ok(HttpResponse::new(200))
            }
            "GET" | "HEAD" => match state.objects.get(&object_key) {
                Some(bytes) => {
                    let response = HttpResponse::new(200)
                        .with_header("content-length", bytes.len().to_string());
                    Ok(if request.method() == "HEAD" {
                        response
                    } else {
                        response.with_body(bytes.clone())
                    })
                }
                None => Ok(error_response_with_code(404, "NoSuchKey")),
            },
            "DELETE" => {
                state.objects.remove(&object_key);
                // S3 answers 204 whether or not the object was there.
                Ok(HttpResponse::new(204))
            }
            _ => Ok(error_response(405)),
        }
    }
}

/// Split an absolute URL into its path and its raw query string.
fn split_path(url: &str) -> (String, String) {
    let after_scheme = url.split_once("://").map_or(url, |(_, rest)| rest);
    let path_and_query = after_scheme
        .find('/')
        .map_or("/", |i| after_scheme.get(i..).unwrap_or("/"));
    let (path, query) = path_and_query
        .split_once('?')
        .unwrap_or((path_and_query, ""));
    (path.to_string(), query.to_string())
}

/// A `ListObjectsV2` answer, paged at the configured size.
fn list_objects(state: &State, query: &str) -> HttpResponse {
    use std::fmt::Write as _;

    let params: BTreeMap<&str, String> = query
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|pair| {
            let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
            (name, percent_decode(value))
        })
        .collect();
    let prefix = params.get("prefix").cloned().unwrap_or_default();
    let after = params
        .get("continuation-token")
        .cloned()
        .unwrap_or_default();

    let matching: Vec<&String> = state
        .objects
        .keys()
        .filter(|k| k.starts_with(&prefix) && (after.is_empty() || **k > after))
        .collect();
    let page: Vec<&&String> = matching.iter().take(state.list_page_size).collect();
    let truncated = matching.len() > page.len();
    let next = page.last().map(|k| (**k).clone()).unwrap_or_default();

    let mut body = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult>");
    let _ = write!(body, "<IsTruncated>{truncated}</IsTruncated>");
    for key in &page {
        let _ = write!(body, "<Contents><Key>{}</Key></Contents>", escape(key));
    }
    if truncated {
        let _ = write!(
            body,
            "<NextContinuationToken>{}</NextContinuationToken>",
            escape(&next)
        );
    }
    body.push_str("</ListBucketResult>");
    HttpResponse::new(200).with_body(body.into_bytes())
}

fn error_response(status: u16) -> HttpResponse {
    error_response_with_code(status, "FixtureError")
}

fn error_response_with_code(status: u16, code: &str) -> HttpResponse {
    HttpResponse::new(status).with_body(
        format!("<?xml version=\"1.0\"?><Error><Code>{code}</Code></Error>").into_bytes(),
    )
}

fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// Undo the signer's percent-encoding for the two parameters the fixture reads.
fn percent_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes.get(i) {
            Some(b'%') if i + 2 < bytes.len() => {
                let hex = text.get(i + 1..i + 3).unwrap_or("");
                if let Ok(byte) = u8::from_str_radix(hex, 16) {
                    out.push(byte);
                    i += 3;
                } else {
                    out.push(b'%');
                    i += 1;
                }
            }
            Some(byte) => {
                out.push(*byte);
                i += 1;
            }
            None => break,
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::{FakeS3, percent_decode, split_path};
    use crate::s3::http::{HttpRequest, HttpTransport};

    #[test]
    fn an_object_round_trips_and_a_missing_one_is_a_404() {
        let fake = FakeS3::new("b");
        assert_eq!(
            fake.execute(
                &HttpRequest::new("PUT", "https://h/b/sha256/aa").with_body(b"x".to_vec())
            )
            .expect("put")
            .status(),
            200
        );
        let got = fake
            .execute(&HttpRequest::new("GET", "https://h/b/sha256/aa"))
            .expect("get");
        assert_eq!(got.status(), 200);
        assert_eq!(got.body(), b"x");
        assert_eq!(
            fake.execute(&HttpRequest::new("GET", "https://h/b/sha256/zz"))
                .expect("get")
                .status(),
            404
        );
        // Deleting a key that was never there is still a 204.
        assert_eq!(
            fake.execute(&HttpRequest::new("DELETE", "https://h/b/sha256/zz"))
                .expect("delete")
                .status(),
            204
        );
    }

    #[test]
    fn the_fault_switches_do_what_they_say() {
        let fake = FakeS3::new("b");
        fake.fail_next(2);
        assert!(
            fake.execute(&HttpRequest::new("GET", "https://h/b/x"))
                .is_err()
        );
        assert!(
            fake.execute(&HttpRequest::new("GET", "https://h/b/x"))
                .is_err()
        );
        assert!(
            fake.execute(&HttpRequest::new("GET", "https://h/b/x"))
                .is_ok()
        );

        fake.respond_next_with_status(1, 500);
        assert_eq!(
            fake.execute(&HttpRequest::new("GET", "https://h/b/x"))
                .expect("a status is a response")
                .status(),
            500
        );
        assert_eq!(
            fake.execute(&HttpRequest::new("GET", "https://h/b/x"))
                .expect("back to normal")
                .status(),
            404
        );

        fake.set_unreachable(true);
        assert!(
            fake.execute(&HttpRequest::new("GET", "https://h/b/x"))
                .is_err()
        );
    }

    #[test]
    fn urls_and_encoded_parameters_are_read_back() {
        assert_eq!(
            split_path("https://host:9000/bucket/key?a=1&b=2"),
            ("/bucket/key".to_string(), "a=1&b=2".to_string())
        );
        assert_eq!(percent_decode("a%2Fb%2Bc"), "a/b+c");
        assert_eq!(percent_decode("plain"), "plain");
    }
}

//! The **transport port**: the request/response value types the S3 backend is
//! written against, and the one trait a real HTTP client implements.
//!
//! # Why the backend does not own an HTTP client
//!
//! Everything hard about talking to an object store — canonical requests, request
//! signing, the status-to-classification mapping, pagination, the bounded retry —
//! is *protocol*, and protocol is deterministic and testable with no socket. What
//! is left is "send these bytes and give me those bytes back", which needs TLS,
//! certificate verification, connection handling and a maintained security
//! posture.
//!
//! So the split follows the boundary the workspace already keeps: the protocol
//! lives here, in a crate whose dependency table is empty and which every
//! `cargo build --all` compiles; the client lives in `dagr-cli` behind a
//! default-off feature, next to the runtime and the rest of the third-party tree.
//! A plain build therefore compiles no HTTP or TLS crate at all, and every
//! interesting failure — an unreachable store, a 403, a 500 that clears on the
//! third try — is inducible in-process against a fake instead of raced against a
//! real endpoint.

use std::error::Error;
use std::fmt;

/// A request the backend needs executed.
///
/// Deliberately a value, not a builder over someone's HTTP types: the whole point
/// is that this crate names no HTTP library. `url` is absolute and already
/// includes the query string; `headers` are in the order the signer produced them
/// and include the `Authorization` header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpRequest {
    method: String,
    url: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl HttpRequest {
    /// A request with no headers and an empty body.
    #[must_use]
    pub fn new(method: impl Into<String>, url: impl Into<String>) -> Self {
        Self {
            method: method.into(),
            url: url.into(),
            headers: Vec::new(),
            body: Vec::new(),
        }
    }

    /// Add a header.
    #[must_use]
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    /// Set the request body.
    #[must_use]
    pub fn with_body(mut self, body: Vec<u8>) -> Self {
        self.body = body;
        self
    }

    /// The HTTP method (`GET`, `PUT`, `HEAD`, `DELETE`).
    #[must_use]
    pub fn method(&self) -> &str {
        &self.method
    }

    /// The absolute URL, query string included.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Every header, in order.
    #[must_use]
    pub fn headers(&self) -> &[(String, String)] {
        &self.headers
    }

    /// The request body.
    #[must_use]
    pub fn body(&self) -> &[u8] {
        &self.body
    }

    /// The first header whose name matches `name`, case-insensitively.
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

/// What a transport got back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpResponse {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl HttpResponse {
    /// A response with the given status, no headers and an empty body.
    #[must_use]
    pub fn new(status: u16) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: Vec::new(),
        }
    }

    /// Add a response header.
    #[must_use]
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    /// Set the response body.
    #[must_use]
    pub fn with_body(mut self, body: Vec<u8>) -> Self {
        self.body = body;
        self
    }

    /// The HTTP status code.
    #[must_use]
    pub fn status(&self) -> u16 {
        self.status
    }

    /// The first header whose name matches `name`, case-insensitively.
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    /// The response body.
    #[must_use]
    pub fn body(&self) -> &[u8] {
        &self.body
    }
}

/// A request that never produced a response at all — a DNS failure, a refused
/// connection, a TLS handshake that did not complete, a read that timed out.
///
/// It is deliberately **not** classified: a transport reports what happened and
/// the backend decides what it means. Every such failure is transient by
/// definition — no response means no evidence about the object — and the backend
/// maps it accordingly.
///
/// A transport must never put a credential in this message. The backend is the
/// only thing that ever sees it, but it also renders it into a `BlobError`, and
/// that reaches an operator.
#[derive(Debug)]
pub struct TransportError {
    message: String,
    source: Option<Box<dyn Error + Send + Sync + 'static>>,
}

impl TransportError {
    /// A transport failure with a human-readable cause.
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            source: None,
        }
    }

    /// Attach the underlying cause, preserved through [`Error::source`].
    #[must_use]
    pub fn with_source(mut self, source: impl Error + Send + Sync + 'static) -> Self {
        self.source = Some(Box::new(source));
        self
    }

    /// The human-readable cause.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl Error for TransportError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_ref()
            .map(|boxed| &**boxed as &(dyn Error + 'static))
    }
}

/// Execute an HTTP request and return the response.
///
/// Synchronous, because the blob port is: `put` / `get` / `head` are blocking
/// calls, exactly as the local backend's file I/O is, and a caller on an async
/// worker treats them the way it treats the scratch store.
///
/// An implementation **does not retry** and **does not interpret status codes**:
/// a 404 and a 500 are both responses, and the bounded retry is the backend's
/// single policy. Two retry loops would multiply the bound.
pub trait HttpTransport {
    /// Send `request` and return what came back.
    ///
    /// # Errors
    ///
    /// [`TransportError`] when no response was produced at all. A response with
    /// any status — including 4xx and 5xx — is a success at this level.
    fn execute(&self, request: &HttpRequest) -> Result<HttpResponse, TransportError>;
}

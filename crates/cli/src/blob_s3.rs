//! The **HTTP client** the object-store backend's sans-IO protocol needs, and the
//! ambient configuration that points it at a bucket.
//!
//! This is the whole of the network dependency M10 adds, and it is here rather
//! than in `dagr-blob` for the reason the workspace keeps every other third-party
//! tree out of a boundary crate: `dagr-blob` declares **no dependencies at all**
//! (an asserted invariant — `scripts/check-blob-feature-gating.sh`), so it holds
//! the protocol and this module holds the socket. Behind the default-off
//! `blob-s3` feature, so `cargo build --all`, `cargo build -p dagr-cli
//! --no-default-features`, and even `--features blob` compile no HTTP or TLS
//! crate: a pipeline using the local backend pays nothing for this.
//!
//! # What is trusted to a maintained crate, and why
//!
//! `dagr-blob` implements SHA-256, HMAC and `SigV4` in-tree, because each is a
//! fixed, fully specified function with published vectors. That argument does
//! **not** extend to TLS, certificate-chain verification or HTTP framing: they
//! are negotiated, adversarial, and carry a standing security-maintenance
//! obligation. So they come from `ureq` + `rustls`, and the trust boundary sits
//! exactly where the argument stops.
//!
//! Two `rustls` details are inherited deliberately from the Kubernetes client's
//! hard-won configuration (T101/T107):
//!
//! * **a crypto provider is installed explicitly, once.** `rustls` 0.23 panics on
//!   the first handshake if no process-level provider was chosen, and a panic on
//!   first use is not an acceptable failure mode for an opt-in feature.
//! * **roots come from the platform trust store** (`rustls-native-certs`), not
//!   from a bundled CA list. That keeps the licence surface inside the existing
//!   allow-list, and it means an operator's private CA — the normal case for an
//!   in-cluster `MinIO` or gateway — works by being trusted where everything else on
//!   the host trusts it, with no dagr-specific configuration.

use std::sync::Arc;
use std::time::Duration;

use dagr_blob::s3::http::{HttpRequest, HttpResponse, HttpTransport, TransportError};
use dagr_blob::{S3Blob, S3Config, S3Credentials};

use crate::config::{
    ambient_env, resolve_blob_endpoint, resolve_blob_region,
};

/// How long a single object-store request may take, end to end.
///
/// Generous, because a blob can be large and a cold object store can be slow; and
/// bounded, because the backend's retry can only be a *bound* if an individual
/// attempt is one. It is deliberately not a `--dagr.*` knob, on the same
/// precedent as the pod stall bound: it becomes one if an acceptance run shows it
/// needs to be.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(120);

/// A real HTTPS transport for the object-store backend.
pub struct HttpsTransport {
    agent: ureq::Agent,
}

impl std::fmt::Debug for HttpsTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpsTransport").finish_non_exhaustive()
    }
}

impl HttpsTransport {
    /// Build a transport trusting the platform's certificate store.
    ///
    /// # Errors
    ///
    /// A message naming what failed when the platform trust store could not be
    /// read. It is a hard failure rather than a fallback to "trust nothing" or
    /// "trust everything": both of those are decisions a storage client has no
    /// business making silently.
    pub fn new() -> Result<Self, String> {
        // Install a provider exactly once per process. A second install is not an
        // error here — another feature (the Kubernetes client) may have installed
        // the same one already, and both want `ring`.
        let _ = rustls::crypto::ring::default_provider().install_default();

        let loaded = rustls_native_certs::load_native_certs();
        if loaded.certs.is_empty() {
            return Err(format!(
                "the platform trust store yielded no certificates ({} error(s) while reading \
                 it), so no object-store endpoint could be verified",
                loaded.errors.len()
            ));
        }
        let roots: Vec<ureq::tls::Certificate<'static>> = loaded
            .certs
            .iter()
            .map(|der| ureq::tls::Certificate::from_der(der.as_ref()).to_owned())
            .collect();

        let tls = ureq::tls::TlsConfig::builder()
            .provider(ureq::tls::TlsProvider::Rustls)
            .root_certs(ureq::tls::RootCerts::Specific(Arc::new(roots)))
            .build();
        let config = ureq::Agent::config_builder()
            .tls_config(tls)
            .timeout_global(Some(REQUEST_TIMEOUT))
            // The backend owns the one bounded retry; a second one underneath it
            // would multiply the bound and make the attempt count in a classified
            // error a lie.
            .max_redirects(0)
            .build();
        Ok(Self {
            agent: ureq::Agent::new_with_config(config),
        })
    }
}

impl HttpTransport for HttpsTransport {
    fn execute(&self, request: &HttpRequest) -> Result<HttpResponse, TransportError> {
        let mut builder = self.agent.request(request.method(), request.url());
        for (name, value) in request.headers() {
            // `host` is set by the client from the URL; sending it again is a
            // duplicate header, and the signature already covers the authority the
            // URL names.
            if name.eq_ignore_ascii_case("host") {
                continue;
            }
            builder = builder.header(name.as_str(), value.as_str());
        }

        // A response with ANY status is a success at this level: the backend owns
        // the status-to-classification mapping, and a client that turned a 404 into
        // an error would erase the one status that means "the referent is gone".
        let sent = if request.body().is_empty() {
            builder.send_empty()
        } else {
            builder.send(request.body())
        };
        let mut response = match sent {
            Ok(response) => response,
            Err(ureq::Error::StatusCode(status)) => {
                return Ok(HttpResponse::new(status));
            }
            Err(err) => {
                return Err(TransportError::new(err.to_string()).with_source(err));
            }
        };

        let status = response.status().as_u16();
        let headers: Vec<(String, String)> = response
            .headers()
            .iter()
            .map(|(name, value)| {
                (
                    name.as_str().to_string(),
                    String::from_utf8_lossy(value.as_bytes()).into_owned(),
                )
            })
            .collect();
        let body = response
            .body_mut()
            .with_config()
            .limit(u64::MAX)
            .read_to_vec()
            .map_err(|err| TransportError::new(err.to_string()))?;

        let mut out = HttpResponse::new(status).with_body(body);
        for (name, value) in headers {
            out = out.with_header(name, value);
        }
        Ok(out)
    }
}

/// Open the object store holding `container` (`<bucket>[/<prefix>]`), taking its
/// endpoint and region from the ambient environment and its credentials from
/// wherever the platform put them.
///
/// The container comes from the caller — usually out of a blob reference — and
/// everything else from the process, which is the split the reference grammar is
/// designed around: the same bucket is reached through different endpoints from
/// inside and outside a cluster, so a reference that carried one would stop
/// resolving when the network changed and the storage did not.
///
/// # Errors
///
/// A message naming what was missing: a container that names no bucket, an
/// unusable endpoint, an unreadable trust store, or — named precisely, and
/// distinguishably from a missing object — no credential in the environment.
pub fn open_ambient(container: String) -> Result<S3Blob<HttpsTransport>, String> {
    let mut config = S3Config::from_container(&container).ok_or_else(|| {
        format!("`{container}` names no bucket, so no object store could be opened")
    })?;
    let read = &ambient_env as &dyn Fn(&str) -> Option<String>;
    let region = resolve_blob_region(None, read).map_err(|err| err.to_string())?;
    config = config.with_region(region);
    if let Some(endpoint) = resolve_blob_endpoint(None, read).map_err(|err| err.to_string())? {
        config = config.with_endpoint(endpoint);
    }
    // The credential error renders what it looked for and never what it found.
    let credentials = S3Credentials::from_ambient_environment().map_err(|err| err.to_string())?;
    Ok(S3Blob::new(config, credentials, HttpsTransport::new()?))
}

#[cfg(test)]
mod tests {
    use super::open_ambient;

    /// A container that names no bucket is refused by name rather than producing a
    /// store addressed at nothing.
    #[test]
    fn a_container_without_a_bucket_is_refused() {
        let err = open_ambient(String::new()).expect_err("no bucket");
        assert!(err.contains("names no bucket"), "{err}");
    }
}

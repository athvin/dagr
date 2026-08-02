//! **Credentials, from the ambient environment only.**
//!
//! dagr holds no credential of its own and adds no credential surface. It does
//! not accept one on the command line, does not read one out of a reference, does
//! not store one, and does not mint one. What it does is read the credential the
//! *platform* already put in the process — an injected secret, a projected token,
//! a mounted profile — which is how every conventional S3 client behaves and how
//! a pod gets storage access by standard cluster mechanisms.
//!
//! # The resolution order
//!
//! 1. **The environment**: `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY`, with an
//!    optional `AWS_SESSION_TOKEN`. This is the tier a Kubernetes `Secret`
//!    projected as environment variables lands in, and the tier `aws configure
//!    export-credentials` and most CI systems populate.
//! 2. **A shared credentials file**: `AWS_SHARED_CREDENTIALS_FILE`, else
//!    `$HOME/.aws/credentials`, under `AWS_PROFILE` (default `default`). This is
//!    the tier a mounted secret volume and a developer machine land in.
//!
//! # What is deliberately **not** implemented
//!
//! Web-identity/IRSA **token exchange**. `AWS_WEB_IDENTITY_TOKEN_FILE` names a
//! projected service-account token that must be traded for temporary credentials
//! by calling STS — a second AWS service, a second protocol, a second signing
//! path, and a background refresh loop. That is a credential *broker*, which is
//! exactly the surface this module's first paragraph says dagr does not add. An
//! operator on IRSA runs the exchange where every other workload does — in the
//! platform, via a projected credential file or injected variables — and dagr
//! reads the result. If the refusal below is what an operator hits, it names
//! precisely what was looked for, which is what makes that a five-second
//! diagnosis rather than a mystery.

use std::error::Error;
use std::fmt;

/// The environment variable holding the access key id.
pub const ACCESS_KEY_ID_ENV: &str = "AWS_ACCESS_KEY_ID";
/// The environment variable holding the secret access key.
pub const SECRET_ACCESS_KEY_ENV: &str = "AWS_SECRET_ACCESS_KEY";
/// The environment variable holding an optional session token.
pub const SESSION_TOKEN_ENV: &str = "AWS_SESSION_TOKEN";
/// The environment variable naming a shared credentials file.
pub const SHARED_CREDENTIALS_FILE_ENV: &str = "AWS_SHARED_CREDENTIALS_FILE";
/// The environment variable naming the profile within that file.
pub const PROFILE_ENV: &str = "AWS_PROFILE";

/// The credential a request is signed with.
///
/// The secret is reachable only through [`expose_secret`](S3Credentials::expose_secret),
/// which the signer calls and nothing else does. There is **no** `Display`, and
/// `Debug` renders a redaction marker rather than the values — so a credential
/// cannot reach a log line, an event record or an error message by the ordinary
/// route, which is a struct being formatted into a diagnostic.
#[derive(Clone)]
pub struct S3Credentials {
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
}

impl S3Credentials {
    /// Credentials from an access key id and its secret.
    #[must_use]
    pub fn new(access_key_id: impl Into<String>, secret_access_key: impl Into<String>) -> Self {
        Self {
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
            session_token: None,
        }
    }

    /// Attach a session token (temporary credentials).
    #[must_use]
    pub fn with_session_token(mut self, token: impl Into<String>) -> Self {
        self.session_token = Some(token.into());
        self
    }

    /// The access key id — an **identifier**, not a secret, and the one part of a
    /// credential that legitimately travels in a signed request.
    #[must_use]
    pub fn access_key_id(&self) -> &str {
        &self.access_key_id
    }

    /// Whether temporary-credential session token is carried.
    #[must_use]
    pub fn has_session_token(&self) -> bool {
        self.session_token.is_some()
    }

    /// The session token, for the signer.
    #[must_use]
    pub fn session_token(&self) -> Option<&str> {
        self.session_token.as_deref()
    }

    /// The secret, for the signer's key-derivation chain and nothing else.
    ///
    /// Named to be conspicuous at the call site: there is exactly one caller,
    /// `sigv4::sign`, and a second one is a review question.
    #[must_use]
    pub fn expose_secret(&self) -> &str {
        &self.secret_access_key
    }

    /// What a redacted credential renders as.
    #[must_use]
    pub fn redacted(&self) -> &'static str {
        "<redacted credential>"
    }

    /// Resolve credentials from the process environment, in the documented order.
    ///
    /// # Errors
    ///
    /// [`CredentialError`] naming every variable and file that was consulted, so a
    /// missing credential is diagnosable without guessing — and so it is never
    /// confused with a missing object.
    pub fn from_ambient_environment() -> Result<Self, CredentialError> {
        Self::from_ambient_environment_in(|name| std::env::var(name).ok())
    }

    /// The same resolution, over an injected environment reader.
    ///
    /// This is the seam the tests drive: `std::env` is process-global, so a suite
    /// that set and unset real variables would be order-dependent under CI
    /// parallelism.
    ///
    /// # Errors
    ///
    /// [`CredentialError`] naming everything that was consulted.
    pub fn from_ambient_environment_in<F>(read: F) -> Result<Self, CredentialError>
    where
        F: Fn(&str) -> Option<String>,
    {
        let non_empty = |name: &str| read(name).filter(|v| !v.trim().is_empty());

        // Tier 1: the environment.
        if let (Some(id), Some(secret)) = (
            non_empty(ACCESS_KEY_ID_ENV),
            non_empty(SECRET_ACCESS_KEY_ENV),
        ) {
            let mut creds = Self::new(id, secret);
            if let Some(token) = non_empty(SESSION_TOKEN_ENV) {
                creds = creds.with_session_token(token);
            }
            return Ok(creds);
        }

        // Tier 2: a shared credentials file, under the selected profile.
        let profile = non_empty(PROFILE_ENV).unwrap_or_else(|| "default".to_string());
        let path = non_empty(SHARED_CREDENTIALS_FILE_ENV).or_else(|| {
            non_empty("HOME").map(|home| format!("{home}/.aws/credentials"))
        });
        if let Some(path) = &path
            && let Ok(text) = std::fs::read_to_string(path)
            && let Some(creds) = from_profile(&text, &profile)
        {
            return Ok(creds);
        }

        Err(CredentialError {
            profile,
            file: path,
        })
    }
}

/// Redacted by construction: the fields are named, the values are not.
impl fmt::Debug for S3Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3Credentials")
            .field("access_key_id", &self.redacted())
            .field("secret_access_key", &self.redacted())
            .field(
                "session_token",
                &if self.session_token.is_some() {
                    self.redacted()
                } else {
                    "<none>"
                },
            )
            .finish()
    }
}

/// No credential was available anywhere in the ambient environment.
///
/// This is deliberately **not** a [`BlobError`](crate::BlobError): a missing
/// credential is not a missing object, and it must never be mistaken for one —
/// an `Absent` verdict refuses a resume plan up front, and a store nobody can
/// authenticate to is not evidence that anything was deleted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialError {
    profile: String,
    file: Option<String>,
}

impl CredentialError {
    /// The profile that was looked for in the shared credentials file.
    #[must_use]
    pub fn profile(&self) -> &str {
        &self.profile
    }

    /// The shared credentials file that was consulted, if one could be located.
    #[must_use]
    pub fn file(&self) -> Option<&str> {
        self.file.as_deref()
    }
}

impl fmt::Display for CredentialError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "no S3 credential is available in this process's environment. dagr holds no \
             credential of its own and reads only what the platform supplied: it looked for \
             `{ACCESS_KEY_ID_ENV}` + `{SECRET_ACCESS_KEY_ENV}` (optionally \
             `{SESSION_TOKEN_ENV}`), then for profile `[{}]` in ",
            self.profile
        )?;
        match &self.file {
            Some(path) => write!(f, "`{path}`")?,
            None => write!(
                f,
                "a shared credentials file (`{SHARED_CREDENTIALS_FILE_ENV}` is unset and no \
                 `HOME` was readable)"
            )?,
        }
        write!(
            f,
            ". Supply one the way every other workload on this platform does — an injected \
             secret, or a credentials file — rather than passing one to dagr."
        )
    }
}

impl Error for CredentialError {}

/// Read `[profile]` out of a shared credentials file. A minimal INI reader: the
/// format has exactly one shape that matters here, and it is not worth a
/// dependency.
fn from_profile(text: &str, profile: &str) -> Option<S3Credentials> {
    let mut in_section = false;
    let mut id = None;
    let mut secret = None;
    let mut token = None;
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }
        if let Some(name) = line.strip_prefix('[').and_then(|l| l.strip_suffix(']')) {
            if in_section {
                break; // the next section ends ours
            }
            in_section = name.trim() == profile;
            continue;
        }
        if !in_section {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let value = value.trim().to_string();
        if value.is_empty() {
            continue;
        }
        match key.trim() {
            "aws_access_key_id" => id = Some(value),
            "aws_secret_access_key" => secret = Some(value),
            "aws_session_token" => token = Some(value),
            _ => {}
        }
    }
    let mut creds = S3Credentials::new(id?, secret?);
    if let Some(token) = token {
        creds = creds.with_session_token(token);
    }
    Some(creds)
}

#[cfg(test)]
mod tests {
    use super::{CredentialError, S3Credentials, from_profile};

    #[test]
    fn the_environment_tier_wins_and_carries_a_session_token() {
        let creds = S3Credentials::from_ambient_environment_in(|name| match name {
            "AWS_ACCESS_KEY_ID" => Some("AKIA".to_string()),
            "AWS_SECRET_ACCESS_KEY" => Some("secret".to_string()),
            "AWS_SESSION_TOKEN" => Some("token".to_string()),
            _ => None,
        })
        .expect("resolved");
        assert_eq!(creds.access_key_id(), "AKIA");
        assert_eq!(creds.session_token(), Some("token"));
    }

    #[test]
    fn a_blank_environment_value_is_not_a_credential() {
        let err = S3Credentials::from_ambient_environment_in(|name| match name {
            "AWS_ACCESS_KEY_ID" => Some("   ".to_string()),
            "AWS_SECRET_ACCESS_KEY" => Some(String::new()),
            _ => None,
        })
        .expect_err("blank is not set");
        assert_eq!(err.profile(), "default");
    }

    #[test]
    fn a_profile_is_read_out_of_a_shared_credentials_file() {
        let text = "\
# a comment
[default]
aws_access_key_id = AKIADEFAULT
aws_secret_access_key = default-secret

[prod]
aws_access_key_id = AKIAPROD
aws_secret_access_key = prod-secret
aws_session_token = prod-token
";
        let default = from_profile(text, "default").expect("default profile");
        assert_eq!(default.access_key_id(), "AKIADEFAULT");
        assert!(!default.has_session_token());

        let prod = from_profile(text, "prod").expect("prod profile");
        assert_eq!(prod.access_key_id(), "AKIAPROD");
        assert_eq!(prod.session_token(), Some("prod-token"));

        assert!(from_profile(text, "absent").is_none());
        // A profile missing half a credential is not a credential.
        assert!(from_profile("[x]\naws_access_key_id = only-an-id\n", "x").is_none());
    }

    #[test]
    fn the_refusal_names_everything_it_consulted_and_leaks_nothing() {
        let err = CredentialError {
            profile: "prod".to_string(),
            file: Some("/etc/aws/credentials".to_string()),
        };
        let text = err.to_string();
        assert!(text.contains("AWS_ACCESS_KEY_ID"));
        assert!(text.contains("AWS_SECRET_ACCESS_KEY"));
        assert!(text.contains("[prod]"));
        assert!(text.contains("/etc/aws/credentials"));
    }

    #[test]
    fn debug_redacts_every_value() {
        let creds = S3Credentials::new("AKIA-visible-id", "a-real-secret").with_session_token("t");
        let rendered = format!("{creds:?}");
        assert!(!rendered.contains("a-real-secret"), "{rendered}");
        assert!(!rendered.contains("AKIA-visible-id"), "{rendered}");
        assert!(rendered.contains("redacted"), "{rendered}");
    }
}

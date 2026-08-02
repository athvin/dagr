//! **AWS Signature Version 4** request signing, and the calendar arithmetic its
//! timestamps need.
//!
//! `SigV4` is the authentication scheme every S3-compatible store speaks. It is a
//! fully specified construction over HMAC-SHA256: build a canonical request, hash
//! it, wrap the hash in a string-to-sign, derive a date/region/service-scoped
//! signing key, and HMAC the two together. Nothing about it is negotiated, so it
//! is implemented here rather than pulled in — the same reason the content
//! address is.
//!
//! The credential itself never travels: the request carries the **access key id**
//! (an identifier, not a secret) and a signature *derived* from the secret. The
//! secret is used only as the root of the key-derivation chain and is never
//! rendered anywhere.

use std::fmt::Write as _;

use crate::digest::{Sha256, to_hex};
use crate::hmac::hmac_sha256;
use crate::s3::creds::S3Credentials;
use crate::s3::http::HttpRequest;

/// The algorithm name that appears in the string-to-sign and the header.
pub(crate) const ALGORITHM: &str = "AWS4-HMAC-SHA256";

/// The service name in the credential scope. Always `s3` here.
pub(crate) const SERVICE: &str = "s3";

/// The scope terminator every `SigV4` credential scope ends with.
const TERMINATOR: &str = "aws4_request";

/// A signing instant, split into the two renderings `SigV4` needs: the full
/// `YYYYMMDDTHHMMSSZ` stamp and the `YYYYMMDD` date the credential scope uses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SigningTime {
    stamp: String,
    date: String,
}

impl SigningTime {
    /// Build a signing time from seconds since the Unix epoch.
    #[must_use]
    pub fn from_unix_seconds(seconds: u64) -> Self {
        let (year, month, day, hour, minute, second) = civil_from_unix_seconds(seconds);
        Self {
            stamp: format!("{year:04}{month:02}{day:02}T{hour:02}{minute:02}{second:02}Z"),
            date: format!("{year:04}{month:02}{day:02}"),
        }
    }

    /// The current instant, from the system clock.
    ///
    /// A clock that is before the Unix epoch (only reachable on a machine whose
    /// clock is badly wrong) resolves to the epoch rather than panicking: a
    /// request signed with a wrong timestamp is rejected by the store with a
    /// message naming the skew, which is a far better failure than a panic inside
    /// a blob write.
    #[must_use]
    pub fn now() -> Self {
        let seconds = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
        Self::from_unix_seconds(seconds)
    }

    /// `YYYYMMDDTHHMMSSZ` — the `x-amz-date` header value.
    #[must_use]
    pub fn stamp(&self) -> &str {
        &self.stamp
    }

    /// `YYYYMMDD` — the date in the credential scope.
    #[must_use]
    pub fn date(&self) -> &str {
        &self.date
    }
}

/// Sign `request` in place, returning it with `x-amz-date`,
/// `x-amz-content-sha256`, an optional `x-amz-security-token`, and the
/// `Authorization` header attached.
///
/// `host` is the authority the request will actually be sent to; it is signed,
/// so a transport that rewrites it invalidates the signature.
#[must_use]
pub fn sign(
    request: HttpRequest,
    host: &str,
    credentials: &S3Credentials,
    region: &str,
    time: &SigningTime,
) -> HttpRequest {
    let payload_hash = {
        let mut hasher = Sha256::new();
        hasher.update(request.body());
        to_hex(&hasher.finish())
    };

    let mut request = request
        .with_header("host", host)
        .with_header("x-amz-date", time.stamp())
        .with_header("x-amz-content-sha256", &payload_hash);
    if let Some(token) = credentials.session_token() {
        request = request.with_header("x-amz-security-token", token);
    }

    // --- The canonical request -------------------------------------------------
    let (path, query) = split_url(request.url());
    let mut headers: Vec<(String, String)> = request
        .headers()
        .iter()
        .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().to_string()))
        .collect();
    headers.sort_by(|a, b| a.0.cmp(&b.0));
    headers.dedup_by(|a, b| a.0 == b.0);

    let (canonical_request, signed_headers) =
        canonical_request(request.method(), path, query, &headers, &payload_hash);

    // --- The string to sign ----------------------------------------------------
    let scope = format!("{}/{region}/{SERVICE}/{TERMINATOR}", time.date());
    let hashed_request = {
        let mut hasher = Sha256::new();
        hasher.update(canonical_request.as_bytes());
        to_hex(&hasher.finish())
    };
    let string_to_sign = format!("{ALGORITHM}\n{}\n{scope}\n{hashed_request}", time.stamp());

    // --- The signature ---------------------------------------------------------
    let signing_key = signing_key(credentials.expose_secret(), time.date(), region, SERVICE);
    let signature = to_hex(&hmac_sha256(&signing_key, string_to_sign.as_bytes()));

    request.with_header(
        "authorization",
        format!(
            "{ALGORITHM} Credential={}/{scope}, SignedHeaders={signed_headers}, \
             Signature={signature}",
            credentials.access_key_id()
        ),
    )
}

/// Build the **canonical request** — the six-line document `SigV4` hashes — and the
/// `;`-joined signed-header list that goes with it.
///
/// `headers` must already be lowercased, value-trimmed and sorted by name.
/// Returned as a pair rather than written into the signature directly so a test
/// can drive the real construction against a published intermediate value instead
/// of re-deriving it.
fn canonical_request(
    method: &str,
    path: &str,
    query: &str,
    headers: &[(String, String)],
    payload_hash: &str,
) -> (String, String) {
    let mut canonical_headers = String::new();
    let mut signed_headers = String::new();
    for (name, value) in headers {
        let _ = writeln!(canonical_headers, "{name}:{value}");
        if !signed_headers.is_empty() {
            signed_headers.push(';');
        }
        signed_headers.push_str(name);
    }
    let canonical = format!(
        "{method}\n{}\n{}\n{canonical_headers}\n{signed_headers}\n{payload_hash}",
        canonical_path(path),
        canonical_query(query),
    );
    (canonical, signed_headers)
}

/// The date/region/service-scoped signing key: four chained HMACs rooted at the
/// secret. The secret is used here and nowhere else.
#[must_use]
pub(crate) fn signing_key(secret: &str, date: &str, region: &str, service: &str) -> [u8; 32] {
    let mut key = Vec::with_capacity(4 + secret.len());
    key.extend_from_slice(b"AWS4");
    key.extend_from_slice(secret.as_bytes());
    let date_key = hmac_sha256(&key, date.as_bytes());
    let region_key = hmac_sha256(&date_key, region.as_bytes());
    let service_key = hmac_sha256(&region_key, service.as_bytes());
    hmac_sha256(&service_key, TERMINATOR.as_bytes())
}

/// Split an absolute URL into its path and its raw query string.
fn split_url(url: &str) -> (&str, &str) {
    let after_scheme = url.split_once("://").map_or(url, |(_, rest)| rest);
    let path_and_query = after_scheme
        .find('/')
        .map_or("/", |i| after_scheme.get(i..).unwrap_or("/"));
    path_and_query
        .split_once('?')
        .unwrap_or((path_and_query, ""))
}

/// The canonical URI: each path segment percent-encoded, `/` preserved.
///
/// S3 is the one service that does **not** double-encode the path, so the segment
/// text is encoded exactly once.
fn canonical_path(path: &str) -> String {
    if path.is_empty() {
        return "/".to_string();
    }
    path.split('/')
        .map(uri_encode)
        .collect::<Vec<_>>()
        .join("/")
}

/// The canonical query string: parameters sorted by encoded name, each
/// `name=value` percent-encoded, `&`-joined. A parameter with no value signs as
/// `name=`.
fn canonical_query(query: &str) -> String {
    if query.is_empty() {
        return String::new();
    }
    let mut params: Vec<(String, String)> = query
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|pair| {
            let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
            (uri_encode(name), uri_encode(value))
        })
        .collect();
    params.sort();
    params
        .into_iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// RFC 3986 percent-encoding: unreserved characters pass through, everything else
/// becomes `%XX` with uppercase hex. `/` is **not** unreserved and is encoded —
/// callers that must preserve it split on it first.
pub(crate) fn uri_encode(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(byte));
        } else {
            let _ = write!(out, "%{byte:02X}");
        }
    }
    out
}

/// Civil date and time from seconds since the Unix epoch, proleptic Gregorian.
///
/// This is Howard Hinnant's `civil_from_days` shifted to a March-based year, the
/// standard branch-free algorithm; it is here rather than behind a date crate for
/// the same reason everything else in this crate is — the dependency table stays
/// empty. Leap years, century rules and the 400-year cycle are all handled, and
/// the round-trip is checked against known instants below.
fn civil_from_unix_seconds(seconds: u64) -> (i64, u32, u32, u32, u32, u32) {
    let days = i64::try_from(seconds / 86_400).unwrap_or(i64::MAX);
    let seconds_of_day = seconds % 86_400;
    let hour = u32::try_from(seconds_of_day / 3_600).unwrap_or(0);
    let minute = u32::try_from((seconds_of_day % 3_600) / 60).unwrap_or(0);
    let second = u32::try_from(seconds_of_day % 60).unwrap_or(0);

    // Shift the epoch to 0000-03-01, which puts the leap day at the end of the
    // cycle and makes the month/day arithmetic a pair of linear maps.
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let day_of_era = z - era * 146_097; // [0, 146096]
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let mp = (5 * day_of_year + 2) / 153; // [0, 11], March-based
    let day = u32::try_from(day_of_year - (153 * mp + 2) / 5 + 1).unwrap_or(1);
    let month = u32::try_from(if mp < 10 { mp + 3 } else { mp - 9 }).unwrap_or(1);
    let year = if month <= 2 { year + 1 } else { year };
    (year, month, day, hour, minute, second)
}

#[cfg(test)]
mod tests {
    use super::{
        SigningTime, canonical_path, canonical_query, civil_from_unix_seconds, sign, signing_key,
        uri_encode,
    };
    use crate::digest::to_hex;
    use crate::s3::creds::S3Credentials;
    use crate::s3::http::HttpRequest;

    /// **The published AWS key-derivation example.** This is the one part of
    /// `SigV4` the documentation gives a byte-exact answer for, and it exercises the
    /// whole HMAC chain — a wrong terminator, a swapped region/service order, or a
    /// mis-prefixed secret all fail here.
    #[test]
    fn the_published_signing_key_derivation_holds() {
        let key = signing_key(
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            "20150830",
            "us-east-1",
            "iam",
        );
        assert_eq!(
            to_hex(&key),
            "c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9"
        );
    }

    /// **The published AWS S3 `GET Object` example's canonical request.** AWS
    /// documents the intermediate value — the hash of the canonical request — for
    /// exactly this reason: it is where every construction mistake shows up
    /// (segment encoding, header lowercasing and sorting, the trailing blank line,
    /// the signed-header list, the empty-payload hash), and it isolates them from
    /// the key derivation, which the vector above already pins.
    ///
    /// Together the two vectors cover the whole construction: this one proves the
    /// document is right, that one proves the key is right, and the signature is
    /// one HMAC of the two.
    #[test]
    fn the_published_s3_canonical_request_hashes_to_its_documented_value() {
        let empty_payload = to_hex(&{
            let mut h = crate::digest::Sha256::new();
            h.update(b"");
            h.finish()
        });
        let headers = vec![
            (
                "host".to_string(),
                "examplebucket.s3.amazonaws.com".to_string(),
            ),
            ("range".to_string(), "bytes=0-9".to_string()),
            ("x-amz-content-sha256".to_string(), empty_payload.clone()),
            ("x-amz-date".to_string(), "20130524T000000Z".to_string()),
        ];
        let (canonical, signed_headers) =
            super::canonical_request("GET", "/test.txt", "", &headers, &empty_payload);
        assert_eq!(signed_headers, "host;range;x-amz-content-sha256;x-amz-date");
        let hashed = to_hex(&{
            let mut h = crate::digest::Sha256::new();
            h.update(canonical.as_bytes());
            h.finish()
        });
        assert_eq!(
            hashed, "7344ae5b7ee6c3e7e6b0fe0640412a37625d1fbfff95c48bbb2dc43964946972",
            "the canonical request is not the document AWS specifies:\n{canonical}"
        );
    }

    /// The whole `sign` path over the same published example, pinned end to end.
    /// The signature is the HMAC of the two vectors above and is reproduced here
    /// so a refactor of `sign` — header injection order, the scope string, the
    /// authorization rendering — cannot drift without a red test.
    #[test]
    fn the_published_s3_get_object_example_reproduces_its_signature() {
        let credentials = S3Credentials::new(
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
        );
        let time = SigningTime {
            stamp: "20130524T000000Z".to_string(),
            date: "20130524".to_string(),
        };
        let request = HttpRequest::new("GET", "https://examplebucket.s3.amazonaws.com/test.txt")
            .with_header("range", "bytes=0-9");
        let signed = sign(
            request,
            "examplebucket.s3.amazonaws.com",
            &credentials,
            "us-east-1",
            &time,
        );
        let authorization = signed
            .header("authorization")
            .expect("the signature is attached");
        assert_eq!(
            authorization,
            "AWS4-HMAC-SHA256 \
             Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
             SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, \
             Signature=67fe34c8530db585abddc51067328adfedb6e42487d2566dc7d927d6e2722900"
        );
    }

    #[test]
    fn a_session_token_is_signed_when_present() {
        let credentials =
            S3Credentials::new("AKIA", "secret").with_session_token("a-projected-token");
        let signed = sign(
            HttpRequest::new("GET", "https://host/bucket/key"),
            "host",
            &credentials,
            "us-east-1",
            &SigningTime::from_unix_seconds(1_700_000_000),
        );
        assert_eq!(
            signed.header("x-amz-security-token"),
            Some("a-projected-token")
        );
        assert!(
            signed
                .header("authorization")
                .expect("signed")
                .contains("x-amz-security-token"),
            "an injected token is part of the signature, not decoration"
        );
    }

    #[test]
    fn the_payload_hash_is_signed_so_a_changed_body_changes_the_signature() {
        let credentials = S3Credentials::new("AKIA", "secret");
        let time = SigningTime::from_unix_seconds(1_700_000_000);
        let one = sign(
            HttpRequest::new("PUT", "https://host/bucket/key").with_body(b"alpha".to_vec()),
            "host",
            &credentials,
            "us-east-1",
            &time,
        );
        let two = sign(
            HttpRequest::new("PUT", "https://host/bucket/key").with_body(b"beta".to_vec()),
            "host",
            &credentials,
            "us-east-1",
            &time,
        );
        assert_ne!(one.header("authorization"), two.header("authorization"));
        assert_ne!(
            one.header("x-amz-content-sha256"),
            two.header("x-amz-content-sha256")
        );
    }

    #[test]
    fn the_canonical_query_is_sorted_and_encoded() {
        assert_eq!(canonical_query(""), "");
        assert_eq!(
            canonical_query("list-type=2&prefix=a/b&continuation-token=x+y"),
            "continuation-token=x%2By&list-type=2&prefix=a%2Fb"
        );
        assert_eq!(canonical_query("marker"), "marker=");
    }

    #[test]
    fn the_canonical_path_encodes_segments_but_not_separators() {
        assert_eq!(canonical_path("/bucket/sha256/abc"), "/bucket/sha256/abc");
        assert_eq!(canonical_path("/bucket/a b"), "/bucket/a%20b");
        assert_eq!(canonical_path(""), "/");
        assert_eq!(uri_encode("a~b_c-d.e"), "a~b_c-d.e");
        assert_eq!(uri_encode("/"), "%2F");
    }

    #[test]
    fn the_calendar_arithmetic_is_right_at_the_awkward_instants() {
        // The epoch itself.
        assert_eq!(civil_from_unix_seconds(0), (1970, 1, 1, 0, 0, 0));
        // A leap day in a year divisible by 400.
        assert_eq!(civil_from_unix_seconds(951_782_400), (2000, 2, 29, 0, 0, 0));
        // The day after a non-leap century boundary's February.
        assert_eq!(
            civil_from_unix_seconds(4_107_542_400),
            (2100, 3, 1, 0, 0, 0)
        );
        // The documented S3 example's instant renders as the documented stamp.
        assert_eq!(
            SigningTime::from_unix_seconds(1_369_353_600).stamp(),
            "20130524T000000Z"
        );
        assert_eq!(
            SigningTime::from_unix_seconds(1_369_353_600).date(),
            "20130524"
        );
        // Time of day survives.
        assert_eq!(
            SigningTime::from_unix_seconds(1_369_353_600 + 3_723).stamp(),
            "20130524T010203Z"
        );
    }
}

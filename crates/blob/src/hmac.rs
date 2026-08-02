//! **HMAC-SHA256** (RFC 2104) over the in-tree SHA-256, and the key-derivation
//! chain AWS Signature Version 4 is defined in terms of.
//!
//! This exists for one reason: signing an object-store request. It is not a
//! general cryptographic toolkit and nothing else in the crate uses it.
//!
//! # Why in-tree, when a request signature *is* secret material
//!
//! The SHA-256 argument that justified an in-tree digest — a fully specified
//! fixed function over public bytes, checkable against published vectors — is
//! narrower than "hand-roll the crypto". HMAC is where it stops applying
//! cleanly, because a signing key is secret material. It is nevertheless in-tree
//! here, deliberately and on a much smaller claim: HMAC-SHA256 is *twelve lines
//! of block padding around the digest already in this crate*, it has published
//! vectors (RFC 4231) that this module checks, and taking a dependency for it
//! would put a third-party crate into `dagr-blob` — whose empty dependency table
//! is an asserted architectural boundary (`scripts/check-blob-feature-gating.sh`).
//!
//! What is *not* hand-rolled is the part where hand-rolling is genuinely
//! dangerous: TLS, certificate verification, and the HTTP transport all come from
//! maintained crates, and they live in `dagr-cli` behind a default-off feature
//! precisely because they cannot live here.
//!
//! The one operation with a timing-attack surface — comparing a received MAC —
//! is not performed anywhere: dagr only ever *produces* signatures.

use crate::digest::Sha256;

/// SHA-256's block size in bytes; HMAC's padding is defined over it.
const BLOCK: usize = 64;

/// `HMAC-SHA256(key, message)` — RFC 2104 with SHA-256 as the hash.
#[must_use]
pub fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    // RFC 2104 §2: a key longer than the block size is hashed first; a shorter
    // one is zero-padded to the block size.
    let mut block = [0u8; BLOCK];
    if key.len() > BLOCK {
        let mut hash = Sha256::new();
        hash.update(key);
        block[..32].copy_from_slice(&hash.finish());
    } else {
        block[..key.len()].copy_from_slice(key);
    }

    let mut inner_pad = [0x36u8; BLOCK];
    let mut outer_pad = [0x5cu8; BLOCK];
    for i in 0..BLOCK {
        inner_pad[i] ^= block[i];
        outer_pad[i] ^= block[i];
    }

    let mut inner = Sha256::new();
    inner.update(&inner_pad);
    inner.update(message);
    let inner = inner.finish();

    let mut outer = Sha256::new();
    outer.update(&outer_pad);
    outer.update(&inner);
    outer.finish()
}

#[cfg(test)]
mod tests {
    use super::hmac_sha256;
    use crate::digest::to_hex;

    /// RFC 4231 §4 test cases 1, 2, 3, 6 and 7 — the published vectors, including
    /// the two that exercise the long-key path (case 6 and 7 use a 131-byte key,
    /// which is longer than SHA-256's 64-byte block and must therefore be hashed
    /// down first). A hand-written HMAC that skipped that branch passes cases 1-3
    /// and fails these.
    #[test]
    fn the_published_rfc_4231_vectors_hold() {
        // Case 1.
        assert_eq!(
            to_hex(&hmac_sha256(&[0x0b; 20], b"Hi There")),
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
        );
        // Case 2 — a key shorter than the block, an ASCII message.
        assert_eq!(
            to_hex(&hmac_sha256(b"Jefe", b"what do ya want for nothing?")),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
        // Case 3.
        assert_eq!(
            to_hex(&hmac_sha256(&[0xaa; 20], &[0xdd; 50])),
            "773ea91e36800e46854db8ebd09181a72959098b3ef8c122d9635514ced565fe"
        );
        // Case 6 — key longer than one block (the hash-the-key branch).
        assert_eq!(
            to_hex(&hmac_sha256(
                &[0xaa; 131],
                b"Test Using Larger Than Block-Size Key - Hash Key First"
            )),
            "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"
        );
        // Case 7 — long key AND a message longer than one block.
        assert_eq!(
            to_hex(&hmac_sha256(
                &[0xaa; 131],
                b"This is a test using a larger than block-size key and a larger than block-size data. The key needs to be hashed before being used by the HMAC algorithm."
            )),
            "9b09ffa71b942fcb27635fbcd5b0e944bfdc63644f0713938a7f51535c3a35e2"
        );
    }

    /// An empty key and an empty message are both legal inputs, and the
    /// zero-padding branch must handle them rather than panicking on a slice.
    #[test]
    fn empty_inputs_are_legal() {
        assert_eq!(
            to_hex(&hmac_sha256(b"", b"")),
            "b613679a0814d9ec772f95d778c35fc5ff1697c493715653c6c712144292c5ad"
        );
    }
}

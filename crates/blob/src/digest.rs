//! The content address: **SHA-256**, implemented here with no dependency.
//!
//! # Why in-tree
//!
//! A content address needs **collision resistance**: two different values that
//! collapsed onto one key would be silent data corruption, so the FNV-1a digest
//! `dagr-core` carries for fingerprints is categorically unsuitable here. That
//! leaves a cryptographic hash — and taking one as a dependency would mean this
//! crate compiles a third-party tree, which is precisely what "a plain
//! `cargo build --all` reaches no storage dependency" forbids.
//!
//! SHA-256 is a fully specified, fixed function (FIPS 180-4) with published test
//! vectors, and dagr uses it as a **hash of public bytes** — never as a MAC, never
//! over secret material, with no key schedule and no constant-time requirement.
//! The implementation below is the specification transcribed; the vectors in this
//! module's tests are the check that the transcription is faithful.
//!
//! # Algorithm identity is part of the key
//!
//! A key renders as `sha256:<hex>` and a reference carries `/sha256/<hex>`, so a
//! recorded content hash names the function that produced it. Changing the
//! function is therefore an observable, reference-visible change — not a silent
//! swap that would make old references un-verifiable.

/// The digest algorithm every blob key is computed with.
pub const ALGORITHM: &str = "sha256";

/// The initial state — the first 32 bits of the fractional parts of the square
/// roots of the first eight primes (FIPS 180-4 §5.3.3).
const H0: [u32; 8] = [
    0x6a09_e667,
    0xbb67_ae85,
    0x3c6e_f372,
    0xa54f_f53a,
    0x510e_527f,
    0x9b05_688c,
    0x1f83_d9ab,
    0x5be0_cd19,
];

/// The round constants — the first 32 bits of the fractional parts of the cube
/// roots of the first sixty-four primes (FIPS 180-4 §4.2.2).
const K: [u32; 64] = [
    0x428a_2f98,
    0x7137_4491,
    0xb5c0_fbcf,
    0xe9b5_dba5,
    0x3956_c25b,
    0x59f1_11f1,
    0x923f_82a4,
    0xab1c_5ed5,
    0xd807_aa98,
    0x1283_5b01,
    0x2431_85be,
    0x550c_7dc3,
    0x72be_5d74,
    0x80de_b1fe,
    0x9bdc_06a7,
    0xc19b_f174,
    0xe49b_69c1,
    0xefbe_4786,
    0x0fc1_9dc6,
    0x240c_a1cc,
    0x2de9_2c6f,
    0x4a74_84aa,
    0x5cb0_a9dc,
    0x76f9_88da,
    0x983e_5152,
    0xa831_c66d,
    0xb003_27c8,
    0xbf59_7fc7,
    0xc6e0_0bf3,
    0xd5a7_9147,
    0x06ca_6351,
    0x1429_2967,
    0x27b7_0a85,
    0x2e1b_2138,
    0x4d2c_6dfc,
    0x5338_0d13,
    0x650a_7354,
    0x766a_0abb,
    0x81c2_c92e,
    0x9272_2c85,
    0xa2bf_e8a1,
    0xa81a_664b,
    0xc24b_8b70,
    0xc76c_51a3,
    0xd192_e819,
    0xd699_0624,
    0xf40e_3585,
    0x106a_a070,
    0x19a4_c116,
    0x1e37_6c08,
    0x2748_774c,
    0x34b0_bcb5,
    0x391c_0cb3,
    0x4ed8_aa4a,
    0x5b9c_ca4f,
    0x682e_6ff3,
    0x748f_82ee,
    0x78a5_636f,
    0x84c8_7814,
    0x8cc7_0208,
    0x90be_fffa,
    0xa450_6ceb,
    0xbef9_a3f7,
    0xc671_78f2,
];

/// A streaming SHA-256 hasher: feed it bytes with [`update`](Sha256::update), take
/// the digest with [`finish`](Sha256::finish).
///
/// Streaming matters because the store hashes objects it reads back from disk in
/// bounded chunks rather than loading a whole blob to compare a digest.
///
/// ```
/// use dagr_blob::digest::Sha256;
///
/// let mut hasher = Sha256::new();
/// hasher.update(b"ab");
/// hasher.update(b"c");
/// assert_eq!(
///     dagr_blob::digest::to_hex(&hasher.finish()),
///     "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
/// );
/// ```
#[derive(Debug, Clone)]
pub struct Sha256 {
    state: [u32; 8],
    /// The partial 64-byte block not yet compressed.
    block: [u8; 64],
    /// How much of `block` is filled.
    filled: usize,
    /// Total message length in **bits**, as the padding requires.
    total_bits: u64,
}

impl Default for Sha256 {
    fn default() -> Self {
        Self::new()
    }
}

impl Sha256 {
    /// A fresh hasher over the empty message.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: H0,
            block: [0; 64],
            filled: 0,
            total_bits: 0,
        }
    }

    /// Feed `data` into the hash. Splitting a message across calls yields the same
    /// digest as hashing it in one — the property the streaming reader relies on.
    pub fn update(&mut self, data: &[u8]) {
        self.total_bits = self.total_bits.wrapping_add(
            u64::try_from(data.len())
                .unwrap_or(u64::MAX)
                .wrapping_mul(8),
        );
        let mut rest = data;
        while !rest.is_empty() {
            let take = (64 - self.filled).min(rest.len());
            let (head, tail) = rest.split_at(take);
            self.block[self.filled..self.filled + take].copy_from_slice(head);
            self.filled += take;
            rest = tail;
            if self.filled == 64 {
                let block = self.block;
                compress(&mut self.state, &block);
                self.filled = 0;
            }
        }
    }

    /// Finish the hash and return the 32-byte digest.
    #[must_use]
    pub fn finish(mut self) -> [u8; 32] {
        // Padding (FIPS 180-4 §5.1.1): a `0x80` byte, then zeros, then the
        // 64-bit big-endian message length in bits.
        let bits = self.total_bits;
        self.update_raw(&[0x80]);
        while self.filled != 56 {
            self.update_raw(&[0x00]);
        }
        self.update_raw(&bits.to_be_bytes());

        let mut out = [0u8; 32];
        for (chunk, word) in out.chunks_exact_mut(4).zip(self.state.iter()) {
            chunk.copy_from_slice(&word.to_be_bytes());
        }
        out
    }

    /// Absorb padding bytes without counting them toward the message length.
    fn update_raw(&mut self, data: &[u8]) {
        for byte in data {
            self.block[self.filled] = *byte;
            self.filled += 1;
            if self.filled == 64 {
                let block = self.block;
                compress(&mut self.state, &block);
                self.filled = 0;
            }
        }
    }
}

/// The compression function over one 64-byte block (FIPS 180-4 §6.2.2).
fn compress(state: &mut [u32; 8], block: &[u8; 64]) {
    let mut w = [0u32; 64];
    for (word, chunk) in w.iter_mut().zip(block.chunks_exact(4)) {
        *word = u32::from_be_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
    }
    for i in 16..64 {
        let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
        let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
        w[i] = w[i - 16]
            .wrapping_add(s0)
            .wrapping_add(w[i - 7])
            .wrapping_add(s1);
    }

    // FIPS 180-4 §6.2.2 names the eight working variables `a`..`h`. They live in
    // one array here rather than as eight bindings — same arithmetic, and the
    // index/letter correspondence is fixed by this comment:
    //   v[0]=a  v[1]=b  v[2]=c  v[3]=d  v[4]=e  v[5]=f  v[6]=g  v[7]=h
    let mut v = *state;
    for (k, wi) in K.iter().zip(w.iter()) {
        let s1 = v[4].rotate_right(6) ^ v[4].rotate_right(11) ^ v[4].rotate_right(25);
        let ch = (v[4] & v[5]) ^ ((!v[4]) & v[6]);
        let temp1 = v[7]
            .wrapping_add(s1)
            .wrapping_add(ch)
            .wrapping_add(*k)
            .wrapping_add(*wi);
        let s0 = v[0].rotate_right(2) ^ v[0].rotate_right(13) ^ v[0].rotate_right(22);
        let maj = (v[0] & v[1]) ^ (v[0] & v[2]) ^ (v[1] & v[2]);
        let temp2 = s0.wrapping_add(maj);

        // h = g; g = f; f = e; e = d + temp1; d = c; c = b; b = a; a = temp1 + temp2
        v[7] = v[6];
        v[6] = v[5];
        v[5] = v[4];
        v[4] = v[3].wrapping_add(temp1);
        v[3] = v[2];
        v[2] = v[1];
        v[1] = v[0];
        v[0] = temp1.wrapping_add(temp2);
    }

    for (slot, value) in state.iter_mut().zip(v) {
        *slot = slot.wrapping_add(value);
    }
}

/// The SHA-256 digest of `bytes`, in one call.
#[must_use]
pub fn sha256(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hasher.finish()
}

/// Lowercase hexadecimal rendering of a digest — the form a key and a reference
/// carry.
#[must_use]
pub fn to_hex(digest: &[u8]) -> String {
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        // Both nibbles are < 16, so `from_digit` cannot fail; the fallback keeps
        // the function total rather than panicking on an impossible branch.
        out.push(char::from_digit(u32::from(byte >> 4), 16).unwrap_or('?'));
        out.push(char::from_digit(u32::from(byte & 0x0f), 16).unwrap_or('?'));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{Sha256, sha256, to_hex};

    /// The published FIPS 180-4 / NIST CAVP vectors. This is the check that the
    /// transcription above is the real function and not a plausible-looking
    /// variant — the reason an in-tree hash is defensible at all.
    #[test]
    fn matches_the_published_vectors() {
        for (message, expected) in [
            (
                &b""[..],
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            ),
            (
                &b"abc"[..],
                "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
            ),
            (
                &b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"[..],
                "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1",
            ),
            (
                &b"abcdefghbcdefghicdefghijdefghijkefghijklfghijklmghijklmnhijklmnoijklmnopjklmnopqklmnopqrlmnopqrsmnopqrstnopqrstu"[..],
                "cf5b16a778af8380036ce59e7b0492370b249b11e8f07a51afac45037afee9d1",
            ),
        ] {
            assert_eq!(to_hex(&sha256(message)), expected, "vector for {message:?}");
        }
    }

    #[test]
    fn the_million_a_vector_holds_across_block_boundaries() {
        let mut hasher = Sha256::new();
        // Fed in irregular chunks so the streaming buffer's block boundaries are
        // exercised rather than aligned away.
        let chunk = vec![b'a'; 997];
        let mut written = 0usize;
        while written + chunk.len() <= 1_000_000 {
            hasher.update(&chunk);
            written += chunk.len();
        }
        hasher.update(&vec![b'a'; 1_000_000 - written]);
        assert_eq!(
            to_hex(&hasher.finish()),
            "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0"
        );
    }

    #[test]
    fn streaming_in_pieces_equals_hashing_at_once() {
        let message: Vec<u8> = (0..500u32)
            .map(|i| u8::try_from(i % 251).unwrap_or(0))
            .collect();
        let at_once = sha256(&message);
        let mut hasher = Sha256::new();
        for piece in message.chunks(7) {
            hasher.update(piece);
        }
        assert_eq!(hasher.finish(), at_once);
    }

    #[test]
    fn distinct_messages_hash_distinctly() {
        assert_ne!(sha256(b"value A"), sha256(b"value B"));
        assert_ne!(sha256(b"ab"), sha256(b"ba"));
        assert_ne!(sha256(b""), sha256(b"\0"));
    }
}

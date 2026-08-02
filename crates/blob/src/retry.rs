//! The **bounded retry** a backend applies to a transient failure, and the sleep
//! port that keeps it testable.
//!
//! # Why the shape is reproduced here rather than imported
//!
//! The engine already has a backoff — `dagr_core::execution::Backoff`, the one a
//! node's retry policy is expressed in — and stacking a *second*, differently
//! shaped policy underneath it would make a run's observed timing the product of
//! two unrelated curves. So this is deliberately **the same shape**:
//! `base · factor^n`, clamped to `cap`, with `n` the zero-based index of the
//! failed attempt.
//!
//! It cannot be the same *type*, because `dagr-blob` keeps no dependency edge
//! onto `dagr-core` — that boundary is the reason this crate can exist with an
//! empty dependency table at all. The shape is therefore reproduced and **pinned
//! by a parity test in `dagr-cli`**, the one crate where both types are visible
//! (`crates/cli/tests/s3_backend_and_blob_gc.rs`), across a matrix of parameters
//! and attempt indices. A drift in either curve fails that test.
//!
//! There is no jitter here, and that is a choice rather than an omission: the
//! engine applies jitter to *node* retries, where many nodes retry against the
//! same downstream at once. A blob operation's retry is already inside one
//! attempt whose start time the engine jittered, so a second draw would only make
//! the bound harder to reason about.

use std::fmt;
use std::time::Duration;

/// How many times a transient blob operation is attempted, and how long the
/// waits between attempts are.
///
/// `attempts` counts **total attempts**, not retries: a budget of 1 tries once
/// and never waits. It is clamped to at least 1, because a store that refuses to
/// try at all is not a store.
#[derive(Debug, Clone, Copy)]
pub struct RetryBudget {
    attempts: u32,
    base: Duration,
    factor: f64,
    cap: Duration,
}

impl RetryBudget {
    /// A budget of `attempts` total tries with delays `base · factor^n` clamped
    /// to `cap`.
    #[must_use]
    pub fn new(attempts: u32, base: Duration, factor: f64, cap: Duration) -> Self {
        Self {
            attempts: attempts.max(1),
            base,
            factor,
            cap,
        }
    }

    /// The total number of attempts (always at least 1).
    #[must_use]
    pub fn attempts(&self) -> u32 {
        self.attempts
    }

    /// The first delay.
    #[must_use]
    pub fn base(&self) -> Duration {
        self.base
    }

    /// The growth factor.
    #[must_use]
    pub fn factor(&self) -> f64 {
        self.factor
    }

    /// The ceiling every delay is clamped to.
    #[must_use]
    pub fn cap(&self) -> Duration {
        self.cap
    }

    /// The delay after the `n`-th failed attempt (zero-based): `base · factor^n`,
    /// clamped to [`cap`](RetryBudget::cap).
    ///
    /// A non-finite or overflowing product resolves to `cap` rather than
    /// panicking or wrapping — the same total behaviour the engine's backoff has,
    /// for the same reason: a delay computation is not a place to fail a run.
    #[must_use]
    pub fn nominal_delay(&self, n: u32) -> Duration {
        let scaled = self.base.as_secs_f64() * self.factor.powi(i32::try_from(n).unwrap_or(i32::MAX));
        if !scaled.is_finite() || scaled < 0.0 {
            return self.cap;
        }
        let delay = Duration::try_from_secs_f64(scaled).unwrap_or(self.cap);
        delay.min(self.cap)
    }
}

impl Default for RetryBudget {
    /// Four attempts, 100 ms doubling, capped at 5 s — long enough to ride out an
    /// object store's usual transient blip, short enough that a genuinely down
    /// store surfaces inside a node attempt rather than consuming it.
    fn default() -> Self {
        Self::new(4, Duration::from_millis(100), 2.0, Duration::from_secs(5))
    }
}

/// How a backend waits between attempts.
///
/// It is a port rather than a direct `std::thread::sleep` for one reason: a test
/// of the retry *schedule* must assert the delays rather than spend them. The
/// shipped implementation is [`ThreadSleeper`].
pub trait Sleeper: fmt::Debug + Send + Sync {
    /// Block the calling thread for `delay`.
    fn sleep(&self, delay: Duration);
}

/// The shipped sleeper: blocks the calling thread.
///
/// Blob operations are blocking by construction (the port is synchronous, and the
/// local backend already pays two `fsync`s on the caller's thread), so a caller on
/// an async worker treats a retrying blob call the way it treats the scratch
/// store.
#[derive(Debug, Clone, Copy, Default)]
pub struct ThreadSleeper;

impl Sleeper for ThreadSleeper {
    fn sleep(&self, delay: Duration) {
        if !delay.is_zero() {
            std::thread::sleep(delay);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{RetryBudget, Sleeper, ThreadSleeper};
    use std::time::Duration;

    #[test]
    fn the_delay_curve_grows_and_clamps() {
        let budget = RetryBudget::new(4, Duration::from_millis(100), 2.0, Duration::from_secs(1));
        assert_eq!(budget.nominal_delay(0), Duration::from_millis(100));
        assert_eq!(budget.nominal_delay(3), Duration::from_millis(800));
        assert_eq!(budget.nominal_delay(4), Duration::from_secs(1));
        assert_eq!(budget.nominal_delay(u32::MAX), Duration::from_secs(1));
    }

    #[test]
    fn a_zero_attempt_budget_is_clamped_to_one() {
        assert_eq!(RetryBudget::new(0, Duration::ZERO, 2.0, Duration::ZERO).attempts(), 1);
    }

    #[test]
    fn a_zero_delay_sleep_does_not_block() {
        ThreadSleeper.sleep(Duration::ZERO);
    }
}

//! The task-facing error type — C1's classified failure surface.
//!
//! [`TaskError`] is the entire error vocabulary a pipeline author may *return*
//! from a task's work. It is **three-valued, permanently**: retry-eligible
//! failure, permanent failure, and deliberate (originated) skip. This is fixed
//! by the T3 error-taxonomy ADR
//! ([`docs/implementation/016-T3-error-taxonomy-adr.md`](https://github.com/athvin/dagr/blob/main/docs/implementation/016-T3-error-taxonomy-adr.md))
//! and arch.md `### C1 · Task`: *"The error a task returns distinguishes at
//! minimum: retry-eligible failure, permanent failure, and deliberate skip."*
//!
//! # What is deliberately absent
//!
//! Two runner classifications are **not** author-returnable and carry no
//! constructor here:
//!
//! - **timeout** is decided by the per-attempt clock (C14), never by the task
//!   body — an author who returns has not timed out, and one who timed out never
//!   returns to report it.
//! - **panic** is precisely the failure that *escaped* the author's `Result`; it
//!   is caught at the framework's boundary (C14), never returned.
//!
//! The framework-internal runner outcome taxonomy (a strict superset that adds
//! timeout and panic, mapping each outcome to a terminal state) belongs to the
//! attempt runner (C14 / T20), **not** to this author-facing surface. Keeping
//! the two types distinct is what makes the superset boundary a type-level fact
//! rather than a convention (T3 ADR §11).

use std::error::Error;
use std::fmt;

/// The classification a [`TaskError`] carries: one of exactly three classes.
///
/// This mirrors the task-facing enum fixed by the T3 ADR and never grows a
/// fourth author-returnable class (a new class would be a spec amendment, never
/// a runtime knob). `Timeout` and `Panic` are deliberately absent — they are the
/// runner's to mint, not the author's to return.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TaskErrorClass {
    /// A transient failure the framework may retry (an I/O blip, a rate limit, a
    /// contended lock). Author intent: *"try me again."* Whether another attempt
    /// is actually scheduled is the runner's decision, governed by the node's
    /// retry budget (C14); once the budget is exhausted this resolves to the same
    /// `failed` terminal state as a permanent failure (T3 ADR §6).
    Retryable,
    /// A failure that retrying cannot fix (bad input, a violated invariant, a
    /// missing prerequisite). Author intent: *"do not retry me."*
    Permanent,
    /// The task decided there is nothing to do — an *originated* skip. Branching
    /// is expressed in the task, not the graph (arch.md Vocabulary); the skip
    /// propagates downstream as `upstream-skipped` (C15). Author intent: *"I am
    /// declining to run."*
    Skip,
}

/// The error a task's work returns instead of its output.
///
/// A `TaskError` is a [`TaskErrorClass`] plus a human-readable message and an
/// optional underlying cause (preserved through [`Error::source`], so a failing
/// attempt's structured error detail — recorded later in the run artifact by
/// C22 — retains the original chain). Construct one with [`TaskError::retryable`],
/// [`TaskError::permanent`], or [`TaskError::skip`]; attach a source with the
/// `*_from` constructors.
///
/// The class is inspected with [`TaskError::class`] or the `is_*` predicates.
/// There is no `Timeout` or `Panic` constructor: those are runner classifications
/// (C14), not author returns (see the [module docs](self)).
#[derive(Debug)]
pub struct TaskError {
    class: TaskErrorClass,
    message: String,
    source: Option<Box<dyn Error + Send + Sync + 'static>>,
}

impl TaskError {
    /// Construct an error of the given class with a message and no source.
    #[must_use]
    fn new(class: TaskErrorClass, message: impl Into<String>) -> Self {
        Self {
            class,
            message: message.into(),
            source: None,
        }
    }

    /// Construct an error of the given class with a message and an underlying
    /// cause, preserved through [`Error::source`].
    #[must_use]
    fn with_source(
        class: TaskErrorClass,
        message: impl Into<String>,
        source: impl Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            class,
            message: message.into(),
            source: Some(Box::new(source)),
        }
    }

    /// A **retry-eligible** failure — a transient error the framework may retry
    /// (subject to the node's retry budget, C14). Author intent: *"try me
    /// again."*
    #[must_use]
    pub fn retryable(message: impl Into<String>) -> Self {
        Self::new(TaskErrorClass::Retryable, message)
    }

    /// A **retry-eligible** failure carrying an underlying cause, preserved
    /// through [`Error::source`].
    #[must_use]
    pub fn retryable_from(
        message: impl Into<String>,
        source: impl Error + Send + Sync + 'static,
    ) -> Self {
        Self::with_source(TaskErrorClass::Retryable, message, source)
    }

    /// A **permanent** failure — retrying cannot fix it (bad input, a violated
    /// invariant). Author intent: *"do not retry me."*
    #[must_use]
    pub fn permanent(message: impl Into<String>) -> Self {
        Self::new(TaskErrorClass::Permanent, message)
    }

    /// A **permanent** failure carrying an underlying cause, preserved through
    /// [`Error::source`].
    #[must_use]
    pub fn permanent_from(
        message: impl Into<String>,
        source: impl Error + Send + Sync + 'static,
    ) -> Self {
        Self::with_source(TaskErrorClass::Permanent, message, source)
    }

    /// A **deliberate skip** — the task decided there is nothing to do (an
    /// *originated* skip). Author intent: *"I am declining to run."*
    #[must_use]
    pub fn skip(message: impl Into<String>) -> Self {
        Self::new(TaskErrorClass::Skip, message)
    }

    /// The classification this error carries.
    #[must_use]
    pub fn class(&self) -> TaskErrorClass {
        self.class
    }

    /// The human-readable message this error carries.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Whether this error is [retry-eligible](TaskErrorClass::Retryable).
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        self.class == TaskErrorClass::Retryable
    }

    /// Whether this error is a [permanent](TaskErrorClass::Permanent) failure.
    #[must_use]
    pub fn is_permanent(&self) -> bool {
        self.class == TaskErrorClass::Permanent
    }

    /// Whether this error is a [deliberate skip](TaskErrorClass::Skip).
    #[must_use]
    pub fn is_skip(&self) -> bool {
        self.class == TaskErrorClass::Skip
    }
}

impl fmt::Display for TaskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self.class {
            TaskErrorClass::Retryable => "retryable",
            TaskErrorClass::Permanent => "permanent",
            TaskErrorClass::Skip => "skip",
        };
        write!(f, "{label}: {}", self.message)
    }
}

impl Error for TaskError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_ref()
            .map(|boxed| &**boxed as &(dyn Error + 'static))
    }
}

//! `dagr-core` — the dagr execution core (placeholder skeleton).
//!
//! This crate will hold dagr's authoring surface and execution core: the task
//! abstraction, typed handles, dependency binding, flow assembly, and the
//! run-loop machinery — the code that *is* a running pipeline. It is the crate
//! whose dependency set is kept minimal and review-gated (arch.md
//! "Stability"), and it is the "live pipeline" surface that renderers must
//! never reach into (arch.md C24 · Renderers).
//!
//! At this milestone the crate is an empty, compiling placeholder created by
//! ticket T1 (crate layout and workspace skeleton). No domain logic, types, or
//! APIs live here yet; they land in later tickets (T9, T10, T11, T13, T14, and
//! the M1+ execution tickets).
//!
//! Lint posture is inherited from `[workspace.lints]` (`unsafe_code = "warn"`
//! per `docs/lint-policy.md`); this crate adds no crate-level lint attributes.

#[cfg(test)]
mod tests {
    /// Placeholder test proving the crate is compiled and in the workspace
    /// build graph (T1 Test plan: "every member crate is discoverable and
    /// testable"). Real tests arrive with the domain logic in later tickets.
    #[test]
    fn crate_is_in_the_build_graph() {}
}

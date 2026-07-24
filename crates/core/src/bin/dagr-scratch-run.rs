//! `dagr-scratch-run` — a **test-support** harness for T54a (ticket 066): the
//! scratch-survives-restart proof.
//!
//! # Why this exists
//!
//! T54a proves C18's durability half — that a node's scratch, written through the
//! run store on disk, **survives a full process exit** and is readable by a
//! **later, separate process** (arch.md `### C18 · Durable scratch store`; "The
//! shape of a run" line 67; T0.6 §8, §9). The honest way to test that is to have a
//! **real, separate OS process** write the scratch and then **exit**, and a
//! *different* process read it back afterward — not an in-process handle round
//! trip (that is already T53's `scratch_store.rs`). This binary is that writing
//! process. It mirrors T68's `dagr-crashy-run` rationale: checked-in, reusable
//! test scaffolding, resolved by the integration test via
//! `CARGO_BIN_EXE_dagr-scratch-run`, shipping in **no released binary**.
//!
//! It links only against `dagr_core` — no new dependency — so `dagr-core` stays
//! dependency-free (arch.md "Stability").
//!
//! # What it does
//!
//! Given a run-store base, a run/node identity, a key/value, and an outcome, it:
//!   1. resolves the node's real [`ScratchStore`] under the base
//!      (`<base>/<pipeline>/<run-id>/scratch/<node>/` — the production path,
//!      identical to what a live run wires) and **writes** the value durably
//!      (the store's atomic write-temp/fsync/rename/fsync-dir discipline);
//!   2. models the node's **run-end lifecycle** by its `outcome` argument:
//!      - `succeed` → the node reached terminal **success**, so its **on-success
//!        hook** (`remove_on_success`) runs — the only per-node deletion at run
//!        end;
//!      - `fail` → the node ended **non-succeeded**, so **no cleanup runs**: run
//!        end deletes nothing of its scratch, exactly the amended C18 rule
//!        ("nothing is deleted implicitly at run end", arch.md line 393; T0.6 §8);
//!   3. writes an on-disk `ready` marker (atomically, write+rename) so the parent
//!      test can synchronise on the scratch work being durably complete **without
//!      a wall-clock sleep**, then **exits `0`**.
//!
//! After exit, nothing shares this process's address space, so a value the parent
//! then reads back can only have come from the run-store medium — which is the
//! whole point of the proof.
//!
//! # Determinism contract (no fixed sleeps)
//!
//! The harness synchronises with its parent through **observable on-disk state**:
//! it writes the `ready` marker only **after** the scratch work (write, and any
//! success-hook deletion) has returned, so the marker's appearance is a
//! sufficient signal that the on-disk state the parent will read is final. The
//! parent additionally reaps this process, so the read happens strictly after the
//! writer is gone. No sleeps, no races.
//!
//! # Usage
//!
//! ```text
//! dagr-scratch-run <base> <pipeline> <run-id> <node> <key> <value> <outcome> <ready-marker>
//! ```
//! `outcome` is `succeed` (run the on-success deletion) or `fail` (retain — run
//! end deletes nothing). Exit codes: `0` on success, `2` on a usage/argument
//! error, `3` if a scratch operation itself failed (a real durability fault the
//! parent should surface, not mask).

use std::path::Path;
use std::process::ExitCode;

use dagr_core::context::{PipelineId, RunId};
use dagr_core::handle::NodeId;
use dagr_core::scratch::ScratchStore;

/// Write the on-disk `ready` marker atomically (write to a temp path, then rename)
/// so the parent observes a complete file the instant it appears — never a
/// half-written marker.
fn signal_ready(marker: &Path) -> std::io::Result<()> {
    let tmp = marker.with_extension("readytmp");
    std::fs::write(&tmp, b"ready")?;
    std::fs::rename(&tmp, marker)
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let [_, base, pipeline, run, node, key, value, outcome, marker] = args.as_slice() else {
        eprintln!(
            "usage: dagr-scratch-run <base> <pipeline> <run-id> <node> <key> <value> <outcome> <ready-marker>"
        );
        return ExitCode::from(2);
    };

    // Resolve the node's real scratch store under the base — the exact production
    // path a live run wires (`<base>/<pipeline>/<run-id>/scratch/<node>/`).
    let store = ScratchStore::for_node(
        Path::new(base),
        &PipelineId::new(pipeline.as_str()),
        &RunId::new(run.as_str()),
        NodeId::from_name(node),
    );

    // 1. Write the value durably through the store's atomic discipline.
    if let Err(e) = store.put(key.as_bytes(), value.as_bytes()) {
        eprintln!("scratch write failed: {e}");
        return ExitCode::from(3);
    }

    // 2. Model the node's run-end lifecycle by its terminal outcome.
    match outcome.as_str() {
        // Terminal SUCCESS: the on-success hook deletes this node's scratch — the
        // only per-node deletion at run end.
        "succeed" => {
            if let Err(e) = store.remove_on_success() {
                eprintln!("on-success deletion failed: {e}");
                return ExitCode::from(3);
            }
        }
        // Non-succeeded terminal: NO cleanup runs. Run end deletes nothing of this
        // node's scratch — it is retained on disk for a later resume / prune.
        "fail" => {}
        other => {
            eprintln!("unknown outcome `{other}` (expected `succeed` or `fail`)");
            return ExitCode::from(2);
        }
    }

    // 3. Signal readiness only after the scratch work is durably complete, so the
    // parent synchronises on observable on-disk state (no sleep), then exit.
    if let Err(e) = signal_ready(Path::new(marker)) {
        eprintln!("could not write ready marker: {e}");
        return ExitCode::from(3);
    }
    ExitCode::SUCCESS
}

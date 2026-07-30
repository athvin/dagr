# The pre-M9 behavioural baseline

`reference.snapshot.txt` is the observable behaviour of dagr **before M9**, and
the M9 acceptance gate (ticket 114 · T99,
`crates/cli/tests/m9_acceptance_gate.rs`) requires the current head to reproduce
it byte for byte.

    pre-m9-commit = 5f87d1143d28e4ce4acbcb313e925fa8ddd13627
    probe         = crates/cli/examples/m9_baseline_capture.rs
    probe-fnv1a64 = 3180b20f849d31e2
    captured      = 2026-07-30

`5f87d11` is `docs: mark 106 T91 done` — the last commit on `main` before any M9
work, one commit before `9248e3e` added the M9 ticket files and three before the
first M9 code landed (`9c0990b`, ticket 107 · T92). So the tree the baseline was
taken from contains **none** of T92–T98: no `[profile.*]`, edition 2021, the
pre-T95 error surface, the pre-T96 metadata and lint ratchet.

## Why a git commit rather than a stashed file

The ticket asks for the baseline to be captured "from `main` **before** the M9
branches land". By the time the gate ships they have all landed — but git holds
that tree exactly, which is *stronger* than a snapshot squirrelled away at the
time, because anyone can reproduce it (see below) rather than having to trust a
file. What matters is that the bytes come from a pre-M9 **engine**, and they do.

## Why the digest is the load-bearing line

The comparison is only meaningful if the *same probe program* ran against both
engines: two different programs printing different bytes would prove nothing.
`probe-fnv1a64` is the FNV-1a 64 digest of the checked-in probe's source, and
`the_baseline_fixture_records_the_pre_m9_commit_and_the_probe_it_was_captured_with`
recomputes it on every run. Edit the probe without re-capturing and the gate
fails, so the two halves cannot drift apart silently.

The probe is written against the public API as it existed at `5f87d11` and uses
no edition-2024 syntax, which is what lets one file compile in both trees.

## What is compared, and what is masked

The snapshot carries, in order: the reference pipeline's masked canonical graph
artifact; its structural fingerprint and policy hash; the overall outcome and
per-node terminal states of a scripted `load -> transform -> publish` run (with
one retryable failure) driven through the real scheduler; that run's folded run
artifact; and every record of its event stream.

The masked fields are exactly the ones that legitimately vary between two runs
of the same script — `wall`, `offset_ns`, `produced_at_offset_ns`, `run_id`,
`worker`, `metrics`, `cost_measured`, `generated_at` — plus the harness's own
normalization of the folded artifact's volatile header, phase durations, and
summary. Sequence numbers, event kinds, node identity, attempt numbers, terminal
states, propagation origins, declared costs, both fingerprints, and the whole
graph artifact are compared verbatim.

## Regenerating it

Only ever from the recorded commit — never from the current head, which would
turn the gate into a tautology:

```sh
git worktree add --detach /tmp/dagr-pre-m9 5f87d1143d28e4ce4acbcb313e925fa8ddd13627
cp crates/cli/examples/m9_baseline_capture.rs /tmp/dagr-pre-m9/crates/cli/examples/
(cd /tmp/dagr-pre-m9 && CARGO_TARGET_DIR=/tmp/dagr-pre-m9-target RUSTFLAGS= \
   cargo run --quiet -p dagr-cli --example m9_baseline_capture) \
  > crates/cli/tests/fixtures/m9-baseline/reference.snapshot.txt
git worktree remove /tmp/dagr-pre-m9
```

Then update `probe-fnv1a64` above. The pre-M9 tree pins toolchain 1.95.0 in its
own `rust-toolchain.toml`, so rustup selects it inside the worktree
automatically; `RUSTFLAGS=` clears the `-D warnings` a CI shell may be exporting,
which that older tree is not obliged to be clean under.

## If the gate fails

A divergence is a **defect**, not a re-basing prompt. M9 changed no behaviour by
design, so a difference here is something that slipped through one of T93–T98.
Investigate the failing line; do not re-capture the fixture to make it green.

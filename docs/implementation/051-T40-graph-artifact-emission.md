# 051 · T40 — C20: graph artifact emission

> **Milestone:** M3 · **Size:** M · **Type:** feature · **Components:** C20
> **Branch:** `feat/t40-graph-artifact-emission` · **Depends on:** T15, T29, T39, T0.7 · **Blocks:** T41, T46, T48, T50, T55, T61

## Why / context
This ticket makes a pipeline binary emit its own structure as a graph artifact without executing anything — the on-demand output that opens M3, "It explains itself." It is governed by C20 (Graph artifact) with the header and stable-name rules from C21 (Graph fingerprint), and it consumes the immutable assembled pipeline from C7 (already byte-identical per T15), the full effective node policy from C5/C29 (T29), the published, versioned graph-artifact schema from T39, and the stable-name trait plus fingerprint-field decisions from the T0.7 ADR. The fingerprint values themselves are computed in T41; this ticket reserves and carries the fingerprint header slot but does not define the hashing algorithm. Emission must run in a credential-free, network-free, database-free environment so it can run in CI on every pull request — that empty-environment guarantee is the reason the artifact is trustworthy.

## Objective
Make the binary emit a schema-valid graph artifact for its assembled pipeline, deterministically and offline. Concretely:
- Serialize every node the running pipeline would use: declared node name, group label, stable task name, stable declared input and output type names, execution class, complete effective policy (every C5 field, with defaulted values written out explicitly), declared resource requirements (the per-pool cost vector), and dependency lists.
- Serialize every edge, tagged with its kind (data versus ordering-only), and for data edges the stable declared name of the carried type.
- Use author-declared stable names everywhere identity or type matters; permit `std::any::type_name` output only in a clearly informational debug field, never as identity and never in any fingerprint-bound position.
- Assemble the versioned header: schema version identifier, tool version, generation time, pipeline identity, the reserved structural-fingerprint / policy-hash / algorithm-version slots (populated by T41), and build provenance — tool version, git commit SHA, and lockfile hash — embedded at build time via the build script so they are fixed per binary.
- Wire emission behind the graph verb of the CLI contract (C26) so it produces the artifact to stdout / a chosen sink, requiring no store, no parameters, no network.
- Guarantee byte-identical output across repeated emissions from one binary, with only the generation-time field excluded from that comparison, and validate every emitted artifact against the published T39 schema.

## Test plan (write these first — TDD)
- **Empty-environment emission.** Setup: a small fixture pipeline assembled in a test with no environment variables, no filesystem fixtures, no network, no credentials, and no parameters supplied. Action: invoke graph emission. Expected: it returns a complete artifact and never touches the network, a database, or credentials; the test asserts success in that stripped environment.
- **Byte-identical repeat.** Setup: one assembled fixture pipeline. Action: emit the artifact twice in the same process. Expected: the two byte streams are identical after masking only the generation-time field; every other byte matches, including header provenance and ordering.
- **Generation time is the only variance.** Setup: emit twice with a controlled clock that returns two different instants. Action: compare raw bytes. Expected: the outputs differ only within the generation-time field's span and are otherwise identical.
- **Node completeness.** Setup: a fixture pipeline whose nodes exercise a group label, a non-default policy on at least one node, and a declared resource cost vector. Action: emit and parse the artifact. Expected: every node in the assembled pipeline appears exactly once, each carrying name, group, stable task name, stable input and output type names, execution class, and dependency lists; no assembled node is missing and none is invented.
- **Full effective policy including defaults.** Setup: one node with an all-default policy (no policy stated) and one node with several fields overridden. Action: emit and inspect both nodes' policy blocks. Expected: the defaulted node shows every C5 field written out at its documented default value (retries, backoff, timeout, cost vector, trigger rule, execution class, group, retention, durability); the overridden node shows the overridden values; neither omits a field.
- **Declared resource requirements present.** Setup: a node declaring a non-zero cost vector with distinct working-memory, output-residency, and thread entries. Action: emit and read that node's declared resource requirements. Expected: every pool entry appears in its native unit and matches what was declared, so bootstrap and the run artifact could juxtapose it against measured cost.
- **Edge kinds and carried types.** Setup: a fixture pipeline containing both a data dependency (a handle-wired edge carrying a typed payload) and an ordering-only dependency. Action: emit and inspect the edge set. Expected: the data edge is tagged as data and records the stable declared name of the carried payload type; the ordering-only edge is tagged as ordering and carries no type name; every edge the runtime would use is present.
- **Stable declared names, never type_name as identity.** Setup: a task and payload type whose declared stable name differs from its Rust path / `type_name` output. Action: emit and inspect. Expected: recorded task and type names are the author-declared stable names; the raw `type_name` string, if present at all, appears only in the informational debug field and nowhere used as identity or in any fingerprint-bound slot.
- **Build-provenance header embedded at build time.** Setup: build the fixture binary through the configured build script. Action: emit and read the header. Expected: the header carries a schema version identifier, tool version, pipeline identity, the reserved fingerprint / algorithm-version slots, and build provenance (tool version, git commit SHA, lockfile hash) whose values are fixed for that binary and identical across repeated emissions from it.
- **Schema validation.** Setup: an emitted artifact and the published T39 graph-artifact schema plus its validation helper. Action: validate the artifact against the schema. Expected: it validates cleanly; a deliberately corrupted copy (a required field removed) fails validation, proving the check has teeth.
- **Two in-process assemblies agree (interlock with T15).** Setup: assemble the same pipeline definition twice in one process and emit each. Action: compare the two artifacts. Expected: byte-identical outside the generation-time field, confirming emission adds no nondeterminism on top of assembly.

## Definition of done
- [ ] The graph artifact can be produced in an empty environment with no configuration, credentials, network, or database present, and a test proves emission succeeds under those conditions (C20).
- [ ] Emitting twice from the same binary produces identical bytes outside the generation-time field, verified by test (C20).
- [ ] Every node the running pipeline would use appears in the artifact with name, group, stable task name, stable declared input and output type names, execution class, complete effective policy, declared resource requirements, and dependency lists (C20).
- [ ] Every edge appears, tagged data-versus-ordering, and each data edge records the stable declared name of its carried type (C20).
- [ ] Recorded type and task names are the author-declared stable names; `std::any::type_name` appears only as an informational debug field and never as identity or in any fingerprint-bound position (C20, C21).
- [ ] Every node's full effective policy — including defaulted values written out explicitly — is present in the artifact, and an all-default node's policy block equals the every-default-written-out form (C5, C20).
- [ ] The header carries schema version identifier, tool version, generation time, pipeline identity, the reserved structural-fingerprint / policy-hash / algorithm-version slots, and build provenance (tool version, git commit SHA, lockfile hash) embedded at build time; everything but generation time is fixed per binary (C20, C21).
- [ ] Generation time is excluded from all byte-identity comparisons and is the only field allowed to vary across emissions from one binary (C20).
- [ ] The emitted artifact validates against the published T39 graph-artifact schema, exercised via the schema validation helper, and a corrupted artifact is rejected (C20).
- [ ] Emission is reachable through the graph verb of the CLI contract and runs without opening a run store or reading parameters (C26 graph verb, C7 no-parameters-during-assembly).
- [ ] Build provenance is embedded through a build script (tool version, git SHA, lockfile hash) so the supply-chain provenance commitment holds for the artifact (Stability · Supply chain, C20).
- [ ] Rustdoc on every new public item, including where the stable-name-versus-`type_name` distinction is documented at the point of use.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None.

## Out of scope
- Computing the structural fingerprint and policy hash, canonical ordering for hashing, the algorithm version value, and the change/no-change matrix — those are T41; this ticket only reserves and carries their header slots.
- The diagram renderer and any visual clustering by group — T46 consumes this artifact.
- Artifact-validation and cross-version compatibility CI wiring beyond validating this artifact against its own published schema — T48.
- Ordering-dependency mechanics beyond serializing the edge kind — the ordering-edge semantics are T50 (this ticket serializes whatever the assembled graph already carries).
- Structure snapshot testing of emitted artifacts — T61.
- The run artifact, event-stream folding, node metrics, and any execution or runtime data — this ticket describes structure only, never an outcome.
- Any drift toward runtime-mutable graph shape, a metadata store, or a query surface over emitted artifacts: the artifact is a fixed-per-binary emission, and inspecting many of them is concatenation outside this tool, not a dagr feature.

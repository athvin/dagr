# 056 · T45 — C25: logging and tracing integration

> **Milestone:** M3 · **Size:** M · **Type:** feature · **Components:** C25
> **Branch:** `feat/t45-logging-and-tracing-integration` · **Depends on:** T20, T30 · **Blocks:** T49

## Why / context
A failed run must be debuggable from logs alone, without correlating wall-clock timestamps across interleaved concurrent nodes. This ticket wires the framework's tracing subscriber so that every attempt executes inside a span carrying run, node, and attempt identity, and so lines emitted beneath it — including from third-party libraries the task calls — are attributable to that node. It builds on the single-attempt execution core (T20 / C14), which already opens a per-attempt span for its own lifecycle events, and on the resource registry (T30 / C9), whose secret wrapper defines the redaction boundary this ticket must honour. It is governed by arch.md §C25, with the secret-boundary rule cross-referenced from §C9 and the attempt-span lifecycle from §C14. Its output is consumed by the M3 demo (T49).

## Objective
Make attempt-scoped logging attributable, mode-switchable, and secret-safe on framework-controlled output paths.

- Establish exactly one process-global tracing subscriber, installed once at bootstrap by the library (coexisting with the test harness and the C14 panic hook), that formats all log/trace output for the run.
- Ensure every attempt runs inside a span whose recorded fields include run identity, node identity, and attempt number, so any line — framework-emitted or third-party — carries that context. Reuse or attach to the per-attempt span the C14 attempt runner already opens rather than introducing a competing span.
- Support two output modes over the same event data: a structured (machine-queryable) mode as the default, and a human-readable mode for local development. Selection is driven by an environment variable in M3 (a library-owned CLI flag is deferred to M4 per C26); switching modes requires no code change and no recompile.
- Guarantee that marked secret values from the C9 registry never appear on any framework-emitted output path (log lines, span fields, formatted diagnostics the framework controls), and document that a task author who formats a secret into their own log line is outside this guarantee.
- Verify the secret boundary with a sentinel-based test that plants a known value into the registry as a secret and asserts it never surfaces on framework output paths.

## Test plan (write these first — TDD)

- **Attribution without timestamp correlation.** Set up a small flow whose task emits a log line at a chosen level and also calls a stub "third-party" library that emits its own line. Run the flow with output captured. Expect: both captured lines carry the node identity and attempt number of the emitting attempt, so a reader can attribute each line to its node and attempt without inspecting or ordering timestamps.

- **Third-party lines inherit the attempt span.** Set up a task that, inside its body, invokes a helper emitting a log line through the global logging facade but with no dagr-specific context of its own. Run one attempt. Expect: the captured line is annotated with the current run, node, and attempt fields, proving the line was emitted beneath the attempt span rather than at the root.

- **Concurrent nodes are unambiguously separable.** Set up a flow with two nodes that run concurrently, each emitting several interleaved log lines. Run with a bounded concurrency that forces overlap. Expect: every captured line is attributable to exactly one of the two nodes via its span fields, and no line is ambiguous even though lines from the two nodes interleave in emission order.

- **Retry attempts are distinguishable.** Set up a task that fails retry-eligibly on its first attempt and succeeds on its second, emitting a line on each attempt. Run it. Expect: the two captured lines carry the same node identity but different attempt numbers, so an operator can tell first-attempt output from retry output.

- **Structured mode is the default and is machine-parseable.** Set up a flow with no mode environment variable set. Run it and capture framework output. Expect: each emitted record parses as a structured record exposing the run, node, and attempt fields as discrete queryable fields (not only as free text).

- **Human-readable mode via environment, no code change.** Take the exact same compiled binary and flow from the previous scenario. Set the mode environment variable to request human-readable output and run again. Expect: output is in the human-readable format; the switch required only the environment variable and no recompilation or source change. Assert that an unset or unrecognized value falls back to the documented default (structured) deterministically.

- **Secret redaction on framework paths (sentinel).** Set up a registry (C9) containing a resource marked as secret whose value is a unique sentinel string unlikely to occur by accident. Run a flow whose framework lifecycle produces log lines and span output. Capture all framework-emitted output for the whole run. Expect: the sentinel never appears anywhere on framework-emitted output paths (log lines, span fields, framework-formatted diagnostics), in either output mode.

- **Task-authored leak is out of guarantee, and documented.** Set up the same secret sentinel, then a task that deliberately formats the secret's revealed value into its own log line. Run it. Expect: the sentinel does appear (the framework does not intercept task-authored content), confirming the boundary is exactly where the spec draws it; and a rustdoc/doc test asserts the public documentation states this boundary explicitly.

- **Single subscriber, coexists with test harness.** Run the whole suite. Expect: installing the subscriber does not panic or error when the test harness's own subscriber/hook is present, is installed at most once per process, and does not double-install across multiple runs in the same process.

## Definition of done

- [ ] Every attempt executes inside a span whose fields carry run identity, node identity, and attempt number; any log line produced during an attempt — including from third-party libraries the task calls — is traceable to its node and attempt without timestamp correlation.
- [ ] The framework installs exactly one process-global tracing subscriber at bootstrap, installed once, coexisting with the test harness and the C14 panic hook, and attaches to (does not duplicate) the C14 per-attempt span.
- [ ] Output is structured by default and machine-queryable, exposing run/node/attempt as discrete fields.
- [ ] A human-readable output mode exists for local development, and switching between structured and human-readable output requires no code change — driven by an environment variable in M3 (CLI flag deferred to M4 per C26); an unset/unrecognized value falls back deterministically to the documented default.
- [ ] Lines from concurrently executing nodes are unambiguously separable by their span fields.
- [ ] Marked secret values from the C9 resource registry never appear on any framework-emitted output path, verified by a test that plants a sentinel value, in both output modes.
- [ ] The documentation states explicitly that a task author who formats a secret into their own log line is outside the redaction guarantee.
- [ ] The mode-selection environment variable name and default, and the redaction boundary, are documented in rustdoc on the public logging surface.
- [ ] CI is green on the ticket branch (fmt, clippy with warnings denied, tests, rustdoc lint, and cargo-audit/deny where configured).

## Open questions
None.

## Out of scope
- The M4 library-owned CLI flag for output-mode selection (C26): M3 ships environment-variable selection only.
- Live-telemetry shipping, push-export of metrics, or any external log/trace sink integration — that is a stream-tailing task/resource concern per C19, not a framework feature.
- Redacting or sanitizing task-authored log content: the framework guarantee covers only framework-controlled output paths; task-authored lines are explicitly outside it.
- The event-stream artifact format and its records (C19) and the run/graph artifacts and renderers (C20, C22, C24): this ticket concerns human/operator log-and-trace output, not the durable artifact trail.
- Changing span identity semantics, node metrics (C23), or any runtime graph-shape mutation — the graph shape is fixed at assembly and this ticket does not touch it.

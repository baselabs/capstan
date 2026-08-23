# Architecture decision records

The permanent, public record of capstan's load-bearing design decisions. Each ADR states the
context, the decision, and the consequences — including what was deliberately refused.

| ADR | Decision |
| --- | --- |
| [0001](0001-position-and-dedup-model.md) | Position and dedup model — GTID set as the sole position; dedup by set membership, never ordinal comparison |
| [0002](0002-fail-closed-server-preconditions.md) | Fail-closed server preconditions — the connect-time gate on binlog/GTID configuration |
| [0003](0003-transaction-shape-and-checkpoint-semantics.md) | Transaction shape and checkpoint semantics — terminators, XA refusal, the processed watermark |
| [0004](0004-c1-scope-lib-owned-checkpoint-only.md) | C1 scope — lib-owned checkpoint mode only; sink-owned mode and explicit start positions deferred |
| [0005](0005-initial-snapshot-cursor-gate-brief-lock.md) | Initial snapshot — cursor-gated suppression + a brief per-chunk lock capturing an exact GTID position |
| [0006](0006-xa-prepare-commit-tracking.md) | XA prepare/commit tracking via a held-out watermark — supersedes 0003 §2 for opt-in `xa: :track` (implementation pending) |
| [0007](0007-value-free-boundary-rule-1.md) | The value-free boundary (Rule 1) — the four leak vectors, the metadata-key and measurement-value gates, Inspect elision, test-time sentinel sweeps |
| [0008](0008-pure-elixir-protocol-client.md) | Pure Elixir, in-library protocol client — no NIF, no protocol dependency; probe-first ownership of the wire surface |
| [0009](0009-fail-closed-supervision-and-streaming-liveness.md) | Fail-closed supervision and streaming liveness — `:temporary` children, halt-is-not-a-crash, heartbeat + epoch-guarded timeout, the two budgets |
| [0010](0010-exclusive-end-bound-of-com-binlog-dump-gtid.md) | The exclusive end bound of `COM_BINLOG_DUMP_GTID` — `high + 1` on the wire, probe-proven, tripwired live |
| [0011](0011-transaction-compression-precondition.md) | Binary-log transaction compression is a precondition — source-unilateral and unconsumable in posture; refused at the gate, decoder halt kept as the dynamic-flip backstop |

A note on provenance markers: "Design refs" (`Q1`, `Q5`, `F6`, …) cite the numbered questions
and findings of the pre-implementation design review, and "Task n" cites rows of the
implementation plan. Those records are machine-local working artifacts and are not shipped in
this repository; the ADRs are self-contained without them.

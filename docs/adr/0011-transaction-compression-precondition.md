# ADR-0011 — Binary-log transaction compression is a precondition, not a stream-time surprise

**Status:** Accepted (2026-08-23) · **Extends:** [ADR-0002](0002-fail-closed-server-preconditions.md) (the gate gains a sixth variable) · **Related:** [ADR-0008](0008-pure-elixir-protocol-client.md) (the no-NIF posture that rules out a zstd dependency)

## Context

MySQL 8.0.20+ can compress row-based transactions into a single zstd-compressed
`Transaction_payload_event` per transaction. Primary-doc facts (MySQL 8.0 reference manual,
"Binary Log Transaction Compression", read first-hand 2026-08-23):

- `binlog_transaction_compression` is **source-unilateral**: a replica or downstream consumer
  **cannot opt out or negotiate uncompressed delivery** — replicas receive compressed payloads
  and must inflate them. The variable is dynamic, not replicated, and per-session settings can
  produce a MIX of compressed and uncompressed payloads in one binlog.
- Excluded from compression: GTID/control/heartbeat events, incident events (and their whole
  transactions), non-transactional events (and their whole transactions), statement-based
  events — so a compression-enabled source still delivers some events bare.

capstan cannot inflate zstd payloads in posture: ADR-0008 bans NIFs, and a pure-Elixir zstd
decoder (FSE + Huffman entropy decoding) is a large, high-blast-radius implementation with no
current partner need. Until that changes, a compression-enabled source is **unconsumable**.
Before this ADR, such a pipeline STARTED, streamed the bare events, and halted
`:compressed_payload_unsupported` at the FIRST compressed transaction — loud, but late: it
surfaced only after the consumer had wired a checkpoint store and begun streaming.

## Decision

`Capstan.Config.check_preconditions/1` gains a sixth variable: `binlog_transaction_compression` must
be OFF (`"0"` as text) or the connection refuses with the distinct, actionable reason
`:binlog_transaction_compression_on` — at connect, before the dump, alongside the other
gate variables (ADR-0002's posture: distinct reason per violation, one query, text-compared).

The decoder's loud halt on a `TRANSACTION_PAYLOAD` event REMAINS, deliberately: the gate reads
the variable once per connect, and compression is dynamic — a source can flip it ON after the
pipeline has connected. The gate converts the common static case into an early start-time
refusal; the decoder halt is the backstop for the mid-stream flip. Both are fail-closed; no
compressed payload is ever partially decoded or guessed at.

A future pure-Elixir zstd decoder (or a superseding ADR-0008 decision) would REMOVE this gate
variable and consume payloads instead — the gate is the honest posture for the capability
capstan actually has.

## Consequences

- Sources that deliberately enable compression are refused at connect with the reason naming
  the setting — actionable without a capstan-side change (disable compression for the source
  capstan tails).
- The gate reads a variable that exists only on MySQL 8.0.20+; on an older server the single
  precondition query errors and the connection refuses `:precondition_query_failed` —
  fail-closed either way, and capstan already requires 8.0+.
- The preflight report (`scripts/capstan-preflight.sql`) folds the check into the precondition
  table so discovery and the runtime gate agree.

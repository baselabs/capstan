# ADR-0007 — The value-free boundary (Rule 1)

**Status:** Accepted (authored from the landed surface 2026-08-23; the posture has governed since the
first slice) · **Honors:** the repo's Critical Rule 1 (AGENTS.md), design Q15/Q16 · **Related:**
[ADR-0002](0002-fail-closed-server-preconditions.md) (refusal atoms), [ADR-0003](0003-transaction-shape-and-checkpoint-semantics.md) (halt reasons)

## Context

A CDC pipeline is a conduit for the source's most sensitive data — row values, DDL literals with
`WHERE` clauses, the replication password. Every channel that leaves the pipeline process
(error returns, log lines, telemetry payloads, crash reports, `inspect` output in a console or
IEx session) is a leak vector if a value can ride it. The pre-implementation design fixed the
posture (Q15/Q16): assume every value is PII or a secret, and make the boundary structurally
incapable of carrying one, rather than careful-by-convention.

Four vectors were named and each got a mechanism:

1. **Row values** — never enter any error, halt reason, log line, or telemetry payload.
   `Capstan.Error` (lib/capstan/error.ex) is the boundary normaliser: a raw driver error,
   exception, or reason tuple reduces to a stable atom `reason` plus at most a structural
   `shape` (an exception's MODULE name, never its message); `message/1` and the derived
   `Inspect` render only those fields. `%Capstan.Change{}` (lib/capstan/change.ex),
   `%Capstan.Transaction{}`, `%Capstan.Snapshot.Chunk{}`, and `%Capstan.Snapshot.State{}` all
   `@derive {Inspect, only: <structural fields>}` — `record`/`old_record`/`rows`/`tables` are
   invisible to the default inspect, so an IEx poke or a crash-report arg dump cannot print them.
2. **DDL statement text** — the raw `QUERY` body is decoded (the terminator classification needs
   it) but the `%Capstan.SchemaChange{}` (lib/capstan/schema_change.ex) carries only
   `schema`/`table`/`kind`; the statement text is redacted before the struct, the sink, telemetry,
   or any log (sink contract, lib/capstan/sink.ex).
3. **`ROWS_QUERY_LOG_EVENT`** — carries the complete original statement of a row change, every
   literal included. The decoder reduces it to `{:rows_query, :discarded}`; the SQL text never
   enters a returned term (lib/capstan/binlog/decoder.ex).
4. **The connection password** — lives only in the connection keyword; no error path, halt
   reason, or payload formats it.

## Decision

The boundary is enforced at runtime and proven at test time, on both telemetry channels:

- **Metadata gates KEYS.** `Capstan.Telemetry.event/3` routes through `validate!/1`, which
  raises on any key outside the allowlist (`server_version`, `server_uuid`, `tls`, `reason`,
  `gtid`, `schema`, `table`, `kind`, `missing_gtids` — telemetry.ex). A future emitter attaching
  a row value cannot ship it; the process raises instead.
- **Measurements gate KEYS and VALUES.** Measurement keys pass their own allowlist (the
  structural vocabulary: `change_count`, `sink_ms`, `establish_ms`, `table_count`,
  `row_count`, `chunk_seq`) and values must be non-negative numbers — a numeric row value
  (a balance, an account number) is a non-negative number, so a type-only gate would let it
  ride. The refusals name the offending KEYS only, never the values: the gate's own exception
  must not become the leak vector (a review round caught exactly that in the first cut).
- **Halts are value-free atoms or tagged outer atoms.** `{:sink_error, reason}` emits the OUTER
  atom only; a compound reason's payload is scrubbed through `Capstan.Error.from/1`.
- **Test time:** `Capstan.ValueFree` (test/support/value_free.ex) plants sentinels — a row value,
  the password, a DDL literal, a `ROWS_QUERY` statement — and scans the log and telemetry
  channels (and the delivered sink outputs) for them across every delivered and error/halt path.

Adjacent memory-safety decision recorded here because it is the same posture: column names stay
**strings**, never `String.to_atom` — a wide or attacker-influenced schema must not exhaust the
atom table.

## Consequences

- Operator-facing reasons are stable, greppable atoms; debugging a value needs the sink or the
  source, never the library's channels — an intentional constraint, occasionally inconvenient.
- Adding a telemetry event or metadata key is a contract change: extend the allowlist and the
  usage-rules table together, and the ValueFree sweep covers the new path.
- Crash reports stay value-free only as long as struct fields remain elided — new structs default
  to `@derive {Inspect, only: ...}`; a struct carrying row data without the derive is a review
  blocker.

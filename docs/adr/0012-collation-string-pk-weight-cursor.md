# ADR-0012 — Collation-ordered string PKs: a server-computed weight-bytes cursor

**Status:** Accepted (2026-08-23) · **Extends:**
[ADR-0005](0005-initial-snapshot-cursor-gate-brief-lock.md) (the order-faithful PK allowlist
gains its string arm) · **Honors:** [ADR-0007](0007-value-free-boundary-rule-1.md) (weights
are row-derived values — same Rule 1 posture as the cursor itself)

## Context

ADR-0005's cursor-gate classifies every streamed change on a snapshot-active table by
`k ≤ cursor` **in Elixir**, while the chunk read pages by `WHERE pk > cursor ORDER BY pk`
**in MySQL under the column's collation**. The gate is correct only when the Elixir comparison
provably reproduces `ORDER BY`. C2 therefore refused the whole string family
(`:snapshot_pk_unsupported_type`) — including the MySQL 8 **default** collation
`utf8mb4_0900_ai_ci`, i.e. the common case.

The refusal is not conservative padding: live measurement (probe/collation_weight_probe.exs,
8.0.46, 16/16) proves Elixir byte order **diverges** from collation order (`'a'` sorts before
`'Z'` under `ai_ci`, after it in bytes), and the divergence mis-classifies real changes — a
streamed insert of a key the gate thinks is already backfilled is forwarded when its chunk has
not run (double delivery), or suppressed when it has (a gap).

MySQL's reference manual (read first-hand) defines `WEIGHT_STRING(str)` as the collation's
comparison-and-sort value with the contract: equal weights ⇔ collation-equal strings,
byte-smaller weights ⇔ sorts first. The function is documented as a **debugging function whose
form may change without notice between versions** — so the design must depend only on its order
contract, computed server-side, and never on a frozen byte form.

## Decision

Accept `CHAR`/`VARCHAR` primary-key columns — any charset, any collation, alone or in composites
and in the NOT-NULL unique-key fallback — with the **server as the only collation oracle**:

1. **Canonical string PK value = its collation weight bytes.** Chunk side: the chunk SELECT
   appends `WEIGHT_STRING(pk)` per string PK column. Stream side: the coordinator resolves the
   (distinct, uncached) string-PK values of a transaction with a batched, item-capped
   `SELECT WEIGHT_STRING(CONVERT(X'..' USING <charset>) COLLATE <column collation>), …` on the
   readers' shared query connection. Byte order of weights is `ORDER BY` order (probed for
   `ai_ci`, both `_bin` weight forms, PAD SPACE over distinct keys, multi-level `as_cs`, and
   composites), so the gate's `compare/2` stays a plain byte/tuple comparison. Distinct PK
   values are collation-distinct by the PK constraint itself (probed: case/accent/space-stripped
   collisions), so weights are also a faithful key identity.
2. **Both literal forms are COLLATE-pinned.** An unpinned `CONVERT(X'..' USING cs)` computes the
   charset-**default** collation's weights — a different order space (measured: `1CAA` vs the
   column's multi-level `1CAA00000002…`) — and in a `WHERE` it raises ERROR 1267 on non-default
   collations. The pagination predicate is `pk > CONVERT(X'..' USING cs) COLLATE <collation>`
   (measured: keeps the `range`+`PRIMARY` index access).
3. **The durable cursor is dual-formed** for string tables: `%{raw: column_bytes, weight: …}` —
   `raw` builds the `WHERE` literal, `weight` feeds the gate. Non-string tables keep the bare
   canonical term (byte-identical to pre-0012). **At resume the `.weight` half is recomputed
   from the persisted `.raw`**: a server upgrade between run and resume could change weight
   forms (the documented version-mutability), and comparing an old-form cursor against
   new-form keys would mis-order silently — recompute makes both sides same-server by
   construction.
4. **String PK columns are projected as `CAST(col AS BINARY)`** — the column's own bytes, which
   is exactly what the binlog row decoder delivers for the same column; `utf8mb4` (the
   connection charset) is byte-identical to the previous text-protocol form. Weight columns are
   cursor-internal and never appear in delivered rows.
5. **Refusals stay, now on measured grounds:** the TEXT family (prefix-PK pagination pays a
   per-page filesort — `Using filesort` vs `Using index`, superlinear backfill) and
   `ENUM`/`SET` (the column's `ORDER BY` is member-position order and its column weights are
   position-based, while any stream-side introducer computes **string** weights — two
   disagreeing order spaces, no uniform mechanism).
6. **Weight resolution fails closed**: query faults are budgeted via the shared retry counter,
   then halt `:snapshot_pk_weight_failed` (value-free). A bounded cache collapses repeated keys;
   the `:start` cursor and completed tables resolve nothing.

## Consequences

- The cost is honest: during a string-PK table's backfill window, every streamed transaction
  with a **new** string-PK key pays ≥1 weight round trip (serialized in the coordinator; the
  assembler's synchronous sink call waits). Composite row-value pagination remains
  `type=index` per page — a pre-existing cost class, unchanged by this ADR and documented in
  usage-rules.
- Weight bytes are row-derived values (Rule 1): they live only in the durable state (already
  Inspect-elided), in memory, and in SQL literals on the query connection — never in logs,
  errors, or telemetry.
- `WEIGHT_STRING()`'s version-mutability is contained, not assumed away: both sides of every
  comparison come from the same server in the same run, and resume recomputes the cursor half.

## Evidence

- Probes: `probe/collation_weight_probe.exs` (16/16) + `probe/collation_extra_probe.exs`
  (COLLATE pin), findings recorded in `probe/FINDINGS.md`; refman `WEIGHT_STRING()` read
  first-hand.
- Marquee: `test/integration/snapshot_test.exs` (ai_ci + as_cs arms with concurrent
  straddling changes, delivered-record-keys invariant, resume).
- Unit tripwires: `test/capstan/snapshot/{primary_key,cursor_gate,chunk_reader,coordinator}_test.exs`.

# ADR-0003 — Transaction shape and checkpoint semantics

**Status:** Accepted (C1) · **Design refs:** Q13, Q14 (plus the Q8 `server_id`-conflict detail)

## Context

A capstan transaction is assembled from a fold over the binlog event stream, and the fold must
know exactly where a transaction ends. MySQL closes a transaction in more than one way, and one
shape — XA — carries row images that may later be rolled back. Getting the grammar wrong loses
data silently: a `COMMIT` misread as DDL drops a transaction's rows; a DDL never recognized as a
terminator never advances the checkpoint; a prepared-then-rolled-back XA transaction delivered
as committed applies phantom rows effect-once, with no server error to catch it. Separately, the
checkpoint must record *something* for a transaction whose changes are all filtered, or a
fully-filtered quiet period stalls progress forever.

## Decision

1. **Three terminators (`Capstan.Assembler`).** The in-flight buffer accumulates row `%Change{}`s
   and is released to output **only** at a terminator; a `{:halt, _}`/`{:error, _}` short-circuits
   with the buffer still un-released, so a partial transaction is structurally impossible to leak.

   - **`XID`** — a transactional (InnoDB) commit.
   - **`QUERY("COMMIT")`** — a non-transactional (MyISAM) commit; the `COMMIT` is the terminator,
     not a change.
   - **a self-committing DDL `QUERY`** — a `QUERY` that is not `BEGIN`/`COMMIT` and arrives with
     no open `BEGIN` (`GTID → QUERY(DDL)`, no `XID`). It yields a `%Capstan.SchemaChange{}` (with
     redacted statement text — only structured `schema`/`table`/`kind`) and advances the
     checkpoint. Without this clause a DDL-carrying GTID would never close and the checkpoint
     would never advance.

2. **XA is refused honestly as `:unsupported_transaction_shape`.** XA support requires tracking
   prepare → commit/rollback across an unbounded window — a real subsystem (deferred to a later
   row), not a branch. The **shipped** handling (hardened by the C1 Task 18 proof layer, commit
   `3fe5e5a`):

   - `XA START` / `XA END` are recognized as **non-DDL transaction-control** statements
     (`xa_statement?/1`, prefix `"XA "`) and **open the transaction block** so its row images
     accumulate in the buffer — exactly as a `BEGIN` would. They are **not** treated as
     self-committing DDL.
   - The whole XA transaction therefore stays **buffered**.
   - `XA_PREPARE_LOG_EVENT` (event type 38) decodes to `{:halt, :unsupported_transaction_shape}`,
     which short-circuits the fold with the **buffer discarded** and the **checkpoint NOT
     advanced** — zero prepare-carried rows are ever delivered.

   The Task 18 proof layer found and fixed a **silent-loss bug** here: before the fix, an
   `XA START` fell through to the "not `BEGIN`/`COMMIT`, no open `BEGIN`" clause and was
   misclassified as self-committing DDL. That emitted a spurious `%SchemaChange{}` and advanced
   the checkpoint **past the XA GTID**, then failed on the following row events — silently losing
   the transaction if the XA later committed. The `xa_statement?/1` clause is what keeps the
   buffer open so the `XA_PREPARE` halt is the terminator that fails closed.

3. **The checkpoint is a PROCESSED-GTID watermark (Q14).** It records **every committed GTID the
   pipeline has processed — delivered *and* filtered** — not a delivery log. A committed
   transaction whose changes are all filtered (or which has none) still advances the position and
   is emitted as a `%Transaction{}` with empty `changes`; `Capstan.AssemblerServer` checkpoints it
   **without** calling the sink. This keeps the persisted set a compact contiguous interval,
   makes progress unconditional during a fully-filtered quiet period, and gives `member?/2` one
   unambiguous meaning ("already processed"). Recording only *delivered* GTIDs would fragment the
   set unboundedly and stall the pipeline into ADR-0002's retention gap.

4. **`server_id`-conflict discrimination on error 1236 (Q8; added this slice, Task 18 commit
   `3fe5e5a`).** MySQL 8.0.x evicts a replica that registers an already-in-use `server_id` by
   sending a dump refusal (error 1236) whose message names `server_uuid`/`server_id` — **not** a
   bare socket drop the reconnect cycle counter would see. `Capstan.Connection.classify_dump_error/2`
   discriminates the overloaded 1236 on its message text: a `server_uuid` message maps to
   `:server_id_conflict`, so two instances sharing a `server_id` halt with an actionable reason
   instead of mutually evicting in an unbounded livelock that emits healthy `:established`
   telemetry on each pass. (Purge messages → `:data_gap`; checksum-negotiation → its own reason;
   any other 1236 → `:unrecognized_dump_error`, never `:data_gap`.)

## Consequences

- DDL advances the checkpoint and delivers a redacted `%SchemaChange{}`; non-transactional
  engines are supported via their `QUERY("COMMIT")` terminator.
- XA fails closed with zero prepare rows delivered and the checkpoint held — the honest refusal
  from replicant ADR-0001's house rule, not a silent phantom-data delivery.
- A long fully-filtered quiet period never stalls the position; the persisted set stays a single
  interval.
- A duplicate `server_id` (rolling deploy, stale node, copy-pasted config) halts actionably
  rather than livelocking while looking healthy.

## Evidence

`lib/capstan/assembler.ex:247-248,280-304,311-333` (three terminators; XA opens the block),
`:169-177` (halt discards the buffer) · `lib/capstan/binlog/decoder.ex:205`
(`@xa_prepare → {:halt, :unsupported_transaction_shape}`) ·
`lib/capstan/assembler_server.ex:195-210` (filtered/empty advances without a sink call) ·
`lib/capstan/connection.ex:169-182` (1236 discrimination), `:405` (`:server_id_conflict` halt).

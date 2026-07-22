# ADR-0005 — Initial snapshot: cursor-gated suppression + brief per-chunk lock

**Status:** Accepted (user-approved 2026-07-21) · **Extends:** [ADR-0002](0002-fail-closed-server-preconditions.md) (adds snapshot-path preconditions) · **Honors, does not supersede:** [ADR-0001](0001-position-and-dedup-model.md), [ADR-0003](0003-transaction-shape-and-checkpoint-semantics.md), [ADR-0004](0004-c1-scope-lib-owned-checkpoint-only.md)

## Context

C2 backfills the rows that pre-exist a capstan pipeline's start, woven into the continuously-running
C1 stream so there is **no handoff instant to get wrong** — no gap and no duplicate at row
granularity, resumable mid-backfill. The source is a read-only MySQL replica (a payment switch's
replica, for the first design partner), so the mechanism may not write to the source, and MySQL has
**no `EXPORT_SNAPSHOT`** (Postgres's atomic snapshot-id export). The central constraint, forced live
on 8.0: **there is no lock-free way to atomically pair a `START TRANSACTION WITH CONSISTENT SNAPSHOT`
read view with an exact GTID position** — an in-transaction `@@global.gtid_executed` read returns the
live global, not the frozen view.

The obvious lock-free design — read a low watermark `LW = @@gtid_executed`, read a chunk, read a high
watermark `HW`, and reconcile the chunk against the window `(LW, HW]` — is **not fail-closed**. A
live probe on 8.0 confirmed that **`@@gtid_executed` LEADS InnoDB row-visibility under concurrent
commit**: a GTID enters `gtid_executed` before its row is visible to a new read view, by up to the
in-flight group-commit depth (300000/300000 iterations with 3 concurrent writers; 0/120000 with a
single writer). So a transaction `gT` touching a chunk key can be in `LW` (hence excluded from the
chunk's window, not dropped from the chunk) while the chunk `SELECT` still reads the STALE value —
and the stream, seeing `gT ≤ LW`, suppresses it. The stale chunk row then overwrites the fresh
streamed row: **silent corruption of exactly the changed rows**, the class C2 exists to prevent. For
a fail-closed-over-silent-corruption library (ADR-0001, "never a parity-shaped lie"), a rare silent
corruption is disqualifying, even though Debezium's read-only snapshot tolerates the same window.

The fix cannot be lock-free. A provable lower bound on a chunk's read view needs EITHER an in-band
write committed through the same path (DBLog's watermark — impossible on a read-only replica) OR a
brief lock that quiesces commits while an exact `(gtid, snapshot-view)` pair is captured. A live
investigation on a reproduced replica topology measured the brief `LOCK TABLES … READ` **cheap**
(~150–265 ms per chunk dominated by per-call connection setup; sub-ms on capstan's persistent
connection), needing only the `LOCK TABLES` privilege (not global `RELOAD`/FTWRL).

## Decision

C2 backfills via **cursor-gated suppression + a brief per-chunk `LOCK TABLES … READ` that captures an
exact GTID position `G`**. The read-only lock-free `(LW, HW]` bracket is **rejected** (not
fail-closed).

1. **Two authorities, never conflated.** The GTID processed-watermark is advanced ONLY by the stream
   (exactly as C1). The snapshot advances a per-table **PK cursor** held in a separate
   `Capstan.SnapshotStore`. The snapshot contributes **zero** GTIDs — it can never become a
   `Capstan.Gtid.member?/2` dedup floor, so ADR-0001's one-authoritative-position invariant is
   untouched. The stream runs continuously from a floor `P0 = @@gtid_executed` read strictly before
   the first chunk and pre-seeded into the `CheckpointStore`, so the dump and the assembler watermark
   both resolve to `P0` through the sanctioned `start_position: :checkpoint` path — **no C1b
   machinery and no ADR-0004 supersession** (the pre-seed IS the documented "start from now"
   pattern). C1b (`%Position{}` override / `:current`) stays deferred and refused fail-closed.

2. **Brief-lock exact position `G` (per chunk of table `T`).** `SET SESSION lock_wait_timeout` (a
   bounded MDL wait — the default is ~1 year) → `LOCK TABLES T READ` → `G = @@gtid_executed` →
   `START TRANSACTION WITH CONSISTENT SNAPSHOT` → `UNLOCK TABLES` → chunk `SELECT … WHERE pk > cursor
   ORDER BY pk LIMIT chunk_size` (as-of the pinned view, lock-free) → `COMMIT`. The lock quiesced
   `T`'s writes and waited for in-flight ones, so `G` is a **provable lower bound** on the chunk's
   view for `T`: no `T`-change `≤ G` is missing from the chunk, and no new `T`-change commits during
   the capture. This is the guarantee the written watermark gives DBLog, obtained with a brief read
   lock instead of a write the replica forbids.

3. **Cursor-gate (strict-once).** A `Snapshot.Coordinator` interposes as the `AssemblerServer`'s sink
   and its `:watermark_observer`. For each streamed row-image on a snapshot-active table it forwards
   iff `k ≤ cursor(T)` (already backfilled) or `T` is complete, else **suppresses** (its future chunk
   delivers it). A fully-suppressed transaction advances the watermark with no sink call (a filtered
   transaction, ADR-0003). A PK-changing UPDATE is split into `delete(k_old)` + `upsert(k_new)`, each
   gated on its own key. The advance gate emits a buffered chunk (as `%Change{op: :snapshot}`) once
   `Gtid.subset?(G, processed_set)`, then advances the cursor and persists it. The chunk read uses
   `LIMIT chunk_size + 1` (a one-row finality look-ahead) so the last chunk carries
   `final_chunk?: true` even when a table's row count is an exact multiple of `chunk_size` (closeout
   F6); the look-ahead row is dropped and re-read as the head of the next chunk, so no row is skipped
   or duplicated.

4. **Sink surface.** A new optional `c:Capstan.Sink.handle_snapshot/2` delivers a chunk as a concrete
   list of `%Capstan.Change{op: :snapshot}` with a value-free `Capstan.Snapshot.Meta`. It is distinct
   from `handle_transaction/1` because a chunk is **not** a committed transaction (no GTID, no
   atomicity) — routing it through `handle_transaction/1` would fabricate a GTID and break ADR-0001.
   Semantics are **upsert-by-PK**: a snapshot row may be superseded by a later streamed change, so a
   compliant sink applies each chunk row as an upsert. This is a HARD C2 precondition (the strict-once
   suppression and the bounded crash-window re-emit both rely on it to converge).

5. **Delivery guarantee.** **Strict-once in normal operation** (the cursor-gate, not convergence). The
   only duplicate window is a crash between the chunk's sink `{:ok, _}` and the `pk_cursor` persist —
   the one in-flight chunk re-emits, exactly the bounded window C1 already accepts; an upsert-by-PK
   sink converges. **Crash-window delete backstop (closeout F1):** a second durable high-water,
   `delivered_pk`, is persisted BEFORE each emit (the `pk_cursor` after). Across that same window it
   sits ahead of the rolled-back `pk_cursor`, and the cursor-gate gates DELETES on it — a streamed
   delete of an already-delivered key (`k ≤ delivered_pk`) is forwarded and sweeps the row, instead of
   being suppressed while the re-read chunk (taken as-of a fresh `G` with the row already gone) omits
   it, which would leave a permanent phantom. Inserts/updates keep gating on `pk_cursor`, so
   strict-once is preserved; a forwarded delete of a not-yet-delivered key is a harmless no-op at the
   upsert/delete-by-PK sink. Effect-once across the crash window is the deferred sink-owned atomic path
   (C1a).

6. **Choke-point observer.** The `AssemblerServer` gains one optional `:watermark_observer` notified
   at the checkpoint choke point (`do_checkpoint/3` success), which fires on EVERY advance —
   delivered, filtered, AND self-committing DDL — by construction, so a DDL on a non-snapshot table
   can never stall the advance gate. No observer ⇒ byte-identical C1.

7. **Snapshot preconditions (an ADR-0002 addendum).** The SELECT path adds its own fail-closed
   preconditions in ADR-0002's posture, each a distinct value-free halt:
   - **Order-faithful PK only.** Chunking pages by MySQL `ORDER BY pk` while the cursor-gate compares
     in Elixir, so the PK type's Elixir term-order must provably match MySQL's: signed/unsigned
     integer + `BINARY`/`VARBINARY` + composites thereof. A collation-ordered STRING PK
     (`CHAR`/`VARCHAR`/`TEXT`) halts `:snapshot_pk_unsupported_type` (Elixir cannot reproduce a
     collation order — a silent gap/dup risk). `BIGINT UNSIGNED` is in-allowlist only with a decoder
     yielding the true value across 2^63. A missing PK with a nullable unique-key column halts
     `:snapshot_table_no_primary_key`.
   - **`LOCK TABLES` privilege** on the snapshot tables (in addition to `SELECT`); no `RELOAD`/FTWRL.
   - **Source identity** — `@@server_uuid` is compared across the stream and query connections, at
     connect and on every query-conn reconnect (`:snapshot_source_mismatch`), so a VIP reconnect to a
     different replica mid-backfill is caught.
   - **`tables: :all` snapshot is refused** (`:config_invalid`) — when the capture allowlist is itself
     `:all`, "snapshot every table on the server" is ambiguous and dangerous, so C2 requires an
     explicit snapshot table list. A concrete capture allowlist (from which snapshot defaults) works.
   - **Config/state table-set reconciliation (closeout F3).** On a `:complete`/mid-snapshot resume the
     configured `snapshot.tables` are reconciled against the stored `%Snapshot.State{}` table keys; a
     divergence (a table added or removed after a durable state exists) halts `:snapshot_config_drifted`
     rather than silently never backfilling an added table — both the `:complete` short-circuit and the
     resume path key off the STORED set. Dropping the durable snapshot state re-introspects config.

### Rejected alternatives

- **Read-only lock-free `(LW, HW]` bracket** — rejected: not fail-closed (the confirmed
  `gtid_executed`-leads-visibility silent corruption above).
- **C1b explicit-start machinery / an ADR-0004 supersession** — rejected as unnecessary scope: the
  `P0` pre-seed into the `CheckpointStore` makes the dump and the watermark agree by construction.
- **Written-watermark (DBLog) path** — impossible on a read-only replica; the brief read lock is the
  correct equivalent.
- **A blanket composite-PK refusal** — rejected: composite integer/binary PKs are order-faithful
  (tuple compare) and ship; only collation-ordered STRING columns are refused.

## Consequences

- The snapshot→stream handoff is **gap-free and dup-free at row granularity, proven live** under
  concurrent load, with the exact-`G` lower bound the correctness linchpin (a bare `gtid_executed`
  read is a red-capable regression). A silent corruption is not possible in normal operation.
- The backfill costs a brief per-chunk table lock. With `replica_preserve_commit_order=ON` a
  commit-head-blocked write stalls the whole parallel applier for the (sub-ms, bounded) hold, so a
  large table incurs some replication lag during backfill; a larger `chunk_size` (fewer locks)
  mitigates it. Negligible for a shadow pilot; the DBA should know.
- New operational requirement: the query account needs `LOCK TABLES` on the snapshot tables. Recorded
  in the preflight/DBA handout.
- New public contract: `c:Capstan.Sink.handle_snapshot/2` (optional; required only in snapshot mode)
  and `%Capstan.Change{op: :snapshot}` (upsert-by-PK). Additive — an absent `:snapshot` config is
  pure C1, byte-for-byte.
- Two named fail-closed capability boundaries ship as loud refusals, each a separable follow-up:
  collation-ordered-string PK (collation-aware cursor comparison) and `tables: :all` snapshot
  resolution (a scoped `information_schema` enumeration). Neither is silent narrowing.

## Evidence

- Probe (the rejected bracket): `@@gtid_executed` leads visibility, 300000/300000 under 3 writers —
  the confirmed silent-corruption defect. Marquee: `test/integration/snapshot_test.exs` (no-gap/no-dup
  under 8-writer load, resume, suppression non-vacuity, advance gate under filtered+DDL).
- Exact-`G` lower bound: `lib/capstan/snapshot/chunk_reader.ex` + `test/capstan/snapshot/chunk_reader_test.exs`
  (live: 0 violations locked / violations≠[] bare — the red-capable regression gate).
- Cursor-gate + PK-changing UPDATE split: `lib/capstan/snapshot/cursor_gate.ex`. Order-faithful PK:
  `lib/capstan/snapshot/primary_key.ex`. Coordinator: `lib/capstan/snapshot/coordinator.ex`. Bootstrap
  `P0` pre-seed + supervisor wiring: `lib/capstan/snapshot.ex`, `lib/capstan/supervisor.ex`. Query conn
  + source identity: `lib/capstan/query.ex`. Choke-point hook: `lib/capstan/assembler_server.ex`.
- Halt catalog: the `:snapshot_*` reasons in `lib/capstan/error.ex`.

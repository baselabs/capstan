# ADR-0006 — XA prepare/commit tracking via a held-out watermark

**Status:** Accepted (design approved 2026-07-22 via best-of-N + a fresh-context adversarial pass;
**implementation pending the C5 plan** — not yet landed in `lib/`). · **Supersedes:**
[ADR-0003](0003-transaction-shape-and-checkpoint-semantics.md) §2 (XA refused) **for the opt-in
`xa: :track` path only**; **weakens** ADR-0003 §3's "never stall into the retention gap" consequence for
that path. · **Honors:** [0001](0001-position-and-dedup-model.md), [0002](0002-fail-closed-server-preconditions.md),
[0004](0004-c1-scope-lib-owned-checkpoint-only.md), [0005](0005-initial-snapshot-cursor-gate-brief-lock.md).

## Context

ADR-0003 §2 refuses XA transactions fail-closed (`:unsupported_transaction_shape`): rows accumulate
across `XA START`/`XA END`, and the `XA_PREPARE` event (type 38) halts with the buffer discarded and the
checkpoint held. C5 replaces the refusal, for opt-in consumers, with real tracking: deliver an
XA-committed transaction's rows and drop a rolled-back one, under bounded state.

The mechanism is hard because a live 8.0.46 probe established that **prepare and commit/rollback are
separate transactions, each with its own GTID** (`G_p` and `G_c`), and — critically — **`XA PREPARE`
publishes `G_p` into `@@gtid_executed` at prepare time**, before the commit. Advancing the durable
checkpoint past `G_p` before `G_c` is known risks **silent data loss** on a crash: MySQL will not resend
a GTID the checkpoint already covers, and any rows held only in memory are then gone with no error.

The design was chosen by conformance-gated best-of-N (three fresh-context, frame-independent candidate
designs — spec-literal / risk-first / simplest-architecture — **all converged** on the same core; winner
spec-literal), then hardened by a fresh-context adversarial pass (8 challenges, all accepted). Full
deliberation: `.forge/specs/2026-07-22-capstan-c5-xa-design.md`; best-of-N audit
`.forge/best-of-n/c5-xa-core-mechanism.md`; adversarial reconciliation
`.forge/reviews/2026-07-22-c5-xa-design-adversarial.md`.

## Decision

1. **Held-out watermark (the core invariant).** The prepare GTID `G_p` never enters the durable
   processed-set except in the **same single checkpoint write** that adds its resolving GTID `G_c`.
   Between prepare and resolution `G_p` is a legitimate **hole** in the set (ADR-0001 non-contiguity).
   Enforced structurally in the pure fold: `XA_PREPARE` emits no output and does not advance `G_p`; the
   commit/rollback unions `{G_p, G_c}` atomically. On a crash in the prepare window, MySQL **re-streams
   the interior hole** `G_p` (proven live) and the pool rebuilds from the binlog — **no durable
   prepared-row store**.

2. **Prepared pool + bound.** An in-memory `prepared` map keyed by a **sha256 digest of the canonical
   XID** (a `Capstan.Xa.Id` value type — raw `gtrid`/`bqual` bytes are never stored) holds the buffered
   rows between prepare and resolution. Bounded by **`max_prepared_transactions`** (count;
   halt-`:xa_prepared_pool_exhausted`-never-evict — eviction would be the silent loss this prevents),
   plus an **optional** operator `max_prepared_bytes` (default off). One-phase XA is an ordinary
   single-GTID commit terminator (no pool entry).

3. **Opt-in, default-refuse.** `xa: :track | :refuse`, default **`:refuse`** — today's behavior
   byte-for-byte for non-XA pipelines. The decoder stays pure and config-free (returns the raw type-38
   body); the **fold** is the policy layer, parameterized like the table filter.

4. **Pre-start dangling prepares.** At connect, capstan runs **`XA RECOVER`** (it already queries at
   connect) and pre-seeds the pool with the source's currently-prepared XIDs (row-less, marked
   pre-start). A later commit/rollback for such an XID is a correct **row-less watermark advance** — its
   rows predate our start (snapshot's domain). The desync discriminator is membership in
   **(pool ∪ startup-`XA RECOVER` set)**; anything else fails closed `:xa_commit_without_prepare` /
   `:xa_rollback_without_prepare`. A re-presented prepare for an already-pooled XID with a matching prepare
   GTID is a benign idempotent re-pool (capstan's in-process reconnect resend), not a duplicate halt.

5. **Rule 1.** The XID `gtrid`/`bqual` are application-chosen, row-value-class bytes. The pool key is a
   digest, never the raw bytes; `%Assembler{}` elides the pool from `Inspect`; telemetry and errors
   correlate on the **GTID** (`G_p`/`G_c`, server-assigned and already value-free), **never** on any
   XID-derived value — a digest of a low-entropy XID is reversible, so even the digest is never emitted.

6. **Snapshot interaction.** When a backfill runs concurrently, the snapshot's exact-`G` capture (under
   its brief `LOCK TABLES … READ`) runs `XA RECOVER` and **excludes any prepared-on-a-snapshot-table
   `G_p` from the exact-`G`**, keeping it a true lower bound on *visible/committed* data — otherwise a
   prepared-but-invisible XA (whose `G_p` is already in `@@gtid_executed`) would over-state chunk
   coverage, the ADR-0005 silent-corruption class.

## Consequences

- XA-committed rows deliver at-least-once across the crash window (upsert-by-PK converges — ADR-0004,
  unchanged); a rolled-back XA delivers zero rows; state is bounded by the prepared **count**.
- **Availability trade (not a free lunch):** if a held `G_p` is **purged** before its commit, resume
  halts fail-closed `:data_gap` — never silent. This weakens ADR-0003 §3's retention-gap-avoidance for
  the `:track` path, accepted under the assumption **prepare-window ≪ binlog-retention** (true for a
  short-2PC source; confirmed per deployment by the preflight). Durable spill would preserve availability
  under purge at the cost of a durable surface — a named separable mode, not built now.
- Additive: an absent `xa:` config is pure prior behavior, byte-for-byte.
- The decoder's type-38 return changes from `{:halt, …}` to `{:ok, {:xa_prepare, body}}`; the loud
  default-refuse halt moves to the assembler (existing decoder/assembler-level tests migrate accordingly).

## Rejected alternatives

- **Durable prepared-row spill (B).** Rejected as the default: hole-resend makes a durable store
  unnecessary for correctness, and it would exceed the ADR-0004 at-least-once boundary. It is **not
  dominated** — it wins on availability-under-purge — so it is retained as a named follow-on mode for a
  partner whose hold-vs-retention profile makes `:data_gap` likely, not as a deferral of the acceptance.
- **Evict-on-overflow.** Rejected: evicting a prepared XA that later commits is exactly the silent loss
  C5 exists to prevent; the pool halts fail-closed instead.

## Evidence

Design note `.forge/specs/2026-07-22-capstan-c5-xa-design.md` (§2 live XA binlog grammar; §6 synthesized
mechanism + crash-window table; §5 decision log). Live probes: separate `G_p`/`G_c`, `G_p` in
`@@gtid_executed` at prepare, interior-hole-resend (re-proven independently by the best-of-N judge),
`XA RECOVER` enumeration. Anchors: `lib/capstan/assembler.ex` (fold + terminators), `binlog/decoder.ex:206`
(type-38), `assembler_server.ex` (checkpoint), `connection.ex:123` (connect-time query; `gap_check`),
`gtid.ex`/`position.ex` (set algebra), `telemetry.ex:26` (allowlist), `supervisor.ex` (`:temporary`
children — pool survives reconnect).

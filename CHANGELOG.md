# Changelog

All notable changes to capstan are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added — C2 initial snapshot

A consistent backfill of the rows that pre-exist a pipeline's start, woven into the
continuously-running C1 stream so there is **no gap and no duplicate** at the snapshot→stream
handoff — proven live under concurrent load, resumable mid-backfill, strict-once in normal
operation, fail-closed on every silent-loss condition, Rule 1 end-to-end. **Additive: absent the
`:snapshot` config, a pipeline is pure C1, byte-for-byte.**

- **Public API** — a `snapshot: [tables:, store:, chunk_size:]` block on `Capstan.start_link/1`; a
  new optional `c:Capstan.Sink.handle_snapshot/2` (required only in snapshot mode) delivering a
  concrete list of `%Capstan.Change{op: :snapshot}` + a value-free `Capstan.Snapshot.Meta`;
  **upsert-by-PK** sink semantics (a hard precondition). A new `Capstan.SnapshotStore` behaviour
  (`read/1`+`write/2` over a `%Capstan.Snapshot.State{}`, `InMemory` reference impl) persists the
  per-table PK cursor, separate from the GTID checkpoint.
- **Mechanism (ADR-0005)** — cursor-gated suppression + a brief per-chunk `LOCK TABLES … READ` that
  captures an **exact GTID position `G`** (a provable lower bound on the chunk's read view). The
  read-only lock-free `@@gtid_executed` bracket was rejected: it leads InnoDB row-visibility under
  concurrent commit (a probe-confirmed silent corruption). The stream advances the GTID watermark;
  the snapshot advances the PK cursor — **two authorities, never conflated** (the snapshot adds no
  GTID; ADR-0001 untouched). No C1b, no ADR-0004 supersession — the `P0` pre-seed makes the dump and
  the watermark agree by construction.
- **New source privilege** — the query account needs **`LOCK TABLES`** on the snapshot tables (in
  addition to `SELECT`); no `RELOAD`/FTWRL. Added to `scripts/capstan-preflight.sql`.
- **Fail-closed snapshot halts** (distinct value-free reason each, tripwired): order-faithful-PK
  only (`:snapshot_pk_unsupported_type`; a collation-ordered string PK is refused — ROADMAP C2a),
  `:snapshot_table_no_primary_key`, `:snapshot_table_not_captured`, `:snapshot_lock_unavailable`,
  `:snapshot_schema_drifted`, `:snapshot_source_mismatch` (`@@server_uuid` across both connections),
  `:snapshot_chunk_read_failed`, `:snapshot_bootstrap_gtid_read_failed`, `:snapshot_coordinator_down`,
  the `SnapshotStore` faults, and `{:snapshot_sink_error, _}`. A `tables: :all` snapshot is refused
  `:config_invalid` — an explicit table list is required (ROADMAP C2b).
- **Delivery guarantee** — strict-once in normal operation; the only duplicate window is a crash
  between a chunk's `{:ok}` and the durable cursor persist (the one in-flight chunk re-emits, C1's
  bounded posture; an upsert-by-PK sink converges). Effect-once across the crash window is the
  deferred sink-owned path (ROADMAP C1a).

## [0.1.0] - 2026-07-21

### Added — C1 streaming spine

The initial streaming spine: connect to MySQL as a replica, tail the row-based binary log from a
GTID position, assemble committed transactions, deliver them to a sink, and durably advance a
processed-GTID checkpoint — halting fail-closed on every condition that could otherwise lose or
corrupt data silently.

- **Public API** — `Capstan.start_link/1` and `child_spec/1`; a supervised pipeline
  (`Capstan.Connection` owning the socket, `Capstan.AssemblerServer` owning assembly, delivery,
  and the checkpoint). Bad config is refused before any socket opens.
- **Position and dedup model (ADR-0001)** — GTID set is the sole authoritative and sole persisted
  position; dedup by set membership via the public `Capstan.Gtid` (`member?/2` and the interval
  algebra). No scalar ordinal; `%Capstan.Position{}` `file`/`pos` are diagnostic only.
- **Fail-closed server preconditions (ADR-0002)** — a connect-time gate on `binlog_format`,
  `binlog_row_image`, `binlog_row_metadata`, `binlog_row_value_options` (`PARTIAL_JSON` refused),
  and `gtid_mode`, each with a distinct value-free reason; `table_id`-keyed schema resolution;
  `ROWS_QUERY_LOG_EVENT` decoded and discarded (Rule 1); TLS on by default with an explicit
  peer-verification choice required (`:tls_verification_unspecified` otherwise).
- **Transaction shape and checkpoint semantics (ADR-0003)** — three terminators (`XID`,
  `QUERY("COMMIT")`, self-committing DDL `QUERY`); XA refused honestly as
  `:unsupported_transaction_shape` with the buffer discarded and the checkpoint held; the
  checkpoint as a processed-GTID watermark that advances on delivered *and* filtered transactions;
  `server_id`-conflict discrimination on error 1236 (`:server_id_conflict`, not a livelock).
- **Sink contract** — `Capstan.Sink` with `handle_transaction/1`, `handle_schema_change/2`, and
  `checkpoint/0`; `changes` typed `Enumerable.t()` (single-pass safe); DDL statement text redacted
  to structured `schema`/`table`/`kind`.
- **Lib-owned checkpointing** — `Capstan.CheckpointStore` behaviour + `InMemory` reference
  implementation, persisting one processed `gtid_set` string per pipeline.
- **Rule 1 (value-free)** — row values, DDL literals, `ROWS_QUERY` SQL, and the connection
  password never reach a log line or telemetry payload; proven red-capable on all four vectors.
- **Telemetry** — connection, transaction, schema-change, and gap events with a value-free
  metadata allowlist.
- **Streaming liveness** — a replication heartbeat requested from the server
  (`:heartbeat_period_ms`, default 15 000) plus a parent-side liveness window
  (`:stream_timeout_ms`, default 60 000; must exceed the heartbeat period or start-up fails
  closed), so a silent half-open partition can no longer hang the pipeline: the timeout emits
  `[:capstan, :connection, :stream_timeout]`, kills the blocked reader, and reconnects; a
  persistent partition halts as `:stream_stalled`. TCP `keepalive` is the OS-level backstop.

### Scope (ADR-0004)

C1 ships **lib-owned checkpoint mode** and **resume-from-durable-checkpoint**
(`start_position: :checkpoint`) only. Sink-owned checkpoint mode and explicit start positions
(`%Capstan.Position{}` override, `:current`) are deferred and refused fail-closed
(`:sink_owned_mode_unsupported`, `:start_position_override_unsupported`,
`:start_position_current_unsupported`).

[Unreleased]: https://github.com/baselabs/capstan/commits/main

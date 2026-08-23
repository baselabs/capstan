# Changelog

All notable changes to capstan are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.1.1] - 2026-08-23

### Fixed

- The hex tarball now ships the decision record with the code it governs: `docs/adr/*`
  (0001–0011) and `docs/ROADMAP.md` join the package files. Contributor-workflow docs
  that reference repo-only files (scripts, docker-compose) remain repository-side.

## [1.1.0] - 2026-08-23

### Added

- The precondition gate gains a sixth variable: `binlog_transaction_compression` must be OFF
  (a distinct `:binlog_transaction_compression_on` refusal at connect, before the dump —
  compression is source-unilateral and capstan cannot inflate zstd payloads; ADR-0011). The
  decoder halt remains the backstop for a dynamic flip after connect.

- Telemetry measurements on both fail-closed channels: `[:capstan, :transaction, :committed]`
  carries `change_count` + `sink_ms`; `[:capstan, :connection, :established]` carries
  `establish_ms`; measurement values must be non-negative numbers (the metadata allowlist
  gates keys, the measurement gate gates values — Rule 1 closes on both channels).
- Property-based law suites (stream_data, dev-only dep) for the Gtid set algebra (7 laws,
  including an independent membership oracle at every interval boundary) and the cursor-gate
  (7 laws: the ADR-0005 gating inversion, monotonicity, the PK-changing update split).
- ADR 0007–0010: the value-free boundary (Rule 1), the pure-Elixir protocol client, fail-closed
  supervision + streaming liveness, and the exclusive end bound of
  `COM_BINLOG_DUMP_GTID`.

## [1.0.0] - 2026-08-23

First stable release. The public surface — `Capstan.start_link/1` options and
refusals, the `Sink` / `CheckpointStore` / `SnapshotStore` behaviours, the
value-free halt catalog, and the telemetry event set — is now covered by the
semver contract: additive-only changes until 2.0. C3–C6 (batching, compressed
transactions, XA tracking, the adapter) remain additive roadmap rows.

### Production hardening (since 0.3.0)

- Published source carries zero machine-local provenance markers — every
  plan/task/closeout identifier across `lib/` rewritten to semantic prose (the
  hex tarball ships `lib/` verbatim, comments included; verified zero by sweep).
- Doc truth: "Three callbacks" → four (`handle_snapshot/2` predates the
  sentence); the C1 memory shape (one transaction buffered whole in pipeline
  memory; snapshot mode adds one `chunk_size`-bounded chunk per table) now
  stated in the sink contract; fixture-capture instructions point at the
  env-driven port; ROADMAP carries no machine-local paths.
- CI tests the DECLARED Elixir floor (`~> 1.15`; a dedicated 1.15.8/OTP 26 leg)
  and enforces the coverage threshold on every push (90.91% over the shipped
  library, test-support harness excluded from the denominator); actions on the
  node-24 runtime.
- The live exclusive-end resume tripwire is multi-uuid-correct: the checkpoint
  is modeled as a real processed watermark, so the tripwire survives a
  long-lived substrate whose `gtid_executed` carries fabricated foreign-uuid
  sets (some already in `gtid_purged`) — where a single-uuid checkpoint is
  refused 1236, the exact `:data_gap` semantics the library implements.
- The whole project compiles warning-free on Elixir 1.20 (was 11 test-compile
  warnings); 629 tests green in the combined run (unit + integration + live +
  docker-gated) against live MySQL 8.0 and 8.4.

## [0.3.0] - 2026-08-22

### Changed

- **Strict option surface**: an option key outside the documented set — at the top
  level, in `connection:`, in `snapshot:`, or in a store block — is refused with the
  new `:unknown_option` instead of silently defaulting (a misspelled key such as
  `stream_timeout:` for `stream_timeout_ms:` is loud, never ignored). `child_spec`'s
  `:id` is accepted. Configs passing extra keys were previously tolerated and now
  refuse — the reason this is a minor bump.
- Align the contributor guide and README with the released 0.2.0 surface:
  C1 streaming and C2 initial snapshot are complete; C3–C6 and the named C2
  follow-ups remain open. Register the existing XA tracking ADR 0006 in ExDoc.
- Compile warning-clean on Elixir 1.20 (pin operators in bitstring `size(...)`, per
  the 1.20 hard deprecation); the supported range is unchanged — the pinned form is
  valid on the declared floor (`~> 1.15`).

### Fixed

- Thread the documented streaming-liveness options (`reconnect_backoff`,
  `heartbeat_period_ms`, `stream_timeout_ms`) through `Capstan.start_link/1`: they
  were silently ignored (defaults always applied) and the promised
  `:invalid_liveness_config` refusal for `stream_timeout_ms <= heartbeat_period_ms`
  was unreachable through the public entry point. The refusal now fires at config
  time, before any socket opens, and valid overrides reach the connection.
- Bound the liveness/backoff values to the schedulable `Process.send_after` ceiling
  (2^32-1 ms) and gave `Capstan.Connection`'s direct-wiring start the same value-shape
  refusal the public path enforces — an over-ceiling, non-positive, or non-integer
  value is a value-free refusal (`:config_invalid` / `:invalid_liveness_config`),
  never a later timer crash or a silently disabled master heartbeat.
- README install snippet pins `~> 0.3.0` (the 0.2.0 tarball shipped `~> 0.1.0`, which
  cannot resolve the release the same README documents).

## [0.2.0] - 2026-07-22

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
  `:snapshot_config_drifted` (the configured `snapshot.tables` no longer match a durable
  `:complete`/mid-snapshot state — a table added or removed after a state exists halts rather than
  silently never backfilling it), the `SnapshotStore` faults, and `{:snapshot_sink_error, _}`. A
  `tables: :all` snapshot is refused `:config_invalid` — an explicit table list is required (ROADMAP
  C2b).
- **Delivery guarantee** — strict-once in normal operation; the only duplicate window is a crash
  between a chunk's `{:ok}` and the durable `pk_cursor` persist (the one in-flight chunk re-emits,
  C1's bounded posture; an upsert-by-PK sink converges). A second durable high-water,
  `delivered_pk`, is persisted BEFORE each chunk emit and backstops a DELETE that lands in that
  same window: on restart the cursor-gate forwards a streamed delete of an already-delivered key
  (`k ≤ delivered_pk`) so the row is swept, never left as a phantom. Effect-once across the crash
  window is the deferred sink-owned path (ROADMAP C1a).

### Docs & tooling

- A test-environment guide (`docs/testing.md`) and a runnable minimal example consumer
  (`examples/print_consumer.exs`). ADR-0005 now ships in the generated documentation.
- Removed a reference to a private repository from the README and ADR-0001 (the downstream-consumer
  example is now stated generically).

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

[Unreleased]: https://github.com/baselabs/capstan/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/baselabs/capstan/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/baselabs/capstan/releases/tag/v0.1.0

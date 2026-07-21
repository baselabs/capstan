# Changelog

All notable changes to capstan are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

### Scope (ADR-0004)

C1 ships **lib-owned checkpoint mode** and **resume-from-durable-checkpoint**
(`start_position: :checkpoint`) only. Sink-owned checkpoint mode and explicit start positions
(`%Capstan.Position{}` override, `:current`) are deferred and refused fail-closed
(`:sink_owned_mode_unsupported`, `:start_position_override_unsupported`,
`:start_position_current_unsupported`).

[Unreleased]: https://github.com/baselabs/capstan/commits/main

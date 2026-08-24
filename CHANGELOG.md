# Changelog

All notable changes to capstan are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.2.2] - 2026-08-24

### Changed — docs point at the runnable reference stack

- README: "Where to go next" leads with the durable reference pipeline
  (`examples/replication_pipeline` — one `docker compose up`: MySQL source →
  capstan release container → MySQL destination, with value-free receipts, an
  idempotent upsert mirror, and a durable GTID checkpoint), and the
  30-second-pipeline note names it alongside the Livebook and print consumer.
  The reference stack itself is repo-side (not in this tarball); this release
  carries the pointers to it.

### Added — `examples/replication_pipeline`: the durable reference stack (docker)

- One `docker compose up` runs the complete production shape: MySQL source
  (ROW binlog + GTID, the five fail-closed preconditions) → capstan as an OTP
  release in one container → MySQL destination, carrying the three reference
  artifacts — a **value-free receipts ledger** (structure only, capstan Rule 1
  upheld at the destination), an **idempotent upsert mirror** (at-least-once
  delivery + idempotent apply = exactly-once effects), and a **durable
  GTID-set checkpoint** in the destination database, seeded on first boot from
  the source's `gtid_executed` (why: an empty checkpoint would request the
  server's full retained history, which a purged server refuses).
- A new `reference-example` CI job builds the stack and drives the README
  sequence end-to-end — insert → mirror + receipts + checkpoint assertions →
  pipeline restart → resume with no duplicate re-delivery. The example is the
  public sink API's out-of-repo canary: breaking the consumer surface goes red
  here, not in a downstream project. Repo-side only — the hex package is
  unchanged.

## [1.2.1] - 2026-08-23

### Fixed — the last zstd conformance holes + CI floor-leg hygiene (no library change)

- The zstd conformance suite now reaches **100% of `Capstan.Zstd`** (was ~94%):
  a test-side encoder for the sequences section (the exact inverse of the
  production reverse bit-reader, with the RFC 8878 default baselines/extras
  transcribed first-hand) crafts VALID frames for the decoder arms the MySQL
  corpus and the reference CLI never emit — RLE and Repeat_Mode sequence
  tables, the 3-byte sequence-count form (n ≥ 0x7F00 in one block), the
  state-advance bitstream exhaustion, both `ll==0` repeat-offset arms, and a
  corrupt/truncated 4-stream jump table over a direct-weight Huffman tree.
  Each valid frame asserts byte-exact output through the production decoder.
- The batching marquee's loop-value assert was vacuous — the pattern re-bound
  the loop variable instead of pinning it, so delivered values were never
  checked against the expected ones. Pinned: each of the three delivered
  transactions is now asserted to carry its expected value, in order.
- CI's Elixir 1.15 floor leg is warning-clean: dead test helpers removed
  (`raw_socket_element/1`, the vestigial sink copies inside the sink-owned
  test module, `@dual_z`), an unused `ctx` underscored, and the predefined
  offset-table length tripwire actually asserted (it was defined but unused).

## [1.2.0] - 2026-08-23

### Fixed — decoder robustness (found by the release gate's coverage battery)

- A compressed block whose sequence-count bytes were truncated **crashed** the
  zstd decoder (`FunctionClauseError` in the mode read — the count's error
  tuple matched the `with`'s success pattern) instead of failing closed. A
  malformed count now refuses `{:error, :truncated_frame}` like every other
  corruption class (proven RED-first by the two crashing tests, now tripwires).
- The zstd conformance suite gains the decoder arms the MySQL-captured corpus
  cannot fire (repetitive SQL never produces them): a reference-encoder
  `zstd_text/` fixture exercising **4-stream Huffman literals** and the large
  literals size formats, a full truncation sweep over real frames, and crafted
  refusal arms per corruption class (reserved block type, block-size bound,
  lying Frame_Content_Size, skippable frames, treeless/FSE-table misuse). The
  TRANSACTION_PAYLOAD TLV-walk refusals, the `:current` start-position
  read-failure arms, and `Capstan.Error`'s pass-through contract get named
  tests. Coverage over the full exercised surface: 88.62% → 91.49%.

### Added — C2a collation-ordered string primary keys (ADR-0012)

- `CHAR`/`VARCHAR` primary keys (any charset, any collation, alone or in composites
  and the unique-key fallback) are now snapshot-eligible. A string PK's cursor-gate
  comparison rides its collation WEIGHT BYTES computed by the SERVER on both sides —
  the chunk read selects `WEIGHT_STRING(pk)` alongside each row, and streamed raws
  resolve through a COLLATE-pinned `CONVERT(X'..' USING charset)` introducer over the
  binlog's column bytes — so `k ≤ cursor` provably reproduces the source's collation
  order (probe-proven for `ai_ci`, both `_bin` weight forms, PAD SPACE over distinct
  keys, multi-level `as_cs`, and composites). Previously the whole string family was
  refused `:snapshot_pk_unsupported_type`, including the MySQL 8 default collation.
- The durable cursor for a string-PK table is dual-formed (`raw` column bytes +
  `weight`); on resume the weight half is RECOMPUTED from the raw half, so both sides
  of every comparison always come from the same server (`WEIGHT_STRING()`'s byte form
  is version-mutable by documentation).
- Chunk rows deliver a string PK column as its RAW COLUMN BYTES (`CAST(col AS
  BINARY)`) — byte-identical to the previous text-protocol form for `utf8mb4` (the
  connection charset), and identical to what the binlog delivers for the same column
  on every other charset.
- Still refused, now on measured grounds: the TEXT family (a prefix PK pays a
  per-chunk filesort — superlinear backfill) and `ENUM`/`SET` (member-position order
  no introducer weight path reproduces).
- New halt reason `:snapshot_pk_weight_failed` (budgeted, then fail-closed) when
  weight resolution fails on the source.
- Known cost, by design: during a string-PK table's backfill window, a streamed
  transaction carrying a NEW string key pays ≥1 weight-resolution round trip
  (repeated keys hit a bounded cache); composite row-value pagination remains a full
  index scan per page (pre-existing for composites of any type).
- Derisk: a bootstrap order-contract canary runs once per string PK column at snapshot
  open (bootstrap and resume) — a source whose `WEIGHT_STRING` byte order disagrees
  with its `ORDER BY` order over the canary vector refuses
  `:snapshot_collation_contract_violated` before any chunk or gate decision. This
  converts the residual "a future server could change the order contract silently"
  into a loud, value-free halt.

### Changed — presentation overhaul (docs as product)

- README rewritten as a user document: what it is, install, a 30-second
  pipeline, the three position-ownership modes, snapshot/backfill, the
  fail-closed posture, and a when-to-use table — the internal status-report
  shape is gone (probe/roadmap material stays in the repo docs where
  contributors need it).
- New `docs/telemetry.md`: every telemetry event with its measurements and
  metadata tables plus attach examples and a standard telemetry_metrics set.
- New `docs/recipes.md`: idempotent sinks, batched warehouse loads,
  snapshot-then-stream migration, start-from-now, TLS against self-signed
  certificates, XA sources, compressed sources, production watch/alerting.
- ExDoc: recipes + telemetry as grouped "Guides" extras; the getting-started
  Livebook's dependency pin bumped to the released line.

### Added — C2c zero-row snapshot completion signal

- A configured snapshot table with ZERO pre-existing rows now delivers exactly
  one snapshot beat — an empty `final_chunk?: true` chunk — instead of silently
  taking the done path with no `handle_snapshot/2` call: a sink gating
  per-table readiness on `final_chunk?` no longer waits forever on empty
  tables. Mid-table drained cursors (the empty look-ahead after non-empty
  pages) are unchanged. RED-first unit tripwire + live marquee (the C2b :all
  set includes a zero-row table; its single beat is the empty final chunk).

### Added — C2b `tables: :all` snapshot resolution

- An `:all` snapshot set (which arises when the capture allowlist is itself
  `:all`) now RESOLVES to a concrete, scoped list instead of refusing
  `:config_invalid`: `Capstan.Snapshot.Tables` enumerates
  `information_schema.TABLES` filtered to `BASE TABLE` outside the four system
  schemas, ordered deterministically. Views/system schemas/temporary tables
  are excluded by construction; an empty result refuses
  `:snapshot_no_base_tables` (never a silent empty backfill). The durable
  snapshot state binds the resolved list, and a configured `:all` always
  reconciles on resume (the stored set is the authority). A concrete capture
  allowlist with an `:all` snapshot is still refused (the `⊆ captured` proof
  is impossible). Live marquee: a dedicated schema with two base tables + a
  view backfills EXACTLY the two base tables.

### Added — C4b compressed-transaction consumption (ADR-0011 consume arm)

- `binlog_transaction_compression=ON` sources are now CONSUMED: `Capstan.Zstd` — a
  pure-Elixir zstd frame decompressor (RFC 8878: literals incl. 4-stream Huffman and
  FSE-compressed weight tables, sequences incl. predefined/RLE/FSE/repeat modes,
  overlap-safe match execution with repeat-offset tracking, multi-block frames,
  XXH64 content-checksum verification) — inflates each `TRANSACTION_PAYLOAD` event,
  and `Capstan.Binlog.TransactionPayload` splits the inner event stream (no inner CRC
  trailers; the outer event's CRC32 covers the compressed payload) into the events the
  assembler folds exactly as bare ones. Byte-exact conformance: live-captured frames
  from a compression-ON MySQL 8.0 substrate, each asserted equal to the same frame
  inflated by the reference `zstd` binary; RFC 8878 Appendix A table crosschecks;
  fail-closed tripwires (tampered bytes, bad magic, reserved bits, dictionaries,
  checksum mismatch, non-ZSTD compression type, uncompressed-size mismatch).
- The `binlog_transaction_compression=OFF` precondition is REMOVED (five gate variables
  remain; ADR-0011 amended) — an ON source no longer refuses
  `:binlog_transaction_compression_on` at connect. Live marquee: a pipeline against a
  compression-ON container delivers inflated transactions with full row values.

### Added — C4a column-type breadth: SET decode + spatial passthrough

- `SET` columns decode to MySQL's text form (selected members comma-joined; empty set is
  `""`); a bitmap naming an undeclared member is a metadata desync and halts
  (`:set_member_out_of_range`). `GEOMETRY` and the spatial family deliver the raw
  SRID+WKB binary verbatim — interpretation is the sink's. Pre-5.6 temporals stay refused
  (a supported 8.0+ source never emits them). Real-byte conformance: live-captured
  `spatial` fixture (POINT columns byte-exact) plus the existing `set_type` fixture now
  decoding `"a,c"`; a live marquee streams every SET form + geometry through a real
  pipeline.

### Added — C3 batching

- `batch: [max_transactions:, flush_ms:, mode:]` — `:lib_owned` (per-transaction
  delivery, batched durable checkpoint writes: one write of the batch's newest position
  at the bound or deadline) and `:sink_owned` (one atomic `handle_batch/2` per batch —
  units + final position together; required in that mode, missing ⇒
  `:sink_missing_handle_batch`). The crash-replay window widens to at most the un-flushed
  batch tail, bounded by `max_transactions`; a quiet stream never holds the checkpoint
  past `flush_ms`. Live-proven: bound-flush, atomic sink-batch delivery, and
  deadline-flush marquees.

### Added — C1b explicit start positions

- `start_position:` now accepts an explicit `%Capstan.Position{}` (the dump AND the
  assembler's dedup watermark seed from it — they can never disagree; the covered
  transactions are not re-delivered) and `:current` (the server's live
  `@@gtid_executed` read once pre-dump against the pipeline's own connection
  coordinates — "start from now" without pre-seeding). A never-written checkpoint
  store no longer clobbers the injected start with an empty set (which dumped full
  history into `:data_gap` on a purged source). `:start_position_override_unsupported`
  / `:start_position_current_unsupported` are retired. Live-proven: override-skips +
  delivers-only-the-tail, and :current-starts-from-now marquees.

### Added — C1a sink-owned checkpoint mode (ADR-0004's deferred arm)

- Omit `checkpoint_store:` and implement `c:Capstan.Sink.checkpoint/0`: the sink
  persists its data and the delivered position ATOMICALLY together and returns the
  position; capstan resumes from `checkpoint/0`, and the post-delivery advance is
  in-memory only (no store write — a crash between delivery and checkpoint is impossible
  by construction). Effect-once across kill/restart — zero replays, zero skips, the
  restart resumes at the checkpoint's successor — proven live on an append-only ledger.
  The connection's dump resumes from the sink's checkpoint (an empty-set dump against a
  purged source would halt `:data_gap` — the mode's position authority is the sink).
  `:sink_owned_mode_unsupported` is retired.

### Added — C5 XA transaction tracking (ADR-0006)

- `xa: :track` (default `:refuse`): two-phase XA transactions deliver **exactly once at
  the resolution** under the held-out watermark — the prepare GTID enters the durable
  checkpoint only in the same single write as its resolver. `XA ROLLBACK` delivers zero
  rows. Prepared state pools in memory bounded by `max_prepared_transactions` (default
  10 000, halt `:xa_prepared_pool_exhausted` — never evicted). Pre-start dangling
  prepares (connect-time `XA RECOVER` enumeration) resolve row-lessly; unknown
  resolutions halt `:xa_commit_without_prepare` / `:xa_rollback_without_prepare`.
  One-phase XA commits immediately. The decoder's type-38 halt moved to the fold's
  policy layer; the default posture is byte-for-byte unchanged. XID bytes are pooled
  under a sha256 digest and never logged, emitted, or telemetered (Rule 1). The source
  account needs `XA_RECOVER_ADMIN` (granted by the seed script). Live-proven: 5
  marquees on MySQL 8.0 plus real-byte fixture conformance and the XID-encoding digest
  equivalence (type-38 body vs the resolution SQL's hex literal, same session).

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

# capstan usage rules

capstan streams committed MySQL row changes from the binary log to a **sink** in your
supervision tree. This file is the consumer-facing contract; the permanent decision record is in
`docs/adr/`.

> **Checkpoint modes:** **lib-owned** (default — capstan persists the processed GTID
> set) and **sink-owned** (C1a — the sink persists its data and the position atomically
> together; see "Sink-owned checkpoint mode"). Explicit start positions remain a named
> follow-up (C1b).

## Starting a pipeline

Configure each pipeline explicitly at `Capstan.start_link/1` — there is no global mutable state.

```elixir
Capstan.start_link(
  connection: [
    host: "replica.internal",
    port: 3306,
    username: "capstan",
    password: "…",
    database: "orders",
    ssl: true,                                  # default true (ADR-0002)
    ssl_opts: [cacertfile: "/etc/ssl/mysql-ca.pem", server_name_indication: :disable],
    auth_plugins: [:caching_sha2_password]      # default; name :mysql_native_password to allow it
  ],
  server_id: 1001,                              # replica identity; MUST be unique in the topology
  sink: MyApp.OrdersSink,                       # implements Capstan.Sink
  checkpoint_store: [module: MyApp.Store, options: []],   # lib-owned mode; see below
  start_position: :checkpoint,                  # :checkpoint (default) | %Position{} | :current
  tables: [{"orders", "line_items"}],           # or :all (default); filter applied before decode
  max_command_retries: 5,                       # default 5; pre-establish failures only
  xa: :track,                                   # default :refuse; see "XA transactions"
  batch: [max_transactions: 1000, flush_ms: 500],  # optional; see "Batching"
  max_prepared_transactions: 10_000,            # the :track prepared-pool bound
  reconnect_backoff: 1_000,                     # default; ms between reconnect attempts
  heartbeat_period_ms: 15_000,                  # default; server heartbeat on a quiet stream
  stream_timeout_ms: 60_000                     # default; MUST be > heartbeat_period_ms
)
```

- Returns `{:ok, supervisor_pid}`, or `{:error, reason}` where `reason` is a value-free atom.
  A bad substrate or config is refused **before** any socket opens. An option key outside
  the documented surface — a misspelled key (`stream_timeout:` for `stream_timeout_ms:`) at
  the top level, in `connection:`, in `snapshot:`, or in a store block — is refused
  `:unknown_option` rather than silently defaulted.
- Embed it with the child spec: `{Capstan, connection: [...], server_id: ..., sink: ...,
  checkpoint_store: [...]}`.

### TLS (ADR-0002)

`ssl` defaults **true**. With TLS on you MUST make an explicit peer-verification choice, or start
fails closed with `:tls_verification_unspecified`:

- authenticated: pass `cacertfile:` (or `cacerts:`). MySQL's auto-generated cert is self-signed
  with no SAN, so also pass `server_name_indication: :disable` (VERIFY_CA — chain verified,
  hostname not); or
- unauthenticated confidentiality only: pass `verify: :verify_none` explicitly; or
- opt out entirely with `ssl: false`.

### Streaming liveness

A silent half-open partition cannot hang the pipeline: capstan asks the server for a
replication heartbeat every `heartbeat_period_ms` (default 15 000) of stream idleness, and a
liveness timer declares the stream dead when **no** frame — event or heartbeat — arrives
within `stream_timeout_ms` (default 60 000, tolerating three missed heartbeats). On fire it
emits `[:capstan, :connection, :stream_timeout]`, drops the connection, and reconnects; a
persistent partition halts `:stream_stalled`. `stream_timeout_ms` MUST exceed
`heartbeat_period_ms`, or the pipeline refuses to start (`:invalid_liveness_config`) — a
window at or below the heartbeat interval would false-drop a healthy idle stream. All three
values must be positive integers within the schedulable timer ceiling (2^32−1 ms ≈ 49.7
days); anything else is refused `:config_invalid` before any socket opens.

### Start positions

`start_position:` accepts three forms. **`:checkpoint`** (default) resumes from the
position authority — the checkpoint store (lib-owned) or your `checkpoint/0`
(sink-owned). An **explicit `%Capstan.Position{}`** resumes BOTH the dump and the
dedup watermark from that position — you assert it is a safe resume point; transactions
it covers are not re-delivered, everything after it streams. **`:current`** reads the
server's live `@@global.gtid_executed` once, pre-dump, and resumes from it — "start
from now" without pre-seeding.

### First start — an empty checkpoint requests FULL retained history

A never-written store sends the binlog dump an **empty GTID set**, which means "everything
the server still retains". Two consequences:

- On a server that has already purged its earliest logs (`@@global.gtid_purged` non-empty),
  the server refuses the dump and the pipeline halts `:data_gap`.
- On a server with complete retained history, the sink replays **all of it** from the top.

To start "from now" instead, **seed the checkpoint store** before first start with the
server's current executed set — the store is the position authority, so a pre-seeded value
is simply resumed from:

```elixir
# 1. Read the server's current position with any MySQL client:
#      SELECT @@global.gtid_executed;
# 2. Write that string into your store before starting the pipeline:
:ok = MyApp.Store.write(store, gtid_executed)
# Capstan.start_link/1 now resumes from it — nothing before it is delivered.
```

### Value-free start-up refusals

`Capstan.start_link/1` returns `{:error, reason}` with one of:
`:server_id_required`, `:config_invalid`, `:tls_verification_unspecified`,
`:invalid_liveness_config`, `:unknown_option`, `:invalid_sink`,
`:sink_missing_handle_transaction`, `:sink_missing_checkpoint`,
`:sink_missing_handle_schema_change`, `:checkpoint_store_required`, `:sink_owned_mode_unsupported`,
`:start_position_override_unsupported`, `:start_position_current_unsupported`. In snapshot mode
also: `:sink_missing_handle_snapshot`, `:snapshot_table_not_captured`, and `:config_invalid` for a
mis-shaped `snapshot:` block or a `tables: :all` snapshot set (see Initial snapshot). No row value,
column value, or password ever appears in an error, log line, or telemetry payload (Rule 1).

## The `Capstan.Sink` behaviour

Four callbacks; every one is `@optional_callbacks` — which are required depends on the mode.
For C1's lib-owned mode you implement `handle_transaction/1` and `handle_schema_change/2`.

```elixir
@callback handle_transaction(Capstan.Transaction.t()) ::
            {:ok, Capstan.Position.t()} | {:error, term()}

@callback handle_schema_change(Capstan.SchemaChange.t(), Capstan.Position.t()) ::
            :ok | {:error, term()}

@callback checkpoint() :: {:ok, Capstan.Position.t() | nil} | {:error, term()}
# checkpoint/0 is consulted only in the (deferred) sink-owned mode.
```

Three load-bearing rules — each guards a silent-loss class the type signature alone does not:

1. **`changes` is `Enumerable.t()`, not a list. Never call `length/1` / `Enum.count/1` on it.**
   C1 delivers a list, but the type is the day-one contract: a later row makes `changes` a lazy,
   single-pass, disk-backed enumerable valid only during the delivery call. Enumerate exactly
   once, streaming (`Enum.reduce/3`, `Enum.each/2`, `for`, or a single-pass `Stream`).

2. **Dedup with `Capstan.Gtid.member?/2` — never an ordinal comparison.** A GTID set is
   legitimately non-contiguous (failover, purge, multi-source), so `commit_lsn <= checkpoint`-style
   comparison silently skips or re-applies. Skip a transaction when
   `Capstan.Gtid.member?(checkpoint_set, {uuid, gno})` is true.

3. **The checkpoint is a PROCESSED watermark, not a delivery log.** It records every committed
   GTID processed — delivered *and* filtered. It does not mean "delivered".

`handle_schema_change/2` receives only structured `schema`/`table`/`kind` — the raw DDL statement
text is redacted before it reaches the sink (Rule 1). Return `{:error, term()}` from either
delivery callback to halt the pipeline fail-closed **without** advancing the checkpoint.

### Batching

`batch: [max_transactions: n, flush_ms: ms, mode: :lib_owned | :sink_owned]` batches the
DURABLE side (absent — no batching; per-transaction delivery and checkpoint,
byte-for-byte the C1 behavior).

- **`:lib_owned`** (default): delivery stays per-transaction (`handle_transaction/1`
  unchanged); only the durable CHECKPOINT write batches — one write of the batch's
  newest position at the bound (`max_transactions`) or the deadline (`flush_ms`),
  whichever first. The crash-replay window widens from one transaction to at most the
  un-flushed batch tail; dedup on restart already covers replay (set membership).
- **`:sink_owned`**: the DELIVERY itself batches — your sink receives ONE
  `c:Capstan.Sink.handle_batch/2` carrying the batch's transactions plus its final
  position, and must apply both **atomically together** (the batch effect-once contract;
  required callback in this mode, `{:error, _}` halts before the checkpoint side).

The batch closes IMMEDIATELY (never across) on any halt and on snapshot coordination.

### XA transactions (ADR-0006)

Two-phase XA is opt-in: `xa: :track` (default `:refuse` halts
`:unsupported_transaction_shape` at the prepare, the original posture). Under `:track` a
prepared transaction's rows are pooled in memory (bounded by `max_prepared_transactions`,
default 10 000 — exceeding halts `:xa_prepared_pool_exhausted`, never evicted) and
delivered **exactly once at the resolution**: `XA COMMIT` delivers the pooled rows as one
transaction; `XA ROLLBACK` delivers zero rows (both advance the watermark over the prepare
AND resolution GTIDs in a single write — a prepare never checkpoints alone, the
crash-safe held-out watermark). A prepare that predates the pipeline resolves row-lessly
(capstan enumerates `XA RECOVER` at every connect). `XA COMMIT`/`ROLLBACK` with no known
prepare halts `:xa_commit_without_prepare` / `:xa_rollback_without_prepare`. The XID bytes
are application data (Rule 1): pooled under a sha256 key, never logged, emitted, or
telemetered — errors correlate on GTIDs. One-phase XA commits immediately. **The source
account needs `XA_RECOVER_ADMIN`** for the connect-time enumeration (the preflight
checks it). During an initial snapshot, a prepared XA on a snapshot table blocks the
brief per-chunk lock (its row locks serialize against `LOCK TABLES … READ`), so a chunk
is never captured over an unresolved XA.

## Sink-owned checkpoint mode

Omit `checkpoint_store:` and implement `c:Capstan.Sink.checkpoint/0` — the sink IS the
checkpoint. `handle_transaction/1` must persist its data AND the delivered position
**atomically together** and return the position; on (re)start capstan reads the resume
position from `checkpoint/0`, delivers, and advances in-memory after each durable sink
write. There is no store child and no separate checkpoint write — a crash between
"delivery" and "checkpoint" is impossible by construction, which is what makes the mode
**effect-once** (zero replays, zero skips across a kill/restart — the C1a acceptance
marquee proves it live on an append-only ledger). An `handle_transaction/1` that returns
`{:ok, position}` without having durably persisted that position breaks the mode's
contract silently — the durability is YOURS to provide.

### Column-type breadth (C4a)

`SET` columns decode to MySQL's text form — the selected members comma-joined (`"a,c"`;
an empty set is `""`). `GEOMETRY` (and its typed aliases: `POINT`, `LINESTRING`,
`POLYGON`, the MULTI_* and `GEOMETRYCOLLECTION` family) deliver the column's **raw
spatial binary** — the 4-byte SRID prefix plus WKB — verbatim; interpretation (WKT,
parsers) is the sink's. A SET bitmap naming a member the schema does not declare is a
metadata desync and halts `{:unsupported_column_type, reason: :set_member_out_of_range}`;
pre-5.6 temporals remain refused (a supported 8.0+ source never emits them).

**Memory shape.** A transaction is buffered whole in pipeline memory between assembly and
delivery, so peak memory scales with the source's largest single transaction; in snapshot mode
add one fully-materialized chunk per table (bounded by `chunk_size`). Streaming/batched delivery
of very large transactions is a named roadmap row (C3).

## Checkpoint store (lib-owned mode)

`Capstan.CheckpointStore` persists exactly one value per pipeline: the processed `gtid_set`
string (`file`/`pos` are diagnostic and never persisted). Implement two callbacks:

```elixir
@callback read(store()) :: {:ok, String.t() | nil} | {:error, term()}   # nil = never written
@callback write(store(), gtid_set :: String.t()) :: :ok | {:error, term()}  # idempotent
```

`Capstan.CheckpointStore.InMemory` is a process-lifetime reference implementation
(`start_link/1`, `read/1`, `write/2`) for tests and ephemeral pipelines — **not durable** across
a restart. For production, implement the two callbacks over storage you already trust (your
application database, a file written atomically) — `write/2` must be durable and idempotent.
Bridge `Capstan.Position` through `read_position/2` / `write_position/3`, which apply
the persist boundary so you never leak `file`/`pos`.

Wiring: `checkpoint_store: [module: impl, options: keyword()]` — the pipeline supervises the
store, calling `impl.start_link(options)` (`options` defaults to `[]`). A store **fault** at
read or write is retried up to 5 times (not configurable in C1; `:config_invalid` is treated
as permanent and halts immediately), then the pipeline halts fail-closed —
`{:checkpoint_read_failed, reason}` / `{:checkpoint_write_failed, reason}`. This budget is
separate from `max_command_retries`, which governs only pre-establish connection failures.

## Initial snapshot (C2)

Backfill the rows that pre-exist the pipeline's start, woven into the running stream so there is
**no gap and no duplicate** at the snapshot→stream handoff (ADR-0005). Absent the `:snapshot`
block, a pipeline is pure C1 — this is purely additive.

```elixir
Capstan.start_link(
  connection: [...], server_id: 1001, sink: MyApp.Sink,
  checkpoint_store: [module: MyApp.GtidStore],
  tables: [{"app", "orders"}],                 # capture allowlist (must be a concrete list to snapshot :all)
  snapshot: [
    tables: [{"app", "orders"}],               # ⊆ captured; DEFAULTS to the captured list
    store:  [module: MyApp.SnapshotStore],      # durable snapshot progress (SEPARATE from the gtid store)
    chunk_size: 4096                            # rows per chunk (bounds the brief lock's hold + memory)
  ]
)
```

**Source privilege.** The query account needs **`LOCK TABLES`** on the snapshot tables (in addition
to `SELECT`) — capstan takes a brief per-chunk `LOCK TABLES … READ` to capture an exact GTID
position (the read-only-replica-safe substitute for a written watermark; ADR-0005). No `RELOAD` /
FTWRL. The preflight (`scripts/capstan-preflight.sql`) checks it.

**The sink implements `handle_snapshot/2`** (optional in general; **required** in snapshot mode):

```elixir
@callback handle_snapshot([Capstan.Change.t()], Capstan.Snapshot.Meta.t()) :: :ok | {:error, term()}
```

- The first argument is a **concrete list** of `%Capstan.Change{op: :snapshot}` (`record` = the full
  row, `old_record` = `nil`) — a bounded, fully-materialized chunk, NOT the lazy `Enumerable.t()` of
  `handle_transaction/1`. `Capstan.Snapshot.Meta` carries only value-free structural identity
  (`schema`/`table`/`chunk_seq`/`g`/`final_chunk?`).
- **Apply each chunk row as an UPSERT keyed on its primary key.** This is a HARD precondition: a
  snapshot row may be superseded by a later streamed change, and the bounded crash-window re-emit
  relies on upsert-by-PK to converge. Return `{:error, term()}` to halt fail-closed.

**Guarantees.** Strict-once in normal operation. The only duplicate window is a crash between the
chunk's `{:ok}` and the durable `pk_cursor` persist — the one in-flight chunk re-emits, exactly C1's
bounded posture; an upsert-by-PK sink converges. A DELETE landing in that same window is swept, not
left as a phantom: a `delivered_pk` high-water (persisted before each emit) lets the cursor-gate
forward a streamed delete of an already-delivered key on restart. **Resumable mid-backfill**: the
per-table PK cursor lives in `MyApp.SnapshotStore` (implement `read/1`+`write/2` over a
`%Capstan.Snapshot.State{}`, same durable/idempotent contract as the checkpoint store), and the
backfill resumes with `WHERE pk > cursor` — never a re-scan from zero. A store whose
`status: :complete` never re-snapshots.

**The snapshot table set is fixed once a durable state exists.** Adding or removing a table in
`snapshot.tables` after a `:complete`/mid-snapshot state is a config/state divergence and halts
`:snapshot_config_drifted` — a newly-added table would otherwise be silently never backfilled. To
re-backfill a changed set, drop the durable snapshot state (a fresh start re-introspects config).

**Supported primary keys (order-faithful only).** Chunking pages by MySQL `ORDER BY pk` while the
gate compares in Elixir, so the PK type's Elixir order must provably match MySQL's: **integer**
(signed/unsigned, incl. `BIGINT UNSIGNED`), **`BINARY`/`VARBINARY`**, and **composites** of those.
A collation-ordered STRING PK (`CHAR`/`VARCHAR`/`TEXT`) is refused `:snapshot_pk_unsupported_type`
(a named follow-up, ROADMAP C2a). A `tables: :all` snapshot — which arises when the capture
allowlist is itself `:all` — is refused `:config_invalid`: **specify an explicit snapshot table
list** ("snapshot every table on the server" is ambiguous and dangerous; ROADMAP C2b).

## Runtime halts (fail-closed)

Any condition that could otherwise lose or corrupt data silently **halts the pipeline**: the
processes exit `{:shutdown, {:halt, reason}}` and — because every child is
`restart: :temporary` — the pipeline is **not** restarted. Your application observes the halt
via `[:capstan, :connection, :halt]` / `[:capstan, :assembler, :halt]` telemetry (metadata
`reason`) and decides whether to restart, reprovision, or page. Every reason is value-free.

**Server preconditions** (checked at every connect; a violation halts without retrying):
`:binlog_format_not_row`, `:binlog_row_image_not_full`, `:binlog_row_metadata_not_full`,
`:binlog_row_value_options_not_empty`, `:gtid_mode_not_on`, `:precondition_query_failed`.

**Compressed transactions are consumed.** `binlog_transaction_compression=ON` sources stream
normally: each `TRANSACTION_PAYLOAD` event is inflated by capstan's in-library pure-Elixir
zstd decoder (RFC 8878; byte-exact conformance against reference-inflated frames) and its
inner events fold through the assembler exactly as bare ones — the sink sees the transactions
an uncompressed source would deliver. GTID/control/non-transactional events still arrive bare
on such sources (a mix, by construction). A malformed payload — a non-ZSTD compression type,
any zstd corruption signal, an uncompressed-size mismatch, a malformed inner header — halts
with a value-free reason (`{:payload_header, _}` / `{:payload_inflate, _}` /
`{:payload_inner, _}`); nothing is ever partially decoded.

**Connection** halts:

- `:data_gap` — the server purged binlogs the pipeline still needs (also the fresh-start
  case above). Re-seed or reprovision; resuming would silently skip transactions.
- `:source_identity_mismatch` — the checkpoint carries GTIDs this server never executed;
  almost always a checkpoint pointed at the wrong server.
- `:server_id_conflict` — another replica with the same `server_id` is attached.
- `:stream_stalled` — a persistent network partition outlived the liveness reconnect budget.
- `:command_retries_exhausted` — pre-establish failures (connect/auth/query) exceeded
  `max_command_retries`.
- `:invalid_liveness_config` — `stream_timeout_ms <= heartbeat_period_ms`.
- `:checksum_negotiation_failed`, `:unrecognized_dump_error`, `{:dump_failed, code}`,
  `:unexpected_stream_packet`, `:receiver_down` — protocol-level refusals; the reason names
  the site.

**Delivery** halts (tagged tuples; telemetry reports the outer atom):

- `{:sink_error, reason}` — your sink returned `{:error, reason}`; the checkpoint is held,
  so the transaction is re-delivered after a restart (at-least-once).
- `{:checkpoint_read_failed, reason}` / `{:checkpoint_write_failed, reason}` — store fault
  after the retry budget.
- `{:assembler_error, reason}` — a stream desync (`:begin_without_gtid`,
  `:rows_without_transaction`, …) or a row the pipeline refuses to decode:
  `:unmapped_table_id`, `{:unsupported_column_type, detail}` (a column outside the decoded breadth, e.g. pre-5.6 temporals), a compressed transaction payload, an unknown event type.
- `:unsupported_transaction_shape` — an XA transaction under the default `xa: :refuse`
  (the buffered rows are discarded, never delivered; ADR-0003). With `xa: :track`
  (ADR-0006) XA no longer halts here — see "XA transactions" for the `:xa_*` halts
  (pool exhaustion, prepare-GTID mismatch, resolution without prepare).
- `{:event_parse_failed, reason}` — CRC mismatch or truncated event.
- `{:event_decode_crashed, %Capstan.Error{}}` / `{:event_processing_crashed, %Capstan.Error{}}`
  — an unexpected raise, captured value-free.

**Snapshot** halts (snapshot mode; each a distinct value-free reason per silent-loss condition):

- `:snapshot_table_no_primary_key` — a snapshot table has no usable key (no PK and no `NOT
  NULL`-complete unique key).
- `:snapshot_pk_unsupported_type` — a PK column outside the order-faithful allowlist (a
  collation-ordered string, `DECIMAL`/`DOUBLE`/temporal).
- `:snapshot_lock_unavailable` — the brief `LOCK TABLES … READ` failed (missing `LOCK TABLES`
  privilege, or a lock-wait timeout) beyond the retry budget.
- `:snapshot_schema_drifted` — a snapshot table's structure changed mid-backfill (a per-chunk
  fingerprint change, or an observed DDL on the table).
- `:snapshot_source_mismatch` — the query connection's `@@server_uuid` differs from the stream's
  (a reconnect to a different replica, at connect or on any query-conn reconnect).
- `:snapshot_config_drifted` — the configured `snapshot.tables` no longer match the durable
  `%Capstan.Snapshot.State{}`'s table set (a table added/removed after a `:complete`/mid-snapshot
  state). Halts rather than silently never backfilling the added table; drop the durable snapshot
  state to re-backfill a changed set.
- `:snapshot_chunk_read_failed` / `:snapshot_query_connect_failed` /
  `:snapshot_bootstrap_gtid_read_failed` — a chunk read, query-connect, or `P0` read fault beyond
  the budget.
- `:snapshot_state_read_failed` / `:snapshot_state_write_failed` — a `SnapshotStore` fault (budgeted,
  then halts).
- `:snapshot_coordinator_down` — the snapshot coordinator died silently (a stranded backfill is a
  gap, so it halts loud).
- `{:snapshot_sink_error, reason}` / `{:snapshot_processing_crashed, %Capstan.Error{}}` — your
  `handle_snapshot/2` returned `{:error, _}`, or the snapshot path raised (captured value-free).

## Telemetry

Both channels are fail-closed value-free and BOTH are key-allowlisted (a numeric row value —
a balance, an account number — is a non-negative number, so a type-only gate would let it
ride): metadata keys and measurement keys each pass their own allowlist, measurement values
must be non-negative numbers, and the refusals name keys only, never values.

| Event | Measurements | Metadata |
| --- | --- | --- |
| `[:capstan, :connection, :established]` | `establish_ms` | `server_version`, `tls` |
| `[:capstan, :connection, :stream_timeout]` | — | `reason` |
| `[:capstan, :connection, :halt]` | — | `reason` |
| `[:capstan, :transaction, :committed]` | `change_count`, `sink_ms` | `gtid` |
| `[:capstan, :transaction, :filtered]` | — | `gtid` |
| `[:capstan, :transaction, :skipped]` | — | `gtid`, `reason` (`:already_processed`) |
| `[:capstan, :schema_change, :received]` | — | `schema`, `table`, `kind` |
| `[:capstan, :snapshot, :started]` | `table_count` | — |
| `[:capstan, :snapshot, :chunk_completed]` | `row_count`, `chunk_seq` | `schema`, `table` |
| `[:capstan, :snapshot, :completed]` | `table_count` | — |
| `[:capstan, :snapshot, :halt]` | — | `reason` |
| `[:capstan, :assembler, :halt]` | — | `reason` |

`change_count` is computed before delivery (never by consuming the sink's single-pass
`changes` enumerable); `sink_ms` spans the sink call; `establish_ms` spans
connect-to-streaming. Commit-to-delivery lag is deliberately NOT measured: the pipeline
does not thread the binlog commit timestamp, and wall-clock skew would make a naive
difference a lie.

The only log line emitted in normal operation is a `Logger` warning when authenticating with
the deprecated `mysql_native_password` plugin.

## Substrate requirements (ADR-0002)

capstan verifies these at connect and refuses to start (a distinct reason per violation) if any
is wrong:

```
binlog_format                 = ROW
binlog_row_image              = FULL
binlog_row_metadata           = FULL
binlog_row_value_options      = ''   # PARTIAL_JSON is refused — it emits JSON diffs, not values
gtid_mode                     = ON
```

`binlog_transaction_compression` may be either OFF or ON: compression is source-unilateral
(no consumer opt-out), and capstan CONSUMES compressed transactions — the in-library
pure-Elixir zstd decoder inflates each `TRANSACTION_PAYLOAD` event (ADR-0011), so an ON
source streams normally instead of being refused before the dump.

`enforce_gtid_consistency = ON` is recommended (implied by `gtid_mode = ON`) but is **not**
separately checked. Multi-source replication is supported — a GTID set expresses multiple source
UUIDs natively. The replication account authenticates via `caching_sha2_password` by default and
needs `REPLICATION SLAVE`, `REPLICATION CLIENT`, and `SELECT`; `LOCK TABLES` for the
snapshot path; and `XA_RECOVER_ADMIN` for `xa: :track` (the connect-time `XA RECOVER`
enumeration).

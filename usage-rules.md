# capstan usage rules

capstan streams committed MySQL row changes from the binary log to a **sink** in your
supervision tree. This file is the consumer-facing contract; the permanent decision record is in
`docs/adr/`.

> **C1 scope (ADR-0004):** C1 ships **lib-owned checkpoint mode** and
> **resume-from-durable-checkpoint** (`start_position: :checkpoint`) only. Sink-owned checkpoint
> mode and explicit start positions are deferred and refused fail-closed today.

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
  start_position: :checkpoint,                  # C1: :checkpoint (default) only
  tables: [{"orders", "line_items"}],           # or :all (default); filter applied before decode
  max_command_retries: 5,                       # default 5; pre-establish failures only
  reconnect_backoff: 1_000,                     # default; ms between reconnect attempts
  heartbeat_period_ms: 15_000,                  # default; server heartbeat on a quiet stream
  stream_timeout_ms: 60_000                     # default; MUST be > heartbeat_period_ms
)
```

- Returns `{:ok, supervisor_pid}`, or `{:error, reason}` where `reason` is a value-free atom.
  A bad substrate or config is refused **before** any socket opens.
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
window at or below the heartbeat interval would false-drop a healthy idle stream.

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
`:server_id_required`, `:config_invalid`, `:tls_verification_unspecified`, `:invalid_sink`,
`:sink_missing_handle_transaction`, `:sink_missing_checkpoint`,
`:sink_missing_handle_schema_change`, `:checkpoint_store_required`, `:sink_owned_mode_unsupported`,
`:start_position_override_unsupported`, `:start_position_current_unsupported`. No row value,
column value, or password ever appears in an error, log line, or telemetry payload (Rule 1).

## The `Capstan.Sink` behaviour

Three callbacks; every one is `@optional_callbacks` — which are required depends on the mode.
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

## Runtime halts (fail-closed)

Any condition that could otherwise lose or corrupt data silently **halts the pipeline**: the
processes exit `{:shutdown, {:halt, reason}}` and — because every child is
`restart: :temporary` — the pipeline is **not** restarted. Your application observes the halt
via `[:capstan, :connection, :halt]` / `[:capstan, :assembler, :halt]` telemetry (metadata
`reason`) and decides whether to restart, reprovision, or page. Every reason is value-free.

**Server preconditions** (checked at every connect; a violation halts without retrying):
`:binlog_format_not_row`, `:binlog_row_image_not_full`, `:binlog_row_metadata_not_full`,
`:binlog_row_value_options_not_empty`, `:gtid_mode_not_on`, `:precondition_query_failed`.

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
  `:unmapped_table_id`, `{:unsupported_column_type, detail}` (spatial, `SET`, pre-5.6
  temporals), a compressed transaction payload, an unknown event type.
- `:unsupported_transaction_shape` — an XA transaction (`XA PREPARE`); the buffered rows are
  discarded, never delivered (ADR-0003).
- `{:event_parse_failed, reason}` — CRC mismatch or truncated event.
- `{:event_decode_crashed, %Capstan.Error{}}` / `{:event_processing_crashed, %Capstan.Error{}}`
  — an unexpected raise, captured value-free.

## Telemetry

All events are emitted through a value-free metadata allowlist (a payload with a key outside
it raises rather than ships). Measurements maps are empty in C1 — attach to the event names:

| Event | Metadata |
| --- | --- |
| `[:capstan, :connection, :established]` | `server_version`, `tls` |
| `[:capstan, :connection, :stream_timeout]` | `reason` |
| `[:capstan, :connection, :halt]` | `reason` |
| `[:capstan, :transaction, :committed]` | `gtid` |
| `[:capstan, :transaction, :filtered]` | `gtid` |
| `[:capstan, :transaction, :skipped]` | `gtid`, `reason` (`:already_processed`) |
| `[:capstan, :schema_change, :received]` | `schema`, `table`, `kind` |
| `[:capstan, :assembler, :halt]` | `reason` |

The only log line emitted in normal operation is a `Logger` warning when authenticating with
the deprecated `mysql_native_password` plugin.

## Substrate requirements (ADR-0002)

capstan verifies these at connect and refuses to start (a distinct reason per violation) if any
is wrong:

```
binlog_format            = ROW
binlog_row_image         = FULL
binlog_row_metadata      = FULL
binlog_row_value_options = ''        # PARTIAL_JSON is refused — it emits JSON diffs, not values
gtid_mode                = ON
```

`enforce_gtid_consistency = ON` is recommended (implied by `gtid_mode = ON`) but is **not**
separately checked. Multi-source replication is supported — a GTID set expresses multiple source
UUIDs natively. The replication account authenticates via `caching_sha2_password` by default and
needs `REPLICATION SLAVE`, `REPLICATION CLIENT`, and `SELECT`.

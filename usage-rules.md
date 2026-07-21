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
    ssl: true,                                  # default true (ADR-0002 / Q6/Q17)
    ssl_opts: [cacertfile: "/etc/ssl/mysql-ca.pem", server_name_indication: :disable],
    auth_plugins: [:caching_sha2_password]      # default; name :mysql_native_password to allow it
  ],
  server_id: 1001,                              # replica identity; MUST be unique in the topology
  sink: MyApp.OrdersSink,                       # implements Capstan.Sink
  checkpoint_store: [module: MyApp.Store, options: []],   # lib-owned mode; see below
  start_position: :checkpoint,                  # C1: :checkpoint (default) only
  tables: [{"orders", "line_items"}],           # or :all (default); filter applied before decode
  max_command_retries: 5                        # default 5
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

### Value-free error reasons

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
a restart. Bridge `Capstan.Position` through `read_position/2` / `write_position/3`, which apply
the persist boundary so you never leak `file`/`pos`.

## Substrate requirements (ADR-0002 / Q5)

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

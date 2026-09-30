# Recipes

Working patterns for the situations that come up when you actually run capstan.
Each recipe assumes the contract in [usage-rules.md](../usage-rules.md).

## An idempotent sink (the foundation of everything)

capstan delivers **at-least-once**; every guarantee above that is your sink
making re-delivery a no-op. The minimal Postgres-shaped example — write the row
keyed by the change's identity and carry the GTID watermark in the SAME
transaction:

```elixir
defmodule MyApp.WarehouseSink do
  @behaviour Capstan.Sink

  @impl true
  def handle_transaction(txn) do
    Repo.transaction(fn ->
      Enum.each(txn.changes, fn change ->
        apply_change(change)
      end)

      # The position advances in the same write as the data: crash between the
      # two is impossible, so a restart resumes exactly after the last applied
      # row (this is the sink-owned effect-once shape — also implement
      # checkpoint/0 and capstan will use yours instead of its store's).
      Repo.insert!(%AppliedWatermark{id: 1, gtid_set: txn.position.gtid_set},
        on_conflict: {:replace, [:gtid_set]},
        conflict_target: :id
      )
    end)

    {:ok, txn.position}
  end

  defp apply_change(%{op: :insert, schema: s, table: t, record: record}),
    do: Repo.insert_all(warehouse_table(s, t), [record], on_conflict: :replace_all)

  defp apply_change(%{op: :delete, schema: s, table: t, old_record: old}) do
    {_, _} = Repo.delete_all(warehouse_table(s, t) |> where_pk(old))
  end

  defp apply_change(%{op: :update, schema: s, table: t, record: record}) do
    {_, _} = Repo.update_all(warehouse_table(s, t) |> where_pk(record), set: record)
  end
end
```

## Warehouse load: batched, one write per flush

For a bulk consumer, batch delivery collapses round trips: the sink receives a
list of transactions plus the batch's final position and persists them in ONE
write.

```elixir
Capstan.start_link(
  connection: connection_opts(),
  server_id: 4321,
  sink: MyApp.WarehouseSink,          # implements handle_batch/2 + checkpoint/0
  checkpoint_store: [module: MyApp.CheckpointStore],
  batch: [max_transactions: 200, flush_ms: 500, mode: :sink_owned],
  tables: [{"shop", "orders"}]
)

# In the sink — the atomic batch write:
@impl true
def handle_batch(txns, %Capstan.Position{} = final_position) do
  Repo.transaction(fn ->
    Enum.each(txns, &apply_transaction/1)
    upsert_watermark(final_position)
  end)

  {:ok, final_position}
end
```

The tradeoff is a wider crash window: on restart, at most the un-flushed batch
tail re-delivers (bounded by `max_transactions`). The dedup checkpoint absorbs
it as long as the batch write is atomic with the watermark — which is the point
of `:sink_owned` mode.

## Snapshot-then-stream migration (backfill an existing table)

Moving an existing table's history into a new consumer without a maintenance
window:

1. Create the snapshot store (same shape as a checkpoint store; per-table
   durable cursors).
2. Start the pipeline WITH the `:snapshot` block and WITHOUT a seeded
   checkpoint — capstan pins the start position `p0`, backfills every
   pre-existing row under a per-chunk brief read lock, and hands off to the
   stream gap-free. Concurrent writes during the backfill are deduplicated by
   the cursor gate (a change a chunk already covers is suppressed from the
   stream; a change after it flows).
3. Kill/restart freely — the backfill resumes per-table from its durable
   cursor.

```elixir
Capstan.start_link(
  connection: connection_opts(),
  server_id: 4322,
  sink: sink,
  checkpoint_store: [module: MyApp.CheckpointStore],
  tables: [{"shop", "orders"}],
  snapshot: [
    tables: [{"shop", "orders"}],       # or :all — the scoped base-table set
    store: [module: MyApp.SnapshotStore],
    chunk_size: 4096
  ]
)
```

Per-table completion: exactly one `final_chunk?: true` beat to `handle_snapshot/2`
(empty tables deliver exactly one EMPTY final chunk — gate per-table readiness
on it).

## "Start from now" (no backfill, no replay)

A fresh consumer that must NOT see history: seed the checkpoint from the
server's live position before the first dump.

```elixir
Capstan.start_link(
  connection: connection_opts(),
  server_id: 4323,
  sink: sink,
  checkpoint_store: [module: MyApp.CheckpointStore],
  start_position: :current,          # reads @@gtid_executed once, pre-dump
  tables: [{"shop", "orders"}]
)
```

An explicit resume point works the same way: `start_position: %Capstan.Position{gtid_set: "…"}`.

## TLS against a self-signed server certificate

MySQL's auto-generated certificate is self-signed with no SAN — peer
verification against `127.0.0.1` fails by hostname. Verify the CHAIN without
the hostname (an explicit operator choice; see usage-rules "TLS"):

```elixir
connection: [
  host: "db.internal",
  username: "capstan",
  password: password,
  ssl: true,
  ssl_opts: [
    cacertfile: "/etc/myapp/mysql-ca.pem",
    server_name_indication: :disable
  ]
]
```

Confidentiality without authentication is also an explicit choice:
`ssl_opts: [verify: :verify_none]`. Either form or a plain connection — but
never a silent default.

## Amazon Aurora MySQL (version 3, the cluster writer endpoint)

Aurora is a named source (ADR-0013). The three settings that differ from a
self-managed server, and what each costs if skipped:

1. **The binlog lives in the DB cluster parameter group** (`binlog_format=ROW`,
   plus the same family as any MySQL source: `binlog_row_image=FULL`,
   `binlog_row_metadata=FULL`, `gtid_mode=ON`, `enforce_gtid_consistency=ON`),
   followed by a writer reboot. An instance-level group edit does nothing. With
   the group's `binlog_format` left `OFF`, `binlog_format` still READS `ROW`
   while `log_bin` is disabled — capstan's `log_bin` check refuses
   `:binlog_disabled` at connect, which is exactly why it exists.
2. **Retention is not a server variable**:
   `CALL mysql.rds_set_configuration('binlog retention hours', N)` (read it back
   with `CALL mysql.rds_show_configuration`). The default `NULL` purges lazily —
   AWS documents the leftovers as "usually not longer than a day" — so set it
   (the version 3 maximum is 2160 hours / 90 days) to cover your worst
   acceptable pipeline downtime; a pause longer than the window halts
   `:data_gap` on restart, and that halt is the correct outcome.
3. **Connect to the cluster (writer) endpoint only.** Readers share the
   parameter group and pass the value checks, but binary logs are served by the
   writer alone.

```elixir
connection: [
  host: "mydbcluster.cluster-c7tj4example.us-east-1.rds.amazonaws.com",
  username: "capstan",
  password: password,
  ssl: true,
  ssl_opts: [cacertfile: "/etc/myapp/global-bundle.pem"]   # the AWS RDS CA bundle
]
```

Unlike the self-signed recipe above, hostname verification stays ON: Aurora's
certificate chains to the AWS CA and names the endpoint host, so no
`server_name_indication: :disable` (with a `cacertfile` and no explicit SNI, capstan defaults
the hostname check to the connection's `host` — ADR-0013's receipt run fixed this).

**Auth:** Aurora MySQL 3's master user defaults to `mysql_native_password` — the inverse of
stock MySQL 8.0 — so capstan's default `caching_sha2_password` posture is refused until the
account is re-pointed once (`ALTER USER … IDENTIFIED WITH caching_sha2_password BY …`;
`default_authentication_plugin` is not modifiable in the cluster group on this engine).

**Failover budget:** a promotion at Serverless v2's floor takes minutes, and the window can
transiently answer the connect-time gate with the binlog disabled. Size the retry budget to
the window — `max_command_retries: 120, reconnect_backoff: 5_000` (≈10 minutes) rode out
every forced failover in the receipt run — and remember failovers count toward the
established-then-dropped budget on a same-uuid cluster (see ADR-0013's uuid observation).

**First start:** prefer
`start_position: :current` (or pre-seed the checkpoint from
`SELECT @@global.gtid_executed`) — an empty checkpoint against Aurora's almost
never-empty `gtid_purged` refuses `:data_gap` by design. **Failovers are not
errors:** the endpoint's DNS change drops the socket, capstan reconnects to the
promoted writer, the checkpoint set gains the new writer's UUID, and the
established-then-dropped budget is reset (a failover is not a `server_id`
conflict). If the old writer's GTIDs do not survive in the promoted writer's
`gtid_executed`, the pipeline halts `:source_identity_mismatch` — re-seed the
checkpoint rather than resume blindly.

## XA sources (two-phase transactions)

XA-prepared rows must NEVER deliver as committed (they may still roll back).
The default policy refuses the first prepare loudly:

```elixir
# default — the pipeline halts :unsupported_transaction_shape on XA PREPARE:
Capstan.start_link(connection: ..., xa: :refuse, ...)

# a source that legitimately uses XA — track through the prepare:
Capstan.start_link(connection: ..., xa: :track, ...)
```

Under `:track`, a prepared transaction's GTID checkpoints ONLY in the same
write as its resolution (`XA COMMIT` delivers the rows; `XA ROLLBACK` drops
them and advances past) — the crash window can never lose a resolution. The
replication account needs `XA_RECOVER_ADMIN` for the connect-time dangling-prepare
enumeration.

## Compressed sources (`binlog_transaction_compression=ON`)

Nothing to configure: capstan consumes compressed transactions natively (the
in-library pure-Elixir zstd decoder inflates each `TRANSACTION_PAYLOAD` event —
byte-exact conformance against the reference `zstd` binary; a malformed payload
halts value-free). GTID/control/non-transactional events still arrive bare on
such sources — the stream is a mix by construction.

## Watching a pipeline in production

Attach the halt events to your alerting and the committed events to your
metrics (full reference: [telemetry.md](telemetry.md)):

```elixir
:telemetry.attach_many(
  "my-app-capstan-halts",
  [[:capstan, :connection, :halt], [:capstan, :assembler, :halt], [:capstan, :snapshot, :halt]],
  fn _event, _measurements, %{reason: reason}, _config ->
    MyApp.Alerting.page("capstan halted: #{inspect(reason)}")
  end,
  nil
)
```

The pipeline does not restart itself on a halt (every child is
`restart: :temporary`): your supervision decides — that is the fail-closed
posture. A restart without investigating the reason is usually wrong; halts
name their cause precisely so you can act on it.

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

# Telemetry reference

Every observable signal capstan emits, through the standard
[`:telemetry`](https://hexdocs.pm/telemetry/) library. **No row value, DDL text,
password, or any other user data ever appears in a measurement or metadata
field** — the `Capstan.Telemetry` boundary allowlists both at runtime, so a
stray value attached to a payload raises instead of shipping (ADR-0007).

## Events

### `[:capstan, :connection, :established]`

A replica connection completed (auth, preconditions, checksum negotiation, dump
started). Reconnects emit it again.

- measurements: `establish_ms` — wall time from connect start to established
- metadata: `server_version` (e.g. `"8.0.46"`), `tls` (e.g. `:secure`/`:plaintext`)

### `[:capstan, :connection, :halt]`

The connection halted fail-closed and the pipeline is stopping (all children are
`restart: :temporary` — a halt is terminal until YOUR supervision restarts it).

- measurements: —
- metadata: `reason` (a value-free atom; the full catalogue is in
  [usage-rules.md](../usage-rules.md) "Runtime halts")

### `[:capstan, :connection, :stream_timeout]`

The liveness timer fired on a silent stream — the pipeline is reconnecting (this
is a recovery event, not a halt; the reconnect budget is `max_command_retries`).

- measurements: —
- metadata: `reason: :stream_stalled`

### `[:capstan, :transaction, :committed]`

One committed transaction was delivered to the sink (or included in a delivered
batch).

- measurements: `change_count` — the number of row changes in the transaction;
  `sink_ms` — the sink call's duration
- metadata: `gtid` — the transaction's GTID (e.g. `"3c94fa12-…:42"`)

### `[:capstan, :transaction, :filtered]`

A committed transaction whose every change was outside the capture allowlist —
no sink call, but the watermark advances (so quiet periods never stall it).

- measurements: —
- metadata: `gtid`

### `[:capstan, :transaction, :skipped]`

A transaction whose GTID is already in the processed set — the dedup
(effect-once) path on restart. The sink is NOT called.

- measurements: —
- metadata: `gtid`, `reason: :already_processed`

### `[:capstan, :schema_change, :received]`

A self-committing DDL was delivered via `handle_schema_change/2`. The DDL
statement text is never surfaced — only the target and a classification.

- measurements: —
- metadata: `schema`, `table`, `kind`

### `[:capstan, :assembler, :halt]`

The assembly/delivery stage halted fail-closed (sink error, checkpoint-write
budget exhausted, event decode failure, stream desync, XA-prepared rows under
`xa: :refuse`, unmapped `table_id`, …). As with every halt, terminal until you
restart it.

- measurements: —
- metadata: `reason` (scrubbed to its value-free outer atom — a compound
  reason like `{:sink_error, raw}` reports `:sink_error`)

### `[:capstan, :snapshot, :started]`

The initial-snapshot coordinator started its backfill.

- measurements: `table_count` — the number of tables to backfill
- metadata: —

### `[:capstan, :snapshot, :chunk_completed]`

One chunk was delivered via `handle_snapshot/2`. (A zero-row table delivers
exactly one empty final chunk — the same event with `row_count: 0`.)

- measurements: `row_count`, `chunk_seq`
- metadata: `schema`, `table`

### `[:capstan, :snapshot, :completed]`

Every snapshot table finished; the coordinator handed authority to the stream.

- measurements: `table_count`
- metadata: —

### `[:capstan, :snapshot, :halt]`

The snapshot halted fail-closed (schema drift mid-backfill, sink error, lock
unavailable past its budget, durable-state fault, …). Terminal like every halt.

- measurements: —
- metadata: `reason`

## Attaching

Log every halt with its reason:

```elixir
:telemetry.attach_many(
  "my-app-capstan-halts",
  [[:capstan, :connection, :halt], [:capstan, :assembler, :halt], [:capstan, :snapshot, :halt]],
  fn _event, _meas, %{reason: reason}, _config ->
    Logger.error("capstan halted: #{inspect(reason)}")
  end,
  nil
)
```

Count delivered changes (a throughput metric):

```elixir
:telemetry.attach("my-app-capstan-changes", [:capstan, :transaction, :committed],
  fn _event, %{change_count: n}, _meta, _config ->
    MyApp.Metrics.add_delivered_changes(n)
  end, nil)
```

With [`telemetry_metrics`](https://hexdocs.pm/telemetry_metrics/), the standard
set:

```elixir
summary("capstan.transaction.committed.duration", unit: {:native, :millisecond}),
counter("capstan.transaction.committed.count"),
sum("capstan.transaction.committed.change_count"),
counter("capstan.snapshot.chunk_completed.count"),
counter("capstan.connection.halt.count"),
counter("capstan.assembler.halt.count"),
counter("capstan.snapshot.halt.count")
```

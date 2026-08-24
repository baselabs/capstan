# capstan

[![Hex.pm](https://img.shields.io/hexpm/v/capstan.svg)](https://hex.pm/packages/capstan)
[![Docs](https://img.shields.io/badge/hex-docs-8e7ce6.svg)](https://hexdocs.pm/capstan)
[![License](https://img.shields.io/hexpm/l/capstan.svg)](https://github.com/baselabs/capstan/blob/main/LICENSE)
[![Run in Livebook](https://livebook.dev/badge/v1/blue.svg)](https://livebook.dev/run?url=https://github.com/baselabs/capstan/blob/main/notebooks/getting_started.livemd)

**capstan is MySQL CDC on the BEAM: it connects to MySQL as a replica, tails the
row-based binary log from a GTID position, and delivers committed transactions to
your sink — durably checkpointed, at-least-once, and fail-closed on every condition
that could otherwise lose or corrupt data silently.** It is
[replicant](https://github.com/baselabs/replicant)'s MySQL sibling.

It is a **library in your supervision tree**, not a daemon: no JVM, no ports, no
second runtime. The MySQL replication-protocol client is in-library and pure
Elixir (`:gen_tcp`/`:ssl`/`:crypto` only).

## When to use it

| You want | capstan |
| --- | --- |
| Row-level change streams out of MySQL 8+ into Elixir | ✅ this is exactly it |
| An initial backfill of existing rows, then a gap-free handoff to the stream | ✅ built in (`:snapshot`) |
| Delivery guarantees without operating Debezium/Maxwell | ✅ one dependency, your tree |
| Schema-change notifications without DDL text leaking into logs | ✅ DDL redacted by design |
| A source with `binlog_transaction_compression=ON` | ✅ consumed — the in-library zstd decoder inflates each compressed transaction (byte-exact conformance vs the reference `zstd`) |
| XA (two-phase) transactions | ✅ tracked or refused, never half-delivered (`xa:` policy) |

## Install

```elixir
def deps do
  [
    {:capstan, "~> 1.2"}
  ]
end
```

MySQL requirements, checked at every connect (one query, distinct refusal per
violation): `binlog_format=ROW`, `binlog_row_image=FULL`, `binlog_row_metadata=FULL`,
`binlog_row_value_options=''` (full JSON), `gtid_mode=ON`. Run
[`scripts/capstan-preflight.sql`](scripts/capstan-preflight.sql) against a
prospective source for a read-only readiness report.

## A 30-second pipeline

```elixir
defmodule MyApp.OrderSink do
  @behaviour Capstan.Sink

  @impl true
  def handle_transaction(txn) do
    Enum.each(txn.changes, fn change ->
      # change.op ∈ {:insert, :update, :delete}; change.record is column => value
      MyApp.Projector.apply!(change)
    end)

    {:ok, txn.position}   # persist this atomically with your own write for effect-once
  end
end

defmodule MyApp.CDC do
  def start_link(opts) do
    Capstan.start_link(
      connection: [
        host: "db.internal",
        username: "capstan",
        password: opts[:password]
      ],
      server_id: 4242,
      sink: MyApp.OrderSink,
      checkpoint_store: [module: MyApp.CheckpointStore],
      tables: [{"shop", "orders"}]
    )
  end
end
```

That is the whole core: connect, stream, deliver, checkpoint. Kill it, restart
it — it resumes from the durable checkpoint with no transaction lost (the sink
sees replays at worst; make it idempotent — that is the contract).

> Try it live (kill/restart resume included) in the
> [getting-started Livebook](notebooks/getting_started.livemd), or run the
> minimal printable consumer in [`examples/`](examples/README.md). For the
> complete production shape as one runnable stack — receipts, idempotent
> mirror, durable checkpoint, all in docker — see
> [`examples/replication_pipeline`](examples/replication_pipeline/README.md).

## The three ways to own your position

**1. Lib-owned checkpointing (default).** You implement a two-callback
`Capstan.CheckpointStore` (read/write a GTID-set string — a table row, a file,
Redis, anything durable). capstan delivers a transaction, then checkpoints.
Crash between the two → the transaction is re-delivered on restart: at-least-once.

**2. Sink-owned checkpointing (effect-once).** Your sink implements
`checkpoint/0` and persists the position atomically with its own write — the
delivery and the position advance become ONE transaction. capstan never
checkpoints past a write the sink has not confirmed.

**3. Batched (`batch: [max_transactions:, flush_ms:, mode:]`).** The checkpoint
write covers N transactions: `:lib_owned` batches the checkpoint writes;
`:sink_owned` hands your `handle_batch/2` the whole list + final position for
one atomic write. Wider crash-replay window, fewer round trips.

## Backfill existing rows first

Add a `:snapshot` block and capstan backfills pre-existing rows before (and
concurrently with) the stream, gap-free — a per-chunk brief read lock pins each
chunk's exact GTID position, and a cursor gate suppresses stream changes a chunk
already covers. Kill it mid-backfill and it resumes per-table from durable
cursors.

```elixir
snapshot: [
  tables: [{"shop", "orders"}, {"shop", "customers"}],   # or :all (scoped base tables)
  store: [module: MyApp.SnapshotStore],
  chunk_size: 4096
]
```

Every table gets exactly one completion signal — a `final_chunk?: true` beat to
`handle_snapshot/2` (empty tables get exactly one empty final chunk). Primary keys
may be integers, `BINARY`/`VARBINARY`, `CHAR`/`VARCHAR` (any charset and collation —
the gate compares the server's own collation sort keys, not string bytes), or
composites of those; the TEXT family and `ENUM`/`SET` keys are refused with a named
halt rather than backfilled in a wrong order.

## Fail-closed, everywhere

Any condition that could silently lose or corrupt data **halts the pipeline**
(`{:shutdown, {:halt, reason}}`; every child is `restart: :temporary`, so the
halt is yours to handle): a purged binlog range, a server identity mismatch, a
malformed event, an unknown transaction shape, an unconsumable compression type,
a sink error, a schema change mid-backfill. Halts surface as
`[:capstan, :connection, :halt]` / `[:capstan, :assembler, :halt]` /
`[:capstan, :snapshot, :halt]` telemetry with a value-free reason. No row value,
no DDL text, no password ever appears in a halt reason, log line, or telemetry
payload — that boundary is enforced at runtime, not by convention.

## Where to go next

- **[examples/replication_pipeline](examples/replication_pipeline/)** — the
  durable reference implementation as a docker stack: one `docker compose up`
  runs MySQL source → capstan (one OTP release container) → MySQL destination
  with value-free receipts, an idempotent upsert mirror, and a durable GTID
  checkpoint you can kill and restart mid-life.
- **[usage-rules.md](usage-rules.md)** — the full operating contract: every
  option, every halt reason, delivery semantics, TLS posture, XA policy.
- **[docs/telemetry.md](docs/telemetry.md)** — every telemetry event with its
  measurements and metadata, plus attach examples.
- **[docs/recipes.md](docs/recipes.md)** — idempotent sinks, warehouse loading,
  snapshot-then-stream migration, batching tradeoffs, TLS, "start from now",
  XA sources.
- **[ADRs](docs/adr/)** — the decision record (position model, fail-closed
  posture, snapshot cursor gate, XA tracking, zstd consumption, …).
- **[CHANGELOG](CHANGELOG.md)**.

## Repository layout

| Directory | Contents |
| --- | --- |
| `lib/` | The Elixir library (Hex package `capstan`) |
| `notebooks/` | The getting-started Livebook |
| `examples/` | Runnable minimal consumers ([examples/README.md](examples/README.md)) |
| `docker-compose.yml`, `scripts/` | The local MySQL test substrate + the read-only preflight report |
| `probe/` | The executed protocol viability probe + committed evidence |
| `docs/` | ADRs, telemetry reference, recipes |
| `usage-rules.md` | Consumer-facing usage contract |

## Support & maintenance

capstan is maintained as the MySQL side of the baselabs CDC stack (replicant's
sibling). Issues and pull requests are welcome; there is no support SLA.

## License

MIT — see [LICENSE](LICENSE).

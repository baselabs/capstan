# capstan

[![Hex.pm](https://img.shields.io/hexpm/v/capstan.svg)](https://hex.pm/packages/capstan)
[![Docs](https://img.shields.io/badge/hex-docs-8e7ce6.svg)](https://hexdocs.pm/capstan)
[![License](https://img.shields.io/hexpm/l/capstan.svg)](https://github.com/baselabs/capstan/blob/main/LICENSE)
[![Run in Livebook](https://livebook.dev/badge/v1/blue.svg)](https://livebook.dev/run?url=https://github.com/baselabs/capstan/blob/main/notebooks/getting_started.livemd)

**capstan is MySQL CDC on the BEAM — it streams committed MySQL row changes from the binary
log to a sink, fail-closed and at-least-once (effect-once is the sink-owned atomic path,
deferred in C1 — see below).** It is [replicant](https://github.com/baselabs/replicant)'s
MySQL sibling: replicant consumes Postgres logical replication (pgoutput) over a replication
slot; capstan consumes MySQL row-based binary-log replication over a registered replica
connection.

> **Status: C1 streaming and C2 initial snapshot are released in 1.0.0.** capstan
> connects to MySQL as a replica, tails the row-based binlog from a GTID position,
> assembles committed transactions, delivers them to a sink, and durably advances a
> processed-GTID checkpoint — fail-closed on every silent-loss condition. C2 adds a
> consistent, resumable backfill of pre-existing rows with a gap-free stream handoff;
> omit `:snapshot` for the unchanged pure-C1 path. C1 ships **lib-owned checkpoint mode** and resume-from-durable-checkpoint
> (`start_position: :checkpoint`) only; sink-owned checkpoint mode and explicit start positions are
> named follow-up rows and are refused fail-closed today (see
> [ADR-0004](docs/adr/0004-c1-scope-lib-owned-checkpoint-only.md)). The protocol viability probe
> that preceded implementation — a ~400-line zero-dependency Elixir client that spoke the full
> MySQL replication protocol against live MySQL 8.0.46 and decoded both a replayed and a
> live-tailed row byte-correct — remains committed under `probe/` (see `probe/FINDINGS.md`).

## Why it exists

The Elixir ecosystem has no MySQL CDC library. Every available option is an external daemon
(Debezium, Maxwell) bolted on through a Port and a JSON wire contract — a second runtime to
operate, and none of the delivery guarantees a BEAM-native library can offer by owning the
sink callback.

capstan is a **library in your supervision tree**, not a daemon.

## Getting started

Two ways in:

- **Try it in ten minutes, no project setup** — open the interactive tour in Livebook. It
  spins up a throwaway MySQL, streams live changes, then kills and restarts the pipeline to
  show resume-with-no-loss-and-no-duplicate:

  [![Run in Livebook](https://livebook.dev/badge/v1/blue.svg)](https://livebook.dev/run?url=https://github.com/baselabs/capstan/blob/main/notebooks/getting_started.livemd)

  (or open
  [`notebooks/getting_started.livemd`](https://github.com/baselabs/capstan/blob/main/notebooks/getting_started.livemd)
  in any Livebook.)

- **Add it to your app** — follow [Installation](#installation) and [Quickstart](#quickstart)
  below, then read [usage-rules.md](usage-rules.md) for the full consumer contract. Before
  pointing it at a real server, confirm the source is ready with the read-only
  [`scripts/capstan-preflight.sql`](scripts/capstan-preflight.sql) (see
  [Substrate requirements](#substrate-requirements)).

## Installation

Add `capstan` to your dependencies:

```elixir
def deps do
  [
    {:capstan, "~> 1.0"}
  ]
end
```

## Quickstart

A sink receives each committed transaction as a `Capstan.Transaction` — `gtid`, `position`,
`commit_ts`, and `changes`, an enumerable of `Capstan.Change` (`op`, `schema`, `table`,
`record`, `old_record`):

```elixir
defmodule MyApp.OrdersSink do
  @behaviour Capstan.Sink

  @impl true
  def handle_transaction(%Capstan.Transaction{changes: changes} = txn) do
    # `changes` is a single-pass enumerable — enumerate exactly once, never length/1.
    Enum.each(changes, fn %Capstan.Change{} = change ->
      # change.op         — :insert | :update | :delete
      # change.record     — the row AFTER the change  (nil on :delete)
      # change.old_record — the row BEFORE the change (nil on :insert)
      MyApp.Projections.apply(change)
    end)

    {:ok, txn.position}
  end

  @impl true
  def handle_schema_change(%Capstan.SchemaChange{}, _position), do: :ok
end
```

Then embed a pipeline in your supervision tree:

```elixir
children = [
  {Capstan,
   connection: [
     host: "replica.internal",
     port: 3306,
     username: "capstan",
     password: System.fetch_env!("CAPSTAN_MYSQL_PASSWORD"),
     database: "orders",
     ssl: true,
     ssl_opts: [cacertfile: "/etc/ssl/mysql-ca.pem", server_name_indication: :disable]
   ],
   server_id: 1001,                    # replica identity — MUST be unique in the topology
   sink: MyApp.OrdersSink,
   checkpoint_store: [module: Capstan.CheckpointStore.InMemory],
   tables: [{"orders", "line_items"}]  # or :all (default)
  }
]
```

Two things to change before production:

1. **The checkpoint store.** `Capstan.CheckpointStore.InMemory` loses the checkpoint on
   restart. Implement the two-callback `Capstan.CheckpointStore` behaviour over storage you
   already trust (your database, a file) to get resume-across-restart — see
   [usage-rules.md](usage-rules.md).
2. **The TLS choice.** `ssl:` defaults to `true` and start-up fails closed until you make an
   explicit peer-verification choice — `cacertfile:`/`cacerts:`, `verify: :verify_none`, or
   `ssl: false`. The options above verify the chain (`VERIFY_CA`).

One behavior to know before the first start: an **empty checkpoint store requests the
server's full retained binlog history**, and a server that has already purged its earliest
logs refuses that dump (the pipeline halts fail-closed with `:data_gap`). To start "from
now", seed the checkpoint store with the server's current `@@global.gtid_executed` before
first start — the recipe is in [usage-rules.md](usage-rules.md). To backfill the rows that
**pre-exist** the pipeline (an initial snapshot woven gap-free/dup-free into the stream), add a
`snapshot:` block and implement `handle_snapshot/2` — see [usage-rules.md](usage-rules.md) §
Initial snapshot and [ADR-0005](docs/adr/0005-initial-snapshot-cursor-gate-brief-lock.md). It
needs the `LOCK TABLES` privilege on the snapshot tables.

[usage-rules.md](usage-rules.md) is the full consumer contract — delivery guarantees, dedup
rules, checkpoint semantics, telemetry, and the fail-closed error surface.

## The contract, shared with replicant

capstan deliberately aligns with replicant's consumer-facing contracts so that porting a sink
between them is a small, *explicit* change rather than a rewrite:

- the same `Sink` behaviour **shape** — `handle_transaction/1`, plus `checkpoint/0`,
  `handle_schema_change/2`, and `handle_snapshot/2` (the initial-snapshot callback; batch
  callbacks are a later row, not shipped yet),
- the same lib-owned `CheckpointStore` alternative for non-transactional sinks,
- the same delivery-guarantee taxonomy — effect-once on the sink-owned atomic path,
  at-least-once with a bounded duplicate window in lib-checkpoint mode (C1 ships the
  lib-checkpoint path; the sink-owned atomic path is deferred — [ADR-0004](docs/adr/0004-c1-scope-lib-owned-checkpoint-only.md)),
- the same value-free error and telemetry conventions (row values never reach a log line).

**The contracts are aligned, not identical, and a sink is not source-blind by default.** Two
differences are forced by MySQL and are deliberate:

1. **Position type.** replicant's position is a scalar `commit_lsn`; capstan's is a GTID set.
   A GTID set is legitimately non-contiguous (failover, purged ranges, multi-source), so it
   cannot be reduced to a comparable integer without lying about gaps.
2. **Dedup idiom.** replicant dedups with `commit_lsn <= checkpoint`; capstan dedups by set
   membership (`Capstan.Gtid.member?/2`). A `<=` comparison on GTIDs is *wrong*, not merely
   different — it would silently skip or re-apply transactions.

Consumers stay source-agnostic through a thin per-source adapter package — not by the two
libraries presenting one identical type.

## Substrate requirements

MySQL 8.0+ configured for row-based replication with GTIDs:

```
binlog_format            = ROW
binlog_row_image         = FULL
binlog_row_metadata      = FULL
binlog_row_value_options = ''      # PARTIAL_JSON emits JSON diffs, not values
gtid_mode                = ON
enforce_gtid_consistency = ON      # implied by gtid_mode=ON; not separately checked
```

capstan verifies the first five at connect and refuses to start if any is wrong. Multi-source
replication (a server aggregating several upstreams) is supported — a GTID set expresses multiple
source UUIDs natively.

To assess a prospective source before any of that — preconditions, GTID posture, retention, XA
usage, and a schema census of the types capstan halts on — have a DBA run the read-only
`scripts/capstan-preflight.sql` and send back the report.

`binlog_row_metadata = FULL` is **required**, not advisory: it is what puts column names and
types in-band on every `TABLE_MAP` event, giving capstan near-parity with pgoutput's `Relation`
messages. The default (`MINIMAL`) does not, and capstan fails closed at connect rather than
decoding rows into positional tuples of unknown provenance.

## Repository layout

| Directory | Contents |
| --- | --- |
| `lib/` | The Elixir library (Hex package `capstan`) — C1 streaming plus the additive C2 snapshot |
| `examples/` | Runnable minimal consumers ([examples/README.md](https://github.com/baselabs/capstan/blob/main/examples/README.md)) |
| `docker-compose.yml`, `scripts/` | The local MySQL test substrate ([docs/testing.md](https://github.com/baselabs/capstan/blob/main/docs/testing.md)) |
| `probe/` | The executed protocol viability probe + committed evidence |
| `docs/adr/` | Architecture decision records ([0001](docs/adr/0001-position-and-dedup-model.md)–[0006](docs/adr/0006-xa-prepare-commit-tracking.md)) |
| `docs/testing.md` | Test environment + how to run the suite |
| `usage-rules.md` | Consumer-facing usage contract |

## Support & maintenance

capstan is maintained as the MySQL side of the baselabs CDC stack (replicant's sibling).
Issues and pull requests are welcome; there is no support SLA.

## License

MIT — see [LICENSE](https://github.com/baselabs/capstan/blob/main/LICENSE).

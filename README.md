# capstan

**capstan is MySQL CDC on the BEAM — it streams committed MySQL row changes from the binary
log, effect-once, to a sink.** It is [replicant](https://github.com/baselabs/replicant)'s
MySQL sibling: replicant consumes Postgres logical replication (pgoutput) over a replication
slot; capstan consumes MySQL row-based binary-log replication over a registered replica
connection.

> **Status: pre-implementation. The protocol viability probe is executed and GREEN
> (2026-07-20); the design pass is in progress.** No library code exists yet. The committed
> evidence is `probe/` — a ~400-line zero-dependency Elixir client that spoke the full MySQL
> replication protocol against live MySQL 8.0.46 and decoded both a replayed and a live-tailed
> row byte-correct. See `probe/FINDINGS.md`.

## Why it exists

The Elixir ecosystem has no MySQL CDC library. Every available option is an external daemon
(Debezium, Maxwell) bolted on through a Port and a JSON wire contract — a second runtime to
operate, and none of the delivery guarantees a BEAM-native library can offer by owning the
sink callback.

capstan is a **library in your supervision tree**, not a daemon.

## The contract, shared with replicant

capstan deliberately aligns with replicant's consumer-facing contracts so that porting a sink
between them is a small, *explicit* change rather than a rewrite:

- the same `Sink` behaviour **shape** — `handle_transaction/1`, plus `checkpoint/0` and
  `handle_schema_change/2` (batch and snapshot callbacks are later rows, not shipped yet),
- the same lib-owned `CheckpointStore` alternative for non-transactional sinks,
- the same delivery-guarantee taxonomy — effect-once on the sink-owned atomic path,
  at-least-once with a bounded duplicate window in lib-checkpoint mode,
- the same value-free error and telemetry conventions (row values never reach a log line).

**The contracts are aligned, not identical, and a sink is not source-blind by default.** Two
differences are forced by MySQL and are deliberate:

1. **Position type.** replicant's position is a scalar `commit_lsn`; capstan's is a GTID set.
   A GTID set is legitimately non-contiguous (failover, purged ranges, multi-source), so it
   cannot be reduced to a comparable integer without lying about gaps.
2. **Dedup idiom.** replicant dedups with `commit_lsn <= checkpoint`; capstan dedups by set
   membership (`Capstan.Gtid.member?/2`). A `<=` comparison on GTIDs is *wrong*, not merely
   different — it would silently skip or re-apply transactions.

Consumers such as [beamline](https://github.com/baselabs/beamline) stay source-agnostic through
a thin per-source adapter package, which is that project's existing pattern — not by the two
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

`binlog_row_metadata = FULL` is **required**, not advisory: it is what puts column names and
types in-band on every `TABLE_MAP` event, giving capstan near-parity with pgoutput's `Relation`
messages. The default (`MINIMAL`) does not, and capstan fails closed at connect rather than
decoding rows into positional tuples of unknown provenance.

## Repository layout

| Directory | Contents |
| --- | --- |
| `lib/` | The Elixir library (hex package `capstan`) — not yet implemented |
| `probe/` | The executed protocol viability probe + committed evidence |
| `docs/adr/` | Architecture decision records |

## License

MIT — see [LICENSE](LICENSE).

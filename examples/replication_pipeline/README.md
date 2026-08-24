# The reference pipeline — capstan as a durable MySQL→MySQL replicator

One `docker compose up` brings up the whole reference stack:

```
MySQL 8.0 (source, ROW binlog + GTID) ──binlog──> [this app] ──> MySQL 8.0 (destination)
                                                   ├─ value-free receipts (cdc_receipts)
                                                   ├─ idempotent mirror (orders)
                                                   └─ durable GTID checkpoint (capstan_checkpoint)
```

This is the runnable form of the pattern `docs/recipes.md` describes: a
sink that records every delivery value-free, applies changes idempotently
(because capstan is at-least-once in lib-owned checkpoint mode), and persists
the processed GTID set so a restart resumes exactly where it left off — across
a replica cutover too, because the position is a GTID *set*, never a log-file
ordinal.

All credentials here are throwaway local-example values, not secrets.

## Run it

```bash
cd examples/replication_pipeline
docker compose up -d --build          # source + destination + pipeline
```

First boot is a full OTP-release build — a few minutes. Watch it flow:

```bash
# write a row on the source
docker compose exec source-mysql mysql -pprobe example_src \
  -e "INSERT INTO orders (id, note) VALUES (1, 'hello'), (2, 'world')"

# see it on the destination: the mirror and the value-free receipts
docker compose exec dest-mysql mysql -pprobe example_dst \
  -e "SELECT * FROM orders; SELECT txn_gtid, schema_name, table_name, op FROM cdc_receipts"
```

### Prove the durability

```bash
docker compose restart pipeline                     # kill/restart mid-life
docker compose exec source-mysql mysql -pprobe example_src \
  -e "INSERT INTO orders (id, note) VALUES (3, 'after restart')"
docker compose exec dest-mysql mysql -pprobe example_dst \
  -e "SELECT id, note FROM orders ORDER BY id;      -- 3 rows, ids 1..3
      SELECT COUNT(*) AS receipts FROM cdc_receipts;" -- no re-delivery of 1..2
```

The checkpoint row (`capstan_checkpoint`) is what made that resume gap-free.

## What each piece teaches

| Piece | The lesson |
| --- | --- |
| `ReplicationPipeline.Sink` | receipts carry GTID/schema/table/op — never a row value (capstan Rule 1, upheld at the destination); the ledger is deliberately not deduplicated so at-least-once re-deliveries stay VISIBLE |
| `ReplicationPipeline.Materializer` | idempotent upsert-by-PK is what turns at-least-once delivery into exactly-once effects |
| `ReplicationPipeline.CheckpointStore` | the durable GTID-set watermark in the destination DB; query faults return value-free errors and capstan applies retry-then-halt |
| `ReplicationPipeline.BootSeed` | the first-boot position seed — an empty checkpoint would request the server's FULL retained history, and a server that has purged refuses it (`:data_gap`). See the module doc for why seed-once beats a persistent `start_position: :current` |
| `docker-compose.yml` (source flags) | the five fail-closed source preconditions capstan checks at every connect |

The mirror is intentionally minimal (one table, `id` PK). Real deployments
extend `Materializer` per table — the seam is the point, not the schema.

Teardown: `docker compose down -v`.

## CI

The `reference-example` CI job builds this stack and runs the exact sequence
above — insert, mirror+receipt+checkpoint assertions, pipeline restart,
no-duplicate-receipt assertion — so the example can never silently rot out of
sync with capstan's public sink API.

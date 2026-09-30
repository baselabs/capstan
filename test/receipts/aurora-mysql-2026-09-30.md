# Aurora MySQL receipt — 2026-09-30

The ADR-0013 cluster run. Real Amazon Aurora MySQL, us-east-2, cluster `capstan-receipt`
(deleted after the run), Serverless v2 (MinCapacity 0.5), one writer + one reader,
publicly accessible over TLS with the AWS RDS global CA bundle and hostname verification
ON. Engine `8.0.45`, Aurora version `3.13.0`, cluster parameter group carrying
`binlog_format=ROW`, `binlog_row_image=FULL`, `binlog_row_metadata=FULL`, `gtid-mode=ON`,
`enforce_gtid_consistency=ON`; `binlog retention hours = 24` via
`mysql.rds_set_configuration`. Run by `mix test --only aurora_mysql` (healthy scenario)
and `AURORA_MYSQL_MISCONFIG=<key>` (one re-parameterized cluster per misconfiguration,
static changes applied by writer reboot). No credential, hostname, or account identifier
appears in this file; every value below is a configuration value or structural identity.

## Observed identity

- Writer `@@server_uuid`: `33ec48fd-1143-3544-bc53-6d217d61b040`.
- **`@@server_uuid` is CLUSTER-WIDE**: writer and reader report the SAME uuid (queried
  per-instance directly). A failover does not change the endpoint's uuid.
- Reader instance: `@@innodb_read_only = 1`, `@@global.gtid_executed = ''` (empty on the
  reader), `@@global.log_bin` reads disabled.

## The gate (six variables) — OBSERVED

Healthy writer row, over the wire via capstan's own client (TLS, hostname verified):

```
binlog_format=ROW  binlog_row_image=FULL  binlog_row_metadata=FULL
binlog_row_value_options=''  gtid_mode=ON  log_bin="1"
```

`log_bin` arrives as the text `"1"` — the boolean flag's simple-query form, never `"ON"`
(observed live; consistent with MySQL 8.0.46/8.4.11). `Config.check_preconditions/1` →
`:ok`.

## Misconfigurations — each refused with its distinct reason (all OBSERVED)

| Scenario (cluster parameter) | Observed row (format/log_bin/metadata/gtid) | Refusal |
|---|---|---|
| `binlog_format=OFF` (key `binlog_format_off`) | `ROW, 0, FULL, ON` — **binlog_format READS `ROW` while the binlog is disabled** | `:binlog_disabled` |
| `binlog_format=OFF` (key `log_bin_off`) | same condition, second run | `:binlog_disabled` |
| `binlog_format=STATEMENT` | `STATEMENT, 1, FULL, ON` | `:binlog_format_not_row` |
| `binlog_format=MIXED` | `MIXED, 1, FULL, ON` | `:binlog_format_not_row` |
| `binlog_row_metadata=MINIMAL` | `ROW, 1, MINIMAL, ON` | `:binlog_row_metadata_not_full` |
| `gtid-mode=OFF` | `ROW, 1, FULL, OFF` | `:gtid_mode_not_on` |

The OFF row is the documented ADR-0013 table row 1, now OBSERVED: a binlog-disabled
Aurora answers `binlog_format=ROW`, so `log_bin` is the variable that names the
condition and `:binlog_disabled` the refusal — exactly why the gate's sixth variable
exists.

## The reader endpoint — OBSERVED

A pipeline connected to the reader endpoint is refused `:binlog_disabled` BEFORE the
dump, zero transactions delivered. This is ADR-0013 decision 1's "reader reports
`log_bin = OFF`" arm: **no `:binlog_source_not_writer` reason is needed** (the gate
already refuses the reader; adding a dump-refusal reason has no observed need).

## Forced failovers — OBSERVED (two, mid-stream)

`aws rds failover-db-cluster`, twice, while streaming to an append-only ledger:

- Every committed row delivered exactly once across both promotions (8/8; GTIDs
  `33ec48fd…:34-41`), no halt, durable checkpoint advanced to `33ec48fd…:1-41`.
- The checkpoint set carries the ONE cluster-wide uuid (Aurora adds no second source
  uuid on failover — the pre-run premise "the set gains the promoted writer's UUID" is
  corrected by the cluster-wide-uuid observation; `gtid_executed` continuity across
  promotion holds trivially under one uuid).
- A mid-backfill snapshot (chunked, 50 rows) RODE OUT a failover and completed gap-free:
  with one cluster uuid the failover is TRANSPARENT to the pinned-identity check —
  no `:snapshot_source_mismatch` fired (it would on a multi-uuid topology).
- **Promotion takes minutes at the 0.5-ACU floor**, and during the window the endpoint
  can transiently answer the gate with `log_bin` disabled. Two product consequences,
  both fixed before this run went green (see CHANGELOG 1.3.0): `log_bin` refusals are
  BUDGETED (retried) rather than immediate halts, and the retry budget must be sized
  to the failover window (`max_command_retries: 120, reconnect_backoff: 5_000` ≈ 10
  minutes rode out every promotion here).

## `start_position: :current` — OBSERVED

Fresh store, `:current` start: the first delivery is the first post-start transaction
(`33ec48fd…:45`), checkpoint seeds `1-45`, nothing pre-start delivered.

## Not runnable on a fresh cluster

The `:data_gap` marquee (checkpoint older than the retention window) requires
`gtid_purged` non-empty; a cluster younger than its retention window has it empty by
construction. The marquee fails loudly by design. The predicate itself is proven on
real MySQL by the gap marquees (`test/integration/gap_test.exs`) and by the simulator
tier; the Aurora-specific observation (what `binlog retention hours` purge looks like
in `gtid_purged`) needs a cluster past its window.

## Also observed, for the recipe

- Aurora MySQL 3's MASTER USER authenticates `mysql_native_password` by default (the
  inverse of stock MySQL 8.0): capstan's default `caching_sha2_password` posture is
  refused until the master user is re-pointed (`ALTER USER … IDENTIFIED WITH
  caching_sha2_password`) — `default_authentication_plugin` is not modifiable in the
  cluster parameter group on this engine.
- TLS with the RDS global CA bundle and hostname verification ON works as documented
  (after the SNI fix; see CHANGELOG 1.3.0) — the cert's SANs carry the cluster, reader,
  and instance endpoints.

## Marquee outcomes

| Marquee | Outcome |
|---|---|
| six-variable gate healthy (`log_bin "1"`) | PASS |
| reader endpoint refused before the dump | PASS (`:binlog_disabled`) |
| two forced failovers, loss 0, no halt | PASS |
| snapshot across a failover, gap-free | PASS |
| `start_position: :current` | PASS |
| retention `:data_gap` | NOT RUNNABLE (fresh cluster; loud by design) |
| misconfigurations ×6 | PASS (distinct reasons, per-row values above) |

Run artifacts: `.kimosabe/scratch/aurora-*.log` (machine-local). Cluster and every
supporting AWS resource deleted at the end of the run.

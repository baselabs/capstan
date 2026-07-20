# MySQL binlog protocol probe — FINDINGS

**Date:** 2026-07-20 · **Verdict: GO — pure Elixir is viable, no Rust engine needed on current evidence**

Probe: `mysql_binlog_probe.exs` (~400 lines, zero deps, `:gen_tcp` + `:crypto` only) against live
MySQL **8.0.46** (docker `mysql-cdc-probe` @ 127.0.0.1:5633, `gtid_mode=ON`,
`binlog_checksum=CRC32`, `binlog_row_metadata=FULL`). Exit: `VERDICT: PASS`, decoded rows
`[[1, "seeded-widget", 42], [2, "live-widget", 7]]`.

## Claims → evidence

| Claim | Evidence |
|---|---|
| Wire handshake + auth in pure Elixir | `[auth] OK (mysql_native_password over plain TCP)` against 8.0.46 |
| COM_QUERY text protocol | `@@gtid_mode/@@binlog_checksum/@@binlog_row_metadata/@@version` read via own resultset parser |
| `COM_BINLOG_DUMP_GTID` accepted | flags=BINLOG_THROUGH_GTID, empty GTID set → full history streamed, then live tail (blocking) |
| Event stream parse | ROTATE, FORMAT_DESCRIPTION, PREVIOUS_GTIDS, GTID, QUERY, TABLE_MAP, WRITE_ROWS_v2, XID, STOP all parsed |
| CRC32 integrity | `crc32=OK` verified per-event via `:erlang.crc32`, including artificial ROTATE events |
| GTID positions | 9 GTIDs parsed `8d7c06f2-…:1..9` (uuid:gno from GTID_LOG_EVENT body) |
| Row decode byte-correct | INT/VARCHAR/BIGINT decoded from WRITE_ROWS null-bitmap + type/meta; both rows exact |
| **Live tailing** | insert fired from a second connection mid-stream, observed ~40s into the blocking stream, decoded |
| Binlog file rotation mid-stream | real ROTATE binlog.000001→000002 handled transparently |
| DDL visibility | `CREATE TABLE widgets …` arrived as QUERY event with schema name |

## Design-relevant discoveries (things the probe CHANGED)

1. **Schema tracking is much smaller than feared on MySQL 8.** With `binlog_row_metadata=FULL`,
   every TABLE_MAP carries **column names + types in-band** (`names=["id","name","qty"]` observed) —
   near-parity with pgoutput Relation messages. Design can REQUIRE `binlog_row_metadata=FULL` as a
   server precondition, exactly like replicant requires `wal_level=logical`. Debezium-style
   schema-history persistence is then only needed for DDL *between* row events of already-mapped
   tables — much narrower. (Default is MINIMAL; precondition check at connect, fail closed.)
2. **Empty-GTID-set dump replays the mysql system schema** (timezone loads, ENUM columns) before
   user data — a real lib filters by schema/table before row decode (probe does) and starts from
   `@@gtid_purged`-aware positions, not the epoch.
3. **STOP_EVENT (type 3)** appears at clean-shutdown binlog boundaries — must be in the event table.
4. Artificial ROTATE events (ts=0, log_pos=0) open every dump AND appear at real rotations —
   position bookkeeping must use them, not assume monotonic log_pos across files.

## Known residuals (named, not blockers)

- **Auth:** probe used `mysql_native_password` (deprecated in 8.4, removed in 9.x). Production needs
  `caching_sha2_password` + TLS — implemented in pure Elixir by MyXQL (Apache-2.0, the reference), so
  proven-tractable, not re-proven here.
- **Type matrix:** probe decoded INT/VARCHAR/BIGINT. Full matrix (DECIMAL/NEWDECIMAL, temporal *2
  types, JSON binary, ENUM/SET, charsets) is grind on top of the same frame — same shape as
  replicant's `casting/types.ex`.
- **>16MB split packets, zstd TRANSACTION_PAYLOAD (8.0.20+ compression), MariaDB divergence** — not
  exercised; each needs a red-capable gate in the real lib.

## Substrate left up

`docker mysql-cdc-probe` (mysql:8.0.46) @ :5633, root/probe, db `probe_db` table `widgets`.

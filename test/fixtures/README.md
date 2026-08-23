# Golden binlog fixtures

Real, captured binlog event bytes — never self-signed/synthetic. Every file under
`test/fixtures/binlog/<scenario>/` is the *verbatim* wire bytes of one binlog event
(19-byte header + body + 4-byte CRC32 trailer), captured from the live
`mysql-cdc-probe` substrate (`127.0.0.1:$MYSQL_PORT_80`, default 11619) by `test/support/fixture_capture.ex`.
Tasks 8–11 decode against these bytes.

## Regenerating

Bring the substrate up (idempotent — never restarts a running server):

```
scripts/dev-substrate.sh --only-80
```

Then, with `mysql-cdc-probe` reachable at `127.0.0.1:$MYSQL_PORT_80` (root/probe,
`mysql_native_password`, database `probe_db`):

```
MIX_ENV=test mix run -e "Capstan.FixtureCapture.capture_all()"
```

Or regenerate a single scenario:

```
MIX_ENV=test mix run -e "Capstan.FixtureCapture.capture_scenario!(scenario)"
```

(pick the scenario map out of the private `scenarios/0` list in
`test/support/fixture_capture.ex`, or just re-run `capture_all/0` — it is
idempotent: each scenario's setup starts with `DROP TABLE IF EXISTS`, and each
scenario's fixture directory is wiped and rewritten on every run).

Each scenario uses a dedicated dump `server_id` (4001–4007) so multiple runs
never collide with the running probe's `server_id=1` or with each other.

**Note:** the captured `GTID`/`PREVIOUS_GTIDS` fixture bytes carry the substrate's
own `server_uuid`. That UUID is inherent to real captured bytes and will differ if
the container is recreated — decode tests must parse it from the bytes, never
hard-code it.

## Scenarios and their generating SQL

### (a) `simple_dml` — 3-column InnoDB table, one INSERT/UPDATE/DELETE (3 own transactions)

```sql
DROP TABLE IF EXISTS widgets;
CREATE TABLE widgets (id INT PRIMARY KEY, name VARCHAR(50), qty INT) ENGINE=InnoDB;

INSERT INTO widgets (id, name, qty) VALUES (1, 'widget-one', 10);
UPDATE widgets SET qty = 20 WHERE id = 1;
DELETE FROM widgets WHERE id = 1;
```

### (b) `multi_table` — single multi-table `UPDATE ta JOIN tb` (the Q3 counterexample)

One statement, one transaction: `Table_map(ta) → Table_map(tb) → Update_rows(ta) →
Update_rows(tb) → Xid`.

```sql
DROP TABLE IF EXISTS ta;
DROP TABLE IF EXISTS tb;
CREATE TABLE ta (id INT PRIMARY KEY, val INT) ENGINE=InnoDB;
CREATE TABLE tb (id INT PRIMARY KEY, val INT) ENGINE=InnoDB;
INSERT INTO ta (id, val) VALUES (1, 100), (2, 200);
INSERT INTO tb (id, val) VALUES (1, 1), (2, 2);

UPDATE ta JOIN tb ON ta.id = tb.id SET ta.val = ta.val + tb.val, tb.val = tb.val + 1;
```

### (c) `alter_ddl` — self-committing DDL (Q13): `Gtid → Query(DDL)`, no `BEGIN`/`XID`

```sql
DROP TABLE IF EXISTS widgets_ddl;
CREATE TABLE widgets_ddl (id INT PRIMARY KEY, name VARCHAR(50)) ENGINE=InnoDB;

ALTER TABLE widgets_ddl ADD COLUMN extra INT DEFAULT 7;
```

### (d) `all_types` — every C1-supported column type, one INSERT

Boundary values at every integer width, signed AND unsigned (F3: signedness is
carried by TABLE_MAP optional-metadata TLV type 1, not the type byte — a
`BIGINT UNSIGNED` of `18446744073709551615` must not decode as `-1`), plus
`DECIMAL`, `VARCHAR`/`CHAR`/`TEXT`/`BLOB`, `DATETIME`/`TIMESTAMP`/`DATE`/`TIME`,
and `ENUM`.

```sql
DROP TABLE IF EXISTS all_types;
CREATE TABLE all_types (
  id INT PRIMARY KEY,
  tiny_s TINYINT,
  tiny_u TINYINT UNSIGNED,
  small_s SMALLINT,
  small_u SMALLINT UNSIGNED,
  medium_s MEDIUMINT,
  medium_u MEDIUMINT UNSIGNED,
  int_s INT,
  int_u INT UNSIGNED,
  big_s BIGINT,
  big_u BIGINT UNSIGNED,
  dec_col DECIMAL(10,2),
  varchar_col VARCHAR(50),
  char_col CHAR(10),
  text_col TEXT,
  blob_col BLOB,
  datetime_col DATETIME,
  timestamp_col TIMESTAMP NULL,
  date_col DATE,
  time_col TIME,
  enum_col ENUM('small', 'medium', 'large')
) ENGINE=InnoDB;

INSERT INTO all_types (
  id, tiny_s, tiny_u, small_s, small_u, medium_s, medium_u, int_s, int_u,
  big_s, big_u, dec_col, varchar_col, char_col, text_col, blob_col,
  datetime_col, timestamp_col, date_col, time_col, enum_col
) VALUES (
  1, -128, 255, -32768, 65535, -8388608, 16777215, -2147483648, 4294967295,
  -9223372036854775808, 18446744073709551615, 12345.67, 'varchar-value',
  'char-val', 'text value here', 'blob-bytes', '2024-01-15 10:30:00',
  '2024-01-15 10:30:00', '2024-01-15', '10:30:00', 'medium'
);
```

### (e) `rows_query` — `binlog_rows_query_log_events = ON`, emits `ROWS_QUERY_LOG_EVENT`

`SET SESSION` is issued on the same connection that runs the INSERT — it is a
session variable and the substrate defaults it OFF.

```sql
DROP TABLE IF EXISTS rq_widgets;
CREATE TABLE rq_widgets (id INT PRIMARY KEY, name VARCHAR(50)) ENGINE=InnoDB;
SET SESSION binlog_rows_query_log_events = ON;

INSERT INTO rq_widgets (id, name) VALUES (1, 'rq-one');
```

### (f) `myisam` — non-transactional engine, terminates with `Query("COMMIT")` not `XID` (Q13)

```sql
DROP TABLE IF EXISTS myisam_widgets;
CREATE TABLE myisam_widgets (id INT PRIMARY KEY, name VARCHAR(50)) ENGINE=MyISAM;

INSERT INTO myisam_widgets (id, name) VALUES (1, 'myisam-one');
```

### (g) `json_col` — a JSON column, one INSERT with a JSON document

```sql
DROP TABLE IF EXISTS json_widgets;
CREATE TABLE json_widgets (id INT PRIMARY KEY, doc JSON) ENGINE=InnoDB;

INSERT INTO json_widgets (id, doc) VALUES (1, '{"a":1,"b":[true,false,null],"c":"text"}');
```

### (h) `set_type` — a `SET` column (F3: `SET` and `ENUM` both wire as type 254)

`all_types` carries an `ENUM` but no `SET`. Both are `MYSQL_TYPE_STRING` (254) on the
wire, distinguishable only by the STRING meta pair — captured here, byte 0 of the meta
is `0xF8` (real type 248 = `SET`) and the `SET_STR_VALUE` optional-metadata TLV lists
the members. C1 must unpack this to DETECT `SET` and halt `:unsupported_column_type`
(the row image is packed differently); a synthetic meta would prove roundtrip, not
conformance, so real captured bytes are required.

```sql
DROP TABLE IF EXISTS set_probe;
CREATE TABLE set_probe (id INT PRIMARY KEY, flags SET('a', 'b', 'c')) ENGINE=InnoDB;

INSERT INTO set_probe (id, flags) VALUES (1, 'a,c');
```

### (i) `frac_temporal` — `DATETIME2`/`TIME2`/`TIMESTAMP2` with non-zero fractional precision

`all_types` uses fsp 0 (zero fractional bytes), which cannot catch a decoder that
ignores the meta and assumes a fixed width. These columns carry fsp 3/6/6 in the meta
(`03 06 06`), so the fractional-second bytes that follow are meta-driven — the only way
the temporal decode is proven non-vacuous.

```sql
DROP TABLE IF EXISTS frac_probe;
CREATE TABLE frac_probe (
  id INT PRIMARY KEY,
  dt DATETIME(3),
  tm TIME(6),
  ts TIMESTAMP(6) NULL
) ENGINE=InnoDB;

INSERT INTO frac_probe (id, dt, tm, ts)
VALUES (1, '2024-01-15 10:30:00.123', '10:30:00.123456', '2024-01-15 10:30:00.654321');
```

### (j) `spatial` — `GEOMETRY`/`POINT` columns (raw passthrough, C4a)

```sql
DROP TABLE IF EXISTS geo_widgets;
CREATE TABLE geo_widgets (id INT PRIMARY KEY, g GEOMETRY, p POINT) ENGINE=InnoDB;
INSERT INTO geo_widgets VALUES (1, ST_GeomFromText('POINT(1 2)'), ST_GeomFromText('POINT(3 4)'));
```

### (k) `xa` — a full two-phase XA: prepare (type 38) + separate resolution transaction (C5)

```sql
DROP TABLE IF EXISTS xa_widgets;
CREATE TABLE xa_widgets (id INT PRIMARY KEY, name VARCHAR(50)) ENGINE=InnoDB;
XA START 'xa-gtrid','xa-bqual',7;
INSERT INTO xa_widgets (id, name) VALUES (1, 'xa-one');
XA END 'xa-gtrid','xa-bqual',7;
XA PREPARE 'xa-gtrid','xa-bqual',7;
XA COMMIT 'xa-gtrid','xa-bqual',7;
```

## Compressed-transaction scenarios (`zstd_*`) — a second substrate

The `zstd_*` scenarios are captured from the throwaway `capstan-zstd` container
(`127.0.0.1:26666`, `--binlog-transaction-compression=ON`, caching_sha2 root/probe),
NOT the shared substrate — each committed transaction arrives as a bare `GTID`
followed by a `TRANSACTION_PAYLOAD_EVENT` (type 40) whose body is a
net_field_length TLV header wrapping one zstd frame. Three files per payload
event are committed:

- `NN-transaction_payload.bin` — the verbatim type-40 event (header + TLV + frame + CRC);
- `NN-transaction_payload.zst` — the zstd frame sliced out of the TLV;
- `NN-transaction_payload.inner` — the same frame inflated by the REFERENCE
  `zstd` binary at capture time: the byte-exact oracle for `Capstan.Zstd` (never
  capstan's own decoder). The inner bytes are `QUERY(BEGIN) + TABLE_MAP +
  ROWS + XID` events WITHOUT per-event CRC trailers — the outer type-40 CRC
  covers the compressed payload as a whole.

Regenerate with the container up (it is disposable: `docker run --name capstan-zstd
-e MYSQL_ROOT_PASSWORD=probe -e MYSQL_DATABASE=probe_db -p 26666:3306 mysql:8.0
--binlog-format=ROW --binlog-row-image=FULL --binlog-row-metadata=FULL
--binlog-row-value-options= --gtid-mode=ON --enforce-gtid-consistency=ON
--server-id=9 --binlog-transaction-compression=ON`; a fresh container has a new
server_uuid — decode tests parse it from the bytes) and a host `zstd` binary on
PATH for the `.inner` oracles:

```
MIX_ENV=test mix run -e 'Capstan.FixtureCapture.capture_only!(["zstd_small", "zstd_rows", "zstd_large", "zstd_repetitive", "zstd_multi"])'
```

- `zstd_small` — one INSERT (3 columns): single small frame.
- `zstd_rows` — INSERT + UPDATE + DELETE: three payloads across row-event kinds.
- `zstd_large` — three ~400KB `REPEAT` rows in one transaction: the inflated
  payload is ~1.18MB, forcing a multi-block frame (multiple 128KB compressed
  blocks, cross-block matches, larger literal sections).
- `zstd_repetitive` — highly repetitive rows (RLE-shaped literals and repeat offsets).
- `zstd_multi` — several transactions in sequence: one frame per payload,
  exercising frame-to-frame state resets (repeat offsets back to `{1,4,8}`).

-- capstan-preflight.sql — READ-ONLY readiness report for a prospective capstan source.
--
-- Run against the MySQL server capstan will connect to (the binlog source — for a
-- replicated topology that is the REPLICA capstan will tail, not the legacy primary):
--
--   mysql --host=<host> --port=<port> --user=<admin> -p --force --table \
--         < capstan-preflight.sql > capstan-preflight-report.txt
--
-- --force continues past errors: on a pre-8.0 server some variables below do not
-- exist and the errors themselves are the finding (capstan requires MySQL 8.0+).
--
-- Privileges: SELECT on information_schema/performance_schema, REPLICATION CLIENT
-- (binary log + replica status), XA_RECOVER_ADMIN (the XA section). A default
-- admin/root account has all three. Every statement is read-only.
--
-- Send back the full output plus answers to the questions at the bottom.

SELECT '== 0. SERVER IDENTITY ==' AS section;
SELECT VERSION() AS mysql_version,
       @@global.server_id AS server_id,
       @@global.server_uuid AS server_uuid,
       @@global.hostname AS hostname,
       @@global.read_only AS read_only,
       @@global.super_read_only AS super_read_only,
       NOW() AS report_time;

SELECT '== 1. CAPSTAN PRECONDITIONS (all five must PASS; checked again at every connect) ==' AS section;
SELECT 'binlog_format' AS setting, @@global.binlog_format AS current_value, 'ROW' AS required,
       IF(@@global.binlog_format = 'ROW', 'PASS', 'FAIL') AS verdict
UNION ALL
SELECT 'binlog_row_image', @@global.binlog_row_image, 'FULL',
       IF(@@global.binlog_row_image = 'FULL', 'PASS', 'FAIL')
UNION ALL
SELECT 'binlog_row_metadata', @@global.binlog_row_metadata, 'FULL',
       IF(@@global.binlog_row_metadata = 'FULL', 'PASS', 'FAIL')
UNION ALL
SELECT 'binlog_row_value_options', @@global.binlog_row_value_options, '(empty)',
       IF(@@global.binlog_row_value_options = '', 'PASS', 'FAIL')
UNION ALL
SELECT 'gtid_mode', @@global.gtid_mode, 'ON',
       IF(@@global.gtid_mode = 'ON', 'PASS', 'FAIL');

-- Informational (NOT a precondition): compressed transactions are CONSUMED —
-- capstan inflates TRANSACTION_PAYLOAD events itself (ADR-0011).
SELECT 'binlog_transaction_compression (informational — consumed by capstan)' AS setting,
       @@global.binlog_transaction_compression AS current_value,
       'consumed either way' AS note;

SELECT '== 2. GTID POSTURE ==' AS section;
SELECT @@global.enforce_gtid_consistency AS enforce_gtid_consistency;
SELECT @@global.gtid_executed AS gtid_executed;
-- Non-empty gtid_purged means the earliest history is gone: a brand-new capstan
-- pipeline must be SEEDED with a start position (see usage-rules.md "First start")
-- rather than started on an empty checkpoint.
SELECT @@global.gtid_purged AS gtid_purged,
       IF(@@global.gtid_purged = '', 'empty — fresh start possible', 'non-empty — SEED the checkpoint before first start') AS note;

SELECT '== 3. UPSTREAM REPLICATION CHAIN (only relevant when this server is itself a replica) ==' AS section;
-- If this server replicates from a pre-GTID primary, Assign_Gtids_To_Anonymous_Transactions
-- (8.0.23+) must be LOCAL or a UUID so incoming transactions get GTIDs, and
-- log_replica_updates below must be ON so they reach THIS server's binlog.
SHOW REPLICA STATUS\G

SELECT '== 4. CASCADING-SOURCE SETTINGS ==' AS section;
SELECT @@global.log_replica_updates AS log_replica_updates,
       IF(@@global.log_replica_updates = 1, 'PASS', 'FAIL — replicated rows will NOT reach this binlog') AS verdict;

SELECT '== 5. RETENTION AND VOLUME ==' AS section;
SELECT @@global.binlog_expire_logs_seconds AS binlog_expire_logs_seconds,
       ROUND(@@global.binlog_expire_logs_seconds / 3600, 1) AS retention_hours,
       @@global.max_binlog_size AS max_binlog_size;
SHOW BINARY LOGS;
-- Rough write-rate context (counters since last restart):
SELECT variable_name, variable_value
FROM performance_schema.global_status
WHERE variable_name IN ('Uptime', 'Com_commit', 'Com_insert', 'Com_update', 'Com_delete', 'Questions');

SELECT '== 6. XA USAGE (any nonzero Com_xa_* or pending rows is a CAPSTAN BLOCKER today — flag it) ==' AS section;
SELECT variable_name, variable_value
FROM performance_schema.global_status
WHERE variable_name IN ('Com_xa_start', 'Com_xa_prepare', 'Com_xa_commit', 'Com_xa_rollback');
XA RECOVER;

SELECT '== 7. SCHEMA CENSUS (application schemas only) ==' AS section;
SELECT 'column type counts' AS census;
SELECT DATA_TYPE, COUNT(*) AS columns_count
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA NOT IN ('mysql', 'sys', 'information_schema', 'performance_schema')
GROUP BY DATA_TYPE
ORDER BY columns_count DESC;

SELECT 'columns capstan HALTS on today (SET, spatial) — each one blocks its table' AS census;
SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA NOT IN ('mysql', 'sys', 'information_schema', 'performance_schema')
  AND DATA_TYPE IN ('set', 'geometry', 'point', 'linestring', 'polygon',
                    'multipoint', 'multilinestring', 'multipolygon', 'geometrycollection')
ORDER BY TABLE_SCHEMA, TABLE_NAME;

SELECT 'columns to verify with the capstan team (json, bit)' AS census;
SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA NOT IN ('mysql', 'sys', 'information_schema', 'performance_schema')
  AND DATA_TYPE IN ('json', 'bit')
ORDER BY TABLE_SCHEMA, TABLE_NAME;

SELECT 'zero-date DEFAULTs (legacy-era data risk — flag for the casting plan)' AS census;
SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, COLUMN_DEFAULT
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA NOT IN ('mysql', 'sys', 'information_schema', 'performance_schema')
  AND COLUMN_DEFAULT LIKE '0000-00-00%';

SELECT 'tables WITHOUT a primary key (complicates idempotent/effect-once application downstream)' AS census;
SELECT t.TABLE_SCHEMA, t.TABLE_NAME
FROM information_schema.TABLES t
LEFT JOIN information_schema.TABLE_CONSTRAINTS c
  ON  c.TABLE_SCHEMA = t.TABLE_SCHEMA
  AND c.TABLE_NAME = t.TABLE_NAME
  AND c.CONSTRAINT_TYPE = 'PRIMARY KEY'
WHERE t.TABLE_SCHEMA NOT IN ('mysql', 'sys', 'information_schema', 'performance_schema')
  AND t.TABLE_TYPE = 'BASE TABLE'
  AND c.CONSTRAINT_NAME IS NULL
ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME;

SELECT '== 8. TLS AND AUTH ==' AS section;
SELECT @@global.require_secure_transport AS require_secure_transport,
       @@global.tls_version AS tls_version,
       @@global.character_set_server AS character_set_server;
SELECT VARIABLE_NAME, VARIABLE_VALUE
FROM performance_schema.global_variables
WHERE VARIABLE_NAME IN ('authentication_policy', 'default_authentication_plugin', 'have_ssl');

SELECT '== 9. EXISTING DOWNSTREAM REPLICAS (capstan needs a server_id colliding with NONE of these) ==' AS section;
SHOW REPLICAS;

SELECT '== DONE — see the questions below ==' AS section;

-- ---------------------------------------------------------------------------
-- Questions the SQL cannot answer — please include with the report:
--
--  1. Which server should capstan tail long-term? (For a planned cutover: tailing
--     the replica NOW carries the GTID checkpoint through promotion unchanged.)
--  2. How is the legacy->8.0 replication done (native replication, a tool, dumps)?
--     What is the cutover plan and window?
--  3. Does any application code use XA transactions (distributed commits)?
--  4. Are card numbers / PANs stored raw or tokenized in the replicated schemas?
--     (Determines PCI scope of the change stream itself.)
--  5. Where should the data land (warehouse, reporting DB, files) and how fresh
--     must it be?
--  6. Peak sustained write rate (txn/s) and the largest single transactions
--     (bulk jobs, settlement batches)?
--  7. Is history needed at the destination (backfill of existing rows), or only
--     changes from a start date?
--
-- Account capstan will need (for later — do NOT create it during discovery):
--
--   CREATE USER 'capstan'@'%' IDENTIFIED WITH caching_sha2_password BY '<secret>';
--   GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO 'capstan'@'%';
--
-- If capstan will run an INITIAL SNAPSHOT (backfill of pre-existing rows — question 7),
-- the account additionally needs LOCK TABLES on the snapshot tables (capstan takes a BRIEF
-- per-chunk `LOCK TABLES <t> READ` to capture an exact GTID position; it does NOT need the
-- global RELOAD/FLUSH TABLES WITH READ LOCK privilege):
--
--   GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT, LOCK TABLES ON *.* TO 'capstan'@'%';
--   -- or scope LOCK TABLES + SELECT to the specific snapshot schema(s).
--
-- Note: the snapshot supports integer / BINARY / VARBINARY / composite primary keys; a
-- collation-ordered string PK (CHAR/VARCHAR/TEXT) is refused (specify order-faithful-PK tables
-- for backfill), and an explicit snapshot table list is required.
-- ---------------------------------------------------------------------------

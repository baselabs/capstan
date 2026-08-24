-- Destination-side tables. Applied once at first container init.

-- One durable checkpoint row per pipeline: the processed GTID set (capstan's
-- sole persisted position — see hexdocs.pm/capstan).
CREATE TABLE IF NOT EXISTS capstan_checkpoint (
  pipeline_id VARCHAR(64) NOT NULL PRIMARY KEY,
  gtid_set    TEXT        NOT NULL,
  updated_at  TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Append-only, value-free delivery receipts — GTID, schema, table, operation,
-- commit timestamp. NEVER a row value (capstan Rule 1 upheld at the
-- destination too). Deliberately not deduplicated: at-least-once
-- re-deliveries across a crash window (lib-owned checkpoint mode) must be
-- VISIBLE here; the tie-out dedups at analysis time, and the mirror stays
-- exactly-once via idempotent upserts.
CREATE TABLE IF NOT EXISTS cdc_receipts (
  id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  txn_gtid    VARCHAR(64)  NOT NULL,
  schema_name VARCHAR(64)  NOT NULL,
  table_name  VARCHAR(64)  NULL,
  op          VARCHAR(32)  NOT NULL,
  commit_ts   DATETIME     NULL,
  received_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  KEY idx_receipts_gtid (txn_gtid),
  KEY idx_receipts_table (schema_name, table_name)
);

-- The idempotent mirror of example_src.orders (materialized by upsert on id).
CREATE TABLE IF NOT EXISTS orders (
  id         INT          NOT NULL PRIMARY KEY,
  note       VARCHAR(128) NOT NULL,
  updated_at TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

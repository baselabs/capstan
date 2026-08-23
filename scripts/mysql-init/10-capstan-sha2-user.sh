#!/usr/bin/env bash
# capstan dev substrate — seed the caching_sha2 replication account the pipeline authenticates as.
#
# The SINGLE definition of the replication user (grants included). Runs once, at first container
# init, from /docker-entrypoint-initdb.d/ (the official image's entrypoint executes *.sh/*.sql here
# against the temporary init server, BEFORE the networked server accepts traffic — so there is no
# init-race with a post-up SQL step). Idempotent (CREATE USER IF NOT EXISTS + ALTER) so a re-run is
# safe. Persists in the data volume; re-runs only if the volume is wiped.
#
# LOCK TABLES is REQUIRED for the C2 snapshot brief-lock capture — it is part of this account's
# contract, not optional. Credentials are THROWAWAY local-only defaults, never a real secret.
set -euo pipefail

USER="${CAPSTAN_SHA2_USER:-capstan_sha2}"
PASS="${CAPSTAN_SHA2_PASSWORD:-capstan_sha2_pw}"

mysql -uroot -p"${MYSQL_ROOT_PASSWORD}" <<SQL
CREATE USER IF NOT EXISTS '${USER}'@'%' IDENTIFIED WITH caching_sha2_password BY '${PASS}';
ALTER USER '${USER}'@'%' IDENTIFIED WITH caching_sha2_password BY '${PASS}';
GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT, LOCK TABLES ON *.* TO '${USER}'@'%';
# XA_RECOVER_ADMIN: capstan's connect-time XA RECOVER enumeration (ADR-0006 §4 —
# the xa: :track pre-seed; MySQL 8.0+ gates XA RECOVER behind this dynamic privilege).
GRANT XA_RECOVER_ADMIN ON *.* TO '${USER}'@'%';
SQL

echo "[capstan-init] caching_sha2 user '${USER}' seeded (REPLICATION SLAVE, REPLICATION CLIENT, SELECT, LOCK TABLES)"

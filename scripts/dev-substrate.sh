#!/usr/bin/env bash
# capstan dev MySQL substrate — thin wrapper over ../docker-compose.yml (the SINGLE SOURCE OF TRUTH
# for server flags, ports, and the capstan_sha2 replication user). This script exists so the
# documented entry point keeps working; the container definitions live in compose, not here, so there
# is exactly one place to change them.
#
# Two servers (see docker-compose.yml; tunables in .env — copy from env.example):
#   mysql-cdc-probe     MySQL 8.0  127.0.0.1:5633   root mysql_native_password; capstan_sha2 caching_sha2
#   mysql-cdc-probe-84  MySQL 8.4  127.0.0.1:5634   root caching_sha2 (Q6 posture); capstan_sha2 caching_sha2
#
# The exact server variables C1's fail-closed precondition gate requires (binlog ROW/FULL/FULL,
# gtid-mode ON, enforce-gtid-consistency ON) live in the compose `command:` blocks. The caching_sha2
# replication user (with LOCK TABLES, which C2's snapshot brief-lock needs) is seeded by
# scripts/mysql-init/ at first container init. Credentials are THROWAWAY for disposable LOCAL
# containers — not secrets, never reused anywhere.
#
# forge rule: never restart or duplicate a LIVE server. `docker compose up -d` is idempotent — a
# running, config-unchanged container is LEFT UNTOUCHED.
#
# Usage:
#   scripts/dev-substrate.sh              # bring both servers up + wait until healthy
#   scripts/dev-substrate.sh --only-80    # just 8.0 (all of C1 up to Task 18 needs only this)
set -euo pipefail
cd "$(dirname "$0")/.."

services=(mysql-80 mysql-84)
[ "${1:-}" = "--only-80" ] && services=(mysql-80)

# One-time cut-over guard. A container from the pre-compose era (created by `docker run`, carrying no
# compose project label) occupies the same name and would make `compose up` fail with a cryptic
# collision. Detect it and print the one action needed, rather than the raw error. Removing it
# restarts that server, so it is deliberately the operator's call — never done automatically here.
for name in mysql-cdc-probe mysql-cdc-probe-84; do
  cid="$(docker ps -aq -f "name=^${name}\$" 2>/dev/null || true)"
  [ -n "$cid" ] || continue
  label="$(docker inspect "$cid" --format '{{index .Config.Labels "com.docker.compose.project"}}' 2>/dev/null || true)"
  if [ -z "$label" ]; then
    echo "  [!] '${name}' exists but was NOT created by compose (legacy 'docker run')."
    echo "      One-time cut-over (restarts that server): docker rm -f ${name}"
    echo "      Then re-run this script — compose will recreate it under management."
    exit 1
  fi
done

echo "capstan dev substrate (docker compose):"
docker compose up -d --wait "${services[@]}"

echo
docker compose ps

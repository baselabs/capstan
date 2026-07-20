#!/usr/bin/env bash
# capstan dev MySQL substrate — the reproducible replacement for the hand-run probe container.
#
# Stands up (idempotently) the local MySQL servers the suite streams CDC from, with the exact
# server variables C1's fail-closed precondition gate (plan Task 4 / design Q5) requires, plus the
# caching_sha2_password replication user Task 2's auth path authenticates as. Nothing here hard-codes
# a server UUID — every consumer reads the UUID live, because a recreated container gets a new one.
#
# Two servers:
#   mysql-cdc-probe     MySQL 8.0.x  127.0.0.1:5633   root stays mysql_native_password so the
#                                                      probe/ diagnostics (native-root) keep working;
#                                                      capstan_sha2 is the caching_sha2 replication user.
#   mysql-cdc-probe-84  MySQL 8.4.x  127.0.0.1:5634   8.4 removed built-in mysql_native_password, so
#                                                      root is caching_sha2 — exercises Q6's default posture.
#
# Idempotent: a server already running is LEFT UNTOUCHED (never restart or duplicate a live server —
# forge rule); only an absent server is created. The replication user is (re)applied on every run.
#
# Credentials below are THROWAWAY for disposable LOCAL containers — not secrets, never reused anywhere.
#
# Usage:
#   scripts/dev-substrate.sh              # bring both servers up + ensure the replication user
#   scripts/dev-substrate.sh --only-80    # just 8.0 (all of C1 up to Task 18 needs only this)
set -euo pipefail

MYSQL_IMAGE_80="${MYSQL_IMAGE_80:-mysql:8.0}"
MYSQL_IMAGE_84="${MYSQL_IMAGE_84:-mysql:8.4}"
ROOT_PW="${MYSQL_ROOT_PASSWORD:-probe}"
SHA2_USER="capstan_sha2"
SHA2_PW="capstan_sha2_pw"   # throwaway — disposable local container only, never a real secret

# The server variables C1 requires (design Q5 / plan Task 4 precondition gate). row_value_options is
# set empty (its default) explicitly, so the artifact documents the full-JSON requirement rather than
# leaning on a default that a future image could change.
COMMON_FLAGS=(
  --binlog-format=ROW
  --binlog-row-image=FULL
  --binlog-row-metadata=FULL
  --binlog-row-value-options=
  --gtid-mode=ON
  --enforce-gtid-consistency=ON
)

start_server() {
  local name="$1" image="$2" port="$3" server_id="$4"; shift 4
  local extra_flags=("$@")
  if [ -n "$(docker ps -q -f "name=^${name}\$")" ]; then
    echo "  [$name] already running on :$port — left untouched"
    return 0
  fi
  if [ -n "$(docker ps -aq -f "name=^${name}\$")" ]; then
    echo "  [$name] exists but stopped — starting"
    docker start "$name" >/dev/null
    return 0
  fi
  echo "  [$name] creating ($image) on 127.0.0.1:$port"
  docker run -d --name "$name" \
    -p "127.0.0.1:${port}:3306" \
    -e "MYSQL_ROOT_PASSWORD=${ROOT_PW}" \
    -e MYSQL_DATABASE=probe_db \
    "$image" \
    "${COMMON_FLAGS[@]}" --server-id="$server_id" "${extra_flags[@]}" >/dev/null
}

# Ready = the REAL networked server answers an authenticated TCP query. The official image's
# entrypoint runs a temporary init server first (socket-only, --skip-networking), and `mysqladmin
# ping` answers on THAT one too — so a ping-based wait races the init, and the user-creation that
# follows then hits a half-initialized server (observed: create failed, script aborted). A TCP probe
# (forced via -h127.0.0.1) only the real, network-bound server accepts; --get-server-public-key lets
# it complete caching_sha2 auth over the plaintext local probe (8.4's root is caching_sha2).
wait_ready() {
  local name="$1"
  printf '  [%s] waiting for ready' "$name"
  for _ in $(seq 1 60); do
    if docker exec "$name" mysql -h127.0.0.1 -P3306 --get-server-public-key \
         -uroot -p"${ROOT_PW}" -N -e 'SELECT 1' >/dev/null 2>&1; then
      printf ' — up\n'; return 0
    fi
    printf '.'; sleep 2
  done
  printf '\n  [%s] never became ready\n' "$name" >&2
  return 1
}

ensure_sha2_user() {
  local name="$1"
  docker exec -i "$name" mysql -uroot -p"${ROOT_PW}" 2>/dev/null <<SQL
CREATE USER IF NOT EXISTS '${SHA2_USER}'@'%' IDENTIFIED WITH caching_sha2_password BY '${SHA2_PW}';
ALTER USER '${SHA2_USER}'@'%' IDENTIFIED WITH caching_sha2_password BY '${SHA2_PW}';
GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO '${SHA2_USER}'@'%';
SQL
  echo "  [$name] caching_sha2 user '${SHA2_USER}' ensured (REPLICATION SLAVE, REPLICATION CLIENT, SELECT)"
}

ONLY=""
if [ "${1:-}" = "--only-80" ]; then
  ONLY=80
fi

echo "capstan dev substrate:"

# 8.0 keeps the native-password default so the native-root probe/ diagnostics keep working.
start_server mysql-cdc-probe "$MYSQL_IMAGE_80" 5633 1 --default-authentication-plugin=mysql_native_password
wait_ready mysql-cdc-probe
ensure_sha2_user mysql-cdc-probe

if [ "$ONLY" != "80" ]; then
  # 8.4 removed built-in mysql_native_password — no default-auth flag (it would error); root is caching_sha2.
  start_server mysql-cdc-probe-84 "$MYSQL_IMAGE_84" 5634 2
  wait_ready mysql-cdc-probe-84
  ensure_sha2_user mysql-cdc-probe-84
fi

echo
echo "Ready:"
echo "  8.0  127.0.0.1:5633  root/${ROOT_PW} (native)        |  ${SHA2_USER}/${SHA2_PW} (caching_sha2)"
if [ "$ONLY" != "80" ]; then
  echo "  8.4  127.0.0.1:5634  root/${ROOT_PW} (caching_sha2)  |  ${SHA2_USER}/${SHA2_PW} (caching_sha2)"
fi

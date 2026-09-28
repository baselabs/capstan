# Testing & the local substrate

capstan's tests run against real MySQL — the protocol, the fail-closed preconditions, and the
snapshot brief-lock are only meaningfully proven against a live server. This describes the test
environment and how to run each tier.

## The substrate

The suite connects to MySQL servers the environment provides; it never starts one. This
repository ships no database container. Three servers, all on `127.0.0.1`:

| Service | Version | Host port | Root auth | Purpose |
|---|---|---|---|---|
| `mysql-80` | 8.0 | `MYSQL_PORT_80` (default `11619`) | `mysql_native_password` | the shared substrate every live/integration marquee streams from |
| `mysql-84` | 8.4 | `MYSQL_PORT_84` (default `15401`) | `caching_sha2_password` | exercises 8.4's default auth posture |
| disposable | 8.0 | `CAPSTAN_DISPOSABLE_MYSQL_PORT` (no default) | `mysql_native_password` | the `:disposable_mysql` marquees reset it (binlog purge, compression, PARTIAL_JSON) |

Both run with the exact server variables capstan's precondition gate requires (`binlog_format=ROW`,
`binlog_row_image=FULL`, `binlog_row_metadata=FULL`, `binlog_row_value_options=''`, `gtid_mode=ON`,
`enforce_gtid_consistency=ON`). The `capstan_sha2` replication user — with `SELECT`, `LOCK TABLES`
(the C2 snapshot brief-lock needs it), `REPLICATION SLAVE`, `REPLICATION CLIENT` — is seeded at first
server init by [`scripts/mysql-init/`](../scripts/mysql-init), which the cluster and CI both mount.

### Providing the servers

**On your machine** (contributors), with Docker:

```bash
# the shared 8.0 substrate (seeds capstan_sha2 from scripts/mysql-init/)
docker run -d --name capstan-mysql-80 -p 127.0.0.1:11619:3306 \
  -e MYSQL_ROOT_PASSWORD=probe -e MYSQL_DATABASE=probe_db \
  -v "$PWD/scripts/mysql-init:/docker-entrypoint-initdb.d:ro" mysql:8.0 \
  --binlog-format=ROW --binlog-row-image=FULL --binlog-row-metadata=FULL \
  --binlog-row-value-options= --gtid-mode=ON --enforce-gtid-consistency=ON --server-id=1 \
  --default-authentication-plugin=mysql_native_password

# the disposable 8.0 (the destructive marquees reset it), marked disposable once it is up
docker run -d --name capstan-disposable -p 127.0.0.1:33061:3306 \
  -e MYSQL_ROOT_PASSWORD=probe -e MYSQL_DATABASE=probe_db mysql:8.0 \
  --binlog-format=ROW --binlog-row-image=FULL --binlog-row-metadata=FULL \
  --binlog-row-value-options= --gtid-mode=ON --enforce-gtid-consistency=ON --server-id=179 \
  --default-authentication-plugin=mysql_native_password
until docker exec capstan-disposable mysql -h127.0.0.1 -uroot -pprobe \
  -e 'CREATE DATABASE IF NOT EXISTS capstan_disposable' 2>/dev/null; do sleep 2; done
export CAPSTAN_DISPOSABLE_MYSQL_PORT=33061

# remove them when done
docker rm -f capstan-mysql-80 capstan-disposable
```

The 8.4 server is the same as the 8.0 one with `mysql:8.4`, port `15401` and
`--mysql-native-password=ON` in place of `--default-authentication-plugin=…` (then re-point root
to `mysql_native_password`, as CI's "Wait for MySQL and re-point root auth" step does).

The disposable server must be MySQL 8.0 and carry the `capstan_disposable` schema; the suite
refuses to reset any other server (a shared substrate port, a server whose `@@server_uuid` is a
shared one's, a server without the schema, a version other than 8.0). Destructive marquees take a
server-scoped lease (a MySQL named lock), so two runs against one disposable server take turns.

**In CI** the workflow starts all of them as runner containers (`.github/workflows/ci.yml`).

**On the maintainer's machine** the shared servers run in the BaseLabs cluster (namespace
`capstan`, managed from `~/Developer/infra/baselabs`) and the disposable one runs there too, in the
cluster's throwaway area; nothing is started with Docker. If a server is unreachable there, stop
and report it.

### Ports are env-driven — nothing is hard-coded

The host ports live in `.env` (copy from [`.env.example`](../.env.example)), read by the
Elixir suite: `config/runtime.exs` loads `.env` via
[Dotenvy](https://hexdocs.pm/dotenvy) and exposes the port as `Capstan.MysqlCase.shared_port/0`.
Keep `MYSQL_PORT_80` equal to the server's published port. `.env` is gitignored;
`.env.example` carries the committed defaults. A real environment variable overrides the `.env` value
(this is how CI sets the port).

## Running the tests

The gate is `mix compile --warnings-as-errors && mix test && mix quality`.

`mix test` excludes the substrate-dependent tags by default, so it runs with **no** MySQL:

| Command | What runs | Needs |
|---|---|---|
| `mix test` | unit tests (mock MySQL over a real loopback socket) | nothing |
| `mix test --only live` | live marquees — real protocol, exact-`G` capture, snapshot paging | the substrate up |
| `mix test --only integration` | end-to-end pipeline marquees (kill/restart, effect-once, fail-closed) | the substrate up |
| `mix test --only disposable_mysql` | destructive marquees (gap purge, bootstrap purge race, compression, PARTIAL_JSON, the disposable guard) | the disposable server, `CAPSTAN_DISPOSABLE_MYSQL_PORT` set |
| `mix quality` | `format --check-formatted` + `credo --strict` + `dialyzer` | nothing |

**Ordering caveat:** `--only live` and `--only integration` share one substrate. The live exact-`G`
proof plants transactions under fabricated source UUIDs, so running `live` before `integration` on the
same substrate can perturb GTID-sequence-sensitive integration assertions. Run `integration` on a
fresh substrate (a reset of the cluster's capstan servers is the infrastructure owner's call) if you
hit that. CI runs `live` and then `integration` against the same container.

## Trying it by hand

[`examples/print_consumer.exs`](../examples/print_consumer.exs) is a minimal consumer you can run
against the substrate to watch changes stream — see [`examples/README.md`](../examples/README.md).

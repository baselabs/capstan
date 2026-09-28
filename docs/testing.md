# Testing & the local substrate

capstan's tests run against real MySQL — the protocol, the fail-closed preconditions, and the
snapshot brief-lock are only meaningfully proven against a live server. This describes the test
environment and how to run each tier.

## The substrate

Two MySQL servers run in the BaseLabs cluster (namespace `capstan`, managed from
`~/Developer/infra/baselabs`); this repository ships no database container. Elsewhere, any MySQL
8.0/8.4 pair with the same flags, credentials and init script works:

| Service | Version | Host port | Root auth | Purpose |
|---|---|---|---|---|
| `mysql-80` | 8.0 | `MYSQL_PORT_80` (default `11619`) | `mysql_native_password` | the shared substrate every live/integration marquee streams from |
| `mysql-84` | 8.4 | `MYSQL_PORT_84` (default `15401`) | `caching_sha2_password` | exercises 8.4's default auth posture |

Both run with the exact server variables capstan's precondition gate requires (`binlog_format=ROW`,
`binlog_row_image=FULL`, `binlog_row_metadata=FULL`, `binlog_row_value_options=''`, `gtid_mode=ON`,
`enforce_gtid_consistency=ON`). The `capstan_sha2` replication user — with `SELECT`, `LOCK TABLES`
(the C2 snapshot brief-lock needs it), `REPLICATION SLAVE`, `REPLICATION CLIENT` — is seeded at first
server init by [`scripts/mysql-init/`](../scripts/mysql-init), which the cluster and CI both mount.

If a server is unreachable, stop and report it; never start a local MySQL (removed September 28,
2026, after agent sessions kept starting databases outside the cluster).

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
| `mix test --only requires_docker` | tests that spin their own throwaway containers (purge/gap, compression) | Docker |
| `mix quality` | `format --check-formatted` + `credo --strict` + `dialyzer` | nothing |

**Ordering caveat:** `--only live` and `--only integration` share one substrate. The live exact-`G`
proof plants transactions under fabricated source UUIDs, so running `live` before `integration` on the
same substrate can perturb GTID-sequence-sensitive integration assertions. Run `integration` on a
fresh substrate (a reset of the cluster's capstan servers is the infrastructure owner's call) if you
hit that, or run the tiers in separate substrate lifecycles as CI does.

## Trying it by hand

[`examples/print_consumer.exs`](../examples/print_consumer.exs) is a minimal consumer you can run
against the substrate to watch changes stream — see [`examples/README.md`](../examples/README.md).

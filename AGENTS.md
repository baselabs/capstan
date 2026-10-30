# capstan — AI Agent & Contributor Guide

How to work effectively in this repo. The Critical Rules are binding.

## What this is

A framework-agnostic Elixir **MySQL** CDC library — replicant's MySQL sibling (replicant
does Postgres logical replication). It connects to MySQL as a replica, tails the **row-based
binary log** from a GTID position, assembles committed transactions, delivers them to a
pluggable sink **effect-once**, durably advances a **processed-GTID checkpoint**, and **halts
fail-closed** on every condition that could otherwise lose or corrupt data silently.

**Pure Elixir** — no Rust/NIF, no MySQL replication-protocol dependency. The protocol client is
in-library and probe-proven (`:gen_tcp`/`:ssl`/`:crypto` only; `decimal` + `jason` + `telemetry`).
Elixir `~> 1.15`. **1.3.1 is the latest release; never pin 1.3.0** (its aurora-sim compose is
first-boot broken). The rules below are grounded in live MySQL probes under `probe/`.
`docs/ROADMAP.md` carries authored scope; current work comes from the GitHub queue
`baselabs/capstan`. `.forge/` is historical. Dated narratives: `docs/reference/agent-history.md`.

## Critical rules

**1. No user or secret value in an error, log, or telemetry event (design Q15/Q16).** Assume every
value is PII or a secret. **Four leak vectors** are bound: (a) row values, (b) **DDL statement text**
(redacted before a `%SchemaChange{}`), (c) **`ROWS_QUERY_LOG_EVENT`** (decoded and discarded, never
retained), (d) the **connection password**. Column names stay **strings** — never `String.to_atom`
(a wide or attacker-influenced schema would exhaust the atom table). Telemetry metadata is
allowlisted (GTIDs, table names, counts, durations, error classes — never values).

**2. Fail closed on server preconditions (design Q5; extended by ADR-0013).** Refuse to start
unless the source's row-image binlog is configured for lossless CDC. The gate checks **six**
variables and refuses with a distinct reason per failure: `binlog_format=ROW`,
`binlog_row_image=FULL`, `binlog_row_metadata=FULL`, `binlog_row_value_options=''` (full JSON, not
PARTIAL_JSON), `gtid_mode=ON`, `log_bin=ON` (text `"1"` over the protocol; on Aurora the one
variable that names a disabled binlog, since `binlog_format` still reads `ROW`; refusal
`:binlog_disabled`). `enforce_gtid_consistency=ON` is required on the server but not gated.
Simple-query results are all **strings** — coerce every value as text before comparing.
`binlog_transaction_compression` is deliberately **not** gated: compressed transactions are
**consumed** by the in-library pure-Elixir zstd decoder, byte-exactly (ADR-0011); a malformed or
non-ZSTD payload halts fail-closed at decode time.

**3. Position is a GTID set — the sole authority and sole persisted value (design Q12).** Dedup is
set **membership**, not an ordinal comparison. `%Position{}` also carries `file`/`pos`, which are
**diagnostic only and never an ordering key**; there is **no exported ordinal**. Multi-source
replication is supported by construction. **The `COM_BINLOG_DUMP_GTID` interval end bound is
EXCLUSIVE**: a checkpoint of `uuid:1-11` encodes as `start=1, end=12`
(`probe/gtid_interval_bound_probe.exs`). An off-by-one skips or replays one transaction per
interval on every restart, **silently**.

**4. Exactly-once is at-least-once + an effect-once sink.** The checkpoint is a **processed
watermark** — every committed GTID *processed* (delivered OR filtered), not a delivery log (design
Q14), so the set stays a compact interval and advances through filtered quiet periods. Skip any
transaction whose GTID is already in the set (`Capstan.Gtid.member?/2`); hand-rolling membership is
unsafe. Do not claim naked exactly-once without an idempotent sink.

**5. Transaction shape is explicit; unknown shapes halt (design Q13).** Three terminators close a
transaction: `XID`; `QUERY("COMMIT")` (non-transactional engines); and the **self-committing DDL
`QUERY`** (`GTID → QUERY(DDL)` with no `BEGIN`/`XID`), which advances the checkpoint and yields
`%SchemaChange{}`. **`XA_PREPARE_LOG_EVENT` halts `:unsupported_transaction_shape` under the default
`xa: :refuse` — its rows must NEVER be delivered** (they may later roll back). Opt-in `xa: :track`
(ADR-0006) replaces the halt with prepare-pool tracking under the held-out watermark: a prepare GTID
checkpoints ONLY in the same write as its resolution GTID; XID bytes are pooled under a sha256 key
and never emitted (Rule 1). Any unknown event type fails closed, never silently skipped.

**6. `TABLE_MAP` is NOT authoritative for the next row event (design Q3).** A multi-table statement
emits `Table_map(94 ta) → Table_map(93 tb) → Update_rows(94) → Update_rows(93)`. Schema resolution
is a **`table_id`-keyed registry**, invalidated on `ROTATE` and `FORMAT_DESCRIPTION` (`table_id` is
unstable across DDL and reused). An unmapped `table_id` halts `:unmapped_table_id`. Column
**signedness** comes from optional-metadata TLV type 1, **not** the type byte (a `BIGINT UNSIGNED`
decodes as `-1` without it); `ENUM`/`SET` both arrive as type 254, distinguished only via the
STRING meta pair.

**7. TLS verification is an explicit operator choice, never a silent default (design Q17).** OTP 28
defaults `:ssl.connect` to `verify_peer` and MySQL's auto-generated cert is self-signed, so `ssl:
true` requires EITHER a `cacertfile` OR an explicit `verify: :verify_none` (documented as
confidentiality without authentication). Neither given → fail closed `:tls_verification_unspecified`.

## Workflow

Make the change, run the tests that cover it, commit with named paths, and push to `main`. CI runs
once on `main`; if it goes red, fix forward. Publishing, deploying and pushes to mirrors need the
owner's approval for that action.

- A test that fails on every run is a defect: fix it, or quarantine it with a reason, in the
  change that finds it.
- Update the CHANGELOG when behavior, a dependency or a contract changes; other docs only when
  a contract changes.

CI runs: `mix compile --warnings-as-errors`, `mix format --check-formatted`, `mix credo --strict`,
`mix test` (plus the `live`, `integration`, `disposable_mysql` and `aurora_sim` tiers) and
`mix dialyzer`; `mix quality` bundles format, credo and dialyzer. Run the ones that cover your change.

**Publish gate:** a version bump or Hex publish requires a FULL documentation sweep first — every
doc in the `mix.exs` `files:` set (including `notebooks/getting_started.livemd` and both
`examples/` READMEs) read end-to-end against the released behavior, version pins and instructions
(read, not grep-checked), plus the relative-link sweep over the shipped set (zero broken;
repo-only targets link absolutely). Stale text lives in the docs a change never touched.

## Testing

- **Unit + real-byte conformance** (`test/**/*_test.exs`): decode REAL captured binlog bytes
  (`test/fixtures/binlog/`, captured from the live substrate) — never self-signed fixtures.
- **Integration** (`test/integration/**`, `:integration`-tagged) runs on the live substrate, which
  must be reachable before `mix test --only integration`.
- **Fail-closed properties get tripwire tests** — the protected mutation itself, proven RED first
  (rename the key, fabricate the foreign `table_id`, tamper a CRC byte, feed a purged range).
- **Managed-source tiers** (`test/integration/aurora_*`, `:aurora_mysql`/`:aurora_sim`, excluded by
  default): the cluster tier runs only against a real Aurora cluster via `AURORA_MYSQL_*`; the
  simulator tier runs against `scripts/aurora-sim/` (real MySQL nodes behind a flipping endpoint).
  Both fail loudly when their environment is absent, never a silent pass.

## Local substrate

MySQL 8.0 and 8.4 run in the BaseLabs cluster (namespace `capstan`, managed from
`~/Developer/infra/baselabs`) with the precondition variables. The `caching_sha2_password`
replication user (with `LOCK TABLES`, for the C2 snapshot brief-lock) is seeded by
**`scripts/mysql-init/`**, mounted by the cluster and CI. Ports live in **`.env`** (`MYSQL_PORT_80`
/ `MYSQL_PORT_84`, `11619` / `15401`), read through Dotenvy in `config/runtime.exs`.
`CAPSTAN_SUBSTRATE_CA_FILE` points at the 8.0 server's `ca.pem` (exported to
`~/.config/baselabs/capstan/mysql-80-ca.pem`).

No database container ships here (removed September 28, 2026). On the maintainer's machine, if a
server is unreachable, stop and report it; never start a local MySQL or move a port. Elsewhere,
any MySQL 8.0/8.4 pair with the same flags, credentials and init script works (`docs/testing.md`).

Destructive marquees (`:disposable_mysql`) run on a third, **disposable** MySQL 8.0 on
`CAPSTAN_DISPOSABLE_MYSQL_PORT`, reset through `Capstan.MysqlCase.with_disposable_mysql/2` (a
server-scoped lease, then refusal unless the server is marked disposable by schema
`capstan_disposable`, is not a shared server by port or `@@server_uuid`, and is MySQL 8.0). CI
starts it as a runner container; `docs/testing.md` describes providing it.

- 8.0 `mysql-cdc-probe` @ `127.0.0.1:$MYSQL_PORT_80` — root is `mysql_native_password` (`probe/`
  authenticates as native root); replication user `capstan_sha2` / `capstan_sha2_pw`.
- 8.4 `mysql-cdc-probe-84` @ `127.0.0.1:$MYSQL_PORT_84` — root is `caching_sha2`; same
  `capstan_sha2` user (granted `XA_RECOVER_ADMIN` for `xa: :track`; servers seeded before that
  grant need it applied live).
- **Never restart, duplicate or start a shared server**; only the disposable server may be reset.
  No server UUID is hard-coded (read it live).
- Credentials are **throwaway** for disposable local containers — not secrets. `.env` is
  gitignored; never put a real password in it.

## Docs & artifacts

- **Tracked / publishable:** `AGENTS.md`, `CLAUDE.md`, `README.md`, `CHANGELOG.md`, `usage-rules.md`,
  `docs/adr/`, `docs/ROADMAP.md`, `docs/telemetry.md`, `docs/recipes.md`, the `examples/` READMEs,
  `notebooks/`, `scripts/`, `LICENSE`. The `mix.exs` `files:` list ships every consumer doc the
  README links; relative links in shipped docs must resolve inside the tarball.
- `.kimosabe/` is machine-local (local exclusion); `.forge/` is historical. Neither is a bare-clone
  dependency. AI-tool state dirs (`.claude/`, `.serena/`, `graphify-out/`) are gitignored.

## graphify (code knowledge graph)

`graphify-out/graph.json` maps this repo (tree-sitter AST; rebuilt by the post-commit hook). Prefer
`graphify query|explain|path` for orientation. Output is NAVIGATION, never evidence (Elixir edges
are file-local; alias-mediated calls are not resolved): verify load-bearing claims against live
code. `graphify update .` refreshes it after large uncommitted changes.

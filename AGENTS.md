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
Elixir `~> 1.15`.

**Current state:** C1 (streaming spine) and C2 (initial snapshot) are implemented
and released as `capstan` 0.2.0. C2 adds cursor-gated, resumable initial
backfill with the brief per-chunk read lock recorded in ADR 0005; omitting
`:snapshot` preserves the C1 stream. C3–C6 and the named C1/C2 follow-up rows
remain open in `docs/ROADMAP.md`. The rules below are binding invariants for the
landed code and future rows, grounded in live MySQL probes under `probe/`.
`docs/ROADMAP.md` carries authored definitions; `.forge/plans/` carries
machine-local task state.

## Critical rules

**1. No user or secret value in an error, log, or telemetry event (design Q15/Q16).** Assume every
value is PII or a secret. **Four leak vectors** are bound: (a) row values, (b) **DDL statement text**
(redacted before a `%SchemaChange{}`), (c) **`ROWS_QUERY_LOG_EVENT`** (decoded and discarded, never
retained), (d) the **connection password**. Column names stay **strings** — never `String.to_atom`
(a wide or attacker-influenced schema would exhaust the atom table). Telemetry metadata is
allowlisted (GTIDs, table names, counts, durations, error classes — never values).

**2. Fail closed on server preconditions (design Q5).** Refuse to start unless the source's
row-image binlog is configured for lossless CDC. The precondition gate checks **five** variables and
refuses with a distinct reason per failure: `binlog_format=ROW`, `binlog_row_image=FULL`,
`binlog_row_metadata=FULL`, `binlog_row_value_options=''` (full JSON, not PARTIAL_JSON),
`gtid_mode=ON`. (`enforce_gtid_consistency=ON` is also required on the server and the dev substrate
sets it; the Q5 gate itself checks the five above.) Simple-query results are all **strings** — coerce
every value as text before comparing.

**3. Position is a GTID set — the sole authority and sole persisted value (design Q12).** Dedup is
set **membership**, not an ordinal comparison. `%Position{}` also carries `file`/`pos`, which are
**diagnostic only and never an ordering key**. There is **no exported ordinal** (it was removed — it
was built from the very `file`/`pos` that failover breaks). Multi-source replication is supported by
construction (a GTID set expresses multiple source UUIDs). **The `COM_BINLOG_DUMP_GTID` interval end
bound is EXCLUSIVE**: a checkpoint of `uuid:1-11` encodes as `start=1, end=12` (probe-proven,
`probe/gtid_interval_bound_probe.exs`). An off-by-one here skips or replays exactly one transaction
per interval on every restart, **silently**.

**4. Exactly-once is at-least-once + an effect-once sink.** The checkpoint is a **processed
watermark** — it records every committed GTID *processed* (delivered OR filtered), not a delivery
log (design Q14). This keeps the set a compact interval and lets progress advance through filtered
quiet periods. Skip any transaction whose GTID is already in the set (`Capstan.Gtid.member?/2`);
hand-rolling membership is unsafe. Do not claim naked exactly-once without an idempotent sink.

**5. Transaction shape is explicit; unknown shapes halt (design Q13).** Three terminators close a
transaction: `XID`; `QUERY("COMMIT")` (non-transactional engines); and the **self-committing DDL
`QUERY`** (`GTID → QUERY(DDL)` with no `BEGIN`/`XID`, live-verified) which advances the checkpoint and
yields `%SchemaChange{}`. **`XA_PREPARE_LOG_EVENT` halts `:unsupported_transaction_shape` — its rows
must NEVER be delivered** (they may later roll back). Compressed transactions
(`TRANSACTION_PAYLOAD_EVENT`) halt loudly. Any unknown event type fails closed, never silently
skipped.

**6. `TABLE_MAP` is NOT authoritative for the next row event (design Q3).** A single multi-table
statement emits `Table_map(94 ta) → Table_map(93 tb) → Update_rows(94) → Update_rows(93)` — the map
right before a row event can be a *different* table. Schema resolution is a **`table_id`-keyed
registry**, invalidated on `ROTATE` and `FORMAT_DESCRIPTION` (`table_id` is unstable across DDL and
reused). An unmapped `table_id` halts `:unmapped_table_id`. Column **signedness** comes from
optional-metadata TLV type 1, **not** the type byte — a `BIGINT UNSIGNED` decodes as `-1` without it;
`ENUM`/`SET` both arrive as type 254 and are distinguished only via the STRING meta pair.

**7. TLS verification is an explicit operator choice, never a silent default (design Q17).** OTP 28
defaults `:ssl.connect` to `verify_peer` and MySQL's auto-generated cert is self-signed, so `ssl:
true` requires EITHER a `cacertfile` OR an explicit `verify: :verify_none` (documented as
confidentiality without authentication). Neither given → fail closed `:tls_verification_unspecified`.

## Development workflow

    mix deps.get
    mix format
    mix compile --warnings-as-errors
    mix test
    mix quality        # format --check-formatted + credo --strict + dialyzer

All gates must pass before a commit. `mix quality` and `mix audit` are defined in `mix.exs`.
Per-file floor on every touched file, **prod AND test**: format, compile (warnings-as-errors), credo.

## Testing

- **Unit + real-byte conformance** (`test/**/*_test.exs`): decode REAL captured binlog bytes
  (`test/fixtures/binlog/`, captured from the live substrate) — never self-signed fixtures. Write the
  test first; prove it RED before GREEN.
- **Integration** (`test/integration/**`, `:integration`-tagged): gate on the live substrate; a
  marquee never observed running is not evidence. Bring the substrate up with
  `scripts/dev-substrate.sh` (below) before running `mix test --only integration`.
- **Fail-closed properties get tripwire tests** — the protected mutation itself, proven RED first
  (rename the key, fabricate the foreign `table_id`, tamper a CRC byte, feed a purged range). A suite
  of happy paths passing green over a broken contract is the failure mode to avoid.

## Local substrate

The MySQL servers the suite streams from are defined ONCE in **`docker-compose.yml`** (the single
source of truth for the five precondition variables + ports + images). The `caching_sha2_password`
replication user — with `LOCK TABLES`, which the C2 snapshot brief-lock needs — is seeded at first
container init by **`scripts/mysql-init/`**. Tunables live in **`.env`** (gitignored; copy from
`.env.example`), so an unedited checkout reproduces the substrate exactly. **No port is hard-coded** —
`MYSQL_PORT_80` / `MYSQL_PORT_84` (default `11619` / `15401`) are read by docker-compose AND by the
Elixir suite (Dotenvy in `config/runtime.exs` → `Capstan.MysqlCase.shared_port/0`).

    docker compose up -d --wait            # both servers (ports from MYSQL_PORT_80/84), ready + user seeded
    docker compose up -d --wait mysql-80   # just 8.0
    docker compose down                    # stop + remove (add -v to wipe the data volumes)

`scripts/dev-substrate.sh` is a thin, forge-safe wrapper over compose (same `--only-80` flag); prefer
either — they drive the same definition.

    scripts/dev-substrate.sh            # both servers
    scripts/dev-substrate.sh --only-80  # just 8.0

- 8.0 `mysql-cdc-probe` @ `127.0.0.1:$MYSQL_PORT_80` — root is `mysql_native_password` (the `probe/`
  diagnostics authenticate as native root); replication user `capstan_sha2` / `capstan_sha2_pw`.
- 8.4 `mysql-cdc-probe-84` @ `127.0.0.1:$MYSQL_PORT_84` — 8.4 removed built-in `mysql_native_password`,
  so root is `caching_sha2` (exercises the default auth posture); same `capstan_sha2` user.
- **Never restart or duplicate a running server** — a live server is left untouched (`docker compose
  up -d` is idempotent). No server UUID is hard-coded (a recreated container gets a new one; read it
  live). Container names are stable (`handshake_test.exs` does `docker exec mysql-cdc-probe …`).
- Credentials are **throwaway** for disposable local containers — not secrets. `.env` is gitignored;
  never put a real password in it.

## Docs & lifecycle-artifact policy

- **Tracked / publishable:** `AGENTS.md`, `CLAUDE.md`, `README.md`, `CHANGELOG.md`, `usage-rules.md`,
  `docs/adr/`, `docs/ROADMAP.md`, `scripts/`, `LICENSE`.
- **Never tracked (machine-local):** everything under `.forge/` — design specs, plans, closeout
  reviews, handoffs, per-project memory, metrics, the dispatch ledger. `.forge/` is gitignored (forge
  default); a bare clone/CI will not have them. `docs/ROADMAP.md` carries **authored** facts only —
  status is DERIVED by `forge-roadmap.py`, never hand-edited.
- AI-tool state dirs (`.claude/`, `.serena/`, `graphify-out/`, etc.) are gitignored.

## graphify (code knowledge graph)

`graphify-out/graph.json` maps this repo (tree-sitter AST; rebuilt by the git post-commit hook; gitignored).

- For orientation ("where is X handled", "what connects A to B", "explain module M"), prefer `graphify query "<question>"` / `graphify explain "<Module>"` / `graphify path "<A>" "<B>"` over grep/Read fan-outs — one call returns a scoped subgraph with file:line hits.
- Graph output is NAVIGATION, never evidence. Edges reflect the last build, not the working tree, and cross-module call edges can be incomplete (Elixir: file-local only — alias-mediated calls are NOT resolved). Consumer sweeps and every load-bearing claim (review finding, plan anchor) still verify against live code: grep + file:line read.
- After large uncommitted changes, `graphify update .` refreshes the graph (AST-only, no API cost, no key).

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

**Current state:** C1 (streaming spine), C2 (initial snapshot), C3 (batching),
C4a/C4b (type breadth + compressed-transaction consumption), the C1a/C1b
position-ownership/start-position rows, C2a (collation-ordered string PKs),
C2b/C2c (`tables: :all` resolution + zero-row completion), XA `:track`, and C7
(**LANDED**: ADR-0013 Accepted — the six-variable gate, the failover-aware cycle
reset, Aurora as a named source verified on a real cluster —
`test/receipts/aurora-mysql-2026-09-30.md` — with the `:aurora_mysql` cluster tier
and the local `:aurora_sim` simulator tier, the latter green in CI on every push).
**1.3.1 is the latest release** (1.3.0 carried the Aurora work but shipped a
first-boot-broken aurora-sim compose; 1.3.1 fixes it — never pin 1.3.0; 1.1.1
carried C1+C2; the 1.2.0 span is additive; 1.2.1–1.2.3 are test-suite and docs
hardening — see CHANGELOG). `examples/replication_pipeline/` is the durable reference
docker stack (repo-side, CI-gated as the public sink API canary). Omitting
`:snapshot` preserves the C1 stream. C6 remains the generic sink-consumer conformance work definition;
the historical roadmap definitions do not reopen the delivered C5 and follow-up mechanisms. The rules below are binding invariants for the
landed code and future rows, grounded in live MySQL probes under `probe/`.
`docs/ROADMAP.md` carries authored scope and acceptance definitions. Current work
pickup uses the GitHub owner queue configured in `.kimosabe/config.toml`
(`baselabs/capstan`); reconcile existing work and receipts before adding tasks.
Historical `.forge/` plans are not the current queue.

## Critical rules

**1. No user or secret value in an error, log, or telemetry event (design Q15/Q16).** Assume every
value is PII or a secret. **Four leak vectors** are bound: (a) row values, (b) **DDL statement text**
(redacted before a `%SchemaChange{}`), (c) **`ROWS_QUERY_LOG_EVENT`** (decoded and discarded, never
retained), (d) the **connection password**. Column names stay **strings** — never `String.to_atom`
(a wide or attacker-influenced schema would exhaust the atom table). Telemetry metadata is
allowlisted (GTIDs, table names, counts, durations, error classes — never values).

**2. Fail closed on server preconditions (design Q5; extended by ADR-0013).** Refuse to start
unless the source's row-image binlog is configured for lossless CDC. The precondition gate checks
**six** variables and refuses with a distinct reason per failure: `binlog_format=ROW`,
`binlog_row_image=FULL`, `binlog_row_metadata=FULL`, `binlog_row_value_options=''` (full JSON, not
PARTIAL_JSON), `gtid_mode=ON`, `log_bin=ON` (the binary log itself — text `"1"` over the protocol;
on Aurora this is the one variable that names a disabled binlog, because `binlog_format` still
reads `ROW` when the cluster group turns logging off; refusal `:binlog_disabled`).
(`enforce_gtid_consistency=ON` is also required on the server and the dev substrate
sets it; the Q5 gate itself checks the six above.) Simple-query results are all **strings** — coerce
every value as text before comparing. `binlog_transaction_compression` is deliberately **not**
gated: compressed transactions are **consumed** — the in-library pure-Elixir zstd decoder inflates
each `TRANSACTION_PAYLOAD` event byte-exactly (ADR-0011's consume arm); a malformed or non-ZSTD
payload halts fail-closed at decode time.

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
yields `%SchemaChange{}`. **`XA_PREPARE_LOG_EVENT` halts `:unsupported_transaction_shape`
under the default `xa: :refuse` — its rows must NEVER be delivered** (they may later
roll back). The opt-in `xa: :track` (ADR-0006) replaces the halt with prepare-pool
tracking under the held-out watermark: a prepare GTID checkpoints ONLY in the same
write as its resolution GTID; XID bytes are pooled under a sha256 key and never
emitted (Rule 1). Compressed transactions
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

**Publish gate:** a version bump or Hex publish requires a FULL documentation sweep first —
every doc in the `mix.exs` `files:` set (including `notebooks/getting_started.livemd` and both
`examples/` READMEs) read end-to-end for correctness against the released behavior, current
version pins, and working instructions (not grep-checked — read), plus the mechanical relative-link
sweep over the shipped set (zero broken; repo-only targets link absolutely). An edit-only pass is
not a sweep: stale text lives in the docs a change never touched.
Per-file floor on every touched file, **prod AND test**: format, compile (warnings-as-errors), credo.

## Testing

- **Unit + real-byte conformance** (`test/**/*_test.exs`): decode REAL captured binlog bytes
  (`test/fixtures/binlog/`, captured from the live substrate) — never self-signed fixtures. Write the
  test first; prove it RED before GREEN.
- **Integration** (`test/integration/**`, `:integration`-tagged): gate on the live substrate; a
  marquee never observed running is not evidence. The substrate (below) must be reachable
  before running `mix test --only integration`.
- **Fail-closed properties get tripwire tests** — the protected mutation itself, proven RED first
  (rename the key, fabricate the foreign `table_id`, tamper a CRC byte, feed a purged range). A suite
  of happy paths passing green over a broken contract is the failure mode to avoid.
- **Managed-source tiers** (`test/integration/aurora_*`, `:aurora_mysql`/`:aurora_sim`-tagged,
  excluded by default): the Aurora cluster tier runs only against a real cluster via the
  `AURORA_MYSQL_*` environment; the simulator tier runs against the local
  `scripts/aurora-sim/` stack (real MySQL nodes behind a flipping endpoint — no AWS account).
  Both fail loudly when their environment is absent, never a silent pass.

## Local substrate

The MySQL 8.0 and 8.4 servers the suite streams from run in the BaseLabs cluster (namespace
`capstan`, managed from `~/Developer/infra/baselabs`) with the five precondition variables. The
`caching_sha2_password` replication user (with `LOCK TABLES`, which the C2 snapshot brief-lock
needs) is seeded by **`scripts/mysql-init/`**, which the cluster and CI both mount. Ports live in
**`.env`** (`MYSQL_PORT_80` / `MYSQL_PORT_84`, `11619` / `15401`), read by the Elixir suite through
Dotenvy in `config/runtime.exs`. `CAPSTAN_SUBSTRATE_CA_FILE` points at the 8.0 server's `ca.pem`
(the cluster exports it to `~/.config/baselabs/capstan/mysql-80-ca.pem`).

This repository ships no database container (the Compose file and its wrapper were removed
September 28, 2026, after agent sessions kept starting databases outside the cluster). On the
maintainer's machine, if a server is unreachable, stop and report it; never start a local MySQL
and never move a port. Elsewhere, any MySQL 8.0/8.4 pair with the same flags, credentials and init
script works (`docs/testing.md` gives the `docker run` lines).

The destructive marquees (`:disposable_mysql`) run on a third server, the **disposable** MySQL 8.0
on `CAPSTAN_DISPOSABLE_MYSQL_PORT`, which they reset through `Capstan.MysqlCase.with_disposable_mysql/2`
(a server-scoped lease, then refusal unless the server is marked disposable by schema
`capstan_disposable`, is not a shared server by port or `@@server_uuid`, and is MySQL 8.0). CI
starts it as a runner container; `docs/testing.md` describes providing it.

- 8.0 `mysql-cdc-probe` @ `127.0.0.1:$MYSQL_PORT_80` — root is `mysql_native_password` (the `probe/`
  diagnostics authenticate as native root); replication user `capstan_sha2` / `capstan_sha2_pw`.
- 8.4 `mysql-cdc-probe-84` @ `127.0.0.1:$MYSQL_PORT_84` — 8.4 removed built-in `mysql_native_password`,
  so root is `caching_sha2` (exercises the default auth posture); same `capstan_sha2` user
(the seed script grants it `XA_RECOVER_ADMIN` for the `xa: :track` connect-time
enumeration; long-lived containers seeded before that grant need it applied live).
- **Never restart, duplicate or start a shared server** — on the maintainer's machine they are the
  BaseLabs cluster's; the disposable server is the only one the suite may reset. No server
  UUID is hard-coded (a recreated server gets a new one; read it live). The TLS handshake test reads
  the 8.0 CA from `CAPSTAN_SUBSTRATE_CA_FILE`.
- Credentials are **throwaway** for disposable local containers — not secrets. `.env` is gitignored;
  never put a real password in it.

## Docs & lifecycle-artifact policy

- **Tracked / publishable:** `AGENTS.md`, `CLAUDE.md`, `README.md`, `CHANGELOG.md`, `usage-rules.md`,
  `docs/adr/`, `docs/ROADMAP.md`, `docs/telemetry.md`, `docs/recipes.md`, `examples/README.md`,
  `examples/replication_pipeline/README.md`, `notebooks/`, `scripts/`, `LICENSE` (the hex package's
  files list in `mix.exs` ships every consumer doc the README links; relative links in shipped docs
  must resolve inside the tarball — repo-only targets link absolutely).
- **Machine-local coordination:** `.kimosabe/` holds local configuration, memory, and
  evidence under the repository's local exclusion policy. Historical `.forge/` artifacts
  remain historical evidence, not current task state; neither is a bare-clone dependency.
  `docs/ROADMAP.md` carries authored scope/acceptance. The configured GitHub owner
  queue and actual receipts govern execution/completion; `forge-roadmap.py` is retired
  as the current pickup rule.
- AI-tool state dirs (`.claude/`, `.serena/`, `graphify-out/`, etc.) are gitignored.

## graphify (code knowledge graph)

`graphify-out/graph.json` maps this repo (tree-sitter AST; rebuilt by the git post-commit hook; gitignored).

- For orientation ("where is X handled", "what connects A to B", "explain module M"), prefer `graphify query "<question>"` / `graphify explain "<Module>"` / `graphify path "<A>" "<B>"` over grep/Read fan-outs — one call returns a scoped subgraph with file:line hits.
- Graph output is NAVIGATION, never evidence. Edges reflect the last build, not the working tree, and cross-module call edges can be incomplete (Elixir: file-local only — alias-mediated calls are NOT resolved). Consumer sweeps and every load-bearing claim (review finding, plan anchor) still verify against live code: grep + file:line read.
- After large uncommitted changes, `graphify update .` refreshes the graph (AST-only, no API cost, no key).

<!-- BEGIN AWS Agent Toolkit rules (local, uncommitted — new AWS experience) -->
# AWS Guidance for the new AWS experience

This user has signed up for the new AWS experience. This experience lets you sign into AWS using a social provider and requires the following additional context.

Where this guidance conflicts with the project's own instructions, the project's instructions take precedence.

## Context

### Terminology:

- Say "project" instead of "account" — a project contains an AWS account and settings for sharing with other collaborators
- Say "team member" instead of "IAM user" — users are invited by email, not created or federated in IAM
- Say "AWS Settings" when referring to management tasks at [settings.aws.com](https://settings.aws.com/) (project management, billing, team members, spend limits). Users view their actual AWS resources in the AWS Management Console.
- Say "selected Region" when referring to the user's Region — not "home Region"
- The user has a managed IAM experience. This includes a managed service control policies (SCP) and resource control policies (RCP) that govern the use of AWS. They will still need to use IAM to create policies to let services work with each other. If there are questions about the SCPs or RCPs, go to the documentation at https://docs.aws.amazon.com/accounts/latest/reference/scps-and-rcps-for-projects.html

### Constraints:

- All projects share a single AWS Region determined by the user's contact address. Resources cannot be created in other Regions
- When developing:
  - MUST create all Regional resources in the project's assigned Region
  - You CAN create AWS WAF and Cloudwatch Logs resources in us-east-1 when there are global resources (like a global WAF instance) that require a connection to dependencies in us-east-1. You should not use these for any other reason, because resources in the selected Region will provide lower cost (due to no cross-Region traffic), increased availability (due to no cross-Region traffic), and easier manageability (due to not needing to look in another Region). When you need to do an inventory of resources, you need to look in both the selected Region and us-east-1 for Cloudwatch Logs or WAF resources.
  - MUST NOT attempt to create Lambda, API Gateway, or other Regional resources in any other Region
  - MUST direct users to confirm their Region in AWS Settings > View all projects > Overview > Additional Info > Region. If the user cannot confirm their Region, check in ~/.aws/config
  - MUST NOT use Lambda@Edge — excluded from both Lambda and CloudFront
  - MUST NOT use CloudFormation StackSets — no multi-account or multi-Region deployments
  - MUST NOT attempt cross-Region actions — no cross-Region replication for DynamoDB/S3/RDS, no multi-Region KMS keys
  - MUST NOT use Route 53 cross-Region routing — geolocation, latency-based, and failover routing policies are not available
  - CloudFront is a global service and its actions ARE allowed in `us-east-1`. A user can create a CloudFront distribution pointing to their project-region Lambda function URL or API Gateway. However, Lambda and API Gateway themselves MUST NOT be created in `us-east-1` — they must be in the project Region.
  - Reduced availability in `eu-north-1` specifically: Amazon Rekognition, Amazon Textract, Amazon Personalize, AWS App Runner are not available in that Region.
- IAM permissions for human access are managed by AWS. Don't assign roles to team members unless absolutely necessary
- The user may have a spend limit if they are on the paid plan. The limit that pauses their project if it's exceeded. If resources suddenly become inaccessible, ask if they have a spend limit configured. Only project owners can modify a spend limit.
- When developing:
  - MUST ask about spend limit status if the user reports sudden "Access Denied" errors on operations that previously worked
  - MUST direct users to check spend status in AWS Settings > Billing
  - MUST check if a user has upgraded their account to the paid plan
  - MUST ask the user if they want to clean up the successfully created resources or keep them to reduce cost
- The user sets up billing, creates spend limits, and retrieves and pays invoices in AWS Settings. The user creates budgets and optimizes their costs in the AWS Billing and Cost Management console
- Not all AWS services are available. If a service isn't working, do the following:
  1. Run the command `aws freetier get-account-plan-state`
  2. If accountPlanType": "FREE", check the [Free Tier supported services list](https://docs.aws.amazon.com/accounts/latest/reference/supported-services-sign-up-new.html#supported-services-free-tier) next,
  3. If accountPlanType": "PAID", check the [Paid Tier supported services list](https://docs.aws.amazon.com/accounts/latest/reference/supported-services-sign-up-new.html#supported-services-paid-plan).
  4. If neither list shows the service, check the [Not supported for this experience list](https://docs.aws.amazon.com/accounts/latest/reference/supported-services-sign-up-new.html#unsupported-services). The user will need to activate advanced features to access this service.
- Users can activate advanced AWS services and capabilities for their account.
- Before starting a task, check whether a relevant AWS skill is available. Load the skill with retrieve_skill and prefer its guidance over general knowledge.

### Help level

- help_level (required): LOW, MEDIUM, or HIGH. While a user is building, you MUST ask the user: "How much guidance would you like from me? Low (I only flag security risks), medium (I ask a couple of clarifying questions if something seems off), or high (I explain what I'm doing, suggest alternatives, and flag best practices)."

You CAN update this rule file to save a user's help_level.

Constraints for each level:

**LOW:**

- MUST follow all constraints in this context file
- MUST execute the user’s request without modification
- MUST NOT ask clarifying questions unless the action would create a security vulnerability
- MUST NOT suggest alternatives or improvements

**MEDIUM:**

- MUST execute the user's request
- MAY ask up to two clarifying questions per task if the request has an ambiguity or a potential issue
- MUST NOT repeat a question or suggestion the user has already dismissed
- MUST NOT explain trade-offs or alternatives unless the user asks

**HIGH:**

- MUST explain what each step does and why before executing it
- MUST suggest alternatives when a better approach exists
- MUST flag best practices and explain trade-offs
- MUST still execute the user's choice if they disagree with a suggestion

<!-- END AWS Agent Toolkit rules -->

## Project contract

**The full agent & contributor contract is in [`AGENTS.md`](AGENTS.md) — read it first.** It carries
what capstan is, the seven binding Critical Rules (Rule 1 value-redaction, fail-closed preconditions,
the GTID-set position model, processed-watermark checkpoint, transaction-shape halts, the `table_id`
registry, TLS posture), the dev workflow, testing, and the local substrate.

Quick pointers:

- **Local MySQL substrate:** the BaseLabs cluster (namespace `capstan`; 8.0 and 8.4 on `127.0.0.1`,
  ports from `.env`, `MYSQL_PORT_80`/`MYSQL_PORT_84` = 11619/15401, with the `capstan_sha2`
  replication user). Never start a local MySQL; if a server is unreachable, stop and report it.
- **Gates:** `mix compile --warnings-as-errors && mix test && mix quality`.
- **Scope / pickup:** [`docs/ROADMAP.md`](docs/ROADMAP.md) defines authored scope and
  acceptance. Current executable work uses the GitHub owner queue selected by
  `.kimosabe/config.toml` (`baselabs/capstan`); reconcile existing work and receipts
  before adding tasks. The roadmap does not assert completion.
- **Local coordination:** `.kimosabe/` holds local memory/evidence/configuration.
  `.forge/` plans and derived status are historical, not the current work queue.

## graphify (code knowledge graph)

`graphify-out/graph.json` maps this repo (tree-sitter AST; rebuilt by the git post-commit hook; gitignored).

- For orientation ("where is X handled", "what connects A to B", "explain module M"), prefer `graphify query "<question>"` / `graphify explain "<Module>"` / `graphify path "<A>" "<B>"` over grep/Read fan-outs — one call returns a scoped subgraph with file:line hits.
- Graph output is NAVIGATION, never evidence. Edges reflect the last build, not the working tree, and cross-module call edges can be incomplete (Elixir: file-local only — alias-mediated calls are NOT resolved). Consumer sweeps and every load-bearing claim (review finding, plan anchor) still verify against live code: grep + file:line read.
- After large uncommitted changes, `graphify update .` refreshes the graph (AST-only, no API cost, no key).

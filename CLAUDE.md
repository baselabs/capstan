## Project contract

**The full agent & contributor contract is in [`AGENTS.md`](AGENTS.md) — read it first.** It carries
what capstan is, the seven binding Critical Rules (Rule 1 value-redaction, fail-closed preconditions,
the GTID-set position model, processed-watermark checkpoint, transaction-shape halts, the `table_id`
registry, TLS posture), the dev workflow, testing, and the local substrate.

Quick pointers:

- **Local MySQL substrate:** `scripts/dev-substrate.sh` (8.0 and 8.4 on `127.0.0.1`, ports from
  `.env` — `MYSQL_PORT_80`/`MYSQL_PORT_84`, defaults 11619/15401 — with the `capstan_sha2`
  replication user). Never restart or duplicate a running container.
- **Gates:** `mix compile --warnings-as-errors && mix test && mix quality`.
- **Status / scope:** `docs/ROADMAP.md` (authored) + `.forge/plans/` (task-level, machine-local).
  The plan is the source of truth — do not re-derive scope from the code.
- **forge artifacts** (design specs, plans, reviews, handoffs, memory, metrics) live under `.forge/`,
  which is **gitignored** — machine-local, not in a bare clone.

## graphify (code knowledge graph)

`graphify-out/graph.json` maps this repo (tree-sitter AST; rebuilt by the git post-commit hook; gitignored).

- For orientation ("where is X handled", "what connects A to B", "explain module M"), prefer `graphify query "<question>"` / `graphify explain "<Module>"` / `graphify path "<A>" "<B>"` over grep/Read fan-outs — one call returns a scoped subgraph with file:line hits.
- Graph output is NAVIGATION, never evidence. Edges reflect the last build, not the working tree, and cross-module call edges can be incomplete (Elixir: file-local only — alias-mediated calls are NOT resolved). Consumer sweeps and every load-bearing claim (review finding, plan anchor) still verify against live code: grep + file:line read.
- After large uncommitted changes, `graphify update .` refreshes the graph (AST-only, no API cost, no key).

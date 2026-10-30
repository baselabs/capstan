## Project contract

**The full agent & contributor contract is in [`AGENTS.md`](AGENTS.md) — read it first.** It carries
the seven binding Critical Rules, the workflow, testing, and the local substrate.

- **Local MySQL substrate:** the BaseLabs cluster (namespace `capstan`; 8.0 and 8.4 on `127.0.0.1`,
  ports from `.env`, `MYSQL_PORT_80`/`MYSQL_PORT_84` = 11619/15401, user `capstan_sha2`). Never start
  a local MySQL; if a server is unreachable, stop and report it. `:disposable_mysql` tests use
  `CAPSTAN_DISPOSABLE_MYSQL_PORT` (`docs/testing.md`).
- **Workflow:** change it, run the tests that cover it, commit with named paths, push to `main`;
  CI runs once there, fix forward if it goes red.
- **History:** `docs/reference/agent-history.md`.

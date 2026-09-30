# ADR-0013: Amazon Aurora MySQL as a named, tested source

**Status:** Proposed (2026-09-29; documentation-verified the same day — every INFERRED fact
below was checked against the AWS Aurora MySQL User Guide, and each row now names its source or
names the live probe that must settle it; see Evidence) · **Extends:**
[ADR-0002](0002-fail-closed-server-preconditions.md) (the connect-time gate gains `log_bin`)
· **Honors:** [ADR-0001](0001-position-and-dedup-model.md) (a failover adds a source UUID to
the set; nothing ordinal), [ADR-0003](0003-transaction-shape-and-checkpoint-semantics.md)
(error 1236 discrimination), [ADR-0009](0009-fail-closed-supervision-and-streaming-liveness.md)
(the two budgets), [ADR-0011](0011-transaction-compression-precondition.md) (compression is
consumed, and `binlog_transaction_compression` is a settable Aurora MySQL version 3 parameter)

## Context

capstan is documented against MySQL 8.0 and 8.4 servers the operator configures directly. Amazon
Aurora MySQL is the most common managed MySQL a consumer will point it at, and it differs from a
self-managed server in exactly the places the fail-closed design cares about: how the binlog is
turned on, how long binlog files live, which endpoint serves a dump, and what a failover does to
the server identity and the GTID stream. None of these differences is visible in the protocol;
each becomes visible only as a halt reason or as a silent gap. Naming Aurora as a source means
stating, per difference, which existing check catches it, what is added, and what is proved on a
real cluster.

Aurora MySQL facts this ADR builds on. Each is marked by how it is known: DOCUMENTED (read in
the AWS Aurora MySQL User Guide; the exact pages are named in Evidence), INFERRED (a reading
that a live probe on a real cluster must confirm before the row lands), OBSERVED (seen on a
live cluster — reserved for the run; no cell carries it yet).

| Aurora behavior | Known as | What it means for capstan |
|---|---|---|
| The binlog is enabled by setting `binlog_format` in the DB **cluster** parameter group (an instance-group edit silently does nothing), followed by a reboot of the writer; the group also accepts `OFF`, and "Setting `binlog_format` to `OFF` in the DB cluster parameter group disables the `log_bin` session variable. This disables binary logging on the Aurora MySQL DB cluster, which in turn resets the `binlog_format` session variable to the default value of `ROW` in the database." | DOCUMENTED (binary-logging page) | **The five-variable gate cannot catch a disabled Aurora binlog**: a binlog-disabled cluster still answers `binlog_format=ROW`, so the refusal only surfaces later at the dump. The gate gains `log_bin` (decision 1) — the one variable AWS names as the disabled marker. A `STATEMENT`/`MIXED` group still refuses `:binlog_format_not_row` as on any MySQL |
| `binlog_row_image` is a cluster parameter on Aurora MySQL versions 2 and 3; `binlog_row_metadata` and `binlog_row_value_options` are cluster parameters that "apply to Aurora MySQL version 3"; `binlog_transaction_compression` and `binlog_transaction_compression_level_zstd` likewise apply to version 3 | DOCUMENTED (cluster-parameter reference) | Version 2 is refused by construction: `binlog_row_metadata` is unknown on its MySQL 5.7 engine, so the six-variable query returns a server error (MySQL 1193, unknown system variable), which `Connection` spends against the command budget, halting `:command_retries_exhausted` — fail-closed, though less specific than a precondition reason. The version floor capstan documents is Aurora MySQL version 3. Compression being settable changes nothing: ADR-0011 consumes it |
| GTID-based replication is supported for Aurora MySQL "version 2 and 3"; the enabling recipe sets `gtid_mode` (`ON` or `ON_PERMISSIVE`) and `enforce_gtid_consistency` (`ON`) in the cluster parameter group, and `gtid_mode=ON` is what "applies … to outgoing replication from an Aurora MySQL cluster" — capstan's direction | DOCUMENTED (GTID pages) | `gtid_mode=ON` is already gated; a capstan source is exactly the outgoing-replication case the `ON` value names. `ON_PERMISSIVE` clusters refuse `:gtid_mode_not_on` (capstan requires fully-GTID transactions; deliberate, unchanged) |
| Binlog retention is governed by `binlog retention hours`, set with `CALL mysql.rds_set_configuration('binlog retention hours', N)` and read with `CALL mysql.rds_show_configuration`. "The default value of `binlog retention hours` is `NULL`. For Aurora MySQL, `NULL` means binary logs are cleaned up lazily. Aurora MySQL binary logs might remain in the system for a certain period, which is usually not longer than a day." The maximum "For Aurora MySQL version 2.11.0 and higher and version 3 DB clusters … is 2160 (90 days)"; `0` is not a valid value; the stored configuration "survive[s] any DB instance reboot or failover" | DOCUMENTED (rds-configuration procedures page) | A NULL retention makes `:data_gap` the normal outcome of any pause longer than about a day — the documented reason the Aurora recipe sets retention explicitly. The library does not call the `rds_` procedures (they do not exist off Aurora); the preflight reports the setting |
| Only the writer serves the binlog: "Binary logs are accessible only from the primary DB instance, not from the replicas." The cluster endpoint "connects to the current primary DB instance" and "during a failover … the DB cluster continues to serve connection requests to the cluster endpoint from the new primary DB instance", while "the physical IP address pointed to by the cluster endpoint changes when the failover mechanism promotes a new DB instance" | DOCUMENTED (binary-logging + cluster-endpoint pages) | A pipeline must connect to the cluster (writer) endpoint. A reader answers the gate's six variables from the shared parameter group and would pass the value checks; what it answers to `@@log_bin` and to `COM_BINLOG_DUMP_GTID` is still not documented — the probe decides whether `log_bin` also catches a reader (decision 1), and the gate refuses before the dump either way once it does |
| "The Aurora cluster volume contains all your user data, schema objects, and internal metadata such as the system tables and the binary log" — so the binlog lives on the shared volume; each instance is its own MySQL server with its own `@@server_uuid`/`@@server_id`; whether the promoted writer's `gtid_executed` still carries the OLD writer's executed GTIDs after a failover | volume: DOCUMENTED (storage page) · UUID continuity: INFERRED (undocumented; the live probe) | If the old UUID's GTIDs remain in `gtid_executed` on the new writer, `gap_check/3` passes and the checkpoint set gains a second source UUID, which ADR-0001 supports by construction. If they do not, the halt is `:source_identity_mismatch`, which is the correct fail-closed outcome and must be documented as such |
| Aurora assigns `@@server_id` per instance; a consumer's `server_id` must not collide with any instance's | DOCUMENTED | The existing `server_id` requirement applies; the preflight prints the instances' ids |
| Aurora's server certificate chains to the AWS RDS CA bundle and names the endpoint host | DOCUMENTED | Hostname verification is possible on Aurora, unlike the self-signed default ADR-0002 §4 describes; the documented posture is `cacertfile:` with the RDS bundle and `server_name_indication` left on |
| Aurora MySQL version 3.03.1+ offers enhanced binlog: `aurora_enhanced_binlog=1` in the cluster group (with `binlog_backup=0`, `binlog_replication_globaldb=0`, and `binlog_format` not `OFF`), and AWS states "The existing binlog consumers can continue to read and consume the binlog files without any gaps in the binlog file sequence." `binlog_transaction_compression` is settable (a version 3 cluster parameter, per the row above) | DOCUMENTED (enhanced-binlog page + parameter reference) · the decoded BYTES on an enhanced-binlog cluster: INFERRED (the probe) | ADR-0011 consumes compression already. Enhanced binlog's consumer-compatibility statement is AWS's claim, not capstan's observation: whether capstan's decoder sees the identical event stream on an enhanced-binlog writer is what the cluster run records, and the receipt names which mode the test cluster ran |

## Decision

Name Aurora MySQL version 3 as a supported source, with these additions and rules.

1. **The gate.** It gains `log_bin`: `Capstan.Config.check_preconditions/1` reads six variables in
   its one `COM_QUERY` (the five of ADR-0002 plus `log_bin`) and refuses `:binlog_disabled` when
   `log_bin` is not `ON`. On a self-managed server this catches a binlog never enabled
   (`skip-log-bin`); on Aurora it catches the cluster whose parameter group was never edited or
   was left at `binlog_format=OFF` — the case the documentation proves the five-variable gate
   cannot see, because `binlog_format` reads `ROW` while the binlog is disabled. Whether the
   same check also catches a reader is decided by the probe: if a reader reports `log_bin = ON`
   and refuses the dump, the dump refusal is classified by its message text under ADR-0003 and a
   text that names the reader role maps to a new `:binlog_source_not_writer`; if a reader
   reports `log_bin = OFF`, `:binlog_disabled` already covers it and no second reason is added.
   No reason is added that the probe did not show a need for.
2. **Identity and gap checks.** They apply unchanged. `Connection.gap_check/3`
   (checkpoint not a subset of `gtid_executed` halts `:source_identity_mismatch`; unapplied
   remainder intersecting `gtid_purged` halts `:data_gap`) is the whole failover and retention
   story. After a failover the checkpoint's GTIDs must appear in the new writer's
   `gtid_executed` or the pipeline halts, and that is the correct outcome. A retention window
   shorter than a pause halts `:data_gap`, and the operator raises `binlog retention hours`
   (documented maximum 2160 on version 3); capstan does not read or set that value itself.
3. **The cycle counter.** It learns to tell a failover from an eviction. When a reconnect observes a
   different `@@server_uuid` from the previous cycle (read through the existing
   `Config.read_server_uuid/1` on the stream socket) and `gap_check/3` passes, the cycle
   counter resets: the server changed, which an eviction by duplicate `server_id` never does.
   A cycle against the same `@@server_uuid` still counts, so `:server_id_conflict` keeps its
   meaning. The reset is value-free (`@@server_uuid` is structural identity, as `Capstan.Query`
   already treats it). One boundary is inherent and accepted: the reset fires at ESTABLISH,
   because the promoted writer's UUID is unknowable while the old socket is dead — so a
   same-UUID drop that already exhausted the budget before the failover's reconnect still
   halts (a sequence of failovers never accumulates; a failover cannot revive an exhausted
   budget).
4. **Start positions on Aurora.** The first-start rule ("an empty checkpoint requests full
   retained history") interacts badly with a NULL retention: `gtid_purged` is almost never empty,
   so a fresh start halts `:data_gap`. The documented Aurora recipe is `start_position: :current`
   (C1b) or a seeded store; the recipe names the retention setting as the prerequisite for any
   resume across downtime.
5. **Snapshots across a failover.** They halt and resume. `Capstan.Query` pins `@@server_uuid` for
   the query connection's life and halts `:snapshot_source_mismatch` on a reconnect to a
   different server. During a failover this fires; on restart the durable per-table cursors
   resume the backfill against the new writer. This is documented as the expected behavior, not
   suppressed.
6. **The preflight script.** It gains an Aurora section: `scripts/capstan-preflight.sql` reports
   `@@aurora_version`, `@@aurora_server_id`, `@@innodb_read_only` (a reader answers ON), the
   instances' `server_id` values, and the result of `CALL mysql.rds_show_configuration`, each
   guarded so the section is skipped on a server that is not Aurora. The library itself stays
   free of Aurora-specific queries.
7. **TLS.** `docs/recipes.md` gains an Aurora recipe using the RDS CA bundle with hostname
   verification on, alongside the existing self-signed recipe; the library needs no change.

## Testing and what is documented as supported

- The proof of the **failover CONTRACT** (the endpoint flip, the promoted writer's
  `gtid_executed` union, the cycle reset, the reader refusal) runs on REAL MySQL with no AWS
  account: `scripts/aurora-sim/` (three MySQL 8.0 nodes behind an HAProxy endpoint the
  `:aurora_sim` tier flips through its admin socket — real binlogs, real GTID promotion, no
  canned responses). That arm verifies capstan's behavior; it does not verify Aurora's engine,
  and its receipts are labeled simulator-observed, never OBSERVED-on-Aurora.
- The proof of **Aurora itself** is a real Aurora MySQL version 3 cluster. No mock, no stand-in
  MySQL 8.0 configured to look like Aurora, no canned failover. Two acceptable forms, in order
  of preference:
  1. a tagged `:aurora_mysql` integration module (excluded by default, like `:disposable_mysql`)
     that reads endpoint, port, user, password and CA path from the environment
     (`AURORA_MYSQL_*`, listed in `.env.example` with empty values), runs on a cluster the
     workflow creates and destroys with infrastructure code committed under `ci/aurora/`, and
     leaves its log as the CI artifact; or
  2. the same module run by the maintainer against a cluster outside the suite, with a receipt
     committed at `test/receipts/aurora-mysql-<date>.md` naming the Aurora version, the
     parameter group values, the endpoint role, `@@server_uuid` before and after the forced
     failover, and each test's outcome. The receipt carries no credential, host name or
     account identifier.
- Supported means: Aurora MySQL version 3 through the cluster writer endpoint with the documented
  parameter group and a non-NULL retention. Version 2 is unsupported by construction (the gate
  refuses it). Aurora Serverless v2 shares the version 3 engine and is covered by the same
  statement; Serverless v1 is not named. Enhanced binlog is named as observed or as untested,
  according to what the receipt shows.

## Acceptance

On the real cluster, each of these is run and its result recorded:

- The gate passes with the documented cluster parameter group; each documented misconfiguration
  refuses with its distinct reason — `binlog_format` left at the default or set `OFF` (the
  binlog-disabled case the documentation names: `binlog_format` reads `ROW`, `log_bin` reads
  disabled) refuses `:binlog_disabled`; a `STATEMENT`/`MIXED` group refuses
  `:binlog_format_not_row`; `binlog_row_metadata=MINIMAL` refuses
  `:binlog_row_metadata_not_full`; `gtid_mode=OFF` refuses `:gtid_mode_not_on`; a reader
  endpoint is refused before the dump.
- A pipeline streams from the writer endpoint to an append-only ledger; a forced failover
  (`aws rds failover-db-cluster`) mid-stream drops the socket, the pipeline reconnects to the
  promoted writer, `gap_check/3` passes, and delivery continues with loss 0 and the checkpoint
  set carrying both writers' UUIDs. The cycle counter reads 0 after the reconnect (the reset
  fired). Red proof: without the reset, the same failover sequence drives the
  established-then-dropped budget to a `:server_id_conflict` halt on a low budget.
- A checkpoint older than `binlog retention hours` halts `:data_gap`; `start_position: :current`
  on a fresh store starts from the writer's `gtid_executed`.
- A snapshot in progress across the failover halts `:snapshot_source_mismatch` and completes on
  restart with no gap and no duplicate at the handoff (the ADR-0005 property, re-proven here).
- `log_bin` red proof: the six-variable gate against a server with `log_bin = OFF` halts
  `:binlog_disabled`; with the check removed the pipeline proceeds to a dump that fails later
  with a less specific reason.
- The 8.0 and 8.4 substrates still pass the full suite: the six-variable query returns the
  enabled `log_bin` literal (`"1"`) on both and nothing else changes.

## Consequences

- One new precondition reason (`:binlog_disabled`) and possibly one dump-refusal reason, each in
  `usage-rules.md`'s halt tables; the gate's refusal count goes from five to six.
- A pipeline on a flapping Aurora cluster no longer halts `:server_id_conflict` for what is a
  sequence of failovers; a genuine duplicate `server_id` still halts as before.
- The test estate gains a paid, remote substrate. Whether it runs in CI or as a receipt is a
  cost decision the maintainer makes when the row is picked up; either way the suite's default
  `mix test` stays offline.
- `docs/recipes.md` and `scripts/capstan-preflight.sql` grow Aurora sections; the library's
  code stays vendor-neutral.

## Non-goals

- No Aurora-specific code path in `lib/` beyond the `log_bin` check and the UUID-aware cycle
  reset, both of which are correct on any MySQL.
- No support for Aurora MySQL version 2 (5.7 compatible), Aurora Serverless v1, or reading from a
  reader endpoint.
- No management of the cluster: capstan does not set parameters, retention, or `gtid_mode`, and
  does not call `rds_` procedures from the library.
- No claim about Amazon RDS for MySQL (the non-Aurora service); its Multi-AZ failover does not
  preserve binlog files the same way and needs its own row.
- No position model change: `file`/`pos` stay diagnostic; the GTID set stays the sole authority.

## Evidence

**Documentation verification (2026-09-29).** Every fact the proposal marked INFERRED was checked
against the AWS Aurora MySQL User Guide (retrieved September 29, 2026; the guide serves each page
in markdown at the same path with a `.md` suffix):

- *Using GTID-based replication* (`mysql-replication-gtid.html`) and *Enabling GTID-based
  replication for an Aurora MySQL cluster* (`mysql-replication-gtid.configuring-aurora.html`) —
  "GTID-based replication is supported for Aurora MySQL version 2 and 3"; the parameter table
  (`gtid_mode`, `enforce_gtid_consistency`); `ON`/`ON_PERMISSIVE` applying to outgoing
  replication. Also: Aurora's own instance-to-instance sync "doesn't involve the binary log".
- *Configuring Aurora MySQL binary logging* (`USER_LogAccess.MySQL.BinaryFormat.html`) — the
  cluster-parameter-group recipe, the writer reboot, "Binary logs are accessible only from the
  primary DB instance, not from the replicas", and the `binlog_format=OFF` ⇒ `log_bin` disabled ⇒
  `binlog_format` reads `ROW` note (quoted in the table; the decisive fact for decision 1).
- *Setting and showing binary log configuration* (`mysql-stored-proc-configuring.html`) —
  retention default `NULL` with lazy cleanup "usually not longer than a day"; maximum 2160 hours
  (90 days) on version 2.11.0+ and version 3; `0` invalid; the setting survives reboot and
  failover.
- *Aurora MySQL configuration parameters* (`AuroraMySQL.Reference.ParameterGroups.html`,
  cluster-level table) — `binlog_row_image` (unrestricted), `binlog_row_metadata`,
  `binlog_row_value_options`, `binlog_transaction_compression`,
  `binlog_transaction_compression_level_zstd` each noted "applies to Aurora MySQL version 3".
- *Setting up enhanced binlog for Aurora MySQL* (`AuroraMySQL.Enhanced.binlog.html`) — version
  3.03.1+ floor; `aurora_enhanced_binlog` cluster parameter with `binlog_backup=0` and
  `binlog_replication_globaldb=0`; the existing-consumer compatibility statement; the clone/
  restore/global-database availability differences.
- *Cluster endpoints* (`Aurora.Endpoints.Cluster.html`) and *Amazon Aurora storage*
  (`Aurora.Overview.StorageReliability.html`) — the cluster endpoint connects to the current
  primary and its target changes across failover; "The Aurora cluster volume contains all your
  user data, schema objects, and internal metadata such as the system tables and the binary log."

The verification corrected two claims of the original proposal: version 2's refusal path is the
unknown-variable query error spent against the command budget (`:command_retries_exhausted`),
not `:precondition_query_failed` (the error is a server error, not a malformed resultset); and
`binlog_format` left at the default does NOT refuse `:binlog_format_not_row` — per the
documented OFF semantics it reads `ROW`, so the binlog-disabled cluster is caught by `log_bin`
(`:binlog_disabled`) instead.

**Still INFERRED — settled only by the live cluster run:** what a reader answers to `@@log_bin`
and to `COM_BINLOG_DUMP_GTID` (undocumented; decides the optional `:binlog_source_not_writer`);
whether the promoted writer's `gtid_executed` carries the old writer's GTIDs after a failover;
whether an enhanced-binlog writer's event bytes decode identically. None of these is OBSERVED
yet: no Aurora cluster exists for this repository today, and the module and receipt this ADR
names are the instruments that turn them. The live run is the maintainer's cost decision
(CI-created cluster or out-of-suite receipt).

**Simulator arm (added 2026-09-29, owner decision: no AWS subscription yet).** The harness is
committed (`scripts/aurora-sim/`: writer, promotable read-only GTID replica with binlog ON,
read-only reader with binlog OFF, HAProxy endpoint with a TCP admin socket) and its tier is
`test/integration/aurora_sim_test.exs` (`:aurora_sim`, excluded by default). It proves on real
MySQL: the reader-endpoint refusal arm of decision 1 (the five value variables pass, `log_bin`
reads disabled, the gate refuses `:binlog_disabled` before the dump), two back-to-back
failovers on `max_command_retries: 1` with loss 0 (the live red proof of the cycle reset), both
writers' UUIDs in the checkpoint set, and the snapshot-across-failover halt-and-resume. The
Aurora-INFERRED rows stay INFERRED — only a real cluster turns them OBSERVED.

**OBSERVED on real (non-Aurora) MySQL during the 2026-09-29 build** (in-cluster throwaway
servers built to `docs/testing.md`'s substrate flags, driven through capstan's own protocol
client): `SELECT @@global.log_bin` returns the text `"1"` on MySQL 8.0.46 and 8.4.11 (never
`"ON"`), the full six-variable row reads `["ROW","FULL","FULL","","ON","1"]` on both, and
`Config.check_preconditions/1` returns `:ok` on both — the gate's enabled literal and the
unchanged behavior on a healthy self-managed source are proven, not assumed.

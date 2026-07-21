# ADR-0002 — Fail-closed server preconditions

**Status:** Accepted (C1) · **Design refs:** Q3, Q5, Q16, Q17, F6

## Context

Row-based binlog decoding is only lossless when the source server is configured for it. A
degraded configuration does not fail loudly — it silently guesses column identity, decoding a
row image against the wrong column list or a partial JSON document presented as a whole row.
That is precisely the silent-corruption class capstan exists to prevent, so every precondition
is checked at start and refused fail-closed rather than worked around.

Two adjacent surfaces share the same root cause (a probe substrate's permissive defaults) and
ride this ADR: the `table_id`-keyed schema resolution that a multi-table statement forces, and
the TLS peer-verification posture that OTP's `verify_peer` default would otherwise select for
the operator.

## Decision

1. **Server-precondition gate at connect — halt at start, never degrade.**
   `Capstan.Config.check_preconditions/1` reads five global variables in a single `COM_QUERY`
   on the already-authenticated socket and refuses with a **distinct reason per violation**
   (MySQL simple-query values are text, so each is compared as a string literal):

   | Variable | Required | Refusal on violation |
   |---|---|---|
   | `binlog_format` | `ROW` | `:binlog_format_not_row` |
   | `binlog_row_image` | `FULL` | `:binlog_row_image_not_full` |
   | `binlog_row_metadata` | `FULL` | `:binlog_row_metadata_not_full` |
   | `binlog_row_value_options` | `''` (empty) | `:binlog_row_value_options_not_empty` |
   | `gtid_mode` | `ON` | `:gtid_mode_not_on` |

   `binlog_row_value_options = PARTIAL_JSON` makes the server emit JSON **diffs** instead of
   values; a JSON column (in C1's supported set) would decode to a partial document with no
   sentinel and no `unchanged:` marker. Reconstructing the whole value needs the prior value,
   which a CDC consumer does not have — so refusing is mechanically forced, not a preference.
   `binlog_row_metadata = MINIMAL` (the server default) omits in-band column names/types, so a
   degraded mode would claim column identity it cannot establish; there is deliberately no such
   mode. `enforce_gtid_consistency` is **not** in the gate — it is implied by `gtid_mode = ON`
   and is documented as recommended, not separately checked.

2. **`table_id`-keyed schema resolution; no persistent schema history.**
   `Capstan.Binlog.TableRegistry` is populated by **every** `TABLE_MAP` and read by the row
   event's **own** `table_id` at decode time — never "the last map seen". A single multi-table
   statement emits `Table_map(94 ta) → Table_map(93 tb) → Update_rows(94) → Update_rows(93)`,
   so the immediately preceding map belongs to a different table. `table_id` is neither stable
   nor unique over time (it moves across an `ALTER`), so the registry is scoped and invalidated
   on `ROTATE`/`FORMAT_DESCRIPTION`. A row event naming an unmapped `table_id` is a fail-closed
   `:unmapped_table_id` halt, never a guess. No schema history is persisted across a restart —
   the registry is rebuilt from the live stream.

3. **`ROWS_QUERY_LOG_EVENT` is handled, not refused.** The event carries the complete original
   SQL of the row change with every literal value — a total Rule-1 leak if surfaced. It carries
   no row-change information capstan needs (the ROWS events carry the data), so the decoder
   returns `{:rows_query, :discarded}` and the SQL text never enters the returned term. There is
   **no** precondition on `binlog_rows_query_log_events`; refusing on a common diagnostic flag
   would be an artificial capability restriction when a deterministic lossless guard exists.

4. **TLS peer verification is an explicit operator choice, never a silent default (Q17/F6).**
   `ssl` defaults **true**. With TLS on, `ssl_opts` must carry EITHER a CA source
   (`cacertfile`/`cacerts`) OR an explicit `verify:`; given neither, `Capstan.Config.validate/1`
   fails closed with `:tls_verification_unspecified` rather than let OTP's `verify_peer` default
   select a posture nobody chose. MySQL's auto-generated server certificate is self-signed with
   no SAN, so the authenticated route (`cacertfile`, driving `verify: :verify_peer`) additionally
   requires the operator to pass `server_name_indication: :disable` (VERIFY_CA semantics: the
   chain is verified, the hostname is not). `validate/1` carries `ssl_opts` through unchanged and
   does **not** inject that — silently weakening every `cacertfile` user would defeat the point.

## Consequences

- capstan refuses to start against the common MySQL default (`binlog_row_metadata = MINIMAL`)
  and against `PARTIAL_JSON`, per replicant ADR-0002's fail-closed posture — a deliberate
  trade of convenience for the guarantee that a decoded row is never silently wrong.
- A multi-table statement decodes each row image against its own table's columns; an unmapped
  `table_id` halts rather than decoding against a stale binding.
- `binlog_rows_query_log_events = ON` is safe: the pipeline runs and the SQL text appears in no
  log or telemetry payload.
- The zero-config TLS path does not "just connect"; an operator makes an informed
  verification choice or the pipeline does not start. This is the accepted cost of never
  shipping unauthenticated TLS chosen by nobody on a stream carrying every row value.

## Evidence

`lib/capstan/config.ex:56-58,119-171` (five-var gate), `:295-308` (TLS posture) ·
`lib/capstan/binlog/table_registry.ex:34,62-68` (own-`table_id` resolve, `:unmapped_table_id`) ·
`lib/capstan/binlog/decoder.ex:131,201` (`ROWS_QUERY → :discarded`) ·
`lib/capstan/protocol/handshake.ex:18,302` (connect-time TLS guard mirror).

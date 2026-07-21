# ADR-0001 — Position and dedup model

**Status:** Accepted (C1) · **Design refs:** Q1, Q2, Q12

## Context

capstan aligns with [replicant](https://github.com/baselabs/replicant)'s consumer-facing
contracts, and replicant's replication position is a scalar `commit_lsn` that a sink dedups
against with `commit_lsn <= checkpoint`. MySQL's position is a **GTID set**, which is
legitimately non-contiguous: failover restarts the binlog file sequence, retention purges
leading ranges, and multi-source replication interleaves several source UUIDs
(`uuid1:1-5,uuid2:1-3`). No scalar can represent such a set without lying about the gaps, and
an ordinal comparison over it silently re-applies or skips transactions across a hole.

An earlier design revision (v1) rejected the GTID-set-only model "because beamline and telemetry
need a monotonic ordering key" and proposed exporting a convenience ordinal
`ordinal = file_seq * 2^32 + pos`. The adversarial pass established that the ordering-key premise
was false (`beamline`'s `source_coordinate` validates any non-empty scalar map with no ordering
requirement) and that the ordinal was derived from the very `file`/`pos` the position model
rejects as failover-broken: after a promotion or `RESET BINARY LOGS AND GTIDS` it moves **backward**
while the GTID set moves **forward**.

## Decision

1. **The GTID set is the sole authoritative and the sole persisted position.**
   `%Capstan.Position{}` carries `gtid_set` (authoritative) plus `file`/`pos` that are
   **diagnostic only — never an ordering key**. `Position.to_persisted/1` returns the
   `gtid_set` string alone; `Position.from_persisted/1` always restores `file: nil, pos: nil`.
   `Capstan.CheckpointStore` persists exactly one value per pipeline identity — the processed
   `gtid_set` string — so there is no second representation to diverge.

2. **Dedup is set membership, via the public `Capstan.Gtid.member?/2`** — never
   `commit_lsn <= checkpoint`. `member?/2` matches the source UUID case-insensitively and tests
   the GNO against the set's inclusive intervals. `Capstan.Gtid` is public, documented contract
   (`parse`, `render`, `member?`, `union`, `subtract`, `intersection`, `subset?`, `disjoint?`,
   `sources`) precisely so no sink author hand-rolls interval arithmetic on the effect-once path,
   where an under-reporting bug fires no halt (silent loss).

3. **No scalar ordinal is exported.** A non-monotonic integer presented as an ordering key is
   worse than absent.

## Consequences

- A committed GTID that falls inside a non-contiguous set is correctly recognized as already
  processed; a `<=` comparison would have silently skipped or re-applied it. This is the
  house rule inherited from replicant ADR-0001: each delivery flavor gets the strongest
  guarantee its structure supports, truthfully documented, never a parity-shaped lie.
- Exactly one durable value exists, so the two-representation divergence class cannot occur.
- Consumers must parse GTID sets to dedup — the cost the ordinal was meant to avoid — but that
  cost is paid safely because `Capstan.Gtid` is the one correct implementation and the `Sink`
  moduledoc states that hand-rolling the check is unsafe.
- Diagnostic `file`/`pos` remain available for operators but carry an explicit "never an
  ordering key" contract in the `Position` moduledoc.

## Evidence

`lib/capstan/position.ex:5-13,29,38-40` · `lib/capstan/checkpoint_store.ex:6-13,111-113` ·
`lib/capstan/gtid.ex:92-100` (`member?/2`), public surface `:65-157` ·
`lib/capstan/sink.ex:38-42` (dedup contract).

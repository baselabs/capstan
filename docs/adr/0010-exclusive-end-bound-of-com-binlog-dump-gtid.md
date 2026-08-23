# ADR-0010 — The exclusive end bound of `COM_BINLOG_DUMP_GTID`

**Status:** Accepted (probe-proven pre-library; recorded as its own ADR 2026-08-23) · **Relates to:** [ADR-0001](0001-position-and-dedup-model.md) (the position model this wire encoding carries)

## Context

The resume position is a GTID set and the dump request encodes it verbatim on the wire
(lib/capstan/protocol/command.ex). The protocol's interval encoding — `n_sids`, then per sid
`n_intervals` and `start`/`end` u64 pairs — has a sign convention that is easy to get backwards
because the GTID-set STRING form is inclusive: a checkpoint of `uuid:1-11` means transactions
1..11 are applied.

A live probe (probe/gtid_interval_bound_probe.exs) established against a real 8.0 server which
convention the DUMP request expects.

## Decision

The wire interval end bound is **EXCLUSIVE**: a checkpoint of `uuid:1-11` (transactions 1..11
applied) encodes as `start = 1, end = 12` — `command.ex` renders each inclusive interval
`{low, high}` as `<<low::64-little, high + 1::64-little>>`. An empty set encodes as `n_sids = 0`
(requests the server's full retained history).

Recorded as its own ADR rather than an amendment to ADR-0001 because it is a WIRE invariant of
the protocol, not a property of the position model: the model is inclusive everywhere
(`Gtid` sets, stored watermarks, `usage-rules`' dedup rule), and the exclusive form exists ONLY
inside this one encoder — `Gtid.parse/1` never converts, and the boundary is documented on both
sides.

## Consequences

- Getting it backwards skips or replays **exactly one transaction per interval, on every
  restart, silently**: an inclusive-`high` encoding tells the server the peer holds through
  `high - 1`, so transaction `high` re-streams (at-least-once duplicate at best, an unbounded
  re-delivery loop at worst); the symmetric mistake skips the last transaction of every
  interval — silent loss, the disqualifying class.
- The invariant is proven continuously, not by argument: a LIVE tripwire
  (test/capstan/protocol/command_test.exs) dumps twice with the server's real `gtid_executed` —
  the correct encoder and a deliberate off-by-one — and asserts they resume at different first
  GTIDs (`k + 1` vs `k`). The tripwire's checkpoint is a REAL processed watermark (every
  source's intervals verbatim, the server's own uuid lowered into the retained window), because
  a single-uuid checkpoint is refused 1236 on a substrate whose `gtid_executed` carries foreign,
  partially-purged uuid sets — the `:data_gap` semantics the library itself implements.
- Any future dump-adjacent command (a name/position-based fallback, a lossy variant) inherits
  the same probe-first requirement and a red-capable tripwire before it ships.

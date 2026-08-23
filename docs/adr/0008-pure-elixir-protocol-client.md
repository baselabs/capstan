# ADR-0008 — Pure Elixir, in-library protocol client

**Status:** Accepted (posture since the first slices; recorded 2026-08-23) · **Related:** [ADR-0002](0002-fail-closed-server-preconditions.md) (the gate the client enforces), [ADR-0009](0009-fail-closed-supervision-and-streaming-liveness.md) (the process that owns the socket)

## Context

capstan speaks the MySQL wire protocol and the replication protocol directly: handshake and
authentication (including `caching_sha2_password`'s RSA/SCRAM exchange), TLS negotiation, the
five-variable precondition query, `SET @master_binlog_checksum` / `@master_heartbeat_period`,
and `COM_BINLOG_DUMP_GTID` with a GTID-set resume position — then a lifetime of binlog event
bytes. The ecosystem norm for this in BEAM-land is either a NIF wrapper around a C client or a
protocol dependency; capstan's sibling (replicant, Postgres) instead owns its protocol surface
in-library.

Before any library code existed, a probe (probe/mysql_binlog_probe.exs, ~400 lines,
probe/FINDINGS.md) proved a zero-dependency client could do the whole job against live MySQL
8.0.46 — connect, authenticate, dump, and decode both a replayed and a live transaction.

## Decision

The protocol client is **in-library and pure Elixir**. Runtime dependencies are exactly
`decimal` + `jason` + `telemetry`; the client itself uses only `:gen_tcp`, `:ssl`, and
`:crypto` (mix.exs). No NIF, no Rust, no MySQL protocol dependency. Foundational commits:
f37ad17 (packet framing incl. >16 MiB split reassembly), fd64dc2 (handshake, caching_sha2
auth, fail-closed TLS), b60a187 (commands + GTID-set resume encoding).

Concretely owned (lib/capstan/protocol/ + lib/capstan/binlog/): packet framing with local
in-file reassembly, the full handshake state machine (both auth plugins; the native-password
deprecation warning is the library's only normal-operation log line), OTP-28-proof explicit TLS
verification posture (ADR-0002), simple-query text-resultset decode, error-1236 discrimination
(ADR-0003), the binlog event header + CRC32, and the row-event/rows decoders with the
optional-metadata TLV handling recorded in Critical Rule 6.

## Consequences

- **Auditable end to end.** Every byte that touches user data is Elixir in this repo, reviewable
  in one place; a protocol-level leak or a fail-closed gap is a grep away, not a NIF away.
- **No ABI/runtime coupling.** No NIF build step, no OTP-major NIF recompiles, no transitive
  native supply chain — the constraint floor stays plain Elixir (`~> 1.15`), continuously proven
  by CI's floor leg.
- **The bytes are ours.** MySQL's quirks (auth plugin rotation across 8.0/8.4/9.x, SAN-less
  auto-generated certs, error-1236 overloading, the exclusive wire end bound of ADR-0010) are
  this repo's to track — the probe-first habit (live substrate, real captured bytes in
  test/fixtures/binlog/) is the compensating control.
- Real-byte conformance (never self-signed fixtures) is mandatory for any new protocol surface:
  a round-trip through our own encoder proves nothing about MySQL's decoder.

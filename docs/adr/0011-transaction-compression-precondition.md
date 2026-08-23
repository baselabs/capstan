# ADR-0011 — Binary-log transaction compression: consumed by the in-library zstd decoder

**Status:** Accepted (2026-08-23; AMENDED same-day — the pure-Elixir decoder landed and the
refuse gate was removed, exactly as this ADR anticipated) · **Extends:**
[ADR-0002](0002-fail-closed-server-preconditions.md) · **Related:**
[ADR-0008](0008-pure-elixir-protocol-client.md) (the no-NIF posture that makes the decoder
in-library in the first place)

## Context

MySQL 8.0.20+ can compress row-based transactions into a single zstd-compressed
`Transaction_payload_event` per transaction. Primary-doc facts (MySQL 8.0 reference manual,
"Binary Log Transaction Compression", read first-hand 2026-08-23):

- `binlog_transaction_compression` is **source-unilateral**: a replica or downstream consumer
  **cannot opt out or negotiate uncompressed delivery** — replicas receive compressed payloads
  and must inflate them. The variable is dynamic, not replicated, and per-session settings can
  produce a MIX of compressed and uncompressed payloads in one binlog.
- Excluded from compression: GTID/control/heartbeat events, incident events (and their whole
  transactions), non-transactional events (and their whole transactions), statement-based
  events — so a compression-enabled source still delivers some events bare.
- The type-40 body is a `net_field_length` TLV header (`0`=end mark, `1`=payload size,
  `2`=compression type — `0`=ZSTD, `255`=NONE, `3`=uncompressed size; server source
  `libbinlogevents/src/codecs/binary.cpp`) wrapping one zstd frame. The inflated bytes are
  the transaction's events WITHOUT per-event CRC trailers — the outer event's CRC32 covers
  the compressed payload as a whole (observed live; inner headers' `event_size` values sum
  exactly to the inflated length).

## Decision (as first accepted)

While capstan could not inflate zstd, the precondition gate refused compression-ON sources
at connect (`:binlog_transaction_compression_on`) and the decoder halted loudly on a type-40
event — the honest posture for the capability capstan had.

## Decision (the amendment — the decoder landed)

`Capstan.Zstd` is a pure-Elixir zstd frame decompressor (RFC 8878, read first-hand):
literals (raw/RLE/Huffman with FSE-compressed weights, 1- and 4-stream), sequences
(predefined/RLE/FSE/repeat tables, the three-state bitstream), overlap-safe sequence
execution with repeat-offset tracking, frame/window/FCS handling, and XXH64 content-checksum
verification. `Capstan.Binlog.TransactionPayload` decodes the type-40 body: TLV parse →
inflate → split the inner event stream → the assembler folds the inner events through the
same transition as bare events (the bare GTID that opened the transaction rode ahead of the
payload on the wire).

Conformance is **byte-exact against reference-inflated frames**: fixtures captured live from
a `binlog_transaction_compression=ON` MySQL 8.0 substrate, each paired with the same frame
inflated by the reference `zstd` binary (`.inner` oracles, never capstan's own decoder), plus
RFC 8878 Appendix A table crosschecks and fail-closed tripwires (tampered bytes, bad magic,
reserved bits, dictionaries, checksums). The server-declared uncompressed size is verified
against the inflated length. The precondition gate's sixth variable is REMOVED (five remain,
ADR-0002); a malformed or non-ZSTD payload halts fail-closed at decode time.

## Consequences

- Compression-ON sources are consumed transparently; the sink sees the same transactions an
  uncompressed source would deliver. GTID/control/non-transactional events still arrive bare
  on such sources — the stream is a mix by construction.
- A dynamic flip ON after connect needs no gate: the decoder consumes whatever arrives.
- Fail-closed posture unchanged: any zstd corruption signal, a non-ZSTD compression type, an
  uncompressed-size mismatch, or a malformed inner header halts with a value-free reason —
  never a partially decoded or guessed payload.
- The preflight report (`scripts/capstan-preflight.sql`) carries the setting as
  informational, so discovery and the runtime posture agree.

# zstd_text — a REFERENCE-ENCODER frame (not MySQL-captured)

Provenance: `/usr/share/dict/words` shuffled deterministically (`:rand.seed(:exsss,
{2026, 8, 23})`, 45_000 words, mixed separators/case/digits — 511_604 bytes) compressed
by the reference `zstd` CLI v1.5.7 at level 3 (`zstd -3`). The `.inner` file IS the
exact original input (the decode oracle).

Why this fixture exists alongside the MySQL-captured `zstd_*` dirs: MySQL row payloads
are repetitive SQL, and every captured frame's literals sections decode through Raw/RLE
or single-stream Huffman — the **4-stream Huffman** literals path (RFC 8878
§3.1.1.3.1.5) and the large size-format arms never fired on them. Diverse natural text
forces the encoder to emit 4-stream compressed literals, so this frame exercises those
decoder arms against the reference implementation's own output (same oracle class as
the captured pairs: never self-signed).

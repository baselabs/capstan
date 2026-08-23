# XXH64 conformance vectors

Deterministic inputs (seeded PRNG, generation script in the test's history)
sized to span every XXH64 branch — empty input (the documented canonical
`ef46db3751d8e999`), the 1/4/8-byte tail classes, sub-32 inputs, exact stripe
boundaries (31/32/33, 63/64/65, 95/96/97), powers of two, and a 128KB input.

`digests.txt` carries each input's XXH64 (seed 0) as computed by an
INDEPENDENT spec-derived C implementation of the xxHash specification
(`xxhash_spec.md`, xxHash dev branch — written from the spec text, sharing no
code with capstan's Elixir). MySQL itself never emits checksummed zstd frames,
so no live-MySQL oracle exists for this path; these vectors ARE that oracle.
The test drives them through the production path: a checksummed frame carrying
the ORACLE digest must verify (and decompress to the input) — a divergence in
capstan's XXH64 reds it.

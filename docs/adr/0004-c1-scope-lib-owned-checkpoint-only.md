# ADR-0004 — C1 scope: lib-owned checkpoint mode only

**Status:** Accepted (user deferral, 2026-07-21) · **Supersedes:** the C1 design's sink-owned /
start-position scope (design § Scope item 7)

## Context

The C1 streaming-spine design scoped **two** checkpoint modes — sink-owned (the sink persists the
position atomically with its own data write, read back through `Capstan.Sink.checkpoint/0`) and
lib-owned (the position persisted by a `Capstan.CheckpointStore`) — and three start positions
(`:checkpoint`, an explicit `%Capstan.Position{}` override, and `:current`). During C1
implementation the `AssemblerServer` was wired for lib-owned mode only, and the spine threads a
start position to the `Connection`'s dump while seeding the assembler's watermark from the
checkpoint store alone. Honoring a sink-owned atomic write path and an explicit start-position
override end-to-end is a C1-completeness follow-up; shipping them half-wired would resume the
dump from an override while the watermark started empty — a silent checkpoint hole.

The user deferred the sink-owned path and explicit start positions for C1 rather than ship them
partially wired.

## Decision

**C1 ships lib-owned checkpoint mode and resume-from-durable-checkpoint
(`start_position: :checkpoint`, the default) ONLY.** Everything else in the design's checkpoint /
start-position scope is deferred and **refused fail-closed today**, not silently ignored:

| Deferred capability | Refusal reason (value-free atom) |
|---|---|
| Sink-owned checkpoint mode (no `:checkpoint_store`) | `:sink_owned_mode_unsupported` |
| Explicit `%Capstan.Position{}` start override | `:start_position_override_unsupported` |
| `start_position: :current` | `:start_position_current_unsupported` |

The `Capstan.Sink` behaviour still declares `checkpoint/0`, and `Capstan.Pipeline` still validates
per-mode callback required-ness (including the sink-owned requirement of `checkpoint/0`) — the
contract surface exists and is validated. What C1 does not do is **run** a sink-owned pipeline:
`Capstan.start_link/1` refuses it before any child starts. `start_position: :checkpoint` resumes a
populated store from its persisted watermark and starts a fresh store from the server's currently
available log.

This SUPERSEDES the design's sink-owned / start-position scope for C1. The deferred work is
tracked in `docs/ROADMAP.md`.

## Consequences

- A sink-owned or explicit-start-position configuration is refused honestly with a distinct
  value-free reason, rather than crashing deep in `AssemblerServer` init or — worse — running with
  a watermark that silently disagrees with the dump's resume position (the checkpoint hole).
- The strongest guarantee the taxonomy offers — effect-once on the sink-owned atomic path — is
  **not** available in C1; lib-owned mode is at-least-once with a bounded duplicate window across a
  kill/restart. Consumers needing atomic effect-once wait for the deferred sink-owned row.
- No partially-wired path ships, so no silent-loss surface is created by the deferral.

## Evidence

`lib/capstan.ex:31-60` (moduledoc scope), `:103-136` (the three fail-closed refusals) ·
`lib/capstan/pipeline.ex:24-27,110-117` (per-mode validation retained; override/`:current`
refused) · `lib/capstan/sink.ex:9-27` (behaviour declares `checkpoint/0`; required-ness is a
per-mode config concern) · `lib/capstan/error.ex:28-30` (the three atoms in the error catalog).

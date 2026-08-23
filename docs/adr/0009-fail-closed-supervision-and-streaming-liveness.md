# ADR-0009 — Fail-closed supervision and streaming liveness

**Status:** Accepted (posture since C1; recorded 2026-08-23) · **Related:** [ADR-0003](0003-transaction-shape-and-checkpoint-semantics.md) (what halts), [ADR-0008](0008-pure-elixir-protocol-client.md) (the socket owner)

## Context

Every condition that could silently lose or corrupt data halts the pipeline (ADR-0001/0003).
Halting is only honest if the halt is LOUD, TERMINAL, and cannot become a livelock or take down
the host application. Separately, a streaming connection's failure mode is the silent half-open
partition: the reader blocks on `recv` forever while everything looks healthy — and MySQL only
heartbeats an IDLE stream, so an active writer elsewhere does not prove OUR stream is alive.

## Decision

**Supervision: halt is a terminal state, not a crash.**

- Every pipeline child is `restart: :temporary` (lib/capstan/pipeline.ex child specs; the
  supervisor is `:one_for_one`). A fail-closed halt exits `{:shutdown, {:halt, reason}}` — not a
  crash — so a halted child is removed and NEVER restarted into a livelock, and the host
  supervisor (the consumer's tree) stays up. Restarting, reprovisioning, or paging is the host
  application's decision, informed by the `[:capstan, :connection, :halt]` /
  `[:capstan, :assembler, :halt]` telemetry (value-free, ADR-0007).
- The `Connection` forwards frames to its receiver with a plain `send/2` — a send to a dead
  receiver is a silent no-op, never a raise that could resurrect a halted pipeline. The
  corresponding silence hazard is closed by MONITORING the receiver: on its `:DOWN` the
  `Connection` stops fail-closed (`:receiver_down`) instead of streaming into a dead pid.
  Monitoring, not linking, so a `:shutdown` stop never couples the two children's restarts.

**Liveness: two timers and two counters (lib/capstan/connection.ex).**

- `SET @master_heartbeat_period` asks the master for a `HEARTBEAT_LOG_EVENT` after
  `heartbeat_period_ms` of idleness — self-contained, independent of the server's
  `slave_net_timeout` — so even a quiet-but-healthy stream keeps delivering frames.
- A parent-side liveness timer fires when NO frame (event or heartbeat) arrives within
  `stream_timeout_ms` (default 4× the heartbeat period: three missed beats): it emits
  `[:capstan, :connection, :stream_timeout]`, kills the recv-blocked reader, and reconnects.
  Epoch-guarded — a stale timeout queued before a reset is ignored. Socket `keepalive: true` is
  the OS backstop for a fully-dead peer, deliberately not primary (~2 h default idle).
- Two INDEPENDENT failure budgets: the command budget (`max_command_retries`, reset when a frame
  arrives — correct for transient pre-establish faults) and the established-then-dropped cycle
  counter (NEVER reset by frames — a duplicate `server_id` evicts this replica after each
  healthy-looking establish, and a frame-reset counter would livelock forever while emitting
  healthy telemetry; it halts `:server_id_conflict` instead). `stream_timeout_ms` must exceed
  `heartbeat_period_ms` or start-up refuses (`:invalid_liveness_config`) — a window at or below
  the interval would false-drop a HEALTHY idle stream; both constructors into the process
  enforce the identical defaults-then-compare predicate.

## Consequences

- A halted pipeline requires an operator (via the host app); the library never decides to
  restart past a data-integrity condition. This is the contract — availability posture is the
  consumer's layer.
- A silent partition is bounded by `stream_timeout_ms`, made visible by telemetry, and a
  persistent one halts `:stream_stalled` once the cycle budget is spent.
- Any future child keeps `restart: :temporary` and routes telemetry halts through
  `Capstan.Telemetry`; a restartable child is a design regression (the livelock class).

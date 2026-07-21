defmodule Capstan.Transaction do
  @moduledoc """
  A decoded, committed transaction delivered to a `Sink`.

  `changes` is typed `Enumerable.t()`, **not** a list. C1 delivers a list, but the
  type is the day-one contract: C3's spill/batch work must be able to switch a
  transaction's `changes` to a lazy, single-pass, disk-backed enumerable — as
  replicant does — without silently breaking a sink. A sink MUST NOT call
  `length/1` (or anything else requiring more than one pass) on `changes`; a
  single-pass enumerable would raise or return a wrong count on a second pass, and
  a `length/1` habit that happens to pass every capstan test today would silently
  consume a single-pass reader later. Enumerate once, streaming (`Enum.reduce/3`,
  `Enum.each/2`, `for`, or `Stream` pipelines that consume the enumerable exactly
  once).

  `position` is the processed GTID set INCLUDING this transaction — see
  `Capstan.Position`. `gtid` is this transaction's own identity, `"uuid:gno"`.
  """

  @type t :: %__MODULE__{
          gtid: String.t(),
          position: Capstan.Position.t(),
          changes: Enumerable.t(),
          commit_ts: DateTime.t()
        }

  # Rule 1: `changes` carries row VALUES (user data). Deriving `Inspect` with `only` renders
  # just the structural identity (`gtid`/`position`/`commit_ts`) and elides `changes`, so an
  # incidental `inspect/1` cannot surface a value — and, since `changes` is a possibly
  # single-pass `Enumerable.t()` (see above), never accidentally consumes it either.
  @derive {Inspect, only: [:gtid, :position, :commit_ts]}
  defstruct [:gtid, :position, :changes, :commit_ts]
end

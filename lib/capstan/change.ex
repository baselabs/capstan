defmodule Capstan.Change do
  @moduledoc """
  A single row change within a `Capstan.Transaction`.

  `record` is the row AFTER the change — a `column_name => value` map — present
  for `:insert`/`:update` and `nil` for `:delete`. `old_record` is the row BEFORE
  the change, present for `:update`/`:delete` and `nil` for `:insert`.

  `:snapshot` (C2) is a backfill row delivered by the initial-snapshot path, NOT a
  streamed committed change: `record` is the full row, `old_record` is `nil`, and it
  carries no GTID (a snapshot chunk is not a committed transaction — see
  `c:Capstan.Sink.handle_snapshot/2`). It is delivered through `handle_snapshot/2`, never
  `handle_transaction/1`.

  Column VALUES are user data (Rule 1) — never logged or attached to telemetry.
  Column NAMES stay strings, never atoms (a wide or attacker-influenced schema
  must not exhaust the atom table).
  """

  @type t :: %__MODULE__{
          op: :insert | :update | :delete | :snapshot,
          schema: String.t(),
          table: String.t(),
          record: map() | nil,
          old_record: map() | nil
        }

  # Rule 1: `record`/`old_record` are row VALUES (user data). Deriving `Inspect` with `only`
  # renders just the structural identity (`op`/`schema`/`table` — the same fields telemetry
  # is allowed to carry) and elides the value maps, so an incidental `inspect/1` of a Change
  # (a logger, a crash dump, a test helper) can never surface a row value.
  @derive {Inspect, only: [:op, :schema, :table]}
  defstruct [:op, :schema, :table, :record, :old_record]
end

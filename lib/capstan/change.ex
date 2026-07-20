defmodule Capstan.Change do
  @moduledoc """
  A single row change within a `Capstan.Transaction`.

  `record` is the row AFTER the change — a `column_name => value` map — present
  for `:insert`/`:update` and `nil` for `:delete`. `old_record` is the row BEFORE
  the change, present for `:update`/`:delete` and `nil` for `:insert`.

  Column VALUES are user data (Rule 1) — never logged or attached to telemetry.
  Column NAMES stay strings, never atoms (a wide or attacker-influenced schema
  must not exhaust the atom table).
  """

  @type t :: %__MODULE__{
          op: :insert | :update | :delete,
          schema: String.t(),
          table: String.t(),
          record: map() | nil,
          old_record: map() | nil
        }

  defstruct [:op, :schema, :table, :record, :old_record]
end

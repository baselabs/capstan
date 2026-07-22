defmodule Capstan.Snapshot.Chunk do
  @moduledoc """
  A transient, in-memory backfill chunk awaiting the advance gate.

  `table` is the `{schema, table}` pair; `seq` the chunk sequence number; `g` the chunk's
  EXACT GTID position string captured under the brief per-chunk `LOCK TABLES … READ` (a
  provable lower bound on the chunk's read view); `rows` the materialized chunk rows (a
  bounded list — at most one chunk is buffered at a time); `max_pk` the canonical PK of the
  last row (the cursor advances to it when the chunk is emitted).

  ## Rule 1

  `rows` and `max_pk` are row VALUES (user data). The `@derive {Inspect, only: [:table,
  :seq, :g]}` renders only the structural identity and elides them, so an incidental inspect
  of a buffered chunk can never surface a row value.
  """

  @type t :: %__MODULE__{
          table: {String.t(), String.t()},
          seq: non_neg_integer(),
          g: String.t(),
          rows: [map()],
          max_pk: term()
        }

  # Rule 1: `rows`/`max_pk` are user data; render only the structural identity.
  @derive {Inspect, only: [:table, :seq, :g]}
  defstruct [:table, :seq, :g, :rows, :max_pk]
end

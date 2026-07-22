defmodule Capstan.Snapshot.Meta do
  @moduledoc """
  The value-free metadata delivered alongside a snapshot chunk to
  `c:Capstan.Sink.handle_snapshot/2`.

  Carries only structural identity — the schema/table, the chunk sequence number, the
  chunk's exact GTID position string `g` (captured under the brief per-chunk lock), and
  whether this is the final chunk of the table. **No field holds a row value** (Rule 1),
  so the whole struct is safe to inspect/log. `g` is a canonical GTID-set string
  (`Capstan.Gtid` form), never a row value.
  """

  @type t :: %__MODULE__{
          schema: String.t(),
          table: String.t(),
          chunk_seq: non_neg_integer(),
          g: String.t(),
          final_chunk?: boolean()
        }

  defstruct [:schema, :table, :chunk_seq, :g, :final_chunk?]
end

defmodule Capstan.SchemaChange do
  @moduledoc """
  A DDL change: the self-committing DDL transaction's structured effect (design
  Q13/Q15).

  There is deliberately **no statement-text field**. Raw DDL SQL routinely embeds
  literal values (`DEFAULT 'secret'`, partition bounds) — a Rule 1 leak — so the
  statement text is redacted before it reaches this struct, telemetry, or any log.
  Only the structured `schema`/`table`/`kind` are exposed.

  `schema`/`table` name the affected object; `table` is `nil` for schema-level DDL.
  `kind` is the DDL kind as an atom (e.g. `:alter_table`, `:create_table`,
  `:drop_table`, `:truncate`, `:other`). `gtid` is the self-committing DDL
  transaction's own identity (Q13).
  """

  @type t :: %__MODULE__{
          schema: String.t() | nil,
          table: String.t() | nil,
          kind: atom(),
          gtid: String.t()
        }

  defstruct [:schema, :table, :kind, :gtid]
end

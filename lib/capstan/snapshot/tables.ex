defmodule Capstan.Snapshot.Tables do
  @moduledoc """
  Resolves a `tables: :all` snapshot set to a concrete, scoped table list
  (ROADMAP C2b).

  `:all` (which arises when the capture allowlist is itself `:all`) resolves to
  the server's **base tables outside the system schemas**, enumerated from
  `information_schema.TABLES`:

    * `TABLE_TYPE = 'BASE TABLE'` — views (`VIEW`) and `information_schema`
      dictionaries (`SYSTEM VIEW`) are excluded;
    * `TABLE_SCHEMA NOT IN ('mysql', 'information_schema', 'performance_schema',
      'sys')` — the four MySQL 8.0 system schemas are excluded;
    * temporary tables never appear in `information_schema.TABLES` (MySQL 8.0
      reference manual, INFORMATION_SCHEMA.TABLES: "The TABLES table does not
      list TEMPORARY tables").

  The result is ordered (schema, then table) so the resolved set is
  deterministic. An empty result refuses `:snapshot_no_base_tables` — an
  "everything" snapshot that finds nothing is a misconfiguration (wrong server
  or missing privileges), not a silent no-op. All returned names are the
  server's own strings (structural identity, the allowlisted telemetry class);
  the SQL embeds no user value.
  """

  alias Capstan.Query

  @system_schemas ["mysql", "information_schema", "performance_schema", "sys"]

  @excluded Enum.map_join(@system_schemas, ", ", fn schema -> "'" <> schema <> "'" end)

  @enumeration_sql """
  SELECT TABLE_SCHEMA, TABLE_NAME
  FROM information_schema.TABLES
  WHERE TABLE_TYPE = 'BASE TABLE'
    AND TABLE_SCHEMA NOT IN (#{@excluded})
  ORDER BY TABLE_SCHEMA, TABLE_NAME
  """

  @doc """
  Enumerates the scoped base tables over an established `query` connection.
  Returns `{:ok, [{schema, table}]}` (ordered, deduplicated by construction),
  `{:error, :snapshot_no_base_tables}` when the server exposes none, or a
  value-free enumeration fault.
  """
  @spec resolve_all(Query.t()) ::
          {:ok, [{String.t(), String.t()}]}
          | {:error, :snapshot_no_base_tables | :snapshot_table_enumeration_failed}
  def resolve_all(%Query{} = query) do
    case Query.query(query, @enumeration_sql) do
      {:ok, rows} when is_list(rows) ->
        case resolve_rows(rows) do
          {:ok, []} -> {:error, :snapshot_no_base_tables}
          {:ok, tables} -> {:ok, tables}
          :error -> {:error, :snapshot_table_enumeration_failed}
        end

      {:error, _reason} ->
        {:error, :snapshot_table_enumeration_failed}
    end
  end

  # Result rows are all-string simple-query cells (Rule 2's text posture); any
  # other shape is a protocol-level desync, refused rather than coerced (and
  # distinct from a genuinely empty enumeration).
  defp resolve_rows(rows) do
    resolved =
      Enum.reduce_while(rows, [], fn
        [schema, table], acc when is_binary(schema) and is_binary(table) ->
          {:cont, [{schema, table} | acc]}

        _other, _acc ->
          {:halt, :error}
      end)

    case resolved do
      :error -> :error
      tables -> {:ok, Enum.reverse(tables)}
    end
  end
end

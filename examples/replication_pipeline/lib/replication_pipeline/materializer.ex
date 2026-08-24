defmodule ReplicationPipeline.Materializer do
  @moduledoc """
  The idempotent mirror: upsert watched-table rows into the destination by
  primary key.

  Implementations MUST be idempotent: capstan is at-least-once (lib-owned
  checkpoint mode — the checkpoint write happens AFTER delivery), so a change
  can be applied twice across a crash window, and idempotent application is
  what turns that into exactly-once effects. `INSERT … ON DUPLICATE KEY
  UPDATE` against the mirror's primary key does exactly that.
  """

  alias Capstan.Change

  @pool ReplicationPipeline.DestPool

  # source table => mirrored table (destination database, same name)
  @mirror %{{"example_src", "orders"} => {"orders"}}

  def apply_change(%Change{} = change) do
    case Map.fetch(@mirror, {change.schema, change.table}) do
      {:ok, {dest_table}} ->
        apply_op(change.op, dest_table, change)

      :error ->
        :ok
    end
  end

  defp apply_op(:delete, dest_table, %Change{old_record: old}) do
    where = where_pk(old)

    query(
      "DELETE FROM #{dest_table} WHERE #{where.sql}",
      where.params
    )
  end

  defp apply_op(op, dest_table, %Change{record: record})
       when op in [:insert, :update] do
    columns = Map.keys(record)
    updates = Enum.map(columns -- ["id"], &"#{&1} = new.#{&1}")

    query(
      "INSERT INTO #{dest_table} (#{Enum.join(columns, ", ")}) " <>
        "VALUES (#{placeholders(columns)}) AS new" <>
        " ON DUPLICATE KEY UPDATE #{Enum.join(updates, ", ")}",
      Enum.map(columns, &Map.fetch!(record, &1))
    )
  end

  defp where_pk(old), do: %{sql: "id = ?", params: [Map.fetch!(old, "id")]}

  defp placeholders(columns), do: columns |> Enum.map(fn _ -> "?" end) |> Enum.join(", ")

  defp query(sql, params) do
    case MyXQL.query(@pool, sql, params) do
      {:ok, _} -> :ok
      {:error, _} -> {:error, :mirror_write_failed}
    end
  end
end

defmodule ReplicationPipeline.Sink do
  @moduledoc """
  The reference sink: an append-only, VALUE-FREE receipt per change, then the
  idempotent mirror.

  The receipt ledger (`cdc_receipts`) records every delivery — GTID, schema,
  table, operation, commit timestamp — and NEVER a row value (capstan Rule 1,
  upheld at the destination too: a receipt safe to read in any environment).
  It is deliberately NOT deduplicated: capstan is at-least-once in lib-owned
  checkpoint mode, so a crash between sink success and the checkpoint write
  re-delivers a transaction, and the ledger SHOWS that duplicate rather than
  hiding it — dedup happens at analysis time; the mirror stays exactly-once
  via idempotent upserts.

  `txn.changes` is a single-pass enumerable — it is enumerated exactly once.
  """

  @behaviour Capstan.Sink

  @pool ReplicationPipeline.DestPool

  @impl Capstan.Sink
  def handle_transaction(%Capstan.Transaction{} = txn) do
    result =
      Enum.reduce_while(txn.changes, :ok, fn %Capstan.Change{} = change, :ok ->
        with :ok <- record_receipt(txn, change),
             :ok <- ReplicationPipeline.Materializer.apply_change(change) do
          {:cont, :ok}
        else
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)

    case result do
      :ok -> {:ok, txn.position}
      {:error, _} = error -> error
    end
  end

  @impl Capstan.Sink
  def handle_schema_change(%Capstan.SchemaChange{} = change, _position) do
    sql = """
    INSERT INTO cdc_receipts (txn_gtid, schema_name, table_name, op, commit_ts)
    VALUES (?, ?, ?, ?, NULL)
    """

    params = [change.gtid, change.schema, change.table, "ddl:#{change.kind}"]

    case MyXQL.query(@pool, sql, params) do
      {:ok, _} -> :ok
      {:error, _} -> {:error, :receipt_insert_failed}
    end
  end

  # One INSERT per change keeps the flow obvious; batch the VALUES rows per
  # transaction if volume demands it.
  defp record_receipt(txn, change) do
    sql = """
    INSERT INTO cdc_receipts (txn_gtid, schema_name, table_name, op, commit_ts)
    VALUES (?, ?, ?, ?, ?)
    """

    params = [txn.gtid, change.schema, change.table, to_string(change.op), txn.commit_ts]

    case MyXQL.query(@pool, sql, params) do
      {:ok, _} -> :ok
      {:error, _} -> {:error, :receipt_insert_failed}
    end
  end
end

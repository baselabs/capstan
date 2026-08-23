defmodule BatchingAtomicSink do
  @moduledoc false
  @behaviour Capstan.Sink

  @impl true
  def checkpoint do
    case :ets.lookup(:c3_batch_pos, :pos) do
      [{:pos, set}] -> {:ok, %Capstan.Position{gtid_set: set, file: nil, pos: nil}}
      [] -> {:ok, nil}
    end
  end

  @impl true
  def handle_transaction(txn), do: {:ok, txn.position}

  @impl true
  def handle_schema_change(_change, position), do: {:ok, position}

  @impl true
  def handle_batch(units, position) do
    # THE atomic write: every unit's identity + the batch position together.
    for unit <- units, do: :ets.insert(:c3_batch_units, {:unit, unit.gtid})
    :ets.insert(:c3_batch_pos, {:pos, position.gtid_set})

    if pid = :persistent_term.get({__MODULE__, :test_pid}, nil) do
      send(pid, {:handle_batch, units, position})
    end

    {:ok, position}
  end
end

defmodule Capstan.Integration.BatchingTest do
  @moduledoc """
  C3 acceptance marquees, live: batched checkpointing preserves the stated guarantees —
  the crash-replay window widens to AT MOST the un-flushed batch tail; a sink-owned
  batch is atomic (units + position in ONE handle_batch); and the flush deadline closes
  a quiet batch.
  """
  use ExUnit.Case, async: false

  alias Capstan.MysqlCase
  alias Capstan.MysqlCase.{DurableStore, Sink}

  @moduletag :integration

  setup_all do
    MysqlCase.ensure_sha2_user!(MysqlCase.query_connection())
    :ok
  end

  setup do
    Sink.configure(%{pid: self()})
    on_exit(fn -> Sink.clear() end)

    table = "c3_#{:erlang.unique_integer([:positive]) |> rem(100_000)}"

    qconn = MysqlCase.socket!(MysqlCase.query_connection())

    MysqlCase.run_all!(qconn, [
      "DROP TABLE IF EXISTS #{table}",
      "CREATE TABLE #{table} (id INT PRIMARY KEY, v VARCHAR(32)) ENGINE=InnoDB"
    ])

    on_exit(fn -> MysqlCase.close!(qconn) end)
    {:ok, table: table, qconn: qconn}
  end

  test "lib-owned batching: the bound closes the batch and ONE checkpoint write covers it", ctx do
    store_table = DurableStore.new_table()
    DurableStore.seed(store_table, ctx.table, MysqlCase.read_gtid_executed!(ctx.qconn))

    {:ok, sup} =
      Capstan.start_link(
        connection: MysqlCase.pipeline_connection(),
        server_id: MysqlCase.unique_server_id(),
        sink: Sink,
        checkpoint_store: [module: DurableStore, options: [table: store_table, key: ctx.table]],
        batch: [max_transactions: 3, flush_ms: 60_000],
        max_command_retries: 5
      )

    on_exit(fn -> MysqlCase.stop_pipeline(sup) end)

    # Streaming barrier: a warm row delivered proves the pipeline is live.
    warm(ctx)
    assert {:txn, _g, _c, _p} = stream_one()

    base = committed_count(store_table, ctx.table)

    # Exactly 3 more transactions → the batch bound fires → the durable set jumps past
    # all three in ONE flush (set subsumption — the newest position covers the batch).
    MysqlCase.run_all!(
      ctx.qconn,
      for(i <- 1..3, do: "INSERT INTO #{ctx.table} (id, v) VALUES (#{i}, 'b-#{i}')")
    )

    for v <- ["b-1", "b-2", "b-3"] do
      # Pin: each delivered txn carries ITS loop value, in order — the
      # unpinned form re-bound v from the row and checked nothing.
      assert {:txn, _g, [%{record: %{"v" => ^v}}], _p} = stream_one()
    end

    wait_for(fn -> committed_count(store_table, ctx.table) >= base + 3 end)
  end

  test "sink-owned batching: the batch delivers ATOMICALLY via handle_batch", ctx do
    :ets.new(:c3_batch_units, [:duplicate_bag, :public, :named_table])
    :ets.new(:c3_batch_pos, [:set, :public, :named_table])
    :persistent_term.put({BatchingAtomicSink, :test_pid}, self())

    on_exit(fn ->
      safe_delete(:c3_batch_units)
      safe_delete(:c3_batch_pos)
    end)

    seed = MysqlCase.read_gtid_executed!(ctx.qconn)
    :ets.insert(:c3_batch_pos, {:pos, seed})

    {:ok, sup} =
      Capstan.start_link(
        connection: MysqlCase.pipeline_connection(),
        server_id: MysqlCase.unique_server_id(),
        sink: BatchingAtomicSink,
        batch: [max_transactions: 2, flush_ms: 60_000, mode: :sink_owned],
        max_command_retries: 5
      )

    on_exit(fn -> MysqlCase.stop_pipeline(sup) end)

    MysqlCase.run_all!(
      ctx.qconn,
      for(i <- 1..2, do: "INSERT INTO #{ctx.table} (id, v) VALUES (#{i + 10}, 'sb-#{i}')")
    )

    # ONE handle_batch carrying BOTH transactions + ONE final position — never a
    # per-transaction delivery in batch mode.
    assert_receive {:handle_batch, units, _position}, 20_000
    assert length(units) == 2
    assert Enum.all?(units, &is_map_key(&1, :gtid))
    refute_receive {:txn, _g, _c, _p}, 300
  end

  test "the flush deadline closes a quiet batch (never holds the checkpoint past flush_ms)",
       ctx do
    store_table = DurableStore.new_table()
    DurableStore.seed(store_table, ctx.table, MysqlCase.read_gtid_executed!(ctx.qconn))

    {:ok, sup} =
      Capstan.start_link(
        connection: MysqlCase.pipeline_connection(),
        server_id: MysqlCase.unique_server_id(),
        sink: Sink,
        checkpoint_store: [module: DurableStore, options: [table: store_table, key: ctx.table]],
        batch: [max_transactions: 1000, flush_ms: 300],
        max_command_retries: 5
      )

    on_exit(fn -> MysqlCase.stop_pipeline(sup) end)

    warm(ctx)
    assert {:txn, _g, _c, _p} = stream_one()

    # ONE transaction, then silence — the DEADLINE (not the bound) must flush it.
    MysqlCase.run!(ctx.qconn, "INSERT INTO #{ctx.table} (id, v) VALUES (99, 'quiet')")
    assert {:txn, _g, [%{record: %{"v" => "quiet"}}], _p} = stream_one()

    base = committed_count(store_table, ctx.table)
    wait_for(fn -> committed_count(store_table, ctx.table) > base end, 5_000)
  end

  ## ---------------------------------------------------------------------------
  ## helpers
  ## ---------------------------------------------------------------------------

  defp safe_delete(table) do
    :ets.delete(table)
  rescue
    ArgumentError -> :ok
  end

  defp warm(ctx) do
    n = System.unique_integer([:positive])
    MysqlCase.run!(ctx.qconn, "INSERT INTO #{ctx.table} (id, v) VALUES (#{n}, 'warm-#{n}')")
  end

  defp stream_one do
    receive do
      {:txn, gtid, changes, pos} -> {:txn, gtid, changes, pos}
    after
      20_000 -> flunk("timed out awaiting delivery")
    end
  end

  defp committed_count(table, key) do
    case DurableStore.current(table, key) do
      nil -> 0
      set -> MysqlCase.committed_count(set)
    end
  end

  defp wait_for(fun, timeout \\ 10_000) do
    deadline = System.monotonic_time() + System.convert_time_unit(timeout, :millisecond, :native)
    wait_until(fun, deadline)
  end

  defp wait_until(fun, deadline) do
    if fun.() do
      :ok
    else
      if System.monotonic_time() > deadline, do: flunk("condition not met in time")
      Process.sleep(50)
      wait_until(fun, deadline)
    end
  end
end

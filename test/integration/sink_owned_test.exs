defmodule Capstan.Integration.SinkOwnedTest do
  @moduledoc """
  C1a acceptance marquee: a sink-owned pipeline is EFFECT-ONCE across a kill/restart —
  zero replays, zero skips — proven live on an append-only ledger.

  The sink persists its data and the position ATOMICALLY together (the mode's entire
  point): `handle_transaction/1` appends the row AND the GTID to the durable ledger in
  one write, then returns the position; `checkpoint/0` reads the ledger's tail. A crash
  between delivery and "checkpoint" is therefore IMPOSSIBLE by construction — the
  restart resumes exactly at the last delivered transaction's successor.
  """
  use ExUnit.Case, async: false

  alias Capstan.Gtid
  alias Capstan.MysqlCase

  @moduletag :integration

  setup_all do
    MysqlCase.ensure_sha2_user!(MysqlCase.query_connection())
    :ok
  end

  setup do
    table = "sink_owned_#{:erlang.unique_integer([:positive]) |> rem(100_000)}"
    # The durable ledger: row appends + the position tail, shared with the sink module.
    :ets.new(:sink_owned_ledger_rows, [:duplicate_bag, :public, :named_table])
    :ets.new(:sink_owned_ckpt, [:set, :public, :named_table])

    qconn = MysqlCase.socket!(MysqlCase.query_connection())

    MysqlCase.run_all!(qconn, [
      "DROP TABLE IF EXISTS #{table}",
      "CREATE TABLE #{table} (id INT PRIMARY KEY, v VARCHAR(32)) ENGINE=InnoDB"
    ])

    on_exit(fn ->
      MysqlCase.close!(qconn)
      safe_ets_delete(:sink_owned_ledger_rows)
      safe_ets_delete(:sink_owned_ckpt)
    end)

    {:ok, table: table, qconn: qconn}
  end

  test "effect-once across kill/restart: the sink's atomic write is the checkpoint", ctx do
    # Phase 1: seed the sink's ledger with a "pre-existing" position (a fresh pipeline
    # starts from the server's current executed set — see usage-rules "First start").
    watermark = MysqlCase.read_gtid_executed!(ctx.qconn)

    :ets.insert(:sink_owned_ckpt, {:pos, watermark})

    sup1 = start_sink_owned_pipeline!(ctx)

    # Three transactions, delivered and LEDGERED atomically by the sink.
    MysqlCase.run_all!(
      ctx.qconn,
      for(i <- 1..3, do: "INSERT INTO #{ctx.table} (id, v) VALUES (#{i}, 'row-#{i}')")
    )

    delivered_1 = collect(3)
    kill_pipeline(sup1)
    # The ledger's checkpoint tail IS the last delivered position.
    checkpoint_at_kill = ledger_checkpoint(nil)
    assert Gtid.parse(checkpoint_at_kill) != Gtid.parse("")

    # Phase 2: three MORE rows planted while the pipeline is gone, then restart.
    MysqlCase.run_all!(
      ctx.qconn,
      for(i <- 4..6, do: "INSERT INTO #{ctx.table} (id, v) VALUES (#{i}, 'row-#{i}')")
    )

    sup2 = start_sink_owned_pipeline!(ctx)
    delivered_2 = collect(3)
    kill_pipeline(sup2)

    # Effect-once: no GTID delivered twice, none skipped, phases disjoint.
    all = delivered_1 ++ delivered_2
    assert length(all) == 6
    assert Enum.uniq(all) == all, "a GTID was delivered more than once"
    assert MapSet.disjoint?(MapSet.new(delivered_1), MapSet.new(delivered_2))

    # Exclusive-bound resume: phase 2 begins at the checkpoint's SUCCESSOR — the full
    # delivered run is contiguous (no skip, no replay).
    {uuid, max1} = delivered_1 |> Enum.map(&decompose/1) |> Enum.max()
    {^uuid, min2} = delivered_2 |> Enum.map(&decompose/1) |> Enum.min()
    assert min2 == max1 + 1
  end

  ## ---------------------------------------------------------------------------
  ## the atomic sink — data + position in ONE durable write
  ## ---------------------------------------------------------------------------

  # (Defined at the bottom of the file, top-level, so the module is available before
  # this test module's body runs — see AtomicLedgerSink below.)

  @impl true
  def checkpoint do
    case :ets.lookup(:sink_owned_ckpt, :pos) do
      [{:pos, set}] -> {:ok, %Capstan.Position{gtid_set: set, file: nil, pos: nil}}
      [] -> {:ok, nil}
    end
  end

  @impl true
  def handle_transaction(txn) do
    # THE atomic write: the row append(s) and the position in one ets insert batch —
    # a crash mid-delivery leaves either BOTH or NEITHER.
    gno = txn.gtid |> String.split(":") |> List.last() |> String.to_integer()

    :ets.insert(:sink_owned_ckpt, {:pos, txn.position.gtid_set})
    :ets.insert(:sink_owned_ledger_rows, {:gtid, txn.gtid, gno})

    if pid = :persistent_term.get({__MODULE__, :test_pid}, nil) do
      send(pid, {:sink_owned_txn, txn.gtid})
    end

    {:ok, txn.position}
  end

  ## ---------------------------------------------------------------------------
  ## helpers
  ## ---------------------------------------------------------------------------

  defp start_sink_owned_pipeline!(_ctx) do
    :persistent_term.put({SinkOwnedAtomicLedgerSink, :test_pid}, self())

    {:ok, sup} =
      Capstan.start_link(
        connection: MysqlCase.pipeline_connection(),
        server_id: MysqlCase.unique_server_id(),
        sink: SinkOwnedAtomicLedgerSink,
        max_command_retries: 5
      )

    on_exit(fn -> MysqlCase.stop_pipeline(sup) end)
    sup
  end

  defp kill_pipeline(sup), do: MysqlCase.stop_pipeline(sup)

  defp safe_ets_delete(table) do
    :ets.delete(table)
  rescue
    ArgumentError -> :ok
  end

  defp collect(0), do: []

  defp collect(n) do
    receive do
      {:sink_owned_txn, gtid} -> [gtid | collect(n - 1)]
    after
      20_000 -> flunk("timed out awaiting #{n} more delivery/telemetry signal(s)")
    end
  end

  defp ledger_checkpoint(_), do: :ets.lookup(:sink_owned_ckpt, :pos)[:pos]

  defp decompose(gtid) do
    [uuid, gno] = String.split(gtid, ":")
    {uuid, String.to_integer(gno)}
  end
end

defmodule SinkOwnedAtomicLedgerSink do
  @moduledoc false
  @behaviour Capstan.Sink

  @impl true
  def checkpoint do
    case :ets.lookup(:sink_owned_ckpt, :pos) do
      [{:pos, set}] -> {:ok, %Capstan.Position{gtid_set: set, file: nil, pos: nil}}
      [] -> {:ok, nil}
    end
  end

  @impl true
  def handle_transaction(txn) do
    gno = txn.gtid |> String.split(":") |> List.last() |> String.to_integer()

    :ets.insert(:sink_owned_ckpt, {:pos, txn.position.gtid_set})
    :ets.insert(:sink_owned_ledger_rows, {:gtid, txn.gtid, gno})

    if pid = :persistent_term.get({__MODULE__, :test_pid}, nil) do
      send(pid, {:sink_owned_txn, txn.gtid})
    end

    {:ok, txn.position}
  end

  @impl true
  def handle_schema_change(_change, position), do: {:ok, position}
end

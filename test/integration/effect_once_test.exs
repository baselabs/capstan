defmodule Capstan.Integration.EffectOnceTest do
  @moduledoc """
  Live-substrate effect-once + resume-correctness marquees (plan Task 18) — the silent-loss
  class this whole library exists to prevent.

  Both marquees drive a REAL pipeline that is STOPPED mid-stream and RESTARTED from a DURABLE
  checkpoint (an ETS cell the test owns, so it outlives the pipeline process):

    * **effect-once across a kill/restart on an APPEND-ONLY LEDGER** — the sink APPENDS every
      delivered GTID to a durable ETS ledger (`:duplicate_bag`), so a double-delivery is VISIBLE
      as the same GTID appearing twice. A PK-upsert count would HIDE it; the ledger cannot.
    * **resume correctness (F2 exclusive-bound)** — the `COM_BINLOG_DUMP_GTID` interval end is
      EXCLUSIVE (`uuid:1-11` → wire end 12). An off-by-one replays or skips exactly one
      transaction per restart. The marquee asserts the restart delivers the checkpoint's
      SUCCESSOR (no skip) and never the checkpoint itself (no replay) — a contiguous run.

  `:integration`-tagged. Never restarts or reconfigures the shared `mysql-cdc-probe` container.
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
    on_exit(&Sink.clear/0)
    :ok
  end

  test "effect-once across a kill/restart: each committed GTID appears exactly once in the ledger" do
    result = run_kill_restart("effect_once_ledger")

    all_delivered = result.phase1 ++ result.phase2
    ledger_gnos = Enum.map(result.ledger, &gno/1)

    # The append-only ledger is the proof surface: a double-delivery shows the same GTID twice.
    assert Enum.sort(ledger_gnos) == Enum.sort(Enum.map(all_delivered, &gno/1))

    assert ledger_gnos == Enum.uniq(ledger_gnos),
           "a GTID was delivered more than once: #{inspect(ledger_gnos)}"

    # All six committed transactions are represented — none lost.
    assert length(Enum.uniq(ledger_gnos)) == 6
  end

  test "resume is exclusive-bound: the restart replays no transaction and skips none" do
    result = run_kill_restart("resume_correctness")

    phase1 = Enum.map(result.phase1, &gno/1)
    phase2 = Enum.map(result.phase2, &gno/1)

    # No replay: the two phases are disjoint — the restart never re-delivers a checkpointed txn.
    assert phase1 -- phase2 == phase1
    assert MapSet.disjoint?(MapSet.new(phase1), MapSet.new(phase2))

    # No skip + exclusive bound: phase2 begins at the checkpoint's SUCCESSOR, and the full
    # delivered run is contiguous with no gap at the restart boundary.
    checkpoint_max = result.checkpoint1_gnos |> Enum.max()
    assert Enum.min(phase2) == checkpoint_max + 1
    combined = phase1 ++ phase2
    assert combined == Enum.to_list(Enum.min(combined)..Enum.max(combined))
  end

  ## ---------------------------------------------------------------------------
  ## the kill/restart harness
  ## ---------------------------------------------------------------------------

  # Plant batch1, start a pipeline, drain batch1 to a DURABLE checkpoint (pipeline idle — no
  # deliver/checkpoint race), STOP it (batch2 not yet in the log), plant batch2, RESTART from the
  # same durable cell, drain batch2. Returns the per-phase delivered GTIDs, the ledger, and the
  # checkpoint captured at the restart boundary.
  defp run_kill_restart(table) do
    qconn = MysqlCase.socket!(MysqlCase.query_connection())
    on_exit(fn -> MysqlCase.close!(qconn) end)

    ledger = :ets.new(:effect_once_ledger, [:public, :duplicate_bag])
    store_table = DurableStore.new_table()
    key = :checkpoint

    Sink.configure(%{pid: self(), ledger: ledger})

    MysqlCase.run_all!(qconn, [
      "DROP TABLE IF EXISTS #{table}",
      "CREATE TABLE #{table} (id INT PRIMARY KEY, n INT) ENGINE=InnoDB"
    ])

    watermark = MysqlCase.read_gtid_executed!(qconn)
    DurableStore.seed(store_table, key, watermark)
    base_count = MysqlCase.committed_count(watermark)

    # batch1 → three committed transactions, planted BEFORE the first pipeline starts.
    MysqlCase.run_all!(
      qconn,
      for(id <- 1..3, do: "INSERT INTO #{table} (id, n) VALUES (#{id}, #{id})")
    )

    sup1 = start_pipeline!(store_table, key)
    phase1 = collect_txns(3)
    # Drain to a clean boundary: the durable checkpoint reflects all three deliveries, so the
    # stop cannot land between a ledger append and its checkpoint write (the at-least-once window).
    wait_checkpoint_at_least(store_table, key, base_count + 3)
    checkpoint1 = DurableStore.current(store_table, key)
    MysqlCase.stop_pipeline(sup1)

    # batch2 → three more, planted only AFTER the first pipeline is gone (resumed mid-stream).
    MysqlCase.run_all!(
      qconn,
      for(id <- 4..6, do: "INSERT INTO #{table} (id, n) VALUES (#{id}, #{id})")
    )

    sup2 = start_pipeline!(store_table, key)
    phase2 = collect_txns(3)
    MysqlCase.stop_pipeline(sup2)

    %{
      phase1: phase1,
      phase2: phase2,
      ledger: :ets.tab2list(ledger) |> Enum.map(fn {:gtid, g} -> g end),
      checkpoint1_gnos: checkpoint1 |> gnos_of()
    }
  end

  defp start_pipeline!(store_table, key) do
    {:ok, sup} =
      Capstan.start_link(
        connection: MysqlCase.pipeline_connection(),
        server_id: MysqlCase.unique_server_id(),
        sink: Sink,
        checkpoint_store: [module: DurableStore, options: [table: store_table, key: key]],
        max_command_retries: 5
      )

    on_exit(fn -> MysqlCase.stop_pipeline(sup) end)
    sup
  end

  defp collect_txns(0), do: []

  defp collect_txns(n) do
    receive do
      {:txn, gtid, _changes, _position} -> [gtid | collect_txns(n - 1)]
    after
      20_000 -> flunk("timed out awaiting delivery #{n} more transaction(s)")
    end
  end

  defp wait_checkpoint_at_least(table, key, min_count, attempts \\ 200) do
    current = DurableStore.current(table, key)

    cond do
      current && MysqlCase.committed_count(current) >= min_count ->
        :ok

      attempts == 0 ->
        flunk("checkpoint never reached #{min_count} committed GTIDs (got #{inspect(current)})")

      true ->
        Process.sleep(50)
        wait_checkpoint_at_least(table, key, min_count, attempts - 1)
    end
  end

  # The trailing numeric component of a single "uuid:gno" GTID string.
  defp gno(gtid), do: gtid |> String.split(":") |> List.last() |> String.to_integer()

  # Every GNO present in a whole gtid_set string (a single UUID's interval band expanded).
  defp gnos_of(gtid_set) do
    gtid_set
    |> Capstan.Gtid.parse()
    |> Capstan.Gtid.sources()
    |> Enum.flat_map(fn {_uuid, intervals} ->
      Enum.flat_map(intervals, fn {low, high} -> Enum.to_list(low..high) end)
    end)
  end
end

defmodule Capstan.Integration.GapTest do
  @moduledoc """
  Live-substrate retention-gap marquees (plan Task 18, design F1/Q4) — BOTH directions of the
  fail-closed gap gate, each against a THROWAWAY `mysql:8.0` container because `PURGE BINARY LOGS`
  is server-wide and destroys binlog history other tests and fixtures depend on.

    * **purge BELOW the checkpoint → the pipeline CONTINUES** — the purged range is already
      applied, so the unapplied remainder is intact; over-rejecting here is the false halt Q4
      forbids.
    * **purge ABOVE the checkpoint remainder → `:data_gap`** — a GTID the pipeline still needs
      fell off the back of the log; continuing would silently skip it, so it halts fail-closed.

  These two are exact duals: each direction's non-vacuity mutation is the OTHER direction's purge.
  Each spins its own container so its binlog state is clean and independent.

  `:requires_docker`-tagged (every marquee here spins a throwaway container), so an absent-Docker
  run EXCLUDES them through ExUnit — a genuine skip in the summary, never a spurious pass. Run
  them with `mix test --only requires_docker` (Docker required).
  """
  use ExUnit.Case, async: false

  alias Capstan.MysqlCase
  alias Capstan.MysqlCase.{SeededStore, Sink}

  @moduletag :requires_docker

  test "purge BELOW the checkpoint: the pipeline continues past the gap" do
    with_gap_substrate(fn ctx ->
      # Purge the file holding the ALREADY-checkpointed batch1; batch2 (the remainder) stays.
      purge_below = ctx.rolled_after_batch1
      MysqlCase.run!(ctx.qconn, "PURGE BINARY LOGS TO '#{purge_below}'")

      sup = start_pipeline!(ctx)
      on_exit(fn -> MysqlCase.stop_pipeline(sup) end)

      # The remainder streams: batch2's two transactions are delivered, no halt.
      assert_receive {:txn, _g1, [_c1], _p1}, 20_000
      assert_receive {:txn, _g2, [_c2], _p2}, 20_000
      refute_receive {:connection_halt, :data_gap}, 500
    end)
  end

  test "purge ABOVE the checkpoint remainder: the pipeline halts :data_gap" do
    with_gap_substrate(fn ctx ->
      # Purge PAST batch2 as well — the remainder the checkpoint still needs is now gone.
      purge_above = ctx.rolled_after_batch2
      MysqlCase.run!(ctx.qconn, "PURGE BINARY LOGS TO '#{purge_above}'")

      sup = start_pipeline!(ctx)
      on_exit(fn -> MysqlCase.stop_pipeline(sup) end)

      # The proactive gap gate refuses at establish, before any delivery.
      assert_receive {:connection_halt, :data_gap}, 20_000
      refute_receive {:txn, _g, _c, _p}, 500
    end)
  end

  ## ---------------------------------------------------------------------------
  ## throwaway substrate: a table + batch1 (checkpointed) + batch2 (remainder),
  ## each isolated in its own binlog file so PURGE can target the boundary.
  ## ---------------------------------------------------------------------------

  # `@moduletag :requires_docker` gates Docker availability at the ExUnit level (excluded when
  # not selected), so this helper always runs the throwaway container; `with_throwaway_mysql`
  # raises a clear error if Docker is somehow absent under an explicit `--only requires_docker`.
  defp with_gap_substrate(body) do
    Sink.configure(%{pid: self()})
    on_exit(&Sink.clear/0)

    MysqlCase.with_throwaway_mysql([], fn port ->
      qconn = MysqlCase.socket!(MysqlCase.query_connection(port))

      try do
        body.(build_gap_state(qconn, port))
      after
        MysqlCase.close!(qconn)
      end
    end)
  end

  # Lay the binlog out so each batch sits in its own file:
  #   file_N: setup + batch1   →  FLUSH  →  file_N+1: batch2   →  FLUSH  →  file_N+2 (current)
  # `checkpoint` is @@gtid_executed after batch1; `rolled_after_batch1` is the file that holds
  # batch2 (PURGE TO it removes batch1's file); `rolled_after_batch2` is the current empty file
  # (PURGE TO it removes batch2's file too).
  defp build_gap_state(qconn, port) do
    MysqlCase.run_all!(qconn, [
      "DROP TABLE IF EXISTS gap_probe",
      "CREATE TABLE gap_probe (id INT PRIMARY KEY, n INT) ENGINE=InnoDB",
      "INSERT INTO gap_probe (id, n) VALUES (1, 1)",
      "INSERT INTO gap_probe (id, n) VALUES (2, 2)",
      "FLUSH BINARY LOGS"
    ])

    checkpoint = MysqlCase.read_gtid_executed!(qconn)
    rolled_after_batch1 = newest_binlog(qconn)

    MysqlCase.run_all!(qconn, [
      "INSERT INTO gap_probe (id, n) VALUES (3, 3)",
      "INSERT INTO gap_probe (id, n) VALUES (4, 4)",
      "FLUSH BINARY LOGS"
    ])

    rolled_after_batch2 = newest_binlog(qconn)

    %{
      qconn: qconn,
      port: port,
      checkpoint: checkpoint,
      rolled_after_batch1: rolled_after_batch1,
      rolled_after_batch2: rolled_after_batch2
    }
  end

  defp start_pipeline!(ctx) do
    halt_handler = MysqlCase.attach_halt_telemetry(self())
    on_exit(fn -> :telemetry.detach(halt_handler) end)

    {:ok, sup} =
      Capstan.start_link(
        connection: MysqlCase.pipeline_connection(ctx.port),
        server_id: MysqlCase.unique_server_id(),
        sink: Sink,
        checkpoint_store: [module: SeededStore, options: [gtid_set: ctx.checkpoint]],
        max_command_retries: 5
      )

    sup
  end

  defp newest_binlog(qconn) do
    MysqlCase.query_rows!(qconn, "SHOW BINARY LOGS") |> List.last() |> hd()
  end
end

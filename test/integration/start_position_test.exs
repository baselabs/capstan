defmodule Capstan.Integration.StartPositionTest do
  @moduledoc """
  C1b acceptance marquees: an explicit `%Position{}` override and `:current` resume
  END-TO-END — the dump AND the assembler watermark agree (no silent checkpoint hole),
  the override's covered transactions are never re-delivered, and `:current` starts
  from the server's live position without pre-seeding.
  """
  use ExUnit.Case, async: false

  alias Capstan.Gtid
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

    table = "c1b_#{:erlang.unique_integer([:positive]) |> rem(100_000)}"

    qconn = MysqlCase.socket!(MysqlCase.query_connection())

    MysqlCase.run_all!(qconn, [
      "DROP TABLE IF EXISTS #{table}",
      "CREATE TABLE #{table} (id INT PRIMARY KEY, v VARCHAR(32)) ENGINE=InnoDB"
    ])

    on_exit(fn -> MysqlCase.close!(qconn) end)
    {:ok, table: table, qconn: qconn}
  end

  test "an explicit %Position{} override skips its covered transactions and delivers only the tail",
       ctx do
    # Plant a pre-override transaction (to be SKIPPED) and capture the position after it.
    MysqlCase.run!(ctx.qconn, "INSERT INTO #{ctx.table} (id, v) VALUES (1, 'pre-override')")
    pre = MysqlCase.read_gtid_executed!(ctx.qconn)

    # The override = `pre`: everything ≤ pre is the operator's asserted resume point.
    {:ok, sup} =
      Capstan.start_link(
        connection: MysqlCase.pipeline_connection(),
        server_id: MysqlCase.unique_server_id(),
        sink: Sink,
        checkpoint_store: [
          module: DurableStore,
          options: [table: DurableStore.new_table(), key: ctx.table]
        ],
        start_position: %Capstan.Position{gtid_set: pre, file: nil, pos: nil},
        max_command_retries: 5
      )

    on_exit(fn -> MysqlCase.stop_pipeline(sup) end)

    # The assembler's watermark seeded from the OVERRIDE, not the empty store — the
    # dump and the watermark agree by construction (multi-source set: rendered equal).
    state = :sys.get_state(MysqlCase.assembler_pid(sup))
    assert Gtid.render(state.processed_set) == Gtid.render(Gtid.parse(pre))

    MysqlCase.run!(ctx.qconn, "INSERT INTO #{ctx.table} (id, v) VALUES (2, 'post-override')")

    # ONLY the post-override transaction is delivered; row 1 never arrives.
    assert {:txn, _gtid, [%{record: %{"v" => "post-override"}}], _pos} = collect_one()
    refute_receive {:txn, _g, _c, _p}, 500
  end

  test ":current starts from the server's live position — no pre-seed, no replay of history",
       ctx do
    # Pre-existing history exists (the setup DDL). :current must NOT stream any of it.
    {:ok, sup} =
      Capstan.start_link(
        connection: MysqlCase.pipeline_connection(),
        server_id: MysqlCase.unique_server_id(),
        sink: Sink,
        checkpoint_store: [
          module: DurableStore,
          options: [table: DurableStore.new_table(), key: ctx.table]
        ],
        start_position: :current,
        max_command_retries: 5
      )

    on_exit(fn -> MysqlCase.stop_pipeline(sup) end)

    # Establish first (the :current read happens pre-dump), then commit NEW work.
    h = MysqlCase.attach_established_telemetry(self())
    Process.sleep(1000)
    :telemetry.detach(h)
    MysqlCase.run!(ctx.qconn, "INSERT INTO #{ctx.table} (id, v) VALUES (9, 'from-now')")

    assert {:txn, _gtid, [%{record: %{"v" => "from-now"}}], _pos} = collect_one()

    # No pre-:current history ever arrives (the setup DDL transaction is NOT re-delivered).
    refute_receive {:txn, _g, [%{record: %{"v" => _}}], _p}, 500
  end

  test ":current seeds the ASSEMBLER WATERMARK too — the first checkpoint covers the skipped interval (span finding C1)",
       ctx do
    # The dump resumes from the resolved :current position; the assembler's processed
    # watermark must be seeded from the SAME value (pipeline.ex: "the same value the dump
    # resumes from — the two can never disagree"). RED (the resolved position reached only
    # the connection spec): the first checkpoint persists the old authority set plus the
    # new GTID alone — the skipped interval is never recorded as processed, so a restart
    # replays it.
    store_table = DurableStore.new_table()

    {:ok, sup} =
      Capstan.start_link(
        connection: MysqlCase.pipeline_connection(),
        server_id: MysqlCase.unique_server_id(),
        sink: Sink,
        checkpoint_store: [
          module: DurableStore,
          options: [table: store_table, key: ctx.table]
        ],
        start_position: :current,
        max_command_retries: 5
      )

    on_exit(fn -> MysqlCase.stop_pipeline(sup) end)

    h = MysqlCase.attach_established_telemetry(self())
    Process.sleep(1000)
    :telemetry.detach(h)

    # The live position the dump skipped TO (the whole pre-:current history).
    w_cur = MysqlCase.read_gtid_executed!(ctx.qconn)

    MysqlCase.run!(ctx.qconn, "INSERT INTO #{ctx.table} (id, v) VALUES (10, 'wm-seed')")
    assert {:txn, _gtid, [%{record: %{"v" => "wm-seed"}}], _pos} = collect_one()

    # The durable checkpoint SUBSUMES the skipped interval once the first txn checks in.
    assert covers_within?(store_table, ctx.table, w_cur, 200),
           "the durable checkpoint never covered the pre-:current interval — the assembler watermark was not seeded"
  end

  defp covers_within?(_table, _key, _w_cur, 0), do: false

  defp covers_within?(table, key, w_cur, attempts) do
    covered? =
      case DurableStore.current(table, key) do
        nil -> false
        set -> Gtid.subset?(Gtid.parse(w_cur), Gtid.parse(set))
      end

    if covered?,
      do: true,
      else: Process.sleep(25) && covers_within?(table, key, w_cur, attempts - 1)
  end

  defp collect_one do
    receive do
      {:txn, gtid, changes, pos} -> {:txn, gtid, changes, pos}
    after
      20_000 -> flunk("timed out awaiting delivery")
    end
  end
end

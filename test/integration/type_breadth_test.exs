defmodule Capstan.Integration.TypeBreadthTest do
  @moduledoc """
  C4a acceptance marquee, live: a pipeline whose table carries SET and GEOMETRY
  columns streams real commits — the SET value arrives comma-joined (MySQL's text
  form) and the spatial value as the raw SRID+WKB binary — and the empty-set and
  multi-member forms all converge.
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

    table = "c4a_#{:erlang.unique_integer([:positive]) |> rem(100_000)}"

    qconn = MysqlCase.socket!(MysqlCase.query_connection())

    MysqlCase.run_all!(qconn, [
      "DROP TABLE IF EXISTS #{table}",
      "CREATE TABLE #{table} (id INT PRIMARY KEY, flags SET('a','b','c'), g GEOMETRY) ENGINE=InnoDB"
    ])

    on_exit(fn -> MysqlCase.close!(qconn) end)
    {:ok, table: table, qconn: qconn}
  end

  test "SET and GEOMETRY stream through a real pipeline and converge on every form", ctx do
    store_table = DurableStore.new_table()
    DurableStore.seed(store_table, ctx.table, MysqlCase.read_gtid_executed!(ctx.qconn))

    {:ok, sup} =
      Capstan.start_link(
        connection: MysqlCase.pipeline_connection(),
        server_id: MysqlCase.unique_server_id(),
        sink: Sink,
        checkpoint_store: [module: DurableStore, options: [table: store_table, key: ctx.table]],
        max_command_retries: 5
      )

    on_exit(fn -> MysqlCase.stop_pipeline(sup) end)

    # All three SET forms + a spatial value in one committed transaction.
    MysqlCase.run_all!(ctx.qconn, [
      "INSERT INTO #{ctx.table} VALUES (1, 'a,c', ST_GeomFromText('POINT(1 2)'))",
      "INSERT INTO #{ctx.table} VALUES (2, '', NULL)",
      "INSERT INTO #{ctx.table} VALUES (3, 'a,b,c', NULL)"
    ])

    # run_all! commits each INSERT as its own transaction — three deliveries.
    changes =
      Enum.flat_map(1..3, fn _i ->
        {:txn, _g, cs, _p} = collect_one()
        cs
      end)

    by_id = Map.new(changes, fn c -> {c.record["id"], c.record} end)

    assert by_id[1]["flags"] == "a,c"
    assert by_id[2]["flags"] == ""
    assert by_id[3]["flags"] == "a,b,c"

    assert by_id[1]["g"] ==
             <<0::32, 1::8, 1::32-little, 1.0::float-64-little, 2.0::float-64-little>>

    assert is_nil(by_id[2]["g"])
    assert is_nil(by_id[3]["g"])
  end

  defp collect_one do
    receive do
      {:txn, gtid, changes, pos} -> {:txn, gtid, changes, pos}
    after
      20_000 -> flunk("timed out awaiting delivery")
    end
  end
end

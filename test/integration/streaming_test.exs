defmodule Capstan.Integration.StreamingTest do
  @moduledoc """
  Live-substrate streaming marquees (plan Task 18) — a REAL `Capstan.start_link/1` pipeline
  tailing the running `mysql-cdc-probe`, proving the happy-path delivery contracts end-to-end:

    * INSERT/UPDATE/DELETE delivered, effect-once, in commit order;
    * a SINGLE multi-table statement delivering BOTH tables correctly (design Q3 — the
      `TABLE_MAP` right before a row event can name a DIFFERENT table);
    * a self-committing DDL delivered as a `%SchemaChange{}` that ADVANCES the checkpoint (Q13);
    * `binlog_rows_query_log_events = ON` — the pipeline runs AND the ROWS_QUERY SQL leaks nowhere.

  `:integration`-tagged: excluded by default, run with `mix test --only integration` against a
  substrate brought up by `scripts/dev-substrate.sh`. Never restarts or reconfigures the container.
  """
  use ExUnit.Case, async: false

  alias Capstan.Gtid
  alias Capstan.MysqlCase
  alias Capstan.MysqlCase.{SeededStore, Sink}

  @moduletag :integration

  setup_all do
    MysqlCase.ensure_sha2_user!(MysqlCase.query_connection())
    :ok
  end

  setup do
    Sink.configure(%{pid: self()})
    on_exit(&Sink.clear/0)
    qconn = MysqlCase.socket!(MysqlCase.query_connection())
    on_exit(fn -> MysqlCase.close!(qconn) end)
    {:ok, qconn: qconn}
  end

  # Set up the table(s), read the live watermark, start a pipeline resuming from it, and return
  # the supervisor — so only the statements planted AFTER the watermark stream to the sink.
  defp start_pipeline!(qconn, setup_sql) do
    MysqlCase.run_all!(qconn, setup_sql)
    watermark = MysqlCase.read_gtid_executed!(qconn)

    {:ok, sup} =
      Capstan.start_link(
        connection: MysqlCase.pipeline_connection(),
        server_id: MysqlCase.unique_server_id(),
        sink: Sink,
        checkpoint_store: [module: SeededStore, options: [gtid_set: watermark]],
        max_command_retries: 5
      )

    on_exit(fn -> MysqlCase.stop_pipeline(sup) end)
    {sup, watermark}
  end

  test "INSERT/UPDATE/DELETE stream effect-once in commit order", %{qconn: qconn} do
    {_sup, _watermark} =
      start_pipeline!(qconn, [
        "DROP TABLE IF EXISTS stream_iud",
        "CREATE TABLE stream_iud (id INT PRIMARY KEY, name VARCHAR(50), qty INT) ENGINE=InnoDB"
      ])

    MysqlCase.run_all!(qconn, [
      "INSERT INTO stream_iud (id, name, qty) VALUES (1, 'widget', 10)",
      "UPDATE stream_iud SET qty = 20 WHERE id = 1",
      "DELETE FROM stream_iud WHERE id = 1"
    ])

    assert_receive {:txn, _g1, [insert], _p1}, 20_000

    assert %Capstan.Change{op: :insert, table: "stream_iud", record: rec, old_record: nil} =
             insert

    assert rec["id"] == 1
    assert rec["qty"] == 10

    assert_receive {:txn, _g2, [update], _p2}, 20_000
    assert %Capstan.Change{op: :update, record: after_row, old_record: before_row} = update
    assert before_row["qty"] == 10
    assert after_row["qty"] == 20

    assert_receive {:txn, _g3, [delete], _p3}, 20_000
    assert %Capstan.Change{op: :delete, record: nil, old_record: gone} = delete
    assert gone["id"] == 1

    # effect-once: no transaction is delivered twice.
    refute_receive {:txn, _g, _c, _p}, 500
  end

  test "a single multi-table statement delivers BOTH tables correctly (Q3)", %{qconn: qconn} do
    {_sup, _watermark} =
      start_pipeline!(qconn, [
        "DROP TABLE IF EXISTS mt_a",
        "DROP TABLE IF EXISTS mt_b",
        "CREATE TABLE mt_a (id INT PRIMARY KEY, val INT) ENGINE=InnoDB",
        "CREATE TABLE mt_b (id INT PRIMARY KEY, val INT) ENGINE=InnoDB",
        "INSERT INTO mt_a (id, val) VALUES (1, 100)",
        "INSERT INTO mt_b (id, val) VALUES (1, 1)"
      ])

    # ONE statement updating both tables → one transaction, two row events whose immediately
    # preceding TABLE_MAP names DIFFERENT tables. A registry keyed on "the last map" would
    # mislabel mt_b's row as mt_a.
    MysqlCase.run!(
      qconn,
      "UPDATE mt_a JOIN mt_b ON mt_a.id = mt_b.id " <>
        "SET mt_a.val = mt_a.val + mt_b.val, mt_b.val = mt_b.val + 1"
    )

    assert_receive {:txn, _gtid, changes, _pos}, 20_000
    by_table = Map.new(changes, fn %Capstan.Change{table: t} = c -> {t, c} end)

    assert %Capstan.Change{op: :update, record: a_after, old_record: a_before} = by_table["mt_a"]
    assert a_before["val"] == 100
    assert a_after["val"] == 101

    assert %Capstan.Change{op: :update, record: b_after, old_record: b_before} = by_table["mt_b"]
    assert b_before["val"] == 1
    assert b_after["val"] == 2
  end

  test "a self-committing DDL is delivered as %SchemaChange{} advancing the checkpoint", %{
    qconn: qconn
  } do
    {_sup, watermark} =
      start_pipeline!(qconn, [
        "DROP TABLE IF EXISTS stream_ddl",
        "CREATE TABLE stream_ddl (id INT PRIMARY KEY, name VARCHAR(50)) ENGINE=InnoDB"
      ])

    MysqlCase.run!(qconn, "ALTER TABLE stream_ddl ADD COLUMN extra INT DEFAULT 7")

    assert_receive {:schema_change, schema_change, position}, 20_000

    assert %Capstan.SchemaChange{kind: :alter_table, table: "stream_ddl", gtid: gtid} =
             schema_change

    # The checkpoint the pipeline persists after the DDL (the delivered `position`) INCLUDES the
    # DDL's own GTID and is strictly AHEAD of the pre-DDL watermark (Q13 advance).
    [{uuid, [{_low, _high} | _]} | _] = Gtid.sources(Gtid.parse(gtid))
    single = String.split(gtid, ":") |> List.last() |> String.to_integer()
    assert Gtid.member?(Gtid.parse(position.gtid_set), {uuid, single})
    refute Gtid.member?(Gtid.parse(watermark), {uuid, single})
  end

  test "ROWS_QUERY enabled: the pipeline runs AND the ROWS_QUERY SQL leaks nothing", %{
    qconn: qconn
  } do
    {_sup, _watermark} =
      start_pipeline!(qconn, [
        "DROP TABLE IF EXISTS stream_rq",
        "CREATE TABLE stream_rq (id INT PRIMARY KEY, note VARCHAR(64)) ENGINE=InnoDB"
      ])

    # With ROWS_QUERY on, the server emits a ROWS_QUERY_LOG_EVENT carrying the ORIGINAL SQL. The
    # sentinel is planted via UPPER(), so the delivered row value is uppercase and the lowercase
    # sentinel exists ONLY in the discarded SQL text — a leak would show it in the delivered change.
    sentinel = "rowsquery_leak_probe_5c1f"

    MysqlCase.run_all!(qconn, [
      "SET SESSION binlog_rows_query_log_events = ON",
      "INSERT INTO stream_rq (id, note) VALUES (1, UPPER('#{sentinel}'))"
    ])

    assert_receive {:txn, _gtid, [change], _pos}, 20_000
    assert %Capstan.Change{op: :insert, table: "stream_rq", record: rec} = change
    # The pipeline RAN (the row was delivered) and the row value is the uppercased sentinel …
    assert rec["note"] == String.upcase(sentinel)
    # … while the lowercase ROWS_QUERY SQL text appears in NO delivered change.
    refute String.contains?(inspect(change), sentinel)
  end
end

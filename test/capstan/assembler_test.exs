defmodule Capstan.AssemblerTest do
  use ExUnit.Case, async: true

  alias Capstan.{Assembler, Change, Position, SchemaChange, Transaction}
  alias Capstan.Binlog.{Decoder, Event}
  alias Capstan.Gtid

  @fixtures_root Path.join([__DIR__, "..", "fixtures", "binlog"])

  # ---------------------------------------------------------------------------
  # helpers — every terminator/multi-table assertion is driven by REAL captured
  # bytes: each fixture file is parsed through Event.parse/1 (header + CRC), and
  # the resulting %Event{} sequence is folded by the assembler. No synthetic events
  # except the XA_PREPARE marker, which is decoded by the REAL Decoder (F9).
  # ---------------------------------------------------------------------------

  defp event!(scenario, filename) do
    {:ok, event} = Event.parse(File.read!(Path.join([@fixtures_root, scenario, filename])))
    event
  end

  defp events(scenario, filenames), do: Enum.map(filenames, &event!(scenario, &1))

  # Every *.bin under a scenario, in captured (numeric-prefix) order.
  defp all_events(scenario) do
    [@fixtures_root, scenario, "*.bin"]
    |> Path.join()
    |> Path.wildcard()
    |> Enum.sort()
    |> Enum.map(fn path ->
      {:ok, event} = Event.parse(File.read!(path))
      event
    end)
  end

  # Derive the GTID identity from the fixture ITSELF, never a hardcoded literal:
  # the substrate's server_uuid, sequence numbers, and event timestamps all change
  # when the fixtures are regenerated (fixtures README).
  defp gtid_of(scenario, filename) do
    {:ok, {:gtid, {uuid, gno}}} = Decoder.decode(event!(scenario, filename))
    {uuid, gno}
  end

  defp empty_start, do: %Position{gtid_set: "", file: nil, pos: nil}

  # The REAL decoder's fail-closed marker for XA_PREPARE (type 38). Built exactly
  # like decoder_test's `event/2` helper and passed through Decoder.decode — never a
  # hand-written {:halt, _} atom (design F9).
  defp xa_prepare_event do
    %Event{
      type: 38,
      timestamp: 0,
      server_id: 1,
      event_size: 19,
      log_pos: 0,
      flags: 0,
      body: <<>>
    }
  end

  # A QUERY event (type 2) carrying `sql` under `schema`. The wire body matches
  # Decoder.decode_query/1: thread_id, exec_time, schema_len, error_code,
  # status_vars_len(=0), schema, NUL, sql. QUERY *wire decoding* is proven by the real
  # `alter_ddl`/`myisam` fixtures; this constructs one only to unit-cover the DDL-string
  # classifier over statement forms the captured fixtures do not include.
  defp query_event(schema, sql) do
    body =
      <<0::32-little, 0::32-little, byte_size(schema)::8, 0::16-little, 0::16-little>> <>
        schema <> <<0>> <> sql

    %Event{
      type: 2,
      timestamp: 1_700_000_000,
      server_id: 1,
      event_size: 0,
      log_pos: 100,
      flags: 0,
      body: body
    }
  end

  # =========================================================================
  # THE NAMED SAFETY PROPERTIES FIRST (risk-first): a transaction-shape halt,
  # or ANY mid-transaction failure, must DISCARD the in-flight transaction —
  # never deliver a partial/phantom transaction with the rows accumulated so far.
  # =========================================================================

  describe "SAFETY — XA_PREPARE halts fail-closed with ZERO rows emitted (F9)" do
    test "the marker originates from the REAL Decoder, not a hand-written atom" do
      assert Decoder.decode(xa_prepare_event()) == {:halt, :unsupported_transaction_shape}
    end

    test "an XA transaction with rows already accumulated emits NOTHING and halts" do
      # GTID -> BEGIN -> TABLE_MAP -> WRITE_ROWS accumulates one insert; then the
      # terminator is an XA_PREPARE instead of an XID. Those prepared rows may later
      # be rolled back, so the halt must discard them: zero output, fail-closed.
      seq =
        events("simple_dml", [
          "01-rotate.bin",
          "02-format_description.bin",
          "03-previous_gtids.bin",
          "04-heartbeat.bin",
          "05-gtid.bin",
          "06-query.bin",
          "07-table_map.bin",
          "08-write_rows.bin"
        ]) ++ [xa_prepare_event()]

      # Zero committed outputs (the only transaction is the XA one, discarded) — its
      # accumulated insert is NOT among the returned outputs.
      assert {:halt, :unsupported_transaction_shape, [], %Position{}} =
               Assembler.run(seq, empty_start())
    end
  end

  describe "SAFETY — mid-transaction failures abort fail-closed (no phantom txn)" do
    test "an unmapped table_id aborts; the accumulated rows are never delivered" do
      # WRITE_ROWS (table_id 103) arrives with NO preceding TABLE_MAP for it — the
      # registry cannot resolve the id, so the fold fails closed rather than guessing.
      seq =
        events("simple_dml", [
          "01-rotate.bin",
          "02-format_description.bin",
          "05-gtid.bin",
          "06-query.bin",
          "08-write_rows.bin"
        ])

      assert {:error, :unmapped_table_id, [], %Position{}} = Assembler.run(seq, empty_start())
    end

    test "a Rows.decode error (unsupported SET column) aborts fail-closed, no txn" do
      # set_type is a real captured InnoDB insert into a SET column. C1 cannot decode
      # SET yet, so Rows.decode returns {:error, {:unsupported_column_type, _}} and the
      # whole transaction is refused — a partial/raw row is never emitted.
      assert {:error, {:unsupported_column_type, detail}, [], %Position{}} =
               Assembler.run(all_events("set_type"), empty_start())

      # Rule 1: the error term carries only schema-level facts, never a row value.
      refute Map.has_key?(detail, :value)
      refute inspect(detail) =~ "a,c"
    end

    test "a Decoder {:error, _} (compressed payload) aborts fail-closed, no txn" do
      compressed = %Event{
        type: 40,
        timestamp: 0,
        server_id: 1,
        event_size: 19,
        log_pos: 0,
        flags: 0,
        body: <<>>
      }

      seq = events("simple_dml", ["05-gtid.bin", "06-query.bin"]) ++ [compressed]

      assert {:error, :compressed_payload_unsupported, [], %Position{}} =
               Assembler.run(seq, empty_start())
    end
  end

  # =========================================================================
  # THE THREE TERMINATORS + Q14 watermark + Q3 multi-table (happy path).
  # =========================================================================

  describe "XID-terminated transaction (behavior 1)" do
    test "GTID/BEGIN/TABLE_MAP/WRITE_ROWS/XID -> one %Transaction{} with the insert" do
      seq =
        events("simple_dml", [
          "01-rotate.bin",
          "02-format_description.bin",
          "03-previous_gtids.bin",
          "04-heartbeat.bin",
          "05-gtid.bin",
          "06-query.bin",
          "07-table_map.bin",
          "08-write_rows.bin",
          "09-xid.bin"
        ])

      {uuid, gno} = gtid_of("simple_dml", "05-gtid.bin")
      xid_event = event!("simple_dml", "09-xid.bin")

      assert {:ok, [%Transaction{} = txn], %Position{} = final} =
               Assembler.run(seq, empty_start())

      assert txn.gtid == "#{uuid}:#{gno}"

      assert txn.changes == [
               %Change{
                 op: :insert,
                 schema: "probe_db",
                 table: "widgets",
                 record: %{"id" => 1, "name" => "widget-one", "qty" => 10},
                 old_record: nil
               }
             ]

      # commit_ts is the XID event's HEADER timestamp (unix seconds), which decode/1
      # drops — proving the fold reads the header off the same %Event{}.
      assert txn.commit_ts == DateTime.from_unix!(xid_event.timestamp)

      # position INCLUDES this txn's GTID, and the fold's final watermark matches.
      assert Gtid.member?(Gtid.parse(txn.position.gtid_set), {uuid, gno})
      assert Gtid.member?(Gtid.parse(final.gtid_set), {uuid, gno})
    end
  end

  describe "QUERY(\"COMMIT\")-terminated transaction — non-transactional engine (behavior 2)" do
    test "myisam fixture: the COMMIT query is the terminator, not a change" do
      {uuid, gno} = gtid_of("myisam", "05-gtid.bin")
      commit_event = event!("myisam", "09-query.bin")

      assert {:ok, [%Transaction{} = txn], _final} =
               Assembler.run(all_events("myisam"), empty_start())

      assert txn.gtid == "#{uuid}:#{gno}"

      # Exactly one change — the INSERT. The COMMIT produced no change.
      assert [%Change{op: :insert, schema: "probe_db", table: "myisam_widgets"} = change] =
               txn.changes

      assert change.record == %{"id" => 1, "name" => "myisam-one"}
      assert txn.commit_ts == DateTime.from_unix!(commit_event.timestamp)
      assert Gtid.member?(Gtid.parse(txn.position.gtid_set), {uuid, gno})
    end
  end

  describe "self-committing DDL QUERY (behavior 3)" do
    test "alter_ddl: GTID -> ALTER (no BEGIN, no XID) -> %SchemaChange{} + position advances" do
      {uuid, gno} = gtid_of("alter_ddl", "05-gtid.bin")

      assert {:ok, [%SchemaChange{} = sc], %Position{} = final} =
               Assembler.run(all_events("alter_ddl"), empty_start())

      assert sc.schema == "probe_db"
      assert sc.table == "widgets_ddl"
      assert sc.kind == :alter_table
      assert sc.gtid == "#{uuid}:#{gno}"

      # The position advances even though DDL has no XID (Q13): Task 15 checkpoints
      # the returned final watermark.
      assert Gtid.member?(Gtid.parse(final.gtid_set), {uuid, gno})
    end

    test "Rule 1: no DDL statement text — column name and DEFAULT literal never leak" do
      assert {:ok, [%SchemaChange{} = sc], _final} =
               Assembler.run(all_events("alter_ddl"), empty_start())

      # The captured DDL is "ALTER TABLE widgets_ddl ADD COLUMN extra INT DEFAULT 7".
      # Only schema/table/kind may survive; the column name and literal must not.
      rendered = inspect(sc)
      refute rendered =~ "extra"
      refute rendered =~ "DEFAULT"
      refute rendered =~ "ADD COLUMN"
      # The DEFAULT literal value itself must not survive either (Rule 1 covers values,
      # not only keywords) — `\b7\b` catches the `DEFAULT 7` literal in isolation.
      refute rendered =~ ~r/\b7\b/
      refute Map.has_key?(Map.from_struct(sc), :sql)
    end
  end

  describe "fully-filtered / empty transaction — Q14 processed-watermark" do
    test "a committed txn whose rows are ALL filtered still advances the position, no sink" do
      # Filter allows only a table the fixture never touches, so every row event in
      # simple_dml's three transactions is filtered BEFORE row decode. Each txn still
      # terminates and advances the watermark, represented with EMPTY changes so
      # Task 15 checkpoints WITHOUT calling the sink.
      filter = [tables: [{"probe_db", "some_other_table"}]]

      assert {:ok, txns, %Position{} = final} =
               Assembler.run(all_events("simple_dml"), empty_start(), filter)

      assert length(txns) == 3
      assert Enum.all?(txns, &match?(%Transaction{}, &1))
      assert Enum.all?(txns, fn %Transaction{changes: c} -> c == [] end)

      # NO stall: all three GTIDs advanced the watermark over the run.
      for file <- ["05-gtid.bin", "10-gtid.bin", "15-gtid.bin"] do
        {uuid, gno} = gtid_of("simple_dml", file)
        assert Gtid.member?(Gtid.parse(final.gtid_set), {uuid, gno})
      end
    end

    test "allowlist is non-vacuous: naming the fixture's table admits its rows" do
      filter = [tables: [{"probe_db", "widgets"}]]

      seq =
        events("simple_dml", [
          "05-gtid.bin",
          "06-query.bin",
          "07-table_map.bin",
          "08-write_rows.bin",
          "09-xid.bin"
        ])

      assert {:ok, [%Transaction{changes: [%Change{op: :insert, table: "widgets"}]}], _} =
               Assembler.run(seq, empty_start(), filter)
    end
  end

  describe "interleaved multi-table rows — Q3 resolve by OWN table_id (behavior 6)" do
    test "multi_table: ta rows cast against ta's map, tb rows against tb's map" do
      {uuid, gno} = gtid_of("multi_table", "05-gtid.bin")

      assert {:ok, [%Transaction{} = txn], _final} =
               Assembler.run(all_events("multi_table"), empty_start())

      assert txn.gtid == "#{uuid}:#{gno}"

      # Both tables' changes ride in the one transaction, each resolved by its OWN
      # table_id (104 -> ta, 105 -> tb). "Last map wins" would cast ta's rows against
      # tb's map and fail the table_id guard — no Transaction would be produced.
      ta_changes = Enum.filter(txn.changes, &(&1.table == "ta"))
      tb_changes = Enum.filter(txn.changes, &(&1.table == "tb"))

      assert length(ta_changes) == 2
      assert length(tb_changes) == 2
      assert Enum.all?(txn.changes, &(&1.op == :update and &1.schema == "probe_db"))

      assert Enum.map(ta_changes, & &1.old_record) == [
               %{"id" => 1, "val" => 100},
               %{"id" => 2, "val" => 200}
             ]

      assert Enum.map(ta_changes, & &1.record) == [
               %{"id" => 1, "val" => 101},
               %{"id" => 2, "val" => 202}
             ]

      assert Enum.map(tb_changes, & &1.record) == [
               %{"id" => 1, "val" => 2},
               %{"id" => 2, "val" => 3}
             ]
    end
  end

  # =========================================================================
  # incremental driver — the primitive Task 15 uses to checkpoint per-txn.
  # =========================================================================

  describe "step/2 — incremental fold for the owning GenServer (Task 15)" do
    test "stepping through a txn advances the state watermark at the terminator" do
      {uuid, gno} = gtid_of("simple_dml", "05-gtid.bin")
      state = Assembler.new(empty_start())

      # Before the terminator: no output, watermark unchanged.
      {state, emitted} =
        Enum.reduce(
          events("simple_dml", [
            "05-gtid.bin",
            "06-query.bin",
            "07-table_map.bin",
            "08-write_rows.bin"
          ]),
          {state, []},
          fn event, {st, out} ->
            assert {:cont, o, st2} = Assembler.step(st, event)
            {st2, out ++ o}
          end
        )

      assert emitted == []
      refute Gtid.member?(Gtid.parse(Assembler.position(state).gtid_set), {uuid, gno})

      # The XID terminator emits the transaction and advances the state watermark.
      assert {:cont, [%Transaction{}], state} =
               Assembler.step(state, event!("simple_dml", "09-xid.bin"))

      assert Gtid.member?(Gtid.parse(Assembler.position(state).gtid_set), {uuid, gno})
    end

    test "step/2 surfaces the fail-closed halt to the caller unchanged" do
      state = Assembler.new(empty_start())
      assert {:halt, :unsupported_transaction_shape} = Assembler.step(state, xa_prepare_event())
    end
  end

  # =========================================================================
  # SYNTHESIS GRAFTS — stream-desync tripwires (fail closed, never silently absorb)
  # and the non-lossy batch return that preserves already-committed transactions.
  # =========================================================================

  describe "SAFETY — stream desyncs fail closed, never silently drop/overwrite a buffer" do
    test "a GTID arriving while a transaction is still open aborts (no silent overwrite)" do
      # Two GTIDs with no terminator between them: the first transaction never closed.
      # Silently opening the second would DROP the first's buffer — a data loss. Fail
      # closed instead.
      seq = events("simple_dml", ["05-gtid.bin", "10-gtid.bin"])

      assert {:error, :gtid_within_open_transaction, [], %Position{}} =
               Assembler.run(seq, empty_start())
    end

    test "a row event with no open transaction aborts fail closed" do
      # WRITE_ROWS with no preceding GTID/BEGIN — a desync. (Also needs no TABLE_MAP:
      # the txn-scope guard fires before registry resolution.)
      seq = events("simple_dml", ["08-write_rows.bin"])

      assert {:error, :rows_without_transaction, [], %Position{}} =
               Assembler.run(seq, empty_start())
    end

    test "a terminator with no open transaction aborts fail closed" do
      seq = events("simple_dml", ["09-xid.bin"])

      assert {:error, :terminator_without_transaction, [], %Position{}} =
               Assembler.run(seq, empty_start())
    end
  end

  describe "self-committing DDL classification across statement forms (kind + table only)" do
    # A real GTID opens the scope; a constructed QUERY(DDL) is the self-committing
    # terminator. Only kind + table may surface (Rule 1); the table must be the object
    # name, never an `IF`/`NOT`/`EXISTS` keyword.
    setup do
      {:ok, {:gtid, {uuid, gno}}} = Decoder.decode(event!("alter_ddl", "05-gtid.bin"))
      {:ok, %{gtid_ev: event!("alter_ddl", "05-gtid.bin"), gtid: "#{uuid}:#{gno}"}}
    end

    for {sql, kind, table} <- [
          {"CREATE TABLE foo (id INT)", :create_table, "foo"},
          {"CREATE TABLE IF NOT EXISTS foo (id INT)", :create_table, "foo"},
          {"CREATE TEMPORARY TABLE bar (id INT)", :create_table, "bar"},
          {"CREATE TEMPORARY TABLE IF NOT EXISTS bar (id INT)", :create_table, "bar"},
          {"DROP TABLE foo", :drop_table, "foo"},
          {"DROP TABLE IF EXISTS foo", :drop_table, "foo"},
          {"TRUNCATE TABLE baz", :truncate, "baz"},
          {"ALTER TABLE `mydb`.`widgets_ddl` ADD COLUMN x INT", :alter_table, "widgets_ddl"}
        ] do
      test "#{sql} -> #{kind}/#{table}", %{gtid_ev: gtid_ev, gtid: gtid} do
        assert {:ok, [%SchemaChange{} = sc], _wm} =
                 Assembler.run([gtid_ev, query_event("probe_db", unquote(sql))], empty_start())

        assert sc.kind == unquote(kind)
        assert sc.table == unquote(table)
        assert sc.gtid == gtid
        # Rule 1: the raw SQL never survives in any field.
        refute inspect(sc) =~ "IF"
        refute inspect(sc) =~ "INT"
      end
    end

    test "a non-DDL / unrecognised statement fails to a safe {:other, nil} — guesses nothing" do
      assert {:ok, [%SchemaChange{kind: :other, schema: nil, table: nil}], _wm} =
               Assembler.run(
                 [
                   event!("alter_ddl", "05-gtid.bin"),
                   query_event("probe_db", "ANALYZE TABLE foo")
                 ],
                 empty_start()
               )
    end
  end

  describe "orphan QUERY desyncs fail closed (behavior-adjacent tripwires)" do
    test "a BEGIN with no open GTID aborts :begin_without_gtid" do
      assert {:error, :begin_without_gtid, [], %Position{}} =
               Assembler.run([query_event("probe_db", "BEGIN")], empty_start())
    end

    test "a non-BEGIN QUERY with no open GTID aborts :query_without_gtid" do
      assert {:error, :query_without_gtid, [], %Position{}} =
               Assembler.run(
                 [query_event("probe_db", "ALTER TABLE foo ADD x INT")],
                 empty_start()
               )
    end
  end

  describe "run/3 preserves already-COMMITTED transactions across a later halt (non-lossy batch)" do
    test "a committed txn BEFORE an XA txn is returned; only the XA in-flight is discarded" do
      # Stream: [full INSERT txn, committed by XID] then [GTID/BEGIN/TABLE_MAP/UPDATE]
      # terminated by an XA_PREPARE. The first transaction is genuinely committed and
      # must survive the halt; the XA transaction's accumulated update is discarded.
      committed =
        events("simple_dml", [
          "05-gtid.bin",
          "06-query.bin",
          "07-table_map.bin",
          "08-write_rows.bin",
          "09-xid.bin"
        ])

      xa_txn =
        events("simple_dml", [
          "10-gtid.bin",
          "11-query.bin",
          "12-table_map.bin",
          "13-update_rows.bin"
        ]) ++ [xa_prepare_event()]

      {uuid1, gno1} = gtid_of("simple_dml", "05-gtid.bin")

      assert {:halt, :unsupported_transaction_shape, [%Transaction{} = t1], %Position{} = wm} =
               Assembler.run(committed ++ xa_txn, empty_start())

      # The surviving output is exactly the first INSERT transaction — not the XA update.
      assert t1.gtid == "#{uuid1}:#{gno1}"
      assert [%Change{op: :insert, table: "widgets"}] = t1.changes
      # The watermark carries the committed GTID (so a caller can checkpoint it) but not
      # the discarded XA one.
      assert Gtid.member?(Gtid.parse(wm.gtid_set), {uuid1, gno1})
      {uuid2, gno2} = gtid_of("simple_dml", "10-gtid.bin")
      refute Gtid.member?(Gtid.parse(wm.gtid_set), {uuid2, gno2})
    end
  end
end

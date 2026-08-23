defmodule Capstan.Binlog.DecoderTest do
  use ExUnit.Case, async: true

  alias Capstan.Binlog.{Decoder, Event, TableMap}

  @fixtures_root Path.join([__DIR__, "..", "..", "fixtures", "binlog"])

  defp fixture(scenario, filename) do
    [@fixtures_root, scenario, filename]
    |> Path.join()
    |> File.read!()
  end

  # Read a captured fixture, run it through Event.parse/1 (header + CRC), then decode.
  defp decode_fixture(scenario, filename) do
    {:ok, event} = Event.parse(fixture(scenario, filename))
    Decoder.decode(event)
  end

  # Minimal synthetic %Event{} for the type-byte-keyed branches that have no fixture.
  defp event(type, body \\ <<>>) do
    %Event{
      type: type,
      timestamp: 0,
      server_id: 1,
      event_size: 19 + byte_size(body),
      log_pos: 0,
      flags: 0,
      body: body
    }
  end

  describe "structural markers (Task 7 fixtures)" do
    test "ROTATE (4) decodes to the next log name + position" do
      assert {:ok, {:rotate, next_name, position}} = decode_fixture("simple_dml", "01-rotate.bin")
      # captured artificial opener: real binlog file name, byte position within it
      assert next_name =~ ~r/\.\d{6}$/
      assert position >= 0
    end

    test "FORMAT_DESCRIPTION (15) decodes to binlog + server version" do
      assert {:ok, {:format_description, binlog_version, server_version}} =
               decode_fixture("simple_dml", "02-format_description.bin")

      assert binlog_version == 4
      assert server_version =~ ~r/^\d+\.\d+/
    end

    test "PREVIOUS_GTIDS (35) decodes to a structural marker (opaque set not consumed here)" do
      assert {:ok, :previous_gtids} = decode_fixture("simple_dml", "03-previous_gtids.bin")
    end

    test "HEARTBEAT (27) decodes to a marker without choking on its body" do
      assert {:ok, :heartbeat} = decode_fixture("simple_dml", "04-heartbeat.bin")
    end

    test "XID (16) decodes to the commit marker + transaction id" do
      assert {:ok, {:xid, xid}} = decode_fixture("simple_dml", "09-xid.bin")
      assert xid > 0
    end

    test "STOP (3) decodes to a marker (keyed on the type byte; empty body)" do
      assert {:ok, :stop} = Decoder.decode(event(3))
    end
  end

  describe "GTID (33)" do
    test "decodes to {uuid, gno} — the transaction identity Task 12/15 consume" do
      assert {:ok, {:gtid, {uuid, gno}}} = decode_fixture("simple_dml", "05-gtid.bin")
      # The substrate's server_uuid is inherent to real captured bytes and differs if
      # the container is recreated (fixtures README) — assert shape, never a literal.
      assert uuid =~ ~r/\A[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\z/
      assert gno > 0
      # cross-check: the decoded identity is a well-formed single GTID for Capstan.Gtid
      assert Capstan.Gtid.member?(Capstan.Gtid.parse("#{uuid}:#{gno}"), {uuid, gno})
    end
  end

  describe "QUERY (2) — SQL text IS returned (Task 12 inspects it for terminators)" do
    test "BEGIN opens a transaction" do
      assert {:ok, {:query, %{schema: "probe_db", sql: "BEGIN"}}} =
               decode_fixture("simple_dml", "06-query.bin")
    end

    test "self-committing DDL carries the full ALTER text" do
      assert {:ok, {:query, %{schema: "probe_db", sql: sql}}} =
               decode_fixture("alter_ddl", "06-query.bin")

      assert sql == "ALTER TABLE widgets_ddl ADD COLUMN extra INT DEFAULT 7"
    end

    test "COMMIT terminates a non-transactional-engine statement" do
      assert {:ok, {:query, %{schema: "probe_db", sql: "COMMIT"}}} =
               decode_fixture("myisam", "09-query.bin")
    end
  end

  describe "TABLE_MAP (19) — this module owns the ENTIRE body (F12)" do
    test "simple 3-column table: identity, raw type list, raw metadata blob, TLV names" do
      assert {:ok, %TableMap{} = tm} = decode_fixture("simple_dml", "07-table_map.bin")

      assert tm.schema == "probe_db"
      assert tm.table == "widgets"
      assert tm.column_types == [3, 15, 3]
      assert tm.column_names == ["id", "name", "qty"]
      # column_metadata is stored RAW (length-prefixed blob consumed as a unit) — the
      # per-column split is Task 11's parse_col_meta, NOT this task's.
      assert tm.column_metadata == <<200, 0>>
      assert tm.table_id > 0
    end

    test "all_types: full type list, RAW metadata blob (not split), and every F3 TLV field" do
      assert {:ok, %TableMap{} = tm} = decode_fixture("all_types", "07-table_map.bin")

      assert tm.schema == "probe_db"
      assert tm.table == "all_types"

      assert tm.column_types ==
               [3, 1, 1, 2, 2, 9, 9, 3, 3, 8, 8, 246, 15, 254, 252, 252, 18, 17, 10, 19, 254]

      # F12: the whole 13-byte metadata blob is retained RAW — Task 11 splits it.
      assert tm.column_metadata == <<10, 2, 200, 0, 254, 40, 2, 2, 0, 0, 0, 247, 1>>
      assert byte_size(tm.column_metadata) == 13

      # F3 column_names (TLV 4)
      assert tm.column_names ==
               ~w(id tiny_s tiny_u small_s small_u medium_s medium_u int_s int_u
                  big_s big_u dec_col varchar_col char_col text_col blob_col
                  datetime_col timestamp_col date_col time_col enum_col)

      # F3 signedness (TLV 1): a bitmap over the 12 NUMERIC columns, MSB-first, 1 = UNSIGNED.
      # id,tiny_s,tiny_u,small_s,small_u,medium_s,medium_u,int_s,int_u,big_s,big_u,dec_col
      #  0    0     1      0       1       0        1       0     1     0     1      0
      # -> 0b00101010, 0b10100000 = <<0x2A, 0xA0>>. Marks exactly the *_u columns unsigned.
      assert tm.signedness == <<0x2A, 0xA0>>

      # F3 default_charset (TLV 2) retained
      assert is_binary(tm.default_charset)

      # F3 enum_str_values (TLV 6): one ENUM column with its three allowed strings.
      # This TLV arrives AFTER the unknown TLV type 10 in the byte stream, so a correct
      # value here also proves unknown TLVs are skipped by their lenenc length (F3
      # tolerate-unknown), and set_str_values stays empty (no SET column present).
      assert tm.enum_str_values == [["small", "medium", "large"]]
      assert tm.set_str_values == []
    end

    test "multi_table: two distinct TABLE_MAPs, and each row event resolves to its own table_id (Q3)" do
      assert {:ok, %TableMap{schema: "probe_db", table: "ta"} = ta} =
               decode_fixture("multi_table", "07-table_map.bin")

      assert {:ok, %TableMap{schema: "probe_db", table: "tb"} = tb} =
               decode_fixture("multi_table", "08-table_map.bin")

      assert ta.table_id != tb.table_id

      assert {:ok, {:update_rows, tid_a, _before, _after, _rows}} =
               decode_fixture("multi_table", "09-update_rows.bin")

      assert {:ok, {:update_rows, tid_b, _before2, _after2, _rows2}} =
               decode_fixture("multi_table", "10-update_rows.bin")

      # The Q3 counterexample: the first row event belongs to ta, the second to tb.
      assert tid_a == ta.table_id
      assert tid_b == tb.table_id
    end

    test "tolerates the unknown COLUMN_VISIBILITY TLV (type 12, 8.0.46) rather than failing" do
      # simple_dml's optional metadata ends with TLV types 8 and 12 — both unknown to
      # C1. A successful decode with names intact proves they are scanned past, not choked on.
      assert {:ok, %TableMap{column_names: ["id", "name", "qty"]}} =
               decode_fixture("simple_dml", "07-table_map.bin")
    end
  end

  describe "ROWS v2 wire types (30/31/32) — STRUCTURE only; row values stay raw bytes" do
    test "WRITE_ROWS (30): {table_id, present bitmap, raw row bytes} — values NOT decoded" do
      assert {:ok, %TableMap{} = tm} = decode_fixture("simple_dml", "07-table_map.bin")

      assert {:ok, {:write_rows, table_id, present, rows}} =
               decode_fixture("simple_dml", "08-write_rows.bin")

      assert table_id == tm.table_id
      assert byte_size(present) == 1
      assert byte_size(rows) == 20
    end

    test "UPDATE_ROWS (31): carries BOTH before- and after-image present bitmaps" do
      assert {:ok, {:update_rows, table_id, before_cols, after_cols, rows}} =
               decode_fixture("simple_dml", "13-update_rows.bin")

      assert is_integer(table_id) and table_id > 0
      assert byte_size(before_cols) == 1
      assert byte_size(after_cols) == 1
      assert byte_size(rows) == 40
    end

    test "DELETE_ROWS (32): {table_id, present bitmap, raw row bytes}" do
      assert {:ok, {:delete_rows, table_id, present, rows}} =
               decode_fixture("simple_dml", "18-delete_rows.bin")

      assert is_integer(table_id) and table_id > 0
      assert byte_size(present) == 1
      assert byte_size(rows) == 20
    end
  end

  describe "SAFETY tripwire — ROWS_QUERY_LOG_EVENT (29) discards the body (Q16)" do
    test "decodes to a discard marker and the original SQL text NEVER appears in the term" do
      {:ok, event} = Event.parse(fixture("rows_query", "07-rows_query.bin"))

      # Sanity: the captured body really does carry the complete SQL, so discarding it
      # is load-bearing (this event carries every literal of the row change — a total
      # Rule-1 leak if surfaced).
      assert String.contains?(event.body, "rq_widgets")
      assert String.contains?(event.body, "rq-one")

      decoded = Decoder.decode(event)
      assert decoded == {:ok, {:rows_query, :discarded}}

      # Serialize the WHOLE returned term (no truncation) and prove no fragment leaked.
      serialized = inspect(decoded, limit: :infinity, printable_limit: :infinity)
      refute serialized =~ "rq_widgets"
      refute serialized =~ "rq-one"
      refute serialized =~ "INSERT"
    end
  end

  describe "SAFETY tripwire — XA_PREPARE_LOG_EVENT (38) fail-closed (Q13/F9, ADR-0006)" do
    # The decoder DECODES the XID (layout per the server source); the loud default
    # :refuse halt lives in the assembler fold (assembler_test proves it) — and a
    # malformed body is a decode refusal, never a guess.
    test "a well-formed type-38 body decodes its XID exactly" do
      body = <<0, 7::32-little, 8::32-little, 5::32-little, "gtrid-42", "bqual">>

      assert {:ok,
              {:xa_prepare, %{one_phase: false, format_id: 7, gtrid: "gtrid-42", bqual: "bqual"}}} =
               Decoder.decode(event(38, body))
    end

    test "the REAL captured fixture decodes (conformance — live 8.0 bytes)" do
      assert {:ok, {:xa_prepare, %{one_phase: false} = xid}} =
               decode_fixture("xa", "10-xa_prepare.bin")

      assert xid.format_id == 7 and xid.gtrid == "xa-gtrid" and xid.bqual == "xa-bqual"
    end

    test "a malformed body is refused, never guessed at" do
      assert {:error, {:xa_prepare_malformed, 0}} = Decoder.decode(event(38))
      assert {:error, {:xa_prepare_malformed, 4}} = Decoder.decode(event(38, <<1, 2, 3, 4>>))
    end
  end

  describe "refusals" do
    test "TRANSACTION_PAYLOAD (40) inflates to its inner event stream (real fixture)" do
      path =
        Path.join([
          @fixtures_root,
          "zstd_small",
          "06-transaction_payload.bin"
        ])

      {:ok, parsed} = Event.parse(File.read!(path))
      assert {:ok, {:transaction_payload, inner}} = Decoder.decode(parsed)
      assert Enum.map(inner, & &1.type) == [2, 19, 30, 16]
    end

    test "an unknown event type fails closed, never a silent skip" do
      assert Decoder.decode(event(200)) == {:error, {:unknown_event_type, 200}}
    end
  end

  describe "robustness sweep over every captured fixture" do
    test "every fixture across every scenario decodes to {:ok, _} without raising" do
      fixtures = Path.wildcard(Path.join(@fixtures_root, "*/*.bin"))
      assert fixtures != []

      for path <- fixtures do
        {:ok, parsed} = Event.parse(File.read!(path))

        assert {:ok, _decoded} = Decoder.decode(parsed),
               "expected #{path} to decode to {:ok, _}"
      end
    end
  end
end

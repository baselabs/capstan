defmodule Capstan.Binlog.RowsTest do
  use ExUnit.Case, async: true

  alias Capstan.Binlog.{Decoder, Event, Rows}

  @fixtures_root Path.join([__DIR__, "..", "..", "fixtures", "binlog"])

  # Every assertion below is driven by REAL captured bytes: the fixture is run through
  # Event.parse/1 (header + CRC) then Decoder.decode/1, and Rows.decode/2 is fed the
  # resulting raw tuple + the resolved %TableMap{}. No synthetic row images.
  defp decode_fixture(scenario, filename) do
    {:ok, event} = Event.parse(File.read!(Path.join([@fixtures_root, scenario, filename])))
    Decoder.decode(event)
  end

  defp table_map(scenario, filename) do
    {:ok, tm} = decode_fixture(scenario, filename)
    tm
  end

  defp rows_tuple(scenario, filename) do
    {:ok, tuple} = decode_fixture(scenario, filename)
    tuple
  end

  describe "all_types — every C1-supported type, INSERT boundary values (real fixture)" do
    setup do
      tm = table_map("all_types", "07-table_map.bin")
      tuple = rows_tuple("all_types", "08-write_rows.bin")
      {:ok, %{result: Rows.decode(tuple, tm)}}
    end

    test "decodes to a single INSERT row", %{result: result} do
      assert {:ok, {:insert, [record]}} = result
      assert map_size(record) == 21
    end

    test "integers decode at every width, signed AND unsigned (F3)", %{result: result} do
      assert {:ok, {:insert, [r]}} = result

      assert r["id"] == 1
      assert r["tiny_s"] == -128
      assert r["tiny_u"] == 255
      assert r["small_s"] == -32_768
      assert r["small_u"] == 65_535
      assert r["medium_s"] == -8_388_608
      assert r["medium_u"] == 16_777_215
      assert r["int_s"] == -2_147_483_648
      assert r["int_u"] == 4_294_967_295
      assert r["big_s"] == -9_223_372_036_854_775_808
    end

    test "F3 NON-VACUITY: BIGINT UNSIGNED 18446744073709551615 must NOT be -1", %{result: result} do
      assert {:ok, {:insert, [r]}} = result
      # The same 8 bytes (0xFF * 8) decode to -1 when signedness is ignored. Resolving
      # it from TLV type 1 is the only thing that yields the unsigned value.
      assert r["big_u"] == 18_446_744_073_709_551_615
      refute r["big_u"] == -1
    end

    test "DECIMAL(10,2) packed BCD decodes to 12345.67", %{result: result} do
      assert {:ok, {:insert, [r]}} = result
      assert Decimal.equal?(r["dec_col"], Decimal.new("12345.67"))
    end

    test "VARCHAR / CHAR / TEXT / BLOB decode by their meta-driven length prefix", %{
      result: result
    } do
      assert {:ok, {:insert, [r]}} = result
      assert r["varchar_col"] == "varchar-value"
      assert r["char_col"] == "char-val"
      assert r["text_col"] == "text value here"
      assert r["blob_col"] == "blob-bytes"
    end

    test "temporals at fsp 0 decode to calendar values", %{result: result} do
      assert {:ok, {:insert, [r]}} = result
      assert r["datetime_col"] == ~N[2024-01-15 10:30:00]
      assert r["timestamp_col"] == ~U[2024-01-15 10:30:00Z]
      assert r["date_col"] == ~D[2024-01-15]
      assert r["time_col"] == ~T[10:30:00]
    end

    test "ENUM decodes to its string member", %{result: result} do
      assert {:ok, {:insert, [r]}} = result
      assert r["enum_col"] == "medium"
    end

    test "the whole row decodes to the exact expected map", %{result: result} do
      assert {:ok, {:insert, [record]}} = result

      assert %{
               "id" => 1,
               "tiny_s" => -128,
               "tiny_u" => 255,
               "small_s" => -32_768,
               "small_u" => 65_535,
               "medium_s" => -8_388_608,
               "medium_u" => 16_777_215,
               "int_s" => -2_147_483_648,
               "int_u" => 4_294_967_295,
               "big_s" => -9_223_372_036_854_775_808,
               "big_u" => 18_446_744_073_709_551_615,
               "varchar_col" => "varchar-value",
               "char_col" => "char-val",
               "text_col" => "text value here",
               "blob_col" => "blob-bytes",
               "datetime_col" => ~N[2024-01-15 10:30:00],
               "timestamp_col" => ~U[2024-01-15 10:30:00Z],
               "date_col" => ~D[2024-01-15],
               "time_col" => ~T[10:30:00],
               "enum_col" => "medium"
             } = record

      assert Decimal.equal?(record["dec_col"], Decimal.new("12345.67"))
    end
  end

  describe "frac_temporal — meta-driven fractional precision (real fixture)" do
    test "DATETIME(3)/TIME(6)/TIMESTAMP(6) read their fractional bytes from the meta" do
      tm = table_map("frac_temporal", "07-table_map.bin")
      tuple = rows_tuple("frac_temporal", "08-write_rows.bin")

      assert {:ok, {:insert, [r]}} = Rows.decode(tuple, tm)
      assert r["id"] == 1
      # A fixed-width temporal decoder (ignoring meta 03/06/06) mis-values these AND
      # desynchronises the columns after them.
      assert r["dt"] == ~N[2024-01-15 10:30:00.123]
      assert r["tm"] == ~T[10:30:00.123456]
      assert r["ts"] == ~U[2024-01-15 10:30:00.654321Z]

      assert r["dt"].microsecond == {123_000, 3}
      assert r["tm"].microsecond == {123_456, 6}
      assert r["ts"].microsecond == {654_321, 6}
    end
  end

  describe "json_col — MySQL internal JSON binary format (real fixture)" do
    test "decodes an object with nested array and literals" do
      tm = table_map("json_col", "07-table_map.bin")
      tuple = rows_tuple("json_col", "08-write_rows.bin")

      assert {:ok, {:insert, [r]}} = Rows.decode(tuple, tm)
      assert r["id"] == 1
      assert r["doc"] == %{"a" => 1, "b" => [true, false, nil], "c" => "text"}
    end
  end

  describe "set_type — SET NAMED SAFETY PROPERTY: detect and fail closed (real fixture)" do
    test "a SET column halts with :unsupported_column_type, never an ENUM-style value" do
      tm = table_map("set_type", "07-table_map.bin")
      tuple = rows_tuple("set_type", "08-write_rows.bin")

      # SET wires as type 254 like ENUM; its row image is a bitfield ('a,c' = 0x05),
      # NOT an index. Decoding it as an ENUM would emit a silently-wrong member. C1
      # defers SET (C4) and MUST fail closed here.
      assert {:error, {:unsupported_column_type, detail}} = Rows.decode(tuple, tm)
      assert detail.reason == :set_deferred
    end
  end

  describe "simple_dml — INSERT / UPDATE / DELETE, before+after pair (real fixture)" do
    setup do
      {:ok, %{tm: table_map("simple_dml", "07-table_map.bin")}}
    end

    test "WRITE_ROWS decodes to an INSERT row", %{tm: tm} do
      tuple = rows_tuple("simple_dml", "08-write_rows.bin")

      assert {:ok, {:insert, [%{"id" => 1, "name" => "widget-one", "qty" => 10}]}} =
               Rows.decode(tuple, tm)
    end

    test "UPDATE_ROWS decodes to a before/after pair", %{tm: tm} do
      tuple = rows_tuple("simple_dml", "13-update_rows.bin")

      assert {:ok, {:update, [{before_row, after_row}]}} = Rows.decode(tuple, tm)
      assert before_row == %{"id" => 1, "name" => "widget-one", "qty" => 10}
      assert after_row == %{"id" => 1, "name" => "widget-one", "qty" => 20}
    end

    test "DELETE_ROWS decodes to a DELETE row (post-update state)", %{tm: tm} do
      tuple = rows_tuple("simple_dml", "18-delete_rows.bin")

      assert {:ok, {:delete, [%{"id" => 1, "name" => "widget-one", "qty" => 20}]}} =
               Rows.decode(tuple, tm)
    end
  end

  describe "multi_table — MANY rows per event + Q3 wrong-schema guard (real fixture)" do
    test "a single UPDATE event carries multiple before/after pairs" do
      ta = table_map("multi_table", "07-table_map.bin")
      tuple = rows_tuple("multi_table", "09-update_rows.bin")

      assert {:ok, {:update, pairs}} = Rows.decode(tuple, ta)
      assert length(pairs) == 2

      assert pairs == [
               {%{"id" => 1, "val" => 100}, %{"id" => 1, "val" => 101}},
               {%{"id" => 2, "val" => 200}, %{"id" => 2, "val" => 202}}
             ]
    end

    test "Q3 GUARD: casting ta's rows against tb's map is refused, not silently wrong" do
      tb = table_map("multi_table", "08-table_map.bin")
      ta_rows = rows_tuple("multi_table", "09-update_rows.bin")

      # ta_rows carries ta's table_id (104); tb's map is table_id 105. Both are live at
      # once in a multi-table statement, so a wrong pairing is the exact Q3 silent-wrong
      # failure. The table_id assertion fails it closed.
      assert {:error, {:table_id_mismatch, detail}} = Rows.decode(ta_rows, tb)
      assert detail.event_table_id != detail.table_map_table_id
    end
  end

  # Every captured fixture is a FULL row image (all columns present, none NULL), so the
  # present-bitmap and per-row NULL-bitmap branches — and truncation — cannot be driven
  # from a fixture alone. These construct the raw image over the REAL `widgets`
  # %TableMap{} (real types/meta/names from simple_dml/07); only the row bytes are
  # constructed, and each asserts a branch a probe-style all-present decoder gets wrong.
  describe "present bitmap / NULL bitmap / truncation (constructed image, real %TableMap{})" do
    setup do
      # widgets columns, in order: id INT (bit 0), name VARCHAR(50) (bit 1), qty INT (bit 2).
      {:ok, %{tm: table_map("simple_dml", "07-table_map.bin")}}
    end

    test "a partial columns-PRESENT bitmap skips the absent column and its bytes", %{tm: tm} do
      # Present = {id, qty}; name (bit 1) is absent. An all-present assumption would read
      # name's length prefix out of id's/qty's bytes and mis-slice the rest.
      present = <<0b0000_0101>>
      raw = <<0>> <> <<1, 0, 0, 0>> <> <<42, 0, 0, 0>>

      assert {:ok, {:insert, [row]}} =
               Rows.decode({:write_rows, tm.table_id, present, raw}, tm)

      assert row == %{"id" => 1, "qty" => 42}
    end

    test "a present-but-NULL column decodes to nil and consumes no value bytes", %{tm: tm} do
      # All three present; name (bit 1) is NULL in the per-row bitmap, so it must appear
      # as nil with zero bytes read for it — the value bytes are id then qty only.
      present = <<0b0000_0111>>
      raw = <<0b0000_0010>> <> <<1, 0, 0, 0>> <> <<7, 0, 0, 0>>

      assert {:ok, {:insert, [row]}} =
               Rows.decode({:write_rows, tm.table_id, present, raw}, tm)

      assert row == %{"id" => 1, "name" => nil, "qty" => 7}
    end

    test "a multi-row WRITE image decodes every row until the bytes are consumed", %{tm: tm} do
      row1 = <<0>> <> <<1, 0, 0, 0>> <> <<3, ?a, ?b, ?c>> <> <<9, 0, 0, 0>>
      row2 = <<0>> <> <<2, 0, 0, 0>> <> <<3, ?x, ?y, ?z>> <> <<8, 0, 0, 0>>

      assert {:ok, {:insert, [first, second]}} =
               Rows.decode({:write_rows, tm.table_id, <<0b0000_0111>>, row1 <> row2}, tm)

      assert first == %{"id" => 1, "name" => "abc", "qty" => 9}
      assert second == %{"id" => 2, "name" => "xyz", "qty" => 8}
    end

    test "an image too short for its NULL bitmap fails closed, never raises" do
      # The real all_types image (21 columns present → a 3-byte NULL bitmap), truncated
      # to 2 bytes. Before the fail-closed halt this raised a MatchError; it must now be
      # a clean {:error, {:truncated_row_image, _}} the pipeline can dispatch.
      tm = table_map("all_types", "07-table_map.bin")
      {:write_rows, table_id, present, raw} = rows_tuple("all_types", "08-write_rows.bin")
      truncated = binary_part(raw, 0, 2)

      assert {:error, {:truncated_row_image, detail}} =
               Rows.decode({:write_rows, table_id, present, truncated}, tm)

      assert detail.have < detail.need_null_bytes
    end

    test "the per-row NULL bitmap is sized by the PRESENT count, across a byte boundary" do
      # Over the real 21-column all_types %TableMap{}, present ONLY the first 9 columns
      # (id + the 8 sub-INT-width integers). 9 present → a 2-byte NULL bitmap; the full
      # 21-column count would be 3 bytes. A decoder that sized the bitmap by the TOTAL
      # column count would consume one value byte as bitmap and desync `id`. The all-
      # present fixtures round 21→3 and cannot distinguish the two; this crosses the 8→9
      # boundary where present-count (2) and total-count (3) diverge.
      tm = table_map("all_types", "07-table_map.bin")

      present = <<0xFF, 0x01, 0x00>>
      null_bitmap = <<0, 0>>

      values =
        <<1, 0, 0, 0>> <>
          <<5>> <>
          <<6>> <>
          <<7, 0>> <>
          <<8, 0>> <>
          <<9, 0, 0>> <> <<10, 0, 0>> <> <<11, 0, 0, 0>> <> <<12, 0, 0, 0>>

      assert {:ok, {:insert, [row]}} =
               Rows.decode({:write_rows, tm.table_id, present, null_bitmap <> values}, tm)

      assert row == %{
               "id" => 1,
               "tiny_s" => 5,
               "tiny_u" => 6,
               "small_s" => 7,
               "small_u" => 8,
               "medium_s" => 9,
               "medium_u" => 10,
               "int_s" => 11,
               "int_u" => 12
             }
    end
  end
end

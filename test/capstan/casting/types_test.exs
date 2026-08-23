defmodule Capstan.Casting.TypesTest do
  use ExUnit.Case, async: true

  alias Capstan.Binlog.{Decoder, Event}
  alias Capstan.Casting.Types

  @fixtures_root Path.join([__DIR__, "..", "..", "fixtures", "binlog"])

  defp decode_fixture(scenario, filename) do
    {:ok, event} = Event.parse(File.read!(Path.join([@fixtures_root, scenario, filename])))
    Decoder.decode(event)
  end

  defp table_map(scenario, filename) do
    {:ok, tm} = decode_fixture(scenario, filename)
    tm
  end

  # The raw value-byte stream of the (all-present, no-null) write image: raw with its
  # leading NULL bitmap stripped. Lets direct cast/4 checks run over REAL captured bytes.
  defp value_bytes(scenario, filename, null_bytes) do
    {:ok, {:write_rows, _tid, _present, raw}} = decode_fixture(scenario, filename)
    <<_null::binary-size(^null_bytes), values::binary>> = raw
    values
  end

  describe "parse_col_meta/2 — per-column meta widths from real TABLE_MAPs" do
    test "all_types: every supported type's meta width, incl. CHAR vs ENUM discrimination" do
      tm = table_map("all_types", "07-table_map.bin")

      assert {:ok, metas} = Types.parse_col_meta(tm.column_types, tm.column_metadata)

      assert metas == [
               {:int, 4},
               {:int, 1},
               {:int, 1},
               {:int, 2},
               {:int, 2},
               {:int, 3},
               {:int, 3},
               {:int, 4},
               {:int, 4},
               {:int, 8},
               {:int, 8},
               {:decimal, 10, 2},
               {:varstring, 200},
               {:char, 40},
               {:blob, 2},
               {:blob, 2},
               {:datetime, 0},
               {:timestamp, 0},
               :date,
               {:time, 0},
               {:enum, 1}
             ]
    end

    test "set_type: STRING meta byte0 0xF8 is detected as SET (not ENUM/CHAR)" do
      tm = table_map("set_type", "07-table_map.bin")

      assert {:ok, [{:int, 4}, {:set, 1}]} =
               Types.parse_col_meta(tm.column_types, tm.column_metadata)
    end

    test "frac_temporal: fsp read from meta 03/06/06 (not the type byte)" do
      tm = table_map("frac_temporal", "07-table_map.bin")

      assert {:ok, [{:int, 4}, {:datetime, 3}, {:time, 6}, {:timestamp, 6}]} =
               Types.parse_col_meta(tm.column_types, tm.column_metadata)
    end

    test "json_col: JSON length-byte width" do
      tm = table_map("json_col", "07-table_map.bin")

      assert {:ok, [{:int, 4}, {:json, 4}]} =
               Types.parse_col_meta(tm.column_types, tm.column_metadata)
    end

    test "simple_dml + multi_table: LONG has zero meta, VARCHAR consumes two" do
      simple = table_map("simple_dml", "07-table_map.bin")

      assert {:ok, [{:int, 4}, {:varstring, 200}, {:int, 4}]} =
               Types.parse_col_meta(simple.column_types, simple.column_metadata)

      multi = table_map("multi_table", "07-table_map.bin")

      assert {:ok, [{:int, 4}, {:int, 4}]} =
               Types.parse_col_meta(multi.column_types, multi.column_metadata)
    end
  end

  describe "parse_col_meta/2 — fail closed on unenumerated types (no 1-byte guess)" do
    test "an unsupported type byte is refused, reporting its column position" do
      # 4 = MYSQL_TYPE_FLOAT — a real type whose meta width C1 does not enumerate.
      assert {:error, {:unsupported_column_type, detail}} = Types.parse_col_meta([4], <<0x04>>)
      assert detail.wire_type == 4
      assert detail.column_index == 0
    end

    test "the refusal points at the offending column even after supported ones" do
      # LONG (0 meta) then DOUBLE (5, unsupported): the failure is column 1, not 0.
      assert {:error, {:unsupported_column_type, %{wire_type: 5, column_index: 1}}} =
               Types.parse_col_meta([3, 5], <<0x08>>)
    end
  end

  describe "cast/4 — signedness (F3), on the REAL big_u bytes" do
    test "the same 8 bytes yield the unsigned value or -1 purely by the signedness bit" do
      # all_types value stream; big_u is the 11th value (offset 32 into the stream:
      # id4 tiny1 tiny1 small2 small2 medium3 medium3 int4 int4 big_s8 = 32).
      values = value_bytes("all_types", "08-write_rows.bin", 3)
      big_u_bytes = binary_part(values, 32, 8)

      # Prove these are the real captured bytes (BIGINT UNSIGNED max = 0xFF * 8).
      assert big_u_bytes == <<0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF>>

      assert Types.cast({:int, 8}, false, [], big_u_bytes) ==
               {:ok, 18_446_744_073_709_551_615, <<>>}

      assert Types.cast({:int, 8}, true, [], big_u_bytes) == {:ok, -1, <<>>}
    end
  end

  describe "cast/4 — DECIMAL packed BCD, on the REAL dec_col bytes" do
    test "the 5 captured bytes decode to 12345.67" do
      values = value_bytes("all_types", "08-write_rows.bin", 3)
      # dec_col follows the 11 integers (offset 40) and is 5 bytes for DECIMAL(10,2).
      dec_bytes = binary_part(values, 40, 5)
      assert dec_bytes == <<0x80, 0x00, 0x30, 0x39, 0x43>>

      assert {:ok, decimal, <<>>} = Types.cast({:decimal, 10, 2}, false, [], dec_bytes)
      assert Decimal.equal?(decimal, Decimal.new("12345.67"))
    end
  end

  describe "cast/4 — ENUM index resolution" do
    test "index selects the 1-based member; index 0 is the empty member" do
      assert Types.cast({:enum, 1}, false, ["small", "medium", "large"], <<2>>) ==
               {:ok, "medium", <<>>}

      assert Types.cast({:enum, 1}, false, ["small", "medium", "large"], <<0>>) == {:ok, "", <<>>}
    end

    test "an out-of-range index fails closed rather than returning nil" do
      assert {:error, {:unsupported_column_type, %{reason: :enum_index_out_of_range}}} =
               Types.cast({:enum, 1}, false, ["small", "medium"], <<5>>)
    end
  end

  describe "cast/4 — SET decodes (C4a) with a fail-closed desync guard" do
    test "a SET bitmap decodes to the selected members, comma-joined (MySQL's text form)" do
      # members a(0) b(1) c(2); bitmap 0b101 = a,c
      assert {:ok, "a,c", <<>>} = Types.cast({:set, 1}, false, ["a", "b", "c"], <<0x05>>)
      assert {:ok, "b", <<>>} = Types.cast({:set, 1}, false, ["a", "b", "c"], <<0x02>>)
      assert {:ok, "", <<>>} = Types.cast({:set, 1}, false, ["a", "b", "c"], <<0x00>>)
      # Trailing columns survive.
      assert {:ok, "a", "X"} = Types.cast({:set, 1}, false, ["a"], <<0x01, "X">>)
    end

    test "a set bit beyond the declared member list is a metadata desync — refused" do
      assert {:error, {:unsupported_column_type, %{reason: :set_member_out_of_range}}} =
               Types.cast({:set, 1}, false, ["a"], <<0x05>>)
    end

    test "a truncated SET bitmap is refused" do
      assert {:error, {:unsupported_column_type, %{reason: :set_truncated}}} =
               Types.cast({:set, 2}, false, ["a"], <<0x01>>)
    end
  end

  describe "cast/4 — GEOMETRY decodes (C4a) as the raw SRID+WKB binary" do
    test "a POINT's stored bytes pass through verbatim, length-prefixed" do
      # The live substrate's HEX(p) for POINT(2 2): SRID 0 + WKB point.
      wkb = <<0::32, 1::8-little, 2.0::float-64-little, 2.0::float-64-little>>

      assert {:ok, ^wkb, <<>>} =
               Types.cast({:geometry, 4}, false, [], <<
                 byte_size(wkb)::32-little,
                 wkb::binary
               >>)
    end
  end
end

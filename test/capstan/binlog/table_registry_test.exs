defmodule Capstan.Binlog.TableRegistryTest do
  use ExUnit.Case, async: true

  alias Capstan.Binlog.{Decoder, Event, TableMap, TableRegistry}

  @fixtures_root Path.join([__DIR__, "..", "..", "fixtures", "binlog"])

  # Decode a captured fixture the same way its consumers do: Event.parse/1 (header +
  # CRC) then Decoder.decode/1. The registry's inputs are the decoded structs/tuples,
  # never bytes — it parses nothing (F12).
  defp decode_fixture(scenario, filename) do
    {:ok, event} =
      [@fixtures_root, scenario, filename]
      |> Path.join()
      |> File.read!()
      |> Event.parse()

    Decoder.decode(event)
  end

  describe "resolution is by the row's OWN table_id (Q3), never last-map-wins" do
    test "a multi-table statement resolves each row event to its own table" do
      # Fixture (b), a single `UPDATE ta JOIN tb`: the server interleaves
      # Table_map(ta) → Table_map(tb) → Update_rows(ta) → Update_rows(tb). Both maps
      # are live at once, so a row must resolve by its OWN table_id — "most recent
      # TABLE_MAP wins" would cast ta's rows against tb's schema (silent wrong values).
      {:ok, %TableMap{table: "ta"} = ta} = decode_fixture("multi_table", "07-table_map.bin")
      {:ok, %TableMap{table: "tb"} = tb} = decode_fixture("multi_table", "08-table_map.bin")

      {:ok, {:update_rows, tid_first, _b1, _a1, _r1}} =
        decode_fixture("multi_table", "09-update_rows.bin")

      {:ok, {:update_rows, tid_second, _b2, _a2, _r2}} =
        decode_fixture("multi_table", "10-update_rows.bin")

      # Guard the premise: the two maps really are distinct tables under distinct ids,
      # and the two row events reference them in the interleaved order.
      assert ta.table_id != tb.table_id
      assert tid_first == ta.table_id
      assert tid_second == tb.table_id

      registry =
        TableRegistry.new()
        |> TableRegistry.put(ta)
        |> TableRegistry.put(tb)

      # The tripwire: the FIRST row event belongs to ta even though tb was mapped last.
      assert {:ok, %TableMap{table: "ta"}} = TableRegistry.resolve(registry, tid_first)
      assert {:ok, %TableMap{table: "tb"}} = TableRegistry.resolve(registry, tid_second)
    end
  end

  describe "a reused table_id (post-ALTER) takes the newest map; the stale binding is gone" do
    test "putting a new map for an existing table_id overwrites the old binding" do
      {:ok, %TableMap{table: "ta"} = ta} = decode_fixture("multi_table", "07-table_map.bin")
      {:ok, %TableMap{table: "tb"} = tb} = decode_fixture("multi_table", "08-table_map.bin")

      registry = TableRegistry.put(TableRegistry.new(), ta)
      assert {:ok, %TableMap{table: "ta"}} = TableRegistry.resolve(registry, ta.table_id)

      # table_id is unstable across DDL and reused (Q3: `ta` moved 92→94 across an
      # ALTER, and a freed id is later handed to another table). MySQL re-emits a
      # TABLE_MAP for the id before its rows, so storing that new map must OVERWRITE —
      # the stale binding must not survive. Re-key tb's real decoded map onto ta's id
      # to stand in for the reuse.
      reused = %TableMap{tb | table_id: ta.table_id}
      registry = TableRegistry.put(registry, reused)

      assert {:ok, %TableMap{table: "tb"}} = TableRegistry.resolve(registry, ta.table_id)
    end
  end

  describe "invalidate/1 drops every binding (owner calls it on ROTATE and FORMAT_DESCRIPTION)" do
    test "a previously-resolvable table_id is unmapped after invalidation" do
      {:ok, ta} = decode_fixture("multi_table", "07-table_map.bin")
      {:ok, tb} = decode_fixture("multi_table", "08-table_map.bin")

      registry =
        TableRegistry.new()
        |> TableRegistry.put(ta)
        |> TableRegistry.put(tb)

      assert {:ok, %TableMap{}} = TableRegistry.resolve(registry, ta.table_id)

      # Across a binlog-file boundary every table_id resets; a retained binding would
      # be stale. The owner (Task 15) invalidates on ROTATE and on FORMAT_DESCRIPTION.
      cleared = TableRegistry.invalidate(registry)

      assert TableRegistry.resolve(cleared, ta.table_id) == {:error, :unmapped_table_id}
      assert TableRegistry.resolve(cleared, tb.table_id) == {:error, :unmapped_table_id}
    end
  end

  describe "an unmapped table_id fails closed (Q3)" do
    test "resolving in an empty registry returns {:error, :unmapped_table_id}" do
      assert TableRegistry.resolve(TableRegistry.new(), 104) == {:error, :unmapped_table_id}
    end

    test "resolving an id that was never bound returns {:error, :unmapped_table_id}" do
      {:ok, ta} = decode_fixture("multi_table", "07-table_map.bin")
      registry = TableRegistry.put(TableRegistry.new(), ta)

      # A populated registry must still fail closed for an id it never stored — it
      # resolves the asked-for id, not "whatever was mapped last".
      assert TableRegistry.resolve(registry, ta.table_id + 1) == {:error, :unmapped_table_id}
    end
  end
end

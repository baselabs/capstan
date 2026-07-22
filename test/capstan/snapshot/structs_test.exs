defmodule Capstan.Snapshot.StructsTest do
  @moduledoc """
  Task 1 (C2) — the additive contract surface: `%Change{op: :snapshot}`, the three
  snapshot structs, and the `Capstan.Sink.handle_snapshot/2` optional callback.

  Rule-1 focus: every value-bearing snapshot struct elides its value fields through
  `@derive {Inspect, only: [...]}` — a crash dump / log that inspects one can never
  surface a row value or a PK cursor (design § Rule 1 / Ch5). Each elision test is
  non-vacuous: a sentinel planted in a value field must be ABSENT from `inspect/1`.
  """
  use ExUnit.Case, async: true

  alias Capstan.Change
  alias Capstan.Snapshot.{Chunk, Meta, State}

  @sentinel "capstan_c2_struct_value_sentinel_9f8e7d"
  @g "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-955"

  describe "%Change{op: :snapshot}" do
    test "inspect shows op/schema/table, never the record value" do
      change = %Change{
        op: :snapshot,
        schema: "orders",
        table: "orders",
        record: %{"secret" => @sentinel},
        old_record: nil
      }

      rendered = inspect(change)

      refute rendered =~ @sentinel
      assert rendered =~ "op: :snapshot"
      assert rendered =~ ~s(table: "orders")
      refute rendered =~ "record:"
    end
  end

  describe "Capstan.Sink.handle_snapshot/2 callback" do
    test "is declared and OPTIONAL" do
      callbacks = Capstan.Sink.behaviour_info(:callbacks)
      optional = Capstan.Sink.behaviour_info(:optional_callbacks)

      assert {:handle_snapshot, 2} in callbacks
      assert {:handle_snapshot, 2} in optional
    end
  end

  describe "%Capstan.Snapshot.Meta{}" do
    test "carries only value-free structural identity" do
      meta = %Meta{schema: "orders", table: "orders", chunk_seq: 3, g: @g, final_chunk?: false}

      assert meta.schema == "orders"
      assert meta.chunk_seq == 3
      assert meta.g == @g
      assert meta.final_chunk? == false
      # Meta has no field that can hold a row value — the whole struct is inspectable.
      refute inspect(meta) =~ @sentinel
    end
  end

  describe "%Capstan.Snapshot.State{}" do
    test "inspect elides the per-table map (pk_cursor is user data, Ch5)" do
      state = %State{
        status: :snapshotting,
        p0: "#{String.slice(@g, 0, 36)}:1-100",
        tables: %{
          {"orders", "orders"} => %{
            fingerprint: "fp-abc",
            pk_columns: ["id"],
            pk_types: [:integer],
            pk_cursor: @sentinel,
            done?: false
          }
        }
      }

      rendered = inspect(state)

      refute rendered =~ @sentinel
      assert rendered =~ "status: :snapshotting"
      refute rendered =~ "tables:"
    end
  end

  describe "%Capstan.Snapshot.Chunk{}" do
    test "inspect elides rows/max_pk (row values), keeps table/seq/g" do
      chunk = %Chunk{
        table: {"orders", "orders"},
        seq: 5,
        g: @g,
        rows: [%{"id" => 1, "secret" => @sentinel}],
        max_pk: @sentinel
      }

      rendered = inspect(chunk)

      refute rendered =~ @sentinel
      assert rendered =~ "seq: 5"
      assert rendered =~ @g
      refute rendered =~ "rows:"
      refute rendered =~ "max_pk:"
    end
  end
end

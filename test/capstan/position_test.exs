defmodule Capstan.PositionTest do
  use ExUnit.Case, async: true

  alias Capstan.Position
  alias Capstan.Transaction

  ## ---------------------------------------------------------------------------
  ## Persistence contract (design Q1/Q12) — gtid_set is the ONLY persisted field
  ## ---------------------------------------------------------------------------

  describe "to_persisted/1" do
    test "returns only the gtid_set string" do
      position = %Position{
        gtid_set: "aaaaaaaa-1111-2222-3333-444444444444:1-11",
        file: "binlog.000042",
        pos: 1745
      }

      assert Position.to_persisted(position) ==
               "aaaaaaaa-1111-2222-3333-444444444444:1-11"
    end
  end

  describe "from_persisted/1" do
    test "builds a Position with nil file/pos" do
      restored = Position.from_persisted("aaaaaaaa-1111-2222-3333-444444444444:1-11")

      assert restored == %Position{
               gtid_set: "aaaaaaaa-1111-2222-3333-444444444444:1-11",
               file: nil,
               pos: nil
             }
    end
  end

  describe "round-trip" do
    test "persisting then restoring a Position with file/pos set drops file/pos, keeps gtid_set" do
      original = %Position{
        gtid_set: "aaaaaaaa-1111-2222-3333-444444444444:1-11",
        file: "binlog.000042",
        pos: 1745
      }

      restored =
        original
        |> Position.to_persisted()
        |> Position.from_persisted()

      assert restored.gtid_set == original.gtid_set
      assert restored.file == nil
      assert restored.pos == nil
    end
  end

  ## ---------------------------------------------------------------------------
  ## Transaction.changes is Enumerable.t(), NOT a list (day-one contract)
  ## ---------------------------------------------------------------------------

  describe "%Transaction{}.changes as Enumerable.t()" do
    test "accepts a lazy Stream (not a list) and enumerates it correctly" do
      lazy_changes = Stream.map(1..3, & &1)

      transaction = %Transaction{
        gtid: "aaaaaaaa-1111-2222-3333-444444444444:12",
        position: %Position{gtid_set: "aaaaaaaa-1111-2222-3333-444444444444:1-12"},
        changes: lazy_changes,
        commit_ts: DateTime.utc_now()
      }

      refute is_list(transaction.changes)
      assert Enum.to_list(transaction.changes) == [1, 2, 3]
    end
  end
end

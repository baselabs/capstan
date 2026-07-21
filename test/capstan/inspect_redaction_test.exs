defmodule Capstan.InspectRedactionTest do
  @moduledoc """
  Rule-1 regression guard (F4): the delivered `Capstan.Change` / `Capstan.Transaction`
  structs must never surface row VALUES through `inspect/1`. Deriving
  `@derive {Inspect, only: [...]}` on each renders only the structural identity and elides
  the value-bearing fields (`record`/`old_record`, `changes`). An accidental removal of the
  derive would re-expose row values in any crash dump / log that inspects one of these —
  this test goes RED if that happens. Always-on (not `:live`), so CI catches the regression.
  """
  use ExUnit.Case, async: true

  alias Capstan.{Change, Position, Transaction}

  @row_sentinel "capstan_inspect_row_value_sentinel_1a2b3c"
  @uuid "3e11fa47-71ca-11e1-9e33-c80aa9429562"

  test "inspect(%Change{}) shows op/schema/table, never record/old_record" do
    change = %Change{
      op: :update,
      schema: "orders",
      table: "line_items",
      record: %{"note" => @row_sentinel},
      old_record: %{"note" => @row_sentinel}
    }

    rendered = inspect(change)

    # Non-vacuity: the default derived inspect would render record/old_record and this
    # sentinel would appear.
    refute rendered =~ @row_sentinel
    assert rendered =~ "op: :update"
    assert rendered =~ ~s(table: "line_items")
    refute rendered =~ "record:"
  end

  test "inspect(%Transaction{}) shows gtid/position/commit_ts, never changes" do
    change = %Change{
      op: :insert,
      schema: "orders",
      table: "line_items",
      record: %{"note" => @row_sentinel}
    }

    txn = %Transaction{
      gtid: "#{@uuid}:7",
      position: %Position{gtid_set: "#{@uuid}:1-7"},
      changes: [change],
      commit_ts: ~U[2026-07-21 00:00:00Z]
    }

    rendered = inspect(txn)

    refute rendered =~ @row_sentinel
    assert rendered =~ "gtid:"
    refute rendered =~ "changes:"
  end

  test "inspect(%Transaction{}) redacts a lazy Stream changes SOURCE too (C3-forward)" do
    # C3 makes `changes` a lazy single-pass Stream. A `%Stream{}` renders STRUCTURALLY —
    # `#Stream<[enum: [<source>], funs: [...]]>` — so if `changes` were not excluded, inspect
    # would surface the Stream's SOURCE (the row values), WITHOUT even running it. The `only:`
    # derive elides `changes` entirely, so the source sentinel never appears. Non-vacuity:
    # remove the derive and the default inspect renders `changes: #Stream<[enum: [<sentinel>]…`.
    txn = %Transaction{
      gtid: "#{@uuid}:1",
      position: %Position{gtid_set: "#{@uuid}:1"},
      changes: Stream.map([@row_sentinel], & &1),
      commit_ts: ~U[2026-07-21 00:00:00Z]
    }

    rendered = inspect(txn)

    refute rendered =~ @row_sentinel
    refute rendered =~ "changes:"
  end
end

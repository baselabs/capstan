defmodule Capstan.Snapshot.CursorGatePropertyTest do
  @moduledoc """
  Property-based gating laws for the cursor-gate — the strict-once classification core.

  The direction is the ADR-0005 inversion: the CHUNK is authoritative for keys above the
  re-read floor (`pk > cursor` — a chunk re-reads and delivers them), so the stream's
  images at or below the cursor are FORWARDED and images above it are SUPPRESSED (the
  chunk delivers them); a DELETE is the crash-window exception, gated on the
  `delivered_pk` high-water instead (a delete the sink already received must be
  forwarded to sweep a phantom). The fixed-payload marquees prove instances of these
  laws; the properties prove them over the whole integer key space, including the
  monotonicity laws (raising a threshold never reverses a decision).
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Capstan.Change
  alias Capstan.Snapshot.CursorGate

  defp table(delivered_pk \\ nil) do
    base = %{pk_columns: ["id"], pk_types: [:bigint], complete?: false}
    if delivered_pk, do: Map.put(base, :delivered_pk, delivered_pk), else: base
  end

  defp insert(k), do: %Change{op: :insert, record: %{"id" => k}, old_record: nil}
  defp delete(k), do: %Change{op: :delete, record: nil, old_record: %{"id" => k}}

  defp forwarded?([_]), do: true
  defp forwarded?([]), do: false

  property "an insert is forwarded iff its key is at or below the re-read floor" do
    check all(cursor <- StreamData.integer(0..500), k <- StreamData.integer(0..500)) do
      assert forwarded?(CursorGate.classify(insert(k), cursor, table())) == k <= cursor
    end
  end

  property "a delete is forwarded iff its key is at or below delivered_pk (crash-window sweep)" do
    check all(
            cursor <- StreamData.integer(0..400),
            delivered_pk <- StreamData.integer(0..500),
            k <- StreamData.integer(0..500)
          ) do
      # delivered_pk sits at or ahead of the rolled-back cursor (the crash-window invariant).
      delivered_pk = max(delivered_pk, cursor)

      assert forwarded?(CursorGate.classify(delete(k), cursor, table(delivered_pk))) ==
               k <= delivered_pk
    end
  end

  property "a :start cursor suppresses every streamed image (nothing is below the floor)" do
    check all(k <- StreamData.integer(0..500)) do
      assert CursorGate.classify(insert(k), :start, table()) == []
      assert CursorGate.classify(delete(k), :start, table()) == []
    end
  end

  property "a complete table forwards everything — the gate is snapshot-mode only" do
    check all(k <- StreamData.integer(0..500)) do
      complete = %{table() | complete?: true}
      assert forwarded?(CursorGate.classify(insert(k), 0, complete))
      assert forwarded?(CursorGate.classify(delete(k), 0, complete))
    end
  end

  property "insert gating is monotone: raising the floor never suppresses a forwarded image" do
    check all(
            c1 <- StreamData.integer(0..400),
            c2 <- StreamData.integer(0..400),
            k <- StreamData.integer(0..500)
          ) do
      c2 = max(c1, c2)

      if forwarded?(CursorGate.classify(insert(k), c1, table())) do
        assert forwarded?(CursorGate.classify(insert(k), c2, table()))
      end
    end
  end

  property "delete gating is monotone in delivered_pk: a swept delete stays swept" do
    check all(
            cursor <- StreamData.integer(0..300),
            d1 <- StreamData.integer(0..400),
            d2 <- StreamData.integer(0..500),
            k <- StreamData.integer(0..500)
          ) do
      d1 = max(cursor, d1)
      d2 = max(d1, d2)

      if forwarded?(CursorGate.classify(delete(k), cursor, table(d1))) do
        assert forwarded?(CursorGate.classify(delete(k), cursor, table(d2)))
      end
    end
  end

  property "a PK-changing update splits into halves gated independently" do
    check all(
            cursor <- StreamData.integer(0..400),
            delivered_pk <- StreamData.integer(0..500),
            k_old <- StreamData.integer(0..500),
            k_new <- StreamData.integer(0..500)
          ) do
      delivered_pk = max(delivered_pk, cursor)

      update = %Change{
        op: :update,
        record: %{"id" => k_new},
        old_record: %{"id" => k_old}
      }

      if k_old != k_new do
        images = CursorGate.classify(update, cursor, table(delivered_pk)) |> Enum.map(& &1.op)

        # delete-half gated on delivered_pk, insert-half on the cursor — each independently.
        assert :delete in images == k_old <= delivered_pk
        assert :insert in images == k_new <= cursor
      else
        # An equal-key update is a single image gated on the cursor, like an insert.
        assert forwarded?(CursorGate.classify(update, cursor, table(delivered_pk))) ==
                 k_new <= cursor
      end
    end
  end
end

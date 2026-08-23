defmodule Capstan.Snapshot.CursorGateTest do
  @moduledoc """
  Task 3 (C2) — `Capstan.Snapshot.CursorGate`, the pure strict-once classification core.

  Every vector here is a pure unit test (no live server, no I/O). They pin the three
  correctness properties the design's exactly-once proof rests on:

    * **Cursor-gate suppression (tripwire 4, strict-once):** a streamed change with
      canonical PK `k ≤ cursor` (already backfilled) or on a `complete?` table is
      FORWARDED; a `k > cursor` change is SUPPRESSED (its future chunk delivers it).
      RED: dropping the suppression double-delivers the key (stream + chunk).
    * **Advance-gate predicate (tripwire 5):** `advance?/2` is
      `Gtid.subset?(Gtid.parse(g), processed_set)` — the cursor may only advance past a
      chunk once the stream's processed watermark covers the chunk's exact `G`. RED:
      advancing before the gate lets a `≤ G` change be BOTH stream-delivered AND in the
      chunk.
    * **PK-changing UPDATE split (tripwire 17, BOTH sub-cases):** an `:update` whose old
      PK `k_old ≠ k_new` splits into `delete(k_old)` + `upsert(k_new)`, EACH gated on its
      own key. (a) `k_old ≤ cursor < k_new` → forward delete, suppress upsert (no
      phantom). (b) `k_new ≤ cursor < k_old` → suppress delete, forward upsert (no gap).
      RED: gating the whole update on a single key (new-only → sub-case-a phantom;
      old-only → sub-case-b gap).
  """
  use ExUnit.Case, async: true

  alias Capstan.Change
  alias Capstan.Gtid
  alias Capstan.Snapshot.CursorGate

  # A single INT PK table (canonical PK is a bare integer).
  @int_pk %{pk_columns: ["id"], pk_types: [:int], complete?: false}
  @int_pk_complete %{@int_pk | complete?: true}

  # A composite (INT, BINARY) PK table (canonical PK is a `{int, binary}` tuple) — the
  # tuple-compare boundary (Ch4 order-faithfulness).
  @composite_pk %{pk_columns: ["a", "b"], pk_types: [:int, :binary], complete?: false}

  # The F1 crash-window shape: `delivered_pk` (20) sits AHEAD of the `cursor` argument
  # (`pk_cursor`) a test passes — the durable state after a crash in the emit→cursor-persist window.
  @int_pk_crash_window %{pk_columns: ["id"], pk_types: [:int], complete?: false, delivered_pk: 20}

  @uuid "3e11fa47-71ca-11e1-9e33-c80aa9429562"

  defp insert(id),
    do: %Change{op: :insert, schema: "s", table: "t", record: %{"id" => id}, old_record: nil}

  defp delete(id),
    do: %Change{op: :delete, schema: "s", table: "t", record: nil, old_record: %{"id" => id}}

  # A PK-changing update: old PK `old_id` -> new PK `new_id`.
  defp pk_update(old_id, new_id) do
    %Change{
      op: :update,
      schema: "s",
      table: "t",
      record: %{"id" => new_id},
      old_record: %{"id" => old_id}
    }
  end

  ## ---------------------------------------------------------------------------
  ## classify/3 — string PK gating (ADR-0012): the gate compares WEIGHT BYTES, not raw
  ## bytes. A string PK table's spec carries `weights` (`%{column => %{raw => weight}}`,
  ## resolved by the coordinator over the shared query connection), and the cursor
  ## argument is the DUAL cursor's `.weight` half. The core case: `'a'` is byte-GREATER
  ## than `'Z'` but collation-LESS — the gate must follow the collation or it silently
  ## double-delivers / gaps exactly the straddling keys (probe Q1b/Q2b).
  ## ---------------------------------------------------------------------------

  describe "classify/3 — string PK gates by weight, not bytes (ADR-0012)" do
    # Live-probed weights under utf8mb4_0900_ai_ci: 'a' -> 1C47, 'Z' -> 1F21, 'zed' -> 1F31.
    @w_a <<0x1C, 0x47>>
    @w_z <<0x1F, 0x21>>
    @w_zed <<0x1F, 0x31>>
    @weights %{"code" => %{"a" => @w_a, "Z" => @w_z, "zed" => @w_zed}}
    @varchar_pk %{pk_columns: ["code"], pk_types: [:varchar], complete?: false, weights: @weights}

    defp s_insert(code),
      do: %Change{
        op: :insert,
        schema: "s",
        table: "t",
        record: %{"code" => code},
        old_record: nil
      }

    defp s_delete(code),
      do: %Change{
        op: :delete,
        schema: "s",
        table: "t",
        record: nil,
        old_record: %{"code" => code}
      }

    test "k with weight < cursor weight forwards though the RAW is byte-greater ('a' vs 'Z')" do
      # Byte order: "a" > "Z" ⇒ a raw-byte gate SUPPRESSES (double-delivery via the future
      # chunk). Collation order: weight('a') < weight('Z') ⇒ already backfilled ⇒ FORWARD.
      change = s_insert("a")
      assert CursorGate.classify(change, @w_z, @varchar_pk) == [change]
    end

    test "k with weight > cursor weight suppresses though the RAW is byte-less ('zed' vs 'Z'... inverted arm)" do
      # "Z" < "a"/"zed" in bytes, but weight('zed') > weight('Z'): a not-yet-backfilled key.
      assert CursorGate.classify(s_insert("zed"), @w_z, @varchar_pk) == []
    end

    test "k == cursor weight forwards (the inclusive boundary, by weight)" do
      change = s_insert("Z")
      assert CursorGate.classify(change, @w_z, @varchar_pk) == [change]
    end

    test "a delete gates on delivered_pk in WEIGHT space (F1 crash-window arm)" do
      spec = Map.put(@varchar_pk, :delivered_pk, @w_z)
      change = s_delete("a")
      assert CursorGate.classify(change, @w_zed, spec) == [change]
      assert CursorGate.classify(s_delete("zed"), @w_zed, spec) == []
    end

    test ":start suppresses WITHOUT weights resolved (the weight-free short-circuit)" do
      # Before the first chunk everything suppresses — no comparison, so the spec carries
      # NO weights map at all (the coordinator resolves nothing while the cursor is :start).
      no_weights = %{pk_columns: ["code"], pk_types: [:varchar], complete?: false}
      assert CursorGate.classify(s_insert("a"), :start, no_weights) == []
    end

    test "a complete? table forwards without weights (the stream is authoritative)" do
      no_weights = %{pk_columns: ["code"], pk_types: [:varchar], complete?: true}
      change = s_insert("a")
      assert CursorGate.classify(change, :start, no_weights) == [change]
    end

    test "a missing weight fails LOUD and VALUE-FREE (Rule 1 — the raw never rides the error)" do
      no_weights = %{pk_columns: ["code"], pk_types: [:varchar], complete?: false}

      assert_raise ArgumentError, ~r/weight/i, fn ->
        CursorGate.classify(s_insert("a"), @w_z, no_weights)
      end
    end

    test "a composite (INT, VARCHAR) gates by {int, weight} tuple order (probe Q7)" do
      spec = %{
        pk_columns: ["i", "k"],
        pk_types: [:int, :varchar],
        complete?: false,
        weights: %{"k" => @weights["code"]}
      }

      change = %Change{
        op: :insert,
        schema: "s",
        table: "t",
        record: %{"i" => 1, "k" => "a"},
        old_record: nil
      }

      # cursor weight {1, w_z}: {1, w_a} < {1, w_z} ⇒ forward; {2, w_a} > ⇒ suppress.
      assert CursorGate.classify(change, {1, @w_z}, spec) == [change]

      change2 = %Change{change | record: %{"i" => 2, "k" => "a"}}
      assert CursorGate.classify(change2, {1, @w_z}, spec) == []
    end

    test "a PK-changing string UPDATE splits and gates each half by weight (tripwire 17 arm)" do
      change = %Change{
        op: :update,
        schema: "s",
        table: "t",
        record: %{"code" => "zed"},
        old_record: %{"code" => "Z"}
      }

      # k_old ('Z') ≤ cursor weight, k_new ('zed') > ⇒ forward the delete, suppress the upsert.
      [forwarded] = CursorGate.classify(change, @w_z, @varchar_pk)
      assert forwarded.op == :delete
      assert forwarded.old_record == %{"code" => "Z"}
    end
  end

  ## ---------------------------------------------------------------------------
  ## classify/3 — single-key gating (tripwire 4, strict-once)
  ## ---------------------------------------------------------------------------

  describe "classify/3 — single-key gating (tripwire 4)" do
    test "k < cursor forwards (already backfilled)" do
      change = insert(5)
      assert CursorGate.classify(change, 10, @int_pk) == [change]
    end

    test "k == cursor forwards (the inclusive boundary)" do
      change = insert(10)
      assert CursorGate.classify(change, 10, @int_pk) == [change]
    end

    test "k > cursor suppresses (its future chunk delivers it — no double-delivery)" do
      # RED anchor: forwarding a `k > cursor` change double-delivers it (stream + chunk).
      assert CursorGate.classify(insert(15), 10, @int_pk) == []
    end

    test ":start cursor suppresses everything while the table is not complete" do
      # Nothing is backfilled yet (`:start`), so every key is `> cursor`.
      assert CursorGate.classify(insert(1), :start, @int_pk) == []
      assert CursorGate.classify(insert(999), :start, @int_pk) == []
    end

    test "a complete table forwards every change, even k > cursor and cursor :start" do
      big = insert(10_000)
      assert CursorGate.classify(big, 10, @int_pk_complete) == [big]
      assert CursorGate.classify(big, :start, @int_pk_complete) == [big]
    end

    test "a delete is gated on its old_record key" do
      d = delete(5)
      assert CursorGate.classify(d, 10, @int_pk) == [d]
      assert CursorGate.classify(delete(15), 10, @int_pk) == []
    end
  end

  ## ---------------------------------------------------------------------------
  ## classify/3 — deletes gate on delivered_pk (the crash-window backstop, F1)
  ## ---------------------------------------------------------------------------

  describe "classify/3 — deletes gate on delivered_pk, not pk_cursor (F1)" do
    test "a delete of an already-delivered key (pk_cursor < k <= delivered_pk) FORWARDS" do
      # k = 15: > pk_cursor (10) yet <= delivered_pk (20). RED (delete gated on pk_cursor): 15 > 10
      # -> suppressed -> the crash-window phantom the two-marker exists to sweep.
      d = delete(15)
      assert CursorGate.classify(d, 10, @int_pk_crash_window) == [d]
    end

    test "a delete beyond delivered_pk (k > delivered_pk) still SUPPRESSES (exact threshold)" do
      # k = 25: > delivered_pk (20) — never delivered, so its future chunk delivers it. The delete
      # threshold is EXACTLY delivered_pk, not an indiscriminate forward-all-deletes.
      assert CursorGate.classify(delete(25), 10, @int_pk_crash_window) == []
    end

    test "an INSERT still gates on pk_cursor even when delivered_pk is ahead (strict-once preserved)" do
      # k = 15: <= delivered_pk (20) but > pk_cursor (10). Only DELETES use the wider delivered_pk
      # threshold — an insert must NOT forward (its chunk delivers it), or strict-once breaks.
      assert CursorGate.classify(insert(15), 10, @int_pk_crash_window) == []
    end

    test "a PK-changing update: the delete-half uses delivered_pk, the upsert-half uses pk_cursor" do
      # pk_update(15 -> 25), pk_cursor 10, delivered_pk 20: delete(15) forwards (15 <= 20), upsert(25)
      # suppresses (25 > 10) — only the delete-half survives.
      assert CursorGate.classify(pk_update(15, 25), 10, @int_pk_crash_window) == [
               %Change{
                 op: :delete,
                 schema: "s",
                 table: "t",
                 record: nil,
                 old_record: %{"id" => 15}
               }
             ]
    end
  end

  ## ---------------------------------------------------------------------------
  ## advance?/2 — the advance-gate predicate (tripwire 5)
  ## ---------------------------------------------------------------------------

  describe "advance?/2 — the advance-gate predicate (tripwire 5)" do
    test "true when G ⊆ processed_set (a string is parsed)" do
      assert CursorGate.advance?("#{@uuid}:1-5", "#{@uuid}:1-10")
    end

    test "true at the exact boundary G == processed_set" do
      assert CursorGate.advance?("#{@uuid}:1-10", "#{@uuid}:1-10")
    end

    test "false when G ⊄ processed_set (the watermark has not yet covered G)" do
      # RED anchor: treating this as advanceable lets a `≤ G` change be delivered by BOTH
      # the stream and the chunk.
      refute CursorGate.advance?("#{@uuid}:1-11", "#{@uuid}:1-10")
    end

    test "false when the source UUID is not present in the processed set at all" do
      other = "00000000-0000-0000-0000-000000000000"
      refute CursorGate.advance?("#{@uuid}:1-5", "#{other}:1-10")
    end

    test "accepts an already-parsed Gtid.t() as the processed_set" do
      processed = Gtid.parse("#{@uuid}:1-10")
      assert CursorGate.advance?("#{@uuid}:1-5", processed)
      refute CursorGate.advance?("#{@uuid}:1-11", processed)
    end
  end

  ## ---------------------------------------------------------------------------
  ## classify/3 — PK-changing UPDATE split (tripwire 17, both sub-cases)
  ## ---------------------------------------------------------------------------

  describe "classify/3 — PK-changing UPDATE split (tripwire 17)" do
    test "sub-case (a): k_old <= cursor < k_new -> forward delete(k_old), suppress upsert(k_new)" do
      # Row leaves the backfilled region: delete the old PK, let k_new's future chunk
      # deliver it. RED (new-only gating): the delete is dropped -> a phantom survives.
      result = CursorGate.classify(pk_update(5, 15), 10, @int_pk)

      assert result == [
               %Change{
                 op: :delete,
                 schema: "s",
                 table: "t",
                 record: nil,
                 old_record: %{"id" => 5}
               }
             ]
    end

    test "sub-case (b): k_new <= cursor < k_old -> suppress delete(k_old), forward upsert(k_new)" do
      # Row enters the backfilled region: k_old's chunk never ran (nothing to delete),
      # so forward only the upsert of k_new. RED (old-only gating): the upsert is dropped
      # -> a gap at k_new.
      result = CursorGate.classify(pk_update(15, 5), 10, @int_pk)

      assert result == [
               %Change{
                 op: :insert,
                 schema: "s",
                 table: "t",
                 record: %{"id" => 5},
                 old_record: nil
               }
             ]
    end

    test "both keys <= cursor -> both images forward (delete then upsert)" do
      result = CursorGate.classify(pk_update(3, 7), 10, @int_pk)

      assert [
               %Change{op: :delete, record: nil, old_record: %{"id" => 3}},
               %Change{op: :insert, record: %{"id" => 7}, old_record: nil}
             ] = result
    end

    test "both keys > cursor -> both images suppressed" do
      assert CursorGate.classify(pk_update(20, 25), 10, @int_pk) == []
    end

    test "a non-PK-mutating update is ONE image, gated once, unchanged" do
      # k_old == k_new (only non-PK columns changed): the change is NOT split.
      change = %Change{
        op: :update,
        schema: "s",
        table: "t",
        record: %{"id" => 8, "v" => 2},
        old_record: %{"id" => 8, "v" => 1}
      }

      assert CursorGate.classify(change, 10, @int_pk) == [change]
      assert CursorGate.classify(change, 5, @int_pk) == []
    end
  end

  ## ---------------------------------------------------------------------------
  ## classify/3 — composite PK + tuple-compare boundary (Ch4)
  ## ---------------------------------------------------------------------------

  describe "classify/3 — composite PK tuple-compare boundary" do
    defp comp_insert(a, b) do
      %Change{
        op: :insert,
        schema: "s",
        table: "t",
        record: %{"a" => a, "b" => b},
        old_record: nil
      }
    end

    test "k == cursor tuple forwards (the inclusive boundary)" do
      change = comp_insert(5, "m")
      assert CursorGate.classify(change, {5, "m"}, @composite_pk) == [change]
    end

    test "second component decides when the first ties (b > cursor.b suppresses)" do
      assert CursorGate.classify(comp_insert(5, "n"), {5, "m"}, @composite_pk) == []
      change = comp_insert(5, "l")
      assert CursorGate.classify(change, {5, "m"}, @composite_pk) == [change]
    end

    test "the first component dominates the tuple order" do
      lower = comp_insert(4, "z")
      assert CursorGate.classify(lower, {5, "m"}, @composite_pk) == [lower]
      assert CursorGate.classify(comp_insert(6, "a"), {5, "m"}, @composite_pk) == []
    end
  end

  ## ---------------------------------------------------------------------------
  ## classify/3 — a :snapshot op is misuse and fails loud VALUE-FREE (Rule 1)
  ## ---------------------------------------------------------------------------

  describe "classify/3 — :snapshot op fails loud without leaking the cursor (Rule 1)" do
    # A canonical PK cursor is USER DATA. A FunctionClauseError (missing clause) would capture
    # the call args — the cursor — in its stacktrace frame; the explicit clause raises a
    # value-free ArgumentError with an arity-only frame. Non-vacuity: the cursor is a distinctive
    # sentinel, asserted absent from BOTH the message and the formatted stacktrace.
    @cursor_sentinel 987_654_321

    test "raises ArgumentError, and neither the message nor the stacktrace carries the cursor" do
      snap = %Change{
        op: :snapshot,
        schema: "s",
        table: "t",
        record: %{"id" => 1},
        old_record: nil
      }

      {exception, stacktrace} =
        try do
          CursorGate.classify(snap, @cursor_sentinel, @int_pk)
          flunk("expected classify/3 to raise on a :snapshot op")
        rescue
          e -> {e, __STACKTRACE__}
        end

      assert %ArgumentError{} = exception
      formatted = Exception.format(:error, exception, stacktrace)
      refute formatted =~ Integer.to_string(@cursor_sentinel)
      # The top frame is the arity-3 clause, never the FunctionClauseError arg list.
      refute formatted =~ "#{@cursor_sentinel}"
    end
  end
end

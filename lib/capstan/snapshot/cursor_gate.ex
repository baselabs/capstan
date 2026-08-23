defmodule Capstan.Snapshot.CursorGate do
  @moduledoc """
  The pure strict-once classification core of the initial snapshot (C2, design §Architecture).

  While a table backfills, the C1 stream keeps running. Every streamed `%Change{}` on a
  snapshot-active table must be routed so each pre-existing key is delivered EXACTLY ONCE —
  once by the stream (for keys already backfilled) or once by a chunk (for keys not yet
  backfilled), never both, never neither. This module is the pure decision function that
  routes them; it holds no state and does no I/O.

  ## The gate (`classify/3`)

  Given a streamed change, the table's per-table `cursor` (the canonical PK of the last
  backfilled row, or `:start` before the first chunk), and the table's `pk_columns` /
  `pk_types` / `complete?`, each row-image is classified per its canonical PK `k`:

    * **forward** iff `k ≤ cursor` (already backfilled) OR the table is `complete?` — the
      stream is authoritative for these keys;
    * **suppress** otherwise (`k > cursor`) — a not-yet-backfilled key whose future chunk
      will deliver it. Forwarding it here would double-deliver (stream + chunk).

  A `:delete` is the ONE asymmetric case: it gates on `delivered_pk` (the high-water the sink has
  RECEIVED) rather than `pk_cursor` (the re-read floor). The two are equal in steady state, so
  suppression is unchanged; they diverge only across the emit→cursor-persist crash window, where
  `delivered_pk` sits ahead of the rolled-back `pk_cursor`. Forwarding a delete of an
  already-delivered key (`k ≤ delivered_pk`) then sweeps the row rather than leaving a permanent
  phantom — the re-read chunk, taken as-of a fresh `G` with the row already gone,
  would otherwise omit it while the suppressed delete never reached the sink. See the `table_spec`
  doc; inserts/updates keep gating on `pk_cursor` so strict-once is preserved.

  `k` is derived from the change's record via `Capstan.Snapshot.PrimaryKey.canonical/2`
  (the raw column values in PK ordinal order) and compared with `PrimaryKey.compare/2` — the
  only ordering the cursor-gate is allowed to use, restricted to order-faithful PK types so
  the Elixir comparison provably matches MySQL `ORDER BY` (Ch4).

  `classify/3` returns the **list of surviving forward-images** (0, 1, or 2), which the
  coordinator folds into the transaction's `changes` and forwards to the real sink.
  A suppressed change yields `[]`.

  ## PK-changing UPDATE split (Ch2, tripwire 17)

  A binlog `%Change{op: :update}` that moves a row's PK carries `old_record` (old PK
  `k_old`) and `record` (new PK `k_new`) with `k_old ≠ k_new`. Gating such an update on a
  single key is wrong when the two straddle the cursor, so it is **split into
  `delete(k_old)` + `upsert(k_new)`, each gated on its own key** — the canonical
  DELETE(old) + INSERT(new) decomposition of a PK move. Both images carry the FULL
  after/before-image (ADR-0002), so each is self-sufficient:

    * `delete(k_old)` — `%Change{op: :delete, old_record: old_record, record: nil}`.
    * `upsert(k_new)` — `%Change{op: :insert, record: record, old_record: nil}`. It is an
      insert because a PK-changing UPDATE makes `k_new` a brand-new key; the sink applies it
      as **upsert-by-PK** (the HARD C2 sink precondition), so it converges even if `k_new`'s
      slot was already backfilled.

  Straddle (a) `k_old ≤ cursor < k_new` → forward the delete, suppress the upsert (its
  future chunk delivers `k_new`) → no phantom. Straddle (b) `k_new ≤ cursor < k_old` →
  suppress the delete (`k_old`'s chunk never ran, nothing was emitted there), forward the
  upsert → no gap. A non-PK-mutating update (`k_old = k_new`) is ONE image, unchanged.

  ## The advance-gate predicate (`advance?/2`)

  A buffered chunk read as-of an exact GTID position `G` may only be emitted (and its cursor
  advanced) once the stream's processed watermark covers `G`:
  `Gtid.subset?(Gtid.parse(g), processed_set)`. This is the ordering that makes suppression
  correct — the cursor cannot advance to include a key until every `gtid ≤ G` has been
  processed by the stream. All GTID-set math routes through `Capstan.Gtid` (ADR-0001); no
  hand-rolled interval arithmetic lives here.

  ## Rule 1

  Pure functions, no I/O: nothing here logs or telemeters a PK value. Canonical PKs and row
  values live only in the returned `%Change{}` images (whose `Inspect` already elides the
  value maps) and in the caller's in-memory state.
  """

  alias Capstan.Change
  alias Capstan.Gtid
  alias Capstan.Snapshot.PrimaryKey

  @typedoc "The per-table backfill cursor: a canonical PK, or `:start` before the first chunk."
  @type cursor :: PrimaryKey.canonical_pk() | :start

  @typedoc """
  The per-table gate context: the introspected PK shape (`pk_columns` in ordinal order and
  their order-faithful `pk_types`), whether the table's backfill is `complete?`, the
  optional `delivered_pk` — the high-water the sink has RECEIVED, used as the DELETE threshold
  (see the module doc's crash-window section) — and the optional `weights` —
  `%{column_name => %{raw => weight_bytes}}` for the STRING pk columns (ADR-0012), resolved
  by the coordinator over the shared query connection before gating. When `delivered_pk` is
  absent, deletes gate on the `cursor` argument (pre-F1 behaviour). Extra keys are ignored, so
  the coordinator may pass a richer per-table state map.
  """
  @type table_spec :: %{
          required(:pk_columns) => [String.t()],
          required(:pk_types) => [PrimaryKey.pk_type()],
          required(:complete?) => boolean(),
          optional(:delivered_pk) => cursor(),
          optional(:weights) => %{optional(String.t()) => %{optional(binary()) => binary()}},
          optional(atom()) => term()
        }

  @doc """
  Classifies one streamed `%Change{}` against the table's `cursor` and `table_spec`,
  returning the list of surviving forward-images (`[]`, one, or — for a straddling
  PK-changing UPDATE that forwards both halves — two).

  An `:insert` is gated on its `record` key, a `:delete` on its `old_record` key, and an
  `:update` on its (equal) key unless it moves the PK, in which case it is split into
  `delete(k_old)` + `upsert(k_new)` and each half is gated on its own key (see the module
  doc). A `:snapshot` change never reaches the gate (the gate classifies streamed images
  only) and raises a value-free `ArgumentError` — a loud, fail-closed misuse signal. It is an
  EXPLICIT clause rather than a `FunctionClauseError` on purpose (Rule 1): a no-clause-match
  error captures the call args — including the `cursor`, a canonical PK / user data — in its
  stacktrace frame, whereas the explicit clause leaves the frame at arity 3 (no arg values).
  """
  @spec classify(Change.t(), cursor(), table_spec()) :: [Change.t()]
  def classify(%Change{op: :update, record: new_rec, old_record: old_rec} = change, cursor, table) do
    k_old = key(old_rec, table)
    k_new = key(new_rec, table)

    case PrimaryKey.compare(k_old, k_new) do
      :eq ->
        gate(change, k_new, cursor, table)

      _pk_changed ->
        delete_image = %Change{change | op: :delete, record: nil, old_record: old_rec}
        upsert_image = %Change{change | op: :insert, record: new_rec, old_record: nil}
        gate(delete_image, k_old, cursor, table) ++ gate(upsert_image, k_new, cursor, table)
    end
  end

  # The single-key ops take the WEIGHT-FREE short-circuits BEFORE canonicalizing (ADR-0012):
  # a complete table forwards and a `:start` cursor suppresses without ever consulting `k`,
  # so the coordinator resolves no weights for them (the bootstrap window and completed
  # tables stay round-trip-free). The `:update` arm canonicalizes unconditionally — the
  # PK-split's `k_old ≠ k_new` compare is a collation compare for string keys.
  def classify(%Change{op: :insert, record: rec} = change, cursor, table) do
    single_key(change, rec, cursor, table)
  end

  def classify(%Change{op: :delete, old_record: old_rec} = change, cursor, table) do
    single_key(change, old_rec, cursor, table)
  end

  # A :snapshot image never reaches the gate — it classifies STREAMED images only. Fail loud,
  # but VALUE-FREE (Rule 1): an explicit clause raising here keeps the stacktrace frame at
  # arity 3, so the `cursor` PK is never captured into a crash report — unlike the
  # FunctionClauseError a missing clause would raise, whose frame carries the call args.
  def classify(%Change{op: :snapshot}, _cursor, _table) do
    raise ArgumentError,
          "Capstan.Snapshot.CursorGate.classify/3 received an op: :snapshot image; " <>
            "the gate classifies streamed :insert/:update/:delete images only"
  end

  # The single-key weight-free short-circuits key on the APPLICABLE threshold: a `:start`
  # cursor suppresses inserts, but a DELETE in the crash window (`delivered_pk` ahead of the
  # rolled-back `pk_cursor`) still gates on `delivered_pk` — a `:start` short-circuit there
  # would strand the phantom the F1 backstop exists to sweep.
  defp single_key(change, rec, cursor, table) do
    cond do
      table.complete? -> [change]
      threshold(change, cursor, table) == :start -> []
      true -> gate(change, key(rec, table), cursor, table)
    end
  end

  defp threshold(%Change{op: :delete}, cursor, table), do: Map.get(table, :delivered_pk, cursor)
  defp threshold(_change, cursor, _table), do: cursor

  @doc """
  The advance-gate predicate: is the chunk's exact GTID position `g` covered by
  `processed_set`? True iff `Gtid.subset?(Gtid.parse(g), processed_set)`.

  `g` is always a canonical GTID-set string. `processed_set` may be either an already-parsed
  `Capstan.Gtid.t()` (the watermark-observer feed) or a string (parsed here). No interval
  math is hand-rolled — the containment decision is `Capstan.Gtid`'s.
  """
  @spec advance?(String.t(), Gtid.t() | String.t()) :: boolean()
  def advance?(g, processed_set) when is_binary(g) do
    processed = if is_binary(processed_set), do: Gtid.parse(processed_set), else: processed_set
    Gtid.subset?(Gtid.parse(g), processed)
  end

  ## ---------------------------------------------------------------------------
  ## internals
  ## ---------------------------------------------------------------------------

  # Forward the image (as a singleton) iff its key is at or below the APPLICABLE threshold, or the
  # table is complete; else suppress (empty list). A `:delete` gates on `delivered_pk` (the
  # high-water the sink has RECEIVED); every other op gates on `pk_cursor` (the `cursor` arg, the
  # re-read floor). The two are equal in steady state and diverge only across the
  # emit→cursor-persist crash window, where `delivered_pk` sits AHEAD of the rolled-back
  # `pk_cursor`: a streamed delete of an already-delivered key (`k ≤ delivered_pk`) is then
  # FORWARDED to sweep the row, instead of being suppressed and leaving a permanent phantom
  # Gating inserts/updates on `pk_cursor` preserves strict-once (the chunk already
  # delivered `≤ pk_cursor`); a delete of a not-yet-delivered key in `(pk_cursor, delivered_pk]`
  # is at worst a harmless no-op at the upsert/delete-by-PK sink.
  defp gate(%Change{op: :delete} = change, k, cursor, table) do
    gate_forward(change, k, delivered_pk(table, cursor), table.complete?)
  end

  defp gate(change, k, cursor, table) do
    gate_forward(change, k, cursor, table.complete?)
  end

  defp gate_forward(change, k, threshold, complete?) do
    if forward?(k, threshold, complete?), do: [change], else: []
  end

  # The delete threshold: `delivered_pk` when the caller supplies it (the coordinator always does);
  # else fall back to `cursor` — a gate context WITHOUT the field behaves exactly as pre-F1 (delete
  # gated on the same cursor as every other op).
  defp delivered_pk(table, cursor), do: Map.get(table, :delivered_pk, cursor)

  defp forward?(_k, _cursor, true), do: true
  defp forward?(_k, :start, false), do: false
  defp forward?(k, cursor, false), do: PrimaryKey.compare(k, cursor) != :gt

  # The change's canonical PK: the raw PK column values in ordinal order, canonicalized so a
  # text-form chunk read and a binlog-decoded streamed change to the same key compare equal.
  # A STRING pk column contributes the {raw, weight} pair — the weight from the spec-carried
  # `weights` map (`%{column => %{raw => weight}}`, resolved by the coordinator over the
  # shared query connection — ADR-0012); canonical/2 then yields the weight bytes the gate
  # compares. A missing weight is a caller bug (the coordinator resolves every string pk raw
  # before gating): `canonical/2` raises LOUD and VALUE-FREE rather than silently comparing
  # raws — a raw-byte compare is exactly the collation divergence C2a retires.
  defp key(record, table) do
    weights = Map.get(table, :weights, %{})

    raw =
      table.pk_columns
      |> Enum.zip(table.pk_types)
      |> Enum.map(fn
        {col, type} when type in [:char, :varchar] ->
          value = record[col]
          {value, fetch_weight(table, weights, col, value)}

        {col, _type} ->
          record[col]
      end)

    PrimaryKey.canonical(table.pk_types, raw)
  end

  defp fetch_weight(table, weights, col, value) do
    case weights do
      %{^col => %{^value => weight}} -> weight
      _ -> raise_unresolved_weight(table)
    end
  end

  # Value-free by construction: the message names the mechanism, never the raw value.
  defp raise_unresolved_weight(_table) do
    raise ArgumentError,
          "Capstan.Snapshot.CursorGate classified a string primary-key change without a " <>
            "resolved collation weight (the coordinator must resolve weights before gating)"
  end
end

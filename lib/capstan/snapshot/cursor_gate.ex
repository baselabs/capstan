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

  `k` is derived from the change's record via `Capstan.Snapshot.PrimaryKey.canonical/2`
  (the raw column values in PK ordinal order) and compared with `PrimaryKey.compare/2` — the
  only ordering the cursor-gate is allowed to use, restricted to order-faithful PK types so
  the Elixir comparison provably matches MySQL `ORDER BY` (Ch4).

  `classify/3` returns the **list of surviving forward-images** (0, 1, or 2), which the
  coordinator (Task 8) folds into the transaction's `changes` and forwards to the real sink.
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
  their order-faithful `pk_types`) plus whether the table's backfill is `complete?`. Extra
  keys are ignored, so the coordinator may pass a richer per-table state map.
  """
  @type table_spec :: %{
          required(:pk_columns) => [String.t()],
          required(:pk_types) => [PrimaryKey.pk_type()],
          required(:complete?) => boolean(),
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
  only) and raises `FunctionClauseError` — a loud, fail-closed misuse signal.
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

  def classify(%Change{op: :insert, record: rec} = change, cursor, table) do
    gate(change, key(rec, table), cursor, table)
  end

  def classify(%Change{op: :delete, old_record: old_rec} = change, cursor, table) do
    gate(change, key(old_rec, table), cursor, table)
  end

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

  # Forward the image (as a singleton) iff its key is at or below the cursor, or the table is
  # complete; else suppress (empty list).
  defp gate(change, k, cursor, table) do
    if forward?(k, cursor, table.complete?), do: [change], else: []
  end

  defp forward?(_k, _cursor, true), do: true
  defp forward?(_k, :start, false), do: false
  defp forward?(k, cursor, false), do: PrimaryKey.compare(k, cursor) != :gt

  # The change's canonical PK: the raw PK column values in ordinal order, canonicalized so a
  # text-form chunk read and a binlog-decoded streamed change to the same key compare equal.
  defp key(record, table) do
    raw = Enum.map(table.pk_columns, &record[&1])
    PrimaryKey.canonical(table.pk_types, raw)
  end
end

defmodule Capstan.Binlog.TableRegistry do
  @moduledoc """
  The `table_id` → `%Capstan.Binlog.TableMap{}` registry — the Q3 schema-resolution core.

  A row event names its table only by a numeric `table_id`. This registry is the one
  place that id is resolved to a decoded `%TableMap{}`, and resolution is **by the row
  event's OWN `table_id`** — never by "the most recent `TABLE_MAP`". A single
  multi-table statement interleaves `TABLE_MAP(ta) → TABLE_MAP(tb) → ROWS(ta) →
  ROWS(tb)` (design Q3, live-verified), so both maps are live at once; "last map wins"
  would cast `ta`'s rows against `tb`'s schema and emit silently wrong values.

  ## Storage and resolution only (F12)

  This module keys a plain map by `table_id` and hands back the stored struct
  verbatim. It parses **no bytes** — `Capstan.Binlog.Decoder` already owns the entire
  `TABLE_MAP` body, including the optional-metadata TLVs, and produces the
  `%TableMap{}` structs this registry stores. Adding any parsing here would cross that
  Task 9/10 boundary.

  ## Invalidation (Q3)

  `table_id` is unstable: it moves across DDL (`ta` observed moving `92 → 94` across an
  `ALTER`) and a freed id is later reused for a different table. Within a binlog file
  the server re-emits a `TABLE_MAP` for a `table_id` before the row events that use it,
  so `put/2` overwriting a reused id keeps the binding current. Across a file boundary
  the ids reset wholesale, so the **owner** (`AssemblerServer`, Task 15) calls
  `invalidate/1` on `ROTATE` and `FORMAT_DESCRIPTION` to drop every binding — a
  `table_id` carried over from the previous file must never resolve to a stale map.

  ## Unmapped ids fail closed

  A `table_id` with no live binding resolves to `{:error, :unmapped_table_id}` — never
  a guess and never the "nearest" map. The owner turns this into a halt (design Q3);
  silently dropping the row would be exactly the loss C1 refuses.
  """

  alias Capstan.Binlog.TableMap

  @typedoc """
  The registry: a map from `table_id` to its currently-bound `%TableMap{}`. Build and
  inspect it only through this module — treat it as opaque.
  """
  @opaque t :: %{optional(non_neg_integer()) => TableMap.t()}

  @doc "An empty registry with no table bindings."
  @spec new() :: t()
  def new, do: %{}

  @doc """
  Binds `table_map` under its own `table_id`, overwriting any prior binding for that
  id. A `table_id` reused after DDL therefore takes the newest map (see the moduledoc).
  """
  @spec put(t(), TableMap.t()) :: t()
  def put(registry, %TableMap{table_id: table_id} = table_map) do
    Map.put(registry, table_id, table_map)
  end

  @doc """
  Resolves a row event's `table_id` to its bound `%TableMap{}`, or
  `{:error, :unmapped_table_id}` when nothing is bound (fail closed — design Q3).
  """
  @spec resolve(t(), non_neg_integer()) :: {:ok, TableMap.t()} | {:error, :unmapped_table_id}
  def resolve(registry, table_id) do
    case Map.fetch(registry, table_id) do
      {:ok, table_map} -> {:ok, table_map}
      :error -> {:error, :unmapped_table_id}
    end
  end

  @doc """
  Drops every binding, returning an empty registry. The owner calls this on `ROTATE`
  and `FORMAT_DESCRIPTION`, where `table_id`s reset and any retained binding would be
  stale (design Q3).
  """
  @spec invalidate(t()) :: t()
  def invalidate(_registry), do: new()
end

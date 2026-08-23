defmodule Capstan.Gtid do
  @moduledoc """
  MySQL GTID-set algebra — the dedup correctness core.

  A GTID set is the sole authority for replication position (ADR-0001): dedup is
  set **membership**, never an ordinal comparison. This module is the one place
  GTID-set membership and interval math live, so no consumer — the connection's gap
  gate, the assembler, or a sink — ever hand-rolls them.

  ## String format (MySQL canonical, INCLUSIVE bounds)

  A comma-separated list of per-UUID entries; each entry is
  `<uuid>:<interval>[:<interval>...]`; an interval is a single number `N` or an
  inclusive range `N-M` with `M >= N`. `""` is the empty set. A UUID is the 36-char
  hyphenated hex form; GNOs are integers `>= 1`. Ranges are inclusive: `uuid:1-11`
  is GTIDs 1..11 and `uuid:7` is `uuid:7-7`. Multi-source sets are supported —
  several UUID entries in one string.

  `render/1` produces the canonical form: UUIDs lower-cased and ordered
  lexicographically, intervals within a UUID sorted ascending and coalesced (merged
  where overlapping or adjacent), single-element intervals rendered as `N`. Parsing
  normalises to this form, so `parse |> render` is idempotent and, for any already
  canonical string `s`, `s |> parse |> render == s`.

  ## Malformed input

  `parse/1` **raises `ArgumentError`** on any string outside the grammar above —
  a bad UUID, a GNO `< 1` or non-numeric, a descending range, an empty entry. A
  dedup core must never silently coerce a malformed position into a plausible-but-
  wrong set, so parsing fails closed rather than guessing.

  ## Single GTID shape

  `member?/2` takes a set and a single GTID as `{uuid :: String.t(), gno ::
  pos_integer()}`. The UUID is matched case-insensitively.

  ## Scope

  The bounds here are INCLUSIVE and stay inclusive. The `COM_BINLOG_DUMP_GTID` wire
  encoding uses an EXCLUSIVE end (`uuid:1-11` -> wire end 12); that conversion belongs to
  the dump-side encoder (`Capstan.Protocol.Command`) and is deliberately not baked into this module.
  """

  @typedoc "A source-server UUID in canonical lower-case 36-char hyphenated form."
  @type uuid :: String.t()

  @typedoc "A GTID transaction sequence number; always `>= 1`."
  @type gno :: pos_integer()

  @typedoc "A single GTID: one transaction from one source."
  @type gtid :: {uuid(), gno()}

  @typep interval :: {gno(), gno()}

  @typedoc """
  A GTID set. Internally a map from UUID to a sorted, coalesced list of inclusive
  `{low, high}` intervals; a UUID with no live intervals is absent (never `[]`). The
  representation is canonical by construction — treat it as opaque and build/inspect
  it only through this module's functions.
  """
  @opaque t :: %{optional(uuid()) => [interval(), ...]}

  @uuid_format ~r/\A[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\z/

  @doc """
  Parses a canonical GTID-set string into a set. Raises `ArgumentError` on malformed
  input. `parse("")` is the empty set.
  """
  @spec parse(String.t()) :: t()
  def parse(string) when is_binary(string) do
    string
    |> split_entries()
    |> Enum.reduce(%{}, fn entry, acc ->
      {uuid, intervals} = parse_entry(entry)
      Map.update(acc, uuid, intervals, &(intervals ++ &1))
    end)
    |> normalize_set()
  end

  @doc """
  Renders a set to its canonical GTID-set string. The empty set renders as `""`.
  """
  @spec render(t()) :: String.t()
  def render(set) do
    set
    |> Enum.sort()
    |> Enum.map_join(",", fn {uuid, intervals} ->
      uuid <> ":" <> Enum.map_join(intervals, ":", &render_interval/1)
    end)
  end

  @doc """
  Is one GTID a member of the set? The dedup idiom `txn.gtid ∈ checkpoint.gtid_set`.
  """
  @spec member?(t(), gtid()) :: boolean()
  def member?(set, {uuid, gno}) when is_integer(gno) do
    set
    |> Map.get(String.downcase(uuid), [])
    |> Enum.any?(fn {low, high} -> low <= gno and gno <= high end)
  end

  @doc """
  Set union with adjacent-range coalescing. `union("uuid:1-3","uuid:4-6")` renders
  `"uuid:1-6"`. Merges per-UUID.
  """
  @spec union(t(), t()) :: t()
  def union(a, b) do
    a
    |> Map.merge(b, fn _uuid, ia, ib -> ia ++ ib end)
    |> normalize_set()
  end

  @doc """
  Set difference `a − b` (GTIDs in `a` not in `b`), splitting intervals as needed.
  `subtract("uuid:1-10","uuid:4-6")` renders `"uuid:1-3:7-10"`.
  """
  @spec subtract(t(), t()) :: t()
  def subtract(a, b) do
    Enum.reduce(a, %{}, fn {uuid, ia}, acc ->
      put_nonempty(acc, uuid, subtract_intervals(ia, Map.get(b, uuid, [])))
    end)
  end

  @doc """
  Set intersection `a ∩ b`, per-UUID. Composes with `subtract/2` for the F1 gap
  predicate `(gtid_executed − checkpoint) ∩ gtid_purged`.
  """
  @spec intersection(t(), t()) :: t()
  def intersection(a, b) do
    Enum.reduce(a, %{}, fn {uuid, ia}, acc ->
      put_nonempty(acc, uuid, intersect_intervals(ia, Map.get(b, uuid, [])))
    end)
  end

  @doc """
  Is `a` a subset of `b`? The empty set is a subset of everything.
  """
  @spec subset?(t(), t()) :: boolean()
  def subset?(a, b), do: subtract(a, b) == %{}

  @doc """
  Are `a` and `b` disjoint? True iff `a ∩ b` is empty.
  """
  @spec disjoint?(t(), t()) :: boolean()
  def disjoint?(a, b), do: intersection(a, b) == %{}

  @doc """
  Returns the set as a canonical, ordered list of per-source intervals: sources sorted
  by UUID, and — within each source — intervals sorted ascending and coalesced.

  Bounds stay **INCLUSIVE**, matching the rest of this module. This is the read-side
  accessor the `COM_BINLOG_DUMP_GTID` resume encoder iterates; the exclusive-end wire
  conversion (`high -> high + 1`) is that encoder's concern and is deliberately not
  applied here. The empty set has no sources.
  """
  @spec sources(t()) :: [{uuid(), [{gno(), gno()}]}]
  def sources(set), do: Enum.sort(set)

  ## parsing

  # Real MySQL `SELECT @@gtid_executed` returns a long set with `,\n` between UUID
  # entries, so trim the whole value and each comma-separated entry — a consumer can
  # feed server output verbatim. Trimming is scoped to entry boundaries: a
  # leading/trailing comma still yields an empty entry that `parse_entry/1` rejects,
  # and whitespace inside an interval token (e.g. `"1 -5"`) still fails
  # `parse_gno/2`, so lenience never loosens the grammar.
  defp split_entries(string) do
    case String.trim(string) do
      "" -> []
      trimmed -> trimmed |> String.split(",") |> Enum.map(&String.trim/1)
    end
  end

  defp parse_entry(entry) do
    case String.split(entry, ":") do
      [uuid | [_ | _] = intervals] ->
        {parse_uuid(uuid), Enum.map(intervals, &parse_interval/1)}

      _ ->
        raise ArgumentError, "capstan: malformed GTID-set entry #{inspect(entry)}"
    end
  end

  defp parse_uuid(uuid) do
    down = String.downcase(uuid)

    if Regex.match?(@uuid_format, down) do
      down
    else
      raise ArgumentError, "capstan: malformed GTID source UUID #{inspect(uuid)}"
    end
  end

  defp parse_interval(token) do
    case String.split(token, "-") do
      [n] -> {parse_gno(n, token), parse_gno(n, token)}
      [n, m] -> parse_range(parse_gno(n, token), parse_gno(m, token), token)
      _ -> raise ArgumentError, "capstan: malformed GTID interval #{inspect(token)}"
    end
  end

  defp parse_range(low, high, _token) when high >= low, do: {low, high}

  defp parse_range(_low, _high, token) do
    raise ArgumentError, "capstan: descending GTID interval #{inspect(token)}"
  end

  # Accepts only canonical positive integers: no sign, no leading zeros. A
  # non-canonical number is malformed, not silently coerced.
  defp parse_gno(text, token) do
    with {n, ""} <- Integer.parse(text),
         true <- n >= 1 and text == Integer.to_string(n) do
      n
    else
      _ ->
        raise ArgumentError, "capstan: GTID numbers must be integers >= 1, got #{inspect(token)}"
    end
  end

  defp render_interval({n, n}), do: Integer.to_string(n)
  defp render_interval({low, high}), do: "#{low}-#{high}"

  ## set / interval algebra
  #
  # `put_nonempty/3` is the single choke point that enforces the canonical
  # invariant: every set-producing operation routes each UUID's intervals through
  # it, so results are always sorted, coalesced, and free of empty-list keys. That
  # is what lets subset?/disjoint? decide via structural equality with `%{}`.

  defp normalize_set(set) do
    Enum.reduce(set, %{}, fn {uuid, intervals}, acc ->
      put_nonempty(acc, uuid, intervals)
    end)
  end

  defp put_nonempty(set, uuid, intervals) do
    case normalize(intervals) do
      [] -> set
      normalized -> Map.put(set, uuid, normalized)
    end
  end

  # Sorts intervals and merges every overlapping OR adjacent pair (`4-6` after
  # `1-3` becomes `1-6`).
  defp normalize([]), do: []

  defp normalize(intervals) do
    intervals
    |> Enum.sort()
    |> Enum.reduce([], &coalesce/2)
    |> Enum.reverse()
  end

  defp coalesce({low, high}, [{plow, phigh} | rest]) when low <= phigh + 1 do
    [{plow, max(phigh, high)} | rest]
  end

  defp coalesce({low, high}, acc), do: [{low, high} | acc]

  defp subtract_intervals(ia, ib) do
    Enum.reduce(ib, ia, fn {low, high}, acc ->
      Enum.flat_map(acc, &subtract_one(&1, low, high))
    end)
  end

  defp subtract_one({low, high}, blow, bhigh) do
    if bhigh < low or blow > high do
      [{low, high}]
    else
      left = if low < blow, do: [{low, blow - 1}], else: []
      right = if high > bhigh, do: [{bhigh + 1, high}], else: []
      left ++ right
    end
  end

  # Two-pointer sweep over two sorted, coalesced lists.
  defp intersect_intervals([], _b), do: []
  defp intersect_intervals(_a, []), do: []

  defp intersect_intervals([{alow, ahigh} | ar] = a, [{blow, bhigh} | br] = b) do
    low = max(alow, blow)
    high = min(ahigh, bhigh)
    overlap = if low <= high, do: [{low, high}], else: []

    cond do
      ahigh < bhigh -> overlap ++ intersect_intervals(ar, b)
      ahigh > bhigh -> overlap ++ intersect_intervals(a, br)
      true -> overlap ++ intersect_intervals(ar, br)
    end
  end
end

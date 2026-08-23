defmodule Capstan.Zstd.Fse do
  @moduledoc """
  FSE (Finite State Entropy) decoding for zstd (RFC 8878 §4.1): the normalized
  probability table description (forward bitstream), the decoding-table
  construction, and one decode step.

  A table cell is `{symbol, nb_bits, new_state_base}`; decoding reads `symbol`
  from the current state and computes the next state as
  `new_state_base + read_bits(nb_bits)` (a reverse reader).
  """

  import Bitwise

  alias Capstan.Zstd.BitReader

  # nb = tableLog - highbit(nextState); tableLog >= 5 means nb fits a byte.
  @cell_default {0, 0, 0}

  @doc """
  Parses an FSE table description from the head of `bin`. Returns
  `{:ok, cells, max_symbol, rest}` where `cells` maps state (0..tableSize-1)
  to `{symbol, nb_bits, new_state_base}` and `rest` is the unconsumed,
  byte-aligned remainder. `max_accuracy_log` is the context's cap (RFC 8878:
  9 for LL/ML tables, 8 for offset tables, 6 for Huffman-weight tables).
  """
  @spec read_table(binary(), non_neg_integer(), non_neg_integer()) ::
          {:ok,
           %{
             optional(non_neg_integer()) =>
               {non_neg_integer(), non_neg_integer(), non_neg_integer()}
           }, non_neg_integer(), binary()}
          | {:error, term()}
  def read_table(bin, max_symbol_value, max_accuracy_log \\ 9) when is_binary(bin) do
    reader = BitReader.Forward.new(bin)

    with {:ok, accuracy_log, r1} <- accuracy(reader, 4, max_accuracy_log),
         {:ok, counts, max_sym, r2} <-
           counts(%{r: r1, al: accuracy_log, ts: 1 <<< accuracy_log}, max_symbol_value),
         {:ok, cells} <- build(counts, accuracy_log) do
      # The description consumes a round number of bytes.
      used = BitReader.Forward.remaining(reader) - BitReader.Forward.remaining(r2)
      {:ok, cells, max_sym, byte_align_drop(bin, used)}
    end
  end

  defp byte_align_drop(bin, bits_used),
    do: binary_part(bin, div(bits_used + 7, 8), byte_size(bin) - div(bits_used + 7, 8))

  # Consumes the 4 accuracy bits and hands the ADVANCED reader back — the
  # probability loop must not re-read them as its first count.
  defp accuracy(reader, n, max_al) do
    {v, r} = BitReader.Forward.read(reader, n)

    al = v + 5

    if al in 5..max_al, do: {:ok, al, r}, else: {:error, :bad_accuracy_log}
  end

  # The probability loop (RFC §4.1.1): remaining starts tableSize+1, threshold
  # tableSize, nbBits accuracy_log+1. Zero-probability symbols are followed by
  # 2-bit repeat flags (3 = another flag follows). A value read of nbBits-1
  # bits that lands under `max` uses one fewer bit; otherwise the full nbBits
  # with the Table-20 adjustment. count = value - 1; remaining -= abs(count).
  defp counts(st, max_sym) do
    parse(st, 0, st.ts + 1, st.ts, st.al + 1, max_sym, [], false)
  end

  # Zero-run flags: each 2-bit flag (0-3) skips that many ZERO-probability
  # symbols — every skipped symbol still needs its 0 entry in the counts list,
  # or every later symbol's index shifts (a silent table corruption). A flag
  # of 3 skips 3 AND another flag follows.
  defp parse(%{r: r} = st, symbol, remaining, threshold, nb, max_sym, acc, true) do
    {flags, r2} = BitReader.Forward.read(r, 2)
    symbol = symbol + flags
    acc = if flags > 0, do: Enum.map(1..flags, fn _ -> 0 end) ++ acc, else: acc

    if flags == 3 do
      parse(%{st | r: r2}, symbol, remaining, threshold, nb, max_sym, acc, true)
    else
      parse(%{st | r: r2}, symbol, remaining, threshold, nb, max_sym, acc, false)
    end
  end

  defp parse(%{r: _r} = st, symbol, remaining, threshold, nb, max_sym, acc, false) do
    if symbol > max_sym do
      # "The context in which the table is to be used specifies an expected
      # number of symbols. If the number of symbols decoded is not equal to
      # the expected, the header should be considered corrupt." (RFC 4.1.1)
      {:error, {:too_many_symbols, symbol}}
    else
      parse_symbol(st, symbol, remaining, threshold, nb, max_sym, acc)
    end
  end

  defp parse_symbol(%{r: r} = st, symbol, remaining, threshold, nb, max_sym, acc) do
    max = 2 * threshold - 1 - remaining

    {partial, r2} = BitReader.Forward.read(r, nb - 1)

    {value, r3} =
      if partial < max do
        {partial, r2}
      else
        {extra, r_e} = BitReader.Forward.read(r2, 1)
        {partial + (extra <<< (nb - 1)), r_e}
      end

    value = if value >= threshold, do: value - max, else: value
    count = value - 1
    remaining = remaining - abs(count)

    cond do
      remaining < 0 ->
        {:error, :distribution_overflow}

      remaining == 1 ->
        {:ok, Enum.reverse([count | acc]), symbol, r3}

      true ->
        {threshold2, nb2} = shrink(remaining, threshold, nb)

        parse(
          %{st | r: r3},
          symbol + 1,
          remaining,
          threshold2,
          nb2,
          max_sym,
          [count | acc],
          count == 0
        )
    end
  end

  defp shrink(remaining, threshold, nb) do
    if remaining < threshold,
      do: shrink(remaining, threshold >>> 1, nb - 1),
      else: {threshold, nb}
  end

  # -- table construction -------------------------------------------------------

  @doc """
  Builds the decoding table from normalized counts (with -1 = lowprob).

  Lowprob (-1) cells are placed from the table's END retreating; regular
  symbols spread from position 0 with the RFC step
  `(ts >>> 1) + (ts >>> 3) + 3`, skipping occupied lowprob cells; place FIRST
  then advance. Each cell's nb/newState derive from the per-symbol nextState
  counter (count, count+1, ... in position order).
  """
  @spec build([integer()], non_neg_integer()) ::
          {:ok,
           %{
             optional(non_neg_integer()) =>
               {non_neg_integer(), non_neg_integer(), non_neg_integer()}
           }}
          | {:error, term()}
  def build(counts, accuracy_log) do
    table_size = 1 <<< accuracy_log
    high = table_size - 1

    {lowprob_cells, symbol_next, high} =
      counts
      |> Enum.with_index()
      |> Enum.reduce({%{}, %{}, high}, fn {count, sym}, {cells, nxt, h} ->
        case count do
          -1 -> {Map.put(cells, h, sym), Map.put(nxt, sym, 1), h - 1}
          _ -> {cells, Map.put(nxt, sym, count), h}
        end
      end)

    step = (table_size >>> 1) + (table_size >>> 3) + 3
    mask = table_size - 1

    {spread, _pos} =
      counts
      |> Enum.with_index()
      |> Enum.reduce({%{}, 0}, fn {count, sym}, {cells, pos} ->
        spread_symbol(count, sym, {cells, pos}, {step, mask, high})
      end)

    cells = Map.merge(spread, lowprob_cells)

    if map_size(cells) != table_size do
      {:error, :table_size_mismatch}
    else
      final =
        Enum.reduce(0..(table_size - 1), {symbol_next, %{}}, fn pos, {nxt, out} ->
          sym = Map.fetch!(cells, pos)
          next_state = Map.fetch!(nxt, sym)
          nb = accuracy_log - highbit(next_state)
          new_base = (next_state <<< nb) - table_size

          {Map.put(nxt, sym, next_state + 1), Map.put(out, pos, {sym, nb, new_base})}
        end)
        |> elem(1)

      {:ok, final}
    end
  end

  # Elixir ranges infer a DESCENDING step when first > last (`1..0` is
  # [1, 0]), so a `1..max(count, 0)` "empty" spread is not empty — a negative
  # count would place phantom cells over regular ones. Guard the count.
  defp spread_symbol(count, _sym, {cells, pos}, _step) when count <= 0, do: {cells, pos}

  defp spread_symbol(count, sym, {cells, pos}, {step, mask, high}) do
    Enum.reduce(1..count, {cells, pos}, fn _, {c, p} ->
      c = Map.put(c, p, sym)
      p = advance(p, step, mask, high)
      {c, p}
    end)
  end

  defp advance(p, step, mask, high) do
    p = p + step &&& mask
    if p > high, do: advance(p, step, mask, high), else: p
  end

  @doc "The table's accuracy-log-equivalent: highbit of the table size."
  def table_log(table_size), do: highbit(table_size)

  defp highbit(0), do: 0

  defp highbit(v), do: highbit(v >>> 1, 0)
  defp highbit(0, acc), do: acc
  defp highbit(v, acc), do: highbit(v >>> 1, acc + 1)

  # -- decode step --------------------------------------------------------------

  @doc "One FSE decode step: `{symbol, next_reader}` for the current state."
  @spec decode(bitreader_state :: non_neg_integer(), map(), Capstan.Zstd.BitReader.Reverse.t()) ::
          {non_neg_integer(), non_neg_integer(), Capstan.Zstd.BitReader.Reverse.t()}
  def decode(state, cells, reader) do
    {sym, nb, base} = Map.get(cells, state, @cell_default)
    {value, reader2} = BitReader.Reverse.read(reader, nb)
    {sym, base + value, reader2}
  end

  @doc "The cell for a state without consuming bits (final-symbol peeks)."
  def symbol(state, cells) do
    {sym, _, _} = Map.get(cells, state, @cell_default)
    sym
  end
end

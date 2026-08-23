defmodule Capstan.Zstd.Huffman do
  @moduledoc """
  zstd Huffman decoding (RFC 8878 §4.2): the weight description (direct nibble
  form or FSE-compressed), the weight→prefix-code conversion, and the
  single-table (X1) decode of 1- and 4-stream literal blocks.
  """

  import Bitwise

  alias Capstan.Zstd.BitReader
  alias Capstan.Zstd.Fse

  @max_bits 11
  @cell_default {0, 0}

  @doc """
  Parses a Huffman tree description from the head of `bin`. Returns
  `{:ok, table, bytes_consumed}` where `table` maps a `tableLog`-bit peek value
  to `{symbol, nb_bits}`.
  """
  @spec read_tree(binary()) :: {:ok, map(), non_neg_integer()} | {:error, term()}
  def read_tree(<<header, rest::binary>>) when header >= 128 do
    # Direct: Number_of_Symbols = headerByte - 127; weights are 4-bit nibbles,
    # first weight in the HIGH nibble.
    n = header - 127
    # Weight nibbles come 2-per-byte; n explicit weights for symbols 0..n-1
    # (the last symbol's weight is deduced).
    weight_bytes = div(n + 1, 2)

    if byte_size(rest) < weight_bytes do
      {:error, :truncated_weights}
    else
      <<wbin::binary-size(^weight_bytes), _::binary>> = rest

      weights =
        for <<b::8 <- wbin>> do
          [b >>> 4, b &&& 0xF]
        end
        |> List.flatten()
        |> Enum.take(n)

      finish_weights(weights, weight_bytes)
    end
  end

  def read_tree(<<header, rest::binary>>) when header < 128 do
    # FSE-compressed weights: `header` bytes of bitstream, two interleaved
    # states, one distribution, accuracy_log <= 6.
    if byte_size(rest) < header do
      {:error, :truncated_weight_fse}
    else
      <<fse_bin::binary-size(^header), _::binary>> = rest

      with {:ok, weights} <- fse_weights(fse_bin) do
        finish_weights(weights, header)
      end
    end
  end

  # Weights 0..last-1 known; the LAST symbol's weight completes the tree to a
  # clean power of two (RFC §4.2.1.3). tableLog = highbit(total)+1 where total
  # = sum(2^(w-1)); lastWeight = 2^tableLog - total, itself a power of 2.
  defp finish_weights(weights, bytes_consumed) do
    total =
      weights
      |> Enum.reject(&(&1 == 0))
      |> Enum.reduce(0, &(&2 + (1 <<< (&1 - 1))))

    table_log = highbit(total) + 1

    if table_log > @max_bits do
      {:error, :huffman_table_log_too_large}
    else
      rest_power = (1 <<< table_log) - total

      if rest_power <= 0 or (rest_power &&& rest_power - 1) != 0 do
        {:error, :weights_not_power_of_two}
      else
        last_weight = highbit(rest_power) + 1
        weights = weights ++ [last_weight]

        {:ok, build_table(weights, table_log), bytes_consumed + 1}
      end
    end
  end

  # X1 table: symbol with weight w occupies 2^(w-1) consecutive cells, symbols
  # laid in natural order grouped by weight ASCENDING (weight 1 first), nb =
  # tableLog+1-w. Weight-0 symbols are REMOVED (RFC 4.2.1.3) — a zero weight
  # has no cells.
  defp build_table(weights, table_log) do
    {table, _} =
      weights
      |> Enum.with_index()
      |> Enum.reject(fn {w, _sym} -> w == 0 end)
      |> Enum.sort_by(fn {w, sym} -> {w, sym} end)
      |> Enum.reduce({%{}, 0}, fn {w, sym}, {tbl, pos} ->
        len = 1 <<< (w - 1)
        nb = table_log + 1 - w

        tbl =
          Enum.reduce(pos..(pos + len - 1), tbl, fn p, acc ->
            Map.put(acc, p, {sym, nb})
          end)

        {tbl, pos + len}
      end)

    table
  end

  # -- FSE-compressed weights ---------------------------------------------------

  defp fse_weights(bin) do
    # RFC 4.2.1.2: weights span 0..12 (tree depth capped at 11 bits) and the
    # weight FSE table's accuracy log is capped at 6. The reverse bitstream
    # starts AFTER the table description: the description is read forward from
    # the head, and read_table's byte-aligned remainder IS the bitstream.
    with {:ok, cells, _max_sym, bitstream} <- Fse.read_table(bin, 255, 6),
         {:ok, r} <- BitReader.Reverse.new(bitstream) do
      accuracy_log = Fse.table_log(map_size(cells))

      # Init state1 then state2, alternate; overflow = zero-filled update.
      {s1, r} = BitReader.Reverse.read(r, accuracy_log)
      {s2, r} = BitReader.Reverse.read(r, accuracy_log)

      decode_weight_states(cells, r, s1, s2, :s1, [])
    end
  end

  defp decode_weight_states(cells, r, s1, s2, active, acc) do
    state = if active == :s1, do: s1, else: s2
    sym = Fse.symbol(state, cells)

    {nb, base} =
      case cells[state] do
        {_s, n, b} -> {n, b}
        nil -> {0, 0}
      end

    {value, r2, truncated?} = BitReader.Reverse.read_padded(r, nb)
    next = base + value

    acc = [sym | acc]

    if truncated? do
      # Final: the other state's symbol completes the list (RFC §4.2.1.2).
      other = if active == :s1, do: s2, else: s1
      other_sym = Fse.symbol(other, cells)
      {:ok, Enum.reverse([other_sym | acc])}
    else
      s1 = if active == :s1, do: next, else: s1
      s2 = if active == :s2, do: next, else: s2
      decode_weight_states(cells, r2, s1, s2, other(active), acc)
    end
  end

  defp other(:s1), do: :s2
  defp other(:s2), do: :s1

  # -- stream decoding ----------------------------------------------------------

  @doc """
  Decodes `count` symbols from one Huffman reverse bitstream using `table`.
  """
  @spec decode_stream(map(), non_neg_integer(), binary()) ::
          {:ok, binary()} | {:error, term()}
  def decode_stream(table, count, bin) do
    table_log = table_log(table)

    with {:ok, r} <- BitReader.Reverse.new(bin) do
      do_stream(table, table_log, r, count, [])
    end
  end

  defp do_stream(_table, _log, %Capstan.Zstd.BitReader.Reverse{} = r, 0, acc) do
    # RFC 4.2.2: a bitstream not entirely and exactly consumed is faulty.
    if BitReader.Reverse.remaining(r) == 0 do
      {:ok, acc |> Enum.reverse() |> IO.iodata_to_binary()}
    else
      {:error, :huffman_stream_not_consumed}
    end
  end

  defp do_stream(table, log, r, n, acc) do
    peeked = BitReader.Reverse.peek(r, log)
    {sym, nb} = Map.get(table, peeked, @cell_default)

    case BitReader.Reverse.read_exact(r, nb) do
      {:ok, _, r2} -> do_stream(table, log, r2, n - 1, [sym | acc])
      {:error, _} = e -> e
    end
  end

  defp table_log(table) do
    highbit(table_size(table))
  end

  defp table_size(table) do
    table |> Map.keys() |> Enum.max() |> Kernel.+(1)
  end

  defp highbit(0), do: 0

  defp highbit(v), do: highbit(v >>> 1, 0)
  defp highbit(0, acc), do: acc
  defp highbit(v, acc), do: highbit(v >>> 1, acc + 1)
end

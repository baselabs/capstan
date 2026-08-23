import Bitwise

defmodule Capstan.Zstd.BitReader do
  import Bitwise

  @moduledoc """
  The two bit readers zstd needs (RFC 8878, conventions derived first-hand).

  - `Forward` — FSE table descriptions: bytes consumed first→last, the
    first-consumed bit is the LSB of a multi-bit value.
  - `Reverse` — Huffman-coded streams and sequence bitstreams: bytes walked
    last→first, bits MSB→LSB within each byte, after skipping the zero padding
    and the final `1` bit in the stream's last byte; the FIRST bit read is the
    MSB of a multi-bit value.

  Both track remaining usable bits so FSE weight decoding can detect the
  overflow condition ("extra bits are zero") and stop.
  """

  # -- forward (LSB-first) ------------------------------------------------------

  defmodule Forward do
    @moduledoc false
    defstruct [:bits, :pos]

    def new(bin) when is_binary(bin) do
      bits =
        for <<b::8 <- bin>> do
          for i <- 0..7, do: (:erlang.bsl(1, i) &&& b) != 0
        end
        |> List.flatten()
        |> List.to_tuple()

      %__MODULE__{bits: bits, pos: 0}
    end

    def remaining(%__MODULE__{bits: bits, pos: pos}) do
      tuple_size(bits) - pos
    end

    def read(%__MODULE__{} = r, n) do
      do_read(r, n, 0, 0)
    end

    # LSB-first assembly: the FIRST consumed bit is the value's LSB (the FSE
    # table description is little-endian — RFC 8878 §4.1.1 "read forward, in
    # little-endian fashion").
    defp do_read(%__MODULE__{bits: bits, pos: pos} = _r, n, acc, shift)
         when pos < tuple_size(bits) and n > 0 do
      bit = elem(bits, pos)

      acc =
        case bit do
          true -> acc + (1 <<< shift)
          false -> acc
        end

      do_read(%__MODULE__{bits: bits, pos: pos + 1}, n - 1, acc, shift + 1)
    end

    defp do_read(r, _n, acc, _shift), do: {acc, r}
  end

  # -- reverse (MSB-first, from the end) ---------------------------------------

  defmodule Reverse do
    @moduledoc false
    @type t :: %__MODULE__{bits: tuple(), pos: non_neg_integer()}
    defstruct [:bits, :pos]

    @doc """
    Bits in READING order. The padding `1` bit (and the zero padding above it)
    in the last byte is skipped: it terminates the stream, never decoded.
    """
    def new(<<>>), do: {:error, :empty_bitstream}

    def new(bin) when is_binary(bin) do
      size = byte_size(bin)
      last = :binary.at(bin, size - 1)

      if last == 0 do
        {:error, :zero_last_byte}
      else
        pad = 8 - highbit(last) - 1
        head = last_byte_bits(last, pad)
        tail = body_bits(bin, size - 2)
        {:ok, %__MODULE__{bits: List.to_tuple(head ++ tail), pos: 0}}
      end
    end

    # The usable bits of the last byte: MSB→LSB below the final `1` flag bit
    # and its zero padding.
    defp last_byte_bits(last, pad) do
      for i <- (7 - pad - 1)..0//-1 do
        (:erlang.bsl(1, i) &&& last) != 0
      end
    end

    defp body_bits(bin, last_index) do
      for j <- last_index..0//-1,
          i <- 7..0//-1 do
        (:erlang.bsl(1, i) &&& :binary.at(bin, j)) != 0
      end
    end

    def remaining(%__MODULE__{bits: bits, pos: pos}) do
      tuple_size(bits) - pos
    end

    def read(%__MODULE__{} = r, n), do: do_read(r, n, 0)

    # A peek reads (advancing) into a throwaway reader — the struct is
    # immutable, so discarding the advanced reader IS the non-consuming peek.
    # (Advancing `pos` conditionally inside do_read instead reads the SAME bit
    # n times.)
    def peek(%__MODULE__{} = r, n) do
      {v, _discarded} = read(r, n)
      v
    end

    # A strict read: consuming n bits when fewer remain is a corruption signal,
    # never zero-fill (the sequences/Huffman loops must fail closed on it — a
    # silent zero-fill decodes wrong values and still ends "fully consumed").
    @spec read_exact(t(), non_neg_integer()) ::
            {:ok, non_neg_integer(), t()} | {:error, :bitstream_exhausted}
    def read_exact(%__MODULE__{} = r, n) when n <= 0, do: {:ok, 0, r}

    def read_exact(%__MODULE__{} = r, n) do
      if remaining(r) < n do
        {:error, :bitstream_exhausted}
      else
        {v, r2} = read(r, n)
        {:ok, v, r2}
      end
    end

    # Reads n bits; if fewer remain, missing bits are ZERO (the FSE overflow rule).
    # Returns {value, reader, truncated?}.
    def read_padded(%__MODULE__{} = r, n) do
      avail = remaining(r)
      take = min(n, avail)
      {value, r2} = read(r, take)
      {value <<< (n - take), r2, avail < n}
    end

    # A short read (fewer bits remain than requested) places the available
    # bits at the TOP of the n-bit value with implicit zeros below — the same
    # placement BIT_lookBits uses — so a trailing Huffman peek still lands in
    # its code's cells.
    defp do_read(%__MODULE__{bits: bits, pos: pos} = _r, n, acc)
         when pos < tuple_size(bits) and n > 0 do
      bit = elem(bits, pos)

      acc =
        case bit do
          true -> acc * 2 + 1
          false -> acc * 2
        end

      do_read(%__MODULE__{bits: bits, pos: pos + 1}, n - 1, acc)
    end

    defp do_read(%__MODULE__{} = r, n, acc), do: {acc <<< n, r}

    defp highbit(0), do: 0
    defp highbit(v), do: highbit(v >>> 1, 0)

    defp highbit(0, acc), do: acc
    defp highbit(v, acc), do: highbit(v >>> 1, acc + 1)
  end
end

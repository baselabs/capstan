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
        (for <<b::8 <- bin>> do
           for i <- 0..7, do: (:erlang.bsl(1, i) &&& b) != 0
         end)
        |> List.flatten()
        |> List.to_tuple()

      %__MODULE__{bits: bits, pos: 0}
    end

    def remaining(%__MODULE__{bits: bits, pos: pos}) do
      tuple_size(bits) - pos
    end

    def read(%__MODULE__{} = r, n) do
      do_read(r, n, 0)
    end

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

    defp do_read(r, _n, acc), do: {acc, r}
  end

  # -- reverse (MSB-first, from the end) ---------------------------------------

  defmodule Reverse do
    @moduledoc false
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

        head =
          for i <- (7 - pad - 1)..0//-1 do
            (:erlang.bsl(1, i) &&& last) != 0
          end

        tail =
          for j <- (size - 2)..0//-1 do
            byte = :binary.at(bin, j)

            for i <- 7..0//-1 do
              (:erlang.bsl(1, i) &&& byte) != 0
            end
          end
          |> List.flatten()

        {:ok, %__MODULE__{bits: List.to_tuple(head ++ tail), pos: 0}}
      end
    end

    def remaining(%__MODULE__{bits: bits, pos: pos}) do
      tuple_size(bits) - pos
    end

    def read(%__MODULE__{} = r, n), do: do_read(r, n, 0, false)
    def peek(%__MODULE__{} = r, n), do: do_read(r, n, 0, true)

    # Reads n bits; if fewer remain, missing bits are ZERO (the FSE overflow rule).
    # Returns {value, reader, truncated?}.
    def read_padded(%__MODULE__{} = r, n) do
      avail = remaining(r)
      take = min(n, avail)
      {value, r2} = read(r, take)
      {value <<< (n - take), r2, avail < n}
    end

    defp do_read(%__MODULE__{bits: bits, pos: pos} = _r, n, acc, peek?)
        when pos < tuple_size(bits) and n > 0 do
      bit = elem(bits, pos)

      acc =
        case bit do
          true -> acc * 2 + 1
          false -> acc * 2
        end

      next_pos = if peek?, do: pos, else: pos + 1
      do_read(%__MODULE__{bits: bits, pos: next_pos}, n - 1, acc, peek?)
    end

    defp do_read(%__MODULE__{} = r, _n, acc, _peek?), do: {acc, r}

    defp highbit(0), do: 0
    defp highbit(v), do: highbit(v >>> 1, 0)

    defp highbit(0, acc), do: acc
    defp highbit(v, acc), do: highbit(v >>> 1, acc + 1)
  end
end

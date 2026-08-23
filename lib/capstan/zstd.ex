defmodule Capstan.Zstd do
  @moduledoc """
  Pure-Elixir zstd frame decompressor (RFC 8878) — built to inflate MySQL
  `TRANSACTION_PAYLOAD_EVENT` bodies (`binlog_transaction_compression=ON`,
  ADR-0011). Decode only; no NIF, no compression (ADR-0008).

  The contract is **byte-exact or fail closed**: every corruption signal the
  RFC names (bad magic, a set reserved bit, an unknown block type, a
  distribution overflow, a sequence bitstream not fully consumed, an offset
  outside the decoded window, a Frame_Content_Size mismatch, a
  content-checksum mismatch) returns `{:error, reason}` — never silently
  mis-decoded bytes. Conformance is proven against real MySQL-produced frames
  inflated by the reference `zstd` binary (`test/capstan/zstd_test.exs`),
  never self-signed fixtures.

  ## Scope decisions (all fail-closed)

    * Dictionaries are refused (`:dictionary_unsupported`) — a dictionary ID
      in the frame header means the frame references out-of-band content
      capstan does not have; guessing is the silent-corruption class this
      module exists to prevent.
    * The XXH64 content checksum (RFC §3.1.1), when the frame carries one, is
      VERIFIED — a present integrity signal is never skipped.
    * Whole-frame in-memory decode: the output accumulator IS the window, so
      an offset is valid iff it is positive and within the bytes decoded so
      far and under the declared Window_Size.
  """

  import Bitwise

  alias Capstan.Zstd.{BitReader, Fse, Huffman}

  @magic <<0x28, 0xB5, 0x2F, 0xFD>>
  @block_max 128 * 1024

  # RFC 8878 Table 16 — literals-length codes: baseline + extra bits.
  @ll_baseline {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 18, 20, 22, 24, 28, 32,
                40, 48, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16_384, 32_768, 65_536}

  @ll_extra {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 3, 3, 4, 6, 7, 8,
             9, 10, 11, 12, 13, 14, 15, 16}

  # RFC 8878 Table 17 — match-length codes.
  @ml_baseline {3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
                25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 37, 39, 41, 43, 47, 51, 59, 67, 83,
                99, 131, 259, 515, 1027, 2051, 4099, 8195, 16_387, 32_771, 65_539}

  @ml_extra {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
             0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 3, 3, 4, 4, 5, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

  # RFC 8878 §3.1.1.3.2.2 — predefined distributions.
  @ll_default {4, 3, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 3, 2, 1,
               1, 1, 1, 1, -1, -1, -1, -1}

  @ml_default {1, 4, 3, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
               1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, -1, -1, -1, -1, -1, -1, -1}

  @of_default {1, 1, 1, 1, 1, 1, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, -1, -1, -1,
               -1, -1}

  # Compile-time table construction; a transcription error in a default
  # distribution fails the build loudly (Fse.build enforces full-table fill).
  @ll_predefined (fn ->
                    {:ok, c} = Fse.build(Tuple.to_list(@ll_default), 6)
                    c
                  end).()
  @ml_predefined (fn ->
                    {:ok, c} = Fse.build(Tuple.to_list(@ml_default), 6)
                    c
                  end).()
  @of_predefined (fn ->
                    {:ok, c} = Fse.build(Tuple.to_list(@of_default), 5)
                    c
                  end).()

  @doc """
  Decompresses a concatenation of zstd frames (magic `0xFD2FB528`) and
  skippable frames (`0x184D2A5X`), returning the concatenated decompressed
  content, or `{:error, reason}` on any corruption signal — never partial or
  guessed output.

  ## The output cap (span review, blocking)

  `max_output_bytes:` bounds the CUMULATIVE decompressed size DURING inflation —
  a frame that would exceed it fails `{:error, :output_too_large}` BEFORE the
  output is materialized. Without this, a valid frame with no content-size TLV
  and thousands of ≤128 KB RLE blocks inflates a ~MB payload toward tens of GB
  as a single BEAM binary and OOM-crashes the node: any post-hoc size check
  fires only after the memory is already spent. Without the option there is no
  cap (the generic utility form); the binary-log path always passes one.
  """
  @spec decompress(binary(), keyword()) :: {:ok, binary()} | {:error, term()}
  def decompress(bin) when is_binary(bin), do: decompress(bin, [])

  def decompress(<<>>, _opts), do: {:error, :bad_magic}

  def decompress(bin, opts) when is_binary(bin) do
    case Keyword.get(opts, :max_output_bytes) do
      nil -> do_frames(bin, [], :unbounded)
      cap when is_integer(cap) and cap > 0 -> do_frames(bin, [], cap)
      _ -> {:error, :bad_output_cap}
    end
  end

  defp do_frames(<<>>, acc, _cap), do: {:ok, :erlang.iolist_to_binary(:lists.reverse(acc))}

  # Skippable frame: 4-byte magic (any 0x184D2A50..5F, LE) + u32 LE size.
  defp do_frames(<<v, 0x2A, 0x4D, 0x18, size::32-little, rest::binary>>, acc, cap)
       when v in 0x50..0x5F do
    case rest do
      <<_skip::binary-size(^size), rest2::binary>> -> do_frames(rest2, acc, cap)
      _ -> {:error, :truncated_frame}
    end
  end

  defp do_frames(<<@magic, rest::binary>>, acc, cap) do
    case frame(rest, cap) do
      {:ok, out, rest2} ->
        produced = Enum.sum(for b <- acc, do: byte_size(b)) + byte_size(out)

        if cap == :unbounded or produced <= cap,
          do: do_frames(rest2, [out | acc], cap),
          else: {:error, :output_too_large}

      {:error, _} = e ->
        e
    end
  end

  defp do_frames(_bin, _acc, _cap), do: {:error, :bad_magic}

  # -- one zstd frame -----------------------------------------------------------

  defp frame(bin, cap) do
    do_frame(bin, cap)
  end

  defp do_frame(<<fhd::8, rest::binary>>, cap) do
    fcs_flag = fhd >>> 6
    single_segment = fhd >>> 5 &&& 1
    checksum? = (fhd >>> 2 &&& 1) == 1

    if (fhd >>> 3 &&& 1) == 1 do
      # RFC 3.1.1.1.1.4: the reserved bit must be zero; a set bit is corrupt.
      {:error, :reserved_bit_set}
    else
      did_size = elem({0, 1, 2, 4}, fhd &&& 3)

      fcs_size =
        case fcs_flag do
          0 -> single_segment
          1 -> 2
          2 -> 4
          3 -> 8
        end

      with {:ok, window, rest2} <- window_size(single_segment, rest),
           {:ok, rest3} <- dictionary_id(did_size, rest2),
           {:ok, fcs, rest4} <- frame_content_size(fcs_size, rest3),
           window <- frame_window(single_segment, window, fcs) do
        st = %{huff: nil, ll: nil, of: nil, ml: nil, reps: {1, 4, 8}, window: window}
        blocks(rest4, st, <<>>, fcs, checksum?, cap)
      end
    end
  end

  # A declared FCS beyond the cap is refused BEFORE any block is read (the cheap
  # pre-check; the block loop enforces it for undeclared/lying sizes).
  defp do_frame(<<>>, _cap), do: {:error, :truncated_frame}

  # Single-segment frames carry no Window_Descriptor: Window_Size = FCS
  # (RFC 3.1.1.1.1.2), which is necessarily present in that case.
  defp window_size(1, rest), do: {:ok, nil, rest}

  defp window_size(0, <<wd::8, rest::binary>>) do
    window_log = 10 + (wd >>> 3)
    base = 1 <<< window_log
    {:ok, base + div(base, 8) * (wd &&& 7), rest}
  end

  defp window_size(0, <<>>), do: {:error, :truncated_frame}

  defp frame_window(1, nil, fcs) when is_integer(fcs), do: max(fcs, 1)
  defp frame_window(0, window, _fcs) when is_integer(window), do: window

  defp dictionary_id(0, rest), do: {:ok, rest}
  defp dictionary_id(_size, _rest), do: {:error, :dictionary_unsupported}

  defp frame_content_size(0, rest), do: {:ok, nil, rest}

  defp frame_content_size(size, rest) do
    case rest do
      <<fcs::binary-size(^size), rest2::binary>> ->
        v = :binary.decode_unsigned(fcs, :little)
        # RFC 3.1.1.1.4: the 2-byte variant carries size − 256.
        {:ok, if(size == 2, do: v + 256, else: v), rest2}

      _ ->
        {:error, :truncated_frame}
    end
  end

  # -- frame block loop ---------------------------------------------------------

  defp blocks(bin, st, out, fcs, checksum?, cap) do
    case bin do
      <<h::24-little, rest::binary>> ->
        last? = (h &&& 1) == 1
        type = h >>> 1 &&& 3
        size = h >>> 3

        case block(type, size, rest, st, out) do
          {:ok, out2, st2, rest2} ->
            next_block(last?, {out, out2, st2, rest2, fcs, checksum?, st.window, cap})

          {:error, _} = e ->
            e
        end

      _ ->
        {:error, :truncated_frame}
    end
  end

  defp next_block(last?, {out, out2, st2, rest2, fcs, checksum?, window, cap}) do
    # RFC 3.1.1.2.4: Block_Maximum_Size bounds every block's decompressed
    # size; exceeding it is corruption. The OUTPUT CAP is checked here too —
    # BEFORE another block can grow the binary (the decompression-bomb bound).
    cond do
      byte_size(out2) - byte_size(out) > min(window, @block_max) ->
        {:error, :block_size_exceeded}

      cap != :unbounded and byte_size(out2) > cap ->
        {:error, :output_too_large}

      last? ->
        close_frame(out2, rest2, fcs, checksum?)

      true ->
        blocks(rest2, st2, out2, fcs, checksum?, cap)
    end
  end

  # Raw_Block
  defp block(0, size, rest, st, out) do
    case rest do
      <<content::binary-size(^size), rest2::binary>> ->
        {:ok, out <> content, st, rest2}

      _ ->
        {:error, :truncated_frame}
    end
  end

  # RLE_Block: one byte, repeated `size` times.
  defp block(1, size, <<byte::8, rest::binary>>, st, out),
    do: {:ok, out <> :binary.copy(<<byte>>, size), st, rest}

  defp block(1, _size, <<>>, _st, _out), do: {:error, :truncated_frame}

  # Compressed_Block
  defp block(2, size, rest, st, out) do
    case rest do
      <<content::binary-size(^size), rest2::binary>> ->
        case compressed_block(content, out, st) do
          {:ok, out2, st2} -> {:ok, out2, st2, rest2}
          {:error, _} = e -> e
        end

      _ ->
        {:error, :truncated_frame}
    end
  end

  # Reserved: "considered to be corrupt data, and a compliant decoder must
  # reject it" (RFC 3.1.1.2.2).
  defp block(3, _size, _rest, _st, _out), do: {:error, :reserved_block_type}

  defp close_frame(out, rest, fcs, checksum?) do
    with :ok <- fcs_check(fcs, out),
         {:ok, rest2} <- content_checksum(checksum?, out, rest) do
      {:ok, out, rest2}
    end
  end

  defp fcs_check(nil, _out), do: :ok
  defp fcs_check(fcs, out) when byte_size(out) == fcs, do: :ok
  defp fcs_check(_fcs, _out), do: {:error, :frame_content_size_mismatch}

  defp content_checksum(false, _out, rest), do: {:ok, rest}

  defp content_checksum(true, out, rest) do
    case rest do
      <<sum::32-little, rest2::binary>> ->
        if (xxh64(out) &&& 0xFFFFFFFF) == sum,
          do: {:ok, rest2},
          else: {:error, :content_checksum_mismatch}

      _ ->
        {:error, :truncated_frame}
    end
  end

  # -- compressed block: literals + sequences -----------------------------------

  defp compressed_block(block, out, st) do
    with {:ok, literals, rest, st2} <- literals_section(block, st),
         {:ok, sequences, st3} <- sequences_section(rest, st2),
         {:ok, out2, reps} <- execute(sequences, literals, out, st3.reps, st3.window) do
      {:ok, out2, %{st3 | reps: reps}}
    end
  end

  # -- literals section (RFC 3.1.1.3.1) -----------------------------------------

  defp literals_section(<<b0::8, rest::binary>>, st) do
    type = b0 &&& 3
    sf = b0 >>> 2 &&& 3

    case type do
      t when t in 0..1 ->
        raw_rle_literals(type, sf, b0, rest, st)

      t when t in 2..3 ->
        huffman_literals(type, sf, <<b0::8, rest::binary>>, st)
    end
  end

  defp literals_section(<<>>, _st), do: {:error, :truncated_frame}

  defp raw_rle_literals(type, sf, b0, rest, st) do
    case {sf, rest} do
      # 1-bit size format: Size_Format's second bit belongs to
      # Regenerated_Size (values 00 and 10 both select it).
      {sf, rest} when sf in [0, 2] ->
        literal_content(type, b0 >>> 3, rest, st)

      {1, <<b1::8, r::binary>>} ->
        literal_content(type, (b0 >>> 4) + (b1 <<< 4), r, st)

      {3, <<b1::8, b2::8, r::binary>>} ->
        literal_content(type, (b0 >>> 4) + (b1 <<< 4) + (b2 <<< 12), r, st)

      _ ->
        {:error, :truncated_frame}
    end
  end

  defp literal_content(0, regen, rest, st) do
    case rest do
      <<literals::binary-size(^regen), rest2::binary>> -> {:ok, literals, rest2, st}
      _ -> {:error, :truncated_frame}
    end
  end

  defp literal_content(1, regen, <<byte::8, rest2::binary>>, st),
    do: {:ok, :binary.copy(<<byte>>, regen), rest2, st}

  defp literal_content(1, _regen, <<>>, _st), do: {:error, :truncated_frame}

  defp huffman_literals(type, sf, content, st) do
    case literals_sizes(sf, content) do
      {regen, comp, header_size} ->
        split_literals_header(type, sf, regen, comp, header_size, content, st)

      :error ->
        {:error, :truncated_frame}
    end
  end

  defp split_literals_header(type, sf, regen, comp, header_size, content, st) do
    case content do
      <<_::binary-size(^header_size), streams_and_tree::binary-size(^comp), rest::binary>> ->
        with {:ok, table, streams} <- huffman_table(type, streams_and_tree, st),
             {:ok, literals} <- huffman_streams(sf, table, regen, streams) do
          {:ok, literals, rest, %{st | huff: table}}
        end

      _ ->
        {:error, :truncated_frame}
    end
  end

  defp literals_sizes(sf, bin)
  # {Regenerated_Size, Compressed_Size, Literals_Section_Header bytes} per
  # RFC 3.1.1.3.1.1: the sizes sit LSB-after the 4 flag bits of the header.
  defp literals_sizes(sf, bin) when byte_size(bin) >= 3 and sf in [0, 1] do
    <<v::24-little>> = binary_part(bin, 0, 3)
    {v >>> 4 &&& 0x3FF, v >>> 14 &&& 0x3FF, 3}
  end

  defp literals_sizes(2, bin) when byte_size(bin) >= 4 do
    <<v::32-little>> = binary_part(bin, 0, 4)
    {v >>> 4 &&& 0x3FFF, v >>> 18 &&& 0x3FFF, 4}
  end

  defp literals_sizes(3, bin) when byte_size(bin) >= 5 do
    <<a::32-little, b::8>> = binary_part(bin, 0, 5)
    v = a + (b <<< 32)
    {v >>> 4 &&& 0x3FFFF, v >>> 22 &&& 0x3FFFF, 5}
  end

  defp literals_sizes(_sf, _bin), do: :error

  defp huffman_table(2, content, _st) do
    case Huffman.read_tree(content) do
      {:ok, table, tree_bytes} ->
        {:ok, table, binary_part(content, tree_bytes, byte_size(content) - tree_bytes)}

      {:error, _} = e ->
        e
    end
  end

  defp huffman_table(3, content, st) do
    # Treeless: the previous Compressed_Literals_Block's tree; the streams are
    # the whole content (no description). No previous tree is corruption.
    case st.huff do
      nil -> {:error, :no_previous_huffman_table}
      table -> {:ok, table, content}
    end
  end

  defp huffman_streams(0, table, regen, streams) do
    Huffman.decode_stream(table, regen, streams)
  end

  defp huffman_streams(_four_streams, table, regen, streams) do
    case streams do
      <<s1::16-little, s2::16-little, s3::16-little, srest::binary>> ->
        s4 = byte_size(srest) - s1 - s2 - s3
        seg = div(regen + 3, 4)
        last = regen - seg * 3

        # RFC 3.1.1.3.1.6: Stream4_Size = Total_Streams_Size − 6 − s1 − s2 − s3;
        # negative values (either size) mean the jump table is corrupt.
        if s4 < 0 or last < 0 do
          {:error, :corrupt_jump_table}
        else
          decode_four_streams(table, {s1, s2, s3, s4, seg, last}, srest)
        end

      _ ->
        {:error, :truncated_frame}
    end
  end

  defp decode_four_streams(table, {s1, s2, s3, s4, seg, last}, srest) do
    <<c1::binary-size(^s1), c2::binary-size(^s2), c3::binary-size(^s3), c4::binary-size(^s4)>> =
      srest

    with {:ok, d1} <- Huffman.decode_stream(table, seg, c1),
         {:ok, d2} <- Huffman.decode_stream(table, seg, c2),
         {:ok, d3} <- Huffman.decode_stream(table, seg, c3),
         {:ok, d4} <- Huffman.decode_stream(table, last, c4) do
      {:ok, <<d1::binary, d2::binary, d3::binary, d4::binary>>}
    end
  end

  # -- sequences section (RFC 3.1.1.3.2) ----------------------------------------

  defp sequences_section(<<0, _rest::binary>>, st), do: {:ok, [], st}

  defp sequences_section(<<b0::8, rest::binary>>, st) do
    # The guard is load-bearing: sequence_count's error clauses return
    # {:error, reason} — a 2-tuple the unguarded {n, rest} pattern would match
    # as success (n = :error), crashing in sequence_modes instead of failing
    # closed on a truncated count.
    with {n, rest} when is_integer(n) <- sequence_count(b0, rest),
         {:ok, modes, rest2} <- sequence_modes(rest),
         {:ok, st2, rest3} <- sequence_tables(modes, st, rest2),
         {:ok, sequences} <- decode_sequences(n, st2, rest3) do
      {:ok, sequences, st2}
    else
      {:error, _} = e -> e
    end
  end

  defp sequences_section(<<>>, _st), do: {:error, :truncated_frame}

  defp sequence_count(b0, <<b1::8, rest::binary>>) when b0 < 255 and b0 >= 128,
    do: {((b0 - 128) <<< 8) + b1, rest}

  defp sequence_count(255, <<b1::8, b2::8, rest::binary>>),
    do: {b1 + (b2 <<< 8) + 0x7F00, rest}

  defp sequence_count(b0, rest) when b0 < 128, do: {b0, rest}
  defp sequence_count(_b0, <<>>), do: {:error, :truncated_frame}
  defp sequence_count(255, <<_::8>>), do: {:error, :truncated_frame}

  defp sequence_modes(<<modes::8, rest::binary>>) do
    if (modes &&& 3) != 0 do
      # RFC 3.1.1.3.2.1: the Reserved field "must be all zeroes".
      {:error, :reserved_modes_bits}
    else
      {:ok, %{ll: modes >>> 6 &&& 3, of: modes >>> 4 &&& 3, ml: modes >>> 2 &&& 3}, rest}
    end
  end

  defp sequence_modes(<<>>), do: {:error, :truncated_frame}

  # Table acquisition order: Literals_Length, Offset, Match_Length (RFC
  # 3.1.1.3.2). Repeat mode reuses the frame's previous table — including RLE
  # and predefined ones; none yet is corruption.
  defp sequence_tables(%{ll: llm, of: ofm, ml: mlm}, st, bin) do
    with {:ok, ll, bin2} <- sequence_table(llm, 35, 9, @ll_predefined, st.ll, bin),
         {:ok, of, bin3} <- sequence_table(ofm, 31, 8, @of_predefined, st.of, bin2),
         {:ok, ml, bin4} <- sequence_table(mlm, 52, 9, @ml_predefined, st.ml, bin3) do
      {:ok, %{st | ll: ll, of: of, ml: ml}, bin4}
    end
  end

  defp sequence_table(0, _max_sym, _max_al, predefined, _prev, bin),
    do: {:ok, predefined, bin}

  # RLE_Mode: the description is a single symbol byte used for every
  # sequence — a zero-accuracy table whose state never moves.
  defp sequence_table(1, max_sym, _max_al, _predefined, _prev, <<sym::8, rest::binary>>) do
    # The RLE description byte is a SYMBOL INDEX — an out-of-range byte is corruption
    # (elem/2 on the baseline tables would crash instead of signalling). The FSE arm
    # already enforces the same bound via too_many_symbols.
    if sym <= max_sym,
      do: {:ok, %{0 => {sym, 0, 0}}, rest},
      else: {:error, :too_many_symbols}
  end

  defp sequence_table(1, _max_sym, _max_al, _predefined, _prev, <<>>),
    do: {:error, :truncated_frame}

  defp sequence_table(2, max_sym, max_al, _predefined, _prev, bin) do
    case Fse.read_table(bin, max_sym, max_al) do
      {:ok, cells, _max, rest} -> {:ok, cells, rest}
      {:error, _} = e -> e
    end
  end

  defp sequence_table(3, _max_sym, _max_al, _predefined, nil, _bin),
    do: {:error, :no_previous_fse_table}

  defp sequence_table(3, _max_sym, _max_al, _predefined, prev, bin),
    do: {:ok, prev, bin}

  defp decode_sequences(n, st, bitstream) do
    ll_al = table_log(st.ll)
    of_al = table_log(st.of)
    ml_al = table_log(st.ml)

    with {:ok, r} <- BitReader.Reverse.new(bitstream),
         {:ok, ll_state, r} <- init_state(r, ll_al),
         {:ok, of_state, r} <- init_state(r, of_al),
         {:ok, ml_state, r} <- init_state(r, ml_al) do
      seq_loop(n, st, r, ll_state, of_state, ml_state, [])
    end
  end

  defp init_state(r, 0), do: {:ok, 0, r}

  defp init_state(r, al) do
    BitReader.Reverse.read_exact(r, al)
  end

  defp seq_loop(0, _st, r, _ll, _of, _ml, acc) do
    # RFC 3.1.1.3.2.1.2: at the end "the bitstream shall be entirely
    # consumed; otherwise, the bitstream is considered corrupted."
    if BitReader.Reverse.remaining(r) == 0 do
      {:ok, Enum.reverse(acc)}
    else
      {:error, :sequence_bits_not_consumed}
    end
  end

  defp seq_loop(n, st, r, ll_state, of_state, ml_state, acc) do
    ll_sym = Fse.symbol(ll_state, st.ll)
    ml_sym = Fse.symbol(ml_state, st.ml)
    of_sym = Fse.symbol(of_state, st.of)

    # Read order per sequence: offset extra, then match extra, then literal
    # extra (RFC 3.1.1.3.2.1.2). An offset code IS its extra-bit count.
    with {:ok, of_extra, r} <- BitReader.Reverse.read_exact(r, of_sym),
         {:ok, ml_extra, r} <- BitReader.Reverse.read_exact(r, elem(@ml_extra, ml_sym)),
         {:ok, ll_extra, r} <- BitReader.Reverse.read_exact(r, elem(@ll_extra, ll_sym)) do
      ll = elem(@ll_baseline, ll_sym) + ll_extra
      ml = elem(@ml_baseline, ml_sym) + ml_extra
      offset_value = (1 <<< of_sym) + of_extra

      advance_and_next(n, st, r, ll_state, of_state, ml_state, [{ll, offset_value, ml} | acc])
    end
  end

  defp advance_and_next(1, st, r, ll_state, of_state, ml_state, acc),
    do: seq_loop(0, st, r, ll_state, of_state, ml_state, acc)

  defp advance_and_next(n, st, r, ll_state, of_state, ml_state, acc) do
    # State update order: LL, then ML, then OF (RFC 3.1.1.3.2.1.2).
    with {:ok, ll_state, r} <- advance(ll_state, st.ll, r),
         {:ok, ml_state, r} <- advance(ml_state, st.ml, r),
         {:ok, of_state, r} <- advance(of_state, st.of, r) do
      seq_loop(n - 1, st, r, ll_state, of_state, ml_state, acc)
    end
  end

  defp advance(state, cells, r) do
    {_sym, nb, base} = Map.get(cells, state, {0, 0, 0})

    case BitReader.Reverse.read_exact(r, nb) do
      {:ok, v, r2} -> {:ok, base + v, r2}
      {:error, _} = e -> e
    end
  end

  # -- sequence execution (RFC 3.1.1.4 / 3.1.1.5) --------------------------------

  defp execute(sequences, literals, out, reps, window) do
    do_execute(sequences, literals, 0, out, reps, window)
  end

  defp do_execute([], literals, pos, out, reps, _window) do
    # "When all sequences are decoded, if there are literals left in the
    # Literals_Section, these bytes are added at the end of the block."
    {:ok, out <> binary_part(literals, pos, byte_size(literals) - pos), reps}
  end

  defp do_execute([{ll, offset_value, ml} | rest], literals, pos, out, reps, window) do
    if pos + ll > byte_size(literals) do
      {:error, :literals_exhausted}
    else
      out = out <> binary_part(literals, pos, ll)
      pos = pos + ll
      {offset, reps} = resolve_offset(offset_value, ll, reps)

      # RFC 3.1.1.4: with no dictionary the match must land inside the bytes
      # decoded so far, and (RFC 3.1.1) no offset may EXCEED Window_Size — an offset
      # EQUAL to Window_Size is the maximum VALID back-reference (span review note:
      # `>=` over-rejected it; single-segment frames never hit this either way).
      if offset < 1 or offset > byte_size(out) or offset > window do
        {:error, :offset_out_of_range}
      else
        out = match_copy(out, offset, ml)
        do_execute(rest, literals, pos, out, reps, window)
      end
    end
  end

  # Overlap-safe match copy: `ml` bytes from `offset` back. When the copy
  # source overlaps the destination (offset < ml) the pattern repeats while
  # copying — :binary.copy over the `offset`-sized tail reproduces that
  # exactly.
  defp match_copy(out, offset, ml) do
    start = byte_size(out) - offset
    chunk = binary_part(out, start, offset)

    copied =
      if ml <= offset do
        binary_part(chunk, 0, ml)
      else
        :binary.copy(chunk, div(ml, offset)) <> binary_part(chunk, 0, rem(ml, offset))
      end

    out <> copied
  end

  # RFC 3.1.1.5. With literals_length == 0 the repeat slots shift by one; an
  # offset_value of 3 in that case resolves to Repeated_Offset1 − 1 and is an
  # INSERT (shift-back), not a repeat. Mirrors the reference ZSTD_updateRep.
  defp resolve_offset(ov, ll, {r1, r2, r3} = reps) when ov <= 3 do
    case ov - 1 + if(ll == 0, do: 1, else: 0) do
      0 -> {r1, reps}
      1 -> {r2, {r2, r1, r3}}
      2 -> {r3, {r3, r1, r2}}
      3 -> {r1 - 1, {r1 - 1, r1, r2}}
    end
  end

  defp resolve_offset(ov, _ll, {r1, r2, _r3}) do
    offset = ov - 3
    {offset, {offset, r1, r2}}
  end

  # -- helpers -------------------------------------------------------------------

  defp table_log(cells), do: Fse.table_log(map_size(cells))

  # -- XXH64 (content-checksum verification, RFC 8878 §3.1.1 / [XXHASH]) --------

  @p1 0x9E3779B185EBCA87
  @p2 0xC2B2AE3D27D4EB4F
  @p3 0x165667B19E3779F9
  @p4 0x85EBCA77C2B2AE63
  @p5 0x27D4EB2F165667C5
  @m64 0xFFFFFFFFFFFFFFFF

  defp xxh64(bin), do: xxh64(bin, 0)

  defp xxh64(bin, seed) do
    len = byte_size(bin)

    {h0, rest} =
      if len >= 32 do
        {v1, v2, v3, v4, rest} = xxh64_stripes(bin, seed)
        h = rotl64(v1, 1) + rotl64(v2, 7) + rotl64(v3, 12) + rotl64(v4, 18) &&& @m64

        {xxh64_merge(xxh64_merge(xxh64_merge(xxh64_merge(h, v1), v2), v3), v4), rest}
      else
        {seed + @p5, bin}
      end

    xxh64_tail(rest, h0 + len &&& @m64) |> xxh64_avalanche()
  end

  defp xxh64_stripes(bin, seed),
    do:
      xxh64_stripes(bin, div(byte_size(bin), 32), seed + @p1 + @p2, seed + @p2, seed, seed - @p1)

  defp xxh64_stripes(rest, 0, v1, v2, v3, v4),
    do: {v1 &&& @m64, v2 &&& @m64, v3 &&& @m64, v4 &&& @m64, rest}

  defp xxh64_stripes(bin, n, v1, v2, v3, v4) do
    <<a::64-little, b::64-little, c::64-little, d::64-little, rest::binary>> = bin

    xxh64_stripes(
      rest,
      n - 1,
      xxh64_round(v1, a),
      xxh64_round(v2, b),
      xxh64_round(v3, c),
      xxh64_round(v4, d)
    )
  end

  defp xxh64_round(acc, input), do: rotl64(acc + input * @p2 &&& @m64, 31) * @p1 &&& @m64

  # mergeAccumulator (spec step 3): acc ^= round(0, accN); acc *= P1; acc += P4
  # — NO rotation (the 27-rotation belongs to the tail's 8-byte step only).
  defp xxh64_merge(h, v), do: bxor(h, xxh64_round(0, v)) * @p1 + @p4 &&& @m64

  defp xxh64_tail(<<k1::64-little, rest::binary>>, h) do
    xxh64_tail(rest, xxh64_merge_short(h, k1))
  end

  defp xxh64_tail(<<k::32-little, rest::binary>>, h) do
    h = rotl64(bxor(h, k * @p1 &&& @m64), 23) * @p2 + @p3 &&& @m64
    xxh64_tail(rest, h)
  end

  defp xxh64_tail(<<b::8, rest::binary>>, h) do
    h = rotl64(bxor(h, b * @p5 &&& @m64), 11) * @p1 &&& @m64
    xxh64_tail(rest, h)
  end

  defp xxh64_tail(<<>>, h), do: h

  defp xxh64_merge_short(h, k1),
    do: rotl64(bxor(h, xxh64_round(0, k1)), 27) * @p1 + @p4 &&& @m64

  defp xxh64_avalanche(h) do
    h = bxor(h, h >>> 33) * @p2 &&& @m64
    h = bxor(h, h >>> 29) * @p3 &&& @m64
    bxor(h, h >>> 32) &&& @m64
  end

  defp rotl64(v, n), do: (v <<< n &&& @m64) ||| v >>> (64 - n)
end

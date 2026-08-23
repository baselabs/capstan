defmodule Capstan.ZstdTest do
  @moduledoc """
  Byte-exact conformance for `Capstan.Zstd.decompress/1` against frames whose
  oracle is always a REFERENCE implementation (RFC 8878) — never self-signed
  fixtures.

  Each MySQL-captured `zstd_*` directory pairs the zstd frame (`.zst`, sliced
  from the live TRANSACTION_PAYLOAD event of a `binlog_transaction_compression=ON`
  substrate) with `.inner` — the same frame inflated by the REFERENCE `zstd`
  binary at capture time. `zstd_text/` is reference-encoder-produced directly
  (see its README): MySQL row payloads are repetitive SQL whose literals never
  take the 4-stream Huffman form, so that frame carries the decoder arms the
  captured corpus cannot. A decoder bit-error anywhere (literals, FSE,
  Huffman, sequences, repeat offsets) cannot pass these.
  """

  use ExUnit.Case, async: true

  import Bitwise

  alias Capstan.Zstd
  alias Capstan.Zstd.Fse

  @fixtures Path.expand("../fixtures/binlog", __DIR__)

  # -- byte-exact conformance over the real captured battery --------------------

  test "every captured MySQL frame inflates to exactly the reference bytes" do
    frames =
      @fixtures
      |> Path.join("zstd_*/**/*.zst")
      |> Path.wildcard()
      |> Enum.sort()

    # The battery must not silently shrink to zero (an allowlist-gate
    # completeness guard: a wildcard matching nothing would vacuously pass).
    assert length(frames) >= 7

    for zst <- frames do
      frame = File.read!(zst)
      oracle = File.read!(String.replace_suffix(zst, ".zst", ".inner"))

      assert {:ok, out} = Zstd.decompress(frame), "frame failed: #{zst}"
      assert out == oracle, "byte mismatch against reference zstd: #{zst}"
    end
  end

  test "the multi-block ~1.2MB frame inflates fully (block loop + window)" do
    zst = Path.join(@fixtures, "zstd_large/06-transaction_payload.zst")
    oracle = File.read!(Path.join(@fixtures, "zstd_large/06-transaction_payload.inner"))
    assert byte_size(oracle) > 128 * 1024 * 4
    assert {:ok, ^oracle} = Zstd.decompress(File.read!(zst))
  end

  test "the diverse-text reference frame is multi-block with oversized literals sections" do
    # Non-degeneracy guard for the fixture the wildcard battery above decodes
    # byte-exact: the point of `zstd_text/` is output beyond one 128 KB block
    # and literals big enough for the 5-byte size format (sf=3) — the shapes
    # repetitive SQL never produces.
    oracle = File.read!(Path.join(@fixtures, "zstd_text/01-transaction_payload.inner"))
    assert byte_size(oracle) > 128 * 1024 * 2
  end

  # -- truncation: every prefix of a real frame is refused ----------------------

  test "every truncation point of a real MySQL frame is refused" do
    [zst | _] =
      @fixtures
      |> Path.join("zstd_rows/*.zst")
      |> Path.wildcard()
      |> Enum.sort()

    frame = File.read!(zst)

    for len <- 0..(byte_size(frame) - 1) do
      assert {:error, _reason} =
               Zstd.decompress(binary_part(frame, 0, len)),
             "prefix of length #{len} was not refused"
    end
  end

  test "every truncation point inside a 4-stream literals block is refused" do
    # The repetitive-SQL corpus truncates only through Raw/RLE literals; this
    # sweep drives truncation THROUGH a compressed literals section — the
    # Huffman header, the 4-stream jump table, and the sequences section of
    # the text frame's first block.
    frame = File.read!(Path.join(@fixtures, "zstd_text/01-transaction_payload.zst"))

    for len <- 0..2000 do
      assert {:error, _reason} =
               Zstd.decompress(binary_part(frame, 0, len)),
             "prefix of length #{len} was not refused"
    end
  end

  test "a frame that is magic alone is :truncated_frame, not :bad_magic" do
    assert {:error, :truncated_frame} = Zstd.decompress(<<0x28, 0xB5, 0x2F, 0xFD>>)
  end

  # -- crafted fail-closed arms (each a named corruption class) -----------------

  # magic + FHD 0x00 (no FCS, not single-segment) + Window_Descriptor 0x38
  # (128 KB window) — the bare header the rle_bomb builder also uses.
  defp bare_header, do: <<0x28, 0xB5, 0x2F, 0xFD, 0x00, 0x38>>

  defp block_header(type, size, last),
    do: <<size <<< 3 ||| type <<< 1 ||| ((last && 1) || 0)::24-little>>

  test "a reserved block type is refused" do
    frame = bare_header() <> block_header(3, 1, true) <> <<0x61>>

    assert {:error, :reserved_block_type} = Zstd.decompress(frame)
  end

  test "an RLE block regenerating beyond Block_Maximum_Size is refused" do
    # Block_Maximum_Size = min(window, 128 KB) = 128 KB here; 128 KB + 1 in one
    # block is corruption by construction (RFC 3.1.1.2.4).
    frame = bare_header() <> block_header(1, 128 * 1024 + 1, true) <> <<0x61>>

    assert {:error, :block_size_exceeded} = Zstd.decompress(frame)
  end

  test "a truncated RLE block (header present, content byte gone) is refused" do
    frame = bare_header() <> block_header(1, 8, true)

    assert {:error, :truncated_frame} = Zstd.decompress(frame)
  end

  test "a raw block whose declared size overruns the frame is refused" do
    frame = bare_header() <> block_header(0, 10, true) <> <<0x61, 0x62, 0x63>>

    assert {:error, :truncated_frame} = Zstd.decompress(frame)
  end

  test "a lying declared Frame_Content_Size is refused" do
    # Single-segment frame declaring 5 bytes of content, carrying 4.
    frame =
      <<0x28, 0xB5, 0x2F, 0xFD, 0xE0, 5::64-little>> <>
        block_header(0, 4, true) <> "abcd"

    assert {:error, :frame_content_size_mismatch} = Zstd.decompress(frame)
  end

  test "a skippable frame is skipped and the following frame decoded" do
    payload = bare_header() <> block_header(0, 4, true) <> "abcd"

    skip = <<0x50, 0x2A, 0x4D, 0x18, 3::32-little, "xyz">>
    assert {:ok, "abcd"} = Zstd.decompress(skip <> payload)

    # The skip size overruns the frame: corruption, never a silent partial skip.
    # (The frame must genuinely END inside the skip — with a trailing frame
    # appended, the skip would eat into it and the remnant fails :bad_magic.)
    short = <<0x50, 0x2A, 0x4D, 0x18, 9::32-little, "xyz">>
    assert {:error, :truncated_frame} = Zstd.decompress(short)
  end

  test "a treeless literals section with no previous Huffman table is refused" do
    # Literals type 3 (Treeless_Literals_Block) with sf=0 (3-byte size header),
    # regen 0 / comp 0, then a zero-sequence section — a VALID block shape
    # whose tree must come from a previous block in the same frame. First
    # block of the frame: there is no previous table (RFC 3.1.1.3.1.3).
    content = <<0x03, 0x00, 0x00, 0x00>>
    frame = bare_header() <> block_header(2, byte_size(content), true) <> content

    assert {:error, :no_previous_huffman_table} = Zstd.decompress(frame)
  end

  # -- crafted literals / sequences section arms ---------------------------------
  #
  # Compressed-block builders for the section shapes the real corpus never
  # truncates at exactly (RFC 3.1.1.3): `raw_sf0/1` wraps bytes as Raw
  # literals in the 1-bit size format; the sequences suffix is appended per
  # test so each arm ends the frame EXACTLY at the boundary it must refuse.

  defp raw_sf0(bytes), do: <<byte_size(bytes) <<< 3::8, bytes::binary>>

  defp comp_block(content),
    do: bare_header() <> block_header(2, byte_size(content), true) <> content

  test "a frame header missing its Window_Descriptor byte is refused" do
    assert {:error, :truncated_frame} = Zstd.decompress(<<0x28, 0xB5, 0x2F, 0xFD, 0x00>>)
  end

  test "a checksummed frame truncated before its digest is refused" do
    input = File.read!(Path.join(@fixtures, "../xxh64/vec_000100.bin"))
    frame = checksummed_frame(input, 0x0)
    undigested = binary_part(frame, 0, byte_size(frame) - 4)

    assert {:error, :truncated_frame} = Zstd.decompress(undigested)
  end

  test "an empty compressed block content is refused at the literals section" do
    assert {:error, :truncated_frame} = Zstd.decompress(bare_header() <> block_header(2, 0, true))
  end

  test "a 2-byte-size-format literals header with its second size byte gone" do
    # sf=1: regen sits in (b0 >>> 4) + (b1 <<< 4) — the frame ends at b0.
    assert {:error, :truncated_frame} = Zstd.decompress(comp_block(<<0x54>>))
  end

  test "raw literals whose declared regenerated size overruns the block" do
    content = <<5 <<< 3::8>> <> "abc" <> <<0>>

    assert {:error, :truncated_frame} = Zstd.decompress(comp_block(content))
  end

  test "RLE literals inflate to the repeated byte (sf=0 size format)" do
    # The SUCCESS arm of RLE literals — a valid compressed block whose only
    # content is one RLE literals section + a zero-sequence section.
    content = <<1 ||| 6 <<< 3::8, "z">> <> <<0>>

    assert {:ok, "zzzzzz"} = Zstd.decompress(comp_block(content))
  end

  test "RLE literals with the repeated byte missing is refused" do
    assert {:error, :truncated_frame} = Zstd.decompress(comp_block(<<1 ||| 6 <<< 3::8>>))
  end

  test "a Huffman literals size header shorter than its size format" do
    # sf=0 needs 3 bytes for the sizes; two is corruption.
    assert {:error, :truncated_frame} = Zstd.decompress(comp_block(<<0x02, 0x00>>))
  end

  test "a Huffman literals section whose compressed size overruns the block" do
    # b0 = 0x02 (type 2, sf 0); sizes encode regen=4, comp=10 — only 5 follow.
    content = <<0x02, 0x40, 0x80, 0x02, "abcde">>

    assert {:error, :truncated_frame} = Zstd.decompress(comp_block(content))
  end

  test "a Huffman literals section with a garbage tree description is refused" do
    # Sizes encode regen=4, comp=3; the 3 "tree" bytes are not a valid
    # description — the tree read itself must fail closed.
    content = <<0x02, 0x40, 0xC0, 0x00, 0xFF, 0xFF, 0xFF>>

    assert {:error, _reason} = Zstd.decompress(comp_block(content))
  end

  test "a compressed block with literals but no sequences section byte" do
    assert {:error, :truncated_frame} = Zstd.decompress(comp_block(raw_sf0("abcd")))
  end

  test "a nonzero sequence count with no bitstream at all is refused" do
    # n=1, all tables predefined (modes 0) — the bitstream is absent.
    assert {:error, _reason} = Zstd.decompress(comp_block(raw_sf0("abcd") <> <<1, 0>>))
  end

  test "a 3-byte sequence count with its second length byte gone" do
    assert {:error, :truncated_frame} = Zstd.decompress(comp_block(raw_sf0("abcd") <> <<255>>))
  end

  test "a 3-byte sequence count with only one of its two length bytes" do
    assert {:error, :truncated_frame} =
             Zstd.decompress(comp_block(raw_sf0("abcd") <> <<255, 1>>))
  end

  test "a sequence count with no Symbol_Compression_Modes byte" do
    assert {:error, :truncated_frame} = Zstd.decompress(comp_block(raw_sf0("abcd") <> <<1>>))
  end

  test "an RLE-mode sequence table with no description byte" do
    # modes 0x40: Literals_Length table in RLE_Mode — the symbol byte is absent.
    assert {:error, :truncated_frame} =
             Zstd.decompress(comp_block(raw_sf0("abcd") <> <<1, 0x40>>))
  end

  test "an FSE-mode sequence table with a garbage description is refused" do
    # modes 0x80: Literals_Length table in FSE_Mode — 0xFF is not a valid
    # accuracy log.
    assert {:error, _reason} =
             Zstd.decompress(comp_block(raw_sf0("abcd") <> <<1, 0x80, 0xFF>>))
  end

  test "a repeat-mode sequence table with no previous table is refused" do
    # modes 0xC0: Literals_Length table in Repeat_Mode — first block of the
    # frame has nothing to repeat.
    assert {:error, :no_previous_fse_table} =
             Zstd.decompress(comp_block(raw_sf0("abcd") <> <<1, 0xC0>>))
  end

  # -- crafted VALID frames: the encoder shapes the reference CLI never emits --
  #
  # A test-side encoder for the sequences section (RFC 8878 §3.1.1.3.2.1.2),
  # bit-for-bit the mirror of the production BitReader.Reverse: values are
  # written MSB-first in read order; the byte stream carries the earliest bits
  # in the LAST byte below the pad-`1` flag, then fills earlier bytes
  # backwards. The default-table baselines/extras are transcribed from RFC
  # Tables 16-17 (fetched first-hand from rfc-editor.org this session).

  @ll_baseline_length 36
  @ll_extra_length 36

  defp ll_baseline,
    do:
      Enum.to_list(0..15) ++
        [
          16,
          18,
          20,
          22,
          24,
          28,
          32,
          40,
          48,
          64,
          128,
          256,
          512,
          1024,
          2048,
          4096,
          8192,
          16_384,
          32_768,
          65_536
        ]

  defp ll_extra,
    do:
      List.duplicate(0, 16) ++ [1, 1, 1, 1, 2, 2, 3, 3, 4, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]

  @ml_baseline_length 53
  @ml_extra_length 53

  defp ml_baseline,
    do:
      Enum.map(0..31, &(&1 + 3)) ++
        [
          35,
          37,
          39,
          41,
          43,
          47,
          51,
          59,
          67,
          83,
          99,
          131,
          259,
          515,
          1027,
          2051,
          4099,
          8195,
          16_387,
          32_771,
          65_539
        ]

  defp ml_extra,
    do:
      List.duplicate(0, 32) ++
        [1, 1, 1, 1, 2, 2, 3, 3, 4, 4, 5, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]

  # n bits of a value, MSB first (exactly what read_exact consumes).
  defp bits_of(_v, 0), do: []

  defp bits_of(v, n), do: [v >>> (n - 1) &&& 1 | bits_of(v, n - 1)]

  # Read-order bits -> the byte stream the Reverse reader parses: the last
  # byte is the flag `1` at bit position s with the FIRST s bits below it,
  # remaining bits fill earlier bytes from the end, MSB-first within bytes.
  defp rev_bitstream(bits) do
    s = rem(length(bits), 8)
    {head, body} = Enum.split(bits, s)
    head_value = Enum.reduce(head, 0, fn b, acc -> acc <<< 1 ||| b end)
    last_byte = head_value ||| 1 <<< s

    body_ints =
      body
      |> Enum.chunk_every(8)
      |> Enum.map(&Enum.reduce(&1, 0, fn b, acc -> acc <<< 1 ||| b end))

    :erlang.list_to_binary(Enum.reverse(body_ints) ++ [last_byte])
  end

  defp predef_tables do
    {:ok, ll} = Fse.build(ll_default(), 6)
    {:ok, of} = Fse.build(of_default(), 5)
    {:ok, ml} = Fse.build(ml_default(), 6)
    %{ll: ll, of: of, ml: ml}
  end

  # The bits of one sequence's reads, in production read order:
  # offset extra, then match extra, then literals extra.
  defp seq_read_bits(ov, ml, ll, of_sym, ml_sym, ll_sym) do
    bits_of(ov - (1 <<< of_sym), of_sym) ++
      bits_of(ml - Enum.at(ml_baseline(), ml_sym), Enum.at(ml_extra(), ml_sym)) ++
      bits_of(ll - Enum.at(ll_baseline(), ll_sym), Enum.at(ll_extra(), ll_sym))
  end

  # A state with the wanted symbol, and whether target t is reachable from s
  # (advance writes t - base in nb bits — the production advance's inverse).
  defp find_state(cells, sym) do
    cells |> Enum.find(fn {_st, {s, _, _}} -> s == sym end) |> elem(0)
  end

  defp reachable?(cells, s, t) do
    {_, nb, base} = cells[s]
    diff = t - base
    diff >= 0 and diff < 1 <<< nb
  end

  defp sym_a(cells, state), do: elem(cells[state], 0)

  # The expected-output mirror of the production match copy: literals, then a
  # back-reference copy that repeats while overlapping (RFC 3.1.1.4).
  defp match_copy(out, offset, ml) do
    start = byte_size(out) - offset
    chunk = binary_part(out, start, offset)

    copied =
      if ml <= offset,
        do: binary_part(chunk, 0, ml),
        else: :binary.copy(chunk, div(ml, offset)) <> binary_part(chunk, 0, rem(ml, offset))

    out <> copied
  end

  test "the transcribed baseline/extra tables match the RFC's declared lengths" do
    # Hand-typed tables have been transcribed with a wrong count twice
    # before — the length tripwires make that loud instead of silent.
    assert length(ll_baseline()) == @ll_baseline_length
    assert length(ll_extra()) == @ll_extra_length
    assert length(ml_baseline()) == @ml_baseline_length
    assert length(ml_extra()) == @ml_extra_length
    # Spot rows from RFC Tables 16-17 at the 1-extra-bit boundaries.
    assert Enum.at(ll_baseline(), 16) == 16 and Enum.at(ll_extra(), 16) == 1
    assert Enum.at(ll_baseline(), 35) == 65_536 and Enum.at(ll_extra(), 35) == 16
    assert Enum.at(ml_baseline(), 32) == 35 and Enum.at(ml_extra(), 32) == 1
    assert Enum.at(ml_baseline(), 52) == 65_539 and Enum.at(ml_extra(), 52) == 16
  end

  describe "crafted VALID sequences sections — RLE tables" do
    # modes 0x54: all three tables in RLE_Mode; symbols ll=4 (ll=4, 0 extra
    # bits), of=1 (1 extra bit: ov = 2 + bit), ml=0 (ml=3, 0 extra bits).
    # No init or advance bits exist for RLE tables (their table log is 0) —
    # the bitstream is ONLY the per-sequence offset-extra bits.
    test "an all-RLE sequences section inflates a two-sequence block exactly" do
      literals = "abcdefgh"
      seqs = [{4, 2, 3}, {4, 3, 3}]

      bits =
        Enum.map(seqs, fn {_ll, ov, _ml} -> bits_of(ov - 2, 1) end) |> List.flatten()

      content =
        raw_sf0(literals) <>
          <<2, 0x54, 4, 1, 0>> <> rev_bitstream(bits)

      # Expected output by the mirror: arm1 (ov=2, ll≠0) takes r2=4; arm2
      # (ov=3, ll≠0) takes r3=8 after the reps shift.
      expected =
        match_copy(match_copy("abcd", 4, 3) |> Kernel.<>("efgh"), 8, 3)

      assert {:ok, ^expected} = Zstd.decompress(comp_block(content))
    end

    test "the 3-byte sequence count form (n >= 0x7F00) inflates exactly" do
      # n = 0x7F00 + 1: every sequence is ll=1 (RLE ll sym 1), ov=1 (RLE of
      # sym 0, arm0: repeat r1), ml=3 (RLE ml sym 0) — one literal byte then
      # three copies of it. ZERO read bits per sequence: the bitstream is the
      # flag byte alone.
      n = 0x7F00 + 1
      literals = for i <- 1..n, do: <<i &&& 0xFF>>
      literals_bin = IO.iodata_to_binary(literals)

      # Raw literals need the 3-byte size format for regen > 4095.
      lit_section =
        <<0x0C ||| (n &&& 0xF) <<< 4, n >>> 4 &&& 0xFF, n >>> 12, literals_bin::binary>>

      content = lit_section <> <<0xFF, 1, 0, 0x54, 1, 0, 0>> <> <<1>>

      expected =
        literals |> Enum.map(fn <<b>> -> :binary.copy(<<b>>, 4) end) |> IO.iodata_to_binary()

      assert byte_size(expected) == n * 4
      assert {:ok, ^expected} = Zstd.decompress(comp_block(content))
    end
  end

  describe "crafted VALID sequences sections — predefined + repeat tables" do
    test "a second block in Repeat_Mode reuses the first block's tables" do
      t = predef_tables()
      s_ll = find_state(t.ll, 4)
      s_of = find_state(t.of, 1)
      s_ml = find_state(t.ml, 0)

      init_bits = bits_of(s_ll, 6) ++ bits_of(s_of, 5) ++ bits_of(s_ml, 6)
      # n=1: no advance bits (the last sequence never advances).
      block1 = raw_sf0("abcdef") <> <<1, 0>> <> rev_bitstream(init_bits ++ bits_of(0, 1))
      block2 = raw_sf0("ghijkl") <> <<1, 0xFC>> <> rev_bitstream(init_bits ++ bits_of(0, 1))

      frame =
        bare_header() <>
          block_header(2, byte_size(block1), false) <>
          block1 <>
          block_header(2, byte_size(block2), true) <> block2

      # Block1: literals "abcd", ov=2 (arm1: r2=4), ml=3, leftover "ef".
      out1 = match_copy("abcd", 4, 3) <> "ef"
      # Block2 continues the frame's reps {4, 1, 8}: literals "ghij", ov=2
      # (arm1: r2=1 — the shifted r2), ml=3, leftover "kl".
      out2 = match_copy(out1 <> "ghij", 1, 3) <> "kl"

      assert {:ok, ^out2} = Zstd.decompress(frame)
    end

    test "a state advance past the bitstream's end is refused (:bitstream_exhausted)" do
      # n=2 with predefined tables: init (6+5+6 bits) + zero extra bits for
      # the first sequence; the advance's next-state read has nothing left.
      t = predef_tables()

      init_bits =
        bits_of(find_state(t.ll, 4), 6) ++
          bits_of(find_state(t.of, 0), 5) ++ bits_of(find_state(t.ml, 0), 6)

      content = raw_sf0("abcd") <> <<2, 0>> <> rev_bitstream(init_bits)

      assert {:error, :bitstream_exhausted} = Zstd.decompress(comp_block(content))
    end

    test "ll==0 repeat-offset arms resolve exactly (ov=2 -> r3, ov=3 -> r1-1)" do
      # ll table PREDEFINED (states walked ll=4 then ll=0 twice); of/ml RLE:
      # of sym 1 (1 extra bit: ov 2 or 3), ml sym 5 (ml=8, 0 extra bits).
      t = predef_tables()

      # Find ll states: sym 4 first, then two reachable sym-0 states in a row.
      {s_a, s_z, s_y} =
        Enum.find_value(
          for s_a <- 0..63, sym_a(t.ll, s_a) == 4 do
            s_a
          end,
          fn s_a ->
            Enum.find_value(
              for s_z <- 0..63, sym_a(t.ll, s_z) == 0, reachable?(t.ll, s_a, s_z) do
                s_z
              end,
              fn s_z ->
                Enum.find_value(
                  for s_y <- 0..63, sym_a(t.ll, s_y) == 0, reachable?(t.ll, s_z, s_y) do
                    s_y
                  end,
                  fn s_y -> {s_a, s_z, s_y} end
                )
              end
            )
          end
        )

      {_, ll_nb, ll_base} = t.ll[s_a]
      {_, z_nb, z_base} = t.ll[s_z]

      bits =
        bits_of(s_a, 6) ++
          seq_read_bits(2, 8, 4, 1, 5, 4) ++
          bits_of(s_z - ll_base, ll_nb) ++
          seq_read_bits(2, 8, 0, 1, 5, 0) ++
          bits_of(s_y - z_base, z_nb) ++
          seq_read_bits(3, 8, 0, 1, 5, 0)

      content = raw_sf0("wxyz") <> <<3, 0x14, 1, 5>> <> rev_bitstream(bits)

      # Mirror: seq1 (ll=4, ov=2, ml=8) arm1 r2=4; seq2 (ll=0, ov=2) arm2
      # r3=8; seq3 (ll=0, ov=3) arm3 r1-1 = 7.
      out = match_copy("wxyz", 4, 8)
      out = match_copy(out, 8, 8)
      out = match_copy(out, 7, 8)

      assert {:ok, ^out} = Zstd.decompress(comp_block(content))
    end
  end

  # -- crafted Huffman jump-table arms (4-stream literals, direct-weight tree) --

  # A minimal direct-weight tree (RFC 4.2.1.1): header 129 = 2 weight nibbles;
  # weights 1,1 complete with the deduced last weight. Two bytes total.
  defp direct_tree, do: <<129, 0x11>>

  # The 4-byte Huffman size header (sf=2): the 32-bit value the decoder reads
  # INCLUDES b0, whose high nibble is the low 4 bits of regen — the sizes sit
  # LSB-after the 4 flag bits (RFC 3.1.1.3.1.1).
  defp huff_sizes(regen, comp),
    do: <<regen <<< 4 ||| comp <<< 18 ||| 0x0A::32-little>>

  test "a 4-stream literals section with a negative Stream4 size is refused" do
    # regen=1 makes last = regen - 3*seg = -2 < 0 — the jump table cannot
    # describe it. The tree is valid; the SECTION sizes are the corruption.
    content = huff_sizes(1, 8) <> direct_tree() <> <<0, 0, 0, 0, 0, 0>>

    assert {:error, :corrupt_jump_table} = Zstd.decompress(comp_block(content))
  end

  test "a 4-stream literals section with a truncated jump table is refused" do
    # comp counts 5 bytes: tree(2) + only 3 of the 6 jump-table bytes.
    content = huff_sizes(1, 5) <> direct_tree() <> <<0, 0, 0>>

    assert {:error, :truncated_frame} = Zstd.decompress(comp_block(content))
  end

  test "an invalid Huffman weight description is refused at the tree read" do
    # Single-stream sizes (sf=0): the 24-bit value includes b0 — regen=0,
    # comp=2; the "tree" opens an FSE-coded weight description of 5 bytes but
    # only 1 follows.
    content = <<0x02, (0x02 ||| 2 <<< 14) >>> 8::16-little>> <> <<5, 1>>

    assert {:error, _} = Zstd.decompress(comp_block(content))
  end

  # -- fail-closed tripwires (protected mutations proven RED) -------------------

  test "a tampered compressed byte is refused, never silently mis-decoded" do
    [zst | _] =
      @fixtures
      |> Path.join("zstd_rows/*.zst")
      |> Path.wildcard()
      |> Enum.sort()

    frame = File.read!(zst)

    # Tamper one bit inside the block content (past the 6-byte
    # magic+FHD+FCS prologue of this single-segment frame).
    <<pre::binary-size(8), byte::8, rest::binary>> = frame
    tampered = <<pre::binary, byte |> bxor(0x40), rest::binary>>

    assert {:error, _reason} = Zstd.decompress(tampered)
  end

  test "a truncated frame is refused" do
    [zst | _] =
      @fixtures
      |> Path.join("zstd_rows/*.zst")
      |> Path.wildcard()
      |> Enum.sort()

    frame = File.read!(zst)
    <<truncated::binary-size(40), _::binary>> = frame
    assert {:error, _reason} = Zstd.decompress(truncated)
  end

  test "a reserved frame-header bit is refused" do
    [zst | _] =
      @fixtures
      |> Path.join("zstd_rows/*.zst")
      |> Path.wildcard()
      |> Enum.sort()

    <<magic::binary-size(4), fhd::8, rest::binary>> = File.read!(zst)
    # bit 3 is reserved and must be zero; setting it must fail closed.
    assert {:error, :reserved_bit_set} =
             Zstd.decompress(<<magic::binary, fhd ||| 0x08, rest::binary>>)
  end

  test "a dictionary ID is refused (no dictionary support)" do
    [zst | _] =
      @fixtures
      |> Path.join("zstd_rows/*.zst")
      |> Path.wildcard()
      |> Enum.sort()

    <<magic::binary-size(4), fhd::8, rest::binary>> = File.read!(zst)
    # DID flag 1 => 1-byte dictionary ID inserted after the header fields.
    did_fhd = (fhd &&& 0xFC) ||| 0x01

    assert {:error, :dictionary_unsupported} =
             Zstd.decompress(<<magic::binary, did_fhd, 0x2A, rest::binary>>)
  end

  test "a bad magic number is refused" do
    assert {:error, :bad_magic} = Zstd.decompress(<<0, 0, 0, 0, 1, 2, 3>>)
    assert {:error, :bad_magic} = Zstd.decompress(<<>>)
  end

  # -- XXH64 conformance: oracle digests through the production verify path -------------

  describe "content-checksum verification against the independent-oracle vectors" do
    @vectors Path.expand("../fixtures/xxh64", __DIR__)

    test "every oracle-vector input verifies and round-trips in a checksummed frame" do
      # MySQL never emits checksummed frames, so the oracle for this path is the
      # committed vector table: inputs + digests computed by an INDEPENDENT
      # spec-derived C implementation (provenance: test/fixtures/xxh64/README.md).
      # Each input is wrapped in a single-segment raw-block frame carrying the
      # ORACLE digest — decompress must verify (capstan's XXH64 == the oracle's,
      # through the real verification code path) and return the input bytes.
      digests =
        @vectors
        |> Path.join("digests.txt")
        |> File.read!()
        |> String.split("\n", trim: true)
        |> Enum.map(&String.split/1)

      assert length(digests) >= 39

      for [name, digest_hex] <- digests do
        input = File.read!(Path.join(@vectors, name))
        digest = String.to_integer(digest_hex, 16)
        frame = checksummed_frame(input, digest)

        assert {:ok, ^input} = Zstd.decompress(frame), "checksum verify failed: #{name}"
      end
    end

    test "a frame whose checksum is NOT the oracle digest is refused" do
      # Tripwire: a flipped checksum bit must fail the verification, never
      # decompress silently (the entire point of verifying the field).
      input = File.read!(Path.join(@vectors, "vec_000100.bin"))
      digest = String.to_integer("ef1ede14bf111783", 16)

      assert {:ok, ^input} = Zstd.decompress(checksummed_frame(input, digest))

      flipped = bxor(digest, 1)

      assert {:error, :content_checksum_mismatch} =
               Zstd.decompress(checksummed_frame(input, flipped))
    end
  end

  # A minimal single-segment raw-block frame with the content-checksum flag set:
  # magic + FHD(fcs=8B, single-segment, checksum) + u64 size + raw block + u32 LE
  # low-32 of the digest.
  defp checksummed_frame(input, digest) do
    fhd = 0b1100_0000 ||| 0b0010_0000 ||| 0b0000_0100
    block_header = <<1 ||| 0 <<< 1 ||| byte_size(input) <<< 3::24-little>>

    <<0x28, 0xB5, 0x2F, 0xFD, fhd, byte_size(input)::64-little, block_header::binary,
      input::binary, digest::32-little>>
  end

  # -- RFC 8878 Appendix A crosschecks (doc-derived, read first-hand) -----------

  # The three predefined distributions (RFC §3.1.1.3.2.2), built structurally
  # with length tripwires — a hand-typed literal has twice been transcribed
  # with a wrong count here; these make that loud instead of silent.
  @ll_default_length 36

  defp ll_default,
    do:
      [4, 3, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1] ++
        [2, 2, 2, 2, 2, 2, 2, 2, 2, 3, 2, 1, 1, 1, 1, 1] ++ List.duplicate(-1, 4)

  @ml_default_length 53

  defp ml_default,
    do: [1, 4, 3, 2, 2, 2, 2, 2, 2] ++ List.duplicate(1, 37) ++ List.duplicate(-1, 7)

  @of_default_length 29

  defp of_default,
    do: [1, 1, 1, 1, 1, 1, 2, 2, 2] ++ List.duplicate(1, 15) ++ List.duplicate(-1, 5)

  test "predefined distributions match the RFC's declared shapes" do
    assert length(ll_default()) == @ll_default_length
    assert length(ml_default()) == @ml_default_length
    assert length(of_default()) == @of_default_length
    # The distributions must exactly fill their tables (lowprob counts as 1).
    assert Enum.sum(Enum.map(ll_default(), &abs/1)) == 64
    assert Enum.sum(Enum.map(ml_default(), &abs/1)) == 64
    assert Enum.sum(Enum.map(of_default(), &abs/1)) == 32
  end

  test "predefined literals-length table matches RFC Appendix A samples" do
    {:ok, cells} = Fse.build(ll_default(), 6)

    # Sampled rows of RFC 8878 Table 28 (state -> {symbol, nb_bits, baseline}).
    assert cells[0] == {0, 4, 0}
    assert cells[2] == {1, 5, 32}
    assert cells[3] == {3, 5, 0}
    assert cells[17] == {25, 5, 32}
    assert cells[31] == {13, 6, 0}
    assert cells[60] == {35, 6, 0}
    assert cells[63] == {32, 6, 0}
  end

  test "predefined match-length table matches RFC Appendix A samples" do
    {:ok, cells} = Fse.build(ml_default(), 6)

    # Sampled rows of RFC 8878 Table 29.
    assert cells[0] == {0, 6, 0}
    assert cells[1] == {1, 4, 0}
    assert cells[2] == {2, 5, 32}
    assert cells[22] == {1, 4, 16}
    assert cells[57] == {52, 6, 0}
    assert cells[63] == {46, 6, 0}
  end

  test "predefined offset table matches RFC Appendix A samples" do
    {:ok, cells} = Fse.build(of_default(), 5)

    # Sampled rows of RFC 8878 Table 30.
    assert cells[0] == {0, 5, 0}
    assert cells[1] == {6, 4, 0}
    assert cells[15] == {7, 4, 16}
    assert cells[27] == {28, 5, 0}
    assert cells[31] == {24, 5, 0}
  end

  ## ===========================================================================
  ## The output cap (span review, blocking — the decompression bomb)
  ##
  ## A valid frame with NO content-size TLV and thousands of <=128 KB RLE blocks
  ## inflates a tiny input toward tens of GB as ONE BEAM binary — every post-hoc
  ## size check fires only after the memory is spent. The cap is enforced DURING
  ## inflation (the block loop), before the output materializes.
  ## ===========================================================================

  describe "decompress/2 max_output_bytes — the decompression bomb bound" do
    # A hand-built frame: magic + FHD 0x00 (no FCS, no checksum, not single-segment) +
    # Window_Descriptor 0x38 (128 KB window) + N RLE blocks (3-byte header + 1 content
    # byte; each regenerates 128 KB) with the last flag on the final block.
    defp rle_bomb(n_blocks) do
      header = <<0x28, 0xB5, 0x2F, 0xFD, 0x00, 0x38>>

      blocks =
        for i <- 1..n_blocks do
          last = if i == n_blocks, do: 1, else: 0
          h = (128 * 1024) <<< 3 ||| 1 <<< 1 ||| last
          <<h::24-little, 0x61>>
        end

      header <> IO.iodata_to_binary(blocks)
    end

    test "an RLE-bomb frame under a small cap fails :output_too_large, fast" do
      # 200 blocks x 128 KB = ~25 MB output from a ~1.4 KB input; the 1 MB cap must trip
      # in the block loop. RED (pre-fix): no cap existed — this returned {:ok, 25 MB}.
      assert {:error, :output_too_large} =
               Zstd.decompress(rle_bomb(200), max_output_bytes: 1024 * 1024)
    end

    test "a frame within the cap still inflates exactly" do
      bomb = rle_bomb(4)
      assert {:ok, out} = Zstd.decompress(bomb, max_output_bytes: 1024 * 1024)
      assert byte_size(out) == 4 * 128 * 1024
      assert out == String.duplicate("a", 4 * 128 * 1024)
    end

    test "the uncapped arity is unchanged (the generic utility form)" do
      assert {:ok, out} = Zstd.decompress(rle_bomb(2))
      assert byte_size(out) == 2 * 128 * 1024
    end

    test "a bogus cap is refused, never silently uncapped" do
      assert {:error, :bad_output_cap} = Zstd.decompress(rle_bomb(1), max_output_bytes: 0)
    end
  end
end

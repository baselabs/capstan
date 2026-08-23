defmodule Capstan.ZstdTest do
  @moduledoc """
  Byte-exact conformance for `Capstan.Zstd.decompress/1` against REAL
  MySQL-produced frames (RFC 8878; `binlog_transaction_compression=ON`
  substrate), never self-signed fixtures.

  Each `zstd_*` fixture directory pairs the captured zstd frame (`.zst`,
  sliced from the live TRANSACTION_PAYLOAD event) with `.inner` — the same
  frame inflated by the REFERENCE `zstd` binary at capture time. A decoder
  bit-error anywhere (literals, FSE, Huffman, sequences, repeat offsets)
  cannot pass these.
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
end

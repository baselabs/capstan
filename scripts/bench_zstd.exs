# capstan zstd decoder benchmark — the three input regimes that matter.
#
#   MIX_ENV=test mix run scripts/bench_zstd.exs
#
# Regimes (measured 2026-08-23, M-series arm64, best of 5):
#   - REAL binlog payloads (the committed fixtures): 0.1–0.4ms per transaction
#     payload — including the 1.18MB multi-block frame (≈2.6 GB/s output; its
#     entropy is low: long matches and literal runs dominate).
#   - Random input: zstd stores it as RAW blocks — no bit-reading at all.
#   - ENTROPY-DENSE input (skewed bytes, every symbol through the Huffman/FSE
#     bit readers): ≈1.35 MB/s output. This is the decoder's worst case and
#     the number that bounds a source that stores compressed/encrypted BLOBs.
# A word-at-a-time reader rewrite is the named optimization if that worst case
# must close; the byte-exact conformance suite (test/capstan/zstd_test.exs) is
# its backstop.

alias Capstan.Zstd

defmodule Capstan.BenchZstd do
  # Weighted byte: 40/296 on 0, uniform otherwise — dense-enough entropy to
  # force Huffman/FSE coding, compressible enough not to become a RAW block.
  defp skewed_byte do
    if :rand.uniform(296) <= 40, do: 0, else: :rand.uniform(255) - 1
  end

  def skew_input(n) do
    :rand.seed(:exsss, {3, 7, 11})
    for(_ <- 1..n, into: <<>>, do: <<skewed_byte()>>)
  end
end

bench = fn label, frame ->
  {:ok, out} = Zstd.decompress(frame)
  {_, _} = Zstd.decompress(frame)

  times =
    for _ <- 1..5 do
      {us, _} = :timer.tc(fn -> Zstd.decompress(frame) end)
      us / 1000
    end

  best = Enum.min(times)
  mbps = byte_size(out) / 1024 / 1024 / (best / 1000)
  IO.puts("#{label}: #{Float.round(best, 1)}ms (#{Float.round(mbps, 2)} MB/s output)")
end

Enum.each(
  [
    {"real: zstd_small (231B out)", "test/fixtures/binlog/zstd_small/06-transaction_payload.zst"},
    {"real: zstd_rows (772B out)", "test/fixtures/binlog/zstd_rows/06-transaction_payload.zst"},
    {"real: zstd_large (1.18MB out)",
     "test/fixtures/binlog/zstd_large/06-transaction_payload.zst"}
  ],
  fn {label, path} -> bench.(label, File.read!(path)) end
)

skew = Path.join(System.tmp_dir(), "capstan-bench-skew.bin")
File.write!(skew, Capstan.BenchZstd.skew_input(400_000))
System.cmd("zstd", ["-19", "-q", "--no-check", "-f", "-o", skew <> ".zst", skew])
bench.("dense: skewed 400KB", File.read!(skew <> ".zst"))
File.rm!(skew)
File.rm!(skew <> ".zst")

defmodule Capstan.GtidPropertyTest do
  @moduledoc """
  Property-based laws for the Gtid set algebra — the dedup/position core.

  The real-byte conformance fixtures prove decode SHAPE; these laws prove the ALGEBRA
  over the whole input space: canonical-form existence (parse is a fixed point of
  render), an independent membership oracle (a naive test-side interval model, not
  Gtid re-consulting itself), union commutativity/coverage, subtract exclusion, and the
  subset/disjoint dualities. A violation of any law is a silent-loss bug class
  (dedup by set membership, ADR-0001).
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Capstan.Gtid

  ## ---------------------------------------------------------------------------
  ## generators
  ## ---------------------------------------------------------------------------

  defp hex_digits(n) do
    StreamData.map(StreamData.list_of(StreamData.integer(0..15), length: n), fn digits ->
      digits |> Enum.map(&Integer.to_string(&1, 16)) |> Enum.join()
    end)
  end

  defp uuid_gen() do
    StreamData.map(
      {hex_digits(8), hex_digits(4), hex_digits(4), hex_digits(4), hex_digits(12)},
      fn {a, b, c, d, e} -> "#{a}-#{b}-#{c}-#{d}-#{e}" end
    )
  end

  @max_gno 500

  defp interval_gen() do
    StreamData.map({StreamData.integer(1..@max_gno), StreamData.integer(0..60)}, fn {lo, span} ->
      {lo, lo + span}
    end)
  end

  # A RAW (unnormalized) set: overlapping, adjacent, and duplicate intervals per uuid —
  # exactly the shapes normalization must coalesce.
  defp raw_set_gen() do
    StreamData.map_of(
      uuid_gen(),
      StreamData.list_of(interval_gen(), min_length: 1, max_length: 5), max_tries: 20)
  end

  defp render_raw(raw) do
    raw
    |> Enum.map(fn {uuid, intervals} ->
      uuid <> ":" <> Enum.map_join(intervals, ":", fn {lo, high} -> "#{lo}-#{high}" end)
    end)
    |> Enum.join(",")
  end

  # The independent membership oracle: every {uuid, gno} the RAW intervals cover.
  defp oracle_members(raw) do
    MapSet.new(
      for {uuid, intervals} <- raw,
          {lo, high} <- intervals,
          gno <- lo..high,
          do: {uuid, gno}
    )
  end

  # Boundary gnos around every interval edge — the off-by-one-sensitive points.
  defp boundary_gnos(raw) do
    for {uuid, intervals} <- raw,
        {lo, high} <- intervals,
        gno <- [max(lo - 1, 1), lo, high, high + 1],
        do: {uuid, gno}
  end

  ## ---------------------------------------------------------------------------
  ## laws
  ## ---------------------------------------------------------------------------

  property "parse/1 reaches a canonical form — render ∘ parse is a fixed point" do
    check all(raw <- raw_set_gen()) do
      parsed = raw |> render_raw() |> Gtid.parse()
      canonical = parsed |> Gtid.render() |> Gtid.parse()
      assert canonical == parsed
      assert canonical |> Gtid.render() |> Gtid.parse() == canonical
    end
  end

  property "member?/2 matches an independent interval oracle at every interval boundary" do
    check all(raw <- raw_set_gen()) do
      parsed = raw |> render_raw() |> Gtid.parse()
      members = oracle_members(raw)

      for {uuid, gno} <- boundary_gnos(raw) do
        assert Gtid.member?(parsed, {uuid, gno}) == MapSet.member?(members, {uuid, gno})
      end
    end
  end

  property "union/2 commutes (same canonical form both ways)" do
    check all(raw_a <- raw_set_gen(), raw_b <- raw_set_gen(), max_runs: 50) do
      a = raw_a |> render_raw() |> Gtid.parse()
      b = raw_b |> render_raw() |> Gtid.parse()
      assert Gtid.render(Gtid.union(a, b)) == Gtid.render(Gtid.union(b, a))
    end
  end

  property "union/2 covers both operands (a member of a is a member of a ∪ b)" do
    check all(raw_a <- raw_set_gen(), raw_b <- raw_set_gen(), max_runs: 50) do
      a = raw_a |> render_raw() |> Gtid.parse()
      union = Gtid.union(a, raw_b |> render_raw() |> Gtid.parse())

      for {uuid, gno} <- boundary_gnos(raw_a) do
        if Gtid.member?(a, {uuid, gno}) do
          assert Gtid.member?(union, {uuid, gno})
        end
      end
    end
  end

  property "subtract/2 excludes every member of the subtracted set" do
    check all(raw_a <- raw_set_gen(), raw_b <- raw_set_gen(), max_runs: 50) do
      a = raw_a |> render_raw() |> Gtid.parse()
      b = raw_b |> render_raw() |> Gtid.parse()
      difference = Gtid.subtract(a, b)

      for {uuid, gno} <- boundary_gnos(raw_b) do
        if Gtid.member?(b, {uuid, gno}) do
          refute Gtid.member?(difference, {uuid, gno})
        end
      end

      assert Gtid.disjoint?(difference, b)
    end
  end

  property "subset?/2 is subtract-emptiness, disjoint?/2 is subtract-is-a" do
    check all(raw_a <- raw_set_gen(), raw_b <- raw_set_gen(), max_runs: 50) do
      a = raw_a |> render_raw() |> Gtid.parse()
      b = raw_b |> render_raw() |> Gtid.parse()

      assert Gtid.subset?(a, b) == (Gtid.subtract(a, b) == %{})
      assert Gtid.disjoint?(a, b) == (Gtid.subtract(a, b) |> Gtid.render() == Gtid.render(a))
    end
  end

  property "sources/1 is the sorted, deduplicated uuid list of the raw set" do
    check all(raw <- raw_set_gen()) do
      parsed = raw |> render_raw() |> Gtid.parse()
      sources = parsed |> Gtid.sources() |> Enum.map(&elem(&1, 0))

      canonical =
        raw |> Map.keys() |> Enum.map(&String.downcase/1) |> Enum.sort()

      assert sources == canonical
    end
  end
end

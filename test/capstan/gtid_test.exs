defmodule Capstan.GtidTest do
  use ExUnit.Case, async: true

  alias Capstan.Gtid

  # Two real 36-char hyphenated UUIDs. @u2 < @u1 lexicographically, so the
  # canonical (sorted) render puts @u2 first — the multi-source order tests
  # depend on that.
  @u1 "8d7c06f2-8460-11f1-9dc3-56461013e0e2"
  @u2 "3e11fa47-71ca-11e1-9e33-c80aa9429562"

  describe "parse/1 |> render/1 round-trip" do
    test "canonical strings are stable under parse |> render" do
      canonical = [
        "",
        "#{@u1}:1-11",
        "#{@u1}:1-3:7:12-20",
        "#{@u1}:7",
        "#{@u1}:1:3:5",
        "#{@u2}:1-3,#{@u1}:1-5"
      ]

      for s <- canonical do
        assert s |> Gtid.parse() |> Gtid.render() == s
      end
    end

    test "parse |> render is idempotent on non-canonical input" do
      messy = "#{@u1}:12-20:1-3:7"
      once = messy |> Gtid.parse() |> Gtid.render()
      twice = once |> Gtid.parse() |> Gtid.render()
      assert once == twice
    end

    test "intervals are sorted and coalesced regardless of input order" do
      assert "#{@u1}:12-20:1-3:7" |> Gtid.parse() |> Gtid.render() ==
               "#{@u1}:1-3:7:12-20"
    end

    test "the UUID is normalised to lower case" do
      assert "#{@u1}:1-5" |> String.upcase() |> Gtid.parse() |> Gtid.render() ==
               "#{@u1}:1-5"
    end

    test "a single-element range N-N renders as N" do
      assert "#{@u1}:7-7" |> Gtid.parse() |> Gtid.render() == "#{@u1}:7"
    end

    test "multi-UUID output is ordered lexicographically by UUID" do
      assert "#{@u1}:1-5,#{@u2}:1-3" |> Gtid.parse() |> Gtid.render() ==
               "#{@u2}:1-3,#{@u1}:1-5"
    end

    test "overlapping intervals in the input coalesce" do
      assert "#{@u1}:1-5:3-8" |> Gtid.parse() |> Gtid.render() == "#{@u1}:1-8"
    end
  end

  describe "parse/1 malformed input (documented policy: raise ArgumentError)" do
    test "a bare UUID with no intervals raises" do
      assert_raise ArgumentError, fn -> Gtid.parse(@u1) end
    end

    test "an empty interval list (trailing colon) raises" do
      assert_raise ArgumentError, fn -> Gtid.parse("#{@u1}:") end
    end

    test "a non-UUID source raises" do
      assert_raise ArgumentError, fn -> Gtid.parse("not-a-uuid:1") end
    end

    test "a UUID of the wrong length raises" do
      assert_raise ArgumentError, fn -> Gtid.parse("8d7c06f2-8460-11f1-9dc3:1") end
    end

    test "GNO zero raises (GNOs are >= 1)" do
      assert_raise ArgumentError, fn -> Gtid.parse("#{@u1}:0") end
    end

    test "a descending range M < N raises" do
      assert_raise ArgumentError, fn -> Gtid.parse("#{@u1}:5-3") end
    end

    test "a non-numeric GNO raises" do
      assert_raise ArgumentError, fn -> Gtid.parse("#{@u1}:abc") end
    end

    test "an incomplete range raises" do
      assert_raise ArgumentError, fn -> Gtid.parse("#{@u1}:5-") end
    end

    test "a signed GNO raises (no silent coercion of non-canonical numbers)" do
      assert_raise ArgumentError, fn -> Gtid.parse("#{@u1}:+5") end
    end

    test "a leading-zero GNO raises" do
      assert_raise ArgumentError, fn -> Gtid.parse("#{@u1}:05") end
    end

    test "an empty entry between commas raises" do
      assert_raise ArgumentError, fn -> Gtid.parse("#{@u1}:1,,#{@u2}:2") end
    end
  end

  describe "parse/1 whitespace at entry boundaries (real @@gtid_executed output)" do
    # Grafted from the best-of-N spec-literal candidate: MySQL `SELECT
    # @@gtid_executed` returns a long set with `,\n` between UUID entries, so the
    # consumer (Task 3) can feed server output verbatim. Trimming is scoped to entry
    # boundaries and never loosens the grammar (last two tests).
    test "a multi-UUID set with ,\\n between entries parses canonically" do
      raw = "#{@u1}:1-5,\n#{@u2}:1-3"
      assert raw |> Gtid.parse() |> Gtid.render() == "#{@u2}:1-3,#{@u1}:1-5"
    end

    test "leading/trailing whitespace around the whole value is ignored" do
      assert "  #{@u1}:1-5\n" |> Gtid.parse() |> Gtid.render() == "#{@u1}:1-5"
    end

    test "a whitespace-only value is the empty set" do
      assert Gtid.parse("  \n") |> Gtid.render() == ""
    end

    test "a trailing comma still raises (trimming does not loosen the grammar)" do
      assert_raise ArgumentError, fn -> Gtid.parse("#{@u1}:1-5,") end
    end

    test "whitespace inside an interval token still raises" do
      assert_raise ArgumentError, fn -> Gtid.parse("#{@u1}:1 -5") end
    end
  end

  describe "member?/2 — the dedup idiom txn.gtid ∈ checkpoint" do
    test "true for a GNO inside a range, false just outside" do
      set = Gtid.parse("#{@u1}:1-5")
      assert Gtid.member?(set, {@u1, 1})
      assert Gtid.member?(set, {@u1, 3})
      assert Gtid.member?(set, {@u1, 5})
      refute Gtid.member?(set, {@u1, 6})
      refute Gtid.member?(set, {@u1, 0})
    end

    test "respects non-contiguous ranges and gaps" do
      set = Gtid.parse("#{@u1}:1-3:7:12-20")
      assert Gtid.member?(set, {@u1, 2})
      refute Gtid.member?(set, {@u1, 4})
      refute Gtid.member?(set, {@u1, 6})
      assert Gtid.member?(set, {@u1, 7})
      refute Gtid.member?(set, {@u1, 8})
      assert Gtid.member?(set, {@u1, 12})
      assert Gtid.member?(set, {@u1, 20})
      refute Gtid.member?(set, {@u1, 21})
    end

    test "single-element interval uuid:7 == uuid:7-7" do
      set = Gtid.parse("#{@u1}:7")
      assert Gtid.member?(set, {@u1, 7})
      refute Gtid.member?(set, {@u1, 6})
      refute Gtid.member?(set, {@u1, 8})
    end

    test "false for a UUID absent from the set" do
      set = Gtid.parse("#{@u1}:1-5")
      refute Gtid.member?(set, {@u2, 3})
    end

    test "the empty set contains no GTID" do
      refute Gtid.member?(Gtid.parse(""), {@u1, 1})
    end

    test "the GTID UUID is matched case-insensitively" do
      set = Gtid.parse("#{@u1}:1-5")
      assert Gtid.member?(set, {String.upcase(@u1), 3})
    end
  end

  describe "union/2 — set union with adjacent-range coalescing" do
    test "adjacent ranges coalesce (the pinned example)" do
      assert Gtid.union(Gtid.parse("#{@u1}:1-3"), Gtid.parse("#{@u1}:4-6"))
             |> Gtid.render() == "#{@u1}:1-6"
    end

    test "overlapping ranges merge" do
      assert Gtid.union(Gtid.parse("#{@u1}:1-5"), Gtid.parse("#{@u1}:3-8"))
             |> Gtid.render() == "#{@u1}:1-8"
    end

    test "non-adjacent ranges stay separate" do
      assert Gtid.union(Gtid.parse("#{@u1}:1-3"), Gtid.parse("#{@u1}:5-6"))
             |> Gtid.render() == "#{@u1}:1-3:5-6"
    end

    test "multi-UUID unions merge per-UUID" do
      assert Gtid.union(Gtid.parse("#{@u1}:1-3"), Gtid.parse("#{@u2}:1-3"))
             |> Gtid.render() == "#{@u2}:1-3,#{@u1}:1-3"
    end

    test "the empty set is the union identity" do
      a = Gtid.parse("#{@u1}:1-3:7")
      assert Gtid.render(Gtid.union(a, Gtid.parse(""))) == Gtid.render(a)
      assert Gtid.render(Gtid.union(Gtid.parse(""), a)) == Gtid.render(a)
    end

    test "union is commutative" do
      a = Gtid.parse("#{@u1}:1-5,#{@u2}:2-2")
      b = Gtid.parse("#{@u1}:4-9")
      assert Gtid.render(Gtid.union(a, b)) == Gtid.render(Gtid.union(b, a))
    end
  end

  describe "subtract/2 — a − b with interval splitting" do
    test "splits an interval around a removed middle (the pinned example)" do
      assert Gtid.subtract(Gtid.parse("#{@u1}:1-10"), Gtid.parse("#{@u1}:4-6"))
             |> Gtid.render() == "#{@u1}:1-3:7-10"
    end

    test "subtracting an equal set yields the empty set" do
      assert Gtid.render(Gtid.subtract(Gtid.parse("#{@u1}:1-3"), Gtid.parse("#{@u1}:1-3"))) ==
               ""
    end

    test "the empty set is the subtract identity" do
      a = Gtid.parse("#{@u1}:1-3:7")
      assert Gtid.render(Gtid.subtract(a, Gtid.parse(""))) == Gtid.render(a)
    end

    test "the empty set is the subtract annihilator on the left" do
      assert Gtid.render(Gtid.subtract(Gtid.parse(""), Gtid.parse("#{@u1}:1-3"))) == ""
    end

    test "a non-overlapping subtrahend leaves the minuend unchanged" do
      assert Gtid.subtract(Gtid.parse("#{@u1}:1-3"), Gtid.parse("#{@u1}:7-9"))
             |> Gtid.render() == "#{@u1}:1-3"
    end

    test "subtraction is per-UUID and renders single elements as N" do
      assert Gtid.subtract(Gtid.parse("#{@u1}:1-5,#{@u2}:1-5"), Gtid.parse("#{@u1}:2-3"))
             |> Gtid.render() == "#{@u2}:1-5,#{@u1}:1:4-5"
    end
  end

  describe "subset?/2" do
    test "a contained set is a subset" do
      assert Gtid.subset?(Gtid.parse("#{@u1}:1-3"), Gtid.parse("#{@u1}:1-10"))
    end

    test "a set reaching beyond is not a subset" do
      refute Gtid.subset?(Gtid.parse("#{@u1}:1-11"), Gtid.parse("#{@u1}:1-3"))
    end

    test "the empty set is a subset of everything, including itself" do
      assert Gtid.subset?(Gtid.parse(""), Gtid.parse("#{@u1}:1-3"))
      assert Gtid.subset?(Gtid.parse(""), Gtid.parse(""))
    end

    test "every set is a subset of itself" do
      a = Gtid.parse("#{@u1}:1-3:7,#{@u2}:9-9")
      assert Gtid.subset?(a, a)
    end

    test "a set with a UUID the superset lacks is not a subset" do
      refute Gtid.subset?(Gtid.parse("#{@u2}:1"), Gtid.parse("#{@u1}:1-3"))
    end
  end

  describe "intersection/2 (F1)" do
    test "keeps only the shared range" do
      assert Gtid.intersection(Gtid.parse("#{@u1}:1-10"), Gtid.parse("#{@u1}:4-6"))
             |> Gtid.render() == "#{@u1}:4-6"
    end

    test "disjoint ranges under the same UUID yield the empty set" do
      assert Gtid.render(Gtid.intersection(Gtid.parse("#{@u1}:1-3"), Gtid.parse("#{@u1}:5-7"))) ==
               ""
    end

    test "intersection is per-UUID; UUIDs present in only one side vanish" do
      a = Gtid.parse("#{@u1}:1-10,#{@u2}:1-10")
      b = Gtid.parse("#{@u1}:5-6,#{@u2}:20-30")
      assert Gtid.render(Gtid.intersection(a, b)) == "#{@u1}:5-6"
    end

    test "intersection with the empty set is empty" do
      a = Gtid.parse("#{@u1}:1-3")
      assert Gtid.render(Gtid.intersection(a, Gtid.parse(""))) == ""
      assert Gtid.render(Gtid.intersection(Gtid.parse(""), a)) == ""
    end

    test "non-contiguous ranges intersect piecewise" do
      assert Gtid.intersection(Gtid.parse("#{@u1}:1-3:7:12-20"), Gtid.parse("#{@u1}:2-15"))
             |> Gtid.render() == "#{@u1}:2-3:7:12-15"
    end

    test "intersection is commutative" do
      a = Gtid.parse("#{@u1}:1-8,#{@u2}:3-4")
      b = Gtid.parse("#{@u1}:5-20")
      assert Gtid.render(Gtid.intersection(a, b)) == Gtid.render(Gtid.intersection(b, a))
    end
  end

  describe "disjoint?/2 (F1)" do
    test "true when nothing is shared" do
      assert Gtid.disjoint?(Gtid.parse("#{@u1}:1-3"), Gtid.parse("#{@u1}:5-7"))
    end

    test "false when a single GNO is shared" do
      refute Gtid.disjoint?(Gtid.parse("#{@u1}:1-5"), Gtid.parse("#{@u1}:5-9"))
    end

    test "different UUIDs are disjoint" do
      assert Gtid.disjoint?(Gtid.parse("#{@u1}:1-3"), Gtid.parse("#{@u2}:1-3"))
    end

    test "the empty set is disjoint with everything" do
      a = Gtid.parse("#{@u1}:1-3")
      assert Gtid.disjoint?(Gtid.parse(""), a)
      assert Gtid.disjoint?(a, Gtid.parse(""))
    end
  end

  describe "canonical-form invariant — no empty residue can fool subset?/disjoint?" do
    # subset?/disjoint? are derived from subtract/intersection via structural
    # equality with the empty set. If an operation left a UUID mapped to an empty
    # interval list instead of dropping the key, that equality would silently
    # break. These four assertions go RED if the prune step is removed.
    test "an emptied UUID is dropped, so subset? of equal sets holds" do
      assert Gtid.subset?(Gtid.parse("#{@u1}:1-3"), Gtid.parse("#{@u1}:1-3"))
    end

    test "an emptied intersection is dropped, so disjoint? holds under a shared UUID" do
      assert Gtid.disjoint?(Gtid.parse("#{@u1}:1-3"), Gtid.parse("#{@u1}:5-7"))
    end

    test "subtracting to empty renders exactly the empty string, not uuid:" do
      assert Gtid.render(Gtid.subtract(Gtid.parse("#{@u1}:1-3"), Gtid.parse("#{@u1}:1-9"))) ==
               ""
    end

    test "intersecting to empty renders exactly the empty string, not uuid:" do
      assert Gtid.render(Gtid.intersection(Gtid.parse("#{@u1}:1-3"), Gtid.parse("#{@u1}:9-20"))) ==
               ""
    end
  end

  describe "gap predicate F1 — (executed − checkpoint) ∩ purged composes end to end" do
    test "a real purged gap is detected" do
      executed = Gtid.parse("#{@u1}:1-100")
      checkpoint = Gtid.parse("#{@u1}:1-60")
      purged = Gtid.parse("#{@u1}:50-70")

      unapplied = Gtid.subtract(executed, checkpoint)
      gap = Gtid.intersection(unapplied, purged)

      assert Gtid.render(unapplied) == "#{@u1}:61-100"
      assert Gtid.render(gap) == "#{@u1}:61-70"
      refute Gtid.disjoint?(unapplied, purged)
    end

    test "no gap when purged sits entirely below the checkpoint" do
      executed = Gtid.parse("#{@u1}:1-100")
      checkpoint = Gtid.parse("#{@u1}:1-60")
      purged = Gtid.parse("#{@u1}:1-40")

      unapplied = Gtid.subtract(executed, checkpoint)
      gap = Gtid.intersection(unapplied, purged)

      assert Gtid.render(gap) == ""
      assert Gtid.disjoint?(unapplied, purged)
    end

    test "disjoint? agrees with intersection being empty across concrete cases" do
      pairs = [
        {"#{@u1}:1-3", "#{@u1}:5-7"},
        {"#{@u1}:1-5", "#{@u1}:5-9"},
        {"#{@u1}:1-3", "#{@u2}:1-3"},
        {"#{@u1}:1-10,#{@u2}:1-10", "#{@u1}:20-30,#{@u2}:5-6"}
      ]

      for {sa, sb} <- pairs do
        a = Gtid.parse(sa)
        b = Gtid.parse(sb)
        assert Gtid.disjoint?(a, b) == (Gtid.render(Gtid.intersection(a, b)) == "")
      end
    end
  end

  describe "property — algebra agrees with a brute-force MapSet oracle" do
    test "union/subtract/intersection/subset?/disjoint? match set theory on small universes" do
      :rand.seed(:exsss, {13, 17, 19})
      uuids = [@u1, @u2]
      max = 30

      for _ <- 1..300 do
        members_a = random_members(uuids, max)
        members_b = random_members(uuids, max)
        set_a = to_gtid_set(members_a)
        set_b = to_gtid_set(members_b)

        assert to_members(Gtid.union(set_a, set_b), uuids, max) ==
                 MapSet.union(members_a, members_b)

        assert to_members(Gtid.subtract(set_a, set_b), uuids, max) ==
                 MapSet.difference(members_a, members_b)

        assert to_members(Gtid.intersection(set_a, set_b), uuids, max) ==
                 MapSet.intersection(members_a, members_b)

        assert Gtid.subset?(set_a, set_b) == MapSet.subset?(members_a, members_b)
        assert Gtid.disjoint?(set_a, set_b) == MapSet.disjoint?(members_a, members_b)

        assert Gtid.render(set_a) ==
                 set_a |> Gtid.render() |> Gtid.parse() |> Gtid.render()
      end
    end
  end

  describe "sources/1 — canonical per-source intervals for the resume encoder (Task 3)" do
    # The COM_BINLOG_DUMP_GTID encoder iterates these. This accessor stays
    # INCLUSIVE — the exclusive-end wire conversion (`high -> high + 1`) is Task 3's
    # concern, never this module's. Ordering (sources by UUID, intervals ascending
    # and coalesced) is a guarantee the encoder relies on for a deterministic wire
    # payload.
    test "the empty set has no sources" do
      assert Gtid.sources(Gtid.parse("")) == []
    end

    test "a single interval keeps INCLUSIVE bounds (11 stays 11, not 12)" do
      assert Gtid.sources(Gtid.parse("#{@u1}:1-11")) == [{@u1, [{1, 11}]}]
    end

    test "a single-element interval N is {N, N}" do
      assert Gtid.sources(Gtid.parse("#{@u1}:7")) == [{@u1, [{7, 7}]}]
    end

    test "multiple intervals under one source stay sorted and separate" do
      assert Gtid.sources(Gtid.parse("#{@u1}:1-3:7-9")) == [{@u1, [{1, 3}, {7, 9}]}]
    end

    test "sources are ordered by UUID and intervals are coalesced canonically" do
      assert Gtid.sources(Gtid.parse("#{@u1}:1-3:2-8,#{@u2}:1-3")) ==
               [{@u2, [{1, 3}]}, {@u1, [{1, 8}]}]
    end
  end

  # Builds a GTID set from individual members by unioning singletons — which also
  # exercises union's adjacent-range coalescing on the way in.
  defp to_gtid_set(members) do
    Enum.reduce(members, Gtid.parse(""), fn {uuid, gno}, acc ->
      Gtid.union(acc, Gtid.parse("#{uuid}:#{gno}"))
    end)
  end

  # Expands a GTID set back to individual members using only the public member?/2,
  # so the oracle never peeks at the internal representation.
  defp to_members(set, uuids, max_gno) do
    for uuid <- uuids,
        gno <- 1..max_gno,
        Gtid.member?(set, {uuid, gno}),
        into: MapSet.new(),
        do: {uuid, gno}
  end

  defp random_members(uuids, max_gno) do
    for uuid <- uuids,
        gno <- 1..max_gno,
        :rand.uniform() < 0.4,
        into: MapSet.new(),
        do: {uuid, gno}
  end
end

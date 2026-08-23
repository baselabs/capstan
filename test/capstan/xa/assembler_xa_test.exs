defmodule Capstan.Xa.AssemblerXaTest do
  @moduledoc """
  The `:track` fold policy (ADR-0006) over the REAL captured XA session plus the
  synthetic policy edges: the held-out watermark (G_p advances ONLY with its resolver
  G_c, in one write), rollback discards, dangling pre-start resolutions, desync
  refusals, the pool bound, re-pool idempotence, and one-phase immediacy.
  """
  use ExUnit.Case, async: true

  alias Capstan.{Assembler, Binlog.Decoder, Binlog.Event, Gtid, Position, Transaction}
  alias Capstan.Xa.Id

  @u "3f9a1c2e-0000-4000-8000-000000000001"

  ## ---------------------------------------------------------------------------
  ## conformance — the REAL captured session (live 8.0 bytes)
  ## ---------------------------------------------------------------------------

  test ":track delivers the XA-committed transaction ONCE, from the real fixture bytes" do
    events =
      fixture_events([
        "05-gtid",
        "06-query",
        "07-table_map",
        "08-write_rows",
        "09-query",
        "10-xa_prepare",
        "11-gtid",
        "12-query"
      ])

    assert {:ok, [txn], pos} = Assembler.run(events, start(), xa: :track)

    {p_uuid, p_gno} = fixture_gtid("05-gtid.bin")
    {c_uuid, c_gno} = fixture_gtid("11-gtid.bin")

    assert %Transaction{changes: [%{op: :insert, table: "xa_widgets"}]} = txn
    assert txn.gtid == "#{p_uuid}:#{p_gno}"

    # The held-out watermark's payoff: ONE position write carrying G_p ∪ G_c.
    set = Gtid.parse(pos.gtid_set)
    assert Gtid.member?(set, {p_uuid, p_gno})
    assert Gtid.member?(set, {c_uuid, c_gno})
  end

  test ":refuse over the SAME bytes keeps the C1 halt byte-for-byte" do
    events =
      fixture_events([
        "05-gtid",
        "06-query",
        "07-table_map",
        "08-write_rows",
        "09-query",
        "10-xa_prepare"
      ])

    assert {:halt, :unsupported_transaction_shape, [], _pos} =
             Assembler.run(events, start(), xa: :refuse)
  end

  ## ---------------------------------------------------------------------------
  ## the held-out watermark + resolution policy (synthetic edges)
  ## ---------------------------------------------------------------------------

  test "after the prepare, NOTHING is emitted and G_p is held out of the watermark" do
    seq = [gtid(@u, 1), xa_query("XA START X'01',X'',1"), prepare("01", "", 1)]

    assert {:ok, [], pos} = Assembler.run(seq, start(), xa: :track)
    refute Gtid.member?(Gtid.parse(pos.gtid_set), {@u, 1})
  end

  test "XA ROLLBACK discards the rows and advances G_p ∪ G_c row-lessly" do
    seq =
      [gtid(@u, 1), xa_query("XA START X'01',X'',1"), prepare("01", "", 1)] ++
        [gtid(@u, 2), xa_query("XA ROLLBACK X'01',X'',1")]

    assert {:ok, [txn], pos} = Assembler.run(seq, start(), xa: :track)
    assert %Transaction{changes: [], gtid: "#{@u}:2"} = txn

    set = Gtid.parse(pos.gtid_set)
    assert Gtid.member?(set, {@u, 1}) and Gtid.member?(set, {@u, 2})
  end

  test "a dangling pre-start prepare's resolution is a row-less advance (XA RECOVER pre-seed)" do
    xid = %{one_phase: false, format_id: 1, gtrid: "a", bqual: ""}
    seq = [gtid(@u, 9), xa_query("XA COMMIT X'61',X'',1")]

    assert {:ok, [txn], _pos} =
             Assembler.run(seq, start(), xa: :track, startup_xids: [Id.digest(xid)])

    assert %Transaction{changes: []} = txn
  end

  test "a resolution with NO prepare and NO pre-seed halts fail-closed (both verbs)" do
    for verb <- ["COMMIT", "ROLLBACK"] do
      seq = [gtid(@u, 9), xa_query("XA #{verb} X'61',X'',1")]

      assert {:halt, halt_atom, [], _pos} = Assembler.run(seq, start(), xa: :track)
      assert halt_atom == :"xa_#{String.downcase(verb)}_without_prepare"
    end
  end

  test "the pool bound halts (never evicts): max_prepared_transactions: 0" do
    seq = [gtid(@u, 1), xa_query("XA START X'01',X'',1"), prepare("01", "", 1)]

    assert {:halt, :xa_prepared_pool_exhausted, [], _pos} =
             Assembler.run(seq, start(), xa: :track, max_prepared_transactions: 0)
  end

  test "a re-presented prepare is idempotent on the same G_p, fail-closed on a different one" do
    same =
      [gtid(@u, 1), xa_query("XA START X'01',X'',1"), prepare("01", "", 1)] ++
        [gtid(@u, 1), xa_query("XA START X'01',X'',1"), prepare("01", "", 1)]

    assert {:ok, [], _pos} = Assembler.run(same, start(), xa: :track)

    mismatch =
      [gtid(@u, 1), xa_query("XA START X'01',X'',1"), prepare("01", "", 1)] ++
        [gtid(@u, 5), xa_query("XA START X'01',X'',1"), prepare("01", "", 1)]

    assert {:halt, :xa_prepared_gtid_mismatch, [], _pos} =
             Assembler.run(mismatch, start(), xa: :track)
  end

  test "one_phase = true commits immediately (an ordinary single-GTID commit, no pool)" do
    seq = [gtid(@u, 1), xa_query("XA START X'01',X'',1"), prepare("01", "", 1, one_phase: true)]

    assert {:ok, [%Transaction{gtid: "#{@u}:1", changes: []}], pos} =
             Assembler.run(seq, start(), xa: :track)

    assert Gtid.member?(Gtid.parse(pos.gtid_set), {@u, 1})
  end

  test "under :refuse a resolution query is NOT intercepted — the C1 desync path stands" do
    seq = [gtid(@u, 9), xa_query("XA COMMIT X'61',X'',1"), gtid(@u, 10)]

    assert {:error, :gtid_within_open_transaction, [], _pos} =
             Assembler.run(seq, start(), xa: :refuse)
  end

  ## ---------------------------------------------------------------------------
  ## builders
  ## ---------------------------------------------------------------------------

  defp start, do: %Position{gtid_set: "", file: nil, pos: nil}

  defp fixture_events(names), do: Enum.map(names, &fixture_event("#{&1}.bin"))

  defp fixture_event(name) do
    {:ok, event} =
      "test/fixtures/binlog/xa/#{name}" |> File.read!() |> Event.parse()

    event
  end

  defp fixture_gtid(name) do
    {:ok, {:gtid, {uuid, gno}}} = name |> fixture_event() |> Decoder.decode()
    {uuid, gno}
  end

  defp gtid(uuid, gno) do
    sid = uuid |> String.replace("-", "") |> Base.decode16!(case: :mixed)

    %Event{
      type: 33,
      timestamp: 1_700_000_000,
      server_id: 1,
      event_size: 0,
      log_pos: 10,
      flags: 0,
      body: <<0>> <> sid <> <<gno::64-little>>
    }
  end

  defp xa_query(sql), do: query_event("probe_db", sql)

  defp query_event(schema, sql) do
    body =
      <<0::32-little, 0::32-little, byte_size(schema)::8, 0::16-little, 0::16-little>> <>
        schema <> <<0>> <> sql

    %Event{
      type: 2,
      timestamp: 1_700_000_000,
      server_id: 1,
      event_size: 0,
      log_pos: 100,
      flags: 0,
      body: body
    }
  end

  defp prepare(gtrid_hex, bqual_hex, format_id, opts \\ []) do
    gtrid = Base.decode16!(gtrid_hex, case: :mixed)
    bqual = Base.decode16!(bqual_hex, case: :mixed)
    one_phase = Keyword.get(opts, :one_phase, false)

    op_byte = if one_phase, do: 1, else: 0

    body =
      <<op_byte>> <>
        <<format_id::32-little>> <>
        <<byte_size(gtrid)::32-little>> <> <<byte_size(bqual)::32-little>> <> gtrid <> bqual

    %Event{
      type: 38,
      timestamp: 1_700_000_000,
      server_id: 1,
      event_size: 0,
      log_pos: 200,
      flags: 0,
      body: body
    }
  end
end

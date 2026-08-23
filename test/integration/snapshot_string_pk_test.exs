defmodule Capstan.Integration.SnapshotStringPkTest do
  @moduledoc """
  C2a / ADR-0012 live marquees — collation-ordered STRING primary keys through the whole
  snapshot pipeline. The acceptance row: *a collation-string-PK table backfills gap-free and
  dup-free; the cursor-gate's `k ≤ cursor` comparison reproduces the source's collation
  order (not Elixir byte-order) so no row is mis-classified.*

  Every fixture band is chosen so **byte order ≠ collation order** (probe
  `probe/collation_weight_probe.exs` Q1b: under `utf8mb4_0900_ai_ci`, `'a…' < 'Z…'` while
  `'Z…' < 'a…'` in bytes) — a gate that compared raws would mis-classify exactly the
  straddling keys, and the no-gap/no-dup assertions would fail RED.

    * **Marquee 1 (ai_ci):** the tripwire-1 shape (concurrent writers across backfilled and
      not-yet-backfilled bands, small chunks, exact convergence + final-value-exactly-once)
      on a `VARCHAR` PK whose two bands invert between the two orders, plus expansion (ß),
      CJK, and 4-byte keys.
    * **Marquee 2 (as_cs):** the multi-level-weight family (`utf8mb4_0900_as_cs` — 'e' and
      'é' are DISTINCT keys) — the adversarial pass's finding 5 arm.
    * **Marquee 3 (reader-level):** the dual cursor against the live server — chunk SELECT
      shape (CAST projection + appended weight column, weight NEVER delivered), pagination
      tiles the table in the server's own `ORDER BY` order, and a mid-table resume from the
      persisted dual cursor re-reads without gap/dup.
    * **Marquee 4 (carried composition):** multi-uuid × snapshot × `xa: :track` — the
      advance gate must fire through a processed set carrying a SECOND source uuid
      (`gtid_next`-pinned commits) while an XA prepare/commit pair straddles the backfill,
      with every affected key delivered exactly once.

  `:integration`-tagged. Never restarts or reconfigures the shared container.
  """

  use ExUnit.Case, async: false

  alias Capstan.MysqlCase
  alias Capstan.MysqlCase.{DurableSnapshotStore, DurableStore, SnapshotSink}
  alias Capstan.Query
  alias Capstan.Snapshot.ChunkReader
  alias Capstan.Snapshot.PrimaryKey

  @moduletag :integration

  @chunk 8
  @writers 6
  @offset 1_000_000

  # The ai_ci fixture bands: 60 'a…' keys + 40 'Z…' keys + specials. Collation order puts
  # the whole 'a' band BEFORE the whole 'Z' band; byte order inverts them. Digits sort
  # before letters, so '0001'… prefix with letters keeps intra-band order stable.
  @a_band for i <- 1..60, do: "a#{String.pad_leading(Integer.to_string(i), 4, "0")}"
  @z_band for i <- 1..40, do: "Z#{String.pad_leading(Integer.to_string(i), 4, "0")}"
  @specials ["ß9", "中7", "𝕏5"]
  @pre_keys @a_band ++ @z_band ++ @specials

  setup_all do
    MysqlCase.ensure_sha2_user!(MysqlCase.query_connection())
    grant = MysqlCase.socket!(MysqlCase.query_connection())
    MysqlCase.run!(grant, "GRANT LOCK TABLES ON *.* TO 'capstan_sha2'@'%'")
    MysqlCase.close!(grant)
    :ok
  end

  setup do
    on_exit(&SnapshotSink.clear/0)
    :ok
  end

  ## ===========================================================================
  ## Marquee 1 — ai_ci straddle under concurrent load (THE C2a acceptance)
  ## ===========================================================================

  test "an ai_ci VARCHAR PK backfills gap-free/dup-free while byte order inverts the bands" do
    ctx = string_ctx("utf8mb4_0900_ai_ci")
    create_string_table!(ctx, "utf8mb4_0900_ai_ci")
    create_heartbeat_table!(ctx)
    seed_string_rows!(ctx, @pre_keys)

    w0 = MysqlCase.read_gtid_executed!(ctx.qconn)
    checkpoint = new_durable_checkpoint(w0)
    snap_store = new_durable_snapshot_store()
    configure_string_sink(ctx)

    writers = spawn_string_writers(ctx, @pre_keys)
    await_gtid_advanced(ctx.qconn, w0, 800)
    sup = start_string_snapshot!(ctx, checkpoint, snap_store, chunk_size: @chunk)

    Enum.each(writers, &Task.await(&1, 120_000))

    drain_heartbeats!(ctx)
    w_final = MysqlCase.read_gtid_executed!(ctx.qconn)
    await_checkpoint_covers(checkpoint, w_final, 4000)
    await_snapshot_completed(60_000)
    MysqlCase.stop_pipeline(sup)

    {deleted, expected} = expected_string_final(@pre_keys)

    assert db_string_final(ctx) == expected
    assert materialize_string(ctx) == expected
    assert_final_value_exactly_once(ctx, {deleted, expected})

    # Non-vacuity: both delivery paths exercised.
    assert source_count(ctx, :chunk) > 0, "no chunk deliveries — vacuous"
    assert source_count(ctx, :stream) > 0, "no stream deliveries — the load did not overlap"
  end

  ## ===========================================================================
  ## Marquee 2 — the multi-level collation family (as_cs)
  ## ===========================================================================

  test "an as_cs VARCHAR PK (multi-level weights, 'e' ≠ 'é') backfills exactly" do
    ctx = string_ctx("utf8mb4_0900_as_cs")

    # Under as_cs the accent is SIGNIFICANT: e… and é… are distinct keys interleaved in
    # neither byte nor simple order — the multi-level weight space.
    keys = for i <- 1..12, do: "e#{String.pad_leading(Integer.to_string(i), 4, "0")}"
    accented = for i <- 1..12, do: "é#{String.pad_leading(Integer.to_string(i), 4, "0")}"
    all_keys = Enum.zip(keys, accented) |> Enum.flat_map(&Tuple.to_list/1)

    create_string_table!(ctx, "utf8mb4_0900_as_cs")
    create_heartbeat_table!(ctx)
    seed_string_rows!(ctx, all_keys)

    w0 = MysqlCase.read_gtid_executed!(ctx.qconn)
    checkpoint = new_durable_checkpoint(w0)
    snap_store = new_durable_snapshot_store()
    configure_string_sink(ctx)

    writers = spawn_string_writers(ctx, all_keys)
    await_gtid_advanced(ctx.qconn, w0, 800)
    sup = start_string_snapshot!(ctx, checkpoint, snap_store, chunk_size: 6)

    Enum.each(writers, &Task.await(&1, 120_000))

    drain_heartbeats!(ctx)
    w_final = MysqlCase.read_gtid_executed!(ctx.qconn)
    await_checkpoint_covers(checkpoint, w_final, 4000)
    await_snapshot_completed(60_000)
    MysqlCase.stop_pipeline(sup)

    {deleted, expected} = expected_string_final(all_keys)
    assert db_string_final(ctx) == expected
    assert materialize_string(ctx) == expected
    assert_final_value_exactly_once(ctx, {deleted, expected})
    assert source_count(ctx, :chunk) > 0 and source_count(ctx, :stream) > 0
  end

  ## ===========================================================================
  ## Marquee 3 — the dual cursor against the live server (reader level)
  ## ===========================================================================

  test "the dual cursor pages in the server's ORDER BY order and resumes without gap/dup" do
    ctx = string_ctx("utf8mb4_0900_ai_ci")
    create_string_table!(ctx, "utf8mb4_0900_ai_ci")
    seed_string_rows!(ctx, @pre_keys)

    {:ok, query} =
      Query.establish(
        connection: MysqlCase.pipeline_connection(),
        connect_fun: &Query.default_connect/1
      )

    on_exit(fn -> Query.close(query) end)

    {:ok, reader} = ChunkReader.open(query, {ctx.schema, ctx.table}, chunk_size: @chunk)

    # The server's own collation order — the ground truth pagination must tile.
    server_order =
      ctx.qconn
      |> MysqlCase.query_rows!("SELECT code FROM #{q(ctx.schema, ctx.table)} ORDER BY code")
      |> Enum.map(&List.first/1)

    assert length(server_order) == length(@pre_keys)
    # The fixture is adversarial: byte order DIFFERS from the collation order.
    assert Enum.sort(@pre_keys) != server_order

    # The delivered-record-keys invariant (adversarial finding 6): the FIRST chunk's records
    # carry exactly the table's columns — the appended WEIGHT_STRING cursor column and any
    # cursor internals never ride a delivered row.
    assert {:ok, first_chunk, _final?, _r} = ChunkReader.read_chunk(reader, :start)
    assert first_chunk.rows != []
    assert Enum.all?(first_chunk.rows, &(Map.keys(&1) |> Enum.sort() == ["code", "v"]))

    # Reopen (the first read consumed the chunk) and page the whole table with the dual
    # cursor, collecting codes in delivery order.
    {:ok, reader} = ChunkReader.open(query, {ctx.schema, ctx.table}, chunk_size: @chunk)
    {tiled, cursors} = drain_string_reader(reader, :start, [], [])

    # Tiling: every key exactly once, in the server's ORDER BY order — the weight-ordered
    # pagination reproduces the collation order (a byte-ordered cursor would tile the
    # byte order and this assert fails RED).
    assert tiled == server_order

    # Every cursor mid-stream is the dual form (raw bytes + weights) — the resume source.
    assert Enum.count(cursors) > 1

    assert Enum.all?(
             tl(cursors),
             &match?(%{raw: r, weight: w} when is_binary(r) and is_binary(w), &1)
           )

    # Mid-table resume: reopen a reader and page from the LAST dual cursor — the remaining
    # keys follow with no skip/dup at the boundary.
    {:ok, reader2} = ChunkReader.open(query, {ctx.schema, ctx.table}, chunk_size: @chunk)
    resume_cursor = List.last(cursors)
    {resumed, _} = drain_string_reader(reader2, resume_cursor, [], [])
    assert resumed == Enum.drop(server_order, length(tiled))
  end

  test "a CHAR(n) PK pages and gates in weight space too (the fixed-length corner)" do
    # The claude-peer's untested corner: CHAR (fixed-length) under a NO PAD collation with
    # short values — chunk-side WEIGHT_STRING(char_col) must equal the stream-side
    # introducer weight, and the tiling must match the server's ORDER BY.
    ctx = string_ctx("utf8mb4_0900_ai_ci")

    MysqlCase.run!(
      ctx.qconn,
      "CREATE TABLE #{q(ctx.schema, ctx.table)} " <>
        "(code CHAR(16) COLLATE utf8mb4_0900_ai_ci NOT NULL PRIMARY KEY, v INT NOT NULL) ENGINE=InnoDB"
    )

    seed_string_rows!(ctx, @pre_keys)

    {:ok, query} =
      Query.establish(
        connection: MysqlCase.pipeline_connection(),
        connect_fun: &Query.default_connect/1
      )

    on_exit(fn -> Query.close(query) end)

    {:ok, reader} = ChunkReader.open(query, {ctx.schema, ctx.table}, chunk_size: @chunk)

    server_order =
      ctx.qconn
      |> MysqlCase.query_rows!("SELECT code FROM #{q(ctx.schema, ctx.table)} ORDER BY code")
      |> Enum.map(&List.first/1)

    assert Enum.sort(@pre_keys) != server_order

    {tiled, cursors} = drain_string_reader(reader, :start, [], [])
    assert tiled == server_order

    assert Enum.all?(
             tl(cursors),
             &match?(%{raw: r, weight: w} when is_binary(r) and is_binary(w), &1)
           )

    # The stream-side introducer weight equals the chunk-side column weight for CHAR too
    # (the canonical-equality half of the strict-once proof).
    {first_raw, first_weight} = hd(Enum.map(tl(cursors), &{&1.raw, &1.weight}))

    assert {:ok, weights} =
             PrimaryKey.resolve_weights(query, "utf8mb4", "utf8mb4_0900_ai_ci", [first_raw])

    assert weights[first_raw] == first_weight
  end

  ## ===========================================================================
  ## Marquee 4 (carried) — multi-uuid × snapshot × xa: :track composition
  ## ===========================================================================

  test "the advance gate fires through a multi-uuid processed set while an XA pair straddles" do
    ctx = string_ctx("utf8mb4_0900_ai_ci")
    create_string_table!(ctx, "utf8mb4_0900_ai_ci")
    create_heartbeat_table!(ctx)
    keys = Enum.take(@a_band, 24)
    seed_string_rows!(ctx, keys)

    w0 = MysqlCase.read_gtid_executed!(ctx.qconn)
    checkpoint = new_durable_checkpoint(w0)
    snap_store = new_durable_snapshot_store()
    configure_string_sink(ctx)

    # A SECOND source uuid's commits touch the snapshot table while the backfill runs — the
    # GTID space the checkpoint/advance sets must carry (multi-source by construction).
    other_uuid = random_uuid()

    # An XA pair that straddles: PREPARE now (its rows invisible to every chunk while
    # prepared), COMMIT after the backfill has progressed — under xa: :track the resolution
    # delivers the rows exactly once and the held-out watermark advances.
    xid = "'c2a-xa-#{System.unique_integer([:positive])}'"
    xa_key = Enum.at(@z_band, 0)

    MysqlCase.run_all!(ctx.qconn, [
      "XA START #{xid}",
      "INSERT INTO #{q(ctx.schema, ctx.table)} (code, v) VALUES ('#{xa_key}', 77)",
      "XA END #{xid}",
      "XA PREPARE #{xid}"
    ])

    sup = start_string_snapshot!(ctx, checkpoint, snap_store, chunk_size: 4, xa: :track)

    # Multi-uuid traffic on the snapshot table while the backfill paces.
    Enum.each(1..10, fn n ->
      MysqlCase.run!(ctx.qconn, "SET gtid_next = '#{other_uuid}:#{n}'")

      MysqlCase.run!(
        ctx.qconn,
        "UPDATE #{q(ctx.schema, ctx.table)} SET v = #{n + @offset} WHERE code = '#{Enum.at(keys, n - 1)}'"
      )

      MysqlCase.run!(ctx.qconn, "SET gtid_next = 'AUTOMATIC'")
    end)

    MysqlCase.run!(ctx.qconn, "XA COMMIT #{xid}")

    drain_heartbeats!(ctx)
    w_final = MysqlCase.read_gtid_executed!(ctx.qconn)
    await_checkpoint_covers(checkpoint, w_final, 4000)
    await_snapshot_completed(60_000)
    MysqlCase.stop_pipeline(sup)

    # The multi-uuid updates converged (delivered + applied by replay), the XA'd row arrived
    # exactly once, and the whole table materializes to the DB's final state. (No writers in
    # this marquee: keys 1..10 carry the multi-uuid update, the rest their seeded value.)
    expected =
      Map.new(Enum.with_index(keys, 1), fn {code, i} ->
        {code, if(i <= 10, do: Integer.to_string(i + @offset), else: Integer.to_string(i))}
      end)
      |> Map.put(xa_key, "77")

    materialized = materialize_string(ctx)
    assert materialized == expected

    assert Map.get(materialized, xa_key) == "77"
    assert ledger_values(ctx, xa_key) == ["77"]

    # The processed set really carried the second uuid (non-vacuity of the multi-uuid arm).
    checkpoint_set = DurableStore.current(checkpoint.table, checkpoint.key)
    assert checkpoint_set =~ other_uuid
  end

  ## ===========================================================================
  ## shared context + substrate lifecycle
  ## ===========================================================================

  defp string_ctx(collation) do
    qconn = MysqlCase.socket!(MysqlCase.query_connection())
    schema = MysqlCase.unique_schema()
    MysqlCase.create_schema!(qconn, schema)

    on_exit(fn ->
      cleanup = MysqlCase.socket!(MysqlCase.query_connection())
      MysqlCase.drop_schema!(cleanup, schema)
      MysqlCase.close!(cleanup)
      MysqlCase.close!(qconn)
    end)

    telemetry = MysqlCase.attach_snapshot_telemetry(self())
    on_exit(fn -> :telemetry.detach(telemetry) end)

    %{
      qconn: qconn,
      schema: schema,
      table: "t",
      heartbeat: "hb",
      collation: collation,
      ledger: MysqlCase.new_ledger()
    }
  end

  defp create_string_table!(ctx, collation) do
    MysqlCase.run!(
      ctx.qconn,
      "CREATE TABLE #{q(ctx.schema, ctx.table)} " <>
        "(code VARCHAR(64) COLLATE #{collation} NOT NULL PRIMARY KEY, v INT NOT NULL) ENGINE=InnoDB"
    )
  end

  defp create_heartbeat_table!(ctx) do
    MysqlCase.run!(
      ctx.qconn,
      "CREATE TABLE #{q(ctx.schema, ctx.heartbeat)} (id INT PRIMARY KEY, n INT NOT NULL) ENGINE=InnoDB"
    )
  end

  defp seed_string_rows!(ctx, codes) do
    codes
    |> Enum.with_index(1)
    |> Enum.map(fn {code, i} -> "('#{code}', #{i})" end)
    |> Enum.chunk_every(200)
    |> Enum.each(fn batch ->
      MysqlCase.run!(
        ctx.qconn,
        "INSERT INTO #{q(ctx.schema, ctx.table)} (code, v) VALUES " <> Enum.join(batch, ", ")
      )
    end)
  end

  defp q(schema, table), do: "`#{schema}`.`#{table}`"

  defp random_uuid do
    <<a::binary-8, b::binary-4, c::binary-4, d::binary-4, e::binary-12>> =
      16 |> :crypto.strong_rand_bytes() |> Base.encode16(case: :lower)

    "#{a}-#{b}-#{c}-#{d}-#{e}"
  end

  ## ---------------------------------------------------------------------------
  ## writers (the string-arm of snapshot_test's tripwire-1 writers)
  ## ---------------------------------------------------------------------------

  defp spawn_string_writers(ctx, keys) do
    n = length(keys)
    slice = div(n, @writers)

    Enum.map(0..(@writers - 1), fn w ->
      lo = w * slice
      hi = if w == @writers - 1, do: n - 1, else: lo + slice - 1
      Task.async(fn -> run_string_writer(ctx, w, Enum.slice(keys, lo..hi)) end)
    end)
  end

  defp run_string_writer(ctx, w, keys) do
    sock = MysqlCase.socket!(MysqlCase.query_connection())

    try do
      Enum.each(writer_string_ops(ctx, w, keys), fn sql ->
        MysqlCase.run!(sock, sql)
        Process.sleep(4)
      end)
    after
      MysqlCase.close!(sock)
    end
  end

  # Update every pre-existing key once (v = index + offset, deterministic), plus brand-new
  # string-key inserts (pure load); writer 0 also deletes the first two keys.
  defp writer_string_ops(ctx, w, keys) do
    updates =
      keys
      |> Enum.with_index(1)
      |> Enum.map(fn {code, i} ->
        "UPDATE #{q(ctx.schema, ctx.table)} SET v = #{i + @offset} WHERE code = '#{code}'"
      end)

    inserts =
      Enum.map(0..9, fn i ->
        "INSERT INTO #{q(ctx.schema, ctx.table)} (code, v) VALUES ('nw#{w}_#{i}', #{i})"
      end)

    deletes =
      if w == 0,
        do: ["DELETE FROM #{q(ctx.schema, ctx.table)} WHERE code = '#{Enum.at(keys, 0)}'"],
        else: []

    Enum.shuffle(updates ++ inserts ++ deletes)
  end

  ## ---------------------------------------------------------------------------
  ## expected final state + assertions
  ## ---------------------------------------------------------------------------

  defp expected_string_final(keys) do
    # Writer w's slice updates each key to (slice-index + offset); writer 0 deletes its
    # FIRST key. Reconstruct exactly what run_string_writer/3 drove.
    n = length(keys)
    slice = div(n, @writers)

    {deleted, expected} =
      Enum.reduce(Enum.with_index(keys, 1), {MapSet.new(), %{}}, fn {code, i}, {del, acc} ->
        w = min(div(i - 1, slice), @writers - 1)
        local_i = i - w * slice

        if w == 0 and local_i == 1 do
          {MapSet.put(del, code), acc}
        else
          {del, Map.put(acc, code, Integer.to_string(local_i + @offset))}
        end
      end)

    {deleted, Map.drop(expected, MapSet.to_list(deleted))}
  end

  defp db_string_final(ctx) do
    ctx.qconn
    |> MysqlCase.query_rows!("SELECT code, v FROM #{q(ctx.schema, ctx.table)}")
    |> Enum.reject(fn [code, _v] -> String.starts_with?(code, "nw") end)
    |> Map.new(fn [code, v] -> {code, v} end)
  end

  defp materialize_string(ctx) do
    ctx.ledger
    |> MysqlCase.ledger_dump()
    |> Enum.filter(fn {{s, t, _pk}, _e} -> s == ctx.schema and t == ctx.table end)
    |> Enum.reject(fn {{_s, _t, pk}, _e} -> String.starts_with?(pk, "nw") end)
    |> Enum.sort_by(fn {_key, entry} -> entry.seq end)
    |> Enum.reduce(%{}, fn
      {{_s, _t, pk}, %{value: :deleted}}, acc -> Map.delete(acc, pk)
      {{_s, _t, pk}, %{value: value}}, acc -> Map.put(acc, pk, value)
    end)
  end

  defp assert_final_value_exactly_once(ctx, {deleted, expected}) do
    entries = MysqlCase.ledger_dump(ctx.ledger)

    Enum.each(expected, fn {pk, final_value} ->
      count =
        Enum.count(entries, fn {{s, t, k}, e} ->
          s == ctx.schema and t == ctx.table and k == pk and e.value == final_value
        end)

      assert count == 1,
             "pre-existing key #{inspect(pk)} final value #{inspect(final_value)} delivered #{count}×"
    end)

    # A deleted pre-existing key is absent from the materialized view (whether the delete
    # streamed or was suppressed-and-chunk-omitted is timing-dependent by design).
    Enum.each(deleted, fn pk -> refute Map.has_key?(materialize_string(ctx), pk) end)
  end

  defp source_count(ctx, source) do
    ctx.ledger
    |> MysqlCase.ledger_dump()
    |> Enum.count(fn {{s, _t, _k}, e} -> s == ctx.schema and e.source == source end)
  end

  defp ledger_values(ctx, pk) do
    ctx.ledger
    |> MysqlCase.ledger_dump()
    |> Enum.filter(fn {{s, t, k}, _e} -> s == ctx.schema and t == ctx.table and k == pk end)
    |> Enum.map(fn {_key, e} -> e.value end)
  end

  ## ---------------------------------------------------------------------------
  ## pipeline start + stores + sink
  ## ---------------------------------------------------------------------------

  defp new_durable_checkpoint(seed) do
    table = DurableStore.new_table()
    :ok = DurableStore.seed(table, :checkpoint, seed)
    %{table: table, key: :checkpoint}
  end

  defp new_durable_snapshot_store do
    %{table: DurableSnapshotStore.new_table(), key: :snapshot}
  end

  # The ledger keys on the pk's RAW BYTES (identity for a string pk — chunk reads deliver
  # CAST-AS-BINARY column bytes, streamed changes the same binlog bytes, so both paths key
  # equal under :varbinary canonicalization).
  defp configure_string_sink(ctx) do
    SnapshotSink.configure(%{
      pid: self(),
      ledger: ctx.ledger,
      pk_columns: ["code"],
      pk_types: [:varbinary],
      value_column: "v"
    })
  end

  defp start_string_snapshot!(ctx, checkpoint, snap_store, opts) do
    {:ok, sup} =
      Capstan.start_link(
        connection: MysqlCase.pipeline_connection(),
        server_id: MysqlCase.unique_server_id(),
        sink: SnapshotSink,
        checkpoint_store: [
          module: DurableStore,
          options: [table: checkpoint.table, key: checkpoint.key]
        ],
        tables: [{ctx.schema, ctx.table}],
        snapshot: [
          tables: [{ctx.schema, ctx.table}],
          store: [
            module: DurableSnapshotStore,
            options: [table: snap_store.table, key: snap_store.key]
          ],
          chunk_size: Keyword.fetch!(opts, :chunk_size)
        ],
        xa: Keyword.get(opts, :xa, :refuse),
        max_command_retries: 5
      )

    on_exit(fn -> MysqlCase.stop_pipeline(sup) end)
    sup
  end

  ## ---------------------------------------------------------------------------
  ## reader-level drain
  ## ---------------------------------------------------------------------------

  defp drain_string_reader(reader, cursor, acc_codes, acc_cursors) do
    case ChunkReader.read_chunk(reader, cursor) do
      {:ok, chunk, true, _r} ->
        {acc_codes ++ chunk_codes(chunk.rows), acc_cursors ++ [chunk.max_pk]}

      {:ok, chunk, false, r} ->
        drain_string_reader(
          r,
          chunk.max_pk,
          acc_codes ++ chunk_codes(chunk.rows),
          acc_cursors ++
            [
              chunk.max_pk
            ]
        )

      {:done, _r} ->
        {acc_codes, acc_cursors}
    end
  end

  defp chunk_codes(rows), do: Enum.map(rows, & &1["code"])

  ## ---------------------------------------------------------------------------
  ## synchronisation
  ## ---------------------------------------------------------------------------

  defp drain_heartbeats!(ctx) do
    Enum.each(1..8, fn n ->
      MysqlCase.run!(
        ctx.qconn,
        "INSERT INTO #{q(ctx.schema, ctx.heartbeat)} (id, n) VALUES (#{n}, #{n})"
      )
    end)
  end

  defp await_snapshot_completed(timeout) do
    receive do
      {:snapshot_event, :completed, _m, _md} -> :ok
      {:snapshot_event, :halt, _m, md} -> flunk("snapshot halted fail-closed: #{inspect(md)}")
    after
      timeout -> flunk("snapshot backfill did not complete within #{timeout}ms")
    end
  end

  defp await_gtid_advanced(_qconn, _w0, 0) do
    flunk("the writers never advanced the watermark past W0 (no concurrent load to overlap)")
  end

  defp await_gtid_advanced(qconn, w0, attempts) do
    current = MysqlCase.read_gtid_executed!(qconn)

    if MysqlCase.committed_count(current) > MysqlCase.committed_count(w0) do
      :ok
    else
      Process.sleep(5)
      await_gtid_advanced(qconn, w0, attempts - 1)
    end
  end

  defp await_checkpoint_covers(_checkpoint, _w_final, 0) do
    flunk("checkpoint never covered the final watermark")
  end

  defp await_checkpoint_covers(checkpoint, w_final, attempts) do
    current = DurableStore.current(checkpoint.table, checkpoint.key)
    target = MysqlCase.committed_count(w_final)

    if current && MysqlCase.committed_count(current) >= target do
      :ok
    else
      Process.sleep(25)
      await_checkpoint_covers(checkpoint, w_final, attempts - 1)
    end
  end
end

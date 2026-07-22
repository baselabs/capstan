defmodule Capstan.Integration.SnapshotTest do
  @moduledoc """
  Live-substrate initial-snapshot marquees (plan Task 11) — the whole-pipeline acceptance gate
  C2 exists to pass. Each drives a REAL `Capstan.start_link/1` snapshot pipeline against the
  running `mysql-cdc-probe` 8.0 on a THROWAWAY schema (`DROP DATABASE` in `on_exit`), delivering
  into an append-only `(schema, table, canonical_pk)` `:duplicate_bag` ledger so a double-delivery
  is VISIBLE (a PK-upsert count would hide it — design Q-tests).

    * **Tripwire 1 — no-gap/no-dup under concurrent load (THE test):** seed N pre-existing rows;
      backfill with a small `chunk_size` while ≥8 writers UPDATE/INSERT/DELETE across backfilled
      AND not-yet-backfilled ranges. The upsert-by-PK replay of the ledger (in delivery order)
      must converge to the DB's final state for the fixed PRE-EXISTING PK set, and each surviving
      pre-existing key's FINAL value must appear EXACTLY ONCE (chunk OR stream, never both, never
      neither). Asserting on the fixed pre-existing PK set is the Task-10 determinism finding: the
      total delivered count is non-deterministic (a concurrent INSERT may or may not be in the
      consistent-snapshot view), but each pre-existing key's final state is exact.
    * **Tripwire 3 — resume mid-backfill:** kill after a few chunks (cursor persisted in a durable
      snapshot store); restart → the remaining chunks run, the already-backfilled ones are NOT
      re-emitted, down-window changes are not lost.
    * **Tripwire 14 — non-vacuity of suppression (adversarial fixture ordering):** a key changed
      at `gtid ≤ G` is ABSENT from stream delivery (suppressed) + PRESENT in the chunk; a key
      changed at `gtid > G` is present in the stream + absent from the chunk. The racing row is
      seeded FIRST so a scope-then-limit chunk cannot fill its slot from other rows.
    * **Tripwire 10 (integration arm) — advance gate under filtered + DDL:** with the chunk's `G`
      held ahead of the stream, only FILTERED transactions (on a non-captured table) and a DDL on
      a NON-snapshot table advance the watermark past `G` → the advance gate still fires (the
      choke-point observer hook) → the backfill completes, no stall.

  `:integration`-tagged (excluded by default; run with `mix test --only integration` against a
  substrate from `scripts/dev-substrate.sh`). Never restarts or reconfigures the shared container.
  See the RED-drop procedures documented at each marquee — each named safety property has a
  provable RED (the brief-lock exact-`G` swap for tripwire 1; a forgetful snapshot store for
  tripwire 3).
  """
  use ExUnit.Case, async: false

  alias Capstan.MysqlCase
  alias Capstan.MysqlCase.{DurableSnapshotStore, DurableStore, SnapshotSink}

  @moduletag :integration

  # Tripwire-1 sizing: N pre-existing rows, a deliberately SMALL chunk so the backfill spans many
  # chunks and overlaps the concurrent writers; ≥8 writers. Disjoint PK bands keep the FINAL state
  # deterministic (each pre-existing key is updated at most once, or deleted, or untouched).
  @n 400
  @chunk 20
  @writers 8
  @delete_hi 40
  @update_hi 360
  @offset 1_000_000

  # Tripwire-3 sizing: enough rows for ~20 chunks so a kill lands cleanly mid-backfill.
  @resume_n 195
  @resume_chunk 10

  setup_all do
    MysqlCase.ensure_sha2_user!(MysqlCase.query_connection())
    # Self-sufficiency: the brief per-chunk lock needs LOCK TABLES (plan Task 0 grants it to the
    # dev substrate, but the marquee grants it too so it never depends on dev-substrate.sh state).
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
  ## Tripwire 1 — no-gap/no-dup under concurrent load (THE test)
  ## ===========================================================================

  test "tripwire 1: no pre-existing row is gapped, duplicated, or delivered stale under load" do
    ctx = new_ctx()
    create_snapshot_table!(ctx)
    create_heartbeat_table!(ctx)
    seed_rows!(ctx, 1..@n)

    # Seed the checkpoint at the PRE-writer watermark so the stream replays every writer commit
    # (the floor is W0, not "start from now"), and start the writers BEFORE the pipeline. The
    # backfill's per-chunk `G` is then captured while the stream is still catching up to the live
    # write frontier — a guaranteed overlap. (Seeding "" / starting writers after would let the
    # chunks burst to completion over the settled seed, a vacuous happy path.)
    w0 = MysqlCase.read_gtid_executed!(ctx.qconn)
    checkpoint = new_durable_checkpoint(ctx, w0)
    snap_store = new_durable_snapshot_store(ctx)
    configure_sink(ctx)

    writers = spawn_writers(ctx)
    await_gtid_advanced(ctx.qconn, w0, 800)
    sup = start_snapshot!(ctx, checkpoint, snap_store, chunk_size: @chunk)

    Enum.each(writers, &Task.await(&1, 120_000))

    drain_markers!(ctx)
    w_final = MysqlCase.read_gtid_executed!(ctx.qconn)
    await_checkpoint_covers(checkpoint, w_final, 4000)
    await_snapshot_completed(60_000)

    MysqlCase.stop_pipeline(sup)

    expected = expected_present_map()
    db_final = db_final_map(ctx, @n)
    materialized = materialize(ctx.ledger, ctx.schema, ctx.table, 1..@n)

    # The DB really reached the deterministic final state the writers drove (guards the fixture).
    assert db_final == expected

    # No gap, no dup, no stale: the upsert-by-PK replay of every pre-existing delivery converges
    # EXACTLY to the DB's final state. A stale chunk row overwriting a fresh streamed row (the
    # bare-`gtid_executed` corruption) makes a key's replayed value diverge → this fails RED.
    assert materialized == expected

    # Exactly once: each surviving pre-existing key's FINAL value appears in the ledger exactly
    # once (chunk OR stream, never both, never neither).
    assert_final_value_exactly_once(ctx, expected)

    # Non-vacuity: BOTH delivery paths were exercised (a happy path over an empty backfill, or a
    # backfill with no concurrent stream traffic, would pass the above vacuously).
    assert chunk_deliveries(ctx) > 0, "no chunk (backfill) deliveries — vacuous"

    assert stream_deliveries(ctx) > 0,
           "no stream deliveries — the concurrent load did not overlap"
  end

  ## ---------------------------------------------------------------------------
  ## Tripwire 1 — RED procedure (documented; executed at build time)
  ## ---------------------------------------------------------------------------
  #
  # The confirmed defect (design §⚠️): replace the brief-lock exact-`G` capture with a bare
  # `@@gtid_executed` read (the removed lock-free bracket). `@@gtid_executed` LEADS InnoDB
  # row-visibility under concurrent commit, so a chunk row read against a view established after a
  # bare `G` read can be STALE for a key whose change is `≤ G` — the stream suppresses that change
  # (k > cursor) and the stale chunk value wins → the row is delivered with a stale value.
  #
  # To prove RED, temporarily neuter `Capstan.Snapshot.ChunkReader.run_capture/2` to the bare
  # bracket (drop steps (i) SET lock_wait_timeout, (ii) LOCK TABLES, (v) UNLOCK TABLES), leaving:
  #
  #     with {:read, {:ok, [[g]]}} when is_binary(g) <- {:read, Query.query(q, @gtid_sql)},
  #          {:read, {:ok, _}} <- {:read, Query.query(q, @start_txn_sql)},
  #          {:read, {:ok, rows}} <- {:read, Query.query(q, chunk_sql(reader, cursor))},
  #          {:read, {:ok, _}} <- {:read, Query.query(q, @commit_sql)} do
  #       {:ok, g, rows}
  #     ...
  #
  # then run `mix test --only integration test/integration/snapshot_test.exs:<line>`. Under the
  # 8-writer load the `materialized == expected` (and `assert_final_value_exactly_once`) assertion
  # fails: a pre-existing key is replayed with its stale (pre-update) value. Revert the mutation
  # (it is NOT part of this task's pathspec — `chunk_reader.ex` is untouched by the commit).

  ## ===========================================================================
  ## Tripwire 3 — resume mid-backfill (durable cursor, no re-scan)
  ## ===========================================================================

  test "tripwire 3: a killed backfill resumes from the durable cursor without re-emitting" do
    ctx = new_ctx()
    create_snapshot_table!(ctx)
    create_heartbeat_table!(ctx)
    seed_rows!(ctx, 1..@resume_n)

    w0 = MysqlCase.read_gtid_executed!(ctx.qconn)
    checkpoint = new_durable_checkpoint(ctx, w0)
    snap_store = new_durable_snapshot_store(ctx)
    configure_sink(ctx)

    # Run 1: a ticker keeps the write frontier ahead of the stream so chunks emit ONE at a time
    # (paced by the advance gate) — kill after 3 emit, so the durable cursor is mid-table.
    ticker1 = start_ticker(ctx)
    sup1 = start_snapshot!(ctx, checkpoint, snap_store, chunk_size: @resume_chunk)
    await_chunk_completions(3)
    stop_ticker(ticker1)
    MysqlCase.stop_pipeline(sup1)
    run1_chunks = 3 + count_chunk_completions()

    # A resumable cursor was persisted (not `:start`), and it did NOT already finish.
    state1 = DurableSnapshotStore.current(snap_store.table, snap_store.key)
    assert %Capstan.Snapshot.State{status: :snapshotting} = state1

    # Down-window changes (while the pipeline is DOWN) across backfilled AND not-yet ranges.
    resume_down_window!(ctx)

    # Run 2: restart from the SAME durable stores; resume the remaining chunks and complete.
    ticker2 = start_ticker(ctx)
    sup2 = start_snapshot!(ctx, checkpoint, snap_store, chunk_size: @resume_chunk)
    await_chunk_completions(1)
    stop_ticker(ticker2)
    drain_markers!(ctx)
    w_final = MysqlCase.read_gtid_executed!(ctx.qconn)
    await_checkpoint_covers(checkpoint, w_final, 4000)
    await_snapshot_completed(60_000)
    MysqlCase.stop_pipeline(sup2)

    # +1 for the completion consumed by `await_chunk_completions(1)` above (proving resume emitted).
    run2_chunks = 1 + count_chunk_completions()

    expected = resume_expected_map()
    assert db_final_map(ctx, @resume_n) == expected
    assert materialize(ctx.ledger, ctx.schema, ctx.table, 1..@resume_n) == expected

    # No re-emit: every chunk was emitted exactly once ACROSS both runs (a re-scan-from-zero
    # would push the total above the table's chunk count and duplicate the 1..3 range).
    assert_final_value_exactly_once(ctx, expected)
    total = div(@resume_n + @resume_chunk - 1, @resume_chunk)
    assert run1_chunks + run2_chunks == total
    assert run1_chunks >= 1 and run2_chunks >= 1
  end

  # Tripwire 3 RED (executed at build time): point the snapshot store at a FORGETFUL store
  # (`Capstan.SnapshotStore.InMemory`, process-lifetime — the cursor dies with the run-1 pipeline)
  # instead of `DurableSnapshotStore`. The `%State{status: :snapshotting} = state1` assertion then
  # fails RED with `right: nil` — the durable cursor was never persisted, so a restart reads no
  # progress and re-scans from `:start` (the re-scan-from-zero dup). The committed test uses the
  # durable store, so `state1` carries the mid-table cursor and the run resumes without re-emitting.

  ## ===========================================================================
  ## Tripwire 14 — non-vacuity of suppression (adversarial fixture ordering)
  ## ===========================================================================

  test "tripwire 14: a <=G change is suppressed+in-chunk; a >G change streams+absent-from-chunk" do
    ctx = new_ctx()
    create_snapshot_table!(ctx)
    create_heartbeat_table!(ctx)

    # Seed the RACING row (id 1) FIRST so a scope-then-limit chunk cannot fill its slot from other
    # rows; id 1 also has the lowest PK so it is always in the first chunk.
    seed_row!(ctx, 1, 10)
    Enum.each(2..6, &seed_row!(ctx, &1, &1 * 10))

    w_base = MysqlCase.read_gtid_executed!(ctx.qconn)
    checkpoint = new_durable_checkpoint(ctx, w_base)
    snap_store = new_durable_snapshot_store(ctx)
    configure_sink(ctx)

    # A change to the racing key (id 1) at `gtid <= G`, committed BEFORE the pipeline starts, then
    # heartbeats push the live watermark to `W_k` while the checkpoint stays at `W_base` — so the
    # chunk's `G = W_k` reflects the change, yet the stream (from `W_base`) still processes it and
    # the cursor-gate SUPPRESSES it (id 1 > cursor `:start`).
    MysqlCase.run!(ctx.qconn, "UPDATE #{q(ctx.schema, ctx.table)} SET v = 111 WHERE id = 1")
    heartbeats!(ctx, 10)

    sup = start_snapshot!(ctx, checkpoint, snap_store, chunk_size: 100)
    await_snapshot_completed(30_000)

    # A change to id 2 at `gtid > G` (after the whole table is backfilled) streams; its chunk row
    # still holds the OLD value (the change is absent from the chunk).
    MysqlCase.run!(ctx.qconn, "UPDATE #{q(ctx.schema, ctx.table)} SET v = 222 WHERE id = 2")
    w_after = MysqlCase.read_gtid_executed!(ctx.qconn)
    await_checkpoint_covers(checkpoint, w_after, 2000)
    MysqlCase.stop_pipeline(sup)

    # id 1 (changed <= G): PRESENT in the chunk with its NEW value, ABSENT from stream delivery.
    assert ledger_values(ctx, 1, :chunk) == ["111"]
    assert ledger_values(ctx, 1, :stream) == []

    # id 2 (changed > G): PRESENT in the stream with its NEW value, and its chunk row is the OLD
    # value (the change is absent from the chunk).
    assert ledger_values(ctx, 2, :stream) == ["222"]
    assert ledger_values(ctx, 2, :chunk) == ["20"]
  end

  ## ===========================================================================
  ## Tripwire 10 (integration arm) — advance gate under filtered + DDL
  ## ===========================================================================

  test "tripwire 10: filtered transactions and a DDL on a non-snapshot table fire the advance gate" do
    ctx = new_ctx()
    create_snapshot_table!(ctx)
    create_heartbeat_table!(ctx)
    seed_rows!(ctx, 1..5)

    w_base = MysqlCase.read_gtid_executed!(ctx.qconn)
    checkpoint = new_durable_checkpoint(ctx, w_base)
    snap_store = new_durable_snapshot_store(ctx)
    configure_sink(ctx)

    # Advance the live watermark past the chunk's `G` using ONLY filtered traffic (heartbeats on the
    # non-captured `hb` table) and a DDL on that non-snapshot table — the DDL committed LAST, so the
    # gate can only reach `G` by observing the DDL's self-committing advance (the choke-point hook).
    heartbeats!(ctx, 15)
    MysqlCase.run!(ctx.qconn, "ALTER TABLE #{q(ctx.schema, ctx.heartbeat)} ADD COLUMN extra INT")

    sup = start_snapshot!(ctx, checkpoint, snap_store, chunk_size: 100)

    # The advance gate fires despite only filtered + DDL traffic advancing past `G` → the backfill
    # completes (a hook that omitted DDL would leave the gate stalled → this times out RED).
    await_snapshot_completed(30_000)
    MysqlCase.stop_pipeline(sup)

    # Non-vacuity: the backfill actually delivered the five pre-existing rows through the gate.
    assert chunk_deliveries(ctx) == 5
  end

  ## ===========================================================================
  ## shared context + substrate lifecycle
  ## ===========================================================================

  defp new_ctx do
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

    %{qconn: qconn, schema: schema, table: "t", heartbeat: "hb", ledger: MysqlCase.new_ledger()}
  end

  defp create_snapshot_table!(ctx) do
    MysqlCase.run!(
      ctx.qconn,
      "CREATE TABLE #{q(ctx.schema, ctx.table)} " <>
        "(id INT PRIMARY KEY, v INT NOT NULL) ENGINE=InnoDB"
    )
  end

  defp create_heartbeat_table!(ctx) do
    MysqlCase.run!(
      ctx.qconn,
      "CREATE TABLE #{q(ctx.schema, ctx.heartbeat)} " <>
        "(id INT PRIMARY KEY, n INT NOT NULL) ENGINE=InnoDB"
    )
  end

  defp seed_rows!(ctx, id_range) do
    id_range
    |> Enum.map(fn id -> "(#{id}, #{id})" end)
    |> Enum.chunk_every(200)
    |> Enum.each(fn batch ->
      MysqlCase.run!(
        ctx.qconn,
        "INSERT INTO #{q(ctx.schema, ctx.table)} (id, v) VALUES " <> Enum.join(batch, ", ")
      )
    end)
  end

  defp seed_row!(ctx, id, v) do
    MysqlCase.run!(
      ctx.qconn,
      "INSERT INTO #{q(ctx.schema, ctx.table)} (id, v) VALUES (#{id}, #{v})"
    )
  end

  # A dotted, backtick-quoted `schema`.`table`.
  defp q(schema, table), do: "`#{schema}`.`#{table}`"

  # `n` FILTERED heartbeat commits on the non-captured `hb` table — each advances the processed
  # watermark (the filtered/empty path) without a sink delivery.
  defp heartbeats!(ctx, n) do
    Enum.each(1..n, fn i ->
      MysqlCase.run!(
        ctx.qconn,
        "INSERT INTO #{q(ctx.schema, ctx.heartbeat)} (id, n) VALUES (#{100_000 + i}, #{i})"
      )
    end)
  end

  ## ===========================================================================
  ## pipeline start + stores
  ## ===========================================================================

  defp new_durable_checkpoint(_ctx, seed) do
    table = DurableStore.new_table()
    :ok = DurableStore.seed(table, :checkpoint, seed)
    %{table: table, key: :checkpoint}
  end

  defp new_durable_snapshot_store(_ctx) do
    %{table: DurableSnapshotStore.new_table(), key: :snapshot}
  end

  defp configure_sink(ctx) do
    SnapshotSink.configure(%{
      pid: self(),
      ledger: ctx.ledger,
      pk_columns: ["id"],
      pk_types: [:int],
      value_column: "v"
    })
  end

  defp start_snapshot!(ctx, checkpoint, snap_store, opts) do
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
        max_command_retries: 5
      )

    on_exit(fn -> MysqlCase.stop_pipeline(sup) end)
    sup
  end

  ## ===========================================================================
  ## concurrent writers (tripwire 1)
  ## ===========================================================================

  # Eight writers, each a DISJOINT slice of the update band so every pre-existing key is updated
  # at most once (final state deterministic). Writer 0 also owns the delete band. Every writer
  # inserts a disjoint block of brand-new keys (not-yet-backfilled-range INSERTs — pure load, not
  # asserted). Each writer's ops are interleaved so the concurrent commit stream overlaps the
  # backfill's chunk captures.
  defp spawn_writers(ctx) do
    slice = div(@update_hi - @delete_hi, @writers)

    Enum.map(0..(@writers - 1), fn w ->
      lo = @delete_hi + 1 + w * slice
      hi = if w == @writers - 1, do: @update_hi, else: lo + slice - 1
      Task.async(fn -> run_writer(ctx, w, lo..hi) end)
    end)
  end

  defp run_writer(ctx, w, update_range) do
    sock = MysqlCase.socket!(MysqlCase.query_connection())

    try do
      ctx
      |> writer_ops(w, update_range)
      |> Enum.each(fn sql ->
        MysqlCase.run!(sock, sql)
        # A small pace so the load lasts long enough to overlap the backfill (the chunk captures
        # must see live concurrent commits, not a settled table).
        Process.sleep(4)
      end)
    after
      MysqlCase.close!(sock)
    end
  end

  # Interleave this writer's updates, new-key inserts, and (writer 0 only) deletes.
  defp writer_ops(ctx, w, update_range) do
    updates = Enum.map(update_range, &update_sql(ctx, &1))
    inserts = Enum.map(0..29, &insert_sql(ctx, 500_000 + w * 1_000 + &1))
    deletes = if w == 0, do: Enum.map(1..@delete_hi, &delete_sql(ctx, &1)), else: []

    Enum.shuffle(updates ++ inserts ++ deletes)
  end

  defp update_sql(ctx, id),
    do: "UPDATE #{q(ctx.schema, ctx.table)} SET v = #{id + @offset} WHERE id = #{id}"

  defp insert_sql(ctx, id),
    do: "INSERT INTO #{q(ctx.schema, ctx.table)} (id, v) VALUES (#{id}, #{id})"

  defp delete_sql(ctx, id), do: "DELETE FROM #{q(ctx.schema, ctx.table)} WHERE id = #{id}"

  # A handful of FILTERED heartbeat commits (on the non-captured `hb` table) to push the watermark
  # past the last chunk's `G` after the writers stop, so the final chunk's advance gate fires.
  defp drain_markers!(ctx) do
    Enum.each(1..8, fn n ->
      MysqlCase.run!(
        ctx.qconn,
        "INSERT INTO #{q(ctx.schema, ctx.heartbeat)} (id, n) VALUES (#{n}, #{n})"
      )
    end)
  end

  ## ===========================================================================
  ## expected final state + assertions
  ## ===========================================================================

  # The deterministic FINAL state of the pre-existing set: 1..@delete_hi deleted (absent),
  # @delete_hi+1..@update_hi updated to `id + @offset`, the rest untouched at `id`.
  defp expected_present_map do
    Map.new(surviving_ids(), fn id ->
      value = if id <= @update_hi, do: id + @offset, else: id
      {id, Integer.to_string(value)}
    end)
  end

  defp surviving_ids, do: (@delete_hi + 1)..@n

  # The resume down-window update band spans keys likely-already-backfilled AND not-yet at the kill
  # point, so both the stream-forward and the future-chunk paths carry a change across the restart.
  defp resume_updated_ids, do: Enum.concat(20..40, 120..140)

  defp resume_down_window!(ctx) do
    Enum.each(resume_updated_ids(), fn id -> MysqlCase.run!(ctx.qconn, update_sql(ctx, id)) end)
  end

  defp resume_expected_map do
    updated = MapSet.new(resume_updated_ids())

    Map.new(1..@resume_n, fn id ->
      value = if MapSet.member?(updated, id), do: id + @offset, else: id
      {id, Integer.to_string(value)}
    end)
  end

  # The DB's actual final state for the pre-existing set (id => value string) — cross-checks the
  # expected fixture against what the writers really committed.
  defp db_final_map(ctx, hi) do
    ctx.qconn
    |> MysqlCase.query_rows!(
      "SELECT id, v FROM #{q(ctx.schema, ctx.table)} WHERE id BETWEEN 1 AND #{hi} ORDER BY id"
    )
    |> Map.new(fn [id, v] -> {String.to_integer(id), v} end)
  end

  # Replay every pre-existing-key delivery in delivery order (`seq`) as upsert-by-PK: an
  # insert/update/snapshot sets the key, a delete removes it. The converged map is the sink's
  # materialized view restricted to the pre-existing set.
  defp materialize(ledger, schema, table, id_range) do
    ledger
    |> MysqlCase.ledger_dump()
    |> Enum.filter(fn {{s, t, pk}, _e} -> s == schema and t == table and pk in id_range end)
    |> Enum.sort_by(fn {_key, entry} -> entry.seq end)
    |> Enum.reduce(%{}, &apply_delivery/2)
  end

  defp apply_delivery({{_s, _t, pk}, %{value: :deleted}}, acc), do: Map.delete(acc, pk)
  defp apply_delivery({{_s, _t, pk}, %{value: value}}, acc), do: Map.put(acc, pk, value)

  defp assert_final_value_exactly_once(ctx, expected) do
    entries = MysqlCase.ledger_dump(ctx.ledger)

    Enum.each(expected, fn {pk, final_value} ->
      count =
        Enum.count(entries, fn {{s, t, k}, e} ->
          s == ctx.schema and t == ctx.table and k == pk and e.value == final_value
        end)

      assert count == 1,
             "pre-existing key #{pk} final value #{inspect(final_value)} delivered #{count}×"
    end)
  end

  defp chunk_deliveries(ctx), do: count_source(ctx, :chunk)
  defp stream_deliveries(ctx), do: count_source(ctx, :stream)

  defp count_source(ctx, source) do
    ctx.ledger
    |> MysqlCase.ledger_dump()
    |> Enum.count(fn {{s, _t, _k}, e} -> s == ctx.schema and e.source == source end)
  end

  # The delivered values for `pk` via a specific source (`:chunk` or `:stream`) on ctx's table.
  defp ledger_values(ctx, pk, source) do
    ctx.ledger
    |> MysqlCase.ledger_dump()
    |> Enum.filter(fn {{s, t, k}, e} ->
      s == ctx.schema and t == ctx.table and k == pk and e.source == source
    end)
    |> Enum.map(fn {_key, e} -> e.value end)
  end

  ## ===========================================================================
  ## synchronisation
  ## ===========================================================================

  defp await_snapshot_completed(timeout) do
    receive do
      {:snapshot_event, :completed, _m, _md} -> :ok
      {:snapshot_event, :halt, _m, md} -> flunk("snapshot halted fail-closed: #{inspect(md)}")
    after
      timeout -> flunk("snapshot backfill did not complete within #{timeout}ms")
    end
  end

  defp await_chunk_completions(0), do: :ok

  defp await_chunk_completions(n) do
    receive do
      {:snapshot_event, :chunk_completed, _m, _md} -> await_chunk_completions(n - 1)
    after
      30_000 -> flunk("did not observe #{n} more chunk completions")
    end
  end

  # Drain (non-blocking) every chunk-completion currently queued — the count since the last await.
  defp count_chunk_completions(acc \\ 0) do
    receive do
      {:snapshot_event, :chunk_completed, _m, _md} -> count_chunk_completions(acc + 1)
    after
      0 -> acc
    end
  end

  # A background committer on the non-captured `hb` table (a single upserted row) that keeps the
  # live write frontier ahead of the stream, so the advance gate paces chunk emission one at a time
  # (letting a kill land cleanly mid-backfill). `stop_ticker/1` ends it.
  defp start_ticker(ctx) do
    spawn(fn ->
      sock = MysqlCase.socket!(MysqlCase.query_connection())
      tick_loop(ctx, sock)
    end)
  end

  defp tick_loop(ctx, sock) do
    receive do
      :stop -> MysqlCase.close!(sock)
    after
      15 ->
        MysqlCase.run_tolerant(
          sock,
          "INSERT INTO #{q(ctx.schema, ctx.heartbeat)} (id, n) VALUES (999_999, 1) " <>
            "ON DUPLICATE KEY UPDATE n = n + 1"
        )

        tick_loop(ctx, sock)
    end
  end

  defp stop_ticker(pid) do
    send(pid, :stop)
    :ok
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

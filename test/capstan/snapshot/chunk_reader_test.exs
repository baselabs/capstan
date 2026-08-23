defmodule Capstan.Snapshot.ChunkReaderTest do
  @moduledoc """
  `Capstan.Snapshot.ChunkReader` — the brief-lock exact-`G` capture (C2 Task 5, design Ch1).

  Two vector classes:

    * **Unit** (default suite) — a scripted mock MySQL server drives the REAL
      `Command.query` / `Capstan.Query.query` path over a real loopback socket, so the pinned
      capture SEQUENCE, the positional fault classification, the budgeted retry, the per-chunk
      fingerprint drift halt, chunk construction, and the Rule-1 cursor-as-literal containment
      are all exercised against genuine framing (never a hollow double).
    * **Live** (`@tag :live`, excluded by default — `mix test --only live`) — THE linchpin:
      the exact-`G` lower-bound proof under active concurrent writers on `mysql-cdc-probe`, its
      non-vacuity control (drop the `LOCK TABLES` → the invariant goes RED), basic paging
      correctness, a schema-drift halt, and a lock-wait-timeout halt.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Capstan.Gtid
  alias Capstan.Protocol.Command
  alias Capstan.Protocol.Handshake
  alias Capstan.Query
  alias Capstan.Snapshot.Chunk
  alias Capstan.Snapshot.ChunkReader

  @loopback {127, 0, 0, 1}

  # The default mock table: PK `id INT`, columns `(id INT, v INT)`.
  @statistics [["PRIMARY", "1", "id", "0"]]
  @pk_columns [["id", "int", "int", "NO", nil, nil], ["v", "int", "int", "NO", nil, nil]]
  @cr_columns [["id", "int", "1"], ["v", "int", "2"]]
  @gtid "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"

  ## ===========================================================================
  ## Unit — the pinned capture SEQUENCE (design § Pinned decisions #2)
  ## ===========================================================================

  describe "read_chunk/2 — the pinned brief-lock capture sequence, in exact order" do
    test "issues SET → LOCK → gtid → START TXN → UNLOCK → chunk SELECT → COMMIT" do
      {reader, agent} = open_reader(default_cfg())

      assert {:ok, %Chunk{}, _final?, _reader} = ChunkReader.read_chunk(reader, :start)

      bracket = capture_bracket(recorded(agent))

      assert bracket == [
               "SET SESSION lock_wait_timeout = 5",
               "LOCK TABLES `test`.`t` READ",
               "SELECT @@global.gtid_executed",
               "START TRANSACTION WITH CONSISTENT SNAPSHOT",
               "UNLOCK TABLES",
               "SELECT `id`, `v` FROM `test`.`t` ORDER BY `id` LIMIT 4097",
               "COMMIT"
             ]
    end

    test "the bounded lock_wait_timeout is SET strictly BEFORE the LOCK (else a contended lock wedges)" do
      {reader, agent} = open_reader(default_cfg())

      assert {:ok, _chunk, _final?, _reader} = ChunkReader.read_chunk(reader, :start)

      sql = recorded(agent)
      set_idx = Enum.find_index(sql, &String.starts_with?(&1, "SET SESSION lock_wait_timeout"))
      lock_idx = Enum.find_index(sql, &String.starts_with?(&1, "LOCK TABLES"))

      assert set_idx < lock_idx
    end

    test "a per-chunk fingerprint COLUMNS read precedes the capture bracket (tripwire 8 guard)" do
      {reader, agent} = open_reader(default_cfg())

      assert {:ok, _chunk, _final?, _reader} = ChunkReader.read_chunk(reader, :start)

      sql = recorded_after_open(agent)
      fp_idx = Enum.find_index(sql, &(&1 =~ "information_schema.COLUMNS"))
      lock_idx = Enum.find_index(sql, &String.starts_with?(&1, "LOCK TABLES"))

      assert fp_idx < lock_idx
    end
  end

  ## ===========================================================================
  ## Unit — chunk construction: G, row images, max_pk, seq, finality, paging
  ## ===========================================================================

  describe "read_chunk/2 — produces a %Chunk{} with G, row images, and the canonical max_pk" do
    test "carries the captured G, the full row images, and the last row's canonical PK" do
      cfg = default_cfg(chunk_rows: [["1", "10"], ["2", "20"], ["3", "30"]])
      {reader, _agent} = open_reader(cfg)

      assert {:ok, chunk, final?, reader2} = ChunkReader.read_chunk(reader, :start)

      assert %Chunk{table: {"test", "t"}, seq: 0, g: @gtid} = chunk

      assert chunk.rows == [
               %{"id" => "1", "v" => "10"},
               %{"id" => "2", "v" => "20"},
               %{"id" => "3", "v" => "30"}
             ]

      # max_pk is the canonical PK of the last row (id=3), an integer via PrimaryKey.canonical/2.
      assert chunk.max_pk == 3
      # 3 rows < chunk_size ⇒ final chunk; the returned reader advances the sequence number.
      assert final? == true
      assert reader2.seq == 1
    end

    test "an exact-multiple table's last full chunk is final? via the look-ahead (closeout F6)" do
      # A table whose row count is an exact multiple of chunk_size: every page is a FULL chunk_size
      # rows, so a `length < chunk_size` finality test would NEVER mark the last page final and the
      # sink would never learn the table completed. The `LIMIT chunk_size + 1` look-ahead fixes it:
      # a full page WITH a further look-ahead row is not final; the last page (no further row) IS.
      cfg = default_cfg(chunk_size: 2, chunk_rows: [["1", "10"], ["2", "20"], ["3", "30"]])
      {reader, agent} = open_reader(cfg)

      # Page 1: the look-ahead read returns 3 rows (chunk_size + 1) ⇒ a further chunk exists. Keep 2,
      # NOT final; the cursor advances to the 2nd row (id 2) — the look-ahead row (id 3) is dropped,
      # re-read as the head of page 2.
      assert {:ok, chunk1, false, reader2} = ChunkReader.read_chunk(reader, :start)
      assert Enum.map(chunk1.rows, & &1["id"]) == ["1", "2"]
      assert chunk1.max_pk == 2

      # Page 2: only 2 rows exist beyond id 2 (== chunk_size, no look-ahead row) ⇒ this IS the final
      # chunk. RED (the pre-fix `length < chunk_size`): 2 rows == chunk_size ⇒ final?=false, so the
      # sink only ever saw a silent {:done} next — never final_chunk?: true for this table.
      set_chunk_rows(agent, [["3", "30"], ["4", "40"]])
      assert {:ok, chunk2, true, _reader3} = ChunkReader.read_chunk(reader2, chunk1.max_pk)
      assert Enum.map(chunk2.rows, & &1["id"]) == ["3", "4"]
      assert chunk2.max_pk == 4
    end

    test "an empty page MID-table returns :done (the drained cursor after non-empty pages)" do
      cfg = default_cfg(chunk_size: 2, chunk_rows: [])
      {reader, _agent} = open_reader(cfg)

      assert {:done, ^reader} = ChunkReader.read_chunk(reader, 5)
    end

    test "a ZERO-ROW table delivers an EMPTY FINAL chunk on its first read (C2c)" do
      # The sink's per-table completion signal is a `final_chunk?: true` beat; a
      # table with no rows must deliver exactly one — empty — or a sink gating
      # per-table readiness on it waits forever. RED (old behavior): the empty
      # first read returned a silent {:done} and the sink heard NOTHING.
      cfg = default_cfg(chunk_size: 2, chunk_rows: [])
      {reader, _agent} = open_reader(cfg)

      assert {:ok, %Chunk{rows: [], max_pk: nil}, true, _reader2} =
               ChunkReader.read_chunk(reader, :start)
    end

    test "the WHERE keyset predicate uses `pk > cursor` for a non-:start cursor" do
      {reader, agent} = open_reader(default_cfg())

      assert {:ok, _chunk, _final?, _reader} = ChunkReader.read_chunk(reader, 41)

      chunk_select = Enum.find(recorded(agent), &String.starts_with?(&1, "SELECT `id`, `v` FROM"))

      assert chunk_select ==
               "SELECT `id`, `v` FROM `test`.`t` WHERE `id` > 41 ORDER BY `id` LIMIT 4097"
    end
  end

  describe "read_chunk/2 — composite and binary PK SQL" do
    test "a composite (INT, VARBINARY) PK pages with a row-value keyset and a hex binary literal" do
      cfg =
        composite_cfg()
        |> Map.put(:chunk_rows, [["7", <<0xAB, 0xCD>>]])

      {reader, agent} = open_reader(cfg)

      # cursor is a composite canonical PK: {int, binary}.
      assert {:ok, chunk, _final?, _reader} = ChunkReader.read_chunk(reader, {3, <<0x01, 0x02>>})

      chunk_select = Enum.find(recorded(agent), &String.starts_with?(&1, "SELECT `a`, `b` FROM"))

      assert chunk_select ==
               "SELECT `a`, `b` FROM `test`.`t` WHERE (`a`, `b`) > (3, X'0102') " <>
                 "ORDER BY `a`, `b` LIMIT 4097"

      # max_pk canonicalizes the composite last row: {7, <<0xAB, 0xCD>>}.
      assert chunk.max_pk == {7, <<0xAB, 0xCD>>}
    end
  end

  ## ===========================================================================
  ## Unit — string PK SQL + the dual cursor (ADR-0012)
  ##
  ## A string PK table pages under its COLLATION: the pk column projects as its raw column
  ## bytes (`CAST(col AS BINARY)` — the binlog-delivered form), a WEIGHT_STRING column rides
  ## along for the cursor, and the keyset literal is COLLATE-pinned (an unpinned CONVERT
  ## raises ERROR 1267 on non-default collations — measured). max_pk is the DUAL cursor
  ## %{raw, weight}; the weight columns NEVER appear in delivered rows.
  ## ===========================================================================

  describe "read_chunk/2 — string PK: CAST projection, WEIGHT_STRING column, pinned literal" do
    test "the chunk SELECT carries CAST(pk AS BINARY), the projection, and WEIGHT_STRING(pk)" do
      cfg = varchar_cfg() |> Map.put(:chunk_rows, [["Z", "5", <<0x1F, 0x21>>]])
      {reader, agent} = open_reader(cfg)

      assert {:ok, %Chunk{}, _final?, _reader} = ChunkReader.read_chunk(reader, :start)

      chunk_select = chunk_select_of(recorded(agent))

      assert chunk_select ==
               "SELECT CAST(`code` AS BINARY), `v`, WEIGHT_STRING(`code`) " <>
                 "FROM `test`.`t` ORDER BY `code` LIMIT 4097"
    end

    test "a non-:start cursor pages by the COLLATE-pinned CONVERT literal over the raw bytes" do
      cfg = varchar_cfg() |> Map.put(:chunk_rows, [["zed", "9", <<0x1F, 0x31>>]])
      {reader, agent} = open_reader(cfg)

      # The dual cursor: raw (the page form) + weight (the gate form).
      cursor = %{raw: "Z", weight: <<0x1F, 0x21>>}

      assert {:ok, %Chunk{}, _final?, _reader} = ChunkReader.read_chunk(reader, cursor)

      assert chunk_select_of(recorded(agent)) ==
               "SELECT CAST(`code` AS BINARY), `v`, WEIGHT_STRING(`code`) " <>
                 "FROM `test`.`t` WHERE `code` > CONVERT(X'5A' USING utf8mb4) " <>
                 "COLLATE utf8mb4_0900_ai_ci ORDER BY `code` LIMIT 4097"
    end

    test "a VIOLATED order contract refuses open :snapshot_collation_contract_violated (the canary tripwire)" do
      # ADR-0012's residual, made loud: a future server whose WEIGHT_STRING byte order
      # diverges from ORDER BY would mis-gate silently. The bootstrap canary compares the
      # two orders over a fixed ASCII vector per string pk column — a disagreement halts
      # fail-closed BEFORE any chunk or gate decision. RED (pre-tripwire): open succeeded.
      cfg = varchar_cfg(contract: :violated)
      {query, _agent} = start_mock(build_cfg(cfg))

      assert {:error, :snapshot_collation_contract_violated} =
               ChunkReader.open(query, {"test", "t"})
    end

    test "the pin names the COLUMN's charset and collation (latin1 / non-default collations)" do
      for {charset, collation} <- [
            {"latin1", "latin1_swedish_ci"},
            {"utf8mb4", "utf8mb4_0900_as_cs"}
          ] do
        cfg =
          varchar_cfg(
            pk_columns: [["code", "varchar", "varchar(64)", "NO", charset, collation]],
            chunk_rows: [["zed", "9", <<0x1F, 0x31>>]]
          )

        {reader, agent} = open_reader(cfg)

        cursor = %{raw: "Z", weight: <<0x1F, 0x21>>}
        assert {:ok, %Chunk{}, _final?, _reader} = ChunkReader.read_chunk(reader, cursor)

        assert chunk_select_of(recorded(agent)) =~
                 "WHERE `code` > CONVERT(X'5A' USING #{charset}) COLLATE #{collation}"
      end
    end

    test "max_pk is the DUAL cursor: %{raw: column bytes, weight: server weight}" do
      cfg =
        varchar_cfg()
        |> Map.put(:chunk_rows, [["Z", "5", <<0x1F, 0x21>>], ["a", "6", <<0x1C, 0x47>>]])

      {reader, _agent} = open_reader(cfg)

      assert {:ok, chunk, _final?, _reader} = ChunkReader.read_chunk(reader, :start)

      assert chunk.max_pk == %{raw: "a", weight: <<0x1C, 0x47>>}
    end

    test "delivered rows carry ONLY the table columns — the weight column never rides (finding 6)" do
      cfg = varchar_cfg() |> Map.put(:chunk_rows, [["Z", "5", <<0x1F, 0x21>>]])
      {reader, _agent} = open_reader(cfg)

      assert {:ok, chunk, _final?, _reader} = ChunkReader.read_chunk(reader, :start)

      assert [%{"code" => "Z", "v" => "5"}] = chunk.rows
      assert Enum.map(chunk.rows, &Map.keys/1) == [["code", "v"]] |> Enum.map(&Enum.sort/1)
    end

    test "a composite (INT, VARCHAR) PK mixes ints and pinned CONVERTs in the row-value keyset" do
      cfg =
        int_varchar_cfg()
        |> Map.put(:chunk_rows, [["1", "a", "9", <<0x1C, 0x47>>]])

      {reader, agent} = open_reader(cfg)

      cursor = %{raw: {0, "zed"}, weight: {0, <<0x1F, 0x31>>}}

      assert {:ok, chunk, _final?, _reader} = ChunkReader.read_chunk(reader, cursor)

      assert chunk_select_of(recorded(agent)) ==
               "SELECT `i`, CAST(`k` AS BINARY), `v`, WEIGHT_STRING(`k`) " <>
                 "FROM `test`.`t` WHERE (`i`, `k`) > (0, CONVERT(X'7A6564' USING utf8mb4) " <>
                 "COLLATE utf8mb4_0900_ai_ci) ORDER BY `i`, `k` LIMIT 4097"

      assert chunk.max_pk == %{raw: {1, "a"}, weight: {1, <<0x1C, 0x47>>}}
    end
  end

  ## ===========================================================================
  ## Unit — positional fault classification (the Task-4 forward-finding)
  ##
  ## `Capstan.Query.query/2` scrubs the MySQL error CODE, so lock-wait-timeout and a
  ## chunk-read fault return the same value-free atom. The reader distinguishes them by
  ## WHICH STATEMENT failed: the LOCK step → :snapshot_lock_unavailable; the chunk SELECT
  ## (and the view statements) → :snapshot_chunk_read_failed.
  ## ===========================================================================

  describe "read_chunk/2 — positional halt classification" do
    test "a fault ON the LOCK statement halts :snapshot_lock_unavailable" do
      cfg = default_cfg(fault: {:lock, :always}, max_retries: 0)
      {reader, _agent} = open_reader(cfg)

      assert {:error, :snapshot_lock_unavailable} = ChunkReader.read_chunk(reader, :start)
    end

    test "a fault ON the chunk SELECT halts :snapshot_chunk_read_failed" do
      cfg = default_cfg(fault: {:chunk, :always}, max_retries: 0)
      {reader, _agent} = open_reader(cfg)

      assert {:error, :snapshot_chunk_read_failed} = ChunkReader.read_chunk(reader, :start)
    end

    test "a fault reading @@global.gtid_executed (lock held) is a read fault, NOT lock_unavailable" do
      cfg = default_cfg(fault: {:gtid, :always}, max_retries: 0)
      {reader, _agent} = open_reader(cfg)

      # Once the lock is HELD, any subsequent fault is a chunk-read fault — the lock WAS obtained.
      assert {:error, :snapshot_chunk_read_failed} = ChunkReader.read_chunk(reader, :start)
    end
  end

  ## ===========================================================================
  ## Unit — the budgeted retry reuses CheckpointStore.retry_decision/2 (no re-derived counter)
  ## ===========================================================================

  describe "read_chunk/2 — budgeted retry over a transient fault" do
    test "a lock fault is retried the budgeted number of times, then halts (attempts counted)" do
      cfg = default_cfg(fault: {:lock, :always}, max_retries: 3)
      {reader, agent} = open_reader(cfg)

      assert {:error, :snapshot_lock_unavailable} = ChunkReader.read_chunk(reader, :start)

      # retry_decision/2: attempts 0,1,2 retry; attempt 3 halts ⇒ 4 LOCK attempts total.
      lock_attempts = Enum.count(recorded(agent), &String.starts_with?(&1, "LOCK TABLES"))
      assert lock_attempts == 4
    end

    test "a lock fault that clears within budget succeeds (non-vacuity of the retry)" do
      # Fail the LOCK twice, then let it succeed on the 3rd attempt.
      cfg = default_cfg(fault: {:lock, 2}, max_retries: 5)
      {reader, agent} = open_reader(cfg)

      assert {:ok, %Chunk{}, _final?, _reader} = ChunkReader.read_chunk(reader, :start)

      lock_attempts = Enum.count(recorded(agent), &String.starts_with?(&1, "LOCK TABLES"))
      assert lock_attempts == 3

      # A failed attempt cleans up (ROLLBACK + UNLOCK) before retrying, so the lock is never orphaned.
      assert Enum.count(recorded(agent), &(&1 == "ROLLBACK")) == 2
    end
  end

  ## ===========================================================================
  ## Unit — schema drift (tripwire 8): a changed fingerprint is a PERMANENT halt
  ## ===========================================================================

  describe "read_chunk/2 — per-chunk structural fingerprint" do
    test "a changed column structure halts :snapshot_schema_drifted, permanently (no retry)" do
      {reader, agent} = open_reader(default_cfg(max_retries: 5))

      # A DDL adds a column between open and the chunk read: the per-chunk fingerprint differs.
      set_columns(agent, [["id", "int", "1"], ["v", "int", "2"], ["w", "bigint", "3"]])

      assert {:error, :snapshot_schema_drifted} = ChunkReader.read_chunk(reader, :start)

      # PERMANENT: exactly ONE fingerprint COLUMNS read after open, and NO LOCK was ever taken.
      after_open = recorded_after_open(agent)
      assert Enum.count(after_open, &(&1 =~ "information_schema.COLUMNS")) == 1
      refute Enum.any?(after_open, &String.starts_with?(&1, "LOCK TABLES"))
    end

    test "an unchanged column structure does NOT drift (non-vacuity)" do
      {reader, _agent} = open_reader(default_cfg())
      assert {:ok, %Chunk{}, _final?, _reader} = ChunkReader.read_chunk(reader, :start)
    end
  end

  ## ===========================================================================
  ## Unit — Rule 1 (tripwire 13): the cursor is a SQL literal that leaks NOWHERE on failure
  ## ===========================================================================

  describe "read_chunk/2 — Rule 1: the cursor value appears in no error or log on a chunk fault" do
    @sentinel_cursor 424_242_424_242

    test "a chunk-read fault returns a bare atom and leaks the cursor into no log" do
      cfg = default_cfg(fault: {:chunk, :always}, max_retries: 0)
      {reader, _agent} = open_reader(cfg)

      {result, log} =
        with_log(fn -> ChunkReader.read_chunk(reader, @sentinel_cursor) end)

      assert {:error, reason} = result
      assert reason == :snapshot_chunk_read_failed
      # The failing SQL carried `WHERE id > 424242424242`; it must appear in NO error or log.
      refute inspect(reason) =~ "424242424242"
      refute log =~ "424242424242"
    end
  end

  ## ===========================================================================
  ## Unit — open/3 surfaces the introspection halts unchanged
  ## ===========================================================================

  describe "open/3 — PK introspection halts surface unchanged" do
    test "a table with no PK and no NOT-NULL unique key halts :snapshot_table_no_primary_key" do
      cfg = default_cfg(statistics: [], pk_columns: @pk_columns)
      {query, _agent} = start_mock(build_cfg(cfg))

      assert {:error, :snapshot_table_no_primary_key} = ChunkReader.open(query, {"test", "t"})
    end
  end

  ## ===========================================================================
  ## Live marquee — real substrate, excluded by default (mix test --only live)
  ## ===========================================================================

  describe "live — exact-G lower bound under concurrent writers (tripwire 2, THE linchpin)" do
    @describetag :live

    test "GREEN: the brief-lock capture — every change with gtid ≤ G is visible in the chunk" do
      {:ok, query} =
        Query.establish(connection: live_conn(schema()), connect_fun: &Query.default_connect/1)

      {:ok, reader} = ChunkReader.open(query, {schema(), "t"}, chunk_size: 100)

      capture_fun = fn ->
        case ChunkReader.read_chunk(reader, :start) do
          {:ok, chunk, _final?, _r} -> {chunk.g, seq_view(chunk.rows)}
          # A lock-wait timeout under relentless load is fail-closed, not a correctness signal — skip it.
          {:error, _reason} -> :skip
          # An empty page before the writers have seeded their rows — skip it.
          {:done, _r} -> :skip
        end
      end

      %{captures: captures, violations: violations} = run_exact_g_proof(capture_fun)

      # The locked capture is an EXACT lower bound: 0 violations across every capture under load.
      assert captures != []

      assert violations == [],
             "locked capture violated the exact-G lower bound: #{inspect(violations)}"

      Query.close(query)
    end

    test "RED (non-vacuity): drop the LOCK (bare gtid_executed read) → the invariant FAILS under load" do
      {:ok, query} =
        Query.establish(connection: live_conn(schema()), connect_fun: &Query.default_connect/1)

      # The lock-free attempt to pair a consistent-snapshot view with an exact GTID — the bracket
      # the design REMOVED. This reproduces the design's § Verified probe: WITHOUT the lock there is
      # no way to atomically pair the frozen view with a position, and an in-transaction
      # `@@GLOBAL.gtid_executed` read returns the LIVE global (ahead of the frozen view). So G
      # overshoots the chunk: a change with gtid ≤ G is absent from the chunk — exactly the invariant
      # negation, and exactly the silent corruption (stale chunk row overwrites a fresh streamed row)
      # that the brief LOCK TABLES prevents by capturing G and the view together under quiescence.
      bare_capture = fn ->
        {:ok, _} = Query.query(query, "START TRANSACTION WITH CONSISTENT SNAPSHOT")
        {:ok, [[g]]} = Query.query(query, "SELECT @@GLOBAL.gtid_executed")
        {:ok, rows} = Query.query(query, "SELECT `id`, `seq` FROM #{qualified()} ORDER BY `id`")
        {:ok, _} = Query.query(query, "COMMIT")
        {g, seq_view(Enum.map(rows, fn [id, seq] -> %{"id" => id, "seq" => seq} end))}
      end

      %{captures: captures, violations: violations} = run_exact_g_proof(bare_capture)

      assert captures != []

      # Non-vacuity: the lock-free bracket MUST corrupt under load (else the guard is vacuous).
      assert violations != [],
             "bare capture showed NO lead across #{Enum.count(captures)} captures — " <>
               "increase the writer count / iterations to reproduce the load-correlated lead"

      Query.close(query)
    end
  end

  describe "live — paging, drift, and lock-wait halts" do
    @describetag :live

    test "basic paging: G is a canonical GTID string, rows/max_pk are correct, chunks tile the table" do
      {:ok, query} =
        Query.establish(connection: live_conn(schema()), connect_fun: &Query.default_connect/1)

      quiesce_seed(query, 250)
      {:ok, reader} = ChunkReader.open(query, {schema(), "t"}, chunk_size: 100)

      ids = drain_all(reader, :start, [])

      # Every seeded id 1..250 appears exactly once, in order (no gap, no dup).
      assert ids == Enum.to_list(1..250)

      Query.close(query)
    end

    test "an exact-multiple table's last FULL chunk is final? live — the look-ahead, not a short page (closeout F6)" do
      {:ok, query} =
        Query.establish(connection: live_conn(schema()), connect_fun: &Query.default_connect/1)

      # Exactly 2 * chunk_size rows: BOTH pages are a full chunk_size, the last one included. This is
      # the case the mock unit test (`set_chunk_rows`) cannot prove — it hand-feeds each page's rows,
      # so it never exercises real MySQL returning exactly chunk_size rows for `LIMIT chunk_size + 1`
      # on the final page. Here the look-ahead runs against `mysql-cdc-probe`: the final page's read
      # returns chunk_size rows (no chunk_size+1'th row exists) ⇒ final?: true. RED against the pre-F6
      # `length < chunk_size` heuristic — a full page (50 == chunk_size) would report final?: false and
      # the sink would learn completion only via a later silent {:done}, never a final_chunk? beat.
      quiesce_seed(query, 100)
      {:ok, reader} = ChunkReader.open(query, {schema(), "t"}, chunk_size: 50)

      pages = drain_pages(reader, :start, [])

      # Two FULL chunk_size pages — the last is not a short page.
      assert Enum.map(pages, fn {ids, _final?} -> length(ids) end) == [50, 50]

      # The last full page IS final via the look-ahead; the first is not. (Pre-F6 ⇒ [false, false].)
      assert Enum.map(pages, fn {_ids, final?} -> final? end) == [false, true]

      # Exact tiling across the two pages: ids 1..100 once, in order — no row skipped or duplicated.
      assert Enum.flat_map(pages, fn {ids, _final?} -> ids end) == Enum.to_list(1..100)

      Query.close(query)
    end

    test "a DDL on the snapshot table mid-backfill halts :snapshot_schema_drifted" do
      {:ok, query} =
        Query.establish(connection: live_conn(schema()), connect_fun: &Query.default_connect/1)

      {:ok, reader} = ChunkReader.open(query, {schema(), "t"}, chunk_size: 10)

      # Alter the table structure after the baseline fingerprint was captured at open.
      root = root_socket()
      run!(root, "ALTER TABLE #{qualified()} ADD COLUMN drift_col INT NULL")
      close_socket(root)

      assert {:error, :snapshot_schema_drifted} = ChunkReader.read_chunk(reader, :start)

      # Restore for other tests sharing the schema.
      root2 = root_socket()
      run!(root2, "ALTER TABLE #{qualified()} DROP COLUMN drift_col")
      close_socket(root2)

      Query.close(query)
    end

    test "a lock-wait timeout beyond the budget halts :snapshot_lock_unavailable" do
      {:ok, query} =
        Query.establish(connection: live_conn(schema()), connect_fun: &Query.default_connect/1)

      # A bounded 1s wait, halt-now (no retries).
      {:ok, reader} =
        ChunkReader.open(query, {schema(), "t"}, lock_wait_timeout: 1, max_retries: 0)

      # Hold a conflicting WRITE lock from a separate session so the reader's READ lock times out.
      blocker = root_socket()
      run!(blocker, "LOCK TABLES #{qualified()} WRITE")

      assert {:error, :snapshot_lock_unavailable} = ChunkReader.read_chunk(reader, :start)

      run!(blocker, "UNLOCK TABLES")
      close_socket(blocker)
      Query.close(query)
    end
  end

  ## ---------------------------------------------------------------------------
  ## Live — the exact-G proof harness
  ## ---------------------------------------------------------------------------

  # Runs W single-writer-per-row hammer tasks (concurrent → group-commit lead on @@gtid_executed)
  # while the given capture_fun captures {G, seq_view} many times. Each increment is assigned its
  # EXACT own gtid via `gtid_next` (a fresh random uuid per run, so gnos never collide with a prior
  # run's gtid_executed), giving a tight ground truth. After the storm, each capture is verified:
  # for row j, i_lower = the largest local_seq whose OWN gtid is a member of G — every such change
  # is ≤ G and MUST be visible, so seq_view[j] ≥ i_lower or the exact-G lower bound was violated.
  defp run_exact_g_proof(capture_fun, opts \\ []) do
    writers = Keyword.get(opts, :writers, 8)
    iterations = Keyword.get(opts, :iterations, 300)

    reset_hot_rows(writers)

    fake_uuid = random_uuid()
    next_gno = :atomics.new(1, [])
    done = :atomics.new(1, [])

    tasks =
      Enum.map(1..writers, fn j ->
        Task.async(fn ->
          ledger = hammer_row(j, iterations, next_gno, fake_uuid)
          :atomics.add(done, 1, 1)
          {j, ledger}
        end)
      end)

    captures =
      capture_fun
      |> capture_until_done(done, writers, [])
      |> Enum.reject(&(&1 == :skip))

    ledgers = tasks |> Task.await_many(120_000) |> Map.new()

    violations =
      for {g, seq_view} <- captures,
          j <- Map.keys(ledgers),
          violation = row_violation(j, g, seq_view, ledgers, fake_uuid),
          violation != nil,
          do: violation

    %{captures: captures, violations: violations}
  end

  defp capture_until_done(capture_fun, done, writers, acc) do
    if :atomics.get(done, 1) >= writers do
      Enum.reverse(acc)
    else
      capture_until_done(capture_fun, done, writers, [capture_fun.() | acc])
    end
  end

  # A single writer owns row j (INSERT once, then increment seq). Each increment's transaction is
  # given an EXACT own gtid via `gtid_next = '<fake_uuid>:<n>'` (n a globally-unique gno), so the
  # ledger maps local_seq i → own gno n_i precisely — no superset pollution from concurrent writers.
  defp hammer_row(j, iterations, next_gno, fake_uuid) do
    socket = root_socket()
    run!(socket, "INSERT INTO #{qualified()} (id, seq) VALUES (#{j}, 0)")

    ledger =
      Enum.reduce(1..iterations, [], fn i, acc ->
        n = :atomics.add_get(next_gno, 1, 1)
        run!(socket, "SET gtid_next = '#{fake_uuid}:#{n}'")
        run!(socket, "UPDATE #{qualified()} SET seq = seq + 1 WHERE id = #{j}")
        run!(socket, "SET gtid_next = 'AUTOMATIC'")
        [{i, n} | acc]
      end)

    close_socket(socket)
    Enum.reverse(ledger)
  end

  # For row j and captured G, i_lower = the largest local_seq whose OWN gtid (`fake_uuid:n_i`) is a
  # member of G — an EXACT lower bound: every such change is ≤ G, hence must be visible. If the
  # view's seq for row j is BELOW i_lower, a change ≤ G is missing from the chunk → a violation.
  defp row_violation(j, g, seq_view, ledgers, fake_uuid) do
    parsed_g = Gtid.parse(g)

    i_lower =
      ledgers
      |> Map.fetch!(j)
      |> Enum.filter(fn {_i, n} -> Gtid.member?(parsed_g, {fake_uuid, n}) end)
      |> Enum.map(fn {i, _} -> i end)
      |> Enum.max(fn -> 0 end)

    observed = Map.get(seq_view, j, 0)
    if observed < i_lower, do: {:row, j, observed: observed, i_lower: i_lower}, else: nil
  end

  defp random_uuid do
    <<a::binary-8, b::binary-4, c::binary-4, d::binary-4, e::binary-12>> =
      16 |> :crypto.strong_rand_bytes() |> Base.encode16(case: :lower)

    "#{a}-#{b}-#{c}-#{d}-#{e}"
  end

  defp seq_view(rows) do
    Map.new(rows, fn %{"id" => id, "seq" => seq} ->
      {String.to_integer(id), String.to_integer(seq)}
    end)
  end

  defp reset_hot_rows(_writers) do
    socket = root_socket()
    run!(socket, "DELETE FROM #{qualified()}")
    close_socket(socket)
  end

  ## ---------------------------------------------------------------------------
  ## Live — helpers: throwaway schema + connections
  ## ---------------------------------------------------------------------------

  # A schema unique to this candidate/run so parallel best-of-N candidates never collide.
  # `:persistent_term` (not the process dictionary) so it is visible from every test process.
  defp schema, do: :persistent_term.get({__MODULE__, :schema})

  defp qualified, do: "`#{schema()}`.`t`"

  defp quiesce_seed(query, n) do
    root = root_socket()
    run!(root, "DELETE FROM #{qualified()}")

    Enum.each(1..n, fn i ->
      run!(
        root,
        "INSERT INTO #{qualified()} (id, seq) VALUES (#{i}, #{i}) " <>
          "ON DUPLICATE KEY UPDATE seq = #{i}"
      )
    end)

    close_socket(root)
    query
  end

  defp drain_all(reader, cursor, acc) do
    case ChunkReader.read_chunk(reader, cursor) do
      {:ok, chunk, true, _r} -> acc ++ chunk_ids(chunk)
      {:ok, chunk, false, r} -> drain_all(r, chunk.max_pk, acc ++ chunk_ids(chunk))
      {:done, _r} -> acc
    end
  end

  # Like drain_all, but records each page as `{ids, final?}` so a test can assert the finality
  # signal per page (not just the tiled ids). Stops on the FIRST `final?: true` page — under the
  # look-ahead an exact-multiple table's last full chunk is already final, so no trailing `{:done}`.
  defp drain_pages(reader, cursor, acc) do
    case ChunkReader.read_chunk(reader, cursor) do
      {:ok, chunk, true, _r} -> Enum.reverse([{chunk_ids(chunk), true} | acc])
      {:ok, chunk, false, r} -> drain_pages(r, chunk.max_pk, [{chunk_ids(chunk), false} | acc])
      {:done, _r} -> Enum.reverse(acc)
    end
  end

  defp chunk_ids(chunk), do: Enum.map(chunk.rows, fn %{"id" => id} -> String.to_integer(id) end)

  defp live_conn(database) do
    [
      host: "127.0.0.1",
      port: Capstan.MysqlCase.shared_port(),
      username: "capstan_sha2",
      password: "capstan_sha2_pw",
      database: database,
      ssl: true,
      ssl_opts: [verify: :verify_none]
    ]
  end

  defp root_socket do
    {:ok, raw} =
      :gen_tcp.connect(
        ~c"127.0.0.1",
        Capstan.MysqlCase.shared_port(),
        [:binary, active: false],
        10_000
      )

    {:ok, result} =
      Handshake.connect({:gen_tcp, raw},
        host: ~c"127.0.0.1",
        username: "root",
        password: "probe",
        ssl: false,
        auth_plugins: [:mysql_native_password]
      )

    result.socket
  end

  defp run!(socket, sql) do
    case Command.query(socket, sql) do
      :ok -> :ok
      {:ok, _rows} -> :ok
      {:error, reason} -> raise "chunk_reader_test: #{sql} failed #{inspect(reason)}"
    end
  end

  defp close_socket({:gen_tcp, s}), do: :gen_tcp.close(s)
  defp close_socket({:ssl, s}), do: :ssl.close(s)

  setup_all do
    if :live in ExUnit.configuration()[:include] do
      name = "capstan_c2t5_spec_#{System.system_time(:millisecond)}_#{:rand.uniform(1_000_000)}"
      :persistent_term.put({__MODULE__, :schema}, name)

      socket = root_socket()

      try do
        run!(socket, "CREATE DATABASE IF NOT EXISTS `#{name}`")
        run!(socket, "CREATE TABLE `#{name}`.`t` (id INT PRIMARY KEY, seq BIGINT NOT NULL)")
      after
        close_socket(socket)
      end

      on_exit(fn ->
        drop = root_socket()
        _ = Command.query(drop, "DROP DATABASE IF EXISTS `#{name}`")
        close_socket(drop)
        :persistent_term.erase({__MODULE__, :schema})
      end)
    end

    :ok
  end

  ## ---------------------------------------------------------------------------
  ## Unit — the scripted mock (a smart MySQL server: responds by SQL, records order,
  ## injects positional faults). Bypasses the handshake by handing back a %Query{} whose
  ## socket points at the mock, so only COM_QUERY traffic flows.
  ## ---------------------------------------------------------------------------

  # `open_reader/1` drives the REAL `ChunkReader.open/3` (introspection + fingerprint) against the
  # mock, then returns the reader + the recorder agent.
  defp open_reader(cfg) do
    {query, agent} = start_mock(build_cfg(cfg))

    open_opts =
      [chunk_size: cfg.chunk_size, max_retries: cfg.max_retries]
      |> Enum.reject(fn {_k, v} -> is_nil(v) end)

    {:ok, reader} = ChunkReader.open(query, {"test", "t"}, open_opts)
    # Mark the end of `open`'s traffic so per-chunk assertions can isolate read_chunk's SQL.
    Agent.update(agent, fn s -> %{s | open_boundary: length(s.recorded)} end)
    {reader, agent}
  end

  defp default_cfg(overrides \\ []) do
    base = %{
      statistics: @statistics,
      pk_columns: @pk_columns,
      columns: @cr_columns,
      chunk_rows: [["1", "10"], ["2", "20"]],
      gtid: @gtid,
      projection_ncols: 2,
      chunk_size: nil,
      max_retries: nil,
      fault: nil
    }

    Enum.reduce(overrides, base, fn {k, v}, acc -> Map.put(acc, k, v) end)
  end

  defp composite_cfg do
    default_cfg(
      statistics: [["PRIMARY", "1", "a", "0"], ["PRIMARY", "2", "b", "0"]],
      pk_columns: [
        ["a", "int", "int", "NO", nil, nil],
        ["b", "varbinary", "varbinary(16)", "NO", nil, nil]
      ],
      columns: [["a", "int", "1"], ["b", "varbinary(16)", "2"]],
      projection_ncols: 2
    )
  end

  # A VARCHAR(64) utf8mb4_0900_ai_ci PK table `(code, v)` — chunk rows carry the projection
  # PLUS the appended WEIGHT_STRING column (3 wire columns).
  defp varchar_cfg(overrides \\ []) do
    base =
      default_cfg(
        statistics: [["PRIMARY", "1", "code", "0"]],
        pk_columns: [["code", "varchar", "varchar(64)", "NO", "utf8mb4", "utf8mb4_0900_ai_ci"]],
        columns: [["code", "varchar(64)", "1"], ["v", "int", "2"]],
        projection_ncols: 3
      )

    Enum.reduce(overrides, base, fn {k, v}, acc -> Map.put(acc, k, v) end)
  end

  # A composite (INT i, VARCHAR k ai_ci) PK table `(i, k, v)` — the weight column appends
  # only for the STRING pk column (4 wire columns).
  defp int_varchar_cfg do
    default_cfg(
      statistics: [["PRIMARY", "1", "i", "0"], ["PRIMARY", "2", "k", "0"]],
      pk_columns: [
        ["i", "int", "int", "NO", nil, nil],
        ["k", "varchar", "varchar(64)", "NO", "utf8mb4", "utf8mb4_0900_ai_ci"]
      ],
      columns: [["i", "int", "1"], ["k", "varchar(64)", "2"], ["v", "int", "3"]],
      projection_ncols: 4
    )
  end

  # The chunk SELECT of a string-PK table (starts with `SELECT CAST(` — not `SELECT \``).
  defp chunk_select_of(recorded),
    do:
      Enum.find(
        recorded,
        &(String.starts_with?(&1, "SELECT") and String.contains?(&1, "FROM `test`.`t`"))
      )

  # The mutable server config the mock consults per request.
  defp build_cfg(cfg),
    do:
      Map.take(cfg, [
        :statistics,
        :pk_columns,
        :columns,
        :chunk_rows,
        :gtid,
        :projection_ncols,
        :fault,
        :contract
      ])

  defp recorded(agent), do: agent |> Agent.get(& &1.recorded) |> Enum.reverse()

  defp recorded_after_open(agent) do
    %{recorded: recorded, open_boundary: boundary} = Agent.get(agent, & &1)
    recorded |> Enum.reverse() |> Enum.drop(boundary)
  end

  # The seven-statement capture bracket (excludes the fingerprint COLUMNS read + any cleanup).
  defp capture_bracket(sql) do
    Enum.filter(sql, fn s ->
      String.starts_with?(s, "SET SESSION lock_wait_timeout") or
        String.starts_with?(s, "LOCK TABLES") or
        s == "SELECT @@global.gtid_executed" or
        s == "START TRANSACTION WITH CONSISTENT SNAPSHOT" or
        s == "UNLOCK TABLES" or
        String.starts_with?(s, "SELECT `") or
        s == "COMMIT"
    end)
  end

  defp set_chunk_rows(agent, rows),
    do: Agent.update(agent, fn s -> put_in(s.cfg.chunk_rows, rows) end)

  defp set_columns(agent, columns),
    do: Agent.update(agent, fn s -> put_in(s.cfg.columns, columns) end)

  defp start_mock(cfg) do
    {:ok, agent} =
      Agent.start_link(fn -> %{cfg: cfg, recorded: [], faults: %{}, open_boundary: 0} end)

    on_exit(fn -> if Process.alive?(agent), do: Agent.stop(agent) end)

    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true, ip: @loopback])
    {:ok, port} = :inet.port(listen)

    spawn(fn ->
      {:ok, srv} = :gen_tcp.accept(listen, 5000)
      :gen_tcp.close(listen)
      serve_loop({:gen_tcp, srv}, agent)
      :gen_tcp.close(srv)
    end)

    {:ok, client} = :gen_tcp.connect(@loopback, port, [:binary, active: false], 5000)
    on_exit(fn -> :gen_tcp.close(client) end)

    {%Query{socket: {:gen_tcp, client}}, agent}
  end

  defp serve_loop(srv, agent) do
    case recv_request(srv) do
      :closed ->
        :ok

      {:ok, sql} ->
        Agent.update(agent, fn s -> %{s | recorded: [sql | s.recorded]} end)
        response = Agent.get_and_update(agent, fn s -> respond(sql, s) end)
        send_response(srv, response)
        serve_loop(srv, agent)
    end
  end

  defp recv_request({:gen_tcp, s}) do
    case :gen_tcp.recv(s, 4, 5000) do
      {:ok, <<len::24-little, _seq::8>>} ->
        {:ok, <<_cmd::8, sql::binary>>} = :gen_tcp.recv(s, len, 5000)
        {:ok, sql}

      {:error, _reason} ->
        :closed
    end
  end

  # Decide the response from the SQL and the (mutable) config; return {response, new_state}. The
  # introspection reads (fixed resultsets) are split out from the capture statements (faultable) to
  # keep each dispatch small.
  defp respond(sql, state) do
    case introspection_response(sql, state.cfg) do
      {:response, response} -> {response, state}
      :statement -> statement_response(sql, state)
    end
  end

  defp introspection_response(sql, cfg) do
    cond do
      # The order-contract canary's weight-ordered half (`... ORDER BY WEIGHT_STRING(v)`);
      # `:contract => :violated` scripts a disagreement for the tripwire's RED test.
      sql =~ "ORDER BY WEIGHT_STRING" ->
        rows =
          if cfg[:contract] == :violated,
            do: [["0266"], ["0141"], ["0259"]],
            else: [["0141"], ["0259"], ["0266"]]

        {:response, {:rows, 1, rows}}

      # The canary's collation-ordered half (`... ORDER BY v`).
      sql =~ "ORDER BY v" ->
        {:response, {:rows, 1, [["0141"], ["0259"], ["0266"]]}}

      sql =~ "information_schema.STATISTICS" ->
        {:response, {:rows, 4, cfg.statistics}}

      sql =~ "information_schema.COLUMNS" and sql =~ "IS_NULLABLE" ->
        # 6-column COLUMNS resultset incl. charset/collation (NULL for non-string columns).
        ncols = cfg.pk_columns |> List.first() |> length()
        {:response, {:rows, ncols, cfg.pk_columns}}

      sql =~ "information_schema.COLUMNS" ->
        {:response, {:rows, 3, cfg.columns}}

      true ->
        :statement
    end
  end

  defp statement_response(sql, %{cfg: cfg} = state) do
    cond do
      String.starts_with?(sql, "SET SESSION lock_wait_timeout") ->
        faultable(:set, :ok, state)

      String.starts_with?(sql, "LOCK TABLES") ->
        faultable(:lock, :ok, state)

      sql == "SELECT @@global.gtid_executed" ->
        faultable(:gtid, {:rows, 1, [[cfg.gtid]]}, state)

      sql in ["START TRANSACTION WITH CONSISTENT SNAPSHOT", "UNLOCK TABLES", "ROLLBACK", "COMMIT"] ->
        {:ok, state}

      String.starts_with?(sql, "SELECT `") ->
        faultable(:chunk, {:rows, cfg.projection_ncols, cfg.chunk_rows}, state)

      # A string-PK chunk SELECT leads with CAST(`pk` AS BINARY), not a bare backtick column.
      String.starts_with?(sql, "SELECT CAST(") ->
        faultable(:chunk, {:rows, cfg.projection_ncols, cfg.chunk_rows}, state)

      true ->
        {:ok, state}
    end
  end

  # Inject a positional fault: while the configured fault targets `kind` and the (per-kind) budget
  # is unspent, respond with a MySQL error packet; else the success response.
  defp faultable(kind, success, %{cfg: %{fault: {kind, spec}}} = state) do
    used = Map.get(state.faults, kind, 0)

    if spec == :always or used < spec do
      {{:error, 1205}, put_in(state.faults[kind], used + 1)}
    else
      {success, state}
    end
  end

  defp faultable(_kind, success, state), do: {success, state}

  ## ---------------------------------------------------------------------------
  ## Unit — MySQL wire framing (mirrors query_test.exs; adds an ERR packet)
  ## ---------------------------------------------------------------------------

  defp send_response(srv, :ok), do: t_send(srv, <<0x00, 0, 0, 2, 0, 0, 0>>, 1)

  defp send_response(srv, {:error, code}),
    do: t_send(srv, <<0xFF, code::16-little, "#HY000mock error">>, 1)

  defp send_response(srv, {:rows, ncols, rows}) do
    t_send(srv, <<ncols>>, 1)
    Enum.each(1..ncols, fn i -> t_send(srv, "coldef_#{i}", i + 1) end)

    rows
    |> Enum.with_index(ncols + 2)
    |> Enum.each(fn {row, seq} -> t_send(srv, encode_text_row(row), seq) end)

    t_send(srv, <<0xFE, 0, 0, 2, 0, 0, 0>>, ncols + 2 + length(rows))
  end

  # Each value < 251 bytes ⇒ a single-byte length prefix; a binary value passes its raw
  # bytes; NULL (the charset/collation cells of non-string columns) is the 0xFB marker.
  defp encode_text_row(values) do
    Enum.reduce(values, <<>>, fn
      nil, acc ->
        acc <> <<0xFB>>

      value, acc ->
        bin = to_wire(value)
        acc <> <<byte_size(bin)::8, bin::binary>>
    end)
  end

  defp to_wire(value) when is_binary(value), do: value

  defp t_send({:gen_tcp, s}, payload, seq), do: :ok = :gen_tcp.send(s, frame(payload, seq))
  defp frame(payload, seq), do: <<byte_size(payload)::24-little, seq::8, payload::binary>>
end

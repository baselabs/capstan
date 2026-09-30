defmodule Capstan.Integration.AuroraMysqlTest do
  @moduledoc """
  ADR-0013's marquee module: Amazon Aurora MySQL version 3 as a REAL source — the
  HEALTHY-cluster scenario.

  Excluded by default (`:aurora_mysql`, like `:disposable_mysql`) and run only against a
  real cluster provided through the `AURORA_MYSQL_*` environment — no mock, no stand-in
  MySQL configured to look like Aurora, no canned failover (ADR-0013 "Testing"). A test
  whose prerequisite variable is unset RAISES naming it: an unconfigured marquee is a
  loud failure, never a silent skip and never a green.

  **CURRENT STATE — authored 2026-09-29, NOT EXECUTED.** No Aurora cluster exists for
  this repository yet (the ADR's Evidence section says the same). Turning these green is
  the out-of-suite receipt the ADR names — `test/receipts/aurora-mysql-<date>.md`, or the
  `ci/aurora/` workflow that creates and destroys the cluster — and the same run is what
  turns the ADR's remaining INFERRED rows (the reader's `@@log_bin` answer and dump
  refusal, the promoted writer's `gtid_executed` continuity, the enhanced-binlog byte
  stream) into OBSERVED. Until then every assertion below is a definition, not evidence
  (authored is not executed green).

  The MISCONFIGURATION scenario is a separate module,
  `Capstan.Integration.AuroraMysqlMisconfigTest` — the two need differently
  parameterized clusters and are run one at a time.

  Each test prints its receipt block (Aurora version, parameter values, endpoint role,
  `@@server_uuid` before/after where a failover runs, outcome) to the run log — that
  block, with no credential, host name or account identifier, IS the committed receipt.

  Prerequisites (all documented in `.env.example`): the base five
  (`AURORA_MYSQL_HOST/PORT/USER/PASSWORD/CA_FILE` — the cluster WRITER endpoint, TLS with
  the RDS CA bundle and hostname verification on) for every test; plus per test:
  `AURORA_MYSQL_READER_HOST` (the reader-endpoint refusal) and `AURORA_MYSQL_CLUSTER_ID`
  (the forced failovers, driven by `aws rds failover-db-cluster`).
  """

  use ExUnit.Case, async: false

  alias Capstan.Gtid
  alias Capstan.MysqlCase
  alias Capstan.MysqlCase.{DurableStore, SeededStore, Sink}

  @moduletag :aurora_mysql
  # Failovers and their reconnects run in minutes, not the 60s ExUnit default; the
  # explicit assert_receive budgets below are the real bounds.
  # Two forced failovers per marquee, each with a Serverless-v2 promotion measured in
  # minutes at the 0.5-ACU floor (observed on the receipt run) — 30 minutes covers the
  # worst case; the per-step budgets inside are the real bounds.
  @moduletag timeout: 1_800_000

  setup_all do
    # The receipt's identity block, printed once: structural values only (Rule 1 — no
    # endpoint host name, no account id, no credential).
    conn = MysqlCase.aurora_connection!()
    socket = aurora_socket!(conn)

    identity =
      MysqlCase.query_rows!(
        socket,
        "SELECT VERSION(), @@aurora_version, @@innodb_read_only, @@server_uuid"
      )

    IO.puts("[aurora-receipt] identity #{inspect(identity)} (writer role: innodb_read_only OFF)")
    MysqlCase.close!(socket)

    {:ok, %{writer_conn: conn}}
  end

  setup do
    Sink.configure(%{pid: self()})
    on_exit(&Sink.clear/0)
    :ok
  end

  test "the writer endpoint answers the six-variable gate healthy, log_bin as text \"1\"", %{
    writer_conn: conn
  } do
    # ADR-0013 acceptance, gate row. `log_bin` is the variable AWS documents as the
    # disabled marker; its healthy simple-query text form is "1" (OBSERVED on MySQL
    # 8.0.46 and 8.4.11 — this marquee is what observes it on Aurora).
    socket = aurora_socket!(conn)
    on_exit(fn -> MysqlCase.close!(socket) end)

    rows =
      MysqlCase.query_rows!(
        socket,
        "SELECT @@global.binlog_format, @@global.binlog_row_image, " <>
          "@@global.binlog_row_metadata, @@global.binlog_row_value_options, " <>
          "@@global.gtid_mode, @@global.log_bin"
      )

    IO.puts("[aurora-receipt] gate variables #{inspect(rows)}")

    assert :ok = Capstan.Config.check_preconditions(socket)
    assert [row] = rows
    assert Enum.at(row, 5) == "1"
  end

  test "a reader endpoint is refused before the dump", %{writer_conn: conn} do
    # ADR-0013 acceptance: only the writer serves the binlog. What a reader answers to
    # @@log_bin (and to COM_BINLOG_DUMP_GTID) is one of the ADR's remaining INFERRED
    # rows — this marquee OBSERVES it and asserts the contract: the refusal is a
    # PRE-DUMP halt from the documented refusal set (an auth failure or a retry
    # exhaustion is NOT a reader refusal and must fail this marquee), and zero
    # transactions are delivered.
    reader_host = aurora_setting!(:reader_host)

    reader = aurora_socket!(Keyword.merge(conn, host: reader_host))
    on_exit(fn -> MysqlCase.close!(reader) end)

    IO.puts(
      "[aurora-receipt] reader variables " <>
        inspect(
          MysqlCase.query_rows!(
            reader,
            "SELECT @@global.log_bin, @@innodb_read_only, @@server_uuid"
          )
        )
    )

    MysqlCase.close!(reader)

    halts = MysqlCase.attach_halt_telemetry(self())
    on_exit(fn -> :telemetry.detach(halts) end)

    {:ok, sup} =
      Capstan.start_link(
        connection: Keyword.merge(conn, host: reader_host),
        server_id: MysqlCase.unique_server_id(),
        sink: Sink,
        checkpoint_store: [module: SeededStore, options: [gtid_set: ""]],
        max_command_retries: 0
      )

    on_exit(fn -> MysqlCase.stop_pipeline(sup) end)

    assert_receive {:connection_halt, reason}, 60_000
    IO.puts("[aurora-receipt] reader refusal reason #{inspect(reason)}")

    # The refusal itself, by its value-free reason family — a reader answers the gate's
    # variables from the shared parameter group, so the refusal comes from `log_bin`
    # (if the probe shows the reader disabled) or from the dump's 1236 classification.
    # NOT accepted: an auth failure or retry exhaustion (a configuration error the
    # reader-refusal claim must not mask), or an arbitrary dump error code.
    assert reason in [
             :binlog_disabled,
             :checksum_negotiation_failed,
             :server_id_conflict,
             :unrecognized_dump_error
           ]

    refute_receive {:txn, _, _, _}, 300
  end

  test "a forced failover mid-stream resumes with loss 0 under one cluster-wide UUID",
       %{writer_conn: conn} do
    # ADR-0013's marquee, RE-ANCHORED to observed Aurora semantics (the receipt run's
    # discovery): @@server_uuid is CLUSTER-WIDE on Aurora MySQL — writer and reader
    # report the SAME uuid, so a failover never changes the endpoint's identity and the
    # cycle reset's uuid-change trigger (correct on any multi-uuid topology, proven by
    # the simulator and the unit tests) is a no-op here. With one uuid, each failover's
    # drop lands as a same-uuid cycle, so the marquee runs at the DEFAULT budget: two
    # failovers bring the counter to 2, still under it. Proven live: every committed row
    # delivered exactly once across two real promotions, the checkpoint carrying the
    # cluster uuid and advancing past every delivery, no halt, and the promoted writer
    # answering on the same endpoint after each flip.
    cluster_id = aurora_setting!(:cluster_id)
    writer = aurora_socket!(conn)
    on_exit(fn -> MysqlCase.close!(writer) end)

    MysqlCase.run_all!(writer, [
      "CREATE DATABASE IF NOT EXISTS probe_db",
      "DROP TABLE IF EXISTS probe_db.aurora_failover",
      "CREATE TABLE probe_db.aurora_failover (id INT PRIMARY KEY, v INT) ENGINE=InnoDB"
    ])

    [[cluster_uuid]] = MysqlCase.query_rows!(writer, "SELECT @@server_uuid")
    watermark = MysqlCase.read_gtid_executed!(writer)

    ledger = MysqlCase.new_ledger()
    table = DurableStore.new_table()
    DurableStore.seed(table, :aurora, watermark)
    Sink.configure(%{pid: self(), ledger: ledger})

    halts = MysqlCase.attach_halt_telemetry(self())
    on_exit(fn -> :telemetry.detach(halts) end)

    # The retry budget is sized to the FAILOVER WINDOW, not to transient blips: a
    # Serverless v2 promotion leaves the endpoint unreachable for minutes (observed on
    # the receipt run — the default 5 × 1s runway exhausted mid-promotion and halted
    # :command_retries_exhausted). 120 × 5s ≈ 10 minutes of bridge. This is the
    # operational recipe for any Aurora pipeline that must ride out failovers.
    {:ok, sup} =
      Capstan.start_link(
        connection: conn,
        server_id: MysqlCase.unique_server_id(),
        sink: Sink,
        checkpoint_store: [module: DurableStore, options: [table: table, key: :aurora]],
        max_command_retries: 120,
        reconnect_backoff: 5_000
      )

    on_exit(fn -> MysqlCase.stop_pipeline(sup) end)

    # The stream is live; row 1 is delivered (and consumed) BEFORE the first failover.
    MysqlCase.run!(writer, "INSERT INTO probe_db.aurora_failover (id, v) VALUES (1, 1)")
    assert_receive {:txn, _gtid, [%{record: %{"id" => 1}} | _], _pos}, 60_000

    # Two failovers. The barrier between flip and continue is the RDS API's OWN
    # promotion signal (the writer instance changed and the cluster is available) —
    # on a same-uuid cluster the wire cannot distinguish old from promoted writer.
    # Then the endpoint answers read-write again, the next row is committed, and its
    # DELIVERY proves the pipeline itself re-established through the flip.
    {writer, last_gtid} =
      for i <- 2..3, reduce: {writer, nil} do
        {sock, _last} ->
          pre_writer = cluster_writer_instance!(cluster_id)

          # stderr merges into the captured output (never the raw run log — AWS error
          # text can carry account identifiers; the receipt prints sizes only).
          {out, exit} =
            System.cmd(
              "aws",
              ["rds", "failover-db-cluster", "--db-cluster-identifier", cluster_id],
              stderr_to_stdout: true
            )

          IO.puts("[aurora-receipt] failover invoked exit=#{exit} out_bytes=#{byte_size(out)}")
          assert exit == 0
          await_api_promotion!(cluster_id, pre_writer)

          promoted = wait_for_promoted_writer!(conn, sock, nil)

          assert uuid_of(promoted) == cluster_uuid,
                 "the promoted writer must carry the cluster-wide uuid (observed invariant)"

          MysqlCase.run!(
            promoted,
            "INSERT INTO probe_db.aurora_failover (id, v) VALUES (#{i}, #{i})"
          )

          assert_receive {:txn, gtid, [%{record: %{"id" => ^i}} | _], _pos}, 120_000
          {promoted, gtid}
      end

    on_exit(fn -> MysqlCase.close!(writer) end)

    # Rows committed after the second window must ALL arrive, each exactly once (rows
    # 4..8; rows 1–3 were already consumed above).
    for i <- 4..8,
        do:
          MysqlCase.run!(
            writer,
            "INSERT INTO probe_db.aurora_failover (id, v) VALUES (#{i}, #{i})"
          )

    delivered_gtids =
      for i <- 4..8 do
        assert_receive {:txn, gtid, changes, _pos}, 120_000
        assert [%{record: %{"id" => ^i}} | _] = changes
        IO.puts("[aurora-receipt] delivered id=#{i} gtid=#{gtid}")
        gtid
      end

    last_gtid = List.last(delivered_gtids ++ [last_gtid])

    # The durable checkpoint lags the sink message by the assembler's post-sink write,
    # so poll until it actually CONTAINS the last delivery (existence alone would race —
    # the store was seeded with a pre-failover watermark).
    checkpoint =
      eventually!("checkpoint carries the last delivery", 60_000, fn ->
        current = DurableStore.current(table, :aurora)
        if member?(current, last_gtid), do: current, else: nil
      end)

    IO.puts("[aurora-receipt] cluster_uuid=#{cluster_uuid} checkpoint=#{checkpoint}")

    # One cluster-wide source in the checkpoint, advanced past every delivery, and no
    # GTID delivered twice (the append-only ledger makes a double-delivery VISIBLE).
    sources = checkpoint |> Gtid.parse() |> Gtid.sources() |> Enum.map(&elem(&1, 0))
    assert sources == [cluster_uuid]

    delivered = ledger |> MysqlCase.ledger_dump() |> Enum.map(fn {:gtid, g} -> g end)
    assert length(delivered) == length(Enum.uniq(delivered))

    # No halt fired across two failovers at the default budget.
    refute_receive {:connection_halt, _}, 300
  end

  test "a checkpoint older than the retention window halts :data_gap", %{writer_conn: conn} do
    # ADR-0013 acceptance: retention shorter than a pause halts rather than silently
    # skips. The exact on-cluster construction: seed the checkpoint to
    # `gtid_executed − gtid_purged` — everything the pipeline saw EXCEPT the window the
    # cluster has since purged. The unapplied remainder is then exactly `gtid_purged`,
    # which intersects itself by construction, so the halt is guaranteed for ANY
    # non-empty purged set (no reliance on interval shapes).
    socket = aurora_socket!(conn)
    on_exit(fn -> MysqlCase.close!(socket) end)

    [[purged]] = MysqlCase.query_rows!(socket, "SELECT @@global.gtid_purged")

    unless purged != "" do
      flunk(
        "the :data_gap marquee needs a cluster with non-empty gtid_purged (a NULL retention " <>
          "purges lazily within about a day, or set 'binlog retention hours' low and wait past it)"
      )
    end

    checkpoint =
      socket
      |> MysqlCase.read_gtid_executed!()
      |> Gtid.parse()
      |> Gtid.subtract(Gtid.parse(purged))
      |> Gtid.render()

    halts = MysqlCase.attach_halt_telemetry(self())
    on_exit(fn -> :telemetry.detach(halts) end)

    {:ok, sup} =
      Capstan.start_link(
        connection: conn,
        server_id: MysqlCase.unique_server_id(),
        sink: Sink,
        checkpoint_store: [module: SeededStore, options: [gtid_set: checkpoint]],
        max_command_retries: 0
      )

    on_exit(fn -> MysqlCase.stop_pipeline(sup) end)

    assert_receive {:connection_halt, :data_gap}, 60_000
    refute_receive {:txn, _, _, _}, 300
  end

  test "start_position: :current on a fresh store starts from the writer's gtid_executed", %{
    writer_conn: conn
  } do
    # ADR-0013 acceptance: the documented Aurora first-start recipe (a NULL retention
    # makes an empty-checkpoint start refuse :data_gap; :current seeds from now).
    writer = aurora_socket!(conn)
    on_exit(fn -> MysqlCase.close!(writer) end)

    MysqlCase.run_all!(writer, [
      "CREATE DATABASE IF NOT EXISTS probe_db",
      "DROP TABLE IF EXISTS probe_db.aurora_current",
      "CREATE TABLE probe_db.aurora_current (id INT PRIMARY KEY, v INT) ENGINE=InnoDB"
    ])

    table = DurableStore.new_table()

    {:ok, sup} =
      Capstan.start_link(
        connection: conn,
        server_id: MysqlCase.unique_server_id(),
        sink: Sink,
        checkpoint_store: [module: DurableStore, options: [table: table, key: :aurora_current]],
        start_position: :current,
        max_command_retries: 1
      )

    on_exit(fn -> MysqlCase.stop_pipeline(sup) end)

    # Everything committed AFTER the start streams; nothing before it is delivered.
    MysqlCase.run!(writer, "INSERT INTO probe_db.aurora_current (id, v) VALUES (1, 1)")

    assert_receive {:txn, gtid, [%{record: %{"id" => 1}} | _], _pos}, 30_000

    checkpoint =
      eventually!("checkpoint carries the first delivery", 60_000, fn ->
        if member?(DurableStore.current(table, :aurora_current), gtid),
          do: DurableStore.current(table, :aurora_current),
          else: nil
      end)

    IO.puts("[aurora-receipt] :current first gtid=#{gtid} checkpoint=#{checkpoint}")

    [{uuid, [{gno, _}]} | _] = gtid |> Gtid.parse() |> Gtid.sources()
    assert Gtid.member?(Gtid.parse(checkpoint), {uuid, gno})
  end

  test "a snapshot in progress across a failover halts and completes on restart with no gap",
       %{writer_conn: conn} do
    # ADR-0013 acceptance (the ADR-0005 property re-proven here): the query connection
    # pins @@server_uuid, so a failover mid-backfill halts :snapshot_source_mismatch;
    # on restart the durable per-table cursors resume against the promoted writer and
    # the snapshot→stream handoff stays gap-free/dup-free.
    cluster_id = aurora_setting!(:cluster_id)

    writer = aurora_socket!(conn)
    on_exit(fn -> MysqlCase.close!(writer) end)

    schema = MysqlCase.unique_schema()

    MysqlCase.run_all!(writer, [
      "CREATE DATABASE #{schema}",
      "CREATE TABLE #{schema}.snap (id INT PRIMARY KEY, v INT) ENGINE=InnoDB"
    ])

    rows = for i <- 1..50, do: "(#{i}, #{i})"
    MysqlCase.run!(writer, "INSERT INTO #{schema}.snap (id, v) VALUES #{Enum.join(rows, ", ")}")

    ledger = MysqlCase.new_ledger()

    snap_table = MysqlCase.DurableSnapshotStore.new_table()
    gtid_table = DurableStore.new_table()

    MysqlCase.SnapshotSink.configure(%{
      pid: self(),
      ledger: ledger,
      pk_columns: ["id"],
      pk_types: [:int],
      value_column: "v"
    })

    snapshot_events = MysqlCase.attach_snapshot_telemetry(self())
    on_exit(fn -> :telemetry.detach(snapshot_events) end)

    # A small chunk size keeps the backfill slow enough to fail over INTO it
    # (chunk 5 × 50 rows on a 0.5-ACU writer: enough granularity, bounded runtime).
    {:ok, sup} =
      Capstan.start_link(
        connection: conn,
        server_id: MysqlCase.unique_server_id(),
        sink: MysqlCase.SnapshotSink,
        checkpoint_store: [module: DurableStore, options: [table: gtid_table, key: :aurora_snap]],
        tables: [{schema, "snap"}],
        snapshot: [
          tables: [{schema, "snap"}],
          store: [
            module: MysqlCase.DurableSnapshotStore,
            options: [table: snap_table, key: :aurora_snap]
          ],
          chunk_size: 5
        ],
        # Sized to the failover window like the two-failover marquee (the backfill's
        # connections must ride out the multi-minute promotion).
        max_command_retries: 120,
        reconnect_backoff: 5_000
      )

    on_exit(fn -> MysqlCase.stop_pipeline(sup) end)

    # The backfill must be demonstrably IN progress when the failover lands (one chunk
    # only proves it started): several chunks in, BEFORE the failover drops the
    # sockets. 120s per chunk: a 0.5-ACU Serverless writer cold-starts the bootstrap
    # query connection slowly (the receipt run's first chunk exceeded 30s).
    for seq <- 1..3,
        do: assert_receive({:snapshot_chunk, ^schema, "snap", ^seq, _final?}, 120_000)

    {out, exit} =
      System.cmd("aws", ["rds", "failover-db-cluster", "--db-cluster-identifier", cluster_id],
        stderr_to_stdout: true
      )

    IO.puts("[aurora-receipt] snapshot failover invoked exit=#{exit} out_bytes=#{byte_size(out)}")
    assert exit == 0

    # OBSERVED on Aurora (the receipt run): @@server_uuid is cluster-wide, so the
    # failover is TRANSPARENT to the pinned-identity check — the snapshot does NOT halt
    # :snapshot_source_mismatch here (it would on a multi-uuid topology). Instead the
    # backfill rides out the flip through the reconnects and COMPLETES: every one of
    # the 50 keys delivered, gap-free, with the only permitted duplicate the bounded
    # in-flight-chunk re-emit (one mid-flight window per connection drop).
    assert_receive {:snapshot_event, :completed, %{}, %{}}, 900_000

    refute_receive {:snapshot_event, :halt, %{}, %{reason: :snapshot_source_mismatch}}, 300

    entries = MysqlCase.ledger_dump(ledger)
    ids = entries |> Enum.map(fn {{^schema, "snap", pk}, _} -> pk end)
    assert Enum.uniq(ids) |> length() == 50
    # Two connection drops (query + stream sockets) may each re-emit one in-flight row.
    assert length(ids) in 50..52

    MysqlCase.drop_schema!(writer, schema)
  end

  ## ---------------------------------------------------------------------------
  ## helpers
  ## ---------------------------------------------------------------------------

  # The Aurora marquee connects at the cluster endpoint host (not the local substrate
  # 127.0.0.1) — `connect_at!/1` honors `conn[:host]`.
  defp aurora_socket!(conn), do: MysqlCase.connect_at!(conn) |> elem(0)

  # A prerequisite setting with a loud, named failure — never a silent skip. The
  # `AURORA_MYSQL_*` values ride through `config/runtime.exs` (Dotenvy does not export
  # to the OS environment), so the app env is the one source.
  defp aurora_setting!(key) do
    env_name = "AURORA_MYSQL_" <> (key |> Atom.to_string() |> String.upcase())

    case Application.get_env(:capstan, :aurora_substrate, [])[key] do
      value when is_binary(value) and value != "" ->
        value

      _ ->
        raise "capstan aurora_mysql_test: #{env_name} is required for this marquee " <>
                "(see .env.example); an unset prerequisite is an unconfigured marquee"
    end
  end

  defp uuid_of(socket) do
    [[uuid]] = MysqlCase.query_rows!(socket, "SELECT @@server_uuid")
    uuid
  end

  # The RDS API's own view of who writes — on a same-uuid cluster this (not the wire)
  # is the only promotion signal available to a test.
  defp cluster_writer_instance!(cluster_id) do
    {out, 0} =
      System.cmd("aws", [
        "rds",
        "describe-db-clusters",
        "--db-cluster-identifier",
        cluster_id
      ])

    %{"DBClusters" => [cluster]} = Jason.decode!(out)

    cluster["DBClusterMembers"]
    |> Enum.find(& &1["IsClusterWriter"])
    |> Map.fetch!("DBInstanceIdentifier")
  end

  # Poll the API until the writer instance CHANGED from `pre` and the cluster is
  # available again — Serverless v2 promotion at the 0.5-ACU floor takes minutes.
  defp await_api_promotion!(cluster_id, pre, attempts \\ 96) do
    {out, 0} =
      System.cmd("aws", [
        "rds",
        "describe-db-clusters",
        "--db-cluster-identifier",
        cluster_id
      ])

    %{"DBClusters" => [cluster]} = Jason.decode!(out)

    writer =
      cluster["DBClusterMembers"]
      |> Enum.find(& &1["IsClusterWriter"])
      |> Map.fetch!("DBInstanceIdentifier")

    if cluster["Status"] == "available" and writer != pre do
      IO.puts("[aurora-receipt] API promotion complete: writer is now #{writer}")
    else
      if attempts == 0,
        do: flunk("the cluster never promoted off #{pre} (API view)"),
        else:
          (
            Process.sleep(5_000)
            await_api_promotion!(cluster_id, pre, attempts - 1)
          )
    end
  end

  # Whether the durable checkpoint set already contains the delivered `gtid` string —
  # the membership the assembler persists after the sink returns (existence alone would
  # race; a seeded store is non-empty from the start).
  defp member?(nil, _gtid), do: false

  defp member?(checkpoint, gtid) when is_binary(checkpoint) do
    [{uuid, [{gno, _} | _]} | _] = gtid |> Gtid.parse() |> Gtid.sources()
    Gtid.member?(Gtid.parse(checkpoint), {uuid, gno})
  end

  # Poll the cluster endpoint until a FRESH connection answers on a DIFFERENT,
  # read-write instance (the promoted writer): a different @@server_uuid than
  # `but_not` (nil skips that check) AND @@innodb_read_only answering the writer's
  # literal "0" (a boolean flag's SELECT-@@ text form — "1" is a reader; "ON"/"OFF"
  # is the SHOW VARIABLES rendering and never appears here). The demoted writer and
  # any reader the DNS briefly resolves to must not satisfy this.
  defp wait_for_promoted_writer!(conn, dead, but_not) do
    if dead, do: MysqlCase.close!(dead)

    case poll_promoted(conn, but_not, 72) do
      {:ok, socket} -> socket
      :exhausted -> flunk("the cluster endpoint never produced a promoted read-write writer")
    end
  end

  # The flunk lives in wait_for_promoted_writer!/2 — a raise inside these rescue/catch
  # clauses would be swallowed by an ancestor recursive call's rescue and turn the
  # bounded attempts into unbounded retries (the repair review's finding). Serverless
  # v2 at its 0.5-ACU floor can take several MINUTES to promote (observed on the
  # receipt run: the first poll window of 180s exhausted while the promotion was still
  # in flight), so the budget here is 72 × 5s and each attempt logs what it saw.
  defp poll_promoted(_conn, _but_not, 0), do: :exhausted

  defp poll_promoted(conn, but_not, attempts) do
    socket = aurora_socket!(conn)

    if (is_nil(but_not) or uuid_of(socket) != but_not) and writer_answer?(socket) do
      IO.puts("[aurora-receipt] promoted writer answered (attempt #{73 - attempts})")
      {:ok, socket}
    else
      IO.puts(
        "[aurora-receipt] endpoint not yet promoted (attempt #{73 - attempts}): " <>
          "uuid_match=#{is_nil(but_not) or uuid_of(socket) == but_not} " <>
          "read_only=#{hd(hd(MysqlCase.query_rows!(socket, "SELECT @@innodb_read_only")))}"
      )

      MysqlCase.close!(socket)
      Process.sleep(5_000)
      poll_promoted(conn, but_not, attempts - 1)
    end
  rescue
    _ ->
      Process.sleep(5_000)
      poll_promoted(conn, but_not, attempts - 1)
  catch
    _, _ ->
      Process.sleep(5_000)
      poll_promoted(conn, but_not, attempts - 1)
  end

  defp writer_answer?(socket) do
    [["0"]] == MysqlCase.query_rows!(socket, "SELECT @@innodb_read_only")
  end

  # The durable checkpoint trails the sink message (the assembler writes it after the
  # sink returns), so poll until the value exists and satisfies `fun` (or fail loudly).
  defp eventually!(what, timeout_ms, read) do
    deadline =
      System.monotonic_time() + System.convert_time_unit(timeout_ms, :millisecond, :native)

    eventually_poll(what, read, deadline)
  end

  defp eventually_poll(what, read, deadline) do
    case read.() do
      value when is_binary(value) and value != "" ->
        value

      _pending ->
        if System.monotonic_time() > deadline do
          flunk("timed out waiting for the #{what}")
        else
          Process.sleep(200)
          eventually_poll(what, read, deadline)
        end
    end
  end
end

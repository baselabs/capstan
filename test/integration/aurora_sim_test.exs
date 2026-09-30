defmodule Capstan.Integration.AuroraSimTest do
  @moduledoc """
  ADR-0013's simulator arm: the Aurora failover contract verified on REAL MySQL, with
  no AWS account — the `scripts/aurora-sim/` stack (three MySQL 8.0 nodes behind an
  HAProxy endpoint the tests flip through its admin socket).

  What this tier proves on real servers and real binlogs/GTIDs (the wire-level
  contract capstan depends on):

    * the endpoint answers the six-variable gate healthy (`log_bin` as text `"1"`);
    * the reader-endpoint node (read-only, binlog OFF) passes the five value variables
      and is refused `:binlog_disabled` BEFORE the dump — ADR-0013 decision 1's
      "reader reports `log_bin = OFF`" arm, observed;
    * a failover (promotion behind the endpoint flip, with the promoted node's
      `gtid_executed` carrying BOTH writers' UUIDs) resumes with loss 0, the
      checkpoint set gaining both UUIDs, and the cycle budget reset live-proven under
      `max_command_retries: 1` — two flips back-to-back would exhaust an unreset
      budget;
    * a snapshot in progress across a failover halts `:snapshot_source_mismatch` and
      completes on restart (the ADR-0005 property).

  What it deliberately does NOT claim: Aurora engine behavior (the cluster parameter
  group's OFF semantics, `@@aurora_version`, enhanced binlog, volume-level binlog
  continuity) — those stay DOCUMENTED in ADR-0013 until a real cluster run. This tier
  is excluded by default (`:aurora_sim`); bring the stack up and run it with:

      docker compose -f scripts/aurora-sim/docker-compose.yml up -d
      mix test --only aurora_sim

  The ports it reads default to the compose stack's (`AURORA_SIM_*` in `.env.example`
  overrides). Throwaway local credentials only.
  """

  use ExUnit.Case, async: false

  alias Capstan.Gtid
  alias Capstan.MysqlCase
  alias Capstan.MysqlCase.{DurableStore, Sink}

  @moduletag :aurora_sim
  @moduletag timeout: 600_000

  # The in-stack service names the replica channels point at (identical in compose and
  # in a cluster manifest); the throwaway harness accounts from scripts/aurora-sim/init.
  @source_writer "writer"
  @source_promotable "promotable"
  @admin_user "admin_sim"
  @admin_password "admin_sim_pw"
  @repl_user "repl"
  @repl_password "repl_pw"

  setup do
    Sink.configure(%{pid: self()})
    on_exit(&Sink.clear/0)
    :ok
  end

  setup_all do
    sim = sim_env()

    writer = connect_node!(sim, :writer_port)
    promotable = connect_node!(sim, :promotable_port)
    reader = connect_node!(sim, :reader_port)

    # The read-only posture is applied HERE (dynamic), not at container init —
    # read-only during INITIALIZE makes first boot exit 1 (found in CI).
    run!(promotable, "SET GLOBAL super_read_only = OFF")
    run!(promotable, "SET GLOBAL read_only = ON")
    run!(promotable, "SET GLOBAL super_read_only = ON")
    run!(reader, "SET GLOBAL super_read_only = OFF")
    run!(reader, "SET GLOBAL read_only = ON")
    run!(reader, "SET GLOBAL super_read_only = ON")

    # The role shapes ARE the simulator's contract — a node that comes up in the wrong
    # role is a broken harness, and every marquee below would lie. The read-only side of
    # the shape is @@global.super_read_only: it is the DYNAMIC variable the posture above
    # sets (read_only follows it; @@innodb_read_only is static, startup-only — Aurora sets
    # it on readers, stock MySQL does not).
    assert [["0", "1"]] = query!(writer, "SELECT @@global.super_read_only, @@global.log_bin")
    assert [["1", "1"]] = query!(promotable, "SELECT @@global.super_read_only, @@global.log_bin")
    assert [["1", "0"]] = query!(reader, "SELECT @@global.super_read_only, @@global.log_bin")
    assert :ok = Capstan.Config.check_preconditions(writer)

    # Wire the GTID replica chain (idempotent): promotable and reader follow the writer.
    for follower <- [promotable, reader] do
      run!(follower, "STOP REPLICA")
      run!(follower, "RESET REPLICA ALL")

      run_tolerant(
        follower,
        "CHANGE REPLICATION SOURCE TO SOURCE_HOST='#{@source_writer}', " <>
          "SOURCE_PORT=3306, SOURCE_USER='#{@repl_user}', SOURCE_PASSWORD='#{@repl_password}', SOURCE_AUTO_POSITION=1, GET_SOURCE_PUBLIC_KEY=1"
      )

      run!(follower, "START REPLICA")
    end

    {:ok,
     %{
       sim: sim,
       writer: writer,
       promotable: promotable,
       reader: reader,
       endpoint_conn: endpoint_conn(sim)
     }}
  end

  test "the endpoint answers the six-variable gate healthy, log_bin as text \"1\"", %{
    endpoint_conn: conn
  } do
    socket = MysqlCase.connect_at!(conn) |> elem(0)
    on_exit(fn -> MysqlCase.close!(socket) end)

    rows =
      MysqlCase.query_rows!(
        socket,
        "SELECT @@global.binlog_format, @@global.binlog_row_image, " <>
          "@@global.binlog_row_metadata, @@global.binlog_row_value_options, " <>
          "@@global.gtid_mode, @@global.log_bin"
      )

    IO.puts("[aurora-sim-receipt] endpoint gate variables #{inspect(rows)}")

    assert :ok = Capstan.Config.check_preconditions(socket)
    assert [["ROW", "FULL", "FULL", "", "ON", "1"]] = rows
  end

  test "the reader node passes the five value variables and is refused :binlog_disabled before the dump",
       %{reader: reader, sim: sim} do
    rows =
      MysqlCase.query_rows!(
        reader,
        "SELECT @@global.binlog_format, @@global.binlog_row_image, " <>
          "@@global.binlog_row_metadata, @@global.binlog_row_value_options, " <>
          "@@global.gtid_mode, @@global.log_bin"
      )

    IO.puts("[aurora-sim-receipt] reader gate variables #{inspect(rows)}")

    # The reader-endpoint contract: the five VALUE variables come from the shared
    # configuration and pass; log_bin is the one that names the condition.
    assert [["ROW", "FULL", "FULL", "", "ON", "0"]] = rows

    halts = MysqlCase.attach_halt_telemetry(self())
    on_exit(fn -> :telemetry.detach(halts) end)

    {:ok, sup} =
      Capstan.start_link(
        connection: node_conn(sim, :reader_port, "capstan_sha2", "capstan_sha2_pw"),
        server_id: MysqlCase.unique_server_id(),
        sink: Sink,
        checkpoint_store: [module: MysqlCase.SeededStore, options: [gtid_set: ""]],
        max_command_retries: 0
      )

    on_exit(fn -> MysqlCase.stop_pipeline(sup) end)

    assert_receive {:connection_halt, :binlog_disabled}, 30_000
    refute_receive {:txn, _, _, _}, 300
  end

  test "two failovers behind the endpoint resume with loss 0 and both writers' UUIDs checkpointed",
       %{sim: sim, writer: writer, promotable: promotable} do
    # `max_command_retries: 1` is the live red proof of the cycle reset (ADR-0013 §3):
    # two flips back-to-back drop the connection twice; without the uuid-aware reset
    # the second drop lands on an unreset counter and halts `:server_id_conflict`.
    plant = MysqlCase.connect_at!(node_conn(sim, :writer_port)) |> elem(0)
    on_exit(fn -> MysqlCase.close!(plant) end)

    MysqlCase.run_all!(plant, [
      "CREATE DATABASE IF NOT EXISTS probe_db",
      "DROP TABLE IF EXISTS probe_db.sim_failover",
      "CREATE TABLE probe_db.sim_failover (id INT PRIMARY KEY, v INT) ENGINE=InnoDB"
    ])

    [[uuid_writer]] = MysqlCase.query_rows!(plant, "SELECT @@server_uuid")
    [[uuid_promotable]] = MysqlCase.query_rows!(promotable, "SELECT @@server_uuid")

    ledger = MysqlCase.new_ledger()
    table = DurableStore.new_table()
    DurableStore.seed(table, :sim, MysqlCase.read_gtid_executed!(plant))
    Sink.configure(%{pid: self(), ledger: ledger})

    halts = MysqlCase.attach_halt_telemetry(self())
    on_exit(fn -> :telemetry.detach(halts) end)

    {:ok, sup} =
      Capstan.start_link(
        connection: endpoint_conn(sim, "capstan_sha2", "capstan_sha2_pw"),
        server_id: MysqlCase.unique_server_id(),
        sink: Sink,
        checkpoint_store: [module: DurableStore, options: [table: table, key: :sim]],
        max_command_retries: 1
      )

    on_exit(fn -> MysqlCase.stop_pipeline(sup) end)

    # Row 1 on the writer, delivered — establish #1 proven before the first flip.
    MysqlCase.run!(plant, "INSERT INTO probe_db.sim_failover (id, v) VALUES (1, 1)")
    assert_receive {:txn, _gtid, [%{record: %{"id" => 1}} | _], _pos}, 30_000

    # Flip 1: writer → promotable. The promotion waits for GTID catch-up, so the
    # pipeline's reconnect passes gap_check against the promoted executed set.
    promote!(sim, writer, promotable, :promotable)
    MysqlCase.run!(promotable, "INSERT INTO probe_db.sim_failover (id, v) VALUES (2, 2)")
    assert_receive {:txn, _gtid, [%{record: %{"id" => 2}} | _], _pos}, 60_000

    # Flip 2: promotable → writer (fail back). Two drops on budget 1 — the reset's
    # live red proof.
    promote!(sim, promotable, writer, :writer)
    MysqlCase.run!(plant, "INSERT INTO probe_db.sim_failover (id, v) VALUES (3, 3)")
    assert_receive {:txn, _gtid, [%{record: %{"id" => 3}} | _], _pos}, 60_000

    # More rows through the failed-back writer, then the loss-0 audit: every id once.
    for i <- 4..6,
        do: MysqlCase.run!(plant, "INSERT INTO probe_db.sim_failover (id, v) VALUES (#{i}, #{i})")

    last_delivered =
      for i <- 4..6 do
        assert_receive {:txn, gtid, [%{record: %{"id" => ^i}} | _], _pos}, 60_000
        IO.puts("[aurora-sim-receipt] delivered id=#{i} gtid=#{gtid}")
        gtid
      end
      |> List.last()

    checkpoint =
      eventually!("checkpoint carries the last delivery", 60_000, fn ->
        current = DurableStore.current(table, :sim)
        if member?(current, last_delivered), do: current, else: nil
      end)

    IO.puts(
      "[aurora-sim-receipt] uuid_writer=#{uuid_writer} uuid_promotable=#{uuid_promotable} checkpoint=#{checkpoint}"
    )

    sources = checkpoint |> Gtid.parse() |> Gtid.sources() |> Enum.map(&elem(&1, 0))
    assert uuid_writer in sources and uuid_promotable in sources

    ledger_gtids = ledger |> MysqlCase.ledger_dump() |> Enum.map(fn {:gtid, g} -> g end)
    assert length(ledger_gtids) == length(Enum.uniq(ledger_gtids))

    # No halt fired across the two flips (the reset did its job under budget 1).
    refute_receive {:connection_halt, _}, 300
  end

  test "a snapshot in progress across a failover halts and completes on restart with no gap",
       %{sim: sim, writer: writer, promotable: promotable} do
    plant = MysqlCase.connect_at!(node_conn(sim, :writer_port)) |> elem(0)
    on_exit(fn -> MysqlCase.close!(plant) end)

    schema = MysqlCase.unique_schema()

    MysqlCase.run_all!(plant, [
      "CREATE DATABASE #{schema}",
      "CREATE TABLE #{schema}.snap (id INT PRIMARY KEY, v INT) ENGINE=InnoDB"
    ])

    rows = for i <- 1..200, do: "(#{i}, #{i})"
    MysqlCase.run!(plant, "INSERT INTO #{schema}.snap (id, v) VALUES #{Enum.join(rows, ", ")}")

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

    start_pipeline = fn ->
      {:ok, sup} =
        Capstan.start_link(
          connection: endpoint_conn(sim, "capstan_sha2", "capstan_sha2_pw"),
          server_id: MysqlCase.unique_server_id(),
          sink: MysqlCase.SnapshotSink,
          checkpoint_store: [module: DurableStore, options: [table: gtid_table, key: :sim_snap]],
          tables: [{schema, "snap"}],
          snapshot: [
            tables: [{schema, "snap"}],
            store: [
              module: MysqlCase.DurableSnapshotStore,
              options: [table: snap_table, key: :sim_snap]
            ],
            chunk_size: 1
          ],
          max_command_retries: 1
        )

      on_exit(fn -> MysqlCase.stop_pipeline(sup) end)
      sup
    end

    sup = start_pipeline.()

    # Several chunks in, the backfill is demonstrably live; flip INTO it.
    for seq <- 1..5 do
      assert_receive {:snapshot_chunk, ^schema, "snap", ^seq, _final?}, 30_000
    end

    promote!(sim, writer, promotable, :promotable)

    halt =
      await_snapshot_halt!(
        System.monotonic_time() + System.convert_time_unit(180_000, :millisecond, :native)
      )

    IO.puts("[aurora-sim-receipt] snapshot halted across failover #{inspect(halt)}")
    MysqlCase.stop_pipeline(sup)

    # Restart through the endpoint (now the promoted writer): the durable cursors
    # resume and the backfill completes; 200 keys, the only permitted duplicate the
    # bounded in-flight-chunk re-emit (one row — chunk_size 1, one restart).
    _sup2 = start_pipeline.()
    assert_receive {:snapshot_event, :completed, %{}, %{}}, 300_000

    ids = ledger |> MysqlCase.ledger_dump() |> Enum.map(fn {{^schema, "snap", pk}, _} -> pk end)
    assert Enum.uniq(ids) |> length() == 200
    assert length(ids) in 200..201

    MysqlCase.drop_schema!(promotable, schema)
  end

  ## ---------------------------------------------------------------------------
  ## the failover primitive
  ## ---------------------------------------------------------------------------

  # Promote `target` (read-only follower → read-write writer behind the endpoint),
  # demote the old writer to a follower of it, and flip the HAProxy endpoint. Waits
  # for GTID catch-up FIRST — the pipeline's reconnect runs gap_check against the
  # promoted executed set, which must already contain the checkpoint's GTIDs.
  defp promote!(sim, old_active, target, target_role) do
    catch_up!(old_active, target)

    run!(target, "STOP REPLICA")
    run_tolerant(target, "RESET REPLICA ALL")
    run!(target, "SET GLOBAL super_read_only = OFF")
    run!(target, "SET GLOBAL read_only = OFF")

    run!(old_active, "SET GLOBAL read_only = ON")
    run!(old_active, "SET GLOBAL super_read_only = ON")
    run_tolerant(old_active, "RESET REPLICA ALL")

    source_name = if target_role == :promotable, do: @source_promotable, else: @source_writer

    run_tolerant(
      old_active,
      "CHANGE REPLICATION SOURCE TO SOURCE_HOST='#{source_name}', SOURCE_PORT=3306, " <>
        "SOURCE_USER='#{@repl_user}', SOURCE_PASSWORD='#{@repl_password}', SOURCE_AUTO_POSITION=1, GET_SOURCE_PUBLIC_KEY=1"
    )

    run!(old_active, "START REPLICA")

    flip_endpoint!(sim, target_role)
    IO.puts("[aurora-sim-receipt] endpoint flipped to #{source_name}")
  end

  # Poll until `target`'s gtid_executed contains `source`'s (replication converged),
  # on a deadline — a broken channel fails the marquee instead of hanging it.
  defp catch_up!(source, target) do
    deadline = System.monotonic_time() + System.convert_time_unit(120_000, :millisecond, :native)
    catch_up_poll(source, target, deadline)
  end

  defp catch_up_poll(source, target, deadline) do
    source_executed = MysqlCase.read_gtid_executed!(source)

    unless Gtid.subset?(
             Gtid.parse(source_executed),
             Gtid.parse(MysqlCase.read_gtid_executed!(target))
           ) do
      if System.monotonic_time() > deadline,
        do: flunk("replication did not converge before the flip deadline"),
        else:
          (
            Process.sleep(200)
            catch_up_poll(source, target, deadline)
          )
    end
  end

  # HAProxy runtime API over TCP: enable the target backend server, park the other, and
  # SEVER the parked server's established sessions — `state maint` alone only refuses NEW
  # connections (existing ones drain on the demoted node), while a real cluster failover
  # drops every open connection through the endpoint. Without the shutdown the pipeline
  # keeps streaming from the demoted writer (its reverse replication makes that path
  # deliver!) and no reconnect — no cycle reset, no identity check — is ever exercised.
  defp flip_endpoint!(sim, target) do
    other = if target == :promotable, do: :writer, else: :promotable
    admin_cmd!(sim, "set server writer_nodes/#{target} state ready")
    admin_cmd!(sim, "set server writer_nodes/#{other} state maint")
    admin_cmd!(sim, "shutdown sessions server writer_nodes/#{other}")
  end

  defp admin_cmd!(sim, command) do
    host = sim[:host] |> String.to_charlist()

    {:ok, sock} = :gen_tcp.connect(host, sim[:admin_port], [:binary, active: false], 5_000)

    :ok = :gen_tcp.send(sock, command <> "\n")
    {:ok, _reply} = :gen_tcp.recv(sock, 0, 5_000)
    :gen_tcp.close(sock)
  end

  ## ---------------------------------------------------------------------------
  ## connections + small query helpers over a held socket
  ## ---------------------------------------------------------------------------

  defp sim_env, do: Application.fetch_env!(:capstan, :aurora_sim)

  defp node_conn(sim, port_key, username \\ nil, password \\ nil) do
    conn = [
      host: sim[:host],
      port: sim[port_key],
      username: username || sim[:user],
      password: password || sim[:password],
      ssl: false
    ]

    # The stack's containers start with mysql_native_password ROOT (the compose flags), so
    # the harness's own connections allow it. The PIPELINE connections pass the
    # capstan_sha2 account explicitly and keep the default caching_sha2 posture, as
    # against any real source.
    if is_nil(username),
      do: Keyword.put(conn, :auth_plugins, [:mysql_native_password]),
      else: conn
  end

  defp endpoint_conn(sim, username \\ nil, password \\ nil),
    do: node_conn(sim, :endpoint_port, username, password)

  # Harness control runs as root (throwaway local stack); pipelines authenticate as
  # capstan_sha2 (the default caching_sha2 posture, as against any real source).
  defp connect_node!(sim, port_key),
    do: MysqlCase.connect_at!(node_conn(sim, port_key)) |> elem(0)

  defp query!(socket, sql), do: MysqlCase.query_rows!(socket, sql)
  defp run!(socket, sql), do: MysqlCase.run!(socket, sql)

  defp run_tolerant(socket, sql) do
    _ = MysqlCase.run_tolerant(socket, sql)
    :ok
  end

  ## ---------------------------------------------------------------------------
  ## polling helpers (the durable checkpoint trails the sink message)
  ## ---------------------------------------------------------------------------

  defp member?(nil, _gtid), do: false

  defp member?(checkpoint, gtid) when is_binary(checkpoint) do
    [{uuid, [{gno, _} | _]} | _] = gtid |> Gtid.parse() |> Gtid.sources()
    Gtid.member?(Gtid.parse(checkpoint), {uuid, gno})
  end

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

  # Loop over in-flight chunk events until the failover's identity halt arrives. Every
  # OTHER event is logged and accumulated, and the timeout flunk names what actually
  # fired — a snapshot run's halt reason is a finding, never noise.
  defp await_snapshot_halt!(deadline, seen \\ []) do
    receive do
      {:snapshot_event, :halt, _measurements, %{reason: :snapshot_source_mismatch}} = halt ->
        halt

      {:connection_halt, :snapshot_source_mismatch} = halt ->
        halt

      other ->
        IO.puts("[aurora-sim-receipt] awaiting halt, saw: #{inspect(other)}")
        await_snapshot_halt!(deadline, [other | seen])
    after
      1_000 ->
        if System.monotonic_time() > deadline,
          do:
            flunk(
              "timed out waiting for :snapshot_source_mismatch across the failover; events seen: " <>
                inspect(Enum.reverse(seen))
            ),
          else: await_snapshot_halt!(deadline, seen)
    end
  end
end

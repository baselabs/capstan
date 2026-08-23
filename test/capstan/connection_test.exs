defmodule Capstan.ConnectionTest do
  use ExUnit.Case, async: false

  alias Capstan.CheckpointStore
  alias Capstan.Connection
  alias Capstan.Position
  alias Capstan.Protocol.Command
  alias Capstan.Protocol.Handshake

  # A canonical source UUID for the GTID-set fixtures below.
  @uuid "3e11fa47-71ca-11e1-9e33-c80aa9429562"

  # The healthy resume scenario (design Q4 / C3): the server has executed 1..14,
  # has purged 1..9, and the checkpoint sits at 1..12. The unapplied remainder is
  # 13..14 which does NOT intersect the purged 1..9 — so the pipeline is healthy and
  # must NOT halt, EVEN THOUGH the checkpoint (1..12) itself intersects purged (1..9).
  # This is the exact case the naive v1 predicate (`checkpoint ∩ purged`) over-rejects.
  @checkpoint "#{@uuid}:1-12"
  @executed "#{@uuid}:1-14"
  @purged "#{@uuid}:1-9"

  # A genuinely missing range: checkpoint at 1..5, remainder 6..14 intersects the
  # purged 1..9 at 6..9 — the log fell off the back of the retention window.
  @gap_checkpoint "#{@uuid}:1-5"

  # A checkpoint carrying GTIDs (15..20) the server never executed (executed only 1..14).
  @foreign_checkpoint "#{@uuid}:1-20"

  @loopback {127, 0, 0, 1}

  ## ---------------------------------------------------------------------------
  ## gap_check/3 — the proactive predicate, BOTH directions (F1 / Q4)
  ## ---------------------------------------------------------------------------

  describe "gap_check/3 — proactive retention-gap predicate (F1)" do
    test "a healthy pipeline whose unapplied remainder is intact does NOT halt" do
      # RED against the v1 predicate: checkpoint(1-12) ∩ purged(1-9) = 1-9 ≠ ∅, so a
      # `checkpoint ∩ purged` implementation would (wrongly) halt this healthy pipeline.
      assert :ok = Connection.gap_check(@executed, @purged, @checkpoint)
    end

    test "a genuinely missing range halts :data_gap" do
      assert {:halt, :data_gap} = Connection.gap_check(@executed, @purged, @gap_checkpoint)
    end

    test "a checkpoint carrying never-executed GTIDs halts :source_identity_mismatch" do
      assert {:halt, :source_identity_mismatch} =
               Connection.gap_check(@executed, @purged, @foreign_checkpoint)
    end

    test "an empty checkpoint (fresh start) never halts, even against purged logs" do
      # A fresh start has no durable position to lose, so requesting from the retained
      # start is not a gap. Without this, every fresh start against any server that has
      # ever purged would falsely halt :data_gap — the over-rejection Q4 forbids.
      assert :ok = Connection.gap_check(@executed, @purged, "")
    end

    test "a multi-source checkpoint with an intact remainder does NOT halt" do
      other = "9f8b1e2c-0000-11e1-1111-c80aa9429562"
      executed = "#{@uuid}:1-14,#{other}:1-8"
      purged = "#{@uuid}:1-9,#{other}:1-3"
      checkpoint = "#{@uuid}:1-12,#{other}:1-6"
      assert :ok = Connection.gap_check(executed, purged, checkpoint)
    end
  end

  ## ---------------------------------------------------------------------------
  ## classify_dump_error/2 — error 1236 is OVERLOADED (F5 / A2)
  ## ---------------------------------------------------------------------------

  describe "classify_dump_error/2 — 1236 discriminated on message (F5)" do
    test "checksum-negotiation text -> :checksum_negotiation_failed" do
      msg =
        "Slave can not handle replication events with the checksum that master is " <>
          "configured to log; the first event 'binlog.000001' at 4"

      assert :checksum_negotiation_failed = Connection.classify_dump_error(1236, msg)
    end

    test "purged text WITH a named missing range -> :data_gap" do
      msg =
        "Cannot replicate because the master purged required binary logs. The GTID set " <>
          "sent by the slave is '#{@uuid}:1-5', and the missing transactions are '#{@uuid}:6-9'."

      assert :data_gap = Connection.classify_dump_error(1236, msg)
    end

    test "purged text WITHOUT a named range -> :data_gap" do
      msg =
        "The slave is connecting using CHANGE MASTER TO MASTER_AUTO_POSITION = 1, but the " <>
          "master has purged binary logs containing GTIDs that the slave requires."

      assert :data_gap = Connection.classify_dump_error(1236, msg)
    end

    test "a duplicate-replica 1236 (server_uuid/server_id) -> :server_id_conflict, not :data_gap" do
      # MySQL 8.0.x evicts a same-server_id replica with this 1236 (Task 18 marquee found
      # the socket-drop cycle-counter path never fires on 8.0.x). It must NOT read as a gap.
      msg =
        "A replica with the same server_uuid/server_id as this replica has connected " <>
          "to the source; the first event ... could not be found"

      reason = Connection.classify_dump_error(1236, msg)
      assert reason == :server_id_conflict
      refute reason == :data_gap
    end

    test "an unrecognized 1236 gets its own reason and is NEVER :data_gap" do
      msg = "Client requested master to start replication from position > file size"
      reason = Connection.classify_dump_error(1236, msg)
      assert reason == :unrecognized_dump_error
      refute reason == :data_gap
    end

    test "a non-1236 dump error is surfaced with its code, never :data_gap" do
      reason = Connection.classify_dump_error(1159, "Got fatal error reading event")
      assert reason == {:dump_failed, 1159}
      refute reason == :data_gap
    end
  end

  ## ---------------------------------------------------------------------------
  ## Lifecycle — SET before dump, frame delivery (F4)
  ## ---------------------------------------------------------------------------

  describe "lifecycle — SET @master_binlog_checksum BEFORE the dump (F4)" do
    test "issues SET before COM_BINLOG_DUMP_GTID and forwards frames to the receiver" do
      {port, _srv} =
        start_mock_server(fn sock, test ->
          serve_full_establish(sock, @executed, @purged, test)
          stream_event(sock, "EVENT-ALPHA", 1)
          stream_event(sock, "EVENT-BETA", 2)
          block_until_closed(sock)
        end)

      start_conn(
        server_id: 42,
        connection: [],
        max_command_retries: 5,
        receiver: self(),
        start_position: %Position{gtid_set: @checkpoint},
        connect_fun: mock_connect_fun(port)
      )

      # The command order on the wire: preconditions -> gtid read -> SET checksum -> SET
      # heartbeat -> dump. Received in this exact order proves both SETs precede the dump (F4 +
      # the liveness heartbeat period).
      assert_receive {:server_recv, :preconditions}, 2000
      assert_receive {:server_recv, :gtid}, 2000
      assert_receive {:server_recv, :set}, 2000
      assert_receive {:server_recv, :heartbeat}, 2000
      assert_receive {:server_recv, :dump}, 2000

      assert_receive {:binlog_event, "EVENT-ALPHA"}, 2000
      assert_receive {:binlog_event, "EVENT-BETA"}, 2000
    end
  end

  ## ---------------------------------------------------------------------------
  ## Lifecycle — proactive gap halt is WIRED (F1)
  ## ---------------------------------------------------------------------------

  describe "lifecycle — a real retention gap halts before any frame is delivered" do
    test "halts :data_gap and delivers zero frames" do
      {port, _srv} =
        start_mock_server(fn sock, test ->
          serve_through_gtid(sock, @executed, @purged, test)
          block_until_closed(sock)
        end)

      start_conn(
        server_id: 43,
        connection: [],
        max_command_retries: 5,
        receiver: self(),
        start_position: %Position{gtid_set: @gap_checkpoint},
        connect_fun: mock_connect_fun(port)
      )

      assert_receive {:capstan_halt, :data_gap}, 2000
      refute_receive {:binlog_event, _}, 300
    end
  end

  ## ---------------------------------------------------------------------------
  ## Lifecycle — a mid-stream 1236 halt is discriminated (F5)
  ## ---------------------------------------------------------------------------

  describe "lifecycle — a 1236 dump refusal is discriminated on its message" do
    test "a checksum-negotiation 1236 halts :checksum_negotiation_failed, not :data_gap" do
      {port, _srv} =
        start_mock_server(fn sock, test ->
          serve_full_establish(sock, @executed, @purged, test)

          error =
            "#HY000Slave can not handle replication events with the checksum that master " <>
              "is configured to log"

          send_pkt(sock, <<0xFF, 1236::16-little, error::binary>>, 1)
          block_until_closed(sock)
        end)

      start_conn(
        server_id: 44,
        connection: [],
        max_command_retries: 5,
        receiver: self(),
        start_position: %Position{gtid_set: @checkpoint},
        connect_fun: mock_connect_fun(port)
      )

      assert_receive {:capstan_halt, :checksum_negotiation_failed}, 2000
      refute_receive {:binlog_event, _}, 300
    end
  end

  ## ---------------------------------------------------------------------------
  ## Lifecycle — command budget halts at max_command_retries + 1 (Q8 / A6)
  ## ---------------------------------------------------------------------------

  describe "lifecycle — the command budget halts at max_command_retries + 1" do
    test "a connect that always fails halts after exactly max+1 attempts" do
      test = self()

      connect_fun = fn _connection ->
        send(test, :connect_attempt)
        {:error, :refused}
      end

      start_conn(
        server_id: 45,
        connection: [],
        max_command_retries: 2,
        receiver: self(),
        start_position: nil,
        connect_fun: connect_fun,
        reconnect_backoff: 20
      )

      assert_receive :connect_attempt, 2000
      assert_receive :connect_attempt, 2000
      assert_receive :connect_attempt, 2000
      assert_receive {:capstan_halt, :command_retries_exhausted}, 2000
      # Exactly max+1 (=3) attempts — no fourth.
      refute_receive :connect_attempt, 300
    end
  end

  ## ---------------------------------------------------------------------------
  ## Lifecycle — server_id conflict livelock (Q8 / C8)
  ## ---------------------------------------------------------------------------

  describe "lifecycle — a duplicate server_id livelock surfaces :server_id_conflict" do
    test "an established-then-dropped cycle counter is not reset by frame arrival" do
      test = self()

      # Each connect establishes cleanly, streams ONE frame, then the server drops the
      # connection — the exact signature of a duplicate-server_id eviction. If frame
      # arrival reset the cycle counter (the A6 bug), this would livelock forever.
      connect_fun = fn _connection ->
        {port, _srv} =
          start_mock_server(fn sock, inner ->
            serve_full_establish(sock, @executed, @purged, inner)
            stream_event(sock, "CYCLE-EVENT", 1)
          end)

        {:ok, raw} = :gen_tcp.connect(@loopback, port, [:binary, active: false], 5000)
        send(test, :cycle_established)
        {:ok, {:gen_tcp, raw}, %{server_version: "mock", tls: false}}
      end

      start_conn(
        server_id: 46,
        connection: [],
        max_command_retries: 2,
        receiver: self(),
        start_position: %Position{gtid_set: @checkpoint},
        connect_fun: connect_fun,
        reconnect_backoff: 20
      )

      # A frame arrives every cycle (proving the counter is NOT frame-reset), and after
      # max+1 (=3) established-then-dropped cycles the livelock is broken.
      assert_receive {:binlog_event, "CYCLE-EVENT"}, 2000
      assert_receive {:binlog_event, "CYCLE-EVENT"}, 2000
      assert_receive {:binlog_event, "CYCLE-EVENT"}, 2000
      assert_receive {:capstan_halt, :server_id_conflict}, 2000
      refute_receive {:binlog_event, _}, 300
    end
  end

  ## ---------------------------------------------------------------------------
  ## Lifecycle — every timer is cancelled on teardown (Q7)
  ## ---------------------------------------------------------------------------

  describe "lifecycle — no async primitive outlives the state it serves" do
    # The terminate/halt-path cancels of the reconnect timer and the streaming reader are
    # NOT independently testable: a `Process.send_after` timer whose destination is the
    # GenServer, and a `spawn_link`'d reader, are BOTH auto-reaped by the runtime the
    # instant the GenServer dies — so `read_timer`/`alive?` read the same after teardown
    # whether or not `terminate/2` cancelled them (this is why the earlier `refute_receive`
    # was vacuous, and why a `read_timer` rewrite would be too). Those cancels stay in the
    # code as correct hygiene. The OBSERVABLE "no stale message survives" property — the
    # one that guards against a killed old reader delivering a phantom event after a
    # drop→reconnect — is the stale-pid frame guard, tested here non-vacuously.
    test "a frame from a NON-current reader is dropped, never forwarded (stale-message guard)" do
      pid =
        start_conn(
          server_id: 47,
          connection: [],
          max_command_retries: 10,
          receiver: self(),
          start_position: nil,
          connect_fun: fn _ -> {:error, :refused} end,
          reconnect_backoff: 60_000
        )

      # A well-formed event frame (0x00 OK marker + event bytes) carrying a pid that is
      # NOT the connection's current reader — a stand-in for a killed old reader after a
      # reconnect. The `%{reader: reader}` match must reject it; if that guard matched any
      # pid, `handle_frame` would forward the phantom event to the receiver.
      stale_reader = spawn(fn -> :ok end)
      send(pid, {:frame, stale_reader, <<0x00, "STALE-EVENT">>})

      refute_receive {:binlog_event, "STALE-EVENT"}, 300
    end
  end

  ## ---------------------------------------------------------------------------
  ## Lifecycle — reconnect re-reads the durable checkpoint, not the frozen start (B2 / F1)
  ## ---------------------------------------------------------------------------

  describe "reconnect resumes from the CURRENT durable checkpoint, not the frozen start (B2)" do
    test "a reconnect re-reads the store — an advance since start changes the gap-check outcome" do
      # The AssemblerServer advances the durable watermark while the Connection is up; on a
      # drop→reconnect the Connection must resume from the ADVANCED position, not the frozen
      # start-up one (else, once retention purged past the stale start, it would raise a FALSE
      # :data_gap on a healthy pipeline — the C3 vector re-opened). Proven observably: seed the
      # store to a HEALTHY position (connect 1 passes gap_check and reaches the dump), then
      # advance it to a source-identity mismatch. A reconnect that re-reads the store halts
      # :source_identity_mismatch; a frozen-start Connection would keep passing gap_check with
      # the healthy start position and never emit this halt (it would instead livelock the
      # established-then-dropped cycle to :server_id_conflict). This distinguishes the fix.
      {:ok, store} = CheckpointStore.InMemory.start_link()

      :ok =
        CheckpointStore.write_position(
          CheckpointStore.InMemory,
          store,
          %Position{gtid_set: @checkpoint}
        )

      # The connect_fun runs INSIDE the Connection process, so `self()` there is not the
      # test — capture the real test pid so the mock server's `{:server_recv, _}` reports
      # reach the assertions below.
      test_pid = self()

      connect_fun = fn _connection ->
        {port, _srv} =
          start_mock_server(fn sock, _inner ->
            # Serve a full establish (precond → gtid → SET → dump), then return — closing the
            # socket, so the reader sees EOF and the Connection schedules a reconnect. On the
            # reconnect the client halts at gap_check (before SET), so the next recv fails and
            # the mock-server rescue closes.
            serve_full_establish(sock, @executed, @purged, test_pid)
          end)

        {:ok, raw} = :gen_tcp.connect(@loopback, port, [:binary, active: false], 5000)
        {:ok, {:gen_tcp, raw}, %{server_version: "mock", tls: false}}
      end

      start_conn(
        server_id: 48,
        connection: [],
        max_command_retries: 5,
        receiver: self(),
        start_position: %Position{gtid_set: @checkpoint},
        checkpoint_store: {CheckpointStore.InMemory, store},
        connect_fun: connect_fun,
        reconnect_backoff: 150
      )

      # Connect 1 reached the dump — gap_check passed with the healthy start position.
      assert_receive {:server_recv, :dump}, 2000

      # The AssemblerServer advances the durable watermark to a source-identity mismatch,
      # BEFORE the reconnect's refresh (the 150ms backoff gives ample margin over this
      # synchronous write).
      :ok =
        CheckpointStore.write_position(
          CheckpointStore.InMemory,
          store,
          %Position{gtid_set: @foreign_checkpoint}
        )

      # Connect 2 (reconnect) re-read the store → the advanced position → halts on it.
      assert_receive {:capstan_halt, :source_identity_mismatch}, 3000
    end
  end

  ## ---------------------------------------------------------------------------
  ## Lifecycle — the receiver monitor stops the Connection when its receiver dies (B3)
  ## ---------------------------------------------------------------------------

  describe "receiver monitor — a dead receiver stops the Connection fail-closed (B3)" do
    test "a receiver :DOWN halts :receiver_down rather than streaming into a dead pid" do
      # The Connection monitors its receiver (the AssemblerServer) in init. When the receiver
      # dies — as it does on ANY fail-closed halt it detects — the Connection must stop
      # fail-closed, not keep streaming events into a dead pid (the silent stall). A failing
      # connect_fun keeps the Connection alive in a reconnect loop, so the :DOWN is what stops it.
      receiver = spawn(fn -> Process.sleep(:infinity) end)

      conn =
        start_conn(
          server_id: 60,
          connection: [],
          max_command_retries: 100,
          receiver: receiver,
          start_position: nil,
          connect_fun: fn _ -> {:error, :refused} end,
          reconnect_backoff: 60_000
        )

      conn_ref = Process.monitor(conn)

      # Barrier: a synchronous call proves the Connection is alive right AFTER the monitor, so
      # the monitor captured a live process — its `:DOWN` will carry conn's real exit reason,
      # not a `:noproc` from racing the monitor against conn's near-immediate death. (It also
      # ensures conn has finished `handle_continue(:connect)` before the receiver is killed.)
      _ = :sys.get_state(conn)
      Process.exit(receiver, :kill)

      assert_receive {:DOWN, ^conn_ref, :process, ^conn, {:shutdown, {:halt, :receiver_down}}},
                     2000
    end
  end

  ## ---------------------------------------------------------------------------
  ## Lifecycle — a raise during establish fails closed, never crashes (B3 hardening)
  ## ---------------------------------------------------------------------------

  describe "establish raise — a malformed server GTID fails closed, never crashes" do
    test "a raise in gap_check (malformed @@gtid_executed) becomes a command failure, then halts" do
      # A malformed @@gtid_executed makes gap_check/3's Gtid.parse RAISE ArgumentError. Without
      # establish_guarded that crashes the :temporary Connection (no restart, no halt, and the
      # server bytes could ride the OTP crash report). With it, the raise is a value-free command
      # failure: spend the budget, reconnect, then halt :command_retries_exhausted.
      test_pid = self()

      connect_fun = fn _ ->
        {port, _srv} =
          start_mock_server(fn sock, _inner ->
            # precondition resultset OK, then a MALFORMED gtid_executed the client will parse.
            {0, _precond} = recv_pkt(sock)
            serve_resultset(sock, ["ROW", "FULL", "FULL", "", "ON"])
            {0, _gtid} = recv_pkt(sock)
            serve_resultset(sock, ["!!not-a-valid-gtid!!", @purged])
            block_until_closed(sock)
          end)

        {:ok, raw} = :gen_tcp.connect(@loopback, port, [:binary, active: false], 5000)
        {:ok, {:gen_tcp, raw}, %{server_version: "mock", tls: false}}
      end

      conn =
        start_conn(
          server_id: 61,
          connection: [],
          max_command_retries: 2,
          receiver: test_pid,
          # A non-empty checkpoint makes gap_check actually parse the (malformed) executed set.
          start_position: %Position{gtid_set: @checkpoint},
          connect_fun: connect_fun,
          reconnect_backoff: 20
        )

      conn_ref = Process.monitor(conn)

      assert_receive {:capstan_halt, :command_retries_exhausted}, 4000

      assert_receive {:DOWN, ^conn_ref, :process, ^conn,
                      {:shutdown, {:halt, :command_retries_exhausted}}},
                     2000
    end
  end

  ## ---------------------------------------------------------------------------
  ## Streaming liveness — a silent half-open partition is detected + reconnected
  ## ---------------------------------------------------------------------------

  describe "streaming liveness — a silent stream (no heartbeat) is detected and reconnected" do
    test "the liveness timer fires, emits :stream_timeout, and reconnects" do
      # A half-open partition: the stream establishes and delivers a frame, then goes SILENT —
      # no more events AND no heartbeats. The parent's liveness timer must fire within
      # stream_timeout_ms, make the stall visible via telemetry, and reconnect. Without the
      # liveness timer the reader blocks on recv forever and none of this happens (the asserts
      # below time out → RED).
      test_pid = self()
      handler = {__MODULE__, make_ref()}

      :telemetry.attach(
        handler,
        [:capstan, :connection, :stream_timeout],
        &__MODULE__.__forward_stream_timeout__/4,
        test_pid
      )

      # The established event carries the connect-to-streaming duration (monotonic ms).
      est_handler = {__MODULE__, make_ref()}

      :telemetry.attach(
        est_handler,
        [:capstan, :connection, :established],
        fn _event, measurements, _meta, pid -> send(pid, {:established, measurements}) end,
        test_pid
      )

      on_exit(fn ->
        :telemetry.detach(handler)
        :telemetry.detach(est_handler)
      end)

      connect_fun = fn _ ->
        {port, _srv} =
          start_mock_server(fn sock, _inner ->
            serve_full_establish(sock, @executed, @purged, test_pid)

            # One frame proves the stream is alive, then SILENCE (no heartbeats) — the partition.
            stream_event(sock, "ALIVE-FRAME", 1)
            block_until_closed(sock)
          end)

        {:ok, raw} = :gen_tcp.connect(@loopback, port, [:binary, active: false], 5000)
        {:ok, {:gen_tcp, raw}, %{server_version: "mock", tls: false}}
      end

      start_conn(
        server_id: 62,
        connection: [],
        max_command_retries: 5,
        receiver: test_pid,
        start_position: %Position{gtid_set: @checkpoint},
        connect_fun: connect_fun,
        reconnect_backoff: 20,
        heartbeat_period_ms: 50,
        stream_timeout_ms: 300
      )

      # The stream is alive (a frame arrived)...
      assert_receive {:binlog_event, "ALIVE-FRAME"}, 2000

      assert_receive {:established, %{establish_ms: ms}}, 2000
      assert is_number(ms) and ms >= 0
      # ...then it goes silent and the liveness timer fires within ~300ms, making it visible...
      assert_receive {:stream_timeout, %{reason: :stream_stalled}}, 2000
      # ...and it reconnects (a fresh mock server delivers the frame again).
      assert_receive {:binlog_event, "ALIVE-FRAME"}, 2000
    end

    test "start-up fails closed :invalid_liveness_config when the window is not > the heartbeat period" do
      # A liveness window at or below the heartbeat period would false-drop a healthy idle
      # stream, so it is refused fail-closed rather than run a check that fires on a working
      # pipeline.
      assert {:error, {:shutdown, {:halt, :invalid_liveness_config}}} =
               GenServer.start(Connection,
                 server_id: 63,
                 connection: [],
                 receiver: self(),
                 start_position: nil,
                 connect_fun: fn _ -> {:error, :refused} end,
                 heartbeat_period_ms: 1000,
                 stream_timeout_ms: 1000
               )
    end

    test "start-up fails closed :invalid_liveness_config on a non-positive or over-ceiling liveness value" do
      # Config.validate/1 refuses these on the public path; the direct-wiring constructor
      # enforces the same set (constructor symmetry). A zero heartbeat silently disables
      # master heartbeats — the liveness timer then false-drops a healthy idle stream — and
      # a value above the Process.send_after ceiling crashes the timer call instead of
      # refusing. Each case below keeps the window comparison VALID so only the
      # value-shape guard can refuse it.
      over_ceiling = 4_294_967_296

      bad_configs = [
        [heartbeat_period_ms: 0, stream_timeout_ms: 60_000],
        [heartbeat_period_ms: -5, stream_timeout_ms: 60_000],
        [heartbeat_period_ms: over_ceiling, stream_timeout_ms: over_ceiling + 1],
        [stream_timeout_ms: over_ceiling],
        [reconnect_backoff: over_ceiling]
      ]

      for extra <- bad_configs do
        assert {:error, {:shutdown, {:halt, :invalid_liveness_config}}} =
                 GenServer.start(
                   Connection,
                   Keyword.merge(
                     [
                       server_id: 63,
                       connection: [],
                       receiver: self(),
                       start_position: nil,
                       connect_fun: fn _ -> {:error, :refused} end
                     ],
                     extra
                   )
                 )
      end
    end
  end

  ## ---------------------------------------------------------------------------
  ## Live probe (Step 3) — real connect + stream against mysql-cdc-probe
  ## ---------------------------------------------------------------------------

  describe "live — frames reach the configured receiver against mysql-cdc-probe" do
    @describetag :live

    test "connects, dumps, and forwards a real event frame" do
      # Resume from the server's CURRENT position: no gap (the checkpoint is exactly the
      # executed set) and the dump-thread preamble (ROTATE / FORMAT_DESCRIPTION /
      # PREVIOUS_GTIDS / HEARTBEAT) always streams, so a frame is guaranteed without
      # writing to the substrate. An empty start would request from GTID 1, which this
      # long-lived (purged) substrate correctly refuses with a 1236 retention gap.
      executed = live_gtid_executed()

      start_conn(
        server_id: 5140,
        connection: [
          host: "127.0.0.1",
          port: Capstan.MysqlCase.shared_port(),
          username: "root",
          password: "probe",
          ssl: false,
          auth_plugins: [:mysql_native_password],
          database: "probe_db"
        ],
        max_command_retries: 5,
        receiver: self(),
        start_position: %Position{gtid_set: executed}
      )

      assert_receive {:binlog_event, event}, 15_000
      assert is_binary(event) and byte_size(event) > 0
    end

    test "a healthy idle stream stays alive — heartbeats reset the liveness timer, no false drop" do
      # Against REAL MySQL: the `SET @master_heartbeat_period` must be accepted and the master
      # must emit heartbeats that reset the parent liveness timer, so a quiet-but-healthy stream
      # never false-drops. Short heartbeat (500ms) + window (2s): over a 4s observation (2× the
      # window) NO :stream_timeout may fire. A broken heartbeat SET / liveness reset would fire it.
      test_pid = self()
      handler = {__MODULE__, make_ref()}

      :telemetry.attach(
        handler,
        [:capstan, :connection, :stream_timeout],
        &__MODULE__.__forward_stream_timeout__/4,
        test_pid
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      executed = live_gtid_executed()

      start_conn(
        server_id: 5141,
        connection: [
          host: "127.0.0.1",
          port: Capstan.MysqlCase.shared_port(),
          username: "root",
          password: "probe",
          ssl: false,
          auth_plugins: [:mysql_native_password],
          database: "probe_db"
        ],
        max_command_retries: 5,
        receiver: test_pid,
        start_position: %Position{gtid_set: executed},
        heartbeat_period_ms: 500,
        stream_timeout_ms: 2000
      )

      # Heartbeat frames arrive (the stream is alive)...
      assert_receive {:binlog_event, _}, 15_000
      # ...and keep resetting the 2s liveness timer, so no stall is detected over 4s.
      refute_receive {:stream_timeout, _}, 4000
    end
  end

  # Reads @@global.gtid_executed off the live substrate so the connection under test can
  # resume from a non-purged position.
  defp live_gtid_executed do
    {:ok, raw} =
      :gen_tcp.connect(
        @loopback,
        Capstan.MysqlCase.shared_port(),
        [:binary, active: false],
        10_000
      )

    {:ok, %{socket: socket}} =
      Handshake.connect({:gen_tcp, raw},
        host: ~c"127.0.0.1",
        username: "root",
        password: "probe",
        ssl: false,
        auth_plugins: [:mysql_native_password]
      )

    {:ok, [[executed]]} = Command.query(socket, "SELECT @@global.gtid_executed")
    :gen_tcp.close(raw)
    executed
  end

  ## ---------------------------------------------------------------------------
  ## helpers — the connection under test
  ## ---------------------------------------------------------------------------

  defp start_conn(opts) do
    {:ok, pid} = GenServer.start(Connection, opts)
    on_exit(fn -> if Process.alive?(pid), do: safe_stop(pid) end)
    pid
  end

  defp safe_stop(pid) do
    GenServer.stop(pid, :normal, 1000)
  catch
    _, _ -> :ok
  end

  # A module-function telemetry handler (not a local capture) so :telemetry does not log a
  # per-attach performance warning into the test output. The test pid rides in `config`.
  def __forward_stream_timeout__(_event, _measurements, metadata, pid) do
    send(pid, {:stream_timeout, metadata})
  end

  # The connect_fun creates the client socket INSIDE the GenServer so the GenServer is
  # the socket's controlling process (passive recv, and the later ownership handoff to
  # the reader, both require this).
  defp mock_connect_fun(port) do
    fn _connection ->
      {:ok, raw} = :gen_tcp.connect(@loopback, port, [:binary, active: false], 5000)
      {:ok, {:gen_tcp, raw}, %{server_version: "mock", tls: false}}
    end
  end

  ## ---------------------------------------------------------------------------
  ## helpers — the mock MySQL server (post-auth wire exchange)
  ## ---------------------------------------------------------------------------

  # Serves the full establish handshake: preconditions resultset -> gtid resultset ->
  # SET OK -> reads the dump command. Reports each received command kind to `test` so
  # the caller can assert the SET-before-dump ordering (F4).
  defp serve_full_establish(sock, executed, purged, test) do
    serve_through_gtid(sock, executed, purged, test)
    # SET @master_binlog_checksum, then SET @master_heartbeat_period — BOTH precede the dump.
    {0, checksum_cmd} = recv_pkt(sock)
    send(test, {:server_recv, classify_cmd(checksum_cmd)})
    serve_ok(sock)
    {0, heartbeat_cmd} = recv_pkt(sock)
    send(test, {:server_recv, classify_cmd(heartbeat_cmd)})
    serve_ok(sock)
    {0, dump_cmd} = recv_pkt(sock)
    send(test, {:server_recv, classify_cmd(dump_cmd)})
    :ok
  end

  # Serves only up to (and including) the gtid_executed/gtid_purged read — used by the
  # gap-halt test, where the client halts before ever issuing the SET.
  defp serve_through_gtid(sock, executed, purged, test) do
    {0, precond_cmd} = recv_pkt(sock)
    send(test, {:server_recv, classify_cmd(precond_cmd)})
    serve_resultset(sock, ["ROW", "FULL", "FULL", "", "ON"])
    {0, gtid_cmd} = recv_pkt(sock)
    send(test, {:server_recv, classify_cmd(gtid_cmd)})
    serve_resultset(sock, [executed, purged])
    :ok
  end

  defp classify_cmd(<<0x1E, _rest::binary>>), do: :dump

  defp classify_cmd(<<0x03, sql::binary>>) do
    cond do
      bin_contains?(sql, "SET @master_binlog_checksum") -> :set
      bin_contains?(sql, "SET @master_heartbeat_period") -> :heartbeat
      bin_contains?(sql, "binlog_format") -> :preconditions
      bin_contains?(sql, "gtid_executed") -> :gtid
      true -> :other_query
    end
  end

  defp bin_contains?(haystack, needle), do: :binary.match(haystack, needle) != :nomatch

  # One-row text resultset with `length(values)` columns (config_test's shape).
  defp serve_resultset(sock, values) do
    ncols = length(values)
    send_pkt(sock, <<ncols>>, 1)
    Enum.each(1..ncols, fn i -> send_pkt(sock, "coldef_#{i}", i + 1) end)
    send_pkt(sock, encode_text_row(values), ncols + 2)
    send_pkt(sock, <<0xFE, 0, 0, 2, 0, 0, 0>>, ncols + 3)
  end

  defp serve_ok(sock), do: send_pkt(sock, <<0x00, 0, 0, 2, 0, 0, 0>>, 1)

  defp stream_event(sock, bytes, seq), do: send_pkt(sock, <<0x00>> <> bytes, seq)

  # Blocks until the client (reader) closes its end of the socket.
  defp block_until_closed(sock), do: :gen_tcp.recv(sock, 0, :infinity)

  defp encode_text_row(values) do
    Enum.reduce(values, <<>>, fn value, acc -> acc <> <<byte_size(value)::8, value::binary>> end)
  end

  defp start_mock_server(script) do
    test = self()
    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true, ip: @loopback])
    {:ok, port} = :inet.port(listen)

    server =
      spawn(fn ->
        {:ok, sock} = :gen_tcp.accept(listen, 5000)
        :gen_tcp.close(listen)

        try do
          script.(sock, test)
        rescue
          _ -> :ok
        catch
          _, _ -> :ok
        after
          :gen_tcp.close(sock)
        end
      end)

    {port, server}
  end

  defp send_pkt(sock, payload, seq) do
    :ok = :gen_tcp.send(sock, <<byte_size(payload)::24-little, seq::8, payload::binary>>)
  end

  defp recv_pkt(sock) do
    {:ok, <<len::24-little, seq::8>>} = :gen_tcp.recv(sock, 4, 5000)

    payload =
      case len do
        0 ->
          <<>>

        n ->
          {:ok, p} = :gen_tcp.recv(sock, n, 5000)
          p
      end

    {seq, payload}
  end
end

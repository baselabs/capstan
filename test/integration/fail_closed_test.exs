defmodule Capstan.Integration.FailClosedTest do
  @moduledoc """
  Live-substrate fail-closed marquees (plan Task 18, design F14) — the five transaction-shape /
  transport properties the design specifies as LIVE shapes, previously unit-only. Proving them on
  real substrate bytes is the whole point: a unit fixture can assert the intended halt while the
  REAL event sequence never reaches it.

    * **MyISAM write** → the non-transactional `QUERY("COMMIT")` terminator delivers the row.
    * **a second pipeline with the SAME `server_id`** → `:server_id_conflict` (a REAL pair — MySQL
      evicts the duplicate replica; the cycle counter must halt, never livelock).
    * **`binlog_transaction_compression=ON`** (throwaway container) → a LOUD
      `:compressed_payload_unsupported` halt, no delivery.
    * **no `ssl:` option** → the connection is ACTUALLY encrypted (asserted on the SOCKET —
      `:ssl.connection_information` — not on the config).
    * **XA START/END/PREPARE** → `:unsupported_transaction_shape`, zero rows. This marquee
      exposed a real assembler silent-loss defect (`XA START` misclassified as DDL,
      advancing the checkpoint past the XA GTID), now fixed in commit `3fe5e5a`.

  Plus the Q5 precondition shape (a throwaway substrate, not one of the F14 five):

    * **`binlog_row_value_options=PARTIAL_JSON`** → refused at the precondition gate
      (`:binlog_row_value_options_not_empty`) before any dump, so a partial-JSON row image is
      never decoded.

  `:integration`-tagged. Never restarts or reconfigures the shared `mysql-cdc-probe`.
  """
  use ExUnit.Case, async: false

  alias Capstan.MysqlCase
  alias Capstan.MysqlCase.{SeededStore, Sink}

  @moduletag :integration

  setup_all do
    MysqlCase.ensure_sha2_user!(MysqlCase.query_connection())
    :ok
  end

  setup do
    Sink.configure(%{pid: self()})
    on_exit(&Sink.clear/0)
    :ok
  end

  ## ---------------------------------------------------------------------------
  ## MyISAM → QUERY("COMMIT") terminator
  ## ---------------------------------------------------------------------------

  test "a MyISAM write is delivered via its QUERY(\"COMMIT\") terminator" do
    qconn = MysqlCase.socket!(MysqlCase.query_connection())
    on_exit(fn -> MysqlCase.close!(qconn) end)

    sup =
      start_shared_pipeline!(qconn, [
        "DROP TABLE IF EXISTS fc_myisam",
        "CREATE TABLE fc_myisam (id INT PRIMARY KEY, name VARCHAR(50)) ENGINE=MyISAM"
      ])

    on_exit(fn -> MysqlCase.stop_pipeline(sup) end)

    MysqlCase.run!(qconn, "INSERT INTO fc_myisam (id, name) VALUES (1, 'myisam-row')")

    # MyISAM is non-transactional: the commit terminator is QUERY("COMMIT"), not XID. The row must
    # still be delivered — a terminator misread as DDL would silently drop it.
    assert_receive {:txn, _gtid, [change], _pos}, 20_000
    assert %Capstan.Change{op: :insert, table: "fc_myisam", record: rec} = change
    assert rec["id"] == 1
  end

  ## ---------------------------------------------------------------------------
  ## duplicate server_id → :server_id_conflict (a REAL replica pair)
  ## ---------------------------------------------------------------------------

  # PROVES the safety-critical property: a duplicate server_id makes the evicted replica halt
  # FAIL-CLOSED (a `{:connection_halt, _}` arrives) — it does NOT livelock (design Q8/C8).
  #
  # This marquee found (and drove the fix for) a real reachability gap: the plan and
  # `Connection`'s cycle counter assumed MySQL DROPS the evicted replica (socket close →
  # `handle_drop` → `:server_id_conflict`), but MySQL 8.0.x evicts it via a 1236 error frame
  # ("A replica with the same server_uuid/server_id as this replica has connected to the
  # source …"). `classify_dump_error/2` now has a `server_uuid` branch mapping that 1236 to
  # `:server_id_conflict` (commit 3fe5e5a), so the halt is the design's actionable Q8 reason —
  # which this marquee asserts exactly.
  test "a second pipeline with the same server_id halts fail-closed, never a livelock" do
    qconn = MysqlCase.socket!(MysqlCase.query_connection())
    on_exit(fn -> MysqlCase.close!(qconn) end)
    watermark = MysqlCase.read_gtid_executed!(qconn)

    established = MysqlCase.attach_established_telemetry(self())
    halts = MysqlCase.attach_halt_telemetry(self())
    on_exit(fn -> :telemetry.detach(established) end)
    on_exit(fn -> :telemetry.detach(halts) end)

    # A single duplicated server_id shared by both replicas. `max_command_retries: 0` collapses any
    # pre-establish budget so the FIRST fail-closed condition halts immediately, not after a wait.
    shared_server_id = MysqlCase.unique_server_id()

    sup_a = start_conflicting_pipeline!(watermark, shared_server_id)
    on_exit(fn -> MysqlCase.stop_pipeline(sup_a) end)
    assert_receive :connection_established, 20_000

    sup_b = start_conflicting_pipeline!(watermark, shared_server_id)
    on_exit(fn -> MysqlCase.stop_pipeline(sup_b) end)

    # The evicted replica halts fail-closed (no livelock) with the design's Q8 reason. On
    # 8.0.x the eviction arrives as a 1236 naming server_uuid/server_id (not a socket drop),
    # which `classify_dump_error/2` now maps to `:server_id_conflict` (the Task 18 finding,
    # fixed) — so the halt is the actionable reason, not a generic one.
    assert_receive {:connection_halt, reason}, 20_000
    assert reason == :server_id_conflict
  end

  ## ---------------------------------------------------------------------------
  ## binlog_transaction_compression=ON → loud halt (throwaway container)
  ## ---------------------------------------------------------------------------

  test "a compressed transaction payload halts :compressed_payload_unsupported" do
    if MysqlCase.docker_available?() do
      MysqlCase.with_throwaway_mysql(["--binlog-transaction-compression=ON"], fn port ->
        qconn = MysqlCase.socket!(MysqlCase.query_connection(port))

        try do
          MysqlCase.run_all!(qconn, [
            "DROP TABLE IF EXISTS fc_compressed",
            "CREATE TABLE fc_compressed (id INT PRIMARY KEY, name VARCHAR(50)) ENGINE=InnoDB"
          ])

          watermark = MysqlCase.read_gtid_executed!(qconn)

          {:ok, sup} =
            Capstan.start_link(
              connection: MysqlCase.pipeline_connection(port),
              server_id: MysqlCase.unique_server_id(),
              sink: Sink,
              checkpoint_store: [module: SeededStore, options: [gtid_set: watermark]],
              max_command_retries: 5
            )

          on_exit(fn -> MysqlCase.stop_pipeline(sup) end)

          # The AssemblerServer-side halt emits no telemetry, so observe the process exit directly.
          ref = Process.monitor(MysqlCase.assembler_pid(sup))

          MysqlCase.run!(
            qconn,
            "INSERT INTO fc_compressed (id, name) VALUES (1, 'compressed-row')"
          )

          assert_receive {:DOWN, ^ref, :process, _pid,
                          {:shutdown,
                           {:halt, {:assembler_error, :compressed_payload_unsupported}}}},
                         20_000

          # No row is delivered — the compressed payload halts fail-closed, never guessed at.
          refute_receive {:txn, _g, _c, _p}, 200
        after
          MysqlCase.close!(qconn)
        end
      end)
    else
      IO.puts(
        "\n[SKIP] compression marquee: Docker unavailable — a throwaway container " <>
          "configured with binlog_transaction_compression=ON is required."
      )
    end
  end

  ## ---------------------------------------------------------------------------
  ## binlog_row_value_options=PARTIAL_JSON → precondition refusal (Q5, throwaway)
  ## ---------------------------------------------------------------------------

  test "a PARTIAL_JSON substrate is refused at the precondition gate" do
    if MysqlCase.docker_available?() do
      MysqlCase.with_throwaway_mysql(["--binlog-row-value-options=PARTIAL_JSON"], fn port ->
        halts = MysqlCase.attach_halt_telemetry(self())
        on_exit(fn -> :telemetry.detach(halts) end)

        {:ok, sup} =
          Capstan.start_link(
            connection: MysqlCase.pipeline_connection(port),
            server_id: MysqlCase.unique_server_id(),
            sink: Sink,
            checkpoint_store: [module: SeededStore, options: [gtid_set: ""]],
            max_command_retries: 5
          )

        on_exit(fn -> MysqlCase.stop_pipeline(sup) end)

        # Config.check_preconditions reads @@global.binlog_row_value_options; PARTIAL_JSON (not "")
        # refuses fail-closed at the FIRST establish step — before the dump — so no partial-JSON
        # row image is ever decoded (design Q5, Rule 2). A precondition violation cannot be cured by
        # reconnecting, so it halts rather than spending the command budget.
        assert_receive {:connection_halt, :binlog_row_value_options_not_empty}, 20_000
      end)
    else
      IO.puts(
        "\n[SKIP] PARTIAL_JSON marquee: Docker unavailable — a throwaway container configured " <>
          "with binlog_row_value_options=PARTIAL_JSON is required."
      )
    end
  end

  ## ---------------------------------------------------------------------------
  ## no ssl: option → the transport is actually encrypted (assert on the socket)
  ## ---------------------------------------------------------------------------

  test "with no ssl: option the connection is actually encrypted (Q6/Q17)" do
    # `pipeline_connection/2` with `ssl_opts:` sets NO `ssl:` key, so it defaults TRUE; F6 requires
    # a verify choice, so `verify: :verify_none` selects confidentiality-without-authentication.
    conn =
      MysqlCase.pipeline_connection(MysqlCase.shared_port(), ssl_opts: [verify: :verify_none])

    {socket, info} = MysqlCase.connect!(conn)
    on_exit(fn -> MysqlCase.close!(socket) end)

    # Assert on the SOCKET, not the config: the transport tag is :ssl and a real TLS session was
    # negotiated (a live protocol/cipher), so bytes actually cross an encrypted channel.
    assert {:ssl, ssl_socket} = socket
    assert info.tls == true
    assert {:ok, tls_info} = :ssl.connection_information(ssl_socket, [:protocol])
    assert Keyword.get(tls_info, :protocol) in [:"tlsv1.2", :"tlsv1.3"]
  end

  ## ---------------------------------------------------------------------------
  ## XA START/END/PREPARE → :unsupported_transaction_shape
  ## ---------------------------------------------------------------------------

  # This marquee EXPOSED a real silent-loss bug (Task 18 finding, now FIXED): against the
  # REAL substrate an XA transaction logs
  #   GTID → QUERY("XA START …") → TABLE_MAP → WRITE_ROWS → QUERY("XA END …") → XA_PREPARE(38)
  # and `Capstan.Assembler.on_query/4` had classified QUERY("XA START …") as a self-committing
  # DDL — delivering a spurious %SchemaChange{}, ADVANCING the checkpoint past the XA GTID, then
  # halting on the following rows (the type-38 handler unreachable, the checkpoint past an
  # undelivered transaction). The assembler now recognises XA verbs as non-DDL transaction
  # control, so the block stays open and XA_PREPARE halts :unsupported_transaction_shape with
  # the buffer discarded and the checkpoint NOT advanced.
  test "XA START/END/PREPARE halts :unsupported_transaction_shape with zero rows" do
    qconn = MysqlCase.socket!(MysqlCase.query_connection())
    on_exit(fn -> MysqlCase.close!(qconn) end)

    # A prepared XA holds a lock on `fc_xa`. If a prior run (or a crash) left one dangling,
    # it would block this run's DROP TABLE. Roll back best-effort BEFORE setup, and again on
    # exit, so the marquee is self-cleaning and never wedges the shared substrate. A fresh
    # cleanup connection is used because a prepared XA cannot be rolled back from a session
    # that is itself inside an XA transaction.
    cleanup_xa = fn ->
      cconn = MysqlCase.socket!(MysqlCase.query_connection())
      MysqlCase.run_tolerant(cconn, "XA ROLLBACK 'capstan_fc_xa'")
      MysqlCase.close!(cconn)
    end

    cleanup_xa.()
    on_exit(cleanup_xa)

    sup =
      start_shared_pipeline!(qconn, [
        "DROP TABLE IF EXISTS fc_xa",
        "CREATE TABLE fc_xa (id INT PRIMARY KEY, name VARCHAR(50)) ENGINE=InnoDB"
      ])

    on_exit(fn -> MysqlCase.stop_pipeline(sup) end)

    ref = Process.monitor(MysqlCase.assembler_pid(sup))

    MysqlCase.run_all!(qconn, [
      "XA START 'capstan_fc_xa'",
      "INSERT INTO fc_xa (id, name) VALUES (1, 'xa-row')",
      "XA END 'capstan_fc_xa'",
      "XA PREPARE 'capstan_fc_xa'"
    ])

    assert_receive {:DOWN, ^ref, :process, _pid,
                    {:shutdown, {:halt, :unsupported_transaction_shape}}},
                   20_000

    # The XA rows may later roll back, so NONE may be delivered.
    refute_receive {:txn, _g, _c, _p}, 200
    refute_receive {:schema_change, _sc, _p}, 200
  end

  ## ---------------------------------------------------------------------------
  ## helpers
  ## ---------------------------------------------------------------------------

  defp start_shared_pipeline!(qconn, setup_sql) do
    MysqlCase.run_all!(qconn, setup_sql)
    watermark = MysqlCase.read_gtid_executed!(qconn)

    {:ok, sup} =
      Capstan.start_link(
        connection: MysqlCase.pipeline_connection(),
        server_id: MysqlCase.unique_server_id(),
        sink: Sink,
        checkpoint_store: [module: SeededStore, options: [gtid_set: watermark]],
        max_command_retries: 5
      )

    sup
  end

  defp start_conflicting_pipeline!(watermark, server_id) do
    {:ok, sup} =
      Capstan.start_link(
        connection: MysqlCase.pipeline_connection(),
        server_id: server_id,
        sink: Sink,
        checkpoint_store: [module: SeededStore, options: [gtid_set: watermark]],
        max_command_retries: 0
      )

    sup
  end
end

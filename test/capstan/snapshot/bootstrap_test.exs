defmodule Capstan.Snapshot.BootstrapTest.CompleteStore do
  @moduledoc false
  # A SnapshotStore whose durable state is already `status: :complete` — proves the bootstrap
  # short-circuits to pure C1 WITHOUT opening any source connection. Its stored table set MATCHES
  # `snapshot_config/0`'s default `[{"probe_db", "capstan_bootstrap_marquee"}]` so `reconcile_tables/2`
  # passes (a mismatched set is the drift tripwire below, not this store's concern).
  @behaviour Capstan.SnapshotStore
  def start_link(_opts), do: Agent.start_link(fn -> :complete end)

  def read(_store) do
    {:ok,
     %Capstan.Snapshot.State{
       status: :complete,
       p0: "u:1-5",
       tables: %{
         {"probe_db", "capstan_bootstrap_marquee"} => %{
           fingerprint: "fp",
           pk_columns: ["id"],
           pk_types: [:integer],
           pk_cursor: 5,
           done?: true
         }
       }
     }}
  end

  def write(_store, _state), do: :ok
end

defmodule Capstan.Snapshot.BootstrapTest.MidSnapshotStore do
  @moduledoc false
  # A SnapshotStore mid-backfill (`status: :snapshotting`) whose stored table set is
  # {probe_db.capstan_bootstrap_marquee} — the config-drift tripwire feeds it a config that ADDS a
  # table, and the reconcile must halt BEFORE any source connection is opened.
  @behaviour Capstan.SnapshotStore
  def read(_store) do
    {:ok,
     %Capstan.Snapshot.State{
       status: :snapshotting,
       p0: "u:1-5",
       tables: %{
         {"probe_db", "capstan_bootstrap_marquee"} => %{
           fingerprint: "fp",
           pk_columns: ["id"],
           pk_types: [:integer],
           pk_cursor: :start,
           done?: false
         }
       }
     }}
  end

  def write(_store, _state), do: :ok
end

defmodule Capstan.Snapshot.BootstrapTest.FaultyStore do
  @moduledoc false
  # A SnapshotStore whose read faults — the bootstrap must fail closed :snapshot_state_read_failed.
  @behaviour Capstan.SnapshotStore
  def start_link(_opts), do: Agent.start_link(fn -> :faulty end)
  def read(_store), do: {:error, :store_unreachable}
  def write(_store, _state), do: :ok
end

defmodule Capstan.Snapshot.BootstrapTest.RaisingCheckpointStore do
  @moduledoc false
  # A checkpoint store that RAISES on write (a realistic durable store on a transient backing-DB
  # outage — the behaviour contract is {:error}, but Ecto/DBConnection RAISE). read/1 succeeds so
  # the bootstrap reaches seed_checkpoint's write, AFTER the query is open (S-1).
  @behaviour Capstan.CheckpointStore
  def read(_store), do: {:ok, nil}
  def write(_store, _gtid_set), do: raise("checkpoint store DB down")
end

defmodule Capstan.Snapshot.BootstrapTest.RaisingSnapshotStore do
  @moduledoc false
  # A snapshot store that RAISES on the initial read (before any source connection opens) — the
  # S-1 shape-gap: bootstrap must fail closed value-free, never an uncaught exception.
  @behaviour Capstan.SnapshotStore
  def read(_store), do: raise("snapshot store DB down")
  def write(_store, _state), do: :ok
end

defmodule Capstan.Snapshot.BootstrapTest.SnapshotSink do
  @moduledoc false
  # A valid snapshot-mode sink: handle_transaction/1 + handle_schema_change/2 + handle_snapshot/2.
  # Forwards each delivery to a configured test pid (persistent_term; the suite is async: false).
  @behaviour Capstan.Sink
  @key {__MODULE__, :pid}

  def configure(pid), do: :persistent_term.put(@key, pid)
  def clear, do: (:persistent_term.erase(@key) && :ok) || :ok
  defp pid, do: :persistent_term.get(@key, nil)

  @impl true
  def handle_transaction(txn) do
    if p = pid(), do: send(p, {:txn, txn.gtid})
    {:ok, txn.position}
  end

  @impl true
  def handle_schema_change(_sc, _position), do: :ok

  @impl true
  def handle_snapshot(changes, meta) do
    if p = pid(), do: send(p, {:snapshot, meta.table, Enum.map(changes, & &1.record)})
    :ok
  end
end

defmodule Capstan.Snapshot.BootstrapTest do
  @moduledoc """
  `Capstan.Snapshot.bootstrap/4` + `seed_checkpoint/2` (C2 Task 10 — the P0 pre-seed).

    * **Unit** (default suite) — the seed guard (never-regress), the `:complete` short-circuit,
      and the source-identity / P0-read faults, driven either purely or against a scripted mock
      MySQL server (the `Capstan.QueryTest` idiom) over a real loopback socket.
    * **Live** (`@tag :live`, `mix test --only live`) — the bootstrap marquee against
      `mysql-cdc-probe`: read `P0`, seed BOTH stores, return the readers + `processed_set = P0`.
    * **Docker** (`@tag :requires_docker`) — retention purge racing the bootstrap: the seed leaves
      a stale-but-gapped checkpoint, so the EXISTING C1 gap gate fires `:data_gap` (tripwire 11).
  """
  use ExUnit.Case, async: false

  alias Capstan.CheckpointStore
  alias Capstan.CheckpointStore.InMemory, as: CheckpointInMemory
  alias Capstan.MysqlCase
  alias Capstan.MysqlCase.DurableStore
  alias Capstan.Position
  alias Capstan.Protocol.Command
  alias Capstan.Protocol.Handshake
  alias Capstan.Query
  alias Capstan.Snapshot
  alias Capstan.Snapshot.State
  alias Capstan.SnapshotStore
  alias Capstan.SnapshotStore.InMemory, as: SnapshotInMemory

  alias Capstan.Snapshot.BootstrapTest.{
    CompleteStore,
    FaultyStore,
    MidSnapshotStore,
    RaisingCheckpointStore,
    RaisingSnapshotStore,
    SnapshotSink
  }

  @loopback {127, 0, 0, 1}

  @uuid_a "aaaaaaaa-1111-2222-3333-444444444444"
  @uuid_b "bbbbbbbb-5555-6666-7777-888888888888"

  @good_precond {5, [["ROW", "FULL", "FULL", "", "ON"]]}

  defp uuid_result(uuid), do: {1, [[uuid]]}

  # A canonical GTID-set string over a real source-UUID (the parser rejects a bare "u:…").
  defp gt(interval), do: @uuid_a <> ":" <> interval

  ## ===========================================================================
  ## seed_checkpoint/2 — seeds P0 ONLY when empty or equal (never regress)
  ## ===========================================================================

  describe "seed_checkpoint/2 — never regresses a stream-advanced watermark" do
    test "an empty (never-written) checkpoint is seeded with P0" do
      {:ok, cstore} = CheckpointInMemory.start_link([])
      p0 = gt("1-100")

      assert :ok = Snapshot.seed_checkpoint({CheckpointInMemory, cstore}, p0)

      assert {:ok, %Position{gtid_set: ^p0}} =
               CheckpointStore.read_position(CheckpointInMemory, cstore)
    end

    test "a checkpoint already equal to P0 stays P0 (idempotent re-seed)" do
      {:ok, cstore} = CheckpointInMemory.start_link([])
      p0 = gt("1-100")
      seed(cstore, p0)

      assert :ok = Snapshot.seed_checkpoint({CheckpointInMemory, cstore}, p0)

      assert {:ok, %Position{gtid_set: ^p0}} =
               CheckpointStore.read_position(CheckpointInMemory, cstore)
    end

    test "a checkpoint advanced PAST P0 is LEFT untouched (never regress)" do
      # The stream advanced the watermark to :1-200; a bootstrap re-seed of the older P0 (:1-100)
      # must NOT regress it. RED: an unconditional write would clobber :1-200 back to :1-100.
      {:ok, cstore} = CheckpointInMemory.start_link([])
      advanced = gt("1-200")
      seed(cstore, advanced)

      assert :ok = Snapshot.seed_checkpoint({CheckpointInMemory, cstore}, gt("1-100"))

      assert {:ok, %Position{gtid_set: ^advanced}} =
               CheckpointStore.read_position(CheckpointInMemory, cstore)
    end
  end

  describe "seed_checkpoint/2 — bootstrap crash-window (Ch5)" do
    test "a re-bootstrap reads a fresh p0' but leaves the earlier-seeded checkpoint untouched" do
      # Crash-window: the checkpoint was seeded (P0 = :1-100) but the %State{} was not yet written,
      # so on re-bootstrap the store reads nil (fresh) and a FRESH @@gtid_executed drift yields
      # p0' = :1-140. The seed must leave the earlier P0 — over-delivery of :1-101..140 is deduped
      # by member?/2; overwriting the checkpoint here would silently drift the resume floor.
      {:ok, cstore} = CheckpointInMemory.start_link([])
      original = gt("1-100")
      seed(cstore, original)

      assert :ok = Snapshot.seed_checkpoint({CheckpointInMemory, cstore}, gt("1-140"))

      assert {:ok, %Position{gtid_set: ^original}} =
               CheckpointStore.read_position(CheckpointInMemory, cstore)
    end
  end

  ## ===========================================================================
  ## bootstrap/4 — a :complete store short-circuits to pure C1 (no source connection)
  ## ===========================================================================

  describe "bootstrap/4 — status: :complete" do
    test "returns :complete WITHOUT opening any source connection" do
      {:ok, cstore} = CheckpointInMemory.start_link([])
      test_pid = self()

      # A connect_fun that would signal (and refuse) if the bootstrap ever tried to connect.
      connect_fun = fn _connection ->
        send(test_pid, :connect_attempted)
        {:error, :should_not_connect}
      end

      opts = [connection: fake_conn(), connect_fun: connect_fun]

      assert :complete =
               Snapshot.bootstrap(
                 opts,
                 snapshot_config(),
                 {CheckpointInMemory, cstore},
                 {CompleteStore, :ignored}
               )

      refute_received :connect_attempted
    end

    test "a snapshot-store READ fault fails closed :snapshot_state_read_failed" do
      {:ok, cstore} = CheckpointInMemory.start_link([])

      assert {:error, :snapshot_state_read_failed} =
               Snapshot.bootstrap(
                 [connection: fake_conn()],
                 snapshot_config(),
                 {CheckpointInMemory, cstore},
                 {FaultyStore, :ignored}
               )
    end

    test "a :complete state whose table set diverges from config halts :snapshot_config_drifted" do
      # F3 (cross-vendor): a table ADDED to `snapshot.tables` after a `:complete` state must NOT be
      # silently skipped. CompleteStore holds {marquee}; config adds `probe_db.added`. RED (before
      # the reconcile): the `:complete` short-circuit fires and `added` is never backfilled.
      {:ok, cstore} = CheckpointInMemory.start_link([])
      test_pid = self()

      connect_fun = fn _connection ->
        send(test_pid, :connect_attempted)
        {:error, :should_not_connect}
      end

      config =
        snapshot_config([{"probe_db", "capstan_bootstrap_marquee"}, {"probe_db", "added"}])

      opts = [connection: fake_conn(), connect_fun: connect_fun]

      assert {:error, :snapshot_config_drifted} =
               Snapshot.bootstrap(
                 opts,
                 config,
                 {CheckpointInMemory, cstore},
                 {CompleteStore, :ignored}
               )

      # The reconcile is a pure key comparison — it halts WITHOUT opening any source connection.
      refute_received :connect_attempted
    end

    test "a mid-snapshot state whose table set diverges from config halts BEFORE connecting" do
      # F3 on the RESUME path: `open_resume/3` reopens only the STORED tables, so a config that adds
      # a table would silently skip it. The reconcile runs before `start_backfill/5`, so no source
      # connection is opened. RED (before the reconcile): the bootstrap proceeds to connect + resume
      # only {marquee}, silently omitting `added`.
      {:ok, cstore} = CheckpointInMemory.start_link([])
      test_pid = self()

      connect_fun = fn _connection ->
        send(test_pid, :connect_attempted)
        {:error, :should_not_connect}
      end

      config =
        snapshot_config([{"probe_db", "capstan_bootstrap_marquee"}, {"probe_db", "added"}])

      opts = [connection: fake_conn(), connect_fun: connect_fun]

      assert {:error, :snapshot_config_drifted} =
               Snapshot.bootstrap(
                 opts,
                 config,
                 {CheckpointInMemory, cstore},
                 {MidSnapshotStore, :ignored}
               )

      refute_received :connect_attempted
    end
  end

  ## ===========================================================================
  ## bootstrap/4 — source-identity (Ch8) + P0-read faults (scripted mock server)
  ## ===========================================================================

  describe "bootstrap/4 — Ch8 cross-connection identity + P0 read" do
    test "a query conn landing on a DIFFERENT replica than the stream halts :snapshot_source_mismatch" do
      # Socket 1 (stream identity via Config.read_server_uuid): reports @uuid_a → pinned.
      # Socket 2 (Query.establish, expected_server_uuid: @uuid_a): reports @uuid_b → mismatch.
      # RED: drop the cross-connection pin → the query conn silently pairs G with the wrong server.
      connect_fun =
        scripted_connect_fun([
          [uuid_result(@uuid_a)],
          [@good_precond, uuid_result(@uuid_b)]
        ])

      {:ok, cstore} = CheckpointInMemory.start_link([])
      {:ok, sstore} = SnapshotInMemory.start_link([])

      assert {:error, :snapshot_source_mismatch} =
               Snapshot.bootstrap(
                 [connection: fake_conn(), connect_fun: connect_fun],
                 snapshot_config(),
                 {CheckpointInMemory, cstore},
                 {SnapshotInMemory, sstore}
               )
    end

    test "a P0 (@@gtid_executed) read fault halts :snapshot_bootstrap_gtid_read_failed" do
      # Identity + establish succeed (both @uuid_a); the P0 read returns an unusable shape.
      connect_fun =
        scripted_connect_fun([
          [uuid_result(@uuid_a)],
          [@good_precond, uuid_result(@uuid_a), {1, []}]
        ])

      {:ok, cstore} = CheckpointInMemory.start_link([])
      {:ok, sstore} = SnapshotInMemory.start_link([])

      assert {:error, :snapshot_bootstrap_gtid_read_failed} =
               Snapshot.bootstrap(
                 [connection: fake_conn(), connect_fun: connect_fun],
                 snapshot_config(),
                 {CheckpointInMemory, cstore},
                 {SnapshotInMemory, sstore}
               )
    end

    test "a fresh :all snapshot set with NO base tables halts :snapshot_no_base_tables" do
      # C2b: `:all` RESOLVES via information_schema enumeration instead of the old
      # :config_invalid refusal. A server exposing no scoped base tables is a
      # misconfiguration (wrong server / missing privileges) — a loud refusal,
      # never a silent empty backfill. RED (old contract): the refusal was
      # :config_invalid, before any enumeration query ran.
      connect_fun =
        scripted_connect_fun([
          [uuid_result(@uuid_a)],
          [@good_precond, uuid_result(@uuid_a), {1, [[gt("1-100")]]}, {2, []}]
        ])

      {:ok, cstore} = CheckpointInMemory.start_link([])
      {:ok, sstore} = SnapshotInMemory.start_link([])

      assert {:error, :snapshot_no_base_tables} =
               Snapshot.bootstrap(
                 [connection: fake_conn(), connect_fun: connect_fun],
                 %{snapshot_config() | tables: :all},
                 {CheckpointInMemory, cstore},
                 {SnapshotInMemory, sstore}
               )
    end

    test "an :all snapshot set resumes against a durable state (config :all reconciles)" do
      # C2b: the durable %State{} binds the RESOLVED set from the fresh start, so a
      # configured `:all` always reconciles — the stored set IS what `:all` meant. The
      # bootstrap proceeds PAST the reconcile (halting later at the refused connect,
      # NOT at :config_invalid). RED (old contract): reconcile refused `:all` outright.
      {:ok, cstore} = CheckpointInMemory.start_link([])

      connect_fun = fn _connection -> {:error, :should_not_connect} end

      assert {:error, :snapshot_query_connect_failed} =
               Snapshot.bootstrap(
                 [connection: fake_conn(), connect_fun: connect_fun],
                 %{snapshot_config() | tables: :all},
                 {CheckpointInMemory, cstore},
                 {MidSnapshotStore, :ignored}
               )
    end

    test "a store that RAISES during build_backfill CLOSES the query + fails closed (S-1, no leak)" do
      # A durable store that RAISES (not {:error}) once the query is open must NOT leak the
      # authenticated query connection. finish_or_close/6 rescues, closes the query, and returns a
      # value-free {:error}. Socket 2 (the query conn) MUST be closed afterward.
      connect_fun =
        scripted_connect_fun(
          [
            [uuid_result(@uuid_a)],
            [@good_precond, uuid_result(@uuid_a), {1, [[gt("1-100")]]}]
          ],
          self()
        )

      {:ok, sstore} = SnapshotInMemory.start_link([])

      assert {:error, :snapshot_bootstrap_crashed} =
               Snapshot.bootstrap(
                 [connection: fake_conn(), connect_fun: connect_fun],
                 snapshot_config(),
                 {RaisingCheckpointStore, :ignored},
                 {SnapshotInMemory, sstore}
               )

      # The query (2nd establish) is closed — no leaked source connection. RED (remove the
      # finish_or_close query-close): the bootstrap/4 rescue still returns {:error}, but the query
      # socket stays OPEN, so the send below succeeds instead of {:error, :closed}.
      assert_receive {:mock_socket, 2, {:gen_tcp, query_sock}}
      assert {:error, :closed} = :gen_tcp.send(query_sock, "x")
    end

    test "a SnapshotStore that RAISES on the initial read fails closed :snapshot_bootstrap_crashed (S-1 shape-gap)" do
      # A store raising on the FIRST read (before any source connection opens) — value-free
      # {:error}, never an uncaught exception out of start_link/1. RED: bootstrap raises uncaught.
      {:ok, cstore} = CheckpointInMemory.start_link([])

      assert {:error, :snapshot_bootstrap_crashed} =
               Snapshot.bootstrap(
                 [connection: fake_conn()],
                 snapshot_config(),
                 {CheckpointInMemory, cstore},
                 {RaisingSnapshotStore, :ignored}
               )
    end

    test "a stream-identity connect failure halts :snapshot_query_connect_failed" do
      connect_fun = fn _connection -> {:error, :econnrefused} end

      {:ok, cstore} = CheckpointInMemory.start_link([])
      {:ok, sstore} = SnapshotInMemory.start_link([])

      assert {:error, :snapshot_query_connect_failed} =
               Snapshot.bootstrap(
                 [connection: fake_conn(), connect_fun: connect_fun],
                 snapshot_config(),
                 {CheckpointInMemory, cstore},
                 {SnapshotInMemory, sstore}
               )
    end
  end

  ## ===========================================================================
  ## Live marquee — the bootstrap against mysql-cdc-probe (mix test --only live)
  ## ===========================================================================

  describe "live — a fresh bootstrap seeds P0 into BOTH stores" do
    @describetag :live

    setup do
      root = live_root_socket()
      ensure_sha2_user!(root)
      plant_table!(root, marquee_schema(), marquee_table(), 5)
      on_exit(fn -> drop_table!() end)
      close_socket(root)
      :ok
    end

    test "reads P0, seeds the checkpoint + %State{}, and returns processed_set = P0" do
      {:ok, cstore} = CheckpointInMemory.start_link([])
      {:ok, sstore} = SnapshotInMemory.start_link([])
      key = {marquee_schema(), marquee_table()}

      assert {:snapshot, %State{} = state, readers, processed} =
               Snapshot.bootstrap(
                 [connection: live_conn(), connect_fun: &Query.default_connect/1],
                 snapshot_config([key]),
                 {CheckpointInMemory, cstore},
                 {SnapshotInMemory, sstore}
               )

      # The %State{} was seeded: a real GTID floor, the table introspected, cursor at :start.
      assert state.status == :snapshotting
      assert is_binary(state.p0) and state.p0 != ""
      assert %{pk_cursor: :start, done?: false} = state.tables[key]

      # BOTH stores hold P0: the checkpoint (so dump+watermark resolve to P0) AND the %State{}.
      assert {:ok, %Position{gtid_set: cp}} =
               CheckpointStore.read_position(CheckpointInMemory, cstore)

      assert cp == state.p0
      assert {:ok, ^state} = SnapshotStore.read(SnapshotInMemory, sstore)

      # processed_set (the coordinator's advance-gate seed, Task-8-F2) is exactly P0.
      assert processed == state.p0

      # One reader per not-done snapshot table, ready for the coordinator.
      assert Map.has_key?(readers, key)
      Query.close(readers[key].query)
    end

    test "the full snapshot pipeline starts the coordinator, attaches it, and backfills the rows" do
      SnapshotSink.configure(self())
      on_exit(&SnapshotSink.clear/0)
      key = {marquee_schema(), marquee_table()}

      {:ok, sup} =
        Capstan.start_link(
          connection: live_conn(),
          server_id: 6100 + rem(System.unique_integer([:positive]), 100),
          sink: SnapshotSink,
          checkpoint_store: [module: CheckpointInMemory],
          snapshot: [
            tables: [key],
            store: [module: SnapshotInMemory],
            chunk_size: 4096
          ]
        )

      on_exit(fn -> MysqlCase.stop_pipeline(sup) end)

      # The coordinator child is wired (snapshot-active), the pure-C1 children are not bypassed.
      ids = sup |> Supervisor.which_children() |> Enum.map(&elem(&1, 0))
      assert :snapshot_coordinator in ids
      assert :assembler in ids
      assert :connection in ids

      # Advance the stream past the chunk's captured G so the buffered backfill chunk emits.
      writer = live_root_socket()

      Enum.each(6..8, fn i ->
        run!(
          writer,
          "INSERT INTO #{marquee_schema()}.#{marquee_table()} (id, n) VALUES (#{i}, #{i})"
        )
      end)

      close_socket(writer)

      # The pre-existing rows arrive via handle_snapshot (backfill delivery through the wiring).
      # The chunk's consistent-snapshot read may also race in a just-inserted row (6..8), so assert
      # the 5 pre-existing ids are a SUBSET of what was delivered rather than an exact count.
      assert_receive {:snapshot, table, records}, 20_000
      assert table == marquee_table()
      delivered = MapSet.new(records, & &1["id"])
      assert MapSet.subset?(MapSet.new(~w(1 2 3 4 5)), delivered)
    end
  end

  ## ===========================================================================
  ## Docker marquee — retention purge racing the bootstrap → :data_gap (tripwire 11)
  ## ===========================================================================

  describe "docker — retention purge racing the bootstrap" do
    @describetag :requires_docker

    test "the seed leaves a stale-but-gapped checkpoint, so the C1 gap gate fires :data_gap" do
      SnapshotSink.configure(self())
      on_exit(&SnapshotSink.clear/0)

      MysqlCase.with_throwaway_mysql([], fn port ->
        qconn = MysqlCase.socket!(MysqlCase.query_connection(port))
        grant_lock_tables!(qconn)

        # Lay the binlog so `stale` sits BEHIND a transaction that is then purged (gap_test's
        # "purge above the checkpoint remainder" shape): setup+INSERT(1) in file_N, `stale` =
        # @@gtid_executed after INSERT(1), INSERT(2) isolated in file_N+1, then PURGE removes both
        # file_N AND file_N+1 — so the stream resuming from `stale` still needs INSERT(2), which is
        # gone → :data_gap.
        MysqlCase.run_all!(qconn, [
          "DROP TABLE IF EXISTS boot_probe",
          "CREATE TABLE boot_probe (id INT PRIMARY KEY, n INT) ENGINE=InnoDB",
          "INSERT INTO boot_probe (id, n) VALUES (1, 1)",
          "FLUSH BINARY LOGS"
        ])

        stale = MysqlCase.read_gtid_executed!(qconn)

        MysqlCase.run_all!(qconn, [
          "INSERT INTO boot_probe (id, n) VALUES (2, 2)",
          "FLUSH BINARY LOGS"
        ])

        purge_target = newest_binlog(qconn)
        MysqlCase.run!(qconn, "PURGE BINARY LOGS TO '#{purge_target}'")

        # A DURABLE checkpoint pinned to the now-purged `stale` watermark. The snapshot store is a
        # fresh InMemory (nil → the FRESH bootstrap path), so the bootstrap reads a fresh
        # @@gtid_executed but seed_checkpoint LEAVES `stale` (≠ fresh). RED (map 1236 to :ok / seed
        # the current position): the resume floor would be current, no gap, no :data_gap.
        table = DurableStore.new_table()
        DurableStore.seed(table, :boot, stale)

        halt_handler = MysqlCase.attach_halt_telemetry(self())
        on_exit(fn -> :telemetry.detach(halt_handler) end)

        {:ok, sup} =
          Capstan.start_link(
            connection: MysqlCase.pipeline_connection(port),
            server_id: MysqlCase.unique_server_id(),
            sink: SnapshotSink,
            checkpoint_store: [module: DurableStore, options: [table: table, key: :boot]],
            snapshot: [
              tables: [{"probe_db", "boot_probe"}],
              store: [module: SnapshotInMemory],
              chunk_size: 128
            ]
          )

        on_exit(fn -> MysqlCase.stop_pipeline(sup) end)

        assert_receive {:connection_halt, :data_gap}, 20_000

        MysqlCase.close!(qconn)
      end)
    end
  end

  ## ---------------------------------------------------------------------------
  ## helpers — config shapes
  ## ---------------------------------------------------------------------------

  defp snapshot_config(tables \\ [{"probe_db", "capstan_bootstrap_marquee"}]) do
    %{tables: tables, store: {SnapshotInMemory, []}, chunk_size: 4096}
  end

  defp fake_conn do
    [
      host: "127.0.0.1",
      port: Capstan.MysqlCase.shared_port(),
      username: "capstan_sha2",
      password: "pw",
      ssl: false
    ]
  end

  defp seed(cstore, gtid_set) do
    :ok =
      CheckpointStore.write_position(
        CheckpointInMemory,
        cstore,
        Position.from_persisted(gtid_set)
      )
  end

  ## ---------------------------------------------------------------------------
  ## helpers — scripted mock MySQL server (the Capstan.QueryTest idiom)
  ## ---------------------------------------------------------------------------

  defp scripted_connect_fun(sequences, capture \\ nil) do
    {:ok, agent} = Agent.start_link(fn -> sequences end)
    {:ok, counter} = Agent.start_link(fn -> 0 end)
    on_exit(fn -> if Process.alive?(agent), do: Agent.stop(agent) end)
    on_exit(fn -> if Process.alive?(counter), do: Agent.stop(counter) end)

    fn _connection ->
      case pop_script(agent) do
        :none -> {:error, :no_more_scripts}
        responses -> establish_mock(responses, counter, capture)
      end
    end
  end

  # Spawn the mock server for one establish; optionally report the socket (with its 1-based
  # establish index) to a capture pid so a leak test can assert the query conn (index 2) is closed.
  defp establish_mock(responses, counter, capture) do
    socket = spawn_mock_server(responses)
    index = Agent.get_and_update(counter, &{&1 + 1, &1 + 1})
    if capture, do: send(capture, {:mock_socket, index, socket})
    {:ok, socket, %{}}
  end

  defp pop_script(agent) do
    Agent.get_and_update(agent, fn
      [head | tail] -> {head, tail}
      [] -> {:none, []}
    end)
  end

  defp spawn_mock_server(responses) do
    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true, ip: @loopback])
    {:ok, port} = :inet.port(listen)

    spawn(fn ->
      {:ok, srv} = :gen_tcp.accept(listen, 5000)
      :gen_tcp.close(listen)
      serve({:gen_tcp, srv}, responses)
      :gen_tcp.close(srv)
    end)

    {:ok, client} = :gen_tcp.connect(@loopback, port, [:binary, active: false], 5000)
    on_exit(fn -> :gen_tcp.close(client) end)
    {:gen_tcp, client}
  end

  defp serve(srv, responses) do
    Enum.reduce_while(responses, :ok, fn response, _acc ->
      case recv_request(srv) do
        :closed ->
          {:halt, :ok}

        :ok ->
          send_response(srv, response)
          {:cont, :ok}
      end
    end)
  end

  defp recv_request({:gen_tcp, s}) do
    case :gen_tcp.recv(s, 4, 5000) do
      {:ok, <<len::24-little, _seq::8>>} ->
        {:ok, _payload} = :gen_tcp.recv(s, len, 5000)
        :ok

      {:error, _reason} ->
        :closed
    end
  end

  defp send_response(srv, {ncols, rows}) do
    t_send(srv, <<ncols>>, 1)
    Enum.each(1..ncols, fn i -> t_send(srv, "coldef_#{i}", i + 1) end)

    rows
    |> Enum.with_index(ncols + 2)
    |> Enum.each(fn {row, seq} -> t_send(srv, encode_text_row(row), seq) end)

    t_send(srv, <<0xFE, 0, 0, 2, 0, 0, 0>>, ncols + 2 + length(rows))
  end

  defp encode_text_row(values) do
    Enum.reduce(values, <<>>, fn value, acc -> acc <> <<byte_size(value)::8, value::binary>> end)
  end

  defp t_send({:gen_tcp, s}, payload, seq), do: :ok = :gen_tcp.send(s, frame(payload, seq))
  defp frame(payload, seq), do: <<byte_size(payload)::24-little, seq::8, payload::binary>>

  ## ---------------------------------------------------------------------------
  ## helpers — live substrate (shared mysql-cdc-probe)
  ## ---------------------------------------------------------------------------

  defp marquee_schema, do: "probe_db"
  defp marquee_table, do: "capstan_bootstrap_marquee"

  defp live_conn do
    [
      host: "127.0.0.1",
      port: Capstan.MysqlCase.shared_port(),
      username: "capstan_sha2",
      password: "capstan_sha2_pw",
      database: "probe_db",
      ssl: true,
      ssl_opts: [verify: :verify_none]
    ]
  end

  defp live_root_socket do
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

  defp ensure_sha2_user!(socket) do
    run!(
      socket,
      "CREATE USER IF NOT EXISTS 'capstan_sha2'@'%' " <>
        "IDENTIFIED WITH caching_sha2_password BY 'capstan_sha2_pw'"
    )

    run!(
      socket,
      "GRANT SELECT, LOCK TABLES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* " <>
        "TO 'capstan_sha2'@'%'"
    )
  end

  defp plant_table!(socket, schema, table, rows) do
    run!(socket, "CREATE DATABASE IF NOT EXISTS #{schema}")
    run!(socket, "DROP TABLE IF EXISTS #{schema}.#{table}")
    run!(socket, "CREATE TABLE #{schema}.#{table} (id INT PRIMARY KEY, n INT) ENGINE=InnoDB")

    Enum.each(1..rows, fn i ->
      run!(socket, "INSERT INTO #{schema}.#{table} (id, n) VALUES (#{i}, #{i * 10})")
    end)
  end

  defp drop_table! do
    socket = live_root_socket()
    run!(socket, "DROP TABLE IF EXISTS #{marquee_schema()}.#{marquee_table()}")
    close_socket(socket)
  end

  defp run!(socket, sql) do
    case Command.query(socket, sql) do
      :ok -> :ok
      {:ok, _rows} -> :ok
      {:error, reason} -> raise "bootstrap_test: #{sql} failed #{inspect(reason)}"
    end
  end

  defp grant_lock_tables!(socket) do
    run!(socket, "GRANT LOCK TABLES ON *.* TO 'capstan_sha2'@'%'")
  end

  defp newest_binlog(qconn) do
    MysqlCase.query_rows!(qconn, "SHOW BINARY LOGS") |> List.last() |> hd()
  end

  defp close_socket({:gen_tcp, s}), do: :gen_tcp.close(s)
  defp close_socket({:ssl, s}), do: :ssl.close(s)
end

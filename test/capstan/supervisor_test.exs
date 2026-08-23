defmodule Capstan.SupervisorTest.LibSink do
  @moduledoc false
  # A valid LIB-owned sink: handle_transaction/1 + handle_schema_change/2 (C1 always
  # delivers DDL, so handle_schema_change/2 is required). No checkpoint/0 — lib mode does
  # not consult it.
  @behaviour Capstan.Sink
  @impl true
  def handle_transaction(_txn), do: {:ok, %Capstan.Position{gtid_set: "", file: nil, pos: nil}}
  @impl true
  def handle_schema_change(_change, _position), do: :ok
end

defmodule Capstan.SupervisorTest.SinkOwnedSink do
  @moduledoc false
  # A valid SINK-owned sink: checkpoint/0 + handle_transaction/1 + handle_schema_change/2.
  @behaviour Capstan.Sink
  @impl true
  def checkpoint, do: {:ok, nil}
  @impl true
  def handle_transaction(_txn), do: {:ok, %Capstan.Position{gtid_set: "", file: nil, pos: nil}}
  @impl true
  def handle_schema_change(_change, _position), do: :ok
end

defmodule Capstan.SupervisorTest.NoTransactionSink do
  @moduledoc false
  # Missing the mandatory handle_transaction/1 (has the others).
  def checkpoint, do: {:ok, nil}
  def handle_schema_change(_change, _position), do: :ok
end

defmodule Capstan.SupervisorTest.NoCheckpointSink do
  @moduledoc false
  # Has handle_transaction/1 + handle_schema_change/2 but NO checkpoint/0 — invalid in
  # sink-owned mode, valid in lib mode.
  def handle_transaction(_txn), do: {:ok, %Capstan.Position{gtid_set: "", file: nil, pos: nil}}
  def handle_schema_change(_change, _position), do: :ok
end

defmodule Capstan.SupervisorTest.NoSchemaChangeSink do
  @moduledoc false
  # Has checkpoint/0 + handle_transaction/1 but NO handle_schema_change/2 — invalid
  # whenever DDL delivery is enabled (C1: always).
  def checkpoint, do: {:ok, nil}
  def handle_transaction(_txn), do: {:ok, %Capstan.Position{gtid_set: "", file: nil, pos: nil}}
end

defmodule Capstan.SupervisorTest.SnapshotSink do
  @moduledoc false
  # A valid SNAPSHOT-mode sink: the lib-owned trio + handle_snapshot/2.
  @behaviour Capstan.Sink
  @impl true
  def handle_transaction(_txn), do: {:ok, %Capstan.Position{gtid_set: "", file: nil, pos: nil}}
  @impl true
  def handle_schema_change(_change, _position), do: :ok
  @impl true
  def handle_snapshot(_changes, _meta), do: :ok
end

defmodule Capstan.SupervisorTest.CompleteSnapStore do
  @moduledoc false
  # A SnapshotStore whose durable state is already `status: :complete`, so the bootstrap
  # short-circuits to pure C1 (no coordinator) WITHOUT opening a source connection. Its stored
  # table set MATCHES the config's `[{"probe_db", "orders"}]` so `reconcile_tables/2` (F3) passes.
  @behaviour Capstan.SnapshotStore
  def start_link(_opts), do: Agent.start_link(fn -> :complete end)

  def read(_store) do
    {:ok,
     %Capstan.Snapshot.State{
       status: :complete,
       p0: "u:1-9",
       tables: %{
         {"probe_db", "orders"} => %{
           fingerprint: "fp",
           pk_columns: ["id"],
           pk_types: [:integer],
           pk_cursor: 1,
           delivered_pk: 1,
           done?: true
         }
       }
     }}
  end

  def write(_store, _state), do: :ok
end

defmodule Capstan.SupervisorTest do
  use ExUnit.Case, async: false

  alias Capstan.Error
  alias Capstan.Pipeline
  alias Capstan.Telemetry
  alias Capstan.ValueFree

  alias Capstan.SupervisorTest.{
    CompleteSnapStore,
    LibSink,
    NoCheckpointSink,
    NoSchemaChangeSink,
    NoTransactionSink,
    SinkOwnedSink,
    SnapshotSink
  }

  # A config that validates (pure) but points the connection at a closed port, so a real
  # pipeline starts (store + AssemblerServer + Connection) WITHOUT a live MySQL: the
  # Connection just retries the refused connect while the AssemblerServer runs.
  defp lib_opts(overrides \\ []) do
    Keyword.merge(
      [
        connection: [
          host: "127.0.0.1",
          port: 1,
          username: "capstan",
          password: "secret",
          ssl: false
        ],
        server_id: 42,
        sink: LibSink,
        checkpoint_store: [module: Capstan.CheckpointStore.InMemory]
      ],
      overrides
    )
  end

  describe "Capstan.Telemetry — the value-free metadata allowlist" do
    test "allowed_meta_keys/0 covers exactly the design's value-free metadata keys" do
      assert Enum.sort(Telemetry.allowed_meta_keys()) ==
               Enum.sort(
                 ~w(server_version server_uuid tls reason gtid schema table kind missing_gtids)a
               )
    end

    test "event/3 with only allowlisted keys fires" do
      attach([[:capstan, :connection, :halt]])
      assert :ok = Telemetry.event([:capstan, :connection, :halt], %{}, %{reason: :data_gap})
      assert_receive {:telemetry, [:capstan, :connection, :halt], %{}, %{reason: :data_gap}}
    end

    test "event/3 REFUSES a non-allowlisted metadata key (a stray value can never ride)" do
      assert_raise ArgumentError, ~r/not in the value-free allowlist/, fn ->
        Telemetry.event([:capstan, :transaction, :committed], %{}, %{row_value: "secret_pii"})
      end
    end

    test "event/3 REFUSES a non-numeric measurement value (numbers carry no identity)" do
      # The metadata channel gates KEYS; the measurement channel gates BOTH — only
      # allowlisted keys with non-negative numbers may ride — so a row value or password
      # can never travel as a measurement either.
      assert_raise ArgumentError, ~r/measurements must be numeric/, fn ->
        Telemetry.event(
          [:capstan, :transaction, :committed],
          %{sink_ms: "12ms"},
          %{gtid: "u:1"}
        )
      end
    end

    test "event/3 REFUSES an off-list measurement KEY even with a numeric value (numeric PII class)" do
      # A numeric row value (a balance, an account number) IS a non-negative number — a
      # type-only gate would let it ride. The measurement channel is key-allowlisted,
      # symmetric with the metadata channel.
      assert_raise ArgumentError, ~r/measurement keys/, fn ->
        Telemetry.event(
          [:capstan, :transaction, :committed],
          %{balance: 123_456},
          %{gtid: "u:1"}
        )
      end
    end

    test "the measurement-gate exception NEVER carries the offending VALUE (Rule 1)" do
      # The refusal is the leak vector's last stand: interpolating the invalid map into
      # the message would ship the secret in the crash report. Keys only.
      err =
        assert_raise ArgumentError, fn ->
          Telemetry.event(
            [:capstan, :transaction, :committed],
            %{row_value: "secret_pii"},
            %{gtid: "u:1"}
          )
        end

      refute err.message =~ "secret_pii"
      assert err.message =~ "row_value"
    end

    test "validate!/1 returns allowlisted metadata unchanged and raises on any off-list key" do
      assert %{gtid: "u:1"} = Telemetry.validate!(%{gtid: "u:1"})

      assert_raise ArgumentError, ~r/password/, fn ->
        Telemetry.validate!(%{gtid: "u:1", password: "hunter2"})
      end
    end
  end

  describe "Capstan.Error — the value-free boundary normaliser" do
    test "from/1 keeps a value-free atom reason" do
      assert %Error{reason: :data_gap, shape: nil} = Error.from(:data_gap)
    end

    test "from/1 DISCARDS an exception message (which may embed a value), keeping only the module" do
      err = Error.from(%RuntimeError{message: "duplicate entry 'topsecret' for key 'PRIMARY'"})
      assert %Error{reason: :unknown, shape: "RuntimeError"} = err
      refute Error.message(err) =~ "topsecret"
      refute inspect(err) =~ "topsecret"
    end

    test "from/1 keeps only the atom tag of a {tag, payload} tuple, discarding the payload" do
      err = Error.from({:connect_failed, [password: "hunter2"]})
      assert %Error{reason: :connect_failed, shape: nil} = err
      refute Error.message(err) =~ "hunter2"
      refute inspect(err) =~ "hunter2"
    end

    test "from/1 maps an unrecognised term to :unknown with no retained payload" do
      assert %Error{reason: :unknown, shape: nil} =
               Error.from("a raw string that could be a value")
    end

    test "message/1 and inspect render only structural fields" do
      err = %Error{reason: :sink_missing_checkpoint, shape: nil}
      assert Error.message(err) == "capstan error reason=sink_missing_checkpoint"
      assert inspect(err) =~ "#Capstan.Error<"
    end
  end

  describe "Capstan.Pipeline.validate_sink/1 — per-mode callback required-ness (routed from Task 13)" do
    test "lib-owned: a sink exporting handle_transaction/1 (+ handle_schema_change/2) is accepted" do
      assert :ok = Pipeline.validate_sink(sink: LibSink, checkpoint_store: [module: X])
    end

    test "lib-owned: a sink missing handle_transaction/1 is REFUSED at start-up" do
      assert {:error, :sink_missing_handle_transaction} =
               Pipeline.validate_sink(sink: NoTransactionSink, checkpoint_store: [module: X])
    end

    test "sink-owned: a sink exporting checkpoint/0 + handle_transaction/1 (+ schema) is accepted" do
      assert :ok = Pipeline.validate_sink(sink: SinkOwnedSink)
    end

    test "sink-owned: a sink missing checkpoint/0 is REFUSED at start-up" do
      assert {:error, :sink_missing_checkpoint} =
               Pipeline.validate_sink(sink: NoCheckpointSink)
    end

    test "sink-owned: a sink missing handle_transaction/1 is REFUSED at start-up" do
      assert {:error, :sink_missing_handle_transaction} =
               Pipeline.validate_sink(sink: NoTransactionSink)
    end

    test "DDL enabled: a sink missing handle_schema_change/2 is REFUSED at start-up" do
      assert {:error, :sink_missing_handle_schema_change} =
               Pipeline.validate_sink(sink: NoSchemaChangeSink)
    end

    test "a sink that is not a loaded module is REFUSED" do
      assert {:error, :invalid_sink} = Pipeline.validate_sink(sink: NotARealModule)
      assert {:error, :invalid_sink} = Pipeline.validate_sink(sink: nil)
      assert {:error, :invalid_sink} = Pipeline.validate_sink(sink: "notamodule")
    end
  end

  describe "Capstan.start_link/1 — refuses a bad substrate before starting" do
    test "a missing server_id is refused (Config validation)" do
      assert {:error, :server_id_required} = Capstan.start_link(lib_opts(server_id: nil))
    end

    test "a mis-shaped connection is refused (Config validation)" do
      assert {:error, :config_invalid} = Capstan.start_link(lib_opts(connection: :not_a_keyword))
    end

    test "TLS on with no verification choice is refused fail-closed (Config F6/Q17)" do
      opts =
        lib_opts(
          connection: [host: "127.0.0.1", port: 1, username: "u", password: "p", ssl: true]
        )

      assert {:error, :tls_verification_unspecified} = Capstan.start_link(opts)
    end

    test "a lib-mode sink missing handle_transaction/1 is refused (per-mode sink check)" do
      assert {:error, :sink_missing_handle_transaction} =
               Capstan.start_link(lib_opts(sink: NoTransactionSink))
    end

    test "a valid SINK-OWNED config now RUNS (C1a) — no store child, checkpoint from the sink" do
      # SinkOwnedSink is a VALID sink-owned sink (checkpoint/0 + handle_transaction/1 +
      # handle_schema_change/2). C1a landed: the mode runs — the AssemblerServer seeds
      # its resume position from c:Sink.checkpoint/0 and NO store child is wired.
      {:ok, sup} =
        Capstan.start_link(
          connection: [host: "127.0.0.1", port: 1, username: "u", password: "p", ssl: false],
          server_id: 42,
          sink: SinkOwnedSink
        )

      on_exit(fn -> stop_supervisor(sup) end)

      ids = sup |> Supervisor.which_children() |> Enum.map(&elem(&1, 0))
      assert :assembler in ids
      assert :connection in ids
      refute :store in ids
    end

    test "a lib-mode checkpoint_store missing :module is refused cleanly, not a KeyError" do
      assert {:error, :checkpoint_store_required} =
               Capstan.start_link(lib_opts(checkpoint_store: [options: []]))

      assert {:error, :checkpoint_store_required} =
               Capstan.start_link(lib_opts(checkpoint_store: "not-a-keyword"))
    end

    test "an explicit %Position{} start_position override is refused fail-closed (C1)" do
      # C1 resumes only from the durable checkpoint; honoring an explicit override
      # end-to-end is a follow-up, so it is refused rather than silently creating a
      # checkpoint hole (dump resumes from the override, watermark from the empty store).
      assert {:error, :start_position_override_unsupported} =
               Capstan.start_link(lib_opts(start_position: %Capstan.Position{gtid_set: "x:1-5"}))
    end

    test "start_position: :current is refused fail-closed (C1 does not wire it)" do
      assert {:error, :start_position_current_unsupported} =
               Capstan.start_link(lib_opts(start_position: :current))
    end

    test "a valid lib-mode config starts a supervised pipeline" do
      assert {:ok, sup} = Capstan.start_link(lib_opts())
      on_exit(fn -> stop_supervisor(sup) end)
      assert is_pid(sup)
      assert Process.alive?(sup)
      # The pipeline is wired: an AssemblerServer child and a Connection child are running.
      ids = sup |> Supervisor.which_children() |> Enum.map(&elem(&1, 0))
      assert :assembler in ids
      assert :connection in ids
    end
  end

  describe "Capstan.start_link/1 — streaming liveness options (documented contract)" do
    test "stream_timeout_ms == heartbeat_period_ms is refused :invalid_liveness_config" do
      # usage-rules.md promises this refusal through start_link/1; wiring/3 dropping the
      # keys made it unreachable (the pair below starts a pipeline only if IGNORED).
      opts = lib_opts(heartbeat_period_ms: 15_000, stream_timeout_ms: 15_000)

      assert {:error, :invalid_liveness_config} = Capstan.start_link(opts)
    end

    test "a stream_timeout_ms below the DEFAULT heartbeat is refused (defaults apply before the comparison)" do
      # Only the window is overridden. The 15_000 default heartbeat must be applied and
      # compared at config time, or the pair flows onward and surfaces as a wrapped
      # failed-child reason instead of the documented bare atom.
      assert {:error, :invalid_liveness_config} =
               Capstan.start_link(lib_opts(stream_timeout_ms: 10_000))
    end

    test "valid liveness overrides propagate to the live Connection, never silently ignored" do
      {:ok, sup} =
        Capstan.start_link(
          lib_opts(
            reconnect_backoff: 2_000,
            heartbeat_period_ms: 5_000,
            stream_timeout_ms: 6_000
          )
        )

      on_exit(fn -> stop_supervisor(sup) end)

      # The closed-port Connection retries while its state is observed directly — the
      # established wiring-observation pattern (see the snapshot :complete test above).
      state = :sys.get_state(child_pid(sup, :connection))
      assert state.reconnect_backoff == 2_000
      assert state.heartbeat_period_ms == 5_000
      assert state.stream_timeout_ms == 6_000
    end

    test "a misspelled option key is refused :unknown_option through start_link" do
      # stream_timeout: for stream_timeout_ms: — the typo would otherwise silently apply
      # the default, the exact ignored-config class the fail-closed posture forbids.
      assert {:error, :unknown_option} = Capstan.start_link(lib_opts(stream_timeout: 5_000))
    end

    test "an unknown checkpoint_store block key is refused :unknown_option" do
      assert {:error, :unknown_option} =
               Capstan.start_link(
                 lib_opts(checkpoint_store: [module: Capstan.CheckpointStore.InMemory, modul: X])
               )
    end

    test "a duplicated known checkpoint_store key is first-wins, matching the snapshot store block" do
      # Keyword semantics: the first occurrence wins, and BOTH store blocks treat
      # duplicates identically (the snapshot side's membership test accepts them).
      {:ok, sup} =
        Capstan.start_link(
          lib_opts(checkpoint_store: [module: Capstan.CheckpointStore.InMemory, module: NotTaken])
        )

      on_exit(fn -> stop_supervisor(sup) end)
      ids = sup |> Supervisor.which_children() |> Enum.map(&elem(&1, 0))
      assert :store in ids
    end
  end

  describe "supervision — a fail-closed halt does not take down the host supervisor" do
    test "an AssemblerServer halt stops the pipeline without restarting it or killing the host" do
      # A connect that hangs keeps the Connection alive-but-idle, so the ONLY halt in the
      # window is the one we drive into the AssemblerServer (no race with a Connection
      # halt). The seam fails closed in prod — Capstan.start_link/1 never injects it.
      {:ok, sup} =
        Capstan.Supervisor.start_link(
          sink: LibSink,
          checkpoint_store: [module: Capstan.CheckpointStore.InMemory],
          connection: [],
          server_id: 42,
          connect_fun: fn _connection -> Process.sleep(:infinity) end
        )

      on_exit(fn -> stop_supervisor(sup) end)

      assembler = child_pid(sup, :assembler)
      assert is_pid(assembler)
      ref = Process.monitor(assembler)

      # Drive the exact fail-closed halt the AssemblerServer performs: {:shutdown,
      # {:halt, reason}} — NOT a crash, so a :temporary child is not restarted.
      send(assembler, {:capstan_halt, :sink_error_probe})

      assert_receive {:DOWN, ^ref, :process, ^assembler, {:shutdown, {:halt, :sink_error_probe}}},
                     2000

      # The host supervisor survives, and the halted child is NOT restarted into a livelock.
      Process.sleep(100)
      assert Process.alive?(sup)
      assert child_pid(sup, :assembler) in [nil, :undefined]
    end
  end

  describe "snapshot wiring — absent :snapshot is byte-identical C1; :complete is pure C1" do
    test "an absent :snapshot key wires NO snapshot children (byte-identical C1)" do
      assert {:ok, sup} = Capstan.start_link(lib_opts())
      on_exit(fn -> stop_supervisor(sup) end)

      ids = sup |> Supervisor.which_children() |> Enum.map(&elem(&1, 0))
      assert :store in ids
      assert :assembler in ids
      assert :connection in ids
      # The snapshot-mode children are NOT wired when :snapshot is absent.
      refute :snapshot_store in ids
      refute :snapshot_coordinator in ids
    end

    test "a :complete snapshot store starts NO coordinator (pure C1, real sink wired directly)" do
      # The connection points at a closed port (retries), so no live substrate is needed; the
      # bootstrap reads the :complete store and short-circuits BEFORE any source connection.
      {:ok, sup} =
        Capstan.Supervisor.start_link(
          connection: [host: "127.0.0.1", port: 1, username: "u", password: "p", ssl: false],
          server_id: 42,
          sink: SnapshotSink,
          checkpoint_store: [module: Capstan.CheckpointStore.InMemory],
          snapshot: %{
            tables: [{"probe_db", "orders"}],
            store: {CompleteSnapStore, []},
            chunk_size: 4096
          }
        )

      on_exit(fn -> stop_supervisor(sup) end)

      ids = sup |> Supervisor.which_children() |> Enum.map(&elem(&1, 0))
      assert :assembler in ids
      assert :connection in ids
      # :complete ⇒ the coordinator is NOT started; the real sink is wired directly.
      refute :snapshot_coordinator in ids
      # The assembler's sink is the REAL sink (not the coordinator module).
      assert %{sink: SnapshotSink} = :sys.get_state(child_pid(sup, :assembler))
    end
  end

  describe "Rule 1 — value-free error/halt paths (row-value + password vectors, F11)" do
    test "a row value never reaches a log line or a telemetry metadata payload" do
      assert :ok = ValueFree.assert_row_value_free()
    end

    test "the connection password never reaches a log line or a telemetry metadata payload" do
      assert :ok = ValueFree.assert_password_free()
    end
  end

  ## ---------------------------------------------------------------------------
  ## helpers
  ## ---------------------------------------------------------------------------

  def forward_telemetry(event, measurements, metadata, %{test: test}) do
    send(test, {:telemetry, event, measurements, metadata})
  end

  defp attach(events) do
    handler_id = {__MODULE__, make_ref()}

    :ok =
      :telemetry.attach_many(handler_id, events, &__MODULE__.forward_telemetry/4, %{test: self()})

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  defp child_pid(sup, id) do
    sup
    |> Supervisor.which_children()
    |> Enum.find_value(nil, fn {child_id, pid, _type, _mods} -> child_id == id && pid end)
  end

  defp stop_supervisor(sup) do
    if Process.alive?(sup), do: Supervisor.stop(sup)
  catch
    :exit, _ -> :ok
  end
end

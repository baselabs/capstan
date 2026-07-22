defmodule Capstan.PipelineTest.SnapshotSink do
  @moduledoc false
  # A valid SNAPSHOT-mode sink: handle_transaction/1 + handle_schema_change/2 + handle_snapshot/2.
  @behaviour Capstan.Sink
  @impl true
  def handle_transaction(_txn), do: {:ok, %Capstan.Position{gtid_set: "", file: nil, pos: nil}}
  @impl true
  def handle_schema_change(_change, _position), do: :ok
  @impl true
  def handle_snapshot(_changes, _meta), do: :ok
end

defmodule Capstan.PipelineTest.NoSnapshotSink do
  @moduledoc false
  # A valid C1 sink WITHOUT handle_snapshot/2 — valid in pure C1, invalid in snapshot mode.
  @behaviour Capstan.Sink
  @impl true
  def handle_transaction(_txn), do: {:ok, %Capstan.Position{gtid_set: "", file: nil, pos: nil}}
  @impl true
  def handle_schema_change(_change, _position), do: :ok
end

defmodule Capstan.PipelineTest.OnlySnapshotSink do
  @moduledoc false
  # Has handle_snapshot/2 (+ handle_schema_change/2) but NOT handle_transaction/1 — the C1
  # required-ness (handle_transaction/1) must still win FIRST in snapshot mode.
  def handle_schema_change(_change, _position), do: :ok
  def handle_snapshot(_changes, _meta), do: :ok
end

defmodule Capstan.PipelineTest do
  use ExUnit.Case, async: true

  alias Capstan.Pipeline
  alias Capstan.PipelineTest.{NoSnapshotSink, OnlySnapshotSink, SnapshotSink}
  alias Capstan.Snapshot.Coordinator
  alias Capstan.Snapshot.State

  # A lib-owned checkpoint store so the snapshot-mode validate_sink path is not masked by the
  # sink-owned `checkpoint/0` requirement.
  @store [module: Capstan.CheckpointStore.InMemory]
  @snapshot_opt [store: [module: MyApp.SnapStore]]

  ## ---------------------------------------------------------------------------
  ## validate_sink/1 — snapshot mode requires handle_snapshot/2 (C2)
  ## ---------------------------------------------------------------------------

  describe "validate_sink/1 — snapshot mode requires handle_snapshot/2" do
    test "a snapshot-mode sink WITH handle_snapshot/2 passes" do
      assert :ok =
               Pipeline.validate_sink(
                 sink: SnapshotSink,
                 checkpoint_store: @store,
                 snapshot: @snapshot_opt
               )
    end

    test "a snapshot-mode sink WITHOUT handle_snapshot/2 → :sink_missing_handle_snapshot" do
      assert {:error, :sink_missing_handle_snapshot} =
               Pipeline.validate_sink(
                 sink: NoSnapshotSink,
                 checkpoint_store: @store,
                 snapshot: @snapshot_opt
               )
    end

    test "the SAME sink (no handle_snapshot/2) is valid when :snapshot is absent (pure C1)" do
      assert :ok = Pipeline.validate_sink(sink: NoSnapshotSink, checkpoint_store: @store)
    end

    test "handle_transaction/1 is still required FIRST in snapshot mode" do
      assert {:error, :sink_missing_handle_transaction} =
               Pipeline.validate_sink(
                 sink: OnlySnapshotSink,
                 checkpoint_store: @store,
                 snapshot: @snapshot_opt
               )
    end
  end

  ## ---------------------------------------------------------------------------
  ## snapshot_mode?/1
  ## ---------------------------------------------------------------------------

  describe "snapshot_mode?/1" do
    test "true when a non-nil :snapshot block is present" do
      assert Pipeline.snapshot_mode?(snapshot: @snapshot_opt)
    end

    test "false when :snapshot is absent" do
      refute Pipeline.snapshot_mode?(sink: SnapshotSink, checkpoint_store: @store)
    end

    test "false when :snapshot is nil" do
      refute Pipeline.snapshot_mode?(snapshot: nil)
    end
  end

  ## ---------------------------------------------------------------------------
  ## validate_snapshot_tables/2 — snapshot tables ⊆ captured allowlist
  ## ---------------------------------------------------------------------------

  describe "validate_snapshot_tables/2 — snapshot tables ⊆ captured" do
    test "a subset of a concrete captured list passes" do
      opts = [tables: [{"o", "orders"}, {"o", "customers"}]]
      snap = %{tables: [{"o", "orders"}], store: {MyApp.SnapStore, []}, chunk_size: 4096}
      assert :ok = Pipeline.validate_snapshot_tables(opts, snap)
    end

    test "a snapshot table outside the captured list → :snapshot_table_not_captured" do
      opts = [tables: [{"o", "orders"}]]
      snap = %{tables: [{"o", "ghost"}], store: {MyApp.SnapStore, []}, chunk_size: 4096}

      assert {:error, :snapshot_table_not_captured} =
               Pipeline.validate_snapshot_tables(opts, snap)
    end

    test ":all capture trivially includes every snapshot table" do
      opts = [tables: :all]
      snap = %{tables: [{"o", "orders"}], store: {MyApp.SnapStore, []}, chunk_size: 4096}
      assert :ok = Pipeline.validate_snapshot_tables(opts, snap)
    end

    test "capture defaulting to :all (no :tables key) trivially includes snapshot tables" do
      snap = %{tables: [{"o", "orders"}], store: {MyApp.SnapStore, []}, chunk_size: 4096}
      assert :ok = Pipeline.validate_snapshot_tables([], snap)
    end

    test "nil snapshot (pure C1) is a no-op :ok" do
      assert :ok = Pipeline.validate_snapshot_tables([tables: [{"o", "orders"}]], nil)
    end

    test "a snapshot table set of :all against a concrete capture list is refused" do
      opts = [tables: [{"o", "orders"}]]
      snap = %{tables: :all, store: {MyApp.SnapStore, []}, chunk_size: 4096}

      assert {:error, :snapshot_table_not_captured} =
               Pipeline.validate_snapshot_tables(opts, snap)
    end
  end

  ## ---------------------------------------------------------------------------
  ## child-spec builders — SnapshotStore + Coordinator (consumed by Task 10's supervisor)
  ## ---------------------------------------------------------------------------

  describe "snapshot_store_spec/2" do
    test "builds a :temporary snapshot-store child spec" do
      spec = Pipeline.snapshot_store_spec(Capstan.SnapshotStore.InMemory, name: :snap)

      assert %{
               id: :snapshot_store,
               restart: :temporary,
               start: {Capstan.SnapshotStore.InMemory, :start_link, [[name: :snap]]}
             } = spec
    end
  end

  describe "coordinator_spec/2" do
    test "threads the pipeline sink + bootstrap wiring into the coordinator start opts" do
      state = %State{status: :snapshotting, p0: "uuid:1-5", tables: %{}}
      readers = %{{"o", "orders"} => :reader_handle}
      store = {Capstan.SnapshotStore.InMemory, :store_pid}

      spec =
        Pipeline.coordinator_spec(
          [sink: SnapshotSink, max_command_retries: 7],
          assembler: :assembler_pid,
          snapshot_store: store,
          snapshot_state: state,
          readers: readers,
          processed_set: "uuid:1-5"
        )

      assert %{
               id: :snapshot_coordinator,
               restart: :temporary,
               start: {Coordinator, :start_link, [coord_opts]}
             } = spec

      assert coord_opts[:sink] == SnapshotSink
      assert coord_opts[:assembler] == :assembler_pid
      assert coord_opts[:snapshot_store] == store
      assert coord_opts[:snapshot_state] == state
      assert coord_opts[:readers] == readers
      assert coord_opts[:processed_set] == "uuid:1-5"
      assert coord_opts[:max_retries] == 7
    end

    test "processed_set defaults to \"\" when omitted from the wiring" do
      state = %State{tables: %{}}

      spec =
        Pipeline.coordinator_spec(
          [sink: SnapshotSink],
          assembler: :a,
          snapshot_store: {MyApp.SnapStore, :h},
          snapshot_state: state,
          readers: %{}
        )

      assert %{start: {Coordinator, :start_link, [coord_opts]}} = spec
      assert coord_opts[:processed_set] == ""
    end
  end
end

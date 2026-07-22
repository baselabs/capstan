defmodule Capstan.SnapshotStoreTest do
  @moduledoc """
  Task 6 (C2) — the durable snapshot-progress behaviour. Mirrors `CheckpointStore`: one
  durable value per pipeline (a whole `%Capstan.Snapshot.State{}`, Pinned decision #3), an
  `InMemory` process-lifetime reference impl, and a fail-closed read/write contract — a store
  fault PROPAGATES (never masquerades as "never snapshotted"), else a re-bootstrap would
  re-scan from zero.
  """
  use ExUnit.Case, async: true

  alias Capstan.Snapshot.State
  alias Capstan.SnapshotStore
  alias Capstan.SnapshotStore.InMemory

  @uuid "3e11fa47-71ca-11e1-9e33-c80aa9429562"

  defp start_store do
    {:ok, store} = InMemory.start_link([])
    store
  end

  defp sample_state do
    %State{
      status: :snapshotting,
      p0: "#{@uuid}:1-100",
      tables: %{
        {"orders", "orders"} => %{
          fingerprint: "fp-abc",
          pk_columns: ["id"],
          pk_types: [:int],
          pk_cursor: 4096,
          done?: false
        }
      }
    }
  end

  describe "a fresh store" do
    test "read/1 returns {:ok, nil} (never snapshotted)" do
      assert SnapshotStore.read(InMemory, start_store()) == {:ok, nil}
    end

    test "the raw InMemory.read/1 callback returns {:ok, nil}" do
      assert InMemory.read(start_store()) == {:ok, nil}
    end
  end

  describe "persistence round-trip (whole %State{})" do
    test "write/2 then read/1 returns the stored state faithfully, incl. the per-table cursor" do
      store = start_store()
      state = sample_state()

      assert :ok = SnapshotStore.write(InMemory, store, state)
      assert {:ok, ^state} = SnapshotStore.read(InMemory, store)
    end

    test "the pipeline-global p0 and the phase round-trip" do
      store = start_store()
      state = %State{status: :initializing, p0: "#{@uuid}:1-9", tables: %{}}

      :ok = SnapshotStore.write(InMemory, store, state)

      assert {:ok, %State{status: :initializing, p0: "#{@uuid}:1-9"}} =
               SnapshotStore.read(InMemory, store)
    end
  end

  describe "idempotent writes" do
    test "writing the same state twice is indistinguishable from once" do
      store = start_store()
      state = sample_state()

      assert :ok = SnapshotStore.write(InMemory, store, state)
      assert :ok = SnapshotStore.write(InMemory, store, state)
      assert {:ok, ^state} = SnapshotStore.read(InMemory, store)
    end
  end

  # A store whose read/1 or write/2 faults. If SnapshotStore.read/2 collapsed the fault to
  # {:ok, nil}, a re-bootstrap would treat the pipeline as never-snapshotted and re-scan the
  # whole backfill from zero — the exact silent dup the fail-closed passthrough guards.
  defmodule FaultyStore do
    @behaviour Capstan.SnapshotStore
    @impl true
    def read(_store), do: {:error, :snapshot_state_read_failed}
    @impl true
    def write(_store, _state), do: {:error, :snapshot_state_write_failed}
  end

  describe "a store fault fails closed (propagates, never nil)" do
    test "read/2 returns the store's {:error, _} verbatim, not {:ok, nil}" do
      assert SnapshotStore.read(FaultyStore, :ignored) == {:error, :snapshot_state_read_failed}
    end

    test "write/2 propagates the store's {:error, _}" do
      assert SnapshotStore.write(FaultyStore, :ignored, sample_state()) ==
               {:error, :snapshot_state_write_failed}
    end
  end

  describe "Rule 1 (Ch5) — the moduledoc BINDS implementers" do
    test "the behaviour moduledoc forbids logging/telemetering the write/2 args (user data)" do
      {:docs_v1, _, _, _, %{"en" => moduledoc}, _, _} = Code.fetch_docs(Capstan.SnapshotStore)
      # The cursor + fingerprint in write/2's %State{} are USER DATA; the binding must be
      # explicit (Ch5). This documents the contract every production impl must honor.
      assert moduledoc =~ "user data" or moduledoc =~ "USER DATA"
      assert moduledoc =~ ~r/never log|not log|must not log/i
    end
  end
end

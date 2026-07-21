defmodule Capstan.CheckpointStoreTest do
  use ExUnit.Case, async: true

  alias Capstan.CheckpointStore
  alias Capstan.CheckpointStore.InMemory
  alias Capstan.Gtid
  alias Capstan.Position

  @uuid "3e11fa47-71ca-11e1-9e33-c80aa9429562"

  defp start_store do
    {:ok, store} = InMemory.start_link([])
    store
  end

  ## ---------------------------------------------------------------------------
  ## A fresh store reads back nil (design § Data model)
  ## ---------------------------------------------------------------------------

  describe "a fresh store" do
    test "read_position/2 returns {:ok, nil}" do
      store = start_store()
      assert CheckpointStore.read_position(InMemory, store) == {:ok, nil}
    end

    test "the raw read/1 callback returns {:ok, nil}" do
      store = start_store()
      assert InMemory.read(store) == {:ok, nil}
    end
  end

  ## ---------------------------------------------------------------------------
  ## Persists and reads back ONLY gtid_set — file/pos never persist (Q1/Q12)
  ## ---------------------------------------------------------------------------

  describe "persistence boundary" do
    test "write_position/3 persists only the gtid_set string, dropping file/pos" do
      store = start_store()

      position = %Position{gtid_set: "#{@uuid}:1-5", file: "binlog.000042", pos: 9999}
      assert :ok = CheckpointStore.write_position(InMemory, store, position)

      # The durable value is the bare gtid_set string — file/pos are not encoded.
      assert InMemory.read(store) == {:ok, "#{@uuid}:1-5"}
    end

    test "read_position/2 restores a Position with nil file/pos" do
      store = start_store()

      position = %Position{gtid_set: "#{@uuid}:1-5", file: "binlog.000042", pos: 9999}
      :ok = CheckpointStore.write_position(InMemory, store, position)

      assert CheckpointStore.read_position(InMemory, store) ==
               {:ok, %Position{gtid_set: "#{@uuid}:1-5", file: nil, pos: nil}}
    end
  end

  ## ---------------------------------------------------------------------------
  ## Writes are idempotent
  ## ---------------------------------------------------------------------------

  describe "idempotent writes" do
    test "writing the same value twice is indistinguishable from writing it once" do
      store = start_store()
      position = %Position{gtid_set: "#{@uuid}:1-5"}

      assert :ok = CheckpointStore.write_position(InMemory, store, position)
      assert :ok = CheckpointStore.write_position(InMemory, store, position)

      assert CheckpointStore.read_position(InMemory, store) ==
               {:ok, %Position{gtid_set: "#{@uuid}:1-5", file: nil, pos: nil}}
    end
  end

  ## ---------------------------------------------------------------------------
  ## Retry budget — mirrors replicant/lib/replicant/checkpoint_store.ex:319-320
  ## ---------------------------------------------------------------------------

  describe "retry budget" do
    test "default_max_retries/0 is 5" do
      assert CheckpointStore.default_max_retries() == 5
    end

    test "retry_decision/2 is :retry while attempt < max_retries, else :halt" do
      assert CheckpointStore.retry_decision(0, 5) == :retry
      assert CheckpointStore.retry_decision(4, 5) == :retry
      assert CheckpointStore.retry_decision(5, 5) == :halt
      assert CheckpointStore.retry_decision(6, 5) == :halt
    end

    test "max_retries: 0 always halts (halt-now opt-out)" do
      assert CheckpointStore.retry_decision(0, 0) == :halt
    end

    test "permanent_reason?/1 splits :config_invalid (permanent) from transient reasons" do
      assert CheckpointStore.permanent_reason?(:config_invalid) == true
      assert CheckpointStore.permanent_reason?(:checkpoint_store_failed) == false
    end
  end

  ## ---------------------------------------------------------------------------
  ## F10 (mandatory tripwire): the persisted set stays a COMPACT single interval
  ## across a long run of sequential GTIDs. The only end-to-end check that
  ## Gtid.union/2 coalesces adjacent ranges through the persist boundary — a union
  ## that failed to coalesce would render "uuid:1:2:3:...:100" and fail this.
  ## ---------------------------------------------------------------------------

  describe "F10 checkpoint compactness" do
    test "100 sequential single-GTID advances read back as one interval uuid:1-100" do
      store = start_store()

      # Model the pipeline: read the current checkpoint, union in the next GTID,
      # write it back — one committed GTID at a time, through the store on every step.
      for gno <- 1..100 do
        base =
          case CheckpointStore.read_position(InMemory, store) do
            {:ok, nil} -> Gtid.parse("")
            {:ok, %Position{gtid_set: set}} -> Gtid.parse(set)
          end

        advanced = Gtid.union(base, Gtid.parse("#{@uuid}:#{gno}"))

        :ok =
          CheckpointStore.write_position(InMemory, store, %Position{
            gtid_set: Gtid.render(advanced)
          })
      end

      {:ok, %Position{gtid_set: final}} = CheckpointStore.read_position(InMemory, store)

      # Rendered as a single compact interval, not a fragment list.
      assert final == "#{@uuid}:1-100"

      # Non-vacuity: exactly one interval for the source (100 intervals if union
      # failed to coalesce adjacent single GNOs).
      assert Gtid.sources(Gtid.parse(final)) == [{@uuid, [{1, 100}]}]
    end
  end

  ## ---------------------------------------------------------------------------
  ## A store read fault MUST propagate — never masquerade as "never checkpointed"
  ## ---------------------------------------------------------------------------

  # A store whose read/1 faults. If read_position/2 collapsed this to {:ok, nil},
  # the pipeline would resume from the EMPTY set and silently re-deliver the whole
  # history effect-once — the exact silent failure the fail-closed passthrough guards.
  defmodule FaultyStore do
    @behaviour Capstan.CheckpointStore
    @impl true
    def read(_store), do: {:error, :store_unavailable}
    @impl true
    def write(_store, _gtid_set), do: {:error, :store_unavailable}
  end

  describe "a store read fault fails closed (propagates, never nil)" do
    test "read_position/2 returns the store's {:error, _} verbatim, not {:ok, nil}" do
      assert CheckpointStore.read_position(FaultyStore, :ignored) ==
               {:error, :store_unavailable}
    end

    test "write_position/3 propagates the store's {:error, _}" do
      position = %Position{gtid_set: "#{@uuid}:1"}

      assert CheckpointStore.write_position(FaultyStore, :ignored, position) ==
               {:error, :store_unavailable}
    end
  end
end

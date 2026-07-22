defmodule Capstan.SnapshotStore do
  @moduledoc """
  The durable snapshot-progress persistence contract (C2), the sibling of
  `Capstan.CheckpointStore`.

  A snapshot store persists **exactly one value** per pipeline identity: the whole
  `Capstan.Snapshot.State` (Pinned decision #3) — the phase, the pipeline-global floor `p0`,
  and the per-`{schema, table}` progress (fingerprint, PK shape, cursor, done flag). This
  mirrors `CheckpointStore`'s one-durable-value model, so a caller advancing the backfill
  never hand-rolls the persistence and the two stores stay structurally parallel.

  The snapshot state is **orthogonal auxiliary state**, never a replication position: it
  carries no GTID that could become a `Capstan.Gtid.member?/2` dedup floor (ADR-0001). The
  processed-GTID checkpoint stays the sole authoritative replication position; this store only
  records "how far the backfill has read".

  ## Behaviour

    * `c:read/1` — `{:ok, %Capstan.Snapshot.State{} | nil}` (`nil` = never written) or a
      value-free `{:error, term()}`.
    * `c:write/2` — durably store the `%State{}` (idempotent: writing the same value twice is
      indistinguishable from writing it once) or `{:error, term()}`.

  A read/write fault **propagates** through `read/2`/`write/3` — it is never collapsed to
  `{:ok, nil}`. A store that faulted must fail closed: masquerading as "never snapshotted"
  would make a re-bootstrap re-scan the whole backfill from zero (a silent duplicate).
  Callers budget the fault with the SHARED `Capstan.CheckpointStore.retry_decision/2` +
  `permanent_reason?/1` (this store does NOT re-derive the counter), then halt fail-closed
  (`:snapshot_state_read_failed` / `:snapshot_state_write_failed`).

  ## Rule 1 (Ch5) — binding on every implementation

  The `%State{}` passed to `write/2` holds **USER DATA**: each table's `pk_cursor` is a row
  value and its `fingerprint` is a column-derived hash. An implementation must **never log**,
  telemeter, or otherwise emit the `write/2` argument (nor the value returned by `read/1`).
  Persist it to the durable store and nowhere else. `%Capstan.Snapshot.State`'s derived
  `Inspect` elides the per-table map, but a store that stringifies or logs the struct through
  another path would defeat that — so this binding is on the STORE, not only the struct.

  `Capstan.SnapshotStore.InMemory` is a process-lifetime reference implementation for tests
  and ephemeral pipelines — it is **NOT** durable across a restart, so it must never be used
  as a production snapshot store.
  """

  alias Capstan.Snapshot.State

  @typedoc "A per-pipeline store handle (e.g. the pid/name of a store process)."
  @type store :: term()

  @doc """
  Read the durable snapshot state for this pipeline: `{:ok, %State{} | nil}` (`nil` = never
  written) or a value-free error.
  """
  @callback read(store()) :: {:ok, State.t() | nil} | {:error, term()}

  @doc """
  Durably persist the whole `%State{}` for this pipeline. Idempotent — re-writing the same
  value is a no-op-equivalent. Returns `:ok` or a value-free error. Rule 1: never log or
  telemeter the argument.
  """
  @callback write(store(), State.t()) :: :ok | {:error, term()}

  @doc """
  Read the snapshot state through `impl`, propagating a fault fail-closed.

  Never collapses `{:error, _}` to `{:ok, nil}` — a faulted store must not read as
  "never snapshotted". `impl` is the callback module; `store` its handle.
  """
  @spec read(module(), store()) :: {:ok, State.t() | nil} | {:error, term()}
  def read(impl, store) when is_atom(impl) do
    case impl.read(store) do
      {:ok, nil} -> {:ok, nil}
      {:ok, %State{} = state} -> {:ok, state}
      {:error, _} = error -> error
    end
  end

  @doc """
  Persist the snapshot state through `impl`. `impl` is the callback module; `store` its handle.
  """
  @spec write(module(), store(), State.t()) :: :ok | {:error, term()}
  def write(impl, store, %State{} = state) when is_atom(impl) do
    impl.write(store, state)
  end

  defmodule InMemory do
    @moduledoc """
    A process-lifetime `Capstan.SnapshotStore` reference implementation.

    Backed by an `Agent` holding one pipeline's `%Capstan.Snapshot.State{}` (`nil` until first
    written). Durable only for the life of the process — a restart loses the snapshot progress
    — so it is intended for tests and ephemeral pipelines, **never** a production durable
    store. Rule 1 (Ch5): it holds the state in memory and never logs it.
    """
    @behaviour Capstan.SnapshotStore

    @doc "Start a fresh store (the snapshot state begins `nil`)."
    @spec start_link(keyword()) :: Agent.on_start()
    def start_link(opts \\ []) do
      Agent.start_link(fn -> nil end, opts)
    end

    @impl Capstan.SnapshotStore
    @spec read(Agent.agent()) :: {:ok, Capstan.Snapshot.State.t() | nil}
    def read(store) do
      {:ok, Agent.get(store, & &1)}
    end

    @impl Capstan.SnapshotStore
    @spec write(Agent.agent(), Capstan.Snapshot.State.t()) :: :ok
    def write(store, %Capstan.Snapshot.State{} = state) do
      Agent.update(store, fn _current -> state end)
    end
  end
end

defmodule Capstan.CheckpointStore do
  @moduledoc """
  The lib-owned checkpoint persistence contract (design § Data model, line 198).

  A checkpoint store persists **exactly one value** per pipeline identity: the
  processed `gtid_set` STRING. `file`/`pos` are never persisted — they are diagnostic
  only and are derived on read (`Capstan.Position.from_persisted/1` restores them as
  `nil`). Because the callback surface only ever sees the persisted string, an
  implementation *cannot* accidentally persist `file`/`pos`; there is exactly one
  durable value and no two-representation divergence to occur on.

  The checkpoint is a **processed** watermark (design Q14): it records every committed
  GTID the pipeline has processed, delivered or filtered — see `Capstan.Sink`.

  ## Behaviour

  An implementation persists and reads back the `gtid_set` string for one pipeline:

    * `c:read/1` — `{:ok, gtid_set | nil}` (`nil` = never written) or a value-free
      `{:error, term()}`.
    * `c:write/2` — durably store the `gtid_set` string (idempotent: writing the same
      value twice is indistinguishable from writing it once) or `{:error, term()}`.

  `Capstan.CheckpointStore.InMemory` is a process-lifetime reference implementation
  for tests and ephemeral pipelines — it is NOT durable across a restart.

  ## Position boundary

  `read_position/2` and `write_position/3` bridge `Capstan.Position` and the persisted
  string through `Capstan.Position.to_persisted/1` and `from_persisted/1`. They are the
  one place the persist boundary is applied, so a caller advancing the checkpoint never
  hand-rolls it and can never leak `file`/`pos`.

  ## Retry budget

  `default_max_retries/0`, `retry_decision/2`, and `permanent_reason?/1` mirror
  `replicant/lib/replicant/checkpoint_store.ex` so a store fault is retried a bounded
  number of times and then halts fail-closed, with the same counter semantics the
  connect-read and mid-stream write sites share (they cannot drift). A *permanent*
  reason halts immediately without spending the budget.
  """

  alias Capstan.Position

  @typedoc "A per-pipeline store handle (e.g. the pid/name of a store process)."
  @type store :: term()

  @doc """
  Read the durable checkpoint for this pipeline: `{:ok, gtid_set | nil}` (`nil` =
  never written) or a value-free error.
  """
  @callback read(store()) :: {:ok, String.t() | nil} | {:error, term()}

  @doc """
  Durably persist `gtid_set` for this pipeline. Idempotent — re-writing the same value
  is a no-op-equivalent. Returns `:ok` or a value-free error.
  """
  @callback write(store(), gtid_set :: String.t()) :: :ok | {:error, term()}

  @default_max_retries 5

  @doc "Default `max_retries` for a checkpoint-store fault when none is configured."
  @spec default_max_retries() :: non_neg_integer()
  def default_max_retries, do: @default_max_retries

  @doc """
  The shared retry decision: `:retry` while `attempt < max_retries`, else `:halt`.

  `attempt` is the count of retries ALREADY made (`0` on the first fault). `max_retries:
  0` ⇒ always `:halt` (halt-now opt-out). Mirrors
  `replicant/lib/replicant/checkpoint_store.ex:319-320`.
  """
  @spec retry_decision(non_neg_integer(), non_neg_integer()) :: :retry | :halt
  def retry_decision(attempt, max_retries) when attempt < max_retries, do: :retry
  def retry_decision(_attempt, _max_retries), do: :halt

  @doc """
  True when a store-fault reason is PERMANENT (retrying cannot fix it) and must halt
  immediately without spending the retry budget.

  `:config_invalid` — a mis-shaped store configuration — is permanent. Every other
  reason is transient at the value-free boundary (a momentary blip and a wrong-host
  misconfig are indistinguishable here), so it is retried, then halted.
  """
  @spec permanent_reason?(atom()) :: boolean()
  def permanent_reason?(:config_invalid), do: true
  def permanent_reason?(_transient), do: false

  @doc """
  Read the checkpoint as a `Capstan.Position` (or `nil`), applying the persist boundary.

  Restores `file`/`pos` as `nil` via `Capstan.Position.from_persisted/1`. `impl` is the
  callback module; `store` is its handle.
  """
  @spec read_position(module(), store()) :: {:ok, Position.t() | nil} | {:error, term()}
  def read_position(impl, store) when is_atom(impl) do
    case impl.read(store) do
      {:ok, nil} -> {:ok, nil}
      {:ok, gtid_set} when is_binary(gtid_set) -> {:ok, Position.from_persisted(gtid_set)}
      {:error, _} = error -> error
    end
  end

  @doc """
  Persist `position`'s `gtid_set` alone, applying the persist boundary.

  `file`/`pos` are dropped by `Capstan.Position.to_persisted/1` before the write, so
  they never reach the store. `impl` is the callback module; `store` is its handle.
  """
  @spec write_position(module(), store(), Position.t()) :: :ok | {:error, term()}
  def write_position(impl, store, %Position{} = position) when is_atom(impl) do
    impl.write(store, Position.to_persisted(position))
  end

  defmodule InMemory do
    @moduledoc """
    A process-lifetime `Capstan.CheckpointStore` reference implementation.

    Backed by an `Agent` holding one pipeline's `gtid_set` string (`nil` until first
    written). It is durable only for the life of the process — a restart loses the
    checkpoint — so it is intended for tests and ephemeral pipelines, **never** as a
    production durable store.
    """
    @behaviour Capstan.CheckpointStore

    @doc "Start a fresh store (the checkpoint begins `nil`)."
    @spec start_link(keyword()) :: Agent.on_start()
    def start_link(opts \\ []) do
      Agent.start_link(fn -> nil end, opts)
    end

    @impl Capstan.CheckpointStore
    @spec read(Agent.agent()) :: {:ok, String.t() | nil}
    def read(store) do
      {:ok, Agent.get(store, & &1)}
    end

    @impl Capstan.CheckpointStore
    @spec write(Agent.agent(), String.t()) :: :ok
    def write(store, gtid_set) when is_binary(gtid_set) do
      Agent.update(store, fn _current -> gtid_set end)
    end
  end
end

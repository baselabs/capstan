defmodule Capstan.Pipeline do
  @moduledoc """
  Pipeline entry — the per-mode `Sink` validation and the child-spec wiring for one
  supervised pipeline (a `Capstan.CheckpointStore` process, a `Capstan.AssemblerServer`,
  and a `Capstan.Connection`).

  `Capstan.Supervisor` starts these children in order and threads the pids, because the
  `Connection`'s `:receiver` must be the `AssemblerServer`'s PID (design Q7). The
  `Connection` forwards frames with a plain `send/2`, so the receiver is a **PID**: a
  send to a terminated `AssemblerServer` is a silent no-op, never a raise that could
  restart a fail-closed pipeline into a livelock.

  ## Per-mode `Sink` callback required-ness (routed from the Task 13 review)

  `Capstan.Sink` declares every callback `@optional_callbacks`; which are REQUIRED
  depends on the checkpoint mode, enforced here at start-up via `function_exported?/3` so
  a sink missing a required callback is refused **before** a first-call crash:

    * **lib-owned** (`:checkpoint_store` configured): `handle_transaction/1`.
    * **sink-owned** (no `:checkpoint_store`): `checkpoint/0` and `handle_transaction/1`.
    * `handle_schema_change/2` whenever DDL delivery is enabled — in C1 the pipeline
      ALWAYS delivers self-committing DDL (design Q13), so it is required in both modes.

  Note: C1's `AssemblerServer` implements only lib-owned checkpoint mode. Sink-owned
  required-ness is still validated here (the routed-from-Task-13 contract) so a bad
  sink-owned sink is refused; `Capstan.start_link/1` refuses to *run* a sink-owned
  pipeline in C1 (`:sink_owned_mode_unsupported`).
  """

  alias Capstan.AssemblerServer
  alias Capstan.Connection
  alias Capstan.Position

  @typedoc "A value-free sink-validation refusal."
  @type sink_error ::
          :invalid_sink
          | :sink_missing_handle_transaction
          | :sink_missing_checkpoint
          | :sink_missing_handle_schema_change

  @doc """
  Validate the configured `:sink` against its checkpoint mode's required callbacks.

  Returns `:ok`, or a distinct value-free refusal naming the missing callback. Every
  check is a runtime `function_exported?/3` (the callbacks are `@optional_callbacks`).
  """
  @spec validate_sink(keyword()) :: :ok | {:error, sink_error()}
  def validate_sink(opts) when is_list(opts) do
    sink = Keyword.get(opts, :sink)

    cond do
      not loaded_module?(sink) ->
        {:error, :invalid_sink}

      not function_exported?(sink, :handle_transaction, 1) ->
        {:error, :sink_missing_handle_transaction}

      not lib_mode?(opts) and not function_exported?(sink, :checkpoint, 0) ->
        {:error, :sink_missing_checkpoint}

      # DDL delivery is always enabled in C1 (the pipeline always delivers self-committing
      # DDL, design Q13), so handle_schema_change/2 is unconditionally required; a future
      # DDL opt-out would gate this clause.
      not function_exported?(sink, :handle_schema_change, 2) ->
        {:error, :sink_missing_handle_schema_change}

      true ->
        :ok
    end
  end

  @doc """
  Is this a lib-owned pipeline? True iff a non-nil `:checkpoint_store` is configured.
  """
  @spec lib_mode?(keyword()) :: boolean()
  def lib_mode?(opts) when is_list(opts) do
    Keyword.has_key?(opts, :checkpoint_store) and Keyword.get(opts, :checkpoint_store) != nil
  end

  @doc """
  The lib-owned checkpoint store's implementation module and its `start_link/1` options,
  drawn from `checkpoint_store: [module: impl, options: keyword()]`.
  """
  @spec checkpoint_store(keyword()) :: {module(), keyword()}
  def checkpoint_store(opts) when is_list(opts) do
    config = Keyword.fetch!(opts, :checkpoint_store)
    {Keyword.fetch!(config, :module), Keyword.get(config, :options, [])}
  end

  @doc """
  Resolve the public `:start_position` (default `:checkpoint`) against the position the
  checkpoint store resumed with.

    * `:checkpoint` — the resumed position (a `%Position{}` or `nil` for a fresh start);
    * a `%Capstan.Position{}` — `{:error, :start_position_override_unsupported}` in C1
      (an explicit override would resume the `Connection`'s dump but NOT the
      `AssemblerServer`'s watermark, which seeds from the store alone — a silent hole; so
      it is refused fail-closed, matching `Capstan.start_link/1`'s pre-flight check);
    * `:current` — `{:error, :start_position_current_unsupported}` (C1 does not implement
      "start from the server's current position"; it needs a live pre-connect query the
      spine does not yet wire);
    * anything else — `{:error, :config_invalid}`.
  """
  @spec resolve_start_position(keyword(), Position.t() | nil) ::
          {:ok, Position.t() | nil}
          | {:error,
             :start_position_override_unsupported
             | :start_position_current_unsupported
             | :config_invalid}
  def resolve_start_position(opts, resumed) when is_list(opts) do
    case Keyword.get(opts, :start_position, :checkpoint) do
      :checkpoint -> {:ok, resumed}
      %Position{} -> {:error, :start_position_override_unsupported}
      :current -> {:error, :start_position_current_unsupported}
      _other -> {:error, :config_invalid}
    end
  end

  @doc "The checkpoint-store child spec (a `:temporary` child — a store fault halts, never restarts)."
  @spec store_spec(module(), keyword()) :: Supervisor.child_spec()
  def store_spec(impl, store_options) when is_atom(impl) and is_list(store_options) do
    %{id: :store, start: {impl, :start_link, [store_options]}, restart: :temporary}
  end

  @doc "The `AssemblerServer` child spec, wired to the started checkpoint store `{impl, handle}`."
  @spec assembler_spec(keyword(), AssemblerServer.checkpoint_store()) :: Supervisor.child_spec()
  def assembler_spec(opts, checkpoint_store) when is_list(opts) do
    server_opts = [
      sink: Keyword.fetch!(opts, :sink),
      checkpoint_store: checkpoint_store,
      tables: Keyword.get(opts, :tables, :all)
    ]

    %{id: :assembler, start: {AssemblerServer, :start_link, [server_opts]}, restart: :temporary}
  end

  @doc """
  The `Connection` child spec, wired so its `:receiver` is the `AssemblerServer` PID, its
  dump resumes from `start_position`, and — given the started checkpoint store
  `{impl, handle}` — it re-reads the durable resume position on every establish (so a
  reconnect resumes from the current watermark, not the frozen start-up position; design
  Q7 / F1). `checkpoint_store` defaults to `nil` so a `Connection` wired without a store
  (a unit test) keeps the injected `start_position`.
  """
  @spec connection_spec(
          keyword(),
          pid(),
          Position.t() | nil,
          AssemblerServer.checkpoint_store() | nil
        ) ::
          Supervisor.child_spec()
  def connection_spec(opts, receiver, start_position, checkpoint_store \\ nil)
      when is_list(opts) and is_pid(receiver) do
    connection_opts =
      [
        server_id: Keyword.fetch!(opts, :server_id),
        connection: Keyword.get(opts, :connection, []),
        receiver: receiver,
        start_position: start_position,
        checkpoint_store: checkpoint_store,
        max_command_retries: Keyword.get(opts, :max_command_retries, 5)
      ] ++ Keyword.take(opts, [:connect_fun, :reconnect_backoff])

    %{id: :connection, start: {Connection, :start_link, [connection_opts]}, restart: :temporary}
  end

  ## ---------------------------------------------------------------------------
  ## helpers
  ## ---------------------------------------------------------------------------

  defp loaded_module?(sink), do: is_atom(sink) and sink != nil and Code.ensure_loaded?(sink)
end

defmodule Capstan.Pipeline do
  @moduledoc """
  Pipeline entry — the per-mode `Sink` validation and the child-spec wiring for one
  supervised pipeline (a `Capstan.CheckpointStore` process, a `Capstan.AssemblerServer`,
  and a `Capstan.Connection`).

  `Capstan.Supervisor` starts these children in order and threads the pids, because the
  `Connection`'s `:receiver` must be the `AssemblerServer`'s PID. The
  `Connection` forwards frames with a plain `send/2`, so the receiver is a **PID**: a
  send to a terminated `AssemblerServer` is a silent no-op, never a raise that could
  restart a fail-closed pipeline into a livelock. The `Connection` also **monitors** that
  PID, so the `AssemblerServer`'s fail-closed halt (which stops it without messaging back)
  is not silent — the `Connection` stops fail-closed on the `:DOWN` instead of streaming
  into the dead pid. Monitoring detects the death without linking, so neither `:temporary`
  child restarts into the livelock the plain-`send` avoids.

  ## Per-mode `Sink` callback required-ness

  `Capstan.Sink` declares every callback `@optional_callbacks`; which are REQUIRED
  depends on the checkpoint mode, enforced here at start-up via `function_exported?/3` so
  a sink missing a required callback is refused **before** a first-call crash:

    * **lib-owned** (`:checkpoint_store` configured): `handle_transaction/1`.
    * **sink-owned** (no `:checkpoint_store`): `checkpoint/0` and `handle_transaction/1`.
    * `handle_schema_change/2` whenever DDL delivery is enabled — in C1 the pipeline
      ALWAYS delivers self-committing DDL (ADR-0003), so it is required in both modes.

  Note: C1's `AssemblerServer` implements only lib-owned checkpoint mode. Sink-owned
  required-ness is still validated here so a bad
  sink-owned sink is refused; `Capstan.start_link/1` refuses to *run* a sink-owned
  pipeline in C1 (`:sink_owned_mode_unsupported`).
  """

  alias Capstan.AssemblerServer
  alias Capstan.Config
  alias Capstan.Connection
  alias Capstan.Position
  alias Capstan.Snapshot.Coordinator

  @typedoc "A value-free sink-validation refusal."
  @type sink_error ::
          :invalid_sink
          | :sink_missing_handle_transaction
          | :sink_missing_checkpoint
          | :sink_missing_handle_schema_change
          | :sink_missing_handle_snapshot

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

      # C2: in SNAPSHOT mode the sink must implement handle_snapshot/2 (upsert-by-PK backfill
      # delivery). Gated on snapshot_mode? so an absent :snapshot block leaves this byte-for-byte
      # the C1 required-set — a C1 sink is never forced to stub a callback its mode never calls.
      snapshot_mode?(opts) and not function_exported?(sink, :handle_snapshot, 2) ->
        {:error, :sink_missing_handle_snapshot}

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
  Is this an initial-snapshot pipeline? True iff a non-nil `:snapshot` block is configured
  (mirrors `lib_mode?/1`). An absent (or `nil`) `:snapshot` key ⇒ pure C1.
  """
  @spec snapshot_mode?(keyword()) :: boolean()
  def snapshot_mode?(opts) when is_list(opts) do
    Keyword.has_key?(opts, :snapshot) and Keyword.get(opts, :snapshot) != nil
  end

  @doc """
  Refuse a snapshot table outside the capture allowlist (`:snapshot_table_not_captured`).

  Every snapshot table MUST be captured by the stream: the stream is what delivers the
  `gtid > G` changes the cursor-gate suppresses from the chunk, so a snapshot table the stream
  does not carry would silently gap. `:all` capture trivially includes every snapshot table.
  `snapshot` is the normalised `Capstan.Config.snapshot_config()` (or `nil` in pure C1 — a
  no-op `:ok`).
  """
  @spec validate_snapshot_tables(keyword(), Config.snapshot_config() | nil) ::
          :ok | {:error, :snapshot_table_not_captured}
  def validate_snapshot_tables(_opts, nil), do: :ok

  def validate_snapshot_tables(opts, %{tables: snapshot_tables}) when is_list(opts) do
    subset?(snapshot_tables, Keyword.get(opts, :tables, :all))
  end

  # An `:all` capture is a superset of every snapshot table. A concrete capture list demands a
  # concrete snapshot list that is a subset; an `:all` snapshot set against a concrete capture
  # cannot be proven captured, so it fails closed.
  defp subset?(_snapshot_tables, :all), do: :ok
  defp subset?(:all, _captured_list), do: {:error, :snapshot_table_not_captured}

  defp subset?(snapshot_tables, captured)
       when is_list(snapshot_tables) and is_list(captured) do
    if MapSet.subset?(MapSet.new(snapshot_tables), MapSet.new(captured)),
      do: :ok,
      else: {:error, :snapshot_table_not_captured}
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

  @doc """
  The durable `Capstan.SnapshotStore` child spec (snapshot mode only), mirroring `store_spec/2`.

  A `:temporary` child — a snapshot-store fault halts fail-closed, never restarts into a
  re-scan-from-zero livelock.
  """
  @spec snapshot_store_spec(module(), keyword()) :: Supervisor.child_spec()
  def snapshot_store_spec(impl, store_options) when is_atom(impl) and is_list(store_options) do
    %{id: :snapshot_store, start: {impl, :start_link, [store_options]}, restart: :temporary}
  end

  @typedoc """
  The bootstrap-provided coordinator wiring (Task 10's supervisor supplies these AFTER the
  assembler + snapshot-store children are up): the started `AssemblerServer` pid the coordinator
  sends `{:capstan_halt, _}` to and observes the watermark from, the started `{impl, handle}`
  snapshot store, the initial `%Capstan.Snapshot.State{}`, the per-table opened `ChunkReader`
  handles, and the initial processed-watermark string (default `""`).
  """
  @type coordinator_wiring :: [
          assembler: pid() | atom(),
          snapshot_store: {module(), term()},
          snapshot_state: Capstan.Snapshot.State.t(),
          readers: %{optional({String.t(), String.t()}) => term()},
          processed_set: String.t()
        ]

  @doc """
  The `Capstan.Snapshot.Coordinator` child spec (snapshot mode only), mirroring `assembler_spec/2`.

  The real (downstream) sink + the retry budget come from the pipeline `opts`; `wiring` carries
  the bootstrap-produced pieces (see `t:coordinator_wiring/0`) — Task 10's supervisor builds it
  once the assembler + snapshot-store children have started. A `:temporary` child: a fail-closed
  snapshot halt never restarts.
  """
  @spec coordinator_spec(keyword(), coordinator_wiring()) :: Supervisor.child_spec()
  def coordinator_spec(opts, wiring) when is_list(opts) and is_list(wiring) do
    coordinator_opts = [
      sink: Keyword.fetch!(opts, :sink),
      assembler: Keyword.fetch!(wiring, :assembler),
      snapshot_store: Keyword.fetch!(wiring, :snapshot_store),
      snapshot_state: Keyword.fetch!(wiring, :snapshot_state),
      readers: Keyword.fetch!(wiring, :readers),
      processed_set: Keyword.get(wiring, :processed_set, ""),
      max_retries: Keyword.get(opts, :max_command_retries, 5)
    ]

    %{
      id: :snapshot_coordinator,
      start: {Coordinator, :start_link, [coordinator_opts]},
      restart: :temporary
    }
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
      ] ++
        Keyword.take(opts, [
          :connect_fun,
          :reconnect_backoff,
          :heartbeat_period_ms,
          :stream_timeout_ms
        ])

    %{id: :connection, start: {Connection, :start_link, [connection_opts]}, restart: :temporary}
  end

  ## ---------------------------------------------------------------------------
  ## helpers
  ## ---------------------------------------------------------------------------

  defp loaded_module?(sink), do: is_atom(sink) and sink != nil and Code.ensure_loaded?(sink)
end

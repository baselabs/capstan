defmodule Capstan do
  @moduledoc """
  Framework-agnostic Elixir CDC consumer for MySQL row-based binary-log replication,
  delivering committed transactions to a pluggable **sink** with fail-closed,
  transaction-granularity delivery: the checkpoint advances only after the sink has
  durably applied a transaction (ADR-0003). In C1's lib-owned checkpoint mode this
  is **at-least-once** — a crash between the sink write and the checkpoint re-delivers the
  transaction on restart (a bounded duplicate window). Effect-once delivery is the
  sink-owned atomic path (the sink persisting its write and the position together), which
  C1 validates but does not yet run — see `Capstan.Sink` and ADR-0004.

  ## Starting a pipeline

  Configure each pipeline explicitly at `start_link/1` (no global mutable state):

      Capstan.start_link(
        connection: [
          host: "replica.internal", port: 3306, username: "capstan",
          password: "…", database: "orders",
          ssl: true, ssl_opts: [cacertfile: "/etc/ssl/mysql-ca.pem"]  # TLS on by default (ADR-0002)
        ],
        server_id: 1001,                       # replica identity; MUST be unique in the topology
        sink: MyApp.OrdersSink,                # implements Capstan.Sink
        checkpoint_store: [module: MyApp.Store],   # lib-owned mode; see below
        start_position: :checkpoint,           # C1: :checkpoint (default) only — see below
        max_command_retries: 5,
        heartbeat_period_ms: 15_000,           # liveness: the window MUST exceed the heartbeat
        stream_timeout_ms: 60_000              # (defaults shown; see "Streaming liveness")
      )

  `start_link/1` validates the options through `Capstan.Config`, enforces the per-mode
  `Capstan.Sink` callback required-ness (`Capstan.Pipeline`), and — for a valid
  configuration — starts a supervised `Capstan.Supervisor` wiring a checkpoint store, a
  `Capstan.AssemblerServer`, and a `Capstan.Connection`. A bad substrate is refused
  **before** any socket is opened (a bad config fails start-up, not first delivery).

  ## Checkpoint mode

  C1 implements **lib-owned** checkpoint mode. Pass
  `checkpoint_store: [module: impl, options: keyword()]`, where `impl` is a
  `Capstan.CheckpointStore` implementation exporting `start_link/1` (e.g.
  `Capstan.CheckpointStore.InMemory` for tests and ephemeral pipelines). The store is
  supervised as part of the pipeline. **Sink-owned** checkpoint mode (the sink persisting
  the position atomically with its own write) is validated but not yet run by C1's
  `AssemblerServer`; a sink-owned configuration is refused with
  `:sink_owned_mode_unsupported`.

  ## Start position

  C1 resumes **only from the durable checkpoint** (`start_position: :checkpoint`, the
  default). A populated store resumes from the persisted watermark. A fresh
  (never-written) store sends the dump an **empty GTID set**, which requests the
  server's full retained history — and a server that has already purged its earliest
  logs refuses that dump, halting `:data_gap`. To start "from now" today, seed the
  checkpoint store with the server's current `@@global.gtid_executed` before first
  start (see usage-rules.md). An explicit `%Capstan.Position{}`
  override and `:current` are **refused fail-closed** (`:start_position_override_unsupported`
  / `:start_position_current_unsupported`) — honoring an explicit resume position
  end-to-end (the `AssemblerServer` seeds its watermark from the store alone today) is a
  C1-completeness follow-up. Refusing avoids a silent checkpoint hole.

  ## Return values

  `{:ok, supervisor_pid}` on success, or `{:error, reason}` — a value-free atom from
  `Capstan.Config` (`:server_id_required`, `:config_invalid`,
  `:tls_verification_unspecified`, `:invalid_liveness_config`, `:unknown_option`),
  from `Capstan.Pipeline` (`:invalid_sink`,
  `:sink_missing_handle_transaction`, `:sink_missing_checkpoint`,
  `:sink_missing_handle_schema_change`, `:sink_missing_handle_snapshot`,
  `:snapshot_table_not_captured`), or from this module
  (`:sink_owned_mode_unsupported`, `:checkpoint_store_required`,
  `:start_position_override_unsupported`, `:start_position_current_unsupported`).

  ## Initial snapshot (C2)

  An optional `snapshot: [tables: […], store: [module: …], chunk_size: 4096]` block enables a
  consistent backfill of pre-existing rows woven into the running stream. It is **additive**:
  with no `:snapshot` key the pipeline is pure C1, byte-for-byte unchanged. In snapshot mode the
  sink must implement `c:Capstan.Sink.handle_snapshot/2` (else `:sink_missing_handle_snapshot`),
  the snapshot tables must be a subset of the capture allowlist (else
  `:snapshot_table_not_captured`), and a missing/mis-shaped `store` is refused `:config_invalid`.
  `:current` / `%Capstan.Position{}` start positions stay refused fail-closed (no C1b).
  """

  alias Capstan.Config
  alias Capstan.Pipeline
  alias Capstan.Position

  @doc """
  Validate `opts` and start a supervised pipeline. See the moduledoc for the option shape
  and the value-free `{:error, reason}` set. Returns `{:ok, supervisor_pid}` on success.
  """
  @spec start_link(keyword()) :: {:ok, pid()} | {:error, term()}
  def start_link(opts) when is_list(opts) do
    with {:ok, config} <- Config.validate(opts),
         :ok <- Pipeline.validate_sink(opts),
         :ok <- require_lib_mode(opts),
         :ok <- validate_checkpoint_store(opts),
         :ok <- validate_start_position(opts),
         {:ok, snapshot} <- Config.validate_snapshot(opts),
         :ok <- Pipeline.validate_snapshot_tables(opts, snapshot) do
      Capstan.Supervisor.start_link(wiring(config, opts, snapshot))
    end
  end

  def start_link(_opts), do: {:error, :config_invalid}

  @doc """
  A supervisor child spec, so a pipeline can be embedded in an application's supervision
  tree: `{Capstan, connection: [...], server_id: ..., sink: ..., checkpoint_store: [...]}`.
  """
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{
      id: Keyword.get(opts, :id, __MODULE__),
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor
    }
  end

  @doc "Stop a running pipeline supervisor started by `start_link/1`."
  @spec stop(pid()) :: :ok
  def stop(supervisor) when is_pid(supervisor), do: Supervisor.stop(supervisor)

  # C1's AssemblerServer runs lib-owned checkpoint mode only; a valid sink-owned
  # configuration is refused honestly rather than crashing at AssemblerServer init.
  defp require_lib_mode(opts) do
    if Pipeline.lib_mode?(opts), do: :ok, else: {:error, :sink_owned_mode_unsupported}
  end

  # Fail closed on a malformed lib-owned checkpoint store BEFORE any child starts, so a
  # missing `:module` surfaces as a clean `{:error, :checkpoint_store_required}` from
  # `start_link/1` rather than a `KeyError` crash deep in `AssemblerServer` init. An
  # unrecognized block key (a typo) refuses `:unknown_option` — same posture as
  # `Config.validate/1`.
  defp validate_checkpoint_store(opts) do
    case Keyword.get(opts, :checkpoint_store) do
      config when is_list(config) ->
        cond do
          not Keyword.keyword?(config) ->
            {:error, :checkpoint_store_required}

          Enum.any?(Keyword.keys(config), &(&1 not in [:module, :options])) ->
            {:error, :unknown_option}

          not (is_atom(Keyword.get(config, :module)) and Keyword.get(config, :module) != nil) ->
            {:error, :checkpoint_store_required}

          true ->
            :ok
        end

      _ ->
        {:error, :checkpoint_store_required}
    end
  end

  # C1 resumes ONLY from the durable checkpoint (`:start_position` default `:checkpoint`).
  # An explicit `%Position{}` override and `:current` are refused fail-closed: the spine
  # threads a resume position to the `Connection`'s dump but the `AssemblerServer` seeds
  # its watermark from the checkpoint store alone, so honoring an override end-to-end is a
  # C1-completeness follow-up. Refusing here converts what would be a silent checkpoint
  # hole (dump resumes from the override, the watermark from empty) into a clean refusal.
  defp validate_start_position(opts) do
    case Keyword.get(opts, :start_position, :checkpoint) do
      :checkpoint -> :ok
      %Position{} -> {:error, :start_position_override_unsupported}
      :current -> {:error, :start_position_current_unsupported}
      _other -> {:error, :config_invalid}
    end
  end

  # Threads the validated wiring to the supervisor. When `snapshot` is `nil` (pure C1) the
  # keyword is IDENTICAL to C1's — byte-for-byte unchanged; the `:snapshot` key is appended
  # only in snapshot mode, so the C1 supervisor sees exactly what it always saw.
  defp wiring(config, opts, snapshot) do
    base = [
      sink: Keyword.fetch!(opts, :sink),
      checkpoint_store: Keyword.fetch!(opts, :checkpoint_store),
      connection: config.connection,
      server_id: config.server_id,
      max_command_retries: config.max_command_retries,
      reconnect_backoff: config.reconnect_backoff,
      heartbeat_period_ms: config.heartbeat_period_ms,
      stream_timeout_ms: config.stream_timeout_ms,
      start_position: Keyword.get(opts, :start_position, :checkpoint),
      tables: Keyword.get(opts, :tables, :all)
    ]

    case snapshot do
      nil -> base
      %{} = snapshot_config -> base ++ [snapshot: snapshot_config]
    end
  end
end

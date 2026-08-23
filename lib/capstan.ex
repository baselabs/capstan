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

  **Lib-owned** (default): pass `checkpoint_store: [module: impl, options: keyword()]`,
  where `impl` is a `Capstan.CheckpointStore` implementation exporting `start_link/1`
  (e.g. `Capstan.CheckpointStore.InMemory` for tests and ephemeral pipelines). The store
  is supervised as part of the pipeline.

  **Sink-owned** (C1a): omit `checkpoint_store:` and implement `c:Capstan.Sink.checkpoint/0`
  — the sink persists its data and the position atomically together and returns the
  position from `handle_transaction/1`; capstan reads the resume position from
  `checkpoint/0` and advances in-memory after each durable sink write. Effect-once by
  construction (see usage-rules "Sink-owned checkpoint mode").

  ## Start position

  `start_position:` accepts three forms. **`:checkpoint`** (default) resumes from the
  position authority — the checkpoint store (lib-owned) or `c:Capstan.Sink.checkpoint/0`
  (sink-owned); a fresh authority sends the dump an **empty GTID set**, which requests
  the server's full retained history — a server that has already purged its earliest
  logs refuses that dump, halting `:data_gap` (seed the authority with the server's
  current `@@global.gtid_executed` for a "from now" start; see usage-rules.md). An
  **explicit `%Capstan.Position{}`** resumes BOTH the dump and the assembler watermark
  from that position — the operator asserts it is a safe resume point (transactions it
  already covers are NOT re-delivered; anything after it streams). **`:current`** reads
  the server's live `@@global.gtid_executed` pre-dump and resumes from it — "start from
  now" without pre-seeding.

  ## Return values

  `{:ok, supervisor_pid}` on success, or `{:error, reason}` — a value-free atom from
  `Capstan.Config` (`:server_id_required`, `:config_invalid`,
  `:tls_verification_unspecified`, `:invalid_liveness_config`, `:unknown_option`),
  from `Capstan.Pipeline` (`:invalid_sink`,
  `:sink_missing_handle_transaction`, `:sink_missing_checkpoint`,
  `:sink_missing_handle_schema_change`, `:sink_missing_handle_snapshot`,
  `:snapshot_table_not_captured`), or from this module
  (`:checkpoint_store_required`).

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

  # C1a: sink-owned checkpoint mode RUNS (ADR-0004's deferred arm, landed). The sink
  # persists its data and the position atomically; the AssemblerServer reads the resume
  # position from c:Sink.checkpoint/0 and advances in-memory after each durable
  # sink write. No store child is wired in this mode.

  # Fail closed on a malformed lib-owned checkpoint store BEFORE any child starts, so a
  # missing `:module` surfaces as a clean `{:error, :checkpoint_store_required}` from
  # `start_link/1` rather than a `KeyError` crash deep in `AssemblerServer` init. An
  # unrecognized block key (a typo) refuses `:unknown_option` — same posture as
  # `Config.validate/1`. Sink-owned mode has NO store (the sink is the checkpoint), so
  # the check is skipped there — but a PRESENT mis-shaped one still refuses.
  defp validate_checkpoint_store(opts) do
    if Pipeline.lib_mode?(opts) or Keyword.has_key?(opts, :checkpoint_store) do
      validate_checkpoint_store_shape(opts)
    else
      :ok
    end
  end

  defp validate_checkpoint_store_shape(opts) do
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

  # C1b: `:start_position` accepts `:checkpoint` (default — resume from the position
  # authority: the checkpoint store in lib-owned mode, `c:Sink.checkpoint/0` in
  # sink-owned), an explicit `%Position{}` (resume BOTH the dump and the assembler
  # watermark from it — the operator asserts this is a safe resume point), or `:current`
  # (resume from the server's live `@@gtid_executed`, read pre-dump). Anything else is a
  # config error.
  defp validate_start_position(opts) do
    case Keyword.get(opts, :start_position, :checkpoint) do
      :checkpoint -> :ok
      %Position{} -> :ok
      :current -> :ok
      _other -> {:error, :config_invalid}
    end
  end

  # Threads the validated wiring to the supervisor. When `snapshot` is `nil` (pure C1) the
  # keyword is IDENTICAL to C1's — byte-for-byte unchanged; the `:snapshot` key is appended
  # only in snapshot mode, so the C1 supervisor sees exactly what it always saw.
  defp wiring(config, opts, snapshot) do
    base = [
      sink: Keyword.fetch!(opts, :sink),
      checkpoint_store: Keyword.get(opts, :checkpoint_store),
      connection: config.connection,
      server_id: config.server_id,
      max_command_retries: config.max_command_retries,
      xa: config.xa,
      max_prepared_transactions: config.max_prepared_transactions,
      batch: config.batch,
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

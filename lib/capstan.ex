defmodule Capstan do
  @moduledoc """
  Framework-agnostic Elixir CDC consumer for MySQL row-based binary-log replication,
  delivering committed transactions to a pluggable **sink** with fail-closed,
  transaction-granularity, effect-once delivery: the checkpoint advances only after the
  sink has durably applied a transaction (design § Scope).

  ## Starting a pipeline

  Configure each pipeline explicitly at `start_link/1` (no global mutable state):

      Capstan.start_link(
        connection: [
          host: "replica.internal", port: 3306, username: "capstan",
          password: "…", database: "orders",
          ssl: true, ssl_opts: [cacertfile: "/etc/ssl/mysql-ca.pem"]  # TLS on by default (Q6/Q17)
        ],
        server_id: 1001,                       # replica identity; MUST be unique in the topology
        sink: MyApp.OrdersSink,                # implements Capstan.Sink
        checkpoint_store: [module: MyApp.Store],   # lib-owned mode; see below
        start_position: :checkpoint,           # :checkpoint (default) | %Capstan.Position{}
        max_command_retries: 5
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

  ## Return values

  `{:ok, supervisor_pid}` on success, or `{:error, reason}` — a value-free atom from
  `Capstan.Config` (`:server_id_required`, `:config_invalid`,
  `:tls_verification_unspecified`), from `Capstan.Pipeline` (`:invalid_sink`,
  `:sink_missing_handle_transaction`, `:sink_missing_checkpoint`,
  `:sink_missing_handle_schema_change`), or from this module
  (`:sink_owned_mode_unsupported`, `:start_position_current_unsupported`).
  """

  alias Capstan.Config
  alias Capstan.Pipeline

  @doc """
  Validate `opts` and start a supervised pipeline. See the moduledoc for the option shape
  and the value-free `{:error, reason}` set. Returns `{:ok, supervisor_pid}` on success.
  """
  @spec start_link(keyword()) :: {:ok, pid()} | {:error, term()}
  def start_link(opts) when is_list(opts) do
    with {:ok, config} <- Config.validate(opts),
         :ok <- Pipeline.validate_sink(opts),
         :ok <- require_lib_mode(opts) do
      Capstan.Supervisor.start_link(wiring(config, opts))
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

  defp wiring(config, opts) do
    [
      sink: Keyword.fetch!(opts, :sink),
      checkpoint_store: Keyword.fetch!(opts, :checkpoint_store),
      connection: config.connection,
      server_id: config.server_id,
      max_command_retries: config.max_command_retries,
      start_position: Keyword.get(opts, :start_position, :checkpoint),
      tables: Keyword.get(opts, :tables, :all)
    ]
  end
end

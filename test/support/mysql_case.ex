defmodule Capstan.MysqlCase do
  @moduledoc """
  Live-substrate integration-marquee support (plan Task 18) — the one place substrate
  setup, connection wiring, throwaway-container lifecycle, and the reusable sink/store
  scaffolding live so a marquee body reads as its property, not its plumbing.

  ## Two substrates

    * **The shared, running `mysql-cdc-probe`** (`127.0.0.1:5633`, `scripts/dev-substrate.sh`)
      — every NON-destructive marquee streams from it read-mostly, on DEDICATED per-marquee
      tables (`DROP TABLE IF EXISTS` in setup). It is **never** restarted, reconfigured, or
      duplicated (forge substrate rule).
    * **A throwaway `mysql:8.0` container** on a fresh ephemeral port — every DESTRUCTIVE
      marquee (`PURGE BINARY LOGS`, a server configured with
      `binlog_transaction_compression=ON`) spins one via `with_throwaway_mysql/2`, provisions
      it, runs, and tears it down with a guaranteed `after`. Those marquees are
      `@moduletag :requires_docker`, so ExUnit EXCLUDES them (a genuine skip, never a spurious
      pass) unless the run selects that tag; `with_throwaway_mysql/2` also raises a clear error
      if Docker is somehow absent under an explicit `--only requires_docker`.

  ## Two connection identities

    * `query_connection/1` — `root` over `mysql_native_password` (plaintext). The planting
      connection: it needs `CREATE`/`DROP`/`INSERT`, which the replication user lacks.
    * `pipeline_connection/2` — `capstan_sha2` over the **default** `caching_sha2_password`
      posture (F7). The shared `root` is native-password, so a pipeline using the default
      `auth_plugins` MUST authenticate as the caching_sha2 replication user; `ensure_sha2_user!/1`
      creates it idempotently so the suite never depends on `dev-substrate.sh` having run.

  ## Reusable scaffolding

    * `Sink` — a `Capstan.Sink` that materialises each delivered transaction's changes
      exactly once (the `Enumerable.t()` contract), appends the committed GTID to an optional
      durable ETS ledger (the effect-once proof), and forwards every output to the test pid.
    * `SeededStore` — a non-durable `Capstan.CheckpointStore` seeded to a live watermark (a
      pipeline resumes from "now", never from empty — which the substrate refuses `:data_gap`).
    * `DurableStore` — an ETS-backed `Capstan.CheckpointStore` whose backing is owned by the
      TEST process, so a checkpoint SURVIVES a pipeline kill/restart (the resume-correctness proof).
  """

  alias Capstan.Gtid
  alias Capstan.Protocol.{Command, Handshake, Packet}

  @host "127.0.0.1"
  @shared_port 5633
  @root_password "probe"
  @sha2_user "capstan_sha2"
  # Throwaway credential for disposable local containers — never a real secret (dev-substrate.sh).
  @sha2_password "capstan_sha2_pw"
  @connect_timeout 20_000

  ## ---------------------------------------------------------------------------
  ## connection option shapes
  ## ---------------------------------------------------------------------------

  @doc "The shared substrate's TCP port."
  @spec shared_port() :: pos_integer()
  def shared_port, do: @shared_port

  @doc """
  The planting connection: `root` over `mysql_native_password` (plaintext), which carries the
  `CREATE`/`DROP`/`INSERT` privileges the replication user lacks.
  """
  @spec query_connection(pos_integer()) :: keyword()
  def query_connection(port \\ @shared_port) do
    [
      host: @host,
      port: port,
      username: "root",
      password: @root_password,
      ssl: false,
      auth_plugins: [:mysql_native_password],
      database: "probe_db"
    ]
  end

  @doc """
  The pipeline connection: `capstan_sha2` over the DEFAULT `caching_sha2_password` posture
  (F7). `opts` may set `ssl_opts:` to run over TLS (the default is plaintext, which exercises
  the caching_sha2 RSA full-auth path). `auth_plugins` is intentionally omitted so the library
  default (`[:caching_sha2_password]`) applies.
  """
  @spec pipeline_connection(pos_integer(), keyword()) :: keyword()
  def pipeline_connection(port \\ @shared_port, opts \\ []) do
    base = [
      host: @host,
      port: port,
      username: @sha2_user,
      password: @sha2_password,
      database: "probe_db"
    ]

    case Keyword.fetch(opts, :ssl_opts) do
      {:ok, ssl_opts} -> Keyword.put(base, :ssl_opts, ssl_opts)
      :error -> Keyword.put(base, :ssl, false)
    end
  end

  ## ---------------------------------------------------------------------------
  ## live query connection (Capstan.FixtureCapture / ValueFree precedent)
  ## ---------------------------------------------------------------------------

  @doc """
  Opens a live connection to `conn`, returning `{socket, handshake_info}` — the authenticated,
  transport-tagged `Capstan.Protocol.Packet.socket` plus the negotiated handshake result (which
  carries `:tls`). Raises on any handshake failure.
  """
  @spec connect!(keyword()) :: {Packet.socket(), map()}
  def connect!(conn) do
    host = @host |> String.to_charlist()
    port = Keyword.fetch!(conn, :port)
    {:ok, raw} = :gen_tcp.connect(host, port, [:binary, active: false], @connect_timeout)

    case Handshake.connect({:gen_tcp, raw}, Keyword.delete(conn, :port)) do
      {:ok, %{} = info} -> {info.socket, info}
      {:error, reason} -> raise "capstan mysql_case: handshake failed #{inspect(reason)}"
    end
  end

  @doc "Opens a live query socket and returns only the socket (the common case)."
  @spec socket!(keyword()) :: Packet.socket()
  def socket!(conn), do: connect!(conn) |> elem(0)

  @doc "Runs `sql` on `socket`, raising on error. Returns `:ok`."
  @spec run!(Packet.socket(), String.t()) :: :ok
  def run!(socket, sql) do
    case Command.query(socket, sql) do
      :ok -> :ok
      {:ok, _rows} -> :ok
      {:error, reason} -> raise "capstan mysql_case: query failed #{inspect(reason)}: #{sql}"
    end
  end

  @doc "Runs every statement in `sqls` on `socket`, in order."
  @spec run_all!(Packet.socket(), [String.t()]) :: :ok
  def run_all!(socket, sqls), do: Enum.each(sqls, &run!(socket, &1))

  @doc """
  Runs `sql` best-effort, swallowing ANY failure (a `{:error, _}` result OR a raised
  transport error). For idempotent cleanup like rolling back a maybe-absent prepared XA
  transaction, where the statement legitimately errors when there is nothing to undo.
  """
  @spec run_tolerant(Packet.socket(), String.t()) :: :ok
  def run_tolerant(socket, sql) do
    _ = Command.query(socket, sql)
    :ok
  rescue
    _ -> :ok
  catch
    _, _ -> :ok
  end

  @doc "Runs `sql` on `socket` and returns its result rows (each a list of string cells)."
  @spec query_rows!(Packet.socket(), String.t()) :: [[String.t()]]
  def query_rows!(socket, sql) do
    case Command.query(socket, sql) do
      {:ok, rows} when is_list(rows) -> rows
      other -> raise "capstan mysql_case: expected rows from #{sql}, got #{inspect(other)}"
    end
  end

  @doc "Reads `@@global.gtid_executed` — the live resume watermark — as a canonical string."
  @spec read_gtid_executed!(Packet.socket()) :: String.t()
  def read_gtid_executed!(socket) do
    case Command.query(socket, "SELECT @@global.gtid_executed") do
      {:ok, [[value]]} when is_binary(value) -> value
      other -> raise "capstan mysql_case: unexpected @@gtid_executed #{inspect(other)}"
    end
  end

  @doc "Closes a transport-tagged socket."
  @spec close!(Packet.socket()) :: :ok
  def close!({:gen_tcp, sock}), do: :gen_tcp.close(sock)
  def close!({:ssl, sock}), do: :ssl.close(sock)

  ## ---------------------------------------------------------------------------
  ## pipeline helpers
  ## ---------------------------------------------------------------------------

  @doc """
  Stops a pipeline supervisor, tolerating an already-dead supervisor and a non-`:normal` exit
  reason. `Capstan.start_link/1` links the supervisor to the caller, so a test whose process has
  already exited may find it gone (or exiting `:shutdown`) when a later `on_exit` runs; either way
  the children are torn down. Returns `:ok`.
  """
  @spec stop_pipeline(pid()) :: :ok
  def stop_pipeline(supervisor) do
    if Process.alive?(supervisor) do
      try do
        Supervisor.stop(supervisor)
      catch
        :exit, _ -> :ok
      end
    end

    :ok
  end

  @doc """
  A per-marquee `server_id` in the 6000-7999 band (mirrors `ValueFree`), unique enough that a
  marquee never collides with the shared substrate's other replicas — EXCEPT the deliberate
  duplicate the `:server_id_conflict` marquee constructs.
  """
  @spec unique_server_id() :: pos_integer()
  def unique_server_id, do: 6000 + rem(System.unique_integer([:positive]), 2000)

  @doc """
  The `:assembler` child pid of a running pipeline supervisor — the process whose exit carries an
  AssemblerServer-side fail-closed halt (`:compressed_payload_unsupported`,
  `:unsupported_transaction_shape`) that emits no telemetry and so must be observed by monitor.
  """
  @spec assembler_pid(pid()) :: pid()
  def assembler_pid(supervisor) do
    {:assembler, pid, _type, _mods} =
      supervisor
      |> Supervisor.which_children()
      |> Enum.find(fn {id, _, _, _} -> id == :assembler end)

    pid
  end

  @doc """
  Attaches a `[:capstan, :connection, :halt]` telemetry handler that forwards each halt reason
  to `test_pid` as `{:connection_halt, reason}`. Returns the handler id (detach it in `on_exit`).
  A `Connection`-side halt (`:server_id_conflict`, `:data_gap`) surfaces here.
  """
  @spec attach_halt_telemetry(pid()) :: {module(), reference()}
  def attach_halt_telemetry(test_pid) do
    handler_id = {__MODULE__, make_ref()}

    :ok =
      :telemetry.attach(
        handler_id,
        [:capstan, :connection, :halt],
        &__MODULE__.__forward_halt__/4,
        test_pid
      )

    handler_id
  end

  @doc false
  @spec __forward_halt__([atom(), ...], map(), map(), pid()) :: :ok
  def __forward_halt__(_event, _measurements, %{reason: reason}, test_pid) do
    send(test_pid, {:connection_halt, reason})
    :ok
  end

  @doc """
  Attaches a `[:capstan, :connection, :established]` telemetry handler forwarding `:connection_established`
  to `test_pid`. Lets the `:server_id_conflict` marquee wait for the FIRST replica to register its
  dump before starting the second (so the conflict is deterministic, not a race).
  """
  @spec attach_established_telemetry(pid()) :: {module(), reference()}
  def attach_established_telemetry(test_pid) do
    handler_id = {__MODULE__, make_ref()}

    :ok =
      :telemetry.attach(
        handler_id,
        [:capstan, :connection, :established],
        &__MODULE__.__forward_established__/4,
        test_pid
      )

    handler_id
  end

  @doc false
  @spec __forward_established__([atom(), ...], map(), map(), pid()) :: :ok
  def __forward_established__(_event, _measurements, _metadata, test_pid) do
    send(test_pid, :connection_established)
    :ok
  end

  ## ---------------------------------------------------------------------------
  ## docker / throwaway container lifecycle
  ## ---------------------------------------------------------------------------

  @doc """
  True iff the local Docker daemon answers `docker info`. The throwaway marquees gate on the
  `:requires_docker` tag (excluded by ExUnit when not selected), so this is the last-resort clear
  error inside `with_throwaway_mysql/2` for an explicit `--only requires_docker` run without Docker.
  """
  @spec docker_available?() :: boolean()
  def docker_available? do
    case System.cmd("docker", ["info"], stderr_to_stdout: true) do
      {_output, 0} -> true
      _ -> false
    end
  rescue
    _ -> false
  end

  @doc """
  Spins a throwaway `mysql:8.0` on a fresh ephemeral port with `extra_flags` (beyond the five
  precondition variables + GTID), waits until it answers an authenticated query, provisions the
  caching_sha2 user + `probe_db`, runs `fun.(port)`, and tears the container down with a
  guaranteed `after` (`docker rm -f`). Callers must be `@moduletag :requires_docker` (the ExUnit
  gate — excluded when not selected, so absent Docker is a genuine skip, never a pass); do NOT
  wrap call sites in a `docker_available?/0` conditional — a pass-when-absent branch is exactly
  the false green the tag replaced. This function raises a clear error as the last resort for an
  explicit `--only requires_docker` run without Docker.
  """
  @spec with_throwaway_mysql([String.t()], (pos_integer() -> result)) :: result when result: var
  def with_throwaway_mysql(extra_flags, fun) when is_list(extra_flags) and is_function(fun, 1) do
    unless docker_available?() do
      raise "capstan mysql_case: Docker unavailable — :requires_docker marquees need a throwaway " <>
              "container. They are excluded by default; run them with Docker present " <>
              "(`mix test --only requires_docker`)."
    end

    name = "capstan-throwaway-#{System.unique_integer([:positive])}"
    port = free_port()

    try do
      start_throwaway!(name, port, extra_flags)
      wait_ready!(name)
      provision_throwaway!(name)
      fun.(port)
    after
      System.cmd("docker", ["rm", "-f", name], stderr_to_stdout: true)
    end
  end

  # The five precondition variables + GTID + enforce-consistency (design Q5), matching
  # scripts/dev-substrate.sh COMMON_FLAGS, plus a distinct server-id and native-root default so
  # the query connection authenticates the same way it does against the shared substrate.
  @common_flags [
    "--binlog-format=ROW",
    "--binlog-row-image=FULL",
    "--binlog-row-metadata=FULL",
    "--binlog-row-value-options=",
    "--gtid-mode=ON",
    "--enforce-gtid-consistency=ON",
    "--default-authentication-plugin=mysql_native_password"
  ]

  defp start_throwaway!(name, port, extra_flags) do
    args =
      [
        "run",
        "-d",
        "--name",
        name,
        "-p",
        "127.0.0.1:#{port}:3306",
        "-e",
        "MYSQL_ROOT_PASSWORD=#{@root_password}",
        "-e",
        "MYSQL_DATABASE=probe_db",
        "mysql:8.0"
      ] ++ @common_flags ++ ["--server-id=#{79 + rem(port, 200)}"] ++ extra_flags

    case System.cmd("docker", args, stderr_to_stdout: true) do
      {_output, 0} -> :ok
      {output, code} -> raise "capstan mysql_case: throwaway create failed (#{code}): #{output}"
    end
  end

  # Ready = the REAL networked server answers an authenticated TCP query (dev-substrate.sh's
  # wait_ready rationale — a socket-only init server answers ping but not this).
  defp wait_ready!(name, attempts \\ 60) do
    Enum.reduce_while(1..attempts, nil, fn n, _ ->
      cmd =
        System.cmd(
          "docker",
          [
            "exec",
            name,
            "mysql",
            "-h127.0.0.1",
            "-uroot",
            "-p#{@root_password}",
            "-N",
            "-e",
            "SELECT 1"
          ],
          stderr_to_stdout: true
        )

      case cmd do
        {_out, 0} ->
          {:halt, :ok}

        _ when n == attempts ->
          raise "capstan mysql_case: throwaway #{name} never became ready"

        _ ->
          Process.sleep(2_000)
          {:cont, nil}
      end
    end)
  end

  # F7: create the caching_sha2 replication user the pipeline authenticates as, mirroring
  # scripts/dev-substrate.sh's ensure_sha2_user (idempotent).
  defp provision_throwaway!(name) do
    sql =
      "CREATE USER IF NOT EXISTS '#{@sha2_user}'@'%' IDENTIFIED WITH caching_sha2_password BY '#{@sha2_password}';" <>
        "GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO '#{@sha2_user}'@'%';"

    case System.cmd(
           "docker",
           ["exec", name, "mysql", "-uroot", "-p#{@root_password}", "-e", sql],
           stderr_to_stdout: true
         ) do
      {_out, 0} ->
        :ok

      {output, code} ->
        raise "capstan mysql_case: throwaway provision failed (#{code}): #{output}"
    end
  end

  # An ephemeral free TCP port: bind :0, read the assigned port, release it. A tiny TOCTOU
  # window remains before docker binds it, acceptable for serially-run integration marquees.
  defp free_port do
    {:ok, listen} = :gen_tcp.listen(0, [:binary, ip: {127, 0, 0, 1}])
    {:ok, port} = :inet.port(listen)
    :ok = :gen_tcp.close(listen)
    port
  end

  ## ---------------------------------------------------------------------------
  ## F7: ensure the caching_sha2 user on the SHARED substrate (self-sufficiency)
  ## ---------------------------------------------------------------------------

  @doc """
  Ensures the caching_sha2 replication user exists on the substrate reachable at `query_conn`
  (F7). Idempotent (`CREATE USER IF NOT EXISTS`); run once in a marquee `setup_all` so the suite
  never depends on `scripts/dev-substrate.sh` having provisioned it.
  """
  @spec ensure_sha2_user!(keyword()) :: :ok
  def ensure_sha2_user!(query_conn) do
    socket = socket!(query_conn)

    try do
      run!(
        socket,
        "CREATE USER IF NOT EXISTS '#{@sha2_user}'@'%' " <>
          "IDENTIFIED WITH caching_sha2_password BY '#{@sha2_password}'"
      )

      run!(
        socket,
        "GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO '#{@sha2_user}'@'%'"
      )
    after
      close!(socket)
    end

    :ok
  end

  ## ===========================================================================
  ## reusable sink + stores
  ## ===========================================================================

  defmodule Sink do
    @moduledoc """
    A configurable `Capstan.Sink` for the marquees. Its config rides in `:persistent_term`
    (the integration marquees are `async: false`, so exactly one config is live at a time — the
    `ValueFree.CapturingSink` precedent). It:

      * materialises `txn.changes` EXACTLY ONCE into a list (honouring the `Enumerable.t()`
        single-pass contract) and forwards `{:txn, gtid, changes, position}` to the test pid;
      * appends the committed GTID to an OPTIONAL durable ETS ledger (`:duplicate_bag`), so a
        double-delivery is VISIBLE as the same GTID twice — never a PK-upsert count that would
        hide it (the effect-once proof);
      * forwards `{:schema_change, sc, position}` for DDL;
      * returns the configured result (`:ok` by default), advancing the checkpoint.
    """
    @behaviour Capstan.Sink

    @key {__MODULE__, :config}

    @doc "Configure the live sink: `%{pid: test_pid, ledger: ets_or_nil}`."
    @spec configure(%{required(:pid) => pid(), optional(:ledger) => :ets.tid() | nil}) :: :ok
    def configure(config), do: :persistent_term.put(@key, Map.put_new(config, :ledger, nil))

    @doc "Erase the live sink config (an `on_exit` hook)."
    @spec clear() :: :ok
    def clear do
      :persistent_term.erase(@key)
      :ok
    end

    defp config, do: :persistent_term.get(@key)

    @impl Capstan.Sink
    def handle_transaction(txn) do
      cfg = config()
      changes = Enum.to_list(txn.changes)
      if cfg.ledger, do: :ets.insert(cfg.ledger, {:gtid, txn.gtid})
      send(cfg.pid, {:txn, txn.gtid, changes, txn.position})
      {:ok, txn.position}
    end

    @impl Capstan.Sink
    def handle_schema_change(schema_change, position) do
      cfg = config()
      send(cfg.pid, {:schema_change, schema_change, position})
      :ok
    end
  end

  defmodule SeededStore do
    @moduledoc """
    A non-durable `Capstan.CheckpointStore` seeded to a starting `gtid_set`, so a pipeline
    resumes from a chosen live watermark rather than from empty (which the substrate refuses
    `:data_gap`). Process-lifetime only — a restart loses it; the restart marquees use
    `DurableStore`.
    """
    @behaviour Capstan.CheckpointStore

    @spec start_link(keyword()) :: Agent.on_start()
    def start_link(opts), do: Agent.start_link(fn -> Keyword.get(opts, :gtid_set, "") end)

    @impl Capstan.CheckpointStore
    def read(store), do: {:ok, Agent.get(store, & &1)}

    @impl Capstan.CheckpointStore
    def write(store, gtid_set) when is_binary(gtid_set),
      do: Agent.update(store, fn _current -> gtid_set end)
  end

  defmodule DurableStore do
    @moduledoc """
    A `Capstan.CheckpointStore` whose one durable value lives in an ETS cell the TEST owns, keyed
    by `{table, key}` in `start_link/1` opts. Because the backing outlives the store PROCESS, a
    pipeline restarted from a NEW `DurableStore` pointing at the same cell resumes from the
    checkpoint the previous run persisted — the resume-correctness / effect-once substrate.

    The test creates the `:public` table and seeds the watermark (`seed/3`) before the first
    pipeline start; the store only ever reads/writes that one cell.
    """
    @behaviour Capstan.CheckpointStore

    @doc "Create the durable ETS backing (`:public` so the store PROCESS can reach it)."
    @spec new_table() :: :ets.tid()
    def new_table, do: :ets.new(:capstan_durable_store, [:public, :set])

    @doc "Seed the durable cell to `gtid_set` (the initial live watermark)."
    @spec seed(:ets.tid(), term(), String.t()) :: :ok
    def seed(table, key, gtid_set) when is_binary(gtid_set) do
      true = :ets.insert(table, {key, gtid_set})
      :ok
    end

    @doc "Read the durable cell directly (the marquee asserts the persisted checkpoint advanced)."
    @spec current(:ets.tid(), term()) :: String.t() | nil
    def current(table, key) do
      case :ets.lookup(table, key) do
        [{^key, value}] -> value
        [] -> nil
      end
    end

    @spec start_link(keyword()) :: Agent.on_start()
    def start_link(opts) do
      table = Keyword.fetch!(opts, :table)
      key = Keyword.fetch!(opts, :key)
      Agent.start_link(fn -> {table, key} end)
    end

    @impl Capstan.CheckpointStore
    def read(store) do
      {table, key} = Agent.get(store, & &1)
      {:ok, current(table, key)}
    end

    @impl Capstan.CheckpointStore
    def write(store, gtid_set) when is_binary(gtid_set) do
      {table, key} = Agent.get(store, & &1)
      true = :ets.insert(table, {key, gtid_set})
      :ok
    end
  end

  @doc """
  The number of committed GTIDs in `gtid_set` for `uuid` — the count of transactions in the
  set's single interval band, used to compare "how many committed" against ledger deliveries.
  """
  @spec committed_count(String.t()) :: non_neg_integer()
  def committed_count(gtid_set) do
    gtid_set
    |> Gtid.parse()
    |> Gtid.sources()
    |> Enum.flat_map(fn {_uuid, intervals} -> intervals end)
    |> Enum.reduce(0, fn {low, high}, acc -> acc + (high - low + 1) end)
  end
end

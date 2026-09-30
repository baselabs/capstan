defmodule Capstan.MysqlCase do
  @moduledoc """
  Live-substrate integration-marquee support (plan Task 18) — the one place substrate
  setup, connection wiring, throwaway-container lifecycle, and the reusable sink/store
  scaffolding live so a marquee body reads as its property, not its plumbing.

  ## Two substrates

    * **The shared, running `mysql-cdc-probe`** (`127.0.0.1`, port `MYSQL_PORT_80` — `shared_port/0`)
      — every NON-destructive marquee streams from it read-mostly, on DEDICATED per-marquee
      tables (`DROP TABLE IF EXISTS` in setup). It is **never** restarted, reconfigured, or
      duplicated (forge substrate rule).
    * **The disposable MySQL 8.0** (`127.0.0.1`, port `CAPSTAN_DISPOSABLE_MYSQL_PORT`), which
      the environment provides and the suite may destroy. Every DESTRUCTIVE marquee
      (`PURGE BINARY LOGS`, `binlog_transaction_compression=ON`, `binlog_row_value_options=PARTIAL_JSON`)
      runs through `with_disposable_mysql/2`, which takes a server-scoped lease, refuses a server
      not marked disposable, and resets binlog/GTID history and the managed variables so each
      marquee starts from a new server's state. Those marquees are `@moduletag :disposable_mysql`,
      so ExUnit EXCLUDES them (a genuine skip, never a spurious pass) unless the run selects that
      tag; selected without the port configured, the helper raises naming the variable.

  ## Two connection identities

    * `query_connection/1` — `root` over `mysql_native_password` (plaintext). The planting
      connection: it needs `CREATE`/`DROP`/`INSERT`, which the replication user lacks.
    * `pipeline_connection/2` — `capstan_sha2` over the **default** `caching_sha2_password`
      posture (F7). The shared `root` is native-password, so a pipeline using the default
      `auth_plugins` MUST authenticate as the caching_sha2 replication user; `ensure_sha2_user!/1`
      creates it idempotently so the suite never depends on the substrate's seed having run.

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
  @root_password "probe"
  @sha2_user "capstan_sha2"
  # Throwaway credential for disposable local containers — never a real secret (.env.example).
  @sha2_password "capstan_sha2_pw"
  @connect_timeout 20_000

  ## ---------------------------------------------------------------------------
  ## connection option shapes
  ## ---------------------------------------------------------------------------

  @doc """
  The shared substrate's TCP port — the SINGLE accessor every test uses (no port literal appears in
  lib/ or test/). Sourced from `MYSQL_PORT_80` via Dotenvy in config/runtime.exs (default in
  .env.example).
  """
  @spec shared_port() :: pos_integer()
  def shared_port, do: Application.fetch_env!(:capstan, :mysql_substrate)[:port_80]

  @doc """
  The planting connection: `root` over `mysql_native_password` (plaintext), which carries the
  `CREATE`/`DROP`/`INSERT` privileges the replication user lacks.
  """
  @spec query_connection(pos_integer()) :: keyword()
  def query_connection(port \\ shared_port()) do
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
  def pipeline_connection(port \\ shared_port(), opts \\ []) do
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
  Opens a live connection to `conn` at ITS OWN `host:` (default `#{@host}`), returning
  `{socket, handshake_info}` — the authenticated, transport-tagged
  `Capstan.Protocol.Packet.socket` plus the negotiated handshake result (which carries
  `:tls`). Raises on any handshake failure. `connect!/1` is this with the local
  substrate host forced; the remote-source marquees (Aurora, aurora-sim) pass their own
  `host:`.
  """
  @spec connect_at!(keyword()) :: {Packet.socket(), map()}
  def connect_at!(conn) do
    host = conn |> Keyword.get(:host, @host) |> String.to_charlist()
    port = Keyword.fetch!(conn, :port)
    {:ok, raw} = :gen_tcp.connect(host, port, [:binary, active: false], @connect_timeout)

    case Handshake.connect({:gen_tcp, raw}, Keyword.delete(conn, :port)) do
      {:ok, %{} = info} -> {info.socket, info}
      {:error, reason} -> raise "capstan mysql_case: handshake failed #{inspect(reason)}"
    end
  end

  @doc """
  Opens a live connection to `conn` on the LOCAL substrate host (`127.0.0.1`), returning
  `{socket, handshake_info}` — the authenticated, transport-tagged
  `Capstan.Protocol.Packet.socket` plus the negotiated handshake result (which carries
  `:tls`). Raises on any handshake failure.
  """
  @spec connect!(keyword()) :: {Packet.socket(), map()}
  def connect!(conn), do: connect_at!(Keyword.put_new(conn, :host, @host))

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
  AssemblerServer-side fail-closed halt (`:unsupported_transaction_shape`, a malformed
  transaction-payload reason) that emits no telemetry and so must be observed by monitor.
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
  ## the disposable server (destructive marquees)
  ## ---------------------------------------------------------------------------

  # The schema that marks a server as deliberately disposable. The provisioning creates it
  # (docs/testing.md); a server without it is never reset.
  @disposable_marker "capstan_disposable"
  # The one server-scoped lease a destructive marquee holds from before its reset until after its
  # pipeline stopped (MySQL named lock: two test runs never share the server at once).
  @disposable_lock "capstan_disposable_lease"
  @disposable_lock_wait_s 600
  # The dynamic server variables a destructive marquee may change, and the baseline every marquee
  # starts from and is restored to (the shared substrate's own values).
  @disposable_baseline [binlog_transaction_compression: "OFF", binlog_row_value_options: ""]

  @doc """
  The disposable server's TCP port (`CAPSTAN_DISPOSABLE_MYSQL_PORT`), or raises naming the
  variable. Only the `:disposable_mysql` marquees use it.
  """
  @spec disposable_port!() :: pos_integer()
  def disposable_port! do
    case Application.get_env(:capstan, :mysql_substrate, [])[:disposable_port] do
      port when is_integer(port) ->
        port

      _ ->
        raise "capstan mysql_case: CAPSTAN_DISPOSABLE_MYSQL_PORT is not set. The :disposable_mysql " <>
                "marquees reset the server they run on, so they need a MySQL 8.0 of their own " <>
                "(see docs/testing.md); they are excluded unless selected."
    end
  end

  @doc """
  Runs `fun.(port)` against the disposable server, in the state of a new server: binlog and GTID
  history reset, the managed variables at their baseline, then `settings` applied (a keyword list
  over `binlog_transaction_compression` and `binlog_row_value_options` only), and the
  `capstan_sha2` user present.

  Before touching anything it takes the server-scoped lease and refuses a server that is not
  deliberately disposable: one without the `#{@disposable_marker}` schema, one that is either shared
  substrate (same port, or the same `@@server_uuid` reached another way), or one that is not
  MySQL 8.0. The lease is released, and the baseline restored, by an `on_exit` registered here, so
  it runs AFTER the `on_exit` callbacks the marquee registers later (its pipeline stops first).
  Callers are `@moduletag :disposable_mysql` modules with `async: false`.
  """
  @spec with_disposable_mysql(keyword(), (pos_integer() -> result)) :: result when result: var
  def with_disposable_mysql(settings, fun) when is_list(settings) and is_function(fun, 1) do
    port = disposable_port!()
    validate_disposable_settings!(settings)
    lease = acquire_disposable_lease!(port)
    ExUnit.Callbacks.on_exit(fn -> release_disposable_lease(lease) end)

    admin = socket!(admin_connection(port))

    try do
      guard_disposable!(admin, port)
      run!(admin, "CREATE DATABASE IF NOT EXISTS probe_db")
      # A new server's binlog and GTID history. The marquees read their watermarks live and pass
      # without it (observed: removing the reset left two consecutive runs green); it keeps a
      # long-lived server's binlogs from growing without bound and each marquee's history to its
      # own statements.
      run!(admin, "RESET MASTER")
      apply_disposable_settings!(admin, Keyword.merge(@disposable_baseline, settings))
    after
      close!(admin)
    end

    ensure_sha2_user!(query_connection(port))
    fun.(port)
  end

  @doc false
  # The identity checks, callable with an explicit list of shared server UUIDs (the test of the
  # guard itself). Raises before any statement that changes server state.
  @spec guard_disposable!(Packet.socket(), pos_integer(), [String.t()] | nil) :: :ok
  def guard_disposable!(admin, port, shared_uuids \\ nil) do
    substrate = Application.get_env(:capstan, :mysql_substrate, [])
    shared_ports = [substrate[:port_80], substrate[:port_84]]

    if port in shared_ports do
      raise "capstan mysql_case: CAPSTAN_DISPOSABLE_MYSQL_PORT (#{port}) is a shared substrate port; " <>
              "the disposable marquees would reset it"
    end

    [[version, uuid, marked]] =
      query_rows!(
        admin,
        "SELECT VERSION(), @@server_uuid, (SELECT COUNT(*) FROM information_schema.SCHEMATA " <>
          "WHERE SCHEMA_NAME = '#{@disposable_marker}')"
      )

    unless marked == "1" do
      raise "capstan mysql_case: the server on port #{port} has no `#{@disposable_marker}` schema, " <>
              "so it is not marked disposable; refusing to reset it (docs/testing.md)"
    end

    unless String.starts_with?(version, "8.0.") do
      raise "capstan mysql_case: the disposable server must be MySQL 8.0 (the version these " <>
              "marquees are written and tested against); it is #{version}"
    end

    if uuid in (shared_uuids || shared_server_uuids(shared_ports)) do
      raise "capstan mysql_case: the disposable server is a shared substrate server " <>
              "(@@server_uuid #{uuid}); refusing to reset it"
    end

    :ok
  end

  # The shared servers' UUIDs, from whichever of them answers (an unreachable one cannot be the
  # server the disposable port reaches).
  defp shared_server_uuids(ports) do
    for port <- ports, is_integer(port), uuid = server_uuid(port), uuid != nil, do: uuid
  end

  defp server_uuid(port) do
    socket = socket!(admin_connection(port))

    try do
      [[uuid]] = query_rows!(socket, "SELECT @@server_uuid")
      uuid
    after
      close!(socket)
    end
  rescue
    _ -> nil
  end

  # root without a default database: the guard runs before `probe_db` is known to exist.
  defp admin_connection(port), do: Keyword.delete(query_connection(port), :database)

  defp validate_disposable_settings!(settings) do
    managed = Keyword.keys(@disposable_baseline)

    case Keyword.keys(settings) -- managed do
      [] ->
        :ok

      other ->
        raise ArgumentError,
              "with_disposable_mysql/2 manages only #{inspect(managed)}; got #{inspect(other)}"
    end
  end

  defp apply_disposable_settings!(admin, settings) do
    for {name, value} <- settings do
      run!(admin, "SET GLOBAL #{name} = '#{value}'")
    end

    :ok
  end

  # The lease lives in its own process, which owns the connection holding the named lock: the
  # marquee's own process ends before its on_exit callbacks run, and the lock must outlive it.
  defp acquire_disposable_lease!(port) do
    parent = self()
    ref = make_ref()

    pid =
      spawn(fn ->
        socket = socket!(admin_connection(port))

        case query_rows!(
               socket,
               "SELECT GET_LOCK('#{@disposable_lock}', #{@disposable_lock_wait_s})"
             ) do
          [["1"]] ->
            send(parent, {ref, :leased})

            receive do
              {:release, from} ->
                _ = apply_disposable_settings!(socket, @disposable_baseline)
                _ = run_tolerant(socket, "DO RELEASE_LOCK('#{@disposable_lock}')")
                close!(socket)
                send(from, {ref, :released})
            end

          other ->
            close!(socket)
            send(parent, {ref, {:busy, other}})
        end
      end)

    receive do
      {^ref, :leased} ->
        {pid, ref}

      {^ref, {:busy, other}} ->
        raise "capstan mysql_case: another test run held the disposable server for " <>
                "#{@disposable_lock_wait_s}s (GET_LOCK returned #{inspect(other)})"
    after
      (@disposable_lock_wait_s + 30) * 1000 ->
        Process.exit(pid, :kill)
        raise "capstan mysql_case: timed out waiting for the disposable server's lease"
    end
  end

  defp release_disposable_lease({pid, ref}) do
    send(pid, {:release, self()})

    receive do
      {^ref, :released} -> :ok
    after
      30_000 -> Process.exit(pid, :kill)
    end
  end

  ## ---------------------------------------------------------------------------
  ## F7: ensure the caching_sha2 user on the SHARED substrate (self-sufficiency)
  ## ---------------------------------------------------------------------------

  @doc """
  Ensures the caching_sha2 replication user exists on the substrate reachable at `query_conn`
  (F7). Idempotent (`CREATE USER IF NOT EXISTS`); run once in a marquee `setup_all` so the suite
  never depends on the substrate's seed having provisioned it.
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

  ## ===========================================================================
  ## ADR-0013: the Aurora source (the :aurora_mysql marquees) and its local
  ## simulator (the :aurora_sim marquees, scripts/aurora-sim/)
  ## ===========================================================================

  @aurora_env_keys [
    host: "AURORA_MYSQL_HOST",
    port: "AURORA_MYSQL_PORT",
    user: "AURORA_MYSQL_USER",
    password: "AURORA_MYSQL_PASSWORD",
    ca_file: "AURORA_MYSQL_CA_FILE"
  ]

  @doc """
  The Aurora cluster WRITER endpoint connection (ADR-0013), built from the
  `AURORA_MYSQL_*` environment (`config/runtime.exs` → `:aurora_substrate`): TLS with
  the RDS CA bundle and hostname verification LEFT ON (the documented Aurora posture —
  unlike the self-signed recipe, no `server_name_indication: :disable`) and the default
  `caching_sha2_password` auth posture.

  Raises naming every missing variable — an unset environment is an unconfigured
  marquee, never a silent skip. The refusal carries variable NAMES only; the credential
  value never reaches it, a log, or telemetry (Rule 1).
  """
  @spec aurora_connection!() :: keyword()
  def aurora_connection! do
    aurora = Application.get_env(:capstan, :aurora_substrate, [])

    case for {key, env_name} <- @aurora_env_keys, is_nil(aurora[key]), do: env_name do
      [] ->
        [
          host: aurora[:host],
          port: aurora[:port],
          username: aurora[:user],
          password: aurora[:password],
          ssl: true,
          ssl_opts: [cacertfile: aurora[:ca_file]]
        ]

      missing ->
        raise "capstan mysql_case: the :aurora_mysql marquees need " <>
                "#{Enum.join(Enum.sort(missing), ", ")} (see .env.example); refusing to run " <>
                "against nothing — an unset Aurora environment is an unconfigured marquee"
    end
  end

  ## ===========================================================================
  ## C2 initial-snapshot marquee scaffolding (plan Task 11)
  ## ===========================================================================

  @doc """
  A per-marquee throwaway SCHEMA name (`capstan_snap_<unique>`). Each snapshot marquee creates
  its own schema, provisions its tables inside it, and `DROP DATABASE`s it in `on_exit` — so no
  marquee ever leaks a schema onto the shared substrate (design Q-tests / forge substrate rule).
  """
  @spec unique_schema() :: String.t()
  def unique_schema, do: "capstan_snap_" <> Integer.to_string(System.unique_integer([:positive]))

  @doc "Creates the throwaway `schema` (fails if it somehow already exists)."
  @spec create_schema!(Packet.socket(), String.t()) :: :ok
  def create_schema!(socket, schema), do: run!(socket, "CREATE DATABASE #{ident(schema)}")

  @doc "Drops the throwaway `schema` best-effort (an `on_exit` teardown; already-gone is fine)."
  @spec drop_schema!(Packet.socket(), String.t()) :: :ok
  def drop_schema!(socket, schema),
    do: run_tolerant(socket, "DROP DATABASE IF EXISTS #{ident(schema)}")

  # A backtick-quoted identifier (embedded backticks doubled) — guards a schema/table name.
  defp ident(name), do: "`" <> String.replace(name, "`", "``") <> "`"

  @doc """
  A fresh append-only `(schema, table, canonical_pk)` `:duplicate_bag` ledger. A double-delivery
  of a key/value is VISIBLE as two rows for the same key — a PK-upsert count would HIDE it. Each
  entry is `{{schema, table, canonical_pk}, %{op:, source:, value:, seq:}}`; `seq` is a
  process-global monotonic stamp giving every delivery a total order for the upsert-by-PK replay.
  """
  @spec new_ledger() :: :ets.tid()
  def new_ledger, do: :ets.new(:capstan_snapshot_ledger, [:public, :duplicate_bag])

  @doc "Every `{key, entry}` in the ledger (unordered — sort by `entry.seq` for replay order)."
  @spec ledger_dump(:ets.tid()) :: [{{String.t(), String.t(), term()}, map()}]
  def ledger_dump(ledger), do: :ets.tab2list(ledger)

  @doc """
  Attaches the four `[:capstan, :snapshot, *]` telemetry events, forwarding each to `test_pid`
  as `{:snapshot_event, suffix, measurements, metadata}` (`suffix` ∈
  `:started | :chunk_completed | :completed | :halt`). Returns the handler id (detach in
  `on_exit`). Lets a marquee await backfill completion (`:completed`) and count chunk emissions
  (`:chunk_completed`) without polling durable state.
  """
  @spec attach_snapshot_telemetry(pid()) :: {module(), reference()}
  def attach_snapshot_telemetry(test_pid) do
    handler_id = {__MODULE__, make_ref()}

    :ok =
      :telemetry.attach_many(
        handler_id,
        [
          [:capstan, :snapshot, :started],
          [:capstan, :snapshot, :chunk_completed],
          [:capstan, :snapshot, :completed],
          [:capstan, :snapshot, :halt]
        ],
        &__MODULE__.__forward_snapshot__/4,
        test_pid
      )

    handler_id
  end

  @doc false
  @spec __forward_snapshot__([atom(), ...], map(), map(), pid()) :: :ok
  def __forward_snapshot__(event, measurements, metadata, test_pid) do
    send(test_pid, {:snapshot_event, List.last(event), measurements, metadata})
    :ok
  end

  defmodule SnapshotSink do
    @moduledoc """
    A `Capstan.Sink` for the initial-snapshot marquees: it records EVERY delivered key into the
    shared append-only `(schema, table, canonical_pk)` ledger, tagged by which path delivered it
    (`:chunk` via `handle_snapshot/2`, `:stream` via `handle_transaction/1`) so a double-delivery
    is visible. The primary key is canonicalised with `Capstan.Snapshot.PrimaryKey.canonical/2`
    (the SAME reconciliation form the pipeline uses), so a text-protocol chunk read (`"5"`) and a
    binlog-decoded streamed change (`5`) key the same slot — otherwise a stale-vs-fresh comparison
    would silently miss.

    Config rides in `:persistent_term` (the marquees are `async: false`, and the coordinator's
    fixed registered name serialises snapshot pipelines, so exactly one config is live).
    """
    @behaviour Capstan.Sink

    alias Capstan.Change
    alias Capstan.Snapshot.PrimaryKey

    @key {__MODULE__, :config}

    @typedoc "The live sink config: the test pid, the ledger, and the PK/value column shape."
    @type config :: %{
            pid: pid(),
            ledger: :ets.tid(),
            pk_columns: [String.t()],
            pk_types: [PrimaryKey.pk_type()],
            value_column: String.t()
          }

    @doc "Configure the live sink."
    @spec configure(config()) :: :ok
    def configure(config), do: :persistent_term.put(@key, config)

    @doc "Erase the live sink config (an `on_exit` hook)."
    @spec clear() :: :ok
    def clear do
      :persistent_term.erase(@key)
      :ok
    end

    defp config, do: :persistent_term.get(@key)

    @impl Capstan.Sink
    @spec handle_transaction(Capstan.Transaction.t()) :: {:ok, Capstan.Position.t()}
    def handle_transaction(txn) do
      cfg = config()
      Enum.each(txn.changes, &record_stream(cfg, &1))
      send(cfg.pid, {:txn, txn.gtid})
      {:ok, txn.position}
    end

    @impl Capstan.Sink
    @spec handle_schema_change(Capstan.SchemaChange.t(), Capstan.Position.t()) :: :ok
    def handle_schema_change(schema_change, _position) do
      cfg = config()
      send(cfg.pid, {:schema_change, schema_change.schema, schema_change.table})
      :ok
    end

    @impl Capstan.Sink
    @spec handle_snapshot([Change.t()], Capstan.Snapshot.Meta.t()) :: :ok
    def handle_snapshot(changes, meta) do
      cfg = config()
      Enum.each(changes, &record_chunk(cfg, &1))
      send(cfg.pid, {:snapshot_chunk, meta.schema, meta.table, meta.chunk_seq, meta.final_chunk?})
      :ok
    end

    # A streamed :delete records the deleted key with the `:deleted` sentinel value (keyed on the
    # BEFORE-image PK); an :insert/:update records its AFTER-image PK + value.
    defp record_stream(cfg, %Change{op: :delete} = change),
      do: append(cfg, change, change.old_record, :deleted, :stream)

    defp record_stream(cfg, %Change{} = change),
      do: append(cfg, change, change.record, value_of(cfg, change.record), :stream)

    # A chunk row is a full after-image (upsert-by-PK).
    defp record_chunk(cfg, %Change{} = change),
      do: append(cfg, change, change.record, value_of(cfg, change.record), :chunk)

    defp append(cfg, change, pk_record, value, source) do
      pk = PrimaryKey.canonical(cfg.pk_types, Enum.map(cfg.pk_columns, &pk_record[&1]))
      entry = %{op: change.op, source: source, value: value, seq: next_seq()}
      :ets.insert(cfg.ledger, {{change.schema, change.table, pk}, entry})
      :ok
    end

    # The value column normalised to a string so a text-protocol chunk read and a binlog-decoded
    # streamed change compare equal (`"20"` == `to_string(20)`).
    defp value_of(cfg, record), do: to_string(record[cfg.value_column])

    defp next_seq, do: System.unique_integer([:monotonic, :positive])
  end

  defmodule DurableSnapshotStore do
    @moduledoc """
    A `Capstan.SnapshotStore` whose one durable `%Capstan.Snapshot.State{}` lives in an ETS cell
    the TEST owns (keyed by `{table, key}`), so it OUTLIVES the store process — the resume
    substrate. A pipeline restarted from a NEW store pointing at the same cell resumes the
    backfill from the persisted per-table PK cursor (never re-scans from zero). Mirrors
    `DurableStore` (the checkpoint sibling).
    """
    @behaviour Capstan.SnapshotStore

    alias Capstan.Snapshot.State

    @doc "Create the durable ETS backing."
    @spec new_table() :: :ets.tid()
    def new_table, do: :ets.new(:capstan_durable_snapshot_store, [:public, :set])

    @doc "Read the persisted `%State{}` directly (a marquee asserts the cursor advanced)."
    @spec current(:ets.tid(), term()) :: State.t() | nil
    def current(table, key) do
      case :ets.lookup(table, key) do
        [{^key, state}] -> state
        [] -> nil
      end
    end

    @spec start_link(keyword()) :: Agent.on_start()
    def start_link(opts) do
      table = Keyword.fetch!(opts, :table)
      key = Keyword.fetch!(opts, :key)
      Agent.start_link(fn -> {table, key} end)
    end

    @impl Capstan.SnapshotStore
    @spec read(Agent.agent()) :: {:ok, State.t() | nil}
    def read(store) do
      {table, key} = Agent.get(store, & &1)
      {:ok, current(table, key)}
    end

    @impl Capstan.SnapshotStore
    @spec write(Agent.agent(), State.t()) :: :ok
    def write(store, %State{} = state) do
      {table, key} = Agent.get(store, & &1)
      true = :ets.insert(table, {key, state})
      :ok
    end
  end
end

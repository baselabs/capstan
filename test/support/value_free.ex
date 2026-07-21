defmodule Capstan.ValueFree do
  @moduledoc """
  Rule-1 assertion helper (design § Invariant conformance) — the day-one enforcement
  layer (F11).

  A sentinel planted in an input — a row VALUE and the connection PASSWORD (Task 16); a
  DDL literal and a `ROWS_QUERY` statement (Task 17) — must appear in **no** log line and
  **no** telemetry metadata payload across every error/halt path reachable with the
  surface under test.

  The four Rule-1 vectors, and the channel each proves non-vacuous:

    1. **ROW VALUE** — a driver error embedding a column value, normalised by
       `Capstan.Error.from/1`; caught through the LOG + `connection.halt` telemetry.
    2. **PASSWORD** — a connect-failure reason carrying the password, same normaliser and
       channels.
    3. **DDL LITERAL** (live) — a self-committing DDL whose SQL embeds a literal
       (`DEFAULT '…'`). `Capstan.Assembler.classify_ddl` reduces it to schema/table/kind
       and DROPS the SQL; `%Capstan.SchemaChange{}` has no statement-text field. The
       sentinel must appear in no delivered `%SchemaChange{}`, no log, no telemetry.
    4. **ROWS_QUERY** (live) — with `binlog_rows_query_log_events = ON` the server emits a
       `ROWS_QUERY_LOG_EVENT` carrying the ORIGINAL statement SQL; `Capstan.Binlog.Decoder`
       discards the body (`{:rows_query, :discarded}`). The sentinel — planted as
       `UPPER('…')` so the delivered row value is uppercased and the lowercase sentinel
       lives ONLY in the SQL text — must appear in no delivered `%Transaction{}`/`%Change{}`,
       no log, no telemetry.

  Vectors 1–2 need only `Capstan.Error` and `Capstan.Telemetry`. Vectors 3–4 drive a REAL
  lib-owned pipeline (`Capstan.start_link/1` + `Capstan.CheckpointStore`-shaped store + a
  capturing sink) against the running `mysql-cdc-probe`, plant the statement on a query
  connection, and observe all three channels — the sink outputs, `ExUnit.CaptureLog`, and a
  `:telemetry` handler on every `[:capstan | _]` event — across BOTH the delivered and the
  sink-error halt paths.

  The helper is deliberately **adversarially non-vacuous**: un-redacting any one vector
  (retaining a raw exception message on `Capstan.Error`; carrying the SQL onto
  `%SchemaChange{}`; logging the discarded `ROWS_QUERY` body) makes that vector's assertion
  go RED through at least one channel. A helper that reported green over a real leak is the
  exact failure this guards.
  """

  import ExUnit.Assertions
  import ExUnit.CaptureLog
  require Logger

  alias Capstan.Error
  alias Capstan.Protocol.Command
  alias Capstan.Protocol.Handshake
  alias Capstan.Telemetry
  alias Capstan.ValueFree.CapturingSink
  alias Capstan.ValueFree.SeededStore

  # Distinctive, unlikely-to-collide sentinels: a match anywhere is a genuine leak, never
  # an incidental substring of some structural atom. The ROWS_QUERY sentinel is lowercase
  # and planted through `UPPER/1`, so the stored (delivered) row value is uppercase and the
  # lowercase sentinel exists ONLY in the ROWS_QUERY SQL text — isolating the SQL-text leak
  # from the legitimate row-value delivery.
  @row_value_sentinel "capstan_rule1_row_value_sentinel_9f3a1c7e"
  @password_sentinel "capstan_rule1_password_sentinel_4b8d2e60"
  @ddl_sentinel "capstan_rule1_ddl_sentinel_7c1e9a4d"
  @rows_query_sentinel "capstan_rule1_rowsquery_sentinel_2af6b8e1"

  # Live substrate (design § local substrate / mysql-cdc-probe). `root` is used because the
  # planted DDL/DML need CREATE/ALTER/INSERT, which the `capstan_sha2` replication user
  # (REPLICATION SLAVE, REPLICATION CLIENT, SELECT only) lacks; these are the
  # `Capstan.FixtureCapture` connect opts. NEVER restarts or duplicates the container.
  @live_host "127.0.0.1"
  @live_port 5633
  @live_connection [
    host: @live_host,
    port: @live_port,
    username: "root",
    password: "probe",
    ssl: false,
    auth_plugins: [:mysql_native_password],
    database: "probe_db"
  ]

  # Every capstan telemetry event (design § Events / telemetry) — the helper attaches to
  # ALL of them so a sentinel riding any payload is caught, not only the ones a given
  # path happens to emit.
  @capstan_events [
    [:capstan, :connection, :established],
    [:capstan, :connection, :halt],
    [:capstan, :transaction, :committed],
    [:capstan, :transaction, :filtered],
    [:capstan, :transaction, :skipped],
    [:capstan, :schema_change, :received],
    [:capstan, :gap, :detected]
  ]

  @doc "The row-value sentinel the row vector plants."
  @spec row_value_sentinel() :: String.t()
  def row_value_sentinel, do: @row_value_sentinel

  @doc "The password sentinel the password vector plants."
  @spec password_sentinel() :: String.t()
  def password_sentinel, do: @password_sentinel

  @doc "The DDL-literal sentinel the DDL vector plants."
  @spec ddl_sentinel() :: String.t()
  def ddl_sentinel, do: @ddl_sentinel

  @doc "The ROWS_QUERY sentinel the rows_query vector plants (lowercase; `UPPER/1`-wrapped)."
  @spec rows_query_sentinel() :: String.t()
  def rows_query_sentinel, do: @rows_query_sentinel

  @doc """
  Row-VALUE vector: a raw driver error whose message embeds a row value is normalised by
  `Capstan.Error.from/1` and surfaced through the log AND a `connection.halt` telemetry
  payload; the row value must appear in neither.

  Un-redacting `Capstan.Error.from/1` (retaining the raw exception message on `:reason`
  or `:shape`) makes this go RED.
  """
  @spec assert_row_value_free() :: :ok
  def assert_row_value_free do
    raw = %RuntimeError{message: "duplicate entry '#{@row_value_sentinel}' for key 'PRIMARY'"}
    refute_leaks(@row_value_sentinel, fn -> drive_error_paths(raw) end)
  end

  @doc """
  PASSWORD vector: a connect failure whose raw reason carries the connection password is
  normalised by `Capstan.Error.from/1` and surfaced through the log AND a
  `connection.halt` telemetry payload; the password must appear in neither.

  Un-redacting `Capstan.Error.from/1` (retaining the tuple payload) makes this go RED.
  """
  @spec assert_password_free() :: :ok
  def assert_password_free do
    raw = {:connect_failed, [host: ~c"db.internal", port: 3306, password: @password_sentinel]}
    refute_leaks(@password_sentinel, fn -> drive_error_paths(raw) end)
  end

  @doc """
  DDL-LITERAL vector (live): a self-committing `ALTER TABLE … DEFAULT '<sentinel>'` is
  streamed through a real lib-owned pipeline; the literal is dropped by
  `Capstan.Assembler.classify_ddl` and never reaches the delivered `%SchemaChange{}`, the
  log, or a telemetry payload — across BOTH the delivered and the sink-error halt paths.

  Un-redacting (carrying the SQL onto `%SchemaChange{}`) makes this go RED through the
  sink-output channel.
  """
  @spec assert_ddl_literal_free() :: :ok
  def assert_ddl_literal_free do
    plant = fn table ->
      ["ALTER TABLE #{table} ADD COLUMN vf_col VARCHAR(64) DEFAULT '#{@ddl_sentinel}'"]
    end

    setup = fn table ->
      [
        "DROP TABLE IF EXISTS #{table}",
        "CREATE TABLE #{table} (id INT PRIMARY KEY) ENGINE=InnoDB"
      ]
    end

    :ok = refute_live_leaks(@ddl_sentinel, setup, plant, :schema_change, :ok)

    :ok =
      refute_live_leaks(@ddl_sentinel, setup, plant, :schema_change, {:error, :vf_sink_rejected})

    :ok
  end

  @doc """
  ROWS_QUERY vector (live): with `binlog_rows_query_log_events = ON`, an INSERT whose SQL
  carries `UPPER('<sentinel>')` is streamed through a real lib-owned pipeline. The decoder
  discards the `ROWS_QUERY` body, so the lowercase sentinel (present only in the SQL text,
  never in the uppercased delivered row value) reaches no output, log, or telemetry —
  across BOTH the delivered and the sink-error halt paths.

  Un-redacting (logging the discarded `ROWS_QUERY` body in `Capstan.Binlog.Decoder`) makes
  this go RED through the log channel.
  """
  @spec assert_rows_query_free() :: :ok
  def assert_rows_query_free do
    # Non-vacuity of THIS vector rests on a `ROWS_QUERY_LOG_EVENT` actually being in the
    # stream — otherwise there is no SQL text to leak and the scan is trivially green. Two
    # guards make that hold: (1) `Capstan.start_link/1` runs `Config.check_preconditions`,
    # which REFUSES to start unless `binlog_format=ROW`, so the vector's pipeline cannot
    # even start on a substrate where a ROWS_QUERY event would be absent (the test fails,
    # it does not pass vacuously); (2) the `SET SESSION binlog_rows_query_log_events = ON`
    # below runs through `run!/2`, which raises if the server rejects it. The insert is a
    # normal ROW-image write, so `refute_live_leaks`'s `refute outputs == []` guard also
    # confirms the transaction was delivered — the SQL text is the only channel the SQL
    # sentinel could ride, and it is discarded (`{:rows_query, :discarded}`).
    plant = fn table ->
      [
        "SET SESSION binlog_rows_query_log_events = ON",
        "INSERT INTO #{table} (id, name) VALUES (1, UPPER('#{@rows_query_sentinel}'))"
      ]
    end

    setup = fn table ->
      [
        "DROP TABLE IF EXISTS #{table}",
        "CREATE TABLE #{table} (id INT PRIMARY KEY, name VARCHAR(64)) ENGINE=InnoDB"
      ]
    end

    :ok = refute_live_leaks(@rows_query_sentinel, setup, plant, :transaction, :ok)

    :ok =
      refute_live_leaks(
        @rows_query_sentinel,
        setup,
        plant,
        :transaction,
        {:error, :vf_sink_rejected}
      )

    :ok
  end

  @doc """
  Run `fun` while capturing all log output and all `[:capstan | _]` telemetry metadata,
  then assert `sentinel` appears in neither. Metadata is deep-scanned (keys AND values,
  via `inspect/1`), so a sentinel nested anywhere in a payload is caught.
  """
  @spec refute_leaks(String.t(), (-> any())) :: :ok
  def refute_leaks(sentinel, fun) when is_binary(sentinel) and is_function(fun, 0) do
    {:ok, agent} = Agent.start_link(fn -> [] end)
    handler_id = {__MODULE__, make_ref()}
    :ok = :telemetry.attach_many(handler_id, @capstan_events, &__MODULE__.__collect__/4, agent)

    log =
      try do
        capture_log(fun)
      after
        :telemetry.detach(handler_id)
      end

    metas = Agent.get(agent, & &1)
    Agent.stop(agent)

    refute String.contains?(log, sentinel),
           "Rule 1 violation: sentinel #{inspect(sentinel)} leaked into a LOG line:\n#{log}"

    for meta <- metas do
      refute String.contains?(inspect(meta), sentinel),
             "Rule 1 violation: sentinel #{inspect(sentinel)} leaked into telemetry metadata " <>
               inspect(meta)
    end

    :ok
  end

  @doc false
  # A module-function handler (not a local capture) keeps :telemetry from logging a
  # per-attach performance warning; the accumulator agent rides in `config`.
  @spec __collect__([atom(), ...], map(), map(), Agent.agent()) :: :ok
  def __collect__(_event, _measurements, metadata, agent) do
    Agent.update(agent, &[metadata | &1])
  end

  # The error/halt paths reachable with only Error + Telemetry: normalise the raw reason,
  # then surface the normalised (value-free) error through BOTH channels a real halt uses
  # — the crash/error LOG and the connection.halt telemetry payload. If the normaliser
  # scrubs correctly, neither channel carries the sentinel.
  defp drive_error_paths(raw) do
    error = Error.from(raw)
    Logger.error(Exception.message(error))
    Logger.error(inspect(error))
    Telemetry.event([:capstan, :connection, :halt], %{}, %{reason: error.reason})
  end

  ## ---------------------------------------------------------------------------
  ## live pipeline (vectors 3 + 4)
  ## ---------------------------------------------------------------------------

  # Drive a REAL lib-owned pipeline over `mysql-cdc-probe` and assert `sentinel` reaches no
  # sink output, no log line, and no telemetry payload.
  #
  #   1. On a query connection, run `setup.(table)` (no sentinel), then read the current
  #      `@@gtid_executed` — the resume watermark. The seeded checkpoint store resumes the
  #      pipeline from exactly here, so only the sentinel-bearing plant streams (an empty
  #      checkpoint would resume from GTID 1, which this purged substrate refuses
  #      `:data_gap`; `Capstan.start_link/1` refuses an explicit position, so a store seeded
  #      to the live watermark is the fail-closed-preserving way to resume from "now").
  #   2. Start the pipeline, run `plant.(table)` (the sentinel), and wait for the sink.
  #   3. Scan the captured sink outputs, log, and telemetry for the sentinel.
  #
  # `output_kind` (`:schema_change` | `:transaction`) is the delivered output's callback and
  # its terminal telemetry event; `sink_result` (`:ok` | `{:error, reason}`) selects the
  # delivered vs. the sink-error halt path — the sink stashes the output BEFORE returning
  # either, so both paths are scanned.
  @spec refute_live_leaks(
          String.t(),
          (String.t() -> [String.t()]),
          (String.t() -> [String.t()]),
          :schema_change | :transaction,
          :ok | {:error, atom()}
        ) :: :ok
  defp refute_live_leaks(sentinel, setup, plant, output_kind, sink_result) do
    # A fixed per-vector name (not unique): the setup's `DROP TABLE IF EXISTS` makes each
    # run idempotent, so repeated live runs never accumulate leftover tables on the
    # substrate (the `Capstan.FixtureCapture` pattern). The runs are sequential (async:
    # false), so there is no concurrent collision.
    table = "vf_#{output_kind}"
    test_pid = self()
    {:ok, collector} = Agent.start_link(fn -> %{outputs: [], telemetry: []} end)

    qconn = live_connect!()

    checkpoint =
      try do
        Enum.each(setup.(table), &run!(qconn, &1))
        read_gtid_executed!(qconn)
      rescue
        e ->
          close!(qconn)
          Agent.stop(collector)
          reraise(e, __STACKTRACE__)
      end

    CapturingSink.configure(%{report_to: test_pid, collector: collector, result: sink_result})

    handler_id = {__MODULE__, make_ref()}

    :ok =
      :telemetry.attach_many(
        handler_id,
        @capstan_events,
        &__MODULE__.__collect_live__/4,
        {collector, test_pid}
      )

    log =
      try do
        capture_log(fn ->
          run_live_pipeline(qconn, checkpoint, plant.(table), output_kind, sink_result)
        end)
      after
        :telemetry.detach(handler_id)
        close!(qconn)
        CapturingSink.clear()
      end

    %{outputs: outputs, telemetry: metas} = Agent.get(collector, & &1)
    Agent.stop(collector)

    # The plant MUST have been delivered — an empty output set would make the scan
    # vacuously green (the leak channel was never exercised).
    refute outputs == [],
           "Rule 1 live harness for #{inspect(sentinel)} delivered no output — the pipeline " <>
             "never processed the planted statement, so the scan proves nothing."

    refute String.contains?(log, sentinel),
           "Rule 1 violation: sentinel #{inspect(sentinel)} leaked into a LOG line:\n#{log}"

    refute String.contains?(inspect(outputs), sentinel),
           "Rule 1 violation: sentinel #{inspect(sentinel)} leaked into a delivered sink output " <>
             inspect(outputs)

    for meta <- metas do
      refute String.contains?(inspect(meta), sentinel),
             "Rule 1 violation: sentinel #{inspect(sentinel)} leaked into telemetry metadata " <>
               inspect(meta)
    end

    :ok
  end

  defp run_live_pipeline(qconn, checkpoint, plant_sql, output_kind, sink_result) do
    {:ok, sup} = Capstan.start_link(live_pipeline_opts(checkpoint))

    try do
      Enum.each(plant_sql, &run!(qconn, &1))
      assert_receive :vf_delivered, 20_000
      await_terminal(output_kind, sink_result)
    after
      Capstan.stop(sup)
    end
  end

  # On the delivered path the AssemblerServer emits the terminal telemetry AFTER the sink
  # returns `{:ok, _}`; await it so it is collected before the scan reads the agent. The
  # sink-error path halts without terminal telemetry, so there is nothing further to await.
  defp await_terminal(:schema_change, :ok),
    do: assert_receive({:vf_telemetry, [:capstan, :schema_change, :received]}, 5_000)

  defp await_terminal(:transaction, :ok),
    do: assert_receive({:vf_telemetry, [:capstan, :transaction, :committed]}, 5_000)

  defp await_terminal(_output_kind, {:error, _reason}), do: :ok

  defp live_pipeline_opts(checkpoint) do
    [
      connection: @live_connection,
      server_id: 6000 + rem(System.unique_integer([:positive]), 2000),
      sink: CapturingSink,
      checkpoint_store: [module: SeededStore, options: [gtid_set: checkpoint]],
      max_command_retries: 5,
      start_position: :checkpoint,
      tables: :all
    ]
  end

  @doc false
  @spec __collect_live__([atom(), ...], map(), map(), {Agent.agent(), pid()}) :: :ok
  def __collect_live__(event, _measurements, metadata, {collector, pid}) do
    Agent.update(collector, fn state -> %{state | telemetry: [metadata | state.telemetry]} end)
    send(pid, {:vf_telemetry, event})
    :ok
  end

  ## ---------------------------------------------------------------------------
  ## live query connection (Capstan.FixtureCapture precedent)
  ## ---------------------------------------------------------------------------

  defp live_connect! do
    host = String.to_charlist(@live_host)
    {:ok, raw} = :gen_tcp.connect(host, @live_port, [:binary, active: false], 20_000)

    # `Handshake.connect/2` takes an already-connected socket, so it has no `:port`
    # `connect_opt` — dropping it also loosens the literal's type so the call type-checks.
    case Handshake.connect({:gen_tcp, raw}, Keyword.delete(@live_connection, :port)) do
      {:ok, %{socket: socket}} -> socket
      {:error, reason} -> raise "capstan value_free: live handshake failed #{inspect(reason)}"
    end
  end

  defp run!(socket, sql) do
    case Command.query(socket, sql) do
      :ok -> :ok
      {:ok, _rows} -> :ok
      {:error, reason} -> raise "capstan value_free: query failed #{inspect(reason)}: #{sql}"
    end
  end

  defp read_gtid_executed!(socket) do
    case Command.query(socket, "SELECT @@global.gtid_executed") do
      {:ok, [[value]]} when is_binary(value) -> value
      other -> raise "capstan value_free: unexpected @@gtid_executed response #{inspect(other)}"
    end
  end

  defp close!({:gen_tcp, sock}), do: :gen_tcp.close(sock)
  defp close!({:ssl, sock}), do: :ssl.close(sock)

  ## ---------------------------------------------------------------------------
  ## live pipeline scaffolding — a capturing sink + a watermark-seeded checkpoint store
  ## ---------------------------------------------------------------------------

  defmodule CapturingSink do
    @moduledoc false
    # A `Capstan.Sink` that stashes every delivered output in a collector agent and signals
    # the test process, returning a configured result. The sink runs inside the
    # AssemblerServer process, so its config (`report_to` pid, `collector` agent, `result`)
    # rides in `:persistent_term` — the live tests are `async: false`, so there is exactly
    # one live config. The output is stashed BEFORE the result is returned, so the
    # sink-error halt path is scanned too.
    @behaviour Capstan.Sink

    @key {__MODULE__, :config}

    def configure(config), do: :persistent_term.put(@key, config)
    def clear, do: :persistent_term.erase(@key)
    defp config, do: :persistent_term.get(@key)

    @impl Capstan.Sink
    def handle_transaction(transaction) do
      cfg = config()
      Agent.update(cfg.collector, fn s -> %{s | outputs: [transaction | s.outputs]} end)
      send(cfg.report_to, :vf_delivered)

      case cfg.result do
        :ok -> {:ok, transaction.position}
        {:error, reason} -> {:error, reason}
      end
    end

    @impl Capstan.Sink
    def handle_schema_change(schema_change, _position) do
      cfg = config()
      Agent.update(cfg.collector, fn s -> %{s | outputs: [schema_change | s.outputs]} end)
      send(cfg.report_to, :vf_delivered)

      case cfg.result do
        :ok -> :ok
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defmodule SeededStore do
    @moduledoc false
    # A `Capstan.CheckpointStore` seeded to a starting `gtid_set` string, so a lib-owned
    # pipeline started via `Capstan.start_link/1` resumes from a chosen live watermark
    # rather than from empty (which this purged substrate refuses `:data_gap`). Same
    # read/1 + write/2 semantics as `Capstan.CheckpointStore.InMemory`, but `start_link/1`
    # consumes its opts for the initial `:gtid_set` (InMemory forwards opts to
    # `Agent.start_link` as GenServer options) — `store_spec` passes no GenServer opts and
    # captures the returned pid, so the signatures stay compatible.
    @behaviour Capstan.CheckpointStore

    @spec start_link(keyword()) :: Agent.on_start()
    def start_link(opts) do
      Agent.start_link(fn -> Keyword.get(opts, :gtid_set, "") end)
    end

    @impl Capstan.CheckpointStore
    def read(store), do: {:ok, Agent.get(store, & &1)}

    @impl Capstan.CheckpointStore
    def write(store, gtid_set) when is_binary(gtid_set) do
      Agent.update(store, fn _current -> gtid_set end)
    end
  end
end

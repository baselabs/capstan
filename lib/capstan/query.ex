defmodule Capstan.Query do
  @moduledoc """
  A `COM_QUERY`-only connection for the initial-snapshot path (C2).

  `Capstan.Connection` is binlog-dump-only — once it issues `COM_BINLOG_DUMP_GTID` the
  socket carries the replication stream and cannot run a `SELECT`. The snapshot needs a
  SEPARATE connection to read `information_schema`, capture each chunk's exact GTID under a
  brief `LOCK TABLES … READ`, and page the chunk rows. `Capstan.Query` is that connection: it
  reuses the C1 handshake / auth / TLS stack (`Capstan.Protocol.Handshake`) and the C1
  text-resultset decoder (`Capstan.Protocol.Command`), and exposes the query primitives the
  `ChunkReader`, `PrimaryKey` introspection, and the bootstrap `P0` read all build on.

  It owns no process. A `%Capstan.Query{}` is a connection HANDLE — an authenticated passive
  socket plus the pinned source identity — that a caller (the `ChunkReader`, the bootstrap)
  drives synchronously, exactly as `Capstan.Config.check_preconditions/1` drives a socket.

  ## Source identity (design Ch8)

  A query connection that silently (re)connects to a DIFFERENT replica mid-backfill corrupts
  the exact-`G` pairing the whole snapshot rests on (the chunk's captured GTID would name a
  position on a server the chunk rows did not come from). So on EVERY (re)establish the
  `@@server_uuid` is read and compared to the pinned value; a mismatch fails closed
  `:snapshot_source_mismatch`. The endpoint + `@@server_uuid` are pinned for the connection's
  life: the first `establish/1` sets the pin (or verifies it against an
  `:expected_server_uuid` — the STREAM connection's identity, supplied by the bootstrap), and
  every `reestablish/1` re-verifies against it. A reconnect that lands on another replica — a
  VIP failover, a moved DNS record — is caught even though the endpoint string is unchanged.

  ## Preconditions (ADR-0002, extended)

  `establish/1` reuses `Capstan.Config.check_preconditions/1` on the query socket: the five
  binlog variables gate the query connection exactly as they gate the stream, because
  `binlog_row_image = FULL` is a HARD reconciliation dependency (a partial after-image would
  make a stream-delivered change unable to stand in for a suppressed chunk row). A precondition
  VIOLATION is permanent (reconnecting cannot cure a mis-configured server), so it surfaces its
  distinct reason without spending the retry budget — the same posture C1 takes.

  ## Retry budget

  A connect/auth failure fails closed `:snapshot_query_connect_failed`. Transient faults are
  budgeted through `Capstan.CheckpointStore.retry_decision/2` + `permanent_reason?/1` — the
  shared counter the checkpoint-store and connect-read sites use, so the semantics cannot
  drift. A permanent connect reason (`:config_invalid`) halts immediately.

  ## Rule 1

  The connection password appears in NO returned term, error, or log emitted here. The struct
  carries the `connection` keyword (which holds the password) only to reconnect, and the
  derived `Inspect` renders ONLY the structural identity (`endpoint`, `server_uuid`), never
  `connection`. Every error is a value-free atom, scrubbed through `Capstan.Error.from/1`; a
  raised transport error is discarded (it could reference the SQL or a value) in favour of a
  bare reason. No SQL is ever logged.
  """

  alias Capstan.CheckpointStore
  alias Capstan.Config
  alias Capstan.Error
  alias Capstan.Protocol.Command
  alias Capstan.Protocol.Handshake
  alias Capstan.Protocol.Packet

  @server_uuid_query "SELECT @@server_uuid"
  @default_connect_timeout 20_000

  # Precondition VIOLATIONS are permanent — reconnecting cannot cure a mis-configured server —
  # so they surface their distinct reason rather than spend the budget (mirrors
  # `Capstan.Connection`'s `@precondition_halts`).
  @precondition_halts [
    :binlog_format_not_row,
    :binlog_row_image_not_full,
    :binlog_row_metadata_not_full,
    :binlog_row_value_options_not_empty,
    :gtid_mode_not_on,
    :precondition_query_failed
  ]

  @derive {Inspect, only: [:endpoint, :server_uuid]}
  defstruct [:socket, :connection, :connect_fun, :max_retries, :endpoint, :server_uuid]

  @typedoc "The pinned `{host, port}` the connection resolves to for its life."
  @type endpoint :: {charlist(), :inet.port_number()}

  @typedoc """
  The socket-open + auth function: mirrors `Capstan.Connection`'s `:connect_fun` shape so a
  test can inject a scripted transport. Default: `default_connect/1` (gen_tcp + Handshake).
  """
  @type connect_fun :: (keyword() -> {:ok, Packet.socket(), map()} | {:error, term()})

  @type t :: %__MODULE__{
          socket: Packet.socket(),
          connection: keyword(),
          connect_fun: connect_fun(),
          max_retries: non_neg_integer(),
          endpoint: endpoint(),
          server_uuid: String.t()
        }

  @typedoc """
  Options for `establish/1`: `:connection` (the C1 connection keyword), optional `:connect_fun`
  (default `default_connect/1`), optional `:max_command_retries` (default
  `CheckpointStore.default_max_retries/0`), and optional `:expected_server_uuid` (the stream
  connection's identity to verify against at the first connect, design Ch8).
  """
  @type establish_opt ::
          {:connection, keyword()}
          | {:connect_fun, connect_fun()}
          | {:max_command_retries, non_neg_integer()}
          | {:expected_server_uuid, String.t()}

  ## ---------------------------------------------------------------------------
  ## establish / reestablish
  ## ---------------------------------------------------------------------------

  @doc """
  Opens an authenticated `COM_QUERY` connection, checks the ADR-0002 preconditions, reads
  `@@server_uuid`, and pins the endpoint + identity for the connection's life.

  With `:expected_server_uuid` set (the stream connection's identity), the observed uuid is
  verified against it at connect (design Ch8); otherwise the first read SETS the pin. Returns
  `{:ok, t()}` or a value-free `{:error, reason}` — `:snapshot_source_mismatch` on an identity
  mismatch, a distinct precondition reason on a bad substrate, or `:snapshot_query_connect_failed`
  when the connect/auth budget is exhausted.
  """
  @spec establish([establish_opt()]) :: {:ok, t()} | {:error, atom()}
  def establish(opts) when is_list(opts) do
    do_establish(opts, Keyword.get(opts, :expected_server_uuid))
  end

  @doc """
  Reconnects, RE-verifying `@@server_uuid` against the pinned value (design Ch8).

  A reconnect that lands on a different replica halts `:snapshot_source_mismatch`; otherwise a
  fresh authenticated handle to the SAME source is returned.
  """
  @spec reestablish(t()) :: {:ok, t()} | {:error, atom()}
  def reestablish(%__MODULE__{server_uuid: pinned} = query) do
    do_establish(base_opts(query), pinned)
  end

  @doc """
  Runs `sql` as a `COM_QUERY` on the pinned socket.

  Returns `{:ok, rows}` for a text resultset (each row a list of column values, `nil` for a
  NULL column), `{:ok, []}` for a statement with no resultset (`SET`, `LOCK TABLES`,
  `UNLOCK TABLES`), or a value-free `{:error, reason}` (scrubbed through `Capstan.Error.from/1`;
  a raised transport error becomes `:transport`). The SQL is never logged — a chunk `WHERE`
  literal can carry a row value (Rule 1).
  """
  @spec query(t(), binary()) :: {:ok, [Command.row()]} | {:error, atom()}
  def query(%__MODULE__{socket: socket}, sql) when is_binary(sql) do
    case Command.query(socket, sql) do
      :ok -> {:ok, []}
      {:ok, rows} -> {:ok, rows}
      {:error, reason} -> {:error, Error.from(reason).reason}
    end
  rescue
    _exception -> {:error, :transport}
  catch
    _kind, _reason -> {:error, :transport}
  end

  @doc "Closes the pinned socket. Idempotent."
  @spec close(t()) :: :ok
  def close(%__MODULE__{socket: socket}), do: close_socket(socket)

  @doc "The pinned `@@server_uuid` (structural identity — safe to surface, never a row value)."
  @spec server_uuid(t()) :: String.t()
  def server_uuid(%__MODULE__{server_uuid: uuid}), do: uuid

  @doc "The pinned `{host, port}` endpoint."
  @spec endpoint(t()) :: endpoint()
  def endpoint(%__MODULE__{endpoint: endpoint}), do: endpoint

  @doc """
  The pure source-identity check: `:ok` when there is no pin yet (the first establish) or the
  observed uuid matches the pin, else `{:error, :snapshot_source_mismatch}` (design Ch8).

  Exposed so the reconnect re-check is provably RED-capable in isolation.
  """
  @spec verify_source_identity(String.t() | nil, String.t()) ::
          :ok | {:error, :snapshot_source_mismatch}
  def verify_source_identity(nil, _observed), do: :ok
  def verify_source_identity(pinned, pinned), do: :ok
  def verify_source_identity(_pinned, _observed), do: {:error, :snapshot_source_mismatch}

  @doc """
  The default socket-open + auth function (gen_tcp + `Handshake.connect/2`), mirroring the C1
  `Capstan.Connection` connect shape. Returns `{:ok, socket, handshake_info}` or
  `{:error, reason}`. Public so a live test can drive the real transport.
  """
  @spec default_connect(keyword()) :: {:ok, Packet.socket(), map()} | {:error, term()}
  def default_connect(connection) do
    host = connection |> Keyword.fetch!(:host) |> normalize_host()
    port = Keyword.fetch!(connection, :port)
    timeout = Keyword.get(connection, :timeout, @default_connect_timeout)

    case :gen_tcp.connect(host, port, [:binary, active: false, keepalive: true], timeout) do
      {:ok, raw} -> authenticate(raw, connection)
      {:error, reason} -> {:error, reason}
    end
  end

  ## ---------------------------------------------------------------------------
  ## establish internals
  ## ---------------------------------------------------------------------------

  defp do_establish(opts, pinned_uuid) do
    ctx = %{
      connection: Keyword.get(opts, :connection, []),
      connect_fun: Keyword.get(opts, :connect_fun, &default_connect/1),
      max_retries: Keyword.get(opts, :max_command_retries, CheckpointStore.default_max_retries()),
      pinned_uuid: pinned_uuid
    }

    attempt_establish(ctx, 0)
  end

  # Budgeted retry over the transient connect/read faults; a permanent outcome (precondition
  # violation, source mismatch, a permanent connect reason) breaks out immediately. The counter
  # is `CheckpointStore.retry_decision/2` — never re-derived here.
  defp attempt_establish(ctx, attempt) do
    case try_establish(ctx) do
      {:ok, query} ->
        {:ok, query}

      {:permanent, reason} ->
        {:error, reason}

      {:transient, _reason} ->
        case CheckpointStore.retry_decision(attempt, ctx.max_retries) do
          :retry -> attempt_establish(ctx, attempt + 1)
          :halt -> {:error, :snapshot_query_connect_failed}
        end
    end
  end

  defp try_establish(ctx) do
    case safe_connect(ctx) do
      {:ok, socket} -> post_connect(socket, ctx)
      {:error, reason} -> classify_connect_error(reason)
    end
  end

  # `default_connect/1` returns tuples; a scripted connect_fun could raise. Normalise a raise to
  # a transient error so the budget still governs it (the raised term is discarded — Rule 1).
  defp safe_connect(ctx) do
    case ctx.connect_fun.(ctx.connection) do
      {:ok, socket, _info} -> {:ok, socket}
      {:error, reason} -> {:error, reason}
    end
  rescue
    _exception -> {:error, :connect_raised}
  catch
    _kind, _reason -> {:error, :connect_raised}
  end

  # A connect/auth failure always fails closed `:snapshot_query_connect_failed`; only a
  # permanent reason (`:config_invalid`) skips the budget.
  defp classify_connect_error(reason) do
    if CheckpointStore.permanent_reason?(reason),
      do: {:permanent, :snapshot_query_connect_failed},
      else: {:transient, reason}
  end

  # `Command.query` -> `Packet.read_packet` raises on a transport error; catch it here (the only
  # scope holding the open socket), close the socket (no fd leak), and treat it as transient.
  defp post_connect(socket, ctx) do
    outcome =
      with :ok <- precheck(socket),
           {:ok, uuid} <- fetch_server_uuid(socket),
           :ok <- identity(ctx.pinned_uuid, uuid) do
        {:ok, build(socket, ctx, uuid)}
      end

    finish(outcome, socket)
  rescue
    _exception ->
      close_socket(socket)
      {:transient, :establish_raised}
  catch
    _kind, _reason ->
      close_socket(socket)
      {:transient, :establish_raised}
  end

  defp finish({:ok, _query} = ok, _socket), do: ok

  defp finish(failure, socket) do
    close_socket(socket)
    failure
  end

  defp precheck(socket) do
    case Config.check_preconditions(socket) do
      :ok -> :ok
      {:error, reason} when reason in @precondition_halts -> {:permanent, reason}
      {:error, _other} -> {:transient, :precondition_read_failed}
    end
  end

  defp fetch_server_uuid(socket) do
    case Command.query(socket, @server_uuid_query) do
      {:ok, [[uuid]]} when is_binary(uuid) -> {:ok, uuid}
      _other -> {:transient, :server_uuid_read_failed}
    end
  end

  defp identity(pinned_uuid, observed_uuid) do
    case verify_source_identity(pinned_uuid, observed_uuid) do
      :ok -> :ok
      {:error, reason} -> {:permanent, reason}
    end
  end

  defp build(socket, ctx, uuid) do
    %__MODULE__{
      socket: socket,
      connection: ctx.connection,
      connect_fun: ctx.connect_fun,
      max_retries: ctx.max_retries,
      endpoint: endpoint_of(ctx.connection),
      server_uuid: uuid
    }
  end

  defp base_opts(%__MODULE__{} = query) do
    [
      connection: query.connection,
      connect_fun: query.connect_fun,
      max_command_retries: query.max_retries
    ]
  end

  defp endpoint_of(connection) do
    host = connection |> Keyword.get(:host) |> normalize_host()
    {host, Keyword.get(connection, :port)}
  end

  ## ---------------------------------------------------------------------------
  ## transport helpers (mirror the C1 connect shape)
  ## ---------------------------------------------------------------------------

  defp authenticate(raw, connection) do
    # `Handshake.connect/2` owns the socket lifetime end-to-end: on `{:error, _}` it has already
    # closed whatever socket it held (the raw `{:gen_tcp, _}` or the upgraded `{:ssl, _}`), so we
    # never close on the error branch (a double-free / orphaned :ssl process). Mirrors
    # `Capstan.Connection.authenticate/2`.
    case Handshake.connect({:gen_tcp, raw}, connection) do
      {:ok, %{socket: socket} = info} -> {:ok, socket, info}
      {:error, reason} -> {:error, reason}
    end
  end

  defp normalize_host(host) when is_binary(host), do: String.to_charlist(host)
  defp normalize_host(host) when is_list(host), do: host

  defp close_socket({:gen_tcp, sock}), do: :gen_tcp.close(sock)
  defp close_socket({:ssl, sock}), do: :ssl.close(sock)
end

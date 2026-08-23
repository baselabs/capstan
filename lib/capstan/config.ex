defmodule Capstan.Config do
  @moduledoc """
  Option validation and the fail-closed server-precondition gate.

  Two clearly separated responsibilities:

    * `validate/1` is **pure** — it normalises the user's `start_link` options and
      refuses a mis-shaped or unsafe configuration before any socket is opened.
    * `check_preconditions/1` issues **one** `COM_QUERY` on an already-authenticated
      socket and refuses to start unless the source's binlog is configured for
      lossless row-based CDC.

  Two additive C2 helpers share this fail-closed posture: `validate_snapshot/1` (pure —
  normalises the `:snapshot` block, or `{:ok, nil}` when absent so the pure-C1 path is
  byte-for-byte unchanged) and `read_server_uuid/1` (the source-identity read reused across
  BOTH connections, design Q-src / Ch8).

  ## Server preconditions (ADR-0002)

  `check_preconditions/1` reads five global variables in a single query and refuses
  with a DISTINCT reason per violation — degraded row decoding silently guesses
  column identity, so the gate fails closed rather than proceed:

    * `binlog_format` must be `ROW` — else `:binlog_format_not_row`
    * `binlog_row_image` must be `FULL` — else `:binlog_row_image_not_full`
    * `binlog_row_metadata` must be `FULL` — else `:binlog_row_metadata_not_full`
    * `binlog_row_value_options` must be empty (`""` = full JSON, not `PARTIAL_JSON`)
      — else `:binlog_row_value_options_not_empty`
    * `gtid_mode` must be `ON` — else `:gtid_mode_not_on`

  `binlog_transaction_compression` is deliberately NOT gated: compression is
  source-unilateral (a consumer cannot opt out; MySQL 8.0.20+) and capstan
  CONSUMES compressed transactions — the pure-Elixir zstd decoder inflates each
  `TRANSACTION_PAYLOAD` event (ADR-0011's consume arm; `Capstan.Zstd`). A
  malformed or non-ZSTD payload still halts fail-closed at decode time.

  MySQL simple-query results are **all text strings**, so every value is compared as
  text against the expected literal and never coerced to a typed term — an empty
  `binlog_row_value_options` arrives as `""`, not `nil` or `0`.

  ## TLS verification posture (ADR-0002)

  `ssl` defaults **true**. Peer verification is an explicit operator choice, never a
  silent default: with TLS on, `ssl_opts` must carry EITHER a `cacertfile`/`cacerts`
  OR an explicit `verify:`. Given neither, `validate/1` fails closed with
  `:tls_verification_unspecified` rather than let OTP's `verify_peer` default select
  a posture nobody chose. This mirrors the guard `Capstan.Protocol.Handshake` applies
  at connect time, moved to config time so a bad TLS configuration is refused before
  any socket is opened.

  **Authenticated TLS against MySQL's auto-generated certificate.** A `cacertfile`
  alone drives `verify: :verify_peer`, but MySQL's auto-generated server certificate
  is self-signed with CN `…Auto_Generated_Server_Certificate` and no SAN, so
  `verify_peer`'s hostname check fails against an address such as `127.0.0.1`. An
  operator taking the authenticated route must ALSO pass
  `server_name_indication: :disable` in `ssl_opts` — the chain is verified, the
  hostname is not (VERIFY_CA semantics). `validate/1` does NOT inject this: that would
  silently weaken every `cacertfile` user, so the operator supplies it deliberately.
  """

  alias Capstan.Protocol.Command
  alias Capstan.Protocol.Packet

  @default_max_command_retries 5
  # Streaming liveness (usage-rules.md) — the same defaults Capstan.Connection falls back to
  # on direct wiring; both constructors must apply identical values before comparing.
  @default_reconnect_backoff 1_000
  @default_heartbeat_period_ms 15_000
  @default_stream_timeout_ms 60_000
  # The reconnect + liveness timers schedule via Process.send_after/3, which raises past
  # the 2^32-1 ms ceiling — an over-ceiling value is a config refusal here, never a
  # later timer crash in the connection.
  @max_timer_ms 4_294_967_295
  @default_snapshot_chunk_size 4096
  @known_auth_plugins [:caching_sha2_password, :mysql_native_password]

  @precondition_query "SELECT @@global.binlog_format, @@global.binlog_row_image, " <>
                        "@@global.binlog_row_metadata, @@global.binlog_row_value_options, " <>
                        "@@global.gtid_mode"

  @server_uuid_query "SELECT @@server_uuid"

  # The public `Capstan.start_link/1` option surface. `:id` is `child_spec/1`'s (it is
  # consumed from the same forwarded opts, so it must pass). Anything outside this set is
  # refused `:unknown_option` — a typo'd key (`stream_timeout:` for `stream_timeout_ms:`)
  # must never silently fall back to a default.
  @start_link_options [
    :connection,
    :server_id,
    :sink,
    :checkpoint_store,
    :start_position,
    :tables,
    :snapshot,
    :max_command_retries,
    :reconnect_backoff,
    :heartbeat_period_ms,
    :stream_timeout_ms,
    :xa,
    :max_prepared_transactions,
    :batch,
    :id
  ]

  # The validated `connection:` block's keys (the block is REBUILT from exactly these, so
  # an extra key has no passthrough today — refusing makes that drop loud).
  @connection_options [
    :host,
    :port,
    :username,
    :password,
    :database,
    :ssl,
    :ssl_opts,
    :auth_plugins
  ]

  # The validated `snapshot:` block's keys; the store blocks (`checkpoint_store:`,
  # snapshot `store:`) share `[:module, :options]` — `options` is implementation
  # passthrough, never introspected.
  @snapshot_options [:tables, :store, :chunk_size]
  @store_block_options [:module, :options]

  @typedoc "The normalised configuration `validate/1` returns on success."
  @type t :: %{
          connection: keyword(),
          server_id: pos_integer(),
          max_command_retries: non_neg_integer(),
          reconnect_backoff: pos_integer(),
          heartbeat_period_ms: pos_integer(),
          stream_timeout_ms: pos_integer(),
          xa: :refuse | :track,
          max_prepared_transactions: pos_integer(),
          batch: batch_config() | nil
        }

  @typedoc "A value-free option-validation refusal."
  @type validation_error ::
          :config_invalid
          | :server_id_required
          | :tls_verification_unspecified
          | :invalid_liveness_config
          | :unknown_option

  @typedoc "The normalised C3 batching configuration, or `nil` when absent (no batching)."
  @type batch_config :: %{
          max_transactions: pos_integer(),
          flush_ms: pos_integer(),
          mode: :lib_owned | :sink_owned
        }

  @typedoc "A value-free precondition-gate refusal (ADR-0002)."
  @type precondition_error ::
          :binlog_format_not_row
          | :binlog_row_image_not_full
          | :binlog_row_metadata_not_full
          | :binlog_row_value_options_not_empty
          | :gtid_mode_not_on
          | :precondition_query_failed

  @typedoc """
  The normalised initial-snapshot configuration (C2), or `nil` when `:snapshot` is absent
  (pure C1). `tables` is the snapshot set (a concrete `{schema, table}` list, or `:all` when
  it defaults to an `:all` capture — resolved to a concrete list at bootstrap). `store` is the
  durable snapshot store `{module, start_link_options}`. `chunk_size` bounds the brief lock's
  hold + buffered memory.
  """
  @type snapshot_config :: %{
          tables: [{String.t(), String.t()}] | :all,
          store: {module(), keyword()},
          chunk_size: pos_integer()
        }

  @doc """
  Validates raw `start_link` options into a normalised config map, or returns a
  value-free error.

  Refuses: `:server_id_required` (missing or non-positive `server_id`),
  `:tls_verification_unspecified` (`ssl: true` with no CA source and no explicit
  `verify:`), `:invalid_liveness_config` (`stream_timeout_ms <= heartbeat_period_ms`,
  compared after defaults are applied — a window at or below the heartbeat interval would
  false-drop a healthy idle stream), `:unknown_option` (a key outside the documented
  surface — top level, `connection:`, `snapshot:`, or a store block — never silently
  defaulted), and `:config_invalid` (any other missing or mis-shaped option, including a
  liveness value beyond the schedulable timer ceiling).
  Defaults applied: `ssl` true, `ssl_opts` `[]`,
  `auth_plugins` `[:caching_sha2_password]`, `password` `""`, `database` `nil`,
  `max_command_retries` `5`, `reconnect_backoff` `1_000`, `heartbeat_period_ms` `15_000`,
  `stream_timeout_ms` `60_000`.
  """
  @spec validate(keyword()) :: {:ok, t()} | {:error, validation_error()}
  def validate(opts) when is_list(opts) do
    with :ok <- reject_unknown_keys(opts, @start_link_options),
         {:ok, server_id} <- fetch_server_id(opts),
         {:ok, connection} <- fetch_connection(opts),
         {:ok, max_command_retries} <- fetch_max_command_retries(opts),
         {:ok, liveness} <- fetch_liveness(opts),
         {:ok, xa} <- fetch_xa(opts),
         {:ok, batch} <- fetch_batch(opts) do
      {:ok,
       %{
         connection: connection,
         server_id: server_id,
         max_command_retries: max_command_retries,
         reconnect_backoff: liveness.reconnect_backoff,
         heartbeat_period_ms: liveness.heartbeat_period_ms,
         stream_timeout_ms: liveness.stream_timeout_ms,
         xa: xa.xa,
         max_prepared_transactions: xa.max_prepared_transactions,
         batch: batch
       }}
    end
  end

  def validate(_opts), do: {:error, :config_invalid}

  @doc """
  Reads the six server preconditions over `socket` and returns `:ok` iff all pass.

  Issues ONE `COM_QUERY` on the already-authenticated socket and compares each value
  as text (ADR-0002). A wrong variable refuses with its distinct reason; a server or
  transport error is surfaced fail-closed, never swallowed into a spurious `:ok`.
  """
  @spec check_preconditions(Packet.socket()) ::
          :ok
          | {:error,
             precondition_error()
             | {:query_error, non_neg_integer()}
             | {:transport, term()}}
  def check_preconditions(socket) do
    case Command.query(socket, @precondition_query) do
      {:ok,
       [
         [
           binlog_format,
           binlog_row_image,
           binlog_row_metadata,
           binlog_row_value_options,
           gtid_mode
         ]
       ]} ->
        evaluate(
          binlog_format,
          binlog_row_image,
          binlog_row_metadata,
          binlog_row_value_options,
          gtid_mode
        )

      {:ok, _unexpected} ->
        {:error, :precondition_query_failed}

      :ok ->
        {:error, :precondition_query_failed}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Normalises the initial-snapshot `:snapshot` block, or `{:ok, nil}` when it is absent (pure C1).

  An absent `:snapshot` key returns `{:ok, nil}` and touches nothing — the pure-C1 path is
  byte-for-byte unchanged. A present block normalises to a `snapshot_config()`:

    * `tables` — the snapshot set. Defaults to the capture allowlist (`:tables`, itself `:all`
      by default). When given it MUST be a non-empty list of `{schema, table}` binary tuples;
      the `⊆ captured` check is `Capstan.Pipeline.validate_snapshot_tables/2`.
    * `store` — REQUIRED, shaped like `checkpoint_store`: `[module: impl, options: keyword()]`.
      A missing or mis-shaped store fails closed `:config_invalid` (a half-configured backfill
      would silently lose its resumability durability).
    * `chunk_size` — a POSITIVE integer, default `#{@default_snapshot_chunk_size}`.

  Any mis-shaped field fails closed with the value-free `:config_invalid` (the generic
  mis-shaped-option refusal, as elsewhere in this module); an unrecognized key in the
  block or its store refuses `:unknown_option`.
  """
  @spec validate_snapshot(keyword()) ::
          {:ok, snapshot_config() | nil} | {:error, :config_invalid | :unknown_option}
  def validate_snapshot(opts) when is_list(opts) do
    case Keyword.get(opts, :snapshot) do
      nil ->
        {:ok, nil}

      snapshot when is_list(snapshot) ->
        if Keyword.keyword?(snapshot),
          do: normalise_snapshot(snapshot, opts),
          else: {:error, :config_invalid}

      _ ->
        {:error, :config_invalid}
    end
  end

  def validate_snapshot(_opts), do: {:error, :config_invalid}

  @doc """
  Reads `@@server_uuid` over an already-authenticated `socket` — the source-identity primitive
  (design Q-src / Ch8).

  The bootstrap reads the STREAM connection's identity through this helper and pins the
  `Capstan.Query` connection against it (`:expected_server_uuid`), so a query connection that
  silently reconnects to a DIFFERENT replica mid-backfill is caught (`:snapshot_source_mismatch`)
  — `@@server_uuid` is compared across BOTH connections. Returns `{:ok, uuid}` (a value-free
  structural identity string) or the value-free `{:error, :server_uuid_read_failed}`; a
  transport/query fault is scrubbed to the bare reason, never the raw term (Rule 1).
  """
  @spec read_server_uuid(Packet.socket()) ::
          {:ok, String.t()} | {:error, :server_uuid_read_failed}
  def read_server_uuid(socket) do
    case Command.query(socket, @server_uuid_query) do
      {:ok, [[uuid]]} when is_binary(uuid) -> {:ok, uuid}
      _other -> {:error, :server_uuid_read_failed}
    end
  rescue
    _exception -> {:error, :server_uuid_read_failed}
  catch
    _kind, _reason -> {:error, :server_uuid_read_failed}
  end

  ## ---------------------------------------------------------------------------
  ## precondition evaluation (text comparison)
  ## ---------------------------------------------------------------------------

  # Every value is text; compare against the expected literal, never a typed term
  # (replicant's A5 class). The first failing variable wins its distinct reason.
  defp evaluate(
         binlog_format,
         binlog_row_image,
         binlog_row_metadata,
         binlog_row_value_options,
         gtid_mode
       ) do
    cond do
      binlog_format != "ROW" -> {:error, :binlog_format_not_row}
      binlog_row_image != "FULL" -> {:error, :binlog_row_image_not_full}
      binlog_row_metadata != "FULL" -> {:error, :binlog_row_metadata_not_full}
      binlog_row_value_options != "" -> {:error, :binlog_row_value_options_not_empty}
      gtid_mode != "ON" -> {:error, :gtid_mode_not_on}
      true -> :ok
    end
  end

  ## ---------------------------------------------------------------------------
  ## option validation (pure)
  ## ---------------------------------------------------------------------------

  # server_id identifies the replica and MUST be a positive integer; missing or
  # non-positive fails closed with its own distinct reason.
  defp fetch_server_id(opts) do
    case Keyword.fetch(opts, :server_id) do
      {:ok, id} when is_integer(id) and id > 0 -> {:ok, id}
      _ -> {:error, :server_id_required}
    end
  end

  # The command-error budget (design Q8), default 5. A present value must be a
  # NON-NEGATIVE integer (`0` = halt-now); a negative or non-integer is a config error,
  # never a silent fallback that would mask a mis-set bound.
  defp fetch_max_command_retries(opts) do
    case Keyword.fetch(opts, :max_command_retries) do
      :error -> {:ok, @default_max_command_retries}
      {:ok, n} when is_integer(n) and n >= 0 -> {:ok, n}
      {:ok, _bad} -> {:error, :config_invalid}
    end
  end

  # Streaming liveness (usage-rules.md "Streaming liveness"): the three public options
  # normalize here, defaults 1_000 / 15_000 / 60_000 — the SAME values Capstan.Connection
  # falls back to on direct wiring, so both constructors compare identical defaults-applied
  # values. A present value must be a positive integer within the schedulable timer
  # ceiling (zero, negative, non-integer, or over-ceiling is a mis-set bound, never a
  # silent fallback). The window comparison fires BEFORE any socket opens: in snapshot
  # mode the bootstrap opens a query connection before the connection child starts,
  # so leaning on Connection.init would open a socket on a bad config.
  # C3 batching: `batch: [max_transactions: n, flush_ms: ms, mode: :lib_owned | :sink_owned]`.
  # Absent ⇒ no batching (per-transaction delivery AND checkpoint, byte-for-byte the
  # C1/C2 behavior). A present block must be a keyword with a positive integer
  # max_transactions (the batch bound — the crash-replay window), an optional positive
  # flush_ms (a time bound so a quiet stream cannot hold the checkpoint forever), and a
  # mode of :lib_owned (default; per-transaction delivery, batched durable CHECKPOINT
  # writes) or :sink_owned (atomic handle_batch/2 delivery + position).
  defp fetch_batch(opts) do
    case Keyword.get(opts, :batch) do
      nil ->
        {:ok, nil}

      batch when is_list(batch) ->
        # Unknown keys are refused like every other sub-config (span-review note): a typo
        # like `flush:` for `flush_ms:` would otherwise silently widen the crash-replay
        # window against operator intent.
        with :ok <- reject_unknown_keys(batch, ~w(max_transactions flush_ms mode)a),
             :ok <- batch_shape(batch) do
          {:ok,
           %{
             max_transactions: Keyword.get(batch, :max_transactions, 1000),
             flush_ms: Keyword.get(batch, :flush_ms, 500),
             mode: Keyword.get(batch, :mode, :lib_owned)
           }}
        end

      _ ->
        {:error, :config_invalid}
    end
  end

  defp batch_shape(batch) do
    positive = fn key, default ->
      case Keyword.get(batch, key, default) do
        n when is_integer(n) and n > 0 -> true
        _ -> false
      end
    end

    checks = [
      Keyword.keyword?(batch),
      positive.(:max_transactions, 1000),
      positive.(:flush_ms, 500),
      Keyword.get(batch, :mode, :lib_owned) in [:lib_owned, :sink_owned]
    ]

    if Enum.all?(checks), do: :ok, else: {:error, :config_invalid}
  end

  # The XA policy (ADR-0006): :refuse (default — the C1 halt posture) or :track.
  # The prepared-pool bound is a POSITIVE integer (zero exhausts the pool on the first
  # prepare — a mis-set bound, not a useful configuration).
  defp fetch_xa(opts) do
    case Keyword.get(opts, :xa, :refuse) do
      policy when policy in [:refuse, :track] -> fetch_max_prepared(opts, policy)
      _ -> {:error, :config_invalid}
    end
  end

  defp fetch_max_prepared(opts, policy) do
    case Keyword.get(opts, :max_prepared_transactions, 10_000) do
      n when is_integer(n) and n > 0 ->
        {:ok, %{xa: policy, max_prepared_transactions: n}}

      _ ->
        {:error, :config_invalid}
    end
  end

  defp fetch_liveness(opts) do
    with {:ok, reconnect_backoff} <-
           fetch_schedulable_ms(opts, :reconnect_backoff, @default_reconnect_backoff),
         {:ok, heartbeat_period_ms} <-
           fetch_schedulable_ms(opts, :heartbeat_period_ms, @default_heartbeat_period_ms),
         {:ok, stream_timeout_ms} <-
           fetch_schedulable_ms(opts, :stream_timeout_ms, @default_stream_timeout_ms) do
      if stream_timeout_ms > heartbeat_period_ms do
        {:ok,
         %{
           reconnect_backoff: reconnect_backoff,
           heartbeat_period_ms: heartbeat_period_ms,
           stream_timeout_ms: stream_timeout_ms
         }}
      else
        {:error, :invalid_liveness_config}
      end
    end
  end

  defp fetch_schedulable_ms(opts, key, default) do
    case Keyword.fetch(opts, key) do
      :error -> {:ok, default}
      {:ok, n} when is_integer(n) and n > 0 and n <= @max_timer_ms -> {:ok, n}
      {:ok, _bad} -> {:error, :config_invalid}
    end
  end

  # Fail closed on an unrecognized option key: a typo would otherwise silently apply the
  # default — the exact ignored-config class the fail-closed posture forbids. The reason
  # is value-free (it names the class, never the key). Non-tuple elements are skipped so
  # a non-keyword list keeps its existing `:server_id_required` shape, not a raise.
  defp reject_unknown_keys(opts, allowed) do
    unknown =
      Enum.flat_map(opts, fn
        {key, _value} -> if key in allowed, do: [], else: [key]
        _other -> []
      end)

    if unknown == [], do: :ok, else: {:error, :unknown_option}
  end

  defp fetch_connection(opts) do
    conn = Keyword.get(opts, :connection)

    if Keyword.keyword?(conn) do
      case reject_unknown_keys(conn, @connection_options) do
        :ok -> normalise_connection(conn)
        {:error, _reason} = error -> error
      end
    else
      {:error, :config_invalid}
    end
  end

  defp normalise_connection(conn) do
    with {:ok, host} <- fetch_host(conn),
         {:ok, port} <- fetch_port(conn),
         {:ok, username} <- fetch_username(conn),
         {:ok, password} <- fetch_password(conn),
         {:ok, database} <- fetch_database(conn),
         {:ok, auth_plugins} <- fetch_auth_plugins(conn),
         {:ok, ssl, ssl_opts} <- fetch_tls(conn) do
      {:ok,
       [
         host: host,
         port: port,
         username: username,
         password: password,
         database: database,
         ssl: ssl,
         ssl_opts: ssl_opts,
         auth_plugins: auth_plugins
       ]}
    end
  end

  # host is required: a non-empty string or a proper charlist (design's
  # `charlist() | String.t()`). A non-empty list that is not all integer codepoints
  # (e.g. `[:foo]`) is not a charlist and is refused.
  defp fetch_host(conn) do
    case Keyword.get(conn, :host) do
      host when is_binary(host) and host != "" -> {:ok, host}
      host when is_list(host) -> validate_charlist_host(host)
      _ -> {:error, :config_invalid}
    end
  end

  defp validate_charlist_host(host) do
    if host != [] and Enum.all?(host, &is_integer/1),
      do: {:ok, host},
      else: {:error, :config_invalid}
  end

  defp fetch_port(conn) do
    case Keyword.get(conn, :port) do
      port when is_integer(port) and port > 0 and port <= 65_535 -> {:ok, port}
      _ -> {:error, :config_invalid}
    end
  end

  defp fetch_username(conn) do
    case Keyword.get(conn, :username) do
      username when is_binary(username) and username != "" -> {:ok, username}
      _ -> {:error, :config_invalid}
    end
  end

  # password is optional and defaults to "" (an empty password authenticates as such).
  defp fetch_password(conn) do
    case Keyword.get(conn, :password, "") do
      password when is_binary(password) -> {:ok, password}
      _ -> {:error, :config_invalid}
    end
  end

  # database is optional; nil means connect with no default schema.
  defp fetch_database(conn) do
    case Keyword.get(conn, :database) do
      nil -> {:ok, nil}
      database when is_binary(database) and database != "" -> {:ok, database}
      _ -> {:error, :config_invalid}
    end
  end

  # auth_plugins defaults to caching_sha2 only; a present value must be a non-empty
  # list of KNOWN plugin atoms so a typo is caught at config time, not at connect.
  defp fetch_auth_plugins(conn) do
    case Keyword.get(conn, :auth_plugins, [:caching_sha2_password]) do
      plugins when is_list(plugins) and plugins != [] ->
        if Enum.all?(plugins, &(&1 in @known_auth_plugins)),
          do: {:ok, plugins},
          else: {:error, :config_invalid}

      _ ->
        {:error, :config_invalid}
    end
  end

  ## ---------------------------------------------------------------------------
  ## snapshot config normalization (pure) — additive; absent :snapshot ⇒ pure C1
  ## ---------------------------------------------------------------------------

  defp normalise_snapshot(snapshot, opts) do
    with :ok <- reject_unknown_keys(snapshot, @snapshot_options),
         {:ok, tables} <- fetch_snapshot_tables(snapshot, opts),
         {:ok, store} <- fetch_snapshot_store(snapshot),
         {:ok, chunk_size} <- fetch_snapshot_chunk_size(snapshot) do
      {:ok, %{tables: tables, store: store, chunk_size: chunk_size}}
    end
  end

  # The snapshot table set defaults to the capture allowlist (`:tables`, itself `:all` by
  # default). When given explicitly it is either `:all` — resolved to the scoped base-table
  # enumeration at bootstrap (C2b, `Capstan.Snapshot.Tables`; valid only against an `:all`
  # capture, enforced by `Capstan.Pipeline.validate_snapshot_tables/2`) — or a NON-EMPTY list
  # of `{schema, table}` binary tuples.
  defp fetch_snapshot_tables(snapshot, opts) do
    case Keyword.get(snapshot, :tables) do
      nil ->
        {:ok, Keyword.get(opts, :tables, :all)}

      :all ->
        {:ok, :all}

      tables when is_list(tables) and tables != [] ->
        if Enum.all?(tables, &table_tuple?/1), do: {:ok, tables}, else: {:error, :config_invalid}

      _ ->
        {:error, :config_invalid}
    end
  end

  defp table_tuple?({schema, table}) when is_binary(schema) and is_binary(table), do: true
  defp table_tuple?(_), do: false

  # The durable snapshot store is REQUIRED in snapshot mode, shaped exactly like
  # `checkpoint_store`: `[module: impl, options: keyword()]`. A missing/mis-shaped store fails
  # closed — a half-configured backfill would silently lose its resumability durability.
  defp fetch_snapshot_store(snapshot) do
    with store when is_list(store) <- Keyword.get(snapshot, :store),
         true <- Keyword.keyword?(store),
         :ok <- reject_unknown_keys(store, @store_block_options),
         module when is_atom(module) and not is_nil(module) <- Keyword.get(store, :module),
         options when is_list(options) <- Keyword.get(store, :options, []),
         true <- Keyword.keyword?(options) do
      {:ok, {module, options}}
    else
      # Only the explicit value-free :unknown_option passes through; every other failed
      # match — including a VALUE shaped like an error tuple (Rule 1: the refusal channel
      # carries atoms only, never config terms) — collapses to :config_invalid.
      {:error, :unknown_option} = error -> error
      _ -> {:error, :config_invalid}
    end
  end

  # rows per chunk (bounds the brief lock's hold + buffered memory), default 4096. A present
  # value must be a POSITIVE integer; zero, negative, or non-integer is a config error, never a
  # silent fallback that would mask a mis-set bound.
  defp fetch_snapshot_chunk_size(snapshot) do
    case Keyword.get(snapshot, :chunk_size, @default_snapshot_chunk_size) do
      n when is_integer(n) and n > 0 -> {:ok, n}
      _ -> {:error, :config_invalid}
    end
  end

  ## ---------------------------------------------------------------------------
  ## option validation (pure) — TLS
  ## ---------------------------------------------------------------------------

  # F6/Q17: mirror Handshake's connect-time guard at config time. With TLS on, an
  # explicit verification choice is required — a CA source or an explicit `verify:`;
  # otherwise fail closed rather than let OTP default to a posture nobody chose.
  # ssl_opts are carried through unchanged (Handshake derives verify_peer at connect).
  defp fetch_tls(conn) do
    ssl? = Keyword.get(conn, :ssl, true)
    ssl_opts = Keyword.get(conn, :ssl_opts, [])

    cond do
      not is_boolean(ssl?) -> {:error, :config_invalid}
      not Keyword.keyword?(ssl_opts) -> {:error, :config_invalid}
      ssl? == false -> {:ok, false, ssl_opts}
      Keyword.has_key?(ssl_opts, :verify) -> {:ok, true, ssl_opts}
      Keyword.has_key?(ssl_opts, :cacertfile) -> {:ok, true, ssl_opts}
      Keyword.has_key?(ssl_opts, :cacerts) -> {:ok, true, ssl_opts}
      true -> {:error, :tls_verification_unspecified}
    end
  end
end

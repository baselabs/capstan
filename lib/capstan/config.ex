defmodule Capstan.Config do
  @moduledoc """
  Option validation and the fail-closed server-precondition gate.

  Two clearly separated responsibilities:

    * `validate/1` is **pure** — it normalises the user's `start_link` options and
      refuses a mis-shaped or unsafe configuration before any socket is opened.
    * `check_preconditions/1` issues **one** `COM_QUERY` on an already-authenticated
      socket and refuses to start unless the source's binlog is configured for
      lossless row-based CDC.

  ## Server preconditions (design Q5)

  `check_preconditions/1` reads five global variables in a single query and refuses
  with a DISTINCT reason per violation — degraded row decoding silently guesses
  column identity, so the gate fails closed rather than proceed:

    * `binlog_format` must be `ROW` — else `:binlog_format_not_row`
    * `binlog_row_image` must be `FULL` — else `:binlog_row_image_not_full`
    * `binlog_row_metadata` must be `FULL` — else `:binlog_row_metadata_not_full`
    * `binlog_row_value_options` must be empty (`""` = full JSON, not `PARTIAL_JSON`)
      — else `:binlog_row_value_options_not_empty`
    * `gtid_mode` must be `ON` — else `:gtid_mode_not_on`

  MySQL simple-query results are **all text strings**, so every value is compared as
  text against the expected literal and never coerced to a typed term — an empty
  `binlog_row_value_options` arrives as `""`, not `nil` or `0`.

  ## TLS verification posture (design Q6/Q17, plan F6)

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
  @known_auth_plugins [:caching_sha2_password, :mysql_native_password]

  @precondition_query "SELECT @@global.binlog_format, @@global.binlog_row_image, " <>
                        "@@global.binlog_row_metadata, @@global.binlog_row_value_options, " <>
                        "@@global.gtid_mode"

  @typedoc "The normalised configuration `validate/1` returns on success."
  @type t :: %{
          connection: keyword(),
          server_id: pos_integer(),
          max_command_retries: non_neg_integer()
        }

  @typedoc "A value-free option-validation refusal."
  @type validation_error ::
          :config_invalid | :server_id_required | :tls_verification_unspecified

  @typedoc "A value-free precondition-gate refusal (design Q5)."
  @type precondition_error ::
          :binlog_format_not_row
          | :binlog_row_image_not_full
          | :binlog_row_metadata_not_full
          | :binlog_row_value_options_not_empty
          | :gtid_mode_not_on
          | :precondition_query_failed

  @doc """
  Validates raw `start_link` options into a normalised config map, or returns a
  value-free error.

  Refuses: `:server_id_required` (missing or non-positive `server_id`),
  `:tls_verification_unspecified` (F6 — `ssl: true` with no CA source and no explicit
  `verify:`), and `:config_invalid` (any other missing or mis-shaped option). Defaults
  applied: `ssl` true, `ssl_opts` `[]`, `auth_plugins` `[:caching_sha2_password]`,
  `password` `""`, `database` `nil`, `max_command_retries` `5`.
  """
  @spec validate(keyword()) :: {:ok, t()} | {:error, validation_error()}
  def validate(opts) when is_list(opts) do
    with {:ok, server_id} <- fetch_server_id(opts),
         {:ok, connection} <- fetch_connection(opts),
         {:ok, max_command_retries} <- fetch_max_command_retries(opts) do
      {:ok,
       %{
         connection: connection,
         server_id: server_id,
         max_command_retries: max_command_retries
       }}
    end
  end

  def validate(_opts), do: {:error, :config_invalid}

  @doc """
  Reads the five server preconditions over `socket` and returns `:ok` iff all pass.

  Issues ONE `COM_QUERY` on the already-authenticated socket and compares each value
  as text (design Q5). A wrong variable refuses with its distinct reason; a server or
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

  defp fetch_connection(opts) do
    conn = Keyword.get(opts, :connection)

    if Keyword.keyword?(conn) do
      normalise_connection(conn)
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

  # host is required: a non-empty string or a charlist (design's `charlist() | String.t()`).
  defp fetch_host(conn) do
    case Keyword.get(conn, :host) do
      host when is_binary(host) and host != "" -> {:ok, host}
      host when is_list(host) and host != [] -> {:ok, host}
      _ -> {:error, :config_invalid}
    end
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

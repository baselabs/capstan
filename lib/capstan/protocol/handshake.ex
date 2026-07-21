defmodule Capstan.Protocol.Handshake do
  @moduledoc """
  MySQL connection handshake, authentication and TLS upgrade.

  A pure protocol module: it takes an already-connected `{:gen_tcp, socket}` plus
  resolved options and drives, in order, the initial-handshake parse, an optional
  TLS upgrade, and authentication — returning an authenticated, transport-tagged
  socket (`{:gen_tcp, _}` or `{:ssl, _}`, per `Capstan.Protocol.Packet`) and the
  negotiated server capabilities. It owns no process, no config validation and no
  socket lifecycle; `Capstan.Connection` supplies the socket and the options.

  ## Transport posture (design Q6 / Q17)

  TLS is expected to be on by default (the caller passes `ssl: false` to opt out).
  Peer verification is an **explicit operator choice, never a silent default**: with
  `ssl: true` the caller must supply EITHER a `cacertfile`/`cacerts` OR an explicit
  `verify:` in `ssl_opts`. Given neither, `connect/2` fails closed with
  `:tls_verification_unspecified` rather than silently selecting `verify_none` —
  OTP would otherwise default to `verify_peer`, which cannot validate MySQL's
  self-signed auto-generated certificate, and the tempting "fix" is unauthenticated
  TLS chosen by nobody.

  ## Authentication (design Q6)

  `caching_sha2_password` is the preferred plugin, including its RSA-public-key
  full-auth path. `mysql_native_password` is honoured only when named in
  `auth_plugins`; using it logs a deprecation warning (removed in MySQL 9.x).

  The full-auth secret handling is fail-closed: on a full-auth request over TLS the
  protocol permits the secure-channel fast path (the password crosses the already
  encrypted channel). Over a plaintext channel the password is **never** sent in
  the clear — it is XORed with the auth nonce and RSA-OAEP encrypted with the
  server's public key; if that key cannot be obtained, `connect/2` fails closed
  with `:insecure_auth_refused`, and a server-supplied key that does not decode
  fails closed with `:bad_public_key` (never a raise).

  ## Rule 1

  The connection password appears in no returned term, error, or log line emitted
  by this module.
  """

  import Bitwise
  require Logger

  alias Capstan.Protocol.Packet

  # Client capability flags (subset the client advertises).
  @client_long_password 0x00000001
  @client_connect_with_db 0x00000008
  @client_protocol_41 0x00000200
  @client_ssl 0x00000800
  @client_secure_connection 0x00008000
  @client_plugin_auth 0x00080000
  @client_plugin_auth_lenenc_client_data 0x00200000
  @client_deprecate_eof 0x01000000

  @base_capabilities @client_long_password ||| @client_protocol_41 |||
                       @client_secure_connection ||| @client_plugin_auth |||
                       @client_plugin_auth_lenenc_client_data ||| @client_deprecate_eof

  @charset_utf8mb4 45
  @max_packet 0x0100_0000
  @default_timeout 20_000

  # caching_sha2_password full-auth sub-protocol markers.
  @request_public_key 0x02
  @fast_auth_success 0x03
  @full_auth_required 0x04

  @typedoc "A transport-tagged socket, as defined by `Capstan.Protocol.Packet`."
  @type socket :: Packet.socket()

  @typedoc "Parsed `Protocol::HandshakeV10` initial handshake."
  @type initial_handshake :: %{
          server_version: String.t(),
          connection_id: non_neg_integer(),
          salt: binary(),
          auth_plugin: String.t(),
          capabilities: non_neg_integer(),
          charset: byte(),
          status: non_neg_integer()
        }

  @typedoc "Result of a successful handshake."
  @type result :: %{
          socket: socket(),
          server_version: String.t(),
          capabilities: non_neg_integer(),
          connection_id: non_neg_integer(),
          tls: boolean()
        }

  @typedoc "Resolved connection options."
  @type connect_opt ::
          {:username, String.t()}
          | {:password, String.t()}
          | {:database, String.t() | nil}
          | {:ssl, boolean()}
          | {:ssl_opts, keyword()}
          | {:auth_plugins, [atom()]}
          | {:host, charlist() | String.t()}
          | {:timeout, timeout()}

  @doc """
  Reads the initial handshake, optionally upgrades to TLS, and authenticates.

  `socket` is an already-connected `{:gen_tcp, socket}`. On success returns
  `{:ok, result}` carrying the authenticated (possibly TLS-upgraded) socket and the
  negotiated capabilities; on failure returns `{:error, reason}` with a value-free
  reason.
  """
  @spec connect(socket(), [connect_opt()]) :: {:ok, result()} | {:error, term()}
  def connect({:gen_tcp, _} = socket, opts) when is_list(opts) do
    ssl? = Keyword.get(opts, :ssl, true)
    ssl_opts = Keyword.get(opts, :ssl_opts, [])
    timeout = Keyword.get(opts, :timeout, @default_timeout)
    auth_plugins = Keyword.get(opts, :auth_plugins, [:caching_sha2_password])

    ctx = %{
      username: Keyword.fetch!(opts, :username),
      password: Keyword.get(opts, :password) || "",
      database: Keyword.get(opts, :database),
      auth_plugins: auth_plugins,
      timeout: timeout
    }

    # Both checks are pure and precede any I/O so an invalid TLS posture fails
    # closed without ever touching the wire. `connect/2` owns the socket it is
    # handed: on the pre-I/O pure failures the raw socket is closed here; once
    # `perform/4` takes over it owns closing the (possibly TLS-upgraded) socket on
    # its own error paths, so a `perform/4` error is passed through untouched and is
    # never routed through this `else` (no double-close).
    with {:ok, lead} <- lead_plugin(auth_plugins),
         {:ok, transport} <- transport(ssl?, ssl_opts) do
      perform(socket, transport, lead, ctx)
    else
      {:error, reason} ->
        close_socket(socket)
        {:error, reason}
    end
  end

  @doc """
  Parses a `Protocol::HandshakeV10` initial handshake packet.

  Returns `{:ok, handshake}` or `{:error, reason}` (a server error packet becomes
  `{:error, {:handshake_error, code}}`).
  """
  @spec parse_initial_handshake(binary()) :: {:ok, initial_handshake()} | {:error, term()}
  def parse_initial_handshake(<<0xFF, code::16-little, _rest::binary>>),
    do: {:error, {:handshake_error, code}}

  def parse_initial_handshake(<<10, rest::binary>>) do
    case :binary.split(rest, <<0>>) do
      [server_version, tail] -> parse_after_version(server_version, tail)
      _ -> {:error, :malformed_handshake}
    end
  end

  def parse_initial_handshake(_), do: {:error, :unsupported_handshake_version}

  @doc """
  The `caching_sha2_password` fast-auth scramble.

  `SHA256(password) XOR SHA256(SHA256(SHA256(password)) <> nonce)`. An empty
  password scrambles to an empty response.
  """
  @spec caching_sha2_scramble(binary(), binary()) :: binary()
  def caching_sha2_scramble("", _nonce), do: <<>>

  def caching_sha2_scramble(password, nonce) do
    digest1 = :crypto.hash(:sha256, password)
    digest2 = :crypto.hash(:sha256, digest1)
    :crypto.exor(digest1, :crypto.hash(:sha256, digest2 <> nonce))
  end

  @doc """
  The `mysql_native_password` scramble (opt-in plugin).

  `SHA1(password) XOR SHA1(nonce <> SHA1(SHA1(password)))`. An empty password
  scrambles to an empty response.
  """
  @spec native_scramble(binary(), binary()) :: binary()
  def native_scramble("", _nonce), do: <<>>

  def native_scramble(password, nonce) do
    sha = &:crypto.hash(:sha, &1)
    stage1 = sha.(password)
    :crypto.exor(stage1, sha.(nonce <> sha.(stage1)))
  end

  @doc """
  Encrypts the password for the `caching_sha2_password` full-auth path.

  The NUL-terminated password is XORed with the auth nonce (cycled) and then
  RSA-OAEP encrypted with the server's PEM-encoded public key. A public key that
  does not decode fails closed with `{:error, :bad_public_key}` — the password is
  never sent when the key is unusable.
  """
  @spec encrypt_password(binary(), binary(), binary()) ::
          {:ok, binary()} | {:error, :bad_public_key}
  def encrypt_password(password, nonce, public_key_pem) do
    obfuscated = xor_nonce(password <> <<0>>, nonce)

    case :public_key.pem_decode(public_key_pem) do
      [pem_entry | _] ->
        public_key = :public_key.pem_entry_decode(pem_entry)

        {:ok,
         :public_key.encrypt_public(obfuscated, public_key, rsa_padding: :rsa_pkcs1_oaep_padding)}

      [] ->
        {:error, :bad_public_key}
    end
  end

  ## ---------------------------------------------------------------------------
  ## handshake orchestration
  ## ---------------------------------------------------------------------------

  # Owns the socket lifetime once I/O begins. The raw `socket` is upgraded to the
  # transport-tagged `active` socket (`{:ssl, _}` after a TLS upgrade, unchanged on
  # plaintext). Because a `with` that rebinds `socket` cannot reach the upgraded
  # value from an error handler, the credential exchange lives in `authenticate_over/7`
  # which closes the `active` socket it holds on any failure. Errors BEFORE the
  # upgrade (a malformed handshake, or `{:tls_failed, _}`) close the raw socket here
  # — defensively, since a failed `:ssl.connect/3` may leave it open.
  defp perform(socket, transport, lead, ctx) do
    {_seq, handshake_bin} = Packet.read_packet(socket, ctx.timeout)
    ssl? = transport != :plaintext
    caps = client_capabilities(ssl?, ctx.database)
    response_seq = if ssl?, do: 2, else: 1

    with {:ok, handshake} <- parse_initial_handshake(handshake_bin),
         {:ok, active} <- maybe_upgrade_tls(socket, transport, caps, ctx.timeout) do
      authenticate_over(active, lead, ctx, handshake, caps, ssl?, response_seq)
    else
      {:error, reason} ->
        close_socket(socket)
        {:error, reason}
    end
  end

  # Runs the HandshakeResponse + authentication over the (possibly TLS-upgraded)
  # `active` socket. On any failure it closes `active` — the crux of the fix: this is
  # the only scope from which the upgraded `{:ssl, _}` socket is reachable, so it is
  # the only place that can free it. A success returns the result unchanged; a
  # returned `{:error, _}` is `perform/4`'s do-block value and so bypasses that
  # function's `else`, guaranteeing a single close.
  defp authenticate_over(active, lead, ctx, handshake, caps, ssl?, response_seq) do
    response = handshake_response(lead, ctx, handshake.salt, caps)

    with :ok <- reply(active, response, response_seq),
         :ok <- authenticate(active, ctx, handshake.salt) do
      {:ok, build_result(active, handshake, caps, ssl?)}
    else
      {:error, reason} ->
        close_socket(active)
        {:error, reason}
    end
  end

  # Frees whichever transport a `{:error, _}` path still holds. `:gen_tcp.close/1`
  # and `:ssl.close/1` are idempotent, so a defensive close of an already-freed
  # socket is harmless.
  defp close_socket({:gen_tcp, sock}), do: :gen_tcp.close(sock)
  defp close_socket({:ssl, sock}), do: :ssl.close(sock)

  defp build_result(socket, handshake, caps, ssl?) do
    %{
      socket: socket,
      server_version: handshake.server_version,
      capabilities: caps &&& handshake.capabilities,
      connection_id: handshake.connection_id,
      tls: ssl?
    }
  end

  # Preference: caching_sha2_password, then the opt-in native plugin.
  defp lead_plugin(auth_plugins) do
    cond do
      :caching_sha2_password in auth_plugins -> {:ok, :caching_sha2_password}
      :mysql_native_password in auth_plugins -> {:ok, :mysql_native_password}
      true -> {:error, :no_supported_auth_plugin}
    end
  end

  # F6 / Q17: with TLS on, require an explicit verification choice — a CA source or
  # an explicit `verify:`. Otherwise fail closed rather than defaulting.
  defp transport(false, _ssl_opts), do: {:ok, :plaintext}

  defp transport(true, ssl_opts) do
    cond do
      Keyword.has_key?(ssl_opts, :verify) ->
        {:ok, {:tls, ssl_opts}}

      Keyword.has_key?(ssl_opts, :cacertfile) or Keyword.has_key?(ssl_opts, :cacerts) ->
        {:ok, {:tls, Keyword.put(ssl_opts, :verify, :verify_peer)}}

      true ->
        {:error, :tls_verification_unspecified}
    end
  end

  # The TLS request precedes any credentials: the 32-byte SSLRequest goes out over
  # the plaintext socket, then the socket is upgraded, and only the authenticated
  # transport carries the HandshakeResponse.
  defp maybe_upgrade_tls(socket, :plaintext, _caps, _timeout), do: {:ok, socket}

  defp maybe_upgrade_tls({:gen_tcp, raw} = socket, {:tls, ssl_opts}, caps, timeout) do
    ssl_request = <<caps::32-little, @max_packet::32-little, @charset_utf8mb4, 0::23*8>>

    with :ok <- reply(socket, ssl_request, 1),
         {:ok, ssl_socket} <- :ssl.connect(raw, ssl_opts, timeout) do
      {:ok, {:ssl, ssl_socket}}
    else
      {:error, reason} -> {:error, {:tls_failed, reason}}
    end
  end

  defp handshake_response(lead, ctx, salt, caps) do
    token = lead_scramble(lead, ctx.password, salt)
    database = if ctx.database, do: ctx.database <> <<0>>, else: <<>>

    <<caps::32-little, @max_packet::32-little, @charset_utf8mb4, 0::23*8>> <>
      ctx.username <>
      <<0>> <>
      <<byte_size(token)::8>> <>
      token <>
      database <>
      plugin_name(lead) <>
      <<0>>
  end

  ## ---------------------------------------------------------------------------
  ## authentication exchange
  ## ---------------------------------------------------------------------------

  defp authenticate(socket, ctx, nonce) do
    case Packet.read_packet(socket, ctx.timeout) do
      {_seq, <<0x00, _rest::binary>>} ->
        :ok

      {_seq, <<0xFF, code::16-little, _rest::binary>>} ->
        {:error, {:auth_failed, code}}

      {_seq, <<0x01, @fast_auth_success>>} ->
        # The server accepts the cached credential; an OK packet follows.
        authenticate(socket, ctx, nonce)

      {seq, <<0x01, @full_auth_required>>} ->
        with :ok <- full_auth(socket, ctx, nonce, seq), do: authenticate(socket, ctx, nonce)

      {seq, <<0xFE, rest::binary>>} ->
        with {:ok, next_nonce} <- switch_auth(socket, ctx, rest, seq),
             do: authenticate(socket, ctx, next_nonce)

      {_seq, _other} ->
        {:error, :unexpected_auth_response}
    end
  end

  # Over TLS the secure-channel fast path sends the NUL-terminated password on the
  # already-encrypted transport.
  defp full_auth({:ssl, _} = socket, ctx, _nonce, seq),
    do: reply(socket, ctx.password <> <<0>>, seq + 1)

  # Over plaintext the password is never sent in the clear: request the server's
  # public key and RSA-encrypt. If no key is obtainable, fail closed.
  defp full_auth({:gen_tcp, _} = socket, ctx, nonce, seq) do
    with :ok <- reply(socket, <<@request_public_key>>, seq + 1),
         {key_seq, <<0x01, pem::binary>>} when byte_size(pem) > 0 <-
           Packet.read_packet(socket, ctx.timeout),
         {:ok, cipher} <- encrypt_password(ctx.password, nonce, pem) do
      reply(socket, cipher, key_seq + 1)
    else
      {:error, _reason} = error -> error
      _no_key -> {:error, :insecure_auth_refused}
    end
  end

  defp switch_auth(socket, ctx, rest, seq) do
    case :binary.split(rest, <<0>>) do
      [plugin_name, auth_data] -> send_switch_response(socket, ctx, plugin_name, auth_data, seq)
      _ -> {:error, :unsupported_auth_switch}
    end
  end

  defp send_switch_response(socket, ctx, plugin_name, auth_data, seq) do
    with {:ok, plugin} <- allow_plugin(plugin_name, ctx.auth_plugins),
         nonce = switch_nonce(auth_data),
         token = switch_scramble(plugin, ctx.password, nonce),
         :ok <- reply(socket, token, seq + 1) do
      {:ok, nonce}
    end
  end

  defp allow_plugin("caching_sha2_password", allowed), do: gate(:caching_sha2_password, allowed)
  defp allow_plugin("mysql_native_password", allowed), do: gate(:mysql_native_password, allowed)
  defp allow_plugin(_other, _allowed), do: {:error, :unsupported_auth_plugin}

  defp gate(plugin, allowed) do
    if plugin in allowed, do: {:ok, plugin}, else: {:error, refusal(plugin)}
  end

  defp refusal(:mysql_native_password), do: :mysql_native_password_not_enabled
  defp refusal(:caching_sha2_password), do: :caching_sha2_password_not_enabled

  # The AuthSwitchRequest carries a 20-byte nonce, usually with a trailing NUL.
  defp switch_nonce(<<nonce::binary-size(20), _tail::binary>>), do: nonce
  defp switch_nonce(other), do: other

  defp lead_scramble(:caching_sha2_password, password, salt),
    do: caching_sha2_scramble(password, salt)

  defp lead_scramble(:mysql_native_password, password, salt) do
    warn_native_password()
    native_scramble(password, salt)
  end

  defp switch_scramble(:caching_sha2_password, password, nonce),
    do: caching_sha2_scramble(password, nonce)

  defp switch_scramble(:mysql_native_password, password, nonce) do
    warn_native_password()
    native_scramble(password, nonce)
  end

  defp plugin_name(:caching_sha2_password), do: "caching_sha2_password"
  defp plugin_name(:mysql_native_password), do: "mysql_native_password"

  defp warn_native_password do
    Logger.warning(
      "capstan: authenticating with mysql_native_password, which is deprecated and " <>
        "removed in MySQL 9.x; prefer caching_sha2_password"
    )
  end

  ## ---------------------------------------------------------------------------
  ## parsing + primitives
  ## ---------------------------------------------------------------------------

  defp parse_after_version(server_version, tail) do
    case tail do
      <<connection_id::32-little, salt1::binary-size(8), 0, cap_low::16-little, charset::8,
        status::16-little, cap_high::16-little, salt_len::8, _reserved::binary-size(10),
        rest::binary>> ->
        salt2_len = max(13, salt_len - 8) - 1

        case rest do
          <<salt2::binary-size(salt2_len), 0, rest2::binary>> ->
            [plugin | _] = :binary.split(rest2, <<0>>)

            {:ok,
             %{
               server_version: server_version,
               connection_id: connection_id,
               salt: salt1 <> salt2,
               auth_plugin: plugin,
               capabilities: cap_low ||| cap_high <<< 16,
               charset: charset,
               status: status
             }}

          _ ->
            {:error, :malformed_handshake}
        end

      _ ->
        {:error, :malformed_handshake}
    end
  end

  defp client_capabilities(ssl?, database) do
    caps = @base_capabilities
    caps = if ssl?, do: caps ||| @client_ssl, else: caps
    if database, do: caps ||| @client_connect_with_db, else: caps
  end

  defp xor_nonce(data, nonce) do
    nonce_len = byte_size(nonce)

    data
    |> :binary.bin_to_list()
    |> Enum.with_index()
    |> Enum.map(fn {byte, index} -> bxor(byte, :binary.at(nonce, rem(index, nonce_len))) end)
    |> :binary.list_to_bin()
  end

  defp reply(socket, payload, seq) do
    case Packet.send_packet(socket, payload, seq) do
      :ok -> :ok
      {:error, reason} -> {:error, {:transport, reason}}
    end
  end
end

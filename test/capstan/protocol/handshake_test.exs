defmodule Capstan.Protocol.HandshakeTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog
  import Bitwise

  alias Capstan.Protocol.Handshake
  alias Capstan.Protocol.Packet

  @loopback {127, 0, 0, 1}
  @username "capstan_test"
  # A distinctive sentinel so a Rule-1 leak of the password is unmistakable.
  @password "s3cr3t-p@ss-SENTINEL"
  @salt for i <- 1..20, into: <<>>, do: <<i>>

  @client_ssl 0x00000800
  @client_connect_with_db 0x00000008

  ## ---------------------------------------------------------------------------
  ## parse_initial_handshake/1
  ## ---------------------------------------------------------------------------

  describe "parse_initial_handshake/1" do
    test "extracts version, connection id, 20-byte salt, plugin and capabilities" do
      hs =
        build_handshake(
          server_version: "8.0.46",
          connection_id: 42,
          salt: @salt,
          plugin: "caching_sha2_password",
          cap_low: 0xFFFF,
          cap_high: 0xFFFF
        )

      assert {:ok, parsed} = Handshake.parse_initial_handshake(hs)
      assert parsed.server_version == "8.0.46"
      assert parsed.connection_id == 42
      assert parsed.salt == @salt
      assert byte_size(parsed.salt) == 20
      assert parsed.auth_plugin == "caching_sha2_password"
      assert parsed.capabilities == 0xFFFFFFFF
    end

    test "reports a server error packet instead of a v10 handshake" do
      assert {:error, {:handshake_error, 1040}} =
               Handshake.parse_initial_handshake(
                 <<0xFF, 1040::16-little, "Too many connections">>
               )
    end

    test "rejects an unsupported protocol version" do
      assert {:error, :unsupported_handshake_version} =
               Handshake.parse_initial_handshake(<<9, "5.0.0", 0>>)
    end
  end

  ## ---------------------------------------------------------------------------
  ## caching_sha2_password fast-auth scramble
  ## ---------------------------------------------------------------------------

  describe "caching_sha2_scramble/2" do
    test "is empty for an empty password (no bytes sent)" do
      assert Handshake.caching_sha2_scramble("", @salt) == <<>>
    end

    test "is 32 bytes for a non-empty password" do
      assert byte_size(Handshake.caching_sha2_scramble(@password, @salt)) == 32
    end

    test "encodes SHA256(password) exactly as the server verifies it" do
      scramble = Handshake.caching_sha2_scramble(@password, @salt)

      # Server-side verification oracle, computed independently with :crypto:
      #   recovered = scramble XOR SHA256(SHA256(SHA256(password)) || nonce)
      #   must equal SHA256(password); the server then checks SHA256(recovered)
      #   against its stored double hash.
      digest1 = :crypto.hash(:sha256, @password)
      digest2 = :crypto.hash(:sha256, digest1)
      recovered = :crypto.exor(scramble, :crypto.hash(:sha256, digest2 <> @salt))

      assert recovered == digest1
    end

    test "mixes the nonce in — a different nonce yields a different scramble" do
      other = for i <- 21..40, into: <<>>, do: <<i>>

      refute Handshake.caching_sha2_scramble(@password, @salt) ==
               Handshake.caching_sha2_scramble(@password, other)
    end
  end

  describe "native_scramble/2 (opt-in plugin)" do
    test "is empty for an empty password" do
      assert Handshake.native_scramble("", @salt) == <<>>
    end

    test "encodes SHA1(password) as the server's native check verifies it" do
      token = Handshake.native_scramble(@password, @salt)
      sha = &:crypto.hash(:sha, &1)
      stage1 = sha.(@password)
      recovered = :crypto.exor(token, sha.(@salt <> sha.(stage1)))

      assert recovered == stage1
      assert byte_size(token) == 20
    end
  end

  ## ---------------------------------------------------------------------------
  ## caching_sha2_password full-auth RSA public-key encryption
  ## ---------------------------------------------------------------------------

  describe "encrypt_password/3 (full-auth RSA path)" do
    test "produces ciphertext that decrypts to the nonce-obfuscated password" do
      {pub_pem, priv} = generate_rsa()

      assert {:ok, ciphertext} = Handshake.encrypt_password(@password, @salt, pub_pem)

      plaintext =
        :public_key.decrypt_private(ciphertext, priv, rsa_padding: :rsa_pkcs1_oaep_padding)

      # The wire format is XOR(password <> NUL, nonce) then RSA-OAEP. Reversing
      # the RSA and the XOR must recover the NUL-terminated password.
      assert unxor(plaintext, @salt) == @password <> <<0>>
    end

    test "the ciphertext never contains the cleartext password" do
      {pub_pem, _priv} = generate_rsa()
      assert {:ok, ciphertext} = Handshake.encrypt_password(@password, @salt, pub_pem)
      assert :binary.match(ciphertext, @password) == :nomatch
    end

    test "fails closed :bad_public_key on a malformed public key (no raise)" do
      assert {:error, :bad_public_key} =
               Handshake.encrypt_password(@password, @salt, "this is not a pem key")
    end

    test "fails closed :bad_public_key on a key that decodes but cannot encrypt (no raise)" do
      # A PEM that pem_decodes to an ENTRY but is not a usable RSA public key — here an RSA
      # PRIVATE key PEM — reaches `pem_entry_decode`/`encrypt_public`, which RAISE
      # (FunctionClauseError). The rescue must convert that to the value-free
      # {:error, :bad_public_key} (the documented "never a raise" contract), not crash. The
      # `[]`-decode case is covered by the sibling test above; this is the decodes-but-unusable
      # case the bare `case` missed.
      {_pub_pem, priv} = generate_rsa()
      priv_pem = :public_key.pem_encode([:public_key.pem_entry_encode(:RSAPrivateKey, priv)])

      assert {:error, :bad_public_key} =
               Handshake.encrypt_password(@password, @salt, priv_pem)
    end
  end

  ## ---------------------------------------------------------------------------
  ## connect/2 — caching_sha2 fast auth (mock server)
  ## ---------------------------------------------------------------------------

  describe "connect/2 — caching_sha2_password over plaintext" do
    test "completes the fast-auth path and returns a tagged socket + server info" do
      socket =
        mock_client(fn srv, test ->
          t_send(srv, build_handshake(salt: @salt), 0)
          {1, resp} = t_recv_pkt(srv)
          send(test, {:resp, decode_response(resp)})
          # AuthMoreData 0x03 = fast_auth_success, then OK.
          t_send(srv, <<0x01, 0x03>>, 2)
          t_send(srv, ok_packet(), 3)
        end)

      assert {:ok, result} =
               Handshake.connect(socket,
                 username: @username,
                 password: @password,
                 ssl: false
               )

      assert_receive {:resp, resp}
      assert resp.username == @username
      assert resp.plugin == "caching_sha2_password"
      assert byte_size(resp.token) == 32

      assert {:gen_tcp, _} = result.socket
      assert result.tls == false
      assert result.server_version == "8.0.46"

      # Rule 1: the password is nowhere in the returned success term.
      refute inspect(result) =~ "s3cr3t"
    end

    test "completes the full-auth RSA path over a non-TLS channel" do
      {pub_pem, priv} = generate_rsa()

      socket =
        mock_client(fn srv, test ->
          t_send(srv, build_handshake(salt: @salt), 0)
          {1, _resp} = t_recv_pkt(srv)
          # AuthMoreData 0x04 = full authentication required.
          t_send(srv, <<0x01, 0x04>>, 2)
          # The client must request the public key (never send cleartext on TCP).
          {3, request} = t_recv_pkt(srv)
          send(test, {:pubkey_request, request})
          t_send(srv, <<0x01>> <> pub_pem, 4)
          {5, encrypted} = t_recv_pkt(srv)

          plaintext =
            :public_key.decrypt_private(encrypted, priv, rsa_padding: :rsa_pkcs1_oaep_padding)

          send(test, {:decrypted, unxor(plaintext, @salt)})
          t_send(srv, ok_packet(), 6)
        end)

      assert {:ok, %{tls: false}} =
               Handshake.connect(socket, username: @username, password: @password, ssl: false)

      assert_receive {:pubkey_request, <<0x02>>}
      assert_receive {:decrypted, decrypted}
      assert decrypted == @password <> <<0>>
    end

    test "refuses full auth over a non-TLS channel when no RSA key is obtainable" do
      socket =
        mock_client(fn srv, _test ->
          t_send(srv, build_handshake(salt: @salt), 0)
          {1, _resp} = t_recv_pkt(srv)
          t_send(srv, <<0x01, 0x04>>, 2)
          {3, <<0x02>>} = t_recv_pkt(srv)
          # Server cannot / will not hand out a public key.
          t_send(srv, error_packet(1045), 4)
        end)

      assert {:error, :insecure_auth_refused} =
               Handshake.connect(socket, username: @username, password: @password, ssl: false)
    end

    test "fails closed :bad_public_key when the server answers with a malformed public key" do
      socket =
        mock_client(fn srv, _test ->
          t_send(srv, build_handshake(salt: @salt), 0)
          {1, _resp} = t_recv_pkt(srv)
          t_send(srv, <<0x01, 0x04>>, 2)
          {3, <<0x02>>} = t_recv_pkt(srv)
          # Non-empty (clears the byte_size guard) but not a decodable PEM key —
          # must fail closed, not raise, and the password must never be sent.
          t_send(srv, <<0x01>> <> "this is not a public key", 4)
        end)

      assert {:error, :bad_public_key} =
               Handshake.connect(socket, username: @username, password: @password, ssl: false)
    end
  end

  ## ---------------------------------------------------------------------------
  ## connect/2 — mysql_native_password gating
  ## ---------------------------------------------------------------------------

  describe "connect/2 — mysql_native_password gating (design Q6)" do
    test "refuses a switch to mysql_native_password unless it is named in auth_plugins" do
      socket =
        mock_client(fn srv, _test ->
          t_send(srv, build_handshake(salt: @salt), 0)
          {1, _resp} = t_recv_pkt(srv)
          t_send(srv, auth_switch("mysql_native_password", @salt), 2)
        end)

      # Default auth_plugins is [:caching_sha2_password] — native is NOT allowed.
      assert {:error, :mysql_native_password_not_enabled} =
               Handshake.connect(socket, username: @username, password: @password, ssl: false)
    end

    test "honours a switch to mysql_native_password when it is explicitly enabled, and warns" do
      socket =
        mock_client(fn srv, test ->
          t_send(srv, build_handshake(salt: @salt), 0)
          {1, _resp} = t_recv_pkt(srv)
          t_send(srv, auth_switch("mysql_native_password", @salt), 2)
          {3, token} = t_recv_pkt(srv)
          send(test, {:switch_token, token})
          t_send(srv, ok_packet(), 4)
        end)

      log =
        capture_log(fn ->
          assert {:ok, %{tls: false}} =
                   Handshake.connect(socket,
                     username: @username,
                     password: @password,
                     ssl: false,
                     auth_plugins: [:caching_sha2_password, :mysql_native_password]
                   )
        end)

      assert_receive {:switch_token, token}
      assert byte_size(token) == 20
      assert log =~ "mysql_native_password"
      assert log =~ "9.x" or log =~ "removed"
      # Rule 1: the warning must not carry the password.
      refute log =~ "s3cr3t"
    end
  end

  ## ---------------------------------------------------------------------------
  ## connect/2 — TLS
  ## ---------------------------------------------------------------------------

  describe "connect/2 — TLS upgrade (F6 / design Q17)" do
    test "sends the TLS request BEFORE any credentials and authenticates over the upgraded socket" do
      tls_opts = server_tls_opts()

      socket =
        mock_client(fn srv, test ->
          # Handshake goes out in the clear.
          t_send(srv, build_handshake(salt: @salt), 0)

          # The FIRST client packet must be the 32-byte SSLRequest — capability
          # flags + max packet + charset + 23 reserved bytes, with CLIENT_SSL set
          # and NO room for a username. Credentials cannot have preceded TLS.
          {1, ssl_request} = t_recv_pkt(srv)

          send(
            test,
            {:ssl_request,
             %{
               size: byte_size(ssl_request),
               ssl_flag?: ssl_request_has_ssl_flag?(ssl_request)
             }}
          )

          {:ok, tls} = :ssl.handshake(srv, tls_opts, 5000)
          tls_srv = {:ssl, tls}

          # Only now, over TLS, do the credentials arrive.
          {2, resp} = t_recv_pkt(tls_srv)
          send(test, {:tls_response, decode_response(resp)})

          t_send(tls_srv, <<0x01, 0x03>>, 3)
          t_send(tls_srv, ok_packet(), 4)
          Process.sleep(50)
        end)

      assert {:ok, result} =
               Handshake.connect(socket,
                 username: @username,
                 password: @password,
                 ssl: true,
                 ssl_opts: [verify: :verify_none]
               )

      assert {:ssl, _} = result.socket
      assert result.tls == true

      assert_receive {:ssl_request, ssl_request}
      assert ssl_request.size == 32, "the pre-TLS packet must be exactly the 32-byte SSLRequest"
      assert ssl_request.ssl_flag?, "the SSLRequest must set CLIENT_SSL"

      assert_receive {:tls_response, tls_response}

      assert tls_response.username == @username,
             "credentials must arrive only over the TLS channel"

      assert byte_size(tls_response.token) == 32
    end

    test "takes the secure-channel cleartext path (never RSA) on full auth over the TLS socket" do
      tls_opts = server_tls_opts()

      socket =
        mock_client(fn srv, test ->
          t_send(srv, build_handshake(salt: @salt), 0)
          {1, _ssl_request} = t_recv_pkt(srv)
          {:ok, tls} = :ssl.handshake(srv, tls_opts, 5000)
          tls_srv = {:ssl, tls}
          {2, _resp} = t_recv_pkt(tls_srv)

          # Full authentication required — but the channel is ALREADY encrypted, so
          # the protocol permits (and the design mandates) the secure-channel path.
          t_send(tls_srv, <<0x01, 0x04>>, 3)

          # Capture the client's reply verbatim. It must be the NUL-terminated
          # cleartext password at server_seq + 1 — NOT a 0x02 public-key request.
          {seq, reply} = t_recv_pkt(tls_srv)
          send(test, {:full_auth_reply, %{seq: seq, payload: reply}})
          t_send(tls_srv, ok_packet(), seq + 1)
          Process.sleep(50)
        end)

      assert {:ok, %{tls: true}} =
               Handshake.connect(socket,
                 username: @username,
                 password: @password,
                 ssl: true,
                 ssl_opts: [verify: :verify_none]
               )

      assert_receive {:full_auth_reply, full_auth_reply}

      assert full_auth_reply.payload == @password <> <<0>>,
             "full auth over TLS must send the NUL-terminated password on the encrypted channel, not the RSA public-key path"

      assert full_auth_reply.seq == 4, "the cleartext reply must be sequenced at server_seq + 1"
    end

    test "fails closed :tls_verification_unspecified when ssl is on with neither cacertfile nor verify" do
      # A socket whose server never speaks: the fail-closed check must fire BEFORE
      # any I/O, so this must NOT hang and must NOT silently choose verify_none.
      socket = mock_client(fn _srv, _test -> Process.sleep(200) end)

      assert {:error, :tls_verification_unspecified} =
               Handshake.connect(socket,
                 username: @username,
                 password: @password,
                 ssl: true,
                 ssl_opts: []
               )
    end
  end

  ## ---------------------------------------------------------------------------
  ## Rule 1 — the password never leaks
  ## ---------------------------------------------------------------------------

  describe "Rule 1 — the connection password never appears in a returned term" do
    test "an auth failure carries only an error code, never the password" do
      socket =
        mock_client(fn srv, _test ->
          t_send(srv, build_handshake(salt: @salt), 0)
          {1, _resp} = t_recv_pkt(srv)
          t_send(srv, error_packet(1045), 4)
        end)

      assert {:error, {:auth_failed, 1045}} =
               result =
               Handshake.connect(socket, username: @username, password: @password, ssl: false)

      refute inspect(result) =~ "s3cr3t"
      refute inspect(result) =~ @password
    end
  end

  ## ---------------------------------------------------------------------------
  ## connect/2 — socket lifetime (connect owns the socket it is handed)
  ## ---------------------------------------------------------------------------

  describe "connect/2 — owns its socket lifetime on every error path" do
    test "closes the raw plaintext socket when authentication fails" do
      {:gen_tcp, raw} =
        socket =
        mock_client(fn srv, _test ->
          t_send(srv, build_handshake(salt: @salt), 0)
          {1, _resp} = t_recv_pkt(srv)
          t_send(srv, error_packet(1045), 4)
        end)

      assert {:error, {:auth_failed, 1045}} =
               Handshake.connect(socket, username: @username, password: @password, ssl: false)

      # Handshake.connect/2 owns the socket it is handed: on the {:error, _} path it
      # must close it. A still-open socket here is the pre-fix leak (the caller then
      # closed the RAW fd, which — after a TLS upgrade — would orphan the :ssl
      # process; on plaintext it double-freed the same fd from two owners).
      assert :inet.port(raw) == {:error, :einval},
             "connect/2 must close the socket it owns on an auth failure (it leaked open)"

      assert :gen_tcp.send(raw, <<0>>) == {:error, :closed}
    end

    test "a transport error reading the initial handshake fails closed :handshake_io_failed (no raise)" do
      # The server accepts then closes WITHOUT sending the initial handshake, so the client's
      # `Packet.read_packet` RAISES on the closed socket. Before the perform/4 rescue this raise
      # propagated out of connect/2 — crashing the caller (the Connection) and orphaning the raw
      # socket. It must instead be a value-free {:error, :handshake_io_failed}, socket freed.
      {:gen_tcp, raw} = socket = mock_client(fn srv, _test -> :gen_tcp.close(srv) end)

      assert {:error, :handshake_io_failed} =
               Handshake.connect(socket, username: @username, password: @password, ssl: false)

      assert :gen_tcp.send(raw, <<0>>) == {:error, :closed},
             "connect/2 must close the raw socket when the handshake read raises (it leaked open)"
    end

    test "a transport error DURING authentication (plaintext) fails closed :handshake_io_failed" do
      # The server sends a valid handshake and reads the response, then closes before the auth
      # result — so the client's `Packet.read_packet` inside authenticate/3 RAISES. On this
      # PLAINTEXT path `active == socket == {:gen_tcp, raw}`, so either handshake rescue frees
      # the same raw socket; the TLS test below is what ISOLATES authenticate_over/7's rescue
      # (where `active` is a distinct `{:ssl, _}`). Here we assert the fail-closed result + freed
      # socket for the plaintext auth-raise.
      {:gen_tcp, raw} =
        socket =
        mock_client(fn srv, _test ->
          t_send(srv, build_handshake(salt: @salt), 0)
          {1, _resp} = t_recv_pkt(srv)
          :gen_tcp.close(srv)
        end)

      assert {:error, :handshake_io_failed} =
               Handshake.connect(socket, username: @username, password: @password, ssl: false)

      assert :gen_tcp.send(raw, <<0>>) == {:error, :closed}
    end

    test "a transport error DURING auth over TLS fails closed and frees the :ssl socket, no orphan" do
      # Exercises the post-TLS-upgrade auth-raise path (`active` is the `{:ssl, _}` socket): the
      # server upgrades TLS, reads the HandshakeResponse, then closes — so the client's read_packet
      # DURING auth RAISES over the :ssl socket. The handshake rescues must fail closed
      # (:handshake_io_failed) AND leave no orphaned :ssl process. (`authenticate_over/7`'s rescue
      # closes `active` gracefully; `perform/4`'s backstop would also reap the :ssl process by
      # closing the raw transport — so the census stays flat, and it goes RED only if BOTH rescues
      # are removed. This is the TLS/:ssl-census counterpart to the plaintext auth-raise test.)
      # An orphaned :ssl process would grow the per-connection census by ~2 per connect.
      tls_opts = server_tls_opts()
      iterations = 10
      before = ssl_conn_procs()

      for _ <- 1..iterations do
        socket =
          mock_client(fn srv, _test ->
            t_send(srv, build_handshake(salt: @salt), 0)
            {1, _ssl_request} = t_recv_pkt(srv)
            {:ok, tls} = :ssl.handshake(srv, tls_opts, 5000)
            {2, _resp} = t_recv_pkt({:ssl, tls})
            # Close mid-auth (no auth result) — the client's next read RAISES over :ssl.
            :ssl.close(tls)
          end)

        assert {:error, :handshake_io_failed} =
                 Handshake.connect(socket,
                   username: @username,
                   password: @password,
                   ssl: true,
                   ssl_opts: [verify: :verify_none]
                 )
      end

      # Let any lagging gen_statem teardown reap before the census.
      Process.sleep(300)

      assert ssl_conn_procs() - before <= 2,
             "the :ssl socket must be freed on a post-upgrade auth raise (it orphaned)"
    end
  end

  ## ---------------------------------------------------------------------------
  ## connect/2 — CLIENT_CONNECT_WITH_DB
  ## ---------------------------------------------------------------------------

  describe "connect/2 — default database (CLIENT_CONNECT_WITH_DB)" do
    test "advertises the flag and appends the NUL-terminated database when database: is set" do
      socket =
        mock_client(fn srv, test ->
          t_send(srv, build_handshake(salt: @salt), 0)
          {1, resp} = t_recv_pkt(srv)
          send(test, {:resp, decode_response(resp)})
          t_send(srv, ok_packet(), 2)
        end)

      assert {:ok, _result} =
               Handshake.connect(socket,
                 username: @username,
                 password: @password,
                 ssl: false,
                 database: "capstan_db"
               )

      assert_receive {:resp, resp}
      assert (resp.capabilities &&& @client_connect_with_db) != 0
      assert resp.database == "capstan_db"
    end

    test "omits the flag and appends no database when database: is nil" do
      socket =
        mock_client(fn srv, test ->
          t_send(srv, build_handshake(salt: @salt), 0)
          {1, resp} = t_recv_pkt(srv)
          send(test, {:resp, decode_response(resp)})
          t_send(srv, ok_packet(), 2)
        end)

      assert {:ok, _result} =
               Handshake.connect(socket, username: @username, password: @password, ssl: false)

      assert_receive {:resp, resp}
      assert (resp.capabilities &&& @client_connect_with_db) == 0
      assert resp.database == nil
    end
  end

  ## ---------------------------------------------------------------------------
  ## Live probe — real substrate (excluded from the default suite)
  ## ---------------------------------------------------------------------------

  describe "connect/2 — live caching_sha2 over TLS against mysql-cdc-probe" do
    @tag :live
    test "authenticates as capstan_sha2 over TLS with verify_none AND with a cacertfile" do
      ca_path = fetch_substrate_ca()

      probes = [
        {[verify: :verify_none], "verify_none"},
        {[verify: :verify_peer, cacertfile: ca_path, server_name_indication: :disable],
         "cacertfile"}
      ]

      for {ssl_opts, label} <- probes do
        {:ok, raw} = :gen_tcp.connect(~c"127.0.0.1", 5633, [:binary, active: false], 10_000)

        assert {:ok, result} =
                 Handshake.connect({:gen_tcp, raw},
                   host: ~c"127.0.0.1",
                   username: "capstan_sha2",
                   password: "capstan_sha2_pw",
                   ssl: true,
                   ssl_opts: ssl_opts
                 ),
               "auth failed for #{label}"

        assert result.tls == true
        assert {:ssl, _} = result.socket
        assert current_user(result.socket) =~ "capstan_sha2"
        refute inspect(result) =~ "capstan_sha2_pw"

        {:ssl, sslsock} = result.socket
        :ssl.close(sslsock)
      end
    end
  end

  describe "connect/2 — live TLS-upgrade-then-auth-fail leaks no :ssl process" do
    @tag :live
    test "a failed auth after the TLS upgrade closes the :ssl socket (no orphaned gen_statem)" do
      iterations = 20
      before = ssl_conn_procs()

      for _ <- 1..iterations do
        {:ok, raw} = :gen_tcp.connect(~c"127.0.0.1", 5633, [:binary, active: false], 10_000)

        # capstan_sha2 is caching_sha2: TLS upgrades under verify_none, THEN the
        # wrong password is rejected — a post-upgrade failure, the leak-prone path.
        assert {:error, {:auth_failed, _code}} =
                 Handshake.connect({:gen_tcp, raw},
                   host: ~c"127.0.0.1",
                   username: "capstan_sha2",
                   password: "DELIBERATELY-WRONG",
                   ssl: true,
                   ssl_opts: [verify: :verify_none]
                 ),
               "expected a post-TLS-upgrade auth failure"
      end

      # Let any lagging gen_statem teardown reap before the census.
      Process.sleep(300)
      growth = ssl_conn_procs() - before

      # Pre-fix each failed connect orphans an :ssl_gen_statem (+ :tls_sender): the
      # caller frees the raw fd but the :ssl process is never closed, so growth
      # tracks `iterations` (measured: ~2 procs/connect). The fixed Handshake closes
      # the :ssl socket on the error path, so the census stays flat.
      assert growth <= 2,
             "orphaned :ssl processes leaked: grew by #{growth} over #{iterations} failed TLS connects"
    end
  end

  ## ---------------------------------------------------------------------------
  ## helpers
  ## ---------------------------------------------------------------------------

  # Counts the per-connection TLS processes (an :ssl_gen_statem plus its :tls_sender
  # sibling). Static ssl infrastructure (:ssl_manager, :ssl_pem_cache,
  # :tls_client_ticket_store) is excluded, so a leaked connection shows up as growth.
  defp ssl_conn_procs do
    Enum.count(Process.list(), &tls_connection_proc?/1)
  end

  defp tls_connection_proc?(pid) do
    with {:dictionary, dict} <- Process.info(pid, :dictionary),
         {mod, _f, _a} <- Keyword.get(dict, :"$initial_call") do
      mod in [:ssl_gen_statem, :tls_sender]
    else
      _ -> false
    end
  end

  # Serves `server_fun.(raw_socket, test_pid)` from a separate process and hands
  # the connected {:gen_tcp, _} client back to the test.
  defp mock_client(server_fun) do
    {:ok, listen} =
      :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true, ip: @loopback])

    {:ok, port} = :inet.port(listen)
    test = self()

    spawn_link(fn ->
      {:ok, srv} = :gen_tcp.accept(listen, 5000)
      :gen_tcp.close(listen)
      server_fun.(srv, test)
      Process.sleep(100)
      :gen_tcp.close(srv)
    end)

    {:ok, client} = :gen_tcp.connect(@loopback, port, [:binary, active: false], 5000)
    on_exit(fn -> :gen_tcp.close(client) end)
    {:gen_tcp, client}
  end

  defp build_handshake(opts) do
    version = Keyword.get(opts, :server_version, "8.0.46")
    conn_id = Keyword.get(opts, :connection_id, 1)
    salt = Keyword.get(opts, :salt, @salt)
    plugin = Keyword.get(opts, :plugin, "caching_sha2_password")
    cap_low = Keyword.get(opts, :cap_low, 0xFFFF)
    cap_high = Keyword.get(opts, :cap_high, 0xFFFF)

    <<salt1::binary-size(8), salt2::binary-size(12)>> = salt

    <<10>> <>
      version <>
      <<0>> <>
      <<conn_id::32-little>> <>
      salt1 <>
      <<0>> <>
      <<cap_low::16-little, 45::8, 0::16-little, cap_high::16-little, 21::8, 0::80>> <>
      salt2 <>
      <<0>> <>
      plugin <>
      <<0>>
  end

  defp decode_response(payload) do
    <<caps::32-little, _max::32-little, _charset::8, _reserved::binary-size(23), rest::binary>> =
      payload

    [username, rest] = :binary.split(rest, <<0>>)
    <<token_len::8, token::binary-size(token_len), rest::binary>> = rest

    {database, rest} =
      if (caps &&& @client_connect_with_db) != 0 do
        [db, tail] = :binary.split(rest, <<0>>)
        {db, tail}
      else
        {nil, rest}
      end

    [plugin, _] = :binary.split(rest, <<0>>)
    %{capabilities: caps, username: username, token: token, plugin: plugin, database: database}
  end

  defp ssl_request_has_ssl_flag?(<<caps::32-little, _::binary>>), do: (caps &&& @client_ssl) != 0

  defp auth_switch(plugin, nonce), do: <<0xFE>> <> plugin <> <<0>> <> nonce <> <<0>>

  defp ok_packet, do: <<0x00, 0, 0, 2, 0, 0, 0>>

  defp error_packet(code), do: <<0xFF, code::16-little, "#28000Access denied">>

  defp t_send({:gen_tcp, s}, payload, seq), do: :ok = :gen_tcp.send(s, frame(payload, seq))
  defp t_send({:ssl, s}, payload, seq), do: :ok = :ssl.send(s, frame(payload, seq))
  defp t_send(raw, payload, seq), do: t_send({:gen_tcp, raw}, payload, seq)

  defp t_recv_pkt({:gen_tcp, s}), do: recv_pkt(fn n -> :gen_tcp.recv(s, n, 5000) end)
  defp t_recv_pkt({:ssl, s}), do: recv_pkt(fn n -> :ssl.recv(s, n, 5000) end)
  defp t_recv_pkt(raw), do: t_recv_pkt({:gen_tcp, raw})

  defp recv_pkt(recv) do
    {:ok, <<len::24-little, seq::8>>} = recv.(4)

    payload =
      if len == 0,
        do: <<>>,
        else:
          (fn ->
             {:ok, p} = recv.(len)
             p
           end).()

    {seq, payload}
  end

  defp frame(payload, seq), do: <<byte_size(payload)::24-little, seq::8, payload::binary>>

  defp generate_rsa do
    priv = :public_key.generate_key({:rsa, 2048, 65_537})
    {:RSAPrivateKey, _v, n, e, _d, _p, _q, _e1, _e2, _c, _o} = priv
    pub = {:RSAPublicKey, n, e}
    pem = :public_key.pem_encode([:public_key.pem_entry_encode(:SubjectPublicKeyInfo, pub)])
    {pem, priv}
  end

  defp unxor(data, nonce) do
    nonce_len = byte_size(nonce)

    data
    |> :binary.bin_to_list()
    |> Enum.with_index()
    |> Enum.map(fn {byte, i} -> bxor(byte, :binary.at(nonce, rem(i, nonce_len))) end)
    |> :binary.list_to_bin()
  end

  defp server_tls_opts do
    key = fn -> :public_key.generate_key({:rsa, 2048, 65_537}) end

    %{server_config: conf} =
      :public_key.pkix_test_data(%{
        server_chain: %{root: [key: key.()], intermediates: [], peer: [key: key.()]},
        client_chain: %{root: [key: key.()], intermediates: [], peer: [key: key.()]}
      })

    [cert: conf[:cert], key: conf[:key]]
  end

  defp fetch_substrate_ca do
    {ca, 0} = System.cmd("docker", ["exec", "mysql-cdc-probe", "cat", "/var/lib/mysql/ca.pem"])

    path =
      Path.join(
        System.tmp_dir!(),
        "capstan-substrate-ca-#{System.unique_integer([:positive])}.pem"
      )

    File.write!(path, ca)
    on_exit(fn -> File.rm_rf(path) end)
    String.to_charlist(path)
  end

  defp current_user(socket) do
    :ok = Packet.send_packet(socket, <<0x03, "SELECT CURRENT_USER()">>, 0)
    {_, <<1>>} = Packet.read_packet(socket)
    {_, _column_def} = Packet.read_packet(socket)
    {_, row} = Packet.read_packet(socket)
    {value, _} = Packet.lenenc_str(row)
    value
  end
end

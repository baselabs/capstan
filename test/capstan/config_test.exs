defmodule Capstan.ConfigTest do
  use ExUnit.Case, async: true

  alias Capstan.Config
  alias Capstan.Protocol.Handshake

  @loopback {127, 0, 0, 1}

  ## ---------------------------------------------------------------------------
  ## Option validation (PURE) — server_id
  ## ---------------------------------------------------------------------------

  describe "validate/1 — server_id (required, positive)" do
    test "accepts a positive integer server_id and carries it into the resolved config" do
      assert {:ok, resolved} = Config.validate(opts())
      assert resolved.server_id == 5115
    end

    test "a missing server_id fails closed with a distinct reason" do
      assert {:error, :server_id_required} = Config.validate(connection: valid_connection())
    end

    test "a zero server_id is refused (must be positive)" do
      assert {:error, :server_id_required} = Config.validate(opts(server_id: 0))
    end

    test "a negative server_id is refused" do
      assert {:error, :server_id_required} = Config.validate(opts(server_id: -1))
    end

    test "a non-integer server_id is refused" do
      assert {:error, :server_id_required} = Config.validate(opts(server_id: "5115"))
    end
  end

  ## ---------------------------------------------------------------------------
  ## Option validation — connection coordinates (host / port / username / …)
  ## ---------------------------------------------------------------------------

  describe "validate/1 — connection shape" do
    test "a missing connection is a config error" do
      assert {:error, :config_invalid} = Config.validate(server_id: 5115)
    end

    test "a non-keyword connection is a config error" do
      assert {:error, :config_invalid} = Config.validate(server_id: 5115, connection: "nope")
    end

    test "a missing host is a config error" do
      conn = [port: 5633, username: "root", ssl: false]
      assert {:error, :config_invalid} = Config.validate(server_id: 5115, connection: conn)
    end

    test "a missing port is a config error" do
      conn = [host: "127.0.0.1", username: "root", ssl: false]
      assert {:error, :config_invalid} = Config.validate(server_id: 5115, connection: conn)
    end

    test "a missing username is a config error" do
      conn = [host: "127.0.0.1", port: 5633, ssl: false]
      assert {:error, :config_invalid} = Config.validate(server_id: 5115, connection: conn)
    end

    test "a charlist host is accepted" do
      conn = [host: ~c"127.0.0.1", port: 5633, username: "root", ssl: false]
      assert {:ok, resolved} = Config.validate(server_id: 5115, connection: conn)
      assert resolved.connection[:host] == ~c"127.0.0.1"
    end

    test "password defaults to an empty string when omitted" do
      conn = [host: "127.0.0.1", port: 5633, username: "root", ssl: false]
      assert {:ok, resolved} = Config.validate(server_id: 5115, connection: conn)
      assert resolved.connection[:password] == ""
    end

    test "database defaults to nil when omitted and is carried through when given" do
      assert {:ok, resolved} = Config.validate(opts())
      assert resolved.connection[:database] == nil

      assert {:ok, with_db} = Config.validate(opts([], database: "app"))
      assert with_db.connection[:database] == "app"
    end
  end

  ## ---------------------------------------------------------------------------
  ## Option validation — ssl default + F6 fail-closed TLS verification
  ## ---------------------------------------------------------------------------

  describe "validate/1 — ssl defaults true and F6 TLS verification (Q17)" do
    test "ssl defaults to true (an omitted ssl with an explicit verify resolves ssl: true)" do
      conn = [host: "127.0.0.1", port: 5633, username: "root", ssl_opts: [verify: :verify_none]]
      assert {:ok, resolved} = Config.validate(server_id: 5115, connection: conn)
      assert resolved.connection[:ssl] == true
    end

    test "ssl defaulting to true with NO verification choice fails closed (proves the default)" do
      conn = [host: "127.0.0.1", port: 5633, username: "root"]

      assert {:error, :tls_verification_unspecified} =
               Config.validate(server_id: 5115, connection: conn)
    end

    test "ssl: true with empty ssl_opts fails closed :tls_verification_unspecified (F6)" do
      assert {:error, :tls_verification_unspecified} =
               Config.validate(opts([], ssl: true, ssl_opts: []))
    end

    test "ssl: true with an explicit verify passes" do
      assert {:ok, resolved} =
               Config.validate(opts([], ssl: true, ssl_opts: [verify: :verify_none]))

      assert resolved.connection[:ssl] == true
    end

    test "ssl: true with a cacertfile passes, and the ssl_opts are carried through unchanged" do
      assert {:ok, resolved} =
               Config.validate(opts([], ssl: true, ssl_opts: [cacertfile: "/etc/ca.pem"]))

      assert resolved.connection[:ssl_opts] == [cacertfile: "/etc/ca.pem"]
    end

    test "ssl: true with cacerts passes" do
      assert {:ok, _resolved} = Config.validate(opts([], ssl: true, ssl_opts: [cacerts: []]))
    end

    test "ssl: false passes with no verification opts and resolves ssl: false" do
      assert {:ok, resolved} = Config.validate(opts([], ssl: false))
      assert resolved.connection[:ssl] == false
    end

    test "a non-boolean ssl is a config error" do
      assert {:error, :config_invalid} = Config.validate(opts([], ssl: :yes))
    end

    test "a non-keyword ssl_opts is a config error" do
      assert {:error, :config_invalid} =
               Config.validate(opts([], ssl: true, ssl_opts: :verify_none))
    end
  end

  ## ---------------------------------------------------------------------------
  ## Option validation — auth_plugins default + validation
  ## ---------------------------------------------------------------------------

  describe "validate/1 — auth_plugins" do
    test "defaults to [:caching_sha2_password]" do
      assert {:ok, resolved} = Config.validate(opts())
      assert resolved.connection[:auth_plugins] == [:caching_sha2_password]
    end

    test "an explicit auth_plugins list is respected" do
      assert {:ok, resolved} = Config.validate(opts([], auth_plugins: [:mysql_native_password]))
      assert resolved.connection[:auth_plugins] == [:mysql_native_password]
    end

    test "an unknown auth plugin is a config error" do
      assert {:error, :config_invalid} = Config.validate(opts([], auth_plugins: [:bogus]))
    end

    test "an empty auth_plugins list is a config error" do
      assert {:error, :config_invalid} = Config.validate(opts([], auth_plugins: []))
    end

    test "a non-list auth_plugins is a config error" do
      assert {:error, :config_invalid} =
               Config.validate(opts([], auth_plugins: :caching_sha2_password))
    end
  end

  ## ---------------------------------------------------------------------------
  ## Option validation — max_command_retries default + non-negative
  ## ---------------------------------------------------------------------------

  describe "validate/1 — max_command_retries (default 5, rejects negatives)" do
    test "defaults to 5" do
      assert {:ok, resolved} = Config.validate(opts())
      assert resolved.max_command_retries == 5
    end

    test "an explicit value is respected" do
      assert {:ok, resolved} = Config.validate(opts(max_command_retries: 3))
      assert resolved.max_command_retries == 3
    end

    test "zero is allowed (halt-now, not a negative)" do
      assert {:ok, resolved} = Config.validate(opts(max_command_retries: 0))
      assert resolved.max_command_retries == 0
    end

    test "a negative value is rejected" do
      assert {:error, :config_invalid} = Config.validate(opts(max_command_retries: -1))
    end

    test "a non-integer value is rejected" do
      assert {:error, :config_invalid} = Config.validate(opts(max_command_retries: "5"))
    end
  end

  ## ---------------------------------------------------------------------------
  ## Precondition gate (I/O) — the five variables, each a DISTINCT refusal (Q5)
  ##
  ## The gate is fed a CONSTRUCTED text resultset over a mock socket — the required
  ## non-vacuity proof that each refusal can go RED (design § Tripwires). Every value
  ## on the wire is a TEXT STRING (MySQL simple-query results, replicant's A5 class);
  ## an empty `binlog_row_value_options` arrives as "" (not nil, not 0).
  ## ---------------------------------------------------------------------------

  describe "check_preconditions/1 — fail-closed gate (Q5)" do
    test "all five correct → :ok (empty binlog_row_value_options accepted as text)" do
      assert :ok = check_result(["ROW", "FULL", "FULL", "", "ON"])
    end

    test "binlog_format ≠ ROW → :binlog_format_not_row" do
      assert {:error, :binlog_format_not_row} =
               check_result(["STATEMENT", "FULL", "FULL", "", "ON"])
    end

    test "binlog_row_image ≠ FULL → :binlog_row_image_not_full" do
      assert {:error, :binlog_row_image_not_full} =
               check_result(["ROW", "MINIMAL", "FULL", "", "ON"])
    end

    test "binlog_row_metadata ≠ FULL → :binlog_row_metadata_not_full" do
      assert {:error, :binlog_row_metadata_not_full} =
               check_result(["ROW", "FULL", "MINIMAL", "", "ON"])
    end

    test "binlog_row_value_options non-empty (PARTIAL_JSON) → :binlog_row_value_options_not_empty" do
      assert {:error, :binlog_row_value_options_not_empty} =
               check_result(["ROW", "FULL", "FULL", "PARTIAL_JSON", "ON"])
    end

    test "gtid_mode ≠ ON → :gtid_mode_not_on" do
      assert {:error, :gtid_mode_not_on} = check_result(["ROW", "FULL", "FULL", "", "OFF"])
    end

    test "a server error while reading the variables propagates fail-closed, never :ok" do
      assert {:error, {:query_error, 1227}} = query_error_result(1227)
    end
  end

  ## ---------------------------------------------------------------------------
  ## Precondition gate — live PASS against mysql-cdc-probe (Step 3)
  ##
  ## The substrate has all five variables correct (ROW/FULL/FULL/''/ON). Excluded
  ## from the default suite; run with `mix test --only live`.
  ## ---------------------------------------------------------------------------

  describe "check_preconditions/1 — live PASS against mysql-cdc-probe" do
    @describetag :live

    test "the substrate satisfies all five preconditions" do
      socket = live_connect()
      assert :ok = Config.check_preconditions(socket)
      close(socket)
    end
  end

  ## ---------------------------------------------------------------------------
  ## helpers — option builders
  ## ---------------------------------------------------------------------------

  # A valid baseline connection (ssl: false so the baseline needs no TLS-verify
  # choice). `conn` overrides merge on top; TLS/auth tests override deliberately.
  defp valid_connection(conn \\ []) do
    Keyword.merge(
      [host: "127.0.0.1", port: 5633, username: "root", password: "probe", ssl: false],
      conn
    )
  end

  defp opts(top \\ [], conn \\ []) do
    Keyword.merge([server_id: 5115, connection: valid_connection(conn)], top)
  end

  ## ---------------------------------------------------------------------------
  ## helpers — precondition gate over a mock socket
  ## ---------------------------------------------------------------------------

  # Serves a single five-column text resultset carrying `values`, then drives the
  # real gate over it (query issuance → text-resultset decode → evaluation).
  defp check_result(values) do
    socket =
      mock_client(fn srv ->
        {0, _request} = t_recv_pkt(srv)
        t_send(srv, <<5>>, 1)
        Enum.each(1..5, fn i -> t_send(srv, "coldef_#{i}", i + 1) end)
        t_send(srv, encode_text_row(values), 7)
        # DEPRECATE_EOF terminator: a 0xFE header with a payload shorter than 9 bytes.
        t_send(srv, <<0xFE, 0, 0, 2, 0, 0, 0>>, 8)
      end)

    Config.check_preconditions(socket)
  end

  # Serves an ERR packet in place of the resultset — the gate must surface it, never
  # silently pass.
  defp query_error_result(code) do
    socket =
      mock_client(fn srv ->
        {0, _request} = t_recv_pkt(srv)
        t_send(srv, <<0xFF, code::16-little, "#HY000the variables read failed">>, 1)
      end)

    Config.check_preconditions(socket)
  end

  # Length-encoded text row: each value < 251 bytes, so a single-byte length prefix.
  # An empty string encodes as <<0>> and decodes back to "".
  defp encode_text_row(values) do
    Enum.reduce(values, <<>>, fn value, acc -> acc <> <<byte_size(value)::8, value::binary>> end)
  end

  defp mock_client(server_fun) do
    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true, ip: @loopback])
    {:ok, port} = :inet.port(listen)

    spawn_link(fn ->
      {:ok, srv} = :gen_tcp.accept(listen, 5000)
      :gen_tcp.close(listen)
      server_fun.({:gen_tcp, srv})
      Process.sleep(100)
      :gen_tcp.close(srv)
    end)

    {:ok, client} = :gen_tcp.connect(@loopback, port, [:binary, active: false], 5000)
    on_exit(fn -> :gen_tcp.close(client) end)
    {:gen_tcp, client}
  end

  defp t_send({:gen_tcp, s}, payload, seq), do: :ok = :gen_tcp.send(s, frame(payload, seq))

  defp frame(payload, seq), do: <<byte_size(payload)::24-little, seq::8, payload::binary>>

  defp t_recv_pkt({:gen_tcp, s}) do
    {:ok, <<len::24-little, seq::8>>} = :gen_tcp.recv(s, 4, 5000)
    {:ok, payload} = :gen_tcp.recv(s, len, 5000)
    {seq, payload}
  end

  ## ---------------------------------------------------------------------------
  ## helpers — live substrate
  ## ---------------------------------------------------------------------------

  defp live_connect do
    {:ok, raw} = :gen_tcp.connect(~c"127.0.0.1", 5633, [:binary, active: false], 10_000)

    {:ok, result} =
      Handshake.connect({:gen_tcp, raw},
        host: ~c"127.0.0.1",
        username: "root",
        password: "probe",
        ssl: false,
        auth_plugins: [:mysql_native_password]
      )

    result.socket
  end

  defp close({:gen_tcp, s}), do: :gen_tcp.close(s)
  defp close({:ssl, s}), do: :ssl.close(s)
end

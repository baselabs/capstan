defmodule Capstan.QueryTest do
  @moduledoc """
  `Capstan.Query` — the `COM_QUERY`-only snapshot connection (C2 Task 4).

  Two vector classes:

    * **Unit** (default suite) — a scripted mock MySQL server (the `Config` mock-socket
      idiom) drives the REAL `Command.query`/`Config.check_preconditions` path over a real
      loopback socket, so the source-identity re-check, the bad-substrate refusal, the
      connect-failure mapping, and Rule 1 are all exercised against genuine framing (never a
      hollow double).
    * **Live** (`@tag :live`, excluded by default — `mix test --only live`) — the marquee:
      establish over TLS as `capstan_sha2` against `mysql-cdc-probe`, read `@@server_uuid` /
      `@@gtid_executed`, run a `SELECT` and a `LOCK TABLES … READ` / `UNLOCK`, and force a
      source-identity mismatch against a fabricated pin.
  """
  use ExUnit.Case, async: false

  alias Capstan.Protocol.Command
  alias Capstan.Protocol.Handshake
  alias Capstan.Query

  @loopback {127, 0, 0, 1}
  @sentinel_password "SENTINEL-PW-must-never-leak-42"

  # Two arbitrary, distinct server-identity strings for the stub vectors (shape-only; the real
  # 36-char uuid is read live in the marquee, never hard-coded in the lib).
  @uuid_a "aaaaaaaa-1111-2222-3333-444444444444"
  @uuid_b "bbbbbbbb-5555-6666-7777-888888888888"

  # A correct five-variable precondition row (ADR-0002: all text; empty binlog_row_value_options
  # is "" not nil) and its resultset shape.
  @good_precond {6, [["ROW", "FULL", "FULL", "", "ON", "0"]]}

  defp precond_row(image), do: {6, [["ROW", image, "FULL", "", "ON", "0"]]}
  defp uuid_result(uuid), do: {1, [[uuid]]}

  # A full successful-establish sequence: the precondition query, then the @@server_uuid read.
  defp ok_sequence(uuid), do: [@good_precond, uuid_result(uuid)]

  ## ---------------------------------------------------------------------------
  ## Source-identity — the PURE guard (Ch8 / tripwire 15). Guard-the-guard.
  ## ---------------------------------------------------------------------------

  describe "verify_source_identity/2 — pinned vs observed @@server_uuid" do
    test "no pin yet (first establish) accepts any observed uuid and sets the pin" do
      assert :ok = Query.verify_source_identity(nil, @uuid_a)
    end

    test "a matching observed uuid passes" do
      assert :ok = Query.verify_source_identity(@uuid_a, @uuid_a)
    end

    test "a differing observed uuid halts :snapshot_source_mismatch" do
      assert {:error, :snapshot_source_mismatch} =
               Query.verify_source_identity(@uuid_a, @uuid_b)
    end
  end

  ## ---------------------------------------------------------------------------
  ## establish/1 — reads @@server_uuid, checks preconditions, pins identity
  ## ---------------------------------------------------------------------------

  describe "establish/1 — first connect pins the endpoint + @@server_uuid" do
    test "reads @@server_uuid over a real COM_QUERY and pins it" do
      connect_fun = scripted_connect_fun([ok_sequence(@uuid_a)])

      assert {:ok, q} = Query.establish(connection: fake_conn(), connect_fun: connect_fun)
      assert Query.server_uuid(q) == @uuid_a
      assert Query.endpoint(q) == {~c"127.0.0.1", Capstan.MysqlCase.shared_port()}
    end

    test "an explicit :expected_server_uuid (the stream conn's identity) is verified at connect" do
      connect_fun = scripted_connect_fun([ok_sequence(@uuid_a)])

      # Bootstrap (Task 10) pins the query conn against the STREAM conn's uuid (design Ch8).
      assert {:ok, q} =
               Query.establish(
                 connection: fake_conn(),
                 connect_fun: connect_fun,
                 expected_server_uuid: @uuid_a
               )

      assert Query.server_uuid(q) == @uuid_a
    end

    test "an :expected_server_uuid that differs from the observed identity halts at connect" do
      connect_fun = scripted_connect_fun([ok_sequence(@uuid_b)])

      assert {:error, :snapshot_source_mismatch} =
               Query.establish(
                 connection: fake_conn(),
                 connect_fun: connect_fun,
                 expected_server_uuid: @uuid_a
               )
    end
  end

  ## ---------------------------------------------------------------------------
  ## reestablish/1 — RE-verifies @@server_uuid on EVERY reconnect (tripwire 15)
  ##
  ## RED: skip the reconnect re-check → a reconnect to a different-uuid server goes
  ## unnoticed. The stub's SECOND establish reports a different uuid; asserting the
  ## mismatch is what goes RED if the re-check is dropped.
  ## ---------------------------------------------------------------------------

  describe "reestablish/1 — source-identity re-check on reconnect (Ch8)" do
    test "a reconnect to a DIFFERENT-uuid server halts :snapshot_source_mismatch" do
      # Call 1 (establish): server reports @uuid_a → pinned. Call 2 (reestablish): a DIFFERENT
      # server reports @uuid_b. A dropped re-check would return {:ok, _} — this assertion is RED.
      connect_fun = scripted_connect_fun([ok_sequence(@uuid_a), ok_sequence(@uuid_b)])

      assert {:ok, q} = Query.establish(connection: fake_conn(), connect_fun: connect_fun)
      assert Query.server_uuid(q) == @uuid_a

      assert {:error, :snapshot_source_mismatch} = Query.reestablish(q)
    end

    test "a reconnect to the SAME-uuid server succeeds (non-vacuity of the re-check)" do
      connect_fun = scripted_connect_fun([ok_sequence(@uuid_a), ok_sequence(@uuid_a)])

      assert {:ok, q} = Query.establish(connection: fake_conn(), connect_fun: connect_fun)
      assert {:ok, q2} = Query.reestablish(q)
      assert Query.server_uuid(q2) == @uuid_a
    end
  end

  ## ---------------------------------------------------------------------------
  ## check_preconditions/1 reuse — refuse a bad substrate (binlog_row_image=FULL
  ## is a hard reconciliation dependency)
  ## ---------------------------------------------------------------------------

  describe "establish/1 — reuses Config.check_preconditions/1 and refuses a bad substrate" do
    test "binlog_row_image ≠ FULL is refused with its distinct reason (no pin set)" do
      # Only the precondition query is served; establish must refuse BEFORE reading the uuid.
      connect_fun = scripted_connect_fun([[precond_row("MINIMAL")]])

      assert {:error, :binlog_row_image_not_full} =
               Query.establish(connection: fake_conn(), connect_fun: connect_fun)
    end

    test "a correctly-configured substrate passes the precondition gate and establishes" do
      connect_fun = scripted_connect_fun([ok_sequence(@uuid_a)])
      assert {:ok, _q} = Query.establish(connection: fake_conn(), connect_fun: connect_fun)
    end
  end

  ## ---------------------------------------------------------------------------
  ## connect/auth failure → :snapshot_query_connect_failed (budgeted via the shared counter)
  ## ---------------------------------------------------------------------------

  describe "establish/1 — connect/auth failure maps to :snapshot_query_connect_failed" do
    test "a connect_fun error halts :snapshot_query_connect_failed after the budget (max=0 = now)" do
      {connect_fun, count} = counting_failing_connect_fun()

      assert {:error, :snapshot_query_connect_failed} =
               Query.establish(
                 connection: fake_conn(),
                 connect_fun: connect_fun,
                 max_command_retries: 0
               )

      # max_command_retries: 0 ⇒ a single attempt (halt-now), no re-derived counter.
      assert Agent.get(count, & &1) == 1
    end

    test "a transient connect error is retried the budgeted number of times, then halts" do
      {connect_fun, count} = counting_failing_connect_fun()

      assert {:error, :snapshot_query_connect_failed} =
               Query.establish(
                 connection: fake_conn(),
                 connect_fun: connect_fun,
                 max_command_retries: 3
               )

      # retry_decision/2: attempts 0,1,2 retry; attempt 3 halts ⇒ 4 total invocations.
      assert Agent.get(count, & &1) == 4
    end
  end

  ## ---------------------------------------------------------------------------
  ## query/2 — text resultset over the pinned socket
  ## ---------------------------------------------------------------------------

  describe "query/2 — executes a COM_QUERY and returns rows" do
    test "returns {:ok, rows} for a resultset and {:ok, []} for a no-resultset statement" do
      # After establish (precond + uuid), serve one extra resultset for the ad-hoc SELECT, then
      # an OK packet for a no-resultset statement (LOCK/SET/UNLOCK).
      connect_fun =
        scripted_connect_fun([
          ok_sequence(@uuid_a) ++ [{2, [["1", "two"]]}, :ok_packet]
        ])

      assert {:ok, q} = Query.establish(connection: fake_conn(), connect_fun: connect_fun)
      assert {:ok, [["1", "two"]]} = Query.query(q, "SELECT 1, 'two'")
      assert {:ok, []} = Query.query(q, "LOCK TABLES t READ")
    end
  end

  ## ---------------------------------------------------------------------------
  ## Rule 1 — the connection password reaches no returned term or error
  ## ---------------------------------------------------------------------------

  describe "Rule 1 — the connection password appears in no returned term or error" do
    test "inspect(query) elides the connection (which carries the password)" do
      connect_fun = scripted_connect_fun([ok_sequence(@uuid_a)])

      assert {:ok, q} =
               Query.establish(
                 connection: fake_conn(@sentinel_password),
                 connect_fun: connect_fun
               )

      rendered = inspect(q)
      refute rendered =~ @sentinel_password
      # …while still rendering the safe structural identity, so the elision isn't vacuous.
      assert rendered =~ @uuid_a
    end

    test "a connect-failure error carries no password (bare value-free atom)" do
      {connect_fun, _count} = counting_failing_connect_fun()

      assert {:error, reason} =
               Query.establish(
                 connection: fake_conn(@sentinel_password),
                 connect_fun: connect_fun,
                 max_command_retries: 0
               )

      assert is_atom(reason)
      refute inspect(reason) =~ @sentinel_password
    end
  end

  ## ===========================================================================
  ## Live marquee — real substrate, excluded by default (mix test --only live)
  ## ===========================================================================

  describe "live — establish over TLS + COM_QUERY against mysql-cdc-probe" do
    @describetag :live

    test "authenticates as capstan_sha2 over TLS, reads identity, runs SELECT + LOCK/UNLOCK" do
      assert {:ok, q} =
               Query.establish(connection: live_conn(), connect_fun: &Query.default_connect/1)

      # A real 36-char @@server_uuid is pinned (read live, never hard-coded).
      uuid = Query.server_uuid(q)
      assert is_binary(uuid) and byte_size(uuid) == 36

      # @@gtid_executed reads back as a canonical string.
      assert {:ok, [[executed]]} = Query.query(q, "SELECT @@global.gtid_executed")
      assert is_binary(executed)

      # An ad-hoc SELECT resultset decodes.
      assert {:ok, [["1"]]} = Query.query(q, "SELECT 1")

      # The brief-lock primitives Task 5 needs: LOCK TABLES … READ / UNLOCK (no resultset).
      assert {:ok, []} = Query.query(q, "LOCK TABLES probe_db.capstan_query_marquee READ")
      assert {:ok, []} = Query.query(q, "UNLOCK TABLES")

      Query.close(q)
    end

    test "a reconnect against a FABRICATED pin halts :snapshot_source_mismatch (Ch8)" do
      assert {:ok, q} =
               Query.establish(connection: live_conn(), connect_fun: &Query.default_connect/1)

      real_uuid = Query.server_uuid(q)
      Query.close(q)

      # Fabricate a pin that cannot match the live server → a reconnect re-check must halt.
      fabricated = %{q | server_uuid: "00000000-0000-0000-0000-000000000000"}
      refute Query.server_uuid(fabricated) == real_uuid

      assert {:error, :snapshot_source_mismatch} = Query.reestablish(fabricated)
    end

    test "the connection password appears in no returned term (Rule 1, live)" do
      assert {:ok, q} =
               Query.establish(connection: live_conn(), connect_fun: &Query.default_connect/1)

      refute inspect(q) =~ "capstan_sha2_pw"
      Query.close(q)
    end
  end

  ## ---------------------------------------------------------------------------
  ## setup — the live marquee's throwaway lock target (created idempotently)
  ## ---------------------------------------------------------------------------

  setup_all do
    if :live in ExUnit.configuration()[:include] do
      socket = live_root_socket()

      try do
        run!(socket, "CREATE DATABASE IF NOT EXISTS probe_db")

        run!(
          socket,
          "CREATE TABLE IF NOT EXISTS probe_db.capstan_query_marquee (id INT PRIMARY KEY)"
        )

        ensure_sha2_user!(socket)
      after
        close_socket(socket)
      end
    end

    :ok
  end

  ## ---------------------------------------------------------------------------
  ## helpers — connection opt shapes
  ## ---------------------------------------------------------------------------

  defp fake_conn(password \\ @sentinel_password) do
    [
      host: "127.0.0.1",
      port: Capstan.MysqlCase.shared_port(),
      username: "capstan_sha2",
      password: password,
      ssl: false
    ]
  end

  defp live_conn do
    [
      host: "127.0.0.1",
      port: Capstan.MysqlCase.shared_port(),
      username: "capstan_sha2",
      password: "capstan_sha2_pw",
      database: "probe_db",
      ssl: true,
      ssl_opts: [verify: :verify_none]
    ]
  end

  ## ---------------------------------------------------------------------------
  ## helpers — scripted mock MySQL server (the Config mock-socket idiom, sequenced)
  ##
  ## `sequences` is a list of per-CONNECTION scripts; the connect_fun hands out the next
  ## on each call (establish, then reestablish). Each script is an ordered list of
  ## responses, one served per incoming COM_QUERY: `{ncols, rows}` for a text resultset,
  ## or `:ok_packet` for a no-resultset statement.
  ## ---------------------------------------------------------------------------

  defp scripted_connect_fun(sequences) do
    {:ok, agent} = Agent.start_link(fn -> sequences end)
    on_exit(fn -> if Process.alive?(agent), do: Agent.stop(agent) end)

    fn _connection ->
      case pop_script(agent) do
        :none -> {:error, :no_more_scripts}
        responses -> {:ok, spawn_mock_server(responses), %{}}
      end
    end
  end

  defp pop_script(agent) do
    Agent.get_and_update(agent, fn
      [head | tail] -> {head, tail}
      [] -> {:none, []}
    end)
  end

  # A connect_fun that always fails, counting its invocations so the retry budget is provable.
  defp counting_failing_connect_fun do
    {:ok, count} = Agent.start_link(fn -> 0 end)
    on_exit(fn -> if Process.alive?(count), do: Agent.stop(count) end)

    fun = fn _connection ->
      Agent.update(count, &(&1 + 1))
      {:error, :econnrefused}
    end

    {fun, count}
  end

  defp spawn_mock_server(responses) do
    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true, ip: @loopback])
    {:ok, port} = :inet.port(listen)

    # Unlinked: a mock that outlives its (closed) client must not signal the test process.
    spawn(fn ->
      {:ok, srv} = :gen_tcp.accept(listen, 5000)
      :gen_tcp.close(listen)
      serve({:gen_tcp, srv}, responses)
      :gen_tcp.close(srv)
    end)

    {:ok, client} = :gen_tcp.connect(@loopback, port, [:binary, active: false], 5000)
    on_exit(fn -> :gen_tcp.close(client) end)
    {:gen_tcp, client}
  end

  # Serve each scripted response in order, one per incoming request; stop cleanly if the
  # client closes early (a refused establish never sends its follow-up query).
  defp serve(srv, responses) do
    Enum.reduce_while(responses, :ok, fn response, _acc ->
      case recv_request(srv) do
        :closed ->
          {:halt, :ok}

        :ok ->
          send_response(srv, response)
          {:cont, :ok}
      end
    end)
  end

  defp recv_request({:gen_tcp, s}) do
    case :gen_tcp.recv(s, 4, 5000) do
      {:ok, <<len::24-little, _seq::8>>} ->
        {:ok, _payload} = :gen_tcp.recv(s, len, 5000)
        :ok

      {:error, _reason} ->
        :closed
    end
  end

  defp send_response(srv, :ok_packet), do: t_send(srv, <<0x00, 0, 0, 2, 0, 0, 0>>, 1)

  defp send_response(srv, {ncols, rows}) do
    t_send(srv, <<ncols>>, 1)
    Enum.each(1..ncols, fn i -> t_send(srv, "coldef_#{i}", i + 1) end)

    rows
    |> Enum.with_index(ncols + 2)
    |> Enum.each(fn {row, seq} -> t_send(srv, encode_text_row(row), seq) end)

    # DEPRECATE_EOF terminator: a 0xFE header with a payload shorter than 8 bytes.
    t_send(srv, <<0xFE, 0, 0, 2, 0, 0, 0>>, ncols + 2 + length(rows))
  end

  # Each value < 251 bytes ⇒ a single-byte length prefix; "" encodes as <<0>> and decodes to "".
  defp encode_text_row(values) do
    Enum.reduce(values, <<>>, fn value, acc -> acc <> <<byte_size(value)::8, value::binary>> end)
  end

  defp t_send({:gen_tcp, s}, payload, seq), do: :ok = :gen_tcp.send(s, frame(payload, seq))
  defp frame(payload, seq), do: <<byte_size(payload)::24-little, seq::8, payload::binary>>

  ## ---------------------------------------------------------------------------
  ## helpers — live substrate (root planter + sha2 user provisioning)
  ## ---------------------------------------------------------------------------

  defp live_root_socket do
    {:ok, raw} =
      :gen_tcp.connect(
        ~c"127.0.0.1",
        Capstan.MysqlCase.shared_port(),
        [:binary, active: false],
        10_000
      )

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

  defp ensure_sha2_user!(socket) do
    run!(
      socket,
      "CREATE USER IF NOT EXISTS 'capstan_sha2'@'%' " <>
        "IDENTIFIED WITH caching_sha2_password BY 'capstan_sha2_pw'"
    )

    run!(
      socket,
      "GRANT SELECT, LOCK TABLES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* " <>
        "TO 'capstan_sha2'@'%'"
    )
  end

  defp run!(socket, sql) do
    case Command.query(socket, sql) do
      :ok -> :ok
      {:ok, _rows} -> :ok
      {:error, reason} -> raise "query_test: #{sql} failed #{inspect(reason)}"
    end
  end

  defp close_socket({:gen_tcp, s}), do: :gen_tcp.close(s)
  defp close_socket({:ssl, s}), do: :ssl.close(s)
end

defmodule Capstan.Protocol.CommandTest do
  use ExUnit.Case, async: true

  alias Capstan.Gtid
  alias Capstan.Protocol.Command
  alias Capstan.Protocol.Handshake
  alias Capstan.Protocol.Packet

  @loopback {127, 0, 0, 1}

  # Two real 36-char hyphenated UUIDs. @u2 < @u1 lexicographically, so the canonical
  # (sorted) encode order puts @u2's SID first — the multi-source order test depends
  # on that.
  @u1 "8d7c06f2-8460-11f1-9dc3-56461013e0e2"
  @u2 "3e11fa47-71ca-11e1-9e33-c80aa9429562"

  ## ---------------------------------------------------------------------------
  ## COM_QUERY — request byte layout
  ## ---------------------------------------------------------------------------

  describe "com_query/1" do
    test "prefixes the SQL with the COM_QUERY command byte 0x03" do
      assert Command.com_query("SELECT 1") == <<0x03, "SELECT 1">>
    end
  end

  ## ---------------------------------------------------------------------------
  ## COM_REGISTER_SLAVE — request byte layout
  ## ---------------------------------------------------------------------------

  describe "com_register_slave/1" do
    test "builds COM_REGISTER_SLAVE (0x15): server_id, empty host/user/pass, zero rank/master" do
      assert Command.com_register_slave(4200) ==
               <<0x15, 4200::32-little, 0, 0, 0, 0::16-little, 0::32-little, 0::32-little>>
    end
  end

  ## ---------------------------------------------------------------------------
  ## COM_BINLOG_DUMP_GTID — header layout
  ## ---------------------------------------------------------------------------

  describe "com_binlog_dump_gtid/2 — header layout" do
    test "command byte, BINLOG_THROUGH_GTID flag, server_id, empty binlog name, pos 4, data_size" do
      dump = parse_dump(Command.com_binlog_dump_gtid(4242, Gtid.parse("#{@u1}:1-11")))

      assert dump.command == 0x1E
      assert dump.flags == 0x04
      assert dump.server_id == 4242
      assert dump.name_len == 0
      assert dump.pos == 4
      assert dump.data_size == byte_size(dump.gtid_data)
    end

    test "an empty GTID set encodes as n_sids = 0 (an 8-byte data block)" do
      dump = parse_dump(Command.com_binlog_dump_gtid(7, Gtid.parse("")))

      assert dump.n_sids == 0
      assert dump.sids == []
      assert dump.data_size == 8
    end
  end

  ## ---------------------------------------------------------------------------
  ## COM_BINLOG_DUMP_GTID — the EXCLUSIVE-end resume encoding (F2)
  ##
  ## An off-by-one on the exclusive end skips or replays exactly one transaction per
  ## interval on every restart, silently. These pin the `high -> high + 1` conversion.
  ## ---------------------------------------------------------------------------

  describe "com_binlog_dump_gtid/2 — EXCLUSIVE end (F2)" do
    test "a checkpoint of uuid:1-11 encodes start=1, end=12 (INCLUSIVE high + 1)" do
      dump = parse_dump(Command.com_binlog_dump_gtid(1, Gtid.parse("#{@u1}:1-11")))

      assert dump.n_sids == 1
      assert [{uuid_hex, intervals}] = dump.sids
      assert uuid_hex == hex(@u1)
      assert intervals == [{1, 12}]
    end

    test "a multi-interval set uuid:1-3:7-9 encodes ends 4 and 10" do
      dump = parse_dump(Command.com_binlog_dump_gtid(1, Gtid.parse("#{@u1}:1-3:7-9")))

      assert [{_uuid, intervals}] = dump.sids
      assert intervals == [{1, 4}, {7, 10}]
    end

    test "a single-element interval uuid:7 encodes start=7, end=8" do
      dump = parse_dump(Command.com_binlog_dump_gtid(1, Gtid.parse("#{@u1}:7")))

      assert [{_uuid, [{7, 8}]}] = dump.sids
    end

    test "a multi-source set encodes both SIDs in canonical UUID order with exclusive ends" do
      dump = parse_dump(Command.com_binlog_dump_gtid(1, Gtid.parse("#{@u1}:1-5,#{@u2}:1-3")))

      assert dump.n_sids == 2
      assert [{h2, [{1, 4}]}, {h1, [{1, 6}]}] = dump.sids
      assert h2 == hex(@u2)
      assert h1 == hex(@u1)
    end

    test "the SID's 16 bytes are Base.decode16 of the hyphen-stripped UUID" do
      dump = parse_dump(Command.com_binlog_dump_gtid(1, Gtid.parse("#{@u1}:1-11")))
      <<n_sids::64-little, raw_uuid::binary-size(16), _::binary>> = dump.gtid_data

      assert n_sids == 1
      assert raw_uuid == Base.decode16!(String.replace(@u1, "-", ""), case: :mixed)
    end
  end

  ## ---------------------------------------------------------------------------
  ## COM_QUERY — text-resultset decode (mock server)
  ## ---------------------------------------------------------------------------

  describe "query/3 — text-resultset decode" do
    test "decodes rows, mapping a NULL column (0xFB) to nil rather than raising" do
      socket =
        mock_client(fn srv, test ->
          {0, request} = t_recv_pkt(srv)
          send(test, {:request, request})
          # 2-column resultset: column-count, 2 column defs, 2 rows, OK/EOF terminator.
          t_send(srv, <<2>>, 1)
          t_send(srv, column_def("gtid_mode"), 2)
          t_send(srv, column_def("note"), 3)
          # Row 1: "ON", then a NULL column (0xFB — the marker, not a length).
          t_send(srv, <<2, "ON", 0xFB>>, 4)
          # Row 2: "abc", "hello".
          t_send(srv, <<3, "abc", 5, "hello">>, 5)
          # DEPRECATE_EOF terminator: 0xFE header with a payload shorter than 9 bytes.
          t_send(srv, <<0xFE, 0, 0, 2, 0, 0, 0>>, 6)
        end)

      assert {:ok, rows} = Command.query(socket, "SELECT @@gtid_mode, note")
      assert rows == [["ON", nil], ["abc", "hello"]]

      # The request on the wire is exactly COM_QUERY (0x03) + the SQL text.
      assert_receive {:request, <<0x03, "SELECT @@gtid_mode, note">>}
    end

    test "returns :ok for a statement with no resultset (OK packet)" do
      sql = "SET @master_binlog_checksum = @@global.binlog_checksum"

      socket =
        mock_client(fn srv, test ->
          {0, request} = t_recv_pkt(srv)
          send(test, {:request, request})
          t_send(srv, <<0x00, 0, 0, 2, 0, 0, 0>>, 1)
        end)

      assert :ok = Command.query(socket, sql)
      assert_receive {:request, <<0x03, ^sql::binary>>}
    end

    test "decodes a resultset with zero rows" do
      socket =
        mock_client(fn srv, _test ->
          {0, _request} = t_recv_pkt(srv)
          t_send(srv, <<1>>, 1)
          t_send(srv, column_def("c"), 2)
          t_send(srv, <<0xFE, 0, 0, 2, 0, 0, 0>>, 3)
        end)

      assert {:ok, []} = Command.query(socket, "SELECT c FROM t WHERE false")
    end

    test "a 0xFE-led row at the length boundary decodes as a row, not a terminator" do
      # The canonical rule: a 0xFE-led packet is the DEPRECATE_EOF terminator iff its
      # TOTAL length < 9 bytes. A column value whose lenenc length prefix is 0xFE (the
      # 8-byte form) makes a row that STARTS with 0xFE and is exactly 9 bytes total
      # (rest = 8) — a ROW, not a terminator. The empty 0xFE-form value decodes to "".
      socket =
        mock_client(fn srv, _test ->
          {0, _request} = t_recv_pkt(srv)
          t_send(srv, <<1>>, 1)
          t_send(srv, column_def("c"), 2)
          # 0xFE-form lenenc column of length 0 -> "": total 9 bytes, rest = 8.
          t_send(srv, <<0xFE, 0::64-little>>, 3)
          # The genuine terminator: total 7 bytes, rest = 6.
          t_send(srv, <<0xFE, 0, 0, 2, 0, 0, 0>>, 4)
        end)

      assert {:ok, [[""]]} = Command.query(socket, "SELECT c FROM t")
    end

    test "surfaces a server error as a value-free {:query_error, code}" do
      socket =
        mock_client(fn srv, _test ->
          {0, _request} = t_recv_pkt(srv)
          t_send(srv, <<0xFF, 1146::16-little, "#42S02Table 'x' doesn't exist">>, 1)
        end)

      assert {:error, {:query_error, 1146}} = Command.query(socket, "SELECT * FROM missing")
    end
  end

  ## ---------------------------------------------------------------------------
  ## Live resume tripwire — real substrate (excluded from the default suite)
  ##
  ## Proves the tripwire can go RED on the exact F2 defect: the correct exclusive-end
  ## encoding and a deliberate off-by-one resume the stream at DIFFERENT first GTIDs.
  ## ---------------------------------------------------------------------------

  describe "live resume tripwire against mysql-cdc-probe (F2 EXCLUSIVE end)" do
    @describetag :live

    test "correct exclusive-end encoding and an off-by-one yield different first GTIDs" do
      setup_socket = live_connect()

      # Read server identity AND prove the text-resultset decoder (including the
      # NULL/0xFB path) against REAL server framing.
      assert {:ok, [[gtid_mode, server_uuid, nil]]} =
               Command.query(setup_socket, "SELECT @@gtid_mode, @@server_uuid, NULL")

      assert gtid_mode == "ON"

      {:ok, [[executed]]} = Command.query(setup_socket, "SELECT @@gtid_executed")
      {:ok, [[purged]]} = Command.query(setup_socket, "SELECT @@gtid_purged")
      close(setup_socket)

      executed_high = high_bound(executed, server_uuid)
      purged_high = high_bound(purged, server_uuid)

      assert executed_high - purged_high >= 3,
             "substrate lacks a retained window to probe " <>
               "(executed_high=#{executed_high} purged_high=#{purged_high})"

      # A checkpoint of uuid:1-k, sitting inside the retained window.
      k = purged_high + div(executed_high - purged_high, 2)
      checkpoint = Gtid.parse("#{server_uuid}:1-#{k}")

      # Correct encoder: wire end = k + 1 -> server resumes AT k + 1 (no replay).
      correct = first_streamed_gtid(Command.com_binlog_dump_gtid(5115, checkpoint))

      # Off-by-one bug: the INCLUSIVE high (k) used verbatim as the wire end -> the
      # server thinks the peer only holds through k-1 and resumes at k, silently
      # REPLAYING transaction k on every restart.
      off_by_one = first_streamed_gtid(buggy_dump(server_uuid, 1, k, 5116))

      assert correct == k + 1,
             "the correct exclusive-end encoding must resume one past the checkpoint"

      assert off_by_one == k, "the off-by-one under-counts by one and replays transaction k"

      refute correct == off_by_one,
             "the tripwire distinguishes the correct encoding from the F2 defect"
    end
  end

  ## ---------------------------------------------------------------------------
  ## helpers — dump payload parsing
  ## ---------------------------------------------------------------------------

  defp hex(uuid), do: uuid |> String.replace("-", "") |> String.downcase()

  defp parse_dump(payload) do
    <<command::8, flags::16-little, server_id::32-little, name_len::32-little, pos::64-little,
      data_size::32-little, gtid_data::binary>> = payload

    # The encoder always emits an empty binlog name, so nothing sits between name_len
    # and pos; asserting it catches a regression that reintroduces a name.
    assert name_len == 0
    assert byte_size(gtid_data) == data_size

    <<n_sids::64-little, sids_bin::binary>> = gtid_data
    {sids, <<>>} = parse_sids(sids_bin, n_sids, [])

    %{
      command: command,
      flags: flags,
      server_id: server_id,
      name_len: name_len,
      pos: pos,
      data_size: data_size,
      gtid_data: gtid_data,
      n_sids: n_sids,
      sids: sids
    }
  end

  defp parse_sids(rest, 0, acc), do: {Enum.reverse(acc), rest}

  defp parse_sids(<<raw_uuid::binary-size(16), n_intervals::64-little, rest::binary>>, n, acc) do
    {intervals, rest} = parse_intervals(rest, n_intervals, [])
    parse_sids(rest, n - 1, [{Base.encode16(raw_uuid, case: :lower), intervals} | acc])
  end

  defp parse_intervals(rest, 0, acc), do: {Enum.reverse(acc), rest}

  defp parse_intervals(<<start::64-little, stop::64-little, rest::binary>>, n, acc) do
    parse_intervals(rest, n - 1, [{start, stop} | acc])
  end

  ## ---------------------------------------------------------------------------
  ## helpers — mock query server
  ## ---------------------------------------------------------------------------

  defp mock_client(server_fun) do
    {:ok, listen} =
      :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true, ip: @loopback])

    {:ok, port} = :inet.port(listen)
    test = self()

    spawn_link(fn ->
      {:ok, srv} = :gen_tcp.accept(listen, 5000)
      :gen_tcp.close(listen)
      server_fun.({:gen_tcp, srv}, test)
      Process.sleep(100)
      :gen_tcp.close(srv)
    end)

    {:ok, client} = :gen_tcp.connect(@loopback, port, [:binary, active: false], 5000)
    on_exit(fn -> :gen_tcp.close(client) end)
    {:gen_tcp, client}
  end

  # Column-definition packets are read and discarded by the decoder, so any bytes
  # work here — only the packet framing matters.
  defp column_def(name), do: "column_def:" <> name

  defp t_send({:gen_tcp, s}, payload, seq), do: :ok = :gen_tcp.send(s, frame(payload, seq))

  defp frame(payload, seq), do: <<byte_size(payload)::24-little, seq::8, payload::binary>>

  defp t_recv_pkt({:gen_tcp, s}) do
    {:ok, <<len::24-little, seq::8>>} = :gen_tcp.recv(s, 4, 5000)

    payload =
      if len == 0 do
        <<>>
      else
        {:ok, p} = :gen_tcp.recv(s, len, 5000)
        p
      end

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

  # The inclusive high bound for `uuid` in a canonical GTID-set string, or 0 when the
  # UUID is absent (an empty gtid_purged).
  defp high_bound(set_string, uuid) do
    case Enum.find(Gtid.sources(Gtid.parse(set_string)), fn {u, _} -> u == uuid end) do
      {^uuid, intervals} -> intervals |> List.last() |> elem(1)
      nil -> 0
    end
  end

  # Hand-built dump that uses the INCLUSIVE high as the wire end (no `+ 1`): the exact
  # F2 off-by-one the correct encoder avoids.
  defp buggy_dump(uuid, low, inclusive_high, server_id) do
    raw = uuid |> String.replace("-", "") |> Base.decode16!(case: :mixed)

    gtid_data =
      <<1::64-little>> <> raw <> <<1::64-little, low::64-little, inclusive_high::64-little>>

    <<0x1E, 0x04::16-little, server_id::32-little, 0::32-little, 4::64-little,
      byte_size(gtid_data)::32-little, gtid_data::binary>>
  end

  defp first_streamed_gtid(dump_payload) do
    socket = live_connect()

    # F4: without this the server refuses COM_BINLOG_DUMP_GTID with error 1236.
    assert :ok = Command.query(socket, "SET @master_binlog_checksum = @@global.binlog_checksum")
    :ok = Packet.send_packet(socket, dump_payload, 0)
    gtid = read_first_gtid(socket)
    close(socket)
    gtid
  end

  defp read_first_gtid(socket) do
    case Packet.read_packet(socket, 20_000) do
      {_seq, <<0xFF, code::16-little, _rest::binary>>} ->
        flunk("server refused the dump with error #{code}")

      {_seq, <<0x00, event::binary>>} ->
        <<_ts::32-little, type::8, _sid::32-little, _sz::32-little, _lp::32-little,
          _fl::16-little, body_ck::binary>> = event

        if type == 33 do
          body_len = byte_size(body_ck) - 4
          <<body::binary-size(body_len), _crc::32-little>> = body_ck
          <<_flags::8, _uuid::binary-size(16), gno::64-little-signed, _::binary>> = body
          gno
        else
          read_first_gtid(socket)
        end

      {_seq, <<0xFE, _rest::binary>>} ->
        flunk("stream ended before any GTID event")
    end
  end
end

# MySQL binlog protocol viability probe — pure Elixir, zero deps.
# Proves: handshake+auth, COM_QUERY, COM_BINLOG_DUMP_GTID, event-stream parse
# (ROTATE/FORMAT_DESCRIPTION/PREVIOUS_GTIDS/GTID/QUERY/TABLE_MAP/WRITE_ROWS/XID),
# CRC32 verification, row-value decode, TABLE_MAP FULL metadata column names,
# and LIVE tailing (an insert issued mid-stream is observed and decoded).
#
# Run: elixir mysql_binlog_probe.exs
# Substrate: mysql:8.0 @ 127.0.0.1:5633, root/probe, gtid_mode=ON, binlog_row_metadata=FULL

defmodule Probe do
  import Bitwise

  @host ~c"127.0.0.1"
  @port 5633
  @user "root"
  @pass "probe"

  # capability flags
  @client_long_password 0x00000001
  @client_protocol_41 0x00000200
  @client_secure_connection 0x00008000
  @client_plugin_auth 0x00080000
  @client_deprecate_eof 0x01000000
  @caps @client_long_password ||| @client_protocol_41 ||| @client_secure_connection |||
          @client_plugin_auth ||| @client_deprecate_eof

  ## ---------- packet framing ----------

  def recv_packet(sock, timeout \\ 20_000) do
    {:ok, <<len::24-little, seq::8>>} = :gen_tcp.recv(sock, 4, timeout)

    payload =
      case len do
        0 -> <<>>
        n -> ({:ok, p} = :gen_tcp.recv(sock, n, timeout); p)
      end

    {seq, payload}
  end

  def send_packet(sock, payload, seq) do
    :ok = :gen_tcp.send(sock, <<byte_size(payload)::24-little, seq::8, payload::binary>>)
  end

  ## ---------- lenenc ----------

  def lenenc_int(<<n, rest::binary>>) when n < 0xFB, do: {n, rest}
  def lenenc_int(<<0xFC, n::16-little, rest::binary>>), do: {n, rest}
  def lenenc_int(<<0xFD, n::24-little, rest::binary>>), do: {n, rest}
  def lenenc_int(<<0xFE, n::64-little, rest::binary>>), do: {n, rest}

  def lenenc_str(bin) do
    {n, rest} = lenenc_int(bin)
    <<s::binary-size(n), rest2::binary>> = rest
    {s, rest2}
  end

  ## ---------- handshake + auth (mysql_native_password) ----------

  def connect do
    {:ok, sock} = :gen_tcp.connect(@host, @port, [:binary, active: false], 10_000)
    {0, hs} = recv_packet(sock)

    <<10, rest::binary>> = hs
    [server_version, rest] = :binary.split(rest, <<0>>)
    <<_thread_id::32-little, salt1::binary-size(8), 0, _cap_low::16-little, _charset::8,
      _status::16-little, _cap_high::16-little, salt_len::8, _reserved::binary-size(10),
      rest2::binary>> = rest

    salt2_len = max(13, salt_len - 8) - 1
    <<salt2::binary-size(salt2_len), 0, rest3::binary>> = rest2
    [plugin, _] = :binary.split(rest3, <<0>>)
    salt = salt1 <> salt2

    IO.puts("[handshake] server_version=#{server_version} auth_plugin=#{plugin} salt=#{byte_size(salt)}B")

    token = native_scramble(@pass, salt)

    resp =
      <<@caps::32-little, 16_777_216::32-little, 45, 0::23*8>> <>
        @user <> <<0, byte_size(token)::8>> <> token <> "mysql_native_password" <> <<0>>

    send_packet(sock, resp, 1)

    case recv_packet(sock) do
      {_, <<0x00, _::binary>>} ->
        IO.puts("[auth] OK (mysql_native_password over plain TCP)")
        sock

      {_, <<0xFF, code::16-little, rest::binary>>} ->
        raise "auth failed: #{code} #{inspect(rest)}"

      {_, <<0xFE, rest::binary>>} ->
        raise "auth switch requested (unexpected): #{inspect(rest)}"
    end
  end

  def native_scramble("", _salt), do: <<>>

  def native_scramble(pass, salt) do
    sha = &:crypto.hash(:sha, &1)
    stage1 = sha.(pass)
    :crypto.exor(stage1, sha.(salt <> sha.(stage1)))
  end

  ## ---------- COM_QUERY (text protocol) ----------

  def query(sock, sql) do
    send_packet(sock, <<0x03, sql::binary>>, 0)

    case recv_packet(sock) do
      {_, <<0x00, _::binary>>} ->
        :ok

      {_, <<0xFF, code::16-little, rest::binary>>} ->
        raise "query error #{code}: #{inspect(rest)} (sql: #{sql})"

      {_, first} ->
        {ncols, <<>>} = lenenc_int(first)
        # column definitions (no EOF thanks to CLIENT_DEPRECATE_EOF)
        for _ <- 1..ncols, do: recv_packet(sock)
        read_rows(sock, ncols, [])
    end
  end

  defp read_rows(sock, ncols, acc) do
    case recv_packet(sock) do
      {_, <<0xFE, rest::binary>>} when byte_size(rest) < 9 ->
        Enum.reverse(acc)

      {_, row} ->
        read_rows(sock, ncols, [decode_text_row(row, ncols, []) | acc])
    end
  end

  defp decode_text_row(_bin, 0, acc), do: Enum.reverse(acc)

  defp decode_text_row(<<0xFB, rest::binary>>, n, acc),
    do: decode_text_row(rest, n - 1, [nil | acc])

  defp decode_text_row(bin, n, acc) do
    {s, rest} = lenenc_str(bin)
    decode_text_row(rest, n - 1, [s | acc])
  end

  ## ---------- COM_BINLOG_DUMP_GTID ----------

  def start_dump(sock, server_id) do
    # empty executed-GTID set => stream everything the server still has
    gtid_set = <<0::64-little>>
    payload =
      <<0x1E, 0x04::16-little, server_id::32-little, 0::32-little>> <>
        <<4::64-little, byte_size(gtid_set)::32-little>> <> gtid_set

    send_packet(sock, payload, 0)
    IO.puts("[dump] COM_BINLOG_DUMP_GTID sent (flags=BINLOG_THROUGH_GTID, empty gtid set)")
  end

  ## ---------- binlog event parsing ----------

  @event_names %{
    2 => :query, 4 => :rotate, 15 => :format_description, 16 => :xid, 19 => :table_map,
    27 => :heartbeat, 30 => :write_rows_v2, 31 => :update_rows_v2, 32 => :delete_rows_v2,
    33 => :gtid, 34 => :anonymous_gtid, 35 => :previous_gtids
  }

  def stream_loop(sock, state) do
    {_seq, packet} = recv_packet(sock, 60_000)

    case packet do
      <<0x00, event::binary>> ->
        state = handle_event(event, state)
        if done?(state), do: state, else: stream_loop(sock, state)

      <<0xFF, code::16-little, rest::binary>> ->
        raise "stream error #{code}: #{inspect(rest)}"

      <<0xFE, _::binary>> ->
        IO.puts("[stream] EOF")
        state
    end
  end

  defp done?(state), do: length(state.decoded_rows) >= 2 and state.xids >= 2

  def handle_event(event, state) do
    <<ts::32-little, type::8, server_id::32-little, size::32-little, log_pos::32-little,
      flags::16-little, body_with_ck::binary>> = event

    # CRC32 trailer (binlog_checksum=CRC32): strip + verify
    body_len = byte_size(body_with_ck) - 4
    <<body::binary-size(body_len), ck::32-little>> = body_with_ck
    full_len = byte_size(event) - 4
    <<checked::binary-size(full_len), _::binary>> = event
    crc_ok = :erlang.crc32(checked) == ck

    name = Map.get(@event_names, type, type)
    meta = "ts=#{ts} server_id=#{server_id} size=#{size} log_pos=#{log_pos} flags=#{flags} crc32=#{if crc_ok, do: "OK", else: "MISMATCH"}"

    case name do
      :rotate ->
        <<pos::64-little, name_bin::binary>> = body
        IO.puts("[event] ROTATE -> #{name_bin} @ #{pos} (#{meta})")
        state

      :format_description ->
        <<binlog_ver::16-little, server_ver::binary-size(50), _::binary>> = body
        [sv, _] = :binary.split(server_ver, <<0>>)
        IO.puts("[event] FORMAT_DESCRIPTION binlog_ver=#{binlog_ver} server=#{sv} (#{meta})")
        state

      :previous_gtids ->
        IO.puts("[event] PREVIOUS_GTIDS #{byte_size(body)}B (#{meta})")
        state

      :gtid ->
        <<_flags::8, sid::binary-size(16), gno::64-little-signed, _::binary>> = body
        uuid = format_uuid(sid)
        IO.puts("[event] GTID #{uuid}:#{gno} (#{meta})")
        %{state | gtids: state.gtids ++ ["#{uuid}:#{gno}"]}

      :anonymous_gtid ->
        IO.puts("[event] ANONYMOUS_GTID (#{meta})")
        state

      :query ->
        <<_tid::32-little, _exec::32-little, schema_len::8, _err::16-little,
          sv_len::16-little, rest::binary>> = body
        <<_sv::binary-size(sv_len), schema::binary-size(schema_len), 0, sql::binary>> = rest
        IO.puts("[event] QUERY schema=#{schema} sql=#{inspect(sql)} (#{meta})")
        state

      :table_map ->
        <<table_id::48-little, _flags::16-little, rest::binary>> = body
        {schema, rest} = str8(rest)
        <<0, rest::binary>> = rest
        {table, rest} = str8(rest)
        <<0, rest::binary>> = rest
        {ncols, rest} = lenenc_int(rest)
        <<col_types::binary-size(ncols), rest::binary>> = rest
        {meta_len, rest} = lenenc_int(rest)
        <<col_meta::binary-size(meta_len), rest::binary>> = rest
        nb = div(ncols + 7, 8)
        <<_null_bitmap::binary-size(nb), opt_meta::binary>> = rest
        col_names = parse_optional_metadata(opt_meta)
        metas = parse_col_meta(:erlang.binary_to_list(col_types), col_meta, [])

        IO.puts(
          "[event] TABLE_MAP table_id=#{table_id} #{schema}.#{table} cols=#{ncols} " <>
            "types=#{inspect(:erlang.binary_to_list(col_types))} names=#{inspect(col_names)} (#{meta})"
        )

        %{state | tables: Map.put(state.tables, table_id, %{schema: schema, table: table, ncols: ncols, types: :erlang.binary_to_list(col_types), metas: metas, names: col_names})}

      :write_rows_v2 ->
        <<table_id::48-little, _flags::16-little, extra_len::16-little, rest::binary>> = body
        extra = extra_len - 2
        <<_extra::binary-size(extra), rest::binary>> = rest
        {ncols, rest} = lenenc_int(rest)
        present_nb = div(ncols + 7, 8)
        <<_present::binary-size(present_nb), rows_bin::binary>> = rest
        tinfo = Map.fetch!(state.tables, table_id)

        if tinfo.schema == "probe_db" do
          rows = decode_rows(rows_bin, tinfo, [])
          IO.puts("[event] WRITE_ROWS #{tinfo.schema}.#{tinfo.table} rows=#{inspect(rows)} (#{meta})")
          %{state | decoded_rows: state.decoded_rows ++ rows}
        else
          IO.puts("[event] WRITE_ROWS #{tinfo.schema}.#{tinfo.table} (filtered — outside probe_db) (#{meta})")
          state
        end

      :xid ->
        <<xid::64-little>> = body
        IO.puts("[event] XID #{xid} — transaction commit (#{meta})")
        state = %{state | xids: state.xids + 1}
        if state.xids == 1 and not state.live_insert_fired do
          IO.puts("[probe] replay done — firing LIVE insert from a second connection...")
          Task.start(fn ->
            Process.sleep(500)
            System.cmd("docker", ["exec", "mysql-cdc-probe", "mysql", "-uroot", "-pprobe", "probe_db", "-e",
              "INSERT INTO widgets VALUES (2,'live-widget',7);"], stderr_to_stdout: true)
          end)
          %{state | live_insert_fired: true}
        else
          state
        end

      :heartbeat ->
        state

      other ->
        IO.puts("[event] #{inspect(other)} #{byte_size(body)}B (#{meta})")
        state
    end
  end

  defp str8(<<len::8, s::binary-size(len), rest::binary>>), do: {s, rest}

  # column meta widths per type (only what the probe table uses + common ones)
  defp parse_col_meta([], _bin, acc), do: Enum.reverse(acc)
  defp parse_col_meta([t | ts], bin, acc) when t in [15, 253, 254] do
    # VARCHAR / VAR_STRING / STRING: 2 bytes
    <<m::16-little, rest::binary>> = bin
    parse_col_meta(ts, rest, [m | acc])
  end
  defp parse_col_meta([t | ts], bin, acc) when t in [3, 8, 1, 2, 9] do
    # LONG/LONGLONG/TINY/SHORT/INT24: no meta
    parse_col_meta(ts, bin, [nil | acc])
  end
  defp parse_col_meta([_t | ts], <<m::8, rest::binary>>, acc) do
    # fallback: assume 1 byte (TIMESTAMP2 etc.) — probe table avoids these
    parse_col_meta(ts, rest, [m | acc])
  end

  # optional metadata TLVs; type 4 = COLUMN_NAME (binlog_row_metadata=FULL)
  defp parse_optional_metadata(<<>>), do: nil
  defp parse_optional_metadata(bin), do: scan_tlv(bin)

  defp scan_tlv(<<>>), do: nil
  defp scan_tlv(<<type::8, rest::binary>>) do
    {len, rest} = lenenc_int(rest)
    <<val::binary-size(len), rest2::binary>> = rest

    if type == 4 do
      names(val, [])
    else
      scan_tlv(rest2)
    end
  end

  defp names(<<>>, acc), do: Enum.reverse(acc)
  defp names(bin, acc) do
    {s, rest} = lenenc_str(bin)
    names(rest, [s | acc])
  end

  # rows: null_bitmap then values (all columns present in probe)
  defp decode_rows(<<>>, _tinfo, acc), do: Enum.reverse(acc)

  defp decode_rows(bin, tinfo, acc) do
    nb = div(tinfo.ncols + 7, 8)
    <<null_bitmap::binary-size(nb), rest::binary>> = bin
    null_bits = for <<b::8 <- null_bitmap>>, do: b
    {vals, rest} = decode_vals(Enum.zip(tinfo.types, tinfo.metas), 0, null_bits, rest, [])
    decode_rows(rest, tinfo, [vals | acc])
  end

  defp decode_vals([], _i, _nulls, rest, acc), do: {Enum.reverse(acc), rest}

  defp decode_vals([{type, meta} | more], i, nulls, bin, acc) do
    is_null = (Enum.at(nulls, div(i, 8)) >>> rem(i, 8) &&& 1) == 1

    {val, bin} =
      cond do
        is_null -> {nil, bin}
        type == 3 -> (<<v::32-little-signed, r::binary>> = bin; {v, r})
        type == 8 -> (<<v::64-little-signed, r::binary>> = bin; {v, r})
        type in [15, 253] and meta > 255 -> (<<len::16-little, v::binary-size(len), r::binary>> = bin; {v, r})
        type in [15, 253] -> (<<len::8, v::binary-size(len), r::binary>> = bin; {v, r})
        true -> raise "probe decoder: unhandled column type #{type}"
      end

    decode_vals(more, i + 1, nulls, bin, [val | acc])
  end

  defp format_uuid(<<a::binary-size(4), b::binary-size(2), c::binary-size(2), d::binary-size(2), e::binary-size(6)>>) do
    [a, b, c, d, e] |> Enum.map(&Base.encode16(&1, case: :lower)) |> Enum.join("-")
  end

  ## ---------- main ----------

  def run do
    sock = connect()

    [[gtid_mode, checksum, row_meta, version]] =
      query(sock, "SELECT @@gtid_mode, @@binlog_checksum, @@binlog_row_metadata, @@version")

    IO.puts("[server] version=#{version} gtid_mode=#{gtid_mode} binlog_checksum=#{checksum} binlog_row_metadata=#{row_meta}")

    :ok = query(sock, "SET @master_binlog_checksum = @@global.binlog_checksum")
    IO.puts("[session] @master_binlog_checksum declared")

    start_dump(sock, 999)

    state = %{tables: %{}, decoded_rows: [], xids: 0, gtids: [], live_insert_fired: false}
    final = stream_loop(sock, state)

    IO.puts("")
    IO.puts("========== PROBE RESULT ==========")
    IO.puts("decoded rows:    #{inspect(final.decoded_rows)}")
    IO.puts("gtids observed:  #{inspect(final.gtids)}")
    IO.puts("txn commits:     #{final.xids}")

    expected = [[1, "seeded-widget", 42], [2, "live-widget", 7]]

    if final.decoded_rows == expected do
      IO.puts("VERDICT: PASS — replayed row AND live-tailed row both decoded byte-correct")
    else
      IO.puts("VERDICT: FAIL — expected #{inspect(expected)}")
      System.halt(1)
    end
  end
end

Probe.run()

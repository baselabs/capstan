# Is the COM_BINLOG_DUMP_GTID interval end INCLUSIVE or EXCLUSIVE?
#
# This is the resume path: capstan restarts by sending its checkpoint set as intervals. An
# off-by-one here skips or replays exactly one transaction per interval on EVERY restart —
# silently, with no server error. The streaming probe never tested it (it only ever sent an
# EMPTY set, probe/mysql_binlog_probe.exs:150), so the encoding is unproven.
#
# Method: send interval (1, N) and observe the FIRST GTID the server streams.
#   end EXCLUSIVE  => "I have 1..N-1"  => server starts at N
#   end INCLUSIVE  => "I have 1..N"    => server starts at N+1

Code.require_file("mysql_binlog_probe_lib.exs", __DIR__)

defmodule Bound do
  import Bitwise

  def dump_with_interval(sock, uuid_bin, start_gno, end_gno, server_id) do
    gtid_data =
      <<1::64-little>> <>
        uuid_bin <>
        <<1::64-little, start_gno::64-little, end_gno::64-little>>

    payload =
      <<0x1E, 0x04::16-little, server_id::32-little, 0::32-little>> <>
        <<4::64-little, byte_size(gtid_data)::32-little>> <> gtid_data

    Probe.send_packet(sock, payload, 0)
  end

  def first_gtid(sock) do
    case Probe.recv_packet(sock, 20_000) do
      {_s, <<0xFF, code::16-little, rest::binary>>} ->
        {:error, code, rest}

      {_s, <<0x00, event::binary>>} ->
        <<_ts::32-little, type::8, _sid::32-little, _sz::32-little, _lp::32-little,
          _fl::16-little, body_ck::binary>> = event

        if type == 33 do
          blen = byte_size(body_ck) - 4
          <<body::binary-size(blen), _crc::32-little>> = body_ck
          <<_flags::8, _sid::binary-size(16), gno::64-little-signed, _::binary>> = body
          {:gtid, gno}
        else
          first_gtid(sock)
        end
    end
  end
end

uuid_hex = "8d7c06f28460 11f1 9dc3 56461013e0e2" |> String.replace(" ", "")

for n <- [12, 13] do
  sock = Probe.connect()
  :ok = Probe.query(sock, "SET @master_binlog_checksum = @@global.binlog_checksum")
  Bound.dump_with_interval(sock, Base.decode16!(uuid_hex, case: :mixed), 1, n, 4300 + n)

  case Bound.first_gtid(sock) do
    {:gtid, gno} ->
      verdict = cond do
        gno == n -> "EXCLUSIVE (server resumed AT the end bound)"
        gno == n + 1 -> "INCLUSIVE (server resumed AFTER the end bound)"
        true -> "UNEXPECTED"
      end
      IO.puts("interval (1, #{n}) -> first streamed GTID = #{gno}  =>  #{verdict}")

    {:error, code, msg} ->
      IO.puts("interval (1, #{n}) -> ERROR #{code}: #{inspect(msg)}")
  end
end

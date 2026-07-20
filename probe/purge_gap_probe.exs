# Focused follow-on probe: what does MySQL do when we request a GTID range it has PURGED?
#
# This is the evidence behind capstan's fail-closed `:data_gap` halt. replicant's analogue is
# "slot absent + checkpoint > 0 → halt, never silently recreate". capstan has no slot, so the
# server holds no retention contract on our behalf; the ONLY protections are (a) a proactive
# @@gtid_purged vs checkpoint comparison at connect and (b) the server's error on dump.
# This probe establishes exactly what (b) looks like on the wire.
#
# Run AFTER mysql_binlog_probe.exs. Purges binlog.000001 first (destructive; disposable substrate).

Code.require_file("mysql_binlog_probe_lib.exs", __DIR__)

sock = Probe.connect()

IO.puts("\n--- before purge ---")
[[purged_before]] = Probe.query(sock, "SELECT @@gtid_purged")
IO.puts("@@gtid_purged = #{inspect(purged_before)}")

# Roll to a fresh file, then purge everything before it.
:ok = Probe.query(sock, "FLUSH BINARY LOGS")
logs = Probe.query(sock, "SHOW BINARY LOGS")
newest = logs |> List.last() |> hd()
IO.puts("binlog files: #{inspect(Enum.map(logs, &hd/1))} — purging to #{newest}")

:ok = Probe.query(sock, "PURGE BINARY LOGS TO '#{newest}'")
IO.puts("purged to #{newest}")

[[purged_after]] = Probe.query(sock, "SELECT @@gtid_purged")
IO.puts("@@gtid_purged = #{inspect(purged_after)}")

# Now ask for EVERYTHING from the beginning (empty executed set) — the purged GTIDs are required
# and no longer exist. This is the exact shape of "our checkpoint fell off the back of the log".
Probe.start_dump(sock, 4242)

case Probe.recv_packet(sock, 20_000) do
  {_seq, <<0xFF, code::16-little, rest::binary>>} ->
    IO.puts("\n=== SERVER REFUSED THE DUMP ===")
    IO.puts("error code: #{code}")
    IO.puts("payload:    #{inspect(rest)}")
    IO.puts("\nVERDICT: PASS — purged-GTID request is refused LOUDLY at dump time.")
    IO.puts("capstan can detect the gap on the wire; it does not need to infer it.")

  {_seq, <<0x00, event::binary>>} ->
    <<_ts::32-little, type::8, _rest::binary>> = event
    IO.puts("\nVERDICT: FAIL — server streamed event type #{type} instead of refusing.")
    IO.puts("A purged-range request would be SILENTLY served: the gap must be detected")
    IO.puts("proactively via @@gtid_purged, and the wire error cannot be relied on.")
    System.halt(1)

  other ->
    IO.puts("\nVERDICT: INCONCLUSIVE — unexpected packet: #{inspect(other)}")
    System.halt(1)
end

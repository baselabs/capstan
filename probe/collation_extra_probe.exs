# C2a follow-up probe — COLLATE-pinned introducer weights + the combined chunk-SQL shape.
#
# The stream-side weight resolution computes WEIGHT_STRING over a raw binlog value introduced
# as CONVERT(X'<col bytes>' USING <col cs>). Without a COLLATE pin, the CONVERT result carries
# the charset's DEFAULT collation — for a column with a NON-default collation (e.g.
# utf8mb4_0900_as_cs vs the utf8mb4 default 0900_ai_ci) the weights would be the wrong
# collation's. This probe proves the pinned form
#   WEIGHT_STRING(CONVERT(X'..' USING <cs>) COLLATE <column collation>)
# equals the column's own WEIGHT_STRING, on a non-default collation where the unpinned form
# measurably differs. Also smoke-checks the combined chunk SELECT shape (CAST AS BINARY
# projection + WEIGHT_STRING column).
#
# Run: elixir probe/collation_extra_probe.exs
# Substrate: mysql-cdc-probe (8.0) @ 127.0.0.1:$MYSQL_PORT_80, probe_db (self-cleaning).

Code.require_file("mysql_binlog_probe_lib.exs", __DIR__)

defmodule Coll2 do
  def q(sock, sql) do
    case Probe.query(sock, sql) do
      :ok -> :ok
      rows -> rows
    end
  end
end

sock = Probe.connect()

Coll2.q(sock, "CREATE DATABASE IF NOT EXISTS probe_db")
Coll2.q(sock, "USE probe_db")
Coll2.q(sock, "DROP TABLE IF EXISTS cw_cs2")

# utf8mb4_0900_as_cs: accent-SENSITIVE — 'é' and 'e' are DISTINCT keys (unlike ai_ci), and its
# weights differ from the charset default utf8mb4_0900_ai_ci.
Coll2.q(sock, """
CREATE TABLE cw_cs2 (
  k VARCHAR(16) COLLATE utf8mb4_0900_as_cs NOT NULL PRIMARY KEY,
  v INT
) ENGINE=InnoDB
""")

Coll2.q(sock, "INSERT INTO cw_cs2 VALUES ('é', 1), ('e', 2)")

e_raw = <<0xC3, 0xA9>>

column =
  Coll2.q(sock, "SELECT HEX(WEIGHT_STRING(k)) FROM cw_cs2 WHERE v = 1")
  |> List.first()
  |> List.first()

unpinned =
  Coll2.q(sock, "SELECT HEX(WEIGHT_STRING(CONVERT(X'C3A9' USING utf8mb4)))")
  |> List.first()
  |> List.first()

pinned =
  Coll2.q(
    sock,
    "SELECT HEX(WEIGHT_STRING(CONVERT(X'C3A9' USING utf8mb4) COLLATE utf8mb4_0900_as_cs))"
  )
  |> List.first()
  |> List.first()

e_pinned =
  Coll2.q(
    sock,
    "SELECT HEX(WEIGHT_STRING(CONVERT(X'65' USING utf8mb4) COLLATE utf8mb4_0900_as_cs))"
  )
  |> List.first()
  |> List.first()

IO.puts("column weight of é (as_cs)      : #{column}")
IO.puts("unpinned introducer (ai_ci!)    : #{unpinned}")
IO.puts("COLLATE-pinned introducer (as_cs): #{pinned}")
IO.puts("pinned weight of e (as_cs)      : #{e_pinned}")

r1 = column == pinned
r2 = column != unpinned
r3 = e_pinned != pinned

IO.puts("[#{r1}] pinned introducer == column weight")
IO.puts("[#{r2}] unpinned introducer DIFFERS (the pin is load-bearing)")
IO.puts("[#{r3}] é and e distinct under as_cs (and distinct weights)")

chunk_shape =
  Coll2.q(sock, "SELECT CAST(k AS BINARY), WEIGHT_STRING(k) FROM cw_cs2 ORDER BY k LIMIT 2")

IO.puts("chunk shape [CAST-AS-BINARY, weight]: #{inspect(chunk_shape)}")

Coll2.q(sock, "DROP TABLE IF EXISTS cw_cs2")
:ok = :gen_tcp.close(sock)

IO.puts("raw é bytes check: #{inspect(e_raw)}")
if r1 and r2 and r3, do: IO.puts("ALL EXTRA CHECKS PASS"), else: exit(1)

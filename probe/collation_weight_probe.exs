# C2a live probe — collation sort-key semantics for a string-PK snapshot cursor.
#
# The cursor-gate classifies streamed changes by `k ≤ cursor` in ELIXIR; the chunk read pages
# by `WHERE pk > cursor ORDER BY pk` in MySQL under the column's COLLATION. For the gate to be
# order-faithful, the Elixir comparison must reproduce the collation order, not code-point
# order. Candidate mechanism: WEIGHT_STRING(pk) — a binary string whose byte order IS the
# collation order (per the refman's WEIGHT_STRING contract). This probe measures, on the live
# 8.0 substrate, every semantic the cursor design depends on:
#
#   Q1  weight-byte order == ORDER BY order (and != Elixir byte order) under utf8mb4_0900_ai_ci
#   Q2  `WHERE pk > '<raw cursor>'` selects exactly {k : weight(k) > weight(cursor)}
#   Q3  weight forms across collation families: 0900_ai_ci, _bin (form AND order), PAD SPACE
#   Q3p PAD SPACE + trailing-space-crafted DISTINCT keys: weight order still == ORDER BY
#       (values equal after space-stripping cannot coexist on a PK — Q10 — so the only
#       ORDER BY / raw-weight divergence is unreachable for distinct keys)
#   Q4  CHAR(n) retrieval padding on the text protocol + CAST(pk AS BINARY) raw form
#   Q5  latin1 column: text-protocol wire form, plain-literal round-trip pagination, and the
#       introducer form WEIGHT_STRING(CONVERT(X'..' USING <cs>)) == server-side WEIGHT_STRING
#   Q6  TEXT prefix PK: allowed, information_schema shape, full-value ORDER BY consistency —
#       and EXPLAIN (filesort per page — the measured TEXT refusal ground)
#   Q7  composite (INT, VARCHAR ci): row-value ORDER BY == (int, weight) tuple order
#   Q8  4-byte utf8mb4 ('𝕏') weights + pagination
#   Q9  EXPLAIN: `WHERE pk > CONVERT(X'..' USING utf8mb4)` vs plain literal — index range?
#   Q10 PAD SPACE: 'a' vs 'a ' — PK collision + weight forms
#   Q11 ai_ci accent equality as PK collision ('e' vs 'é') — distinct PK ⇒ distinct weights
#   Q12 information_schema: CHARACTER_SET_NAME/COLLATION_NAME/DATA_TYPE shapes; PAD_ATTRIBUTE
#   Q13 ENUM('b','a') PK — is the ENUM's weight position-based (matching ORDER BY) or
#       string-based? Decides whether the introducer weight path can ever serve ENUM.
#   Q14 the batched stream-side weight query shape (multiple CONVERT forms in one SELECT)
#
# Run: elixir probe/collation_weight_probe.exs
# Substrate: mysql-cdc-probe (8.0) @ 127.0.0.1:$MYSQL_PORT_80, probe_db (self-cleaning).

Code.require_file("mysql_binlog_probe_lib.exs", __DIR__)

defmodule Coll do
  def q(sock, sql) do
    case Probe.query(sock, sql) do
      :ok -> :ok
      rows -> rows
    end
  end

  def one(sock, sql) do
    case q(sock, sql) do
      [row] -> row
      other -> raise "expected one row, got: #{inspect(other)}\n  sql: #{sql}"
    end
  end

  def rows1(sock, sql), do: Enum.map(q(sock, sql), fn [v] -> v end)

  def byte_sorted(values), do: Enum.sort(values, &(&1 <= &2))

  def unhex(nil), do: nil

  def unhex(h) when is_binary(h) do
    {:ok, bytes} = Base.decode16(h, case: :mixed)
    bytes
  end

  def verdict(name, cond, evidence) do
    IO.puts("[#{if cond, do: "PASS", else: "FAIL"}] #{name}\n       #{evidence}")
    cond
  end
end

sock = Probe.connect()

Coll.q(sock, "CREATE DATABASE IF NOT EXISTS probe_db")
Coll.q(sock, "USE probe_db")

# Self-clean both before (leftovers) and after (this run).
tables = ~w(cw_ai cw_bin cw_pad cw_char cw_lat cw_text cw_comp cw_enum cw_e)
for t <- tables, do: Coll.q(sock, "DROP TABLE IF EXISTS #{t}")

results = []

# --- fixtures ------------------------------------------------------------------
Coll.q(sock, """
CREATE TABLE cw_ai (
  k VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL PRIMARY KEY,
  v INT
) ENGINE=InnoDB
""")

values = ["9", "Z", "a", "b", "zed", "中", "Æon", "𝕏", "ß"]

for {v, i} <- Enum.with_index(values, 1),
    do: Coll.q(sock, "INSERT INTO cw_ai VALUES ('#{v}', #{i})")

Coll.q(sock, """
CREATE TABLE cw_pad (
  k VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL PRIMARY KEY,
  v INT
) ENGINE=InnoDB
""")

# Q3p: distinct-after-stripping values incl. trailing-space carriers and control chars.
# ('ab' + 'ab ' collide under PAD SPACE — proven live here, and again as Q10.)
Coll.q(sock, "INSERT INTO cw_pad VALUES ('ab', 1)")

pad_fixture_collision =
  try do
    Coll.q(sock, "INSERT INTO cw_pad VALUES ('ab ', 2)")
    false
  rescue
    _ -> true
  end

IO.puts("[fixture] PAD SPACE 'ab' + 'ab ' collide: #{pad_fixture_collision}")
Coll.q(sock, "INSERT INTO cw_pad VALUES ('ab x', 2), ('ab!', 3), ('ab\\x01', 4), ('ab z', 5)")

Coll.q(sock, """
CREATE TABLE cw_bin (
  k VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL PRIMARY KEY,
  v INT
) ENGINE=InnoDB
""")

Coll.q(sock, "INSERT INTO cw_bin VALUES ('a', 1), ('B', 2), ('中', 3)")

Coll.q(sock, """
CREATE TABLE cw_char (
  k CHAR(8) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL PRIMARY KEY,
  v INT
) ENGINE=InnoDB
""")

Coll.q(sock, "INSERT INTO cw_char VALUES ('ab', 1)")

Coll.q(sock, """
CREATE TABLE cw_lat (
  k VARCHAR(16) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL PRIMARY KEY,
  v INT
) ENGINE=InnoDB
""")

Coll.q(sock, "INSERT INTO cw_lat VALUES ('é', 1), ('ü', 2), ('Z', 3), ('a', 4)")

text_pk_allowed =
  try do
    Coll.q(sock, """
    CREATE TABLE cw_text (
      t TEXT COLLATE utf8mb4_0900_ai_ci NOT NULL,
      v INT,
      PRIMARY KEY (t(10))
    ) ENGINE=InnoDB
    """)

    true
  rescue
    _ -> false
  end

if text_pk_allowed,
  do: Coll.q(sock, "INSERT INTO cw_text VALUES ('abcdefghij1', 1), ('abcdefghik2', 2)")

Coll.q(sock, """
CREATE TABLE cw_comp (
  i INT NOT NULL,
  k VARCHAR(16) COLLATE utf8mb4_0900_ai_ci NOT NULL,
  v INT,
  PRIMARY KEY (i, k)
) ENGINE=InnoDB
""")

Coll.q(sock, "INSERT INTO cw_comp VALUES (1, 'Z', 1), (1, 'a', 2), (2, 'Z', 3), (0, 'zed', 4)")

Coll.q(sock, """
CREATE TABLE cw_enum (
  k ENUM('b','a') NOT NULL PRIMARY KEY,
  v INT
) ENGINE=InnoDB
""")

Coll.q(sock, "INSERT INTO cw_enum VALUES ('b', 1), ('a', 2)")

# --- Q1: weight-byte order == ORDER BY order (and != Elixir byte order) ----------------
ordered = Coll.rows1(sock, "SELECT k FROM cw_ai ORDER BY k")
weighted = Coll.q(sock, "SELECT HEX(WEIGHT_STRING(k)) w, k FROM cw_ai")

weight_sorted =
  weighted |> Enum.sort_by(fn [w, _k] -> Coll.unhex(w) end) |> Enum.map(fn [_w, k] -> k end)

elixir_sorted = Coll.byte_sorted(values)

IO.puts("--- Q1: utf8mb4_0900_ai_ci ordering ---")
IO.puts("  ORDER BY k          : #{inspect(ordered)}")
IO.puts("  weight-byte order   : #{inspect(weight_sorted)}")
IO.puts("  Elixir byte order   : #{inspect(elixir_sorted)}")

results =
  results ++
    [
      Coll.verdict(
        "Q1a weight-byte order == ORDER BY order",
        ordered == weight_sorted,
        "ORDER BY #{inspect(ordered)} vs weights #{inspect(weight_sorted)}"
      ),
      Coll.verdict(
        "Q1b Elixir byte order DIVERGES from ORDER BY (the mis-classification C2a must fix)",
        ordered != elixir_sorted,
        "ORDER BY #{inspect(ordered)} vs byte #{inspect(elixir_sorted)}"
      )
    ]

# --- Q2: WHERE pk > cursor selects exactly {weight > weight(cursor)} -------------------
cursor = "Z"

wcursor =
  Coll.one(sock, "SELECT HEX(WEIGHT_STRING(_utf8mb4 '#{cursor}' COLLATE utf8mb4_0900_ai_ci))")
  |> List.first()

where_gt = Coll.rows1(sock, "SELECT k FROM cw_ai WHERE k > '#{cursor}' ORDER BY k")

weight_gt =
  weighted
  |> Enum.filter(fn [w, _k] -> Coll.unhex(w) > Coll.unhex(wcursor) end)
  |> Enum.map(fn [_w, k] -> k end)

byte_gt = values |> Enum.filter(&(&1 > cursor)) |> Coll.byte_sorted()

IO.puts("--- Q2: pagination at cursor '#{cursor}' (weight #{wcursor}) ---")
IO.puts("  WHERE k > cursor    : #{inspect(where_gt)}")
IO.puts("  weight > w(cursor)  : #{inspect(weight_gt)}")
IO.puts("  byte > cursor       : #{inspect(byte_gt)}")

results =
  results ++
    [
      Coll.verdict(
        "Q2a WHERE pk > cursor == {weight > weight(cursor)}",
        where_gt == weight_gt,
        "WHERE #{inspect(where_gt)} vs weights #{inspect(weight_gt)}"
      ),
      Coll.verdict(
        "Q2b 'a' excluded by WHERE though byte > cursor (divergence proven live)",
        "a" not in where_gt and "a" in byte_gt,
        "byte-order membership of 'a' would mis-classify it as not-yet-backfilled"
      )
    ]

# --- Q3: WEIGHT_STRING forms across collation families --------------------------------
IO.puts("--- Q3: weight forms ---")

for {coll, ch} <- [
      {"utf8mb4_0900_ai_ci", "a"},
      {"utf8mb4_0900_ai_ci", "A"},
      {"utf8mb4_0900_ai_ci", "Z"},
      {"utf8mb4_0900_ai_ci", "中"},
      {"utf8mb4_0900_bin", "a"},
      {"utf8mb4_bin", "a"},
      {"utf8mb4_general_ci", "a"}
    ] do
  w =
    Coll.one(sock, "SELECT HEX(WEIGHT_STRING(_utf8mb4 '#{ch}' COLLATE #{coll}))") |> List.first()

  IO.puts("  #{coll} '#{ch}' -> #{w}")
end

# _bin: the weight FORM differs by family (0900_bin = raw bytes; legacy utf8mb4_bin = 3-byte
# code-point cells) — only ORDER agreement matters for the cursor. Compare orders on cw_bin.
bin_ordered = Coll.rows1(sock, "SELECT k FROM cw_bin ORDER BY k")

bin_weighted =
  Coll.q(sock, "SELECT HEX(WEIGHT_STRING(k)) w, k FROM cw_bin")
  |> Enum.sort_by(fn [w, _] -> Coll.unhex(w) end)
  |> Enum.map(fn [_w, k] -> k end)

results =
  results ++
    [
      Coll.verdict(
        "Q3a _bin: weight-byte order == ORDER BY (form may differ; order is what the gate needs)",
        bin_ordered == bin_weighted,
        "ORDER BY #{inspect(bin_ordered)} vs weights #{inspect(bin_weighted)}"
      )
    ]

# --- Q3p: PAD SPACE with trailing-space-crafted distinct keys ---------------------------
pad_ordered = Coll.rows1(sock, "SELECT k FROM cw_pad ORDER BY k")

pad_weighted =
  Coll.q(sock, "SELECT HEX(WEIGHT_STRING(k)) w, k FROM cw_pad")
  |> Enum.sort_by(fn [w, _] -> Coll.unhex(w) end)
  |> Enum.map(fn [_w, k] -> k end)

IO.puts("--- Q3p: PAD SPACE general_ci crafted values ---")
IO.puts("  ORDER BY k        : #{inspect(pad_ordered)}")
IO.puts("  weight-byte order : #{inspect(pad_weighted)}")

results =
  results ++
    [
      Coll.verdict(
        "Q3p PAD SPACE: weight order == ORDER BY over DISTINCT keys",
        pad_ordered == pad_weighted,
        "#{inspect(pad_ordered)} vs #{inspect(pad_weighted)}"
      )
    ]

# --- Q4: CHAR(n) retrieval padding ------------------------------------------------------
IO.puts("--- Q4: CHAR(8) 'ab' ---")
char_text = Coll.one(sock, "SELECT k FROM cw_char") |> List.first()
char_hex = Coll.one(sock, "SELECT HEX(k) FROM cw_char") |> List.first()
char_bin = Coll.one(sock, "SELECT HEX(CAST(k AS BINARY)) FROM cw_char") |> List.first()

IO.puts(
  "  text-protocol k     : #{inspect(char_text)} (#{char_hex}), CAST AS BINARY: #{char_bin}"
)

results =
  results ++
    [
      Coll.verdict(
        "Q4 CHAR(n) retrieved WITHOUT padding (text protocol form == stored value)",
        char_text == "ab",
        "got #{inspect(char_text)}"
      )
    ]

# --- Q5: latin1 column — wire form, round-trip, introducer equivalence -----------------
IO.puts("--- Q5: latin1_swedish_ci ---")

lat_raw = Coll.one(sock, "SELECT k FROM cw_lat WHERE v = 1") |> List.first()

lat_forms =
  Coll.q(
    sock,
    "SELECT HEX(k), HEX(CAST(k AS BINARY)), HEX(WEIGHT_STRING(k)) FROM cw_lat ORDER BY k"
  )

IO.puts("  raw text-protocol 'é' bytes: #{inspect(lat_raw)} (expect utf8mb4 C3A9 on the wire)")
IO.puts("  [HEX(k)=column bytes, CAST-AS-BINARY, weight]:")
for row <- lat_forms, do: IO.puts("    #{inspect(row)}")

lat_where = Coll.rows1(sock, "SELECT HEX(CAST(k AS BINARY)) FROM cw_lat WHERE k > 'a' ORDER BY k")

lat_where_conv =
  Coll.rows1(
    sock,
    "SELECT HEX(CAST(k AS BINARY)) FROM cw_lat WHERE k > CONVERT(X'61' USING latin1) ORDER BY k"
  )

w_col =
  Coll.one(sock, "SELECT HEX(WEIGHT_STRING(k)) FROM cw_lat WHERE CAST(k AS BINARY) = X'E9'")
  |> List.first()

w_conv = Coll.one(sock, "SELECT HEX(WEIGHT_STRING(CONVERT(X'E9' USING latin1)))") |> List.first()

introducer_parse_ok =
  try do
    Coll.q(sock, "SELECT HEX(WEIGHT_STRING(_latin1 X'E9'))")
    true
  rescue
    _ -> false
  end

IO.puts("  WHERE k > 'a' (plain literal): #{inspect(lat_where)}")
IO.puts("  WHERE k > CONVERT(X'61' USING latin1): #{inspect(lat_where_conv)}")
IO.puts("  weight(column é)=#{w_col}  weight(CONVERT(X'E9' USING latin1))=#{w_conv}")
IO.puts("  _latin1 X'..' introducer parses: #{introducer_parse_ok}")

results =
  results ++
    [
      Coll.verdict(
        "Q5a text protocol delivers latin1 as utf8mb4 on the wire (é -> C3A9)",
        lat_raw == <<0xC3, 0xA9>>,
        "got #{inspect(lat_raw)}"
      ),
      Coll.verdict(
        "Q5b plain-literal pagination == CONVERT-form pagination (round-trip lossless)",
        lat_where == lat_where_conv and lat_where != [],
        "#{inspect(lat_where)} vs #{inspect(lat_where_conv)}"
      ),
      Coll.verdict(
        "Q5c WEIGHT_STRING(CONVERT(X'<col bytes>' USING <col cs>)) == column's own weight",
        w_col == w_conv and w_col not in [nil, ""],
        "column #{w_col} vs introducer #{w_conv}"
      )
    ]

# --- Q6: TEXT prefix PK ------------------------------------------------------------------
IO.puts("--- Q6: TEXT prefix PK ---")

if text_pk_allowed do
  is_cols =
    Coll.q(
      sock,
      "SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='probe_db' AND TABLE_NAME='cw_text' AND COLUMN_NAME='t'"
    )

  text_ordered = Coll.rows1(sock, "SELECT t FROM cw_text ORDER BY t")
  text_where = Coll.rows1(sock, "SELECT t FROM cw_text WHERE t > 'abcdefghij1' ORDER BY t")
  text_plan = Coll.q(sock, "EXPLAIN SELECT t FROM cw_text WHERE t > 'abcdefghij1' ORDER BY t")
  varchar_plan = Coll.q(sock, "EXPLAIN SELECT k FROM cw_ai WHERE k > 'Z' ORDER BY k")
  IO.puts("  information_schema: #{inspect(is_cols)}")

  IO.puts(
    "  ORDER BY t: #{inspect(text_ordered)}; WHERE t > 'abcdefghij1': #{inspect(text_where)}"
  )

  IO.puts("  EXPLAIN text pk : #{inspect(Enum.at(text_plan, 0) |> Enum.take(12))}")
  IO.puts("  EXPLAIN varchar : #{inspect(Enum.at(varchar_plan, 0) |> Enum.take(12))}")

  text_filesort =
    text_plan |> Enum.at(0) |> Enum.at(11) |> to_string() |> String.contains?("filesort")

  results =
    results ++
      [
        Coll.verdict(
          "Q6a TEXT prefix PK: full-value ORDER BY deterministic and > consistent",
          text_ordered == ["abcdefghij1", "abcdefghik2"] and text_where == ["abcdefghik2"],
          "ordered #{inspect(text_ordered)} where #{inspect(text_where)}"
        ),
        Coll.verdict(
          "Q6b TEXT pk pagination pays a FILESORT per page (varchar pk: none) — measured refusal ground",
          text_filesort,
          "text Extra: #{inspect(Enum.at(text_plan, 0) |> Enum.at(11))}"
        )
      ]
else
  IO.puts("  CREATE TABLE with PRIMARY KEY(t(10)) was REFUSED by the server")
end

# --- Q7: composite row-value ordering -----------------------------------------------------
IO.puts("--- Q7: composite (INT, VARCHAR ai_ci) ---")
comp_ordered = Coll.q(sock, "SELECT i, HEX(WEIGHT_STRING(k)) FROM cw_comp ORDER BY i, k")

comp_weighted =
  Coll.q(sock, "SELECT i, HEX(WEIGHT_STRING(k)) FROM cw_comp")
  |> Enum.sort_by(fn [i, w] -> {String.to_integer(i), Coll.unhex(w)} end)

IO.puts("  ORDER BY i,k        : #{inspect(comp_ordered)}")
IO.puts("  (i, weight) order   : #{inspect(comp_weighted)}")

results =
  results ++
    [
      Coll.verdict(
        "Q7 composite row-value ORDER BY (i,k) == (i, weight(k)) order",
        comp_ordered == comp_weighted,
        "#{inspect(comp_ordered)} vs #{inspect(comp_weighted)}"
      )
    ]

# --- Q8: 4-byte utf8mb4 --------------------------------------------------------------------
smp_weight = Coll.one(sock, "SELECT HEX(WEIGHT_STRING(k)) FROM cw_ai WHERE v = 8") |> List.first()
smp_hex = Coll.one(sock, "SELECT HEX(k) FROM cw_ai WHERE v = 8") |> List.first()
IO.puts("--- Q8: '𝕏' (#{smp_hex}) weight = #{smp_weight}")

results =
  results ++
    [
      Coll.verdict(
        "Q8 4-byte char has a nonempty weight",
        smp_weight not in [nil, ""],
        smp_weight
      )
    ]

# --- Q9: EXPLAIN — CONVERT literal vs plain literal index use ------------------------------
IO.puts("--- Q9: EXPLAIN on cw_ai ---")
plan_plain = Coll.q(sock, "EXPLAIN SELECT k FROM cw_ai WHERE k > 'Z'")
plan_conv = Coll.q(sock, "EXPLAIN SELECT k FROM cw_ai WHERE k > CONVERT(X'5A' USING utf8mb4)")
p_plain = plan_plain |> Enum.at(0) |> Enum.take(12)
p_conv = plan_conv |> Enum.at(0) |> Enum.take(12)
IO.puts("  plain literal  : #{inspect(p_plain)}")
IO.puts("  CONVERT literal: #{inspect(p_conv)}")

results =
  results ++
    [
      Coll.verdict(
        "Q9 CONVERT-form cursor keeps the PK range scan (no full scan / filesort)",
        Enum.at(p_conv, 4) == "range" and Enum.at(p_conv, 6) == "PRIMARY",
        "type=#{inspect(Enum.at(p_conv, 4))} key=#{inspect(Enum.at(p_conv, 6))} extra=#{inspect(Enum.at(p_conv, 11))}"
      )
    ]

# --- Q10/Q11: PK collisions under ai / PAD SPACE ---------------------------------------------
pad_collision =
  try do
    Coll.q(sock, "INSERT INTO cw_pad VALUES ('ab  ', 9)")
    false
  rescue
    _ -> true
  end

Coll.q(
  sock,
  "CREATE TABLE cw_e (k VARCHAR(8) COLLATE utf8mb4_0900_ai_ci PRIMARY KEY) ENGINE=InnoDB"
)

Coll.q(sock, "INSERT INTO cw_e VALUES ('e')")

ai_collision =
  try do
    Coll.q(sock, "INSERT INTO cw_e VALUES ('é')")
    false
  rescue
    _ -> true
  end

IO.puts("--- Q10: PAD SPACE insert 'ab  ' next to 'ab': collision=#{pad_collision}")
IO.puts("--- Q11: ai_ci insert 'é' next to 'e': collision=#{ai_collision}")

results =
  results ++
    [
      Coll.verdict(
        "Q10 PAD SPACE: space-stripped-equal values collide on the PK",
        pad_collision,
        ""
      ),
      Coll.verdict("Q11 ai_ci: 'e' and 'é' collide on the PK", ai_collision, "")
    ]

# --- Q12: information_schema shapes ------------------------------------------------------------
IO.puts("--- Q12: information_schema ---")

is_rows =
  Coll.q(sock, """
  SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, CHARACTER_SET_NAME, COLLATION_NAME
  FROM information_schema.COLUMNS
  WHERE TABLE_SCHEMA='probe_db' AND TABLE_NAME IN ('cw_ai','cw_char','cw_lat','cw_bin')
    AND COLUMN_NAME = 'k'
  ORDER BY TABLE_NAME
  """)

for row <- is_rows, do: IO.puts("  #{inspect(row)}")

pad_attr =
  Coll.q(
    sock,
    "SELECT COLLATION_NAME, PAD_ATTRIBUTE FROM information_schema.COLLATIONS WHERE COLLATION_NAME IN ('utf8mb4_0900_ai_ci','utf8mb4_general_ci','utf8mb4_bin','latin1_swedish_ci')"
  )

IO.puts("  PAD_ATTRIBUTE: #{inspect(pad_attr)}")

# --- Q13: ENUM weight basis -----------------------------------------------------------------
IO.puts("--- Q13: ENUM('b','a') ---")
enum_ordered = Coll.rows1(sock, "SELECT k FROM cw_enum ORDER BY k")
enum_weights = Coll.q(sock, "SELECT k, HEX(WEIGHT_STRING(k)) FROM cw_enum")

enum_string_weights =
  Coll.q(sock, "SELECT HEX(WEIGHT_STRING(_utf8mb4 'b')), HEX(WEIGHT_STRING(_utf8mb4 'a'))")

IO.puts("  ORDER BY k (member position): #{inspect(enum_ordered)}")
IO.puts("  column weights: #{inspect(enum_weights)}")
IO.puts("  plain string weights b/a: #{inspect(enum_string_weights)}")

# --- Q14: batched weight query shape -----------------------------------------------------------
batched =
  Coll.q(
    sock,
    "SELECT HEX(WEIGHT_STRING(CONVERT(X'E4B8AD' USING utf8mb4))), HEX(WEIGHT_STRING(CONVERT(X'61' USING utf8mb4)))"
  )

IO.puts("--- Q14: batched introducer weight query: #{inspect(batched)}")

results =
  results ++
    [
      Coll.verdict(
        "Q14 batched multi-key weight query returns one row of weights",
        match?([[w1, w2]] when is_binary(w1) and is_binary(w2), batched),
        inspect(batched)
      )
    ]

# --- teardown --------------------------------------------------------------------------------
for t <- tables, do: Coll.q(sock, "DROP TABLE IF EXISTS #{t}")
:ok = :gen_tcp.close(sock)

IO.puts("\n=== #{Enum.count(results, & &1)}/#{length(results)} checks passed ===")
if Enum.any?(results, &(!&1)), do: exit(1)

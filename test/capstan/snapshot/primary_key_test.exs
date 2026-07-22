defmodule Capstan.Snapshot.PrimaryKeyTest do
  @moduledoc """
  `Capstan.Snapshot.PrimaryKey` — the C2 Ch3/Ch4 order-faithful correctness core.

  Two vector classes:

    * **Unit** (default suite) — pure functions over constructed `information_schema`
      resultset shapes (the text-protocol `[binary() | nil]` rows the live server would
      return) and over constructed PK values. The positive type allowlist, the
      NOT-NULL-complete unique-key fallback, the `canonical/2` equality form (incl. the
      BIGINT UNSIGNED decode across 2^63), and the `compare/2` cursor ordering are all
      exercised without a live server, RED-first.
    * **Live** (`@tag :live`, excluded by default — `mix test --only live`) — the marquee:
      `introspect/2` against `mysql-cdc-probe` on a throwaway schema whose tables carry an
      int PK, a composite `(INT, VARBINARY)` PK, a `VARCHAR` PK (refuse), a `BIGINT
      UNSIGNED` PK, and a nullable-unique-key table (refuse). Each classification is proven
      against the LIVE server's actual `information_schema` shape, not test literals
      (`feedback_failclosed_guard_validated_against_real_upstream_shape`), and `compare/2`
      is cross-checked against a live `ORDER BY pk`.
  """
  use ExUnit.Case, async: false

  alias Capstan.Protocol.Command
  alias Capstan.Protocol.Handshake
  alias Capstan.Query
  alias Capstan.Snapshot.PrimaryKey

  @schema "capstan_pk_marquee"

  ## ---------------------------------------------------------------------------
  ## resolve_pk_type/2 — the positive type allowlist (Ch3.2 / Ch4, tripwire 7)
  ##
  ## ACCEPT only signed/unsigned integer + BINARY/VARBINARY. REFUSE everything else
  ## with :snapshot_pk_unsupported_type — the collation-ordered string family (CHAR/
  ## VARCHAR/TEXT, both _bin and _ci report data_type varchar/char) and DECIMAL/DOUBLE/
  ## FLOAT/DATE/DATETIME/temporal, whose Elixir term-order diverges from MySQL ORDER BY.
  ## ---------------------------------------------------------------------------

  describe "resolve_pk_type/2 — the positive order-faithful allowlist" do
    test "signed integer family accepts" do
      assert {:ok, :tinyint} = PrimaryKey.resolve_pk_type("tinyint", "tinyint")
      assert {:ok, :smallint} = PrimaryKey.resolve_pk_type("smallint", "smallint")
      assert {:ok, :mediumint} = PrimaryKey.resolve_pk_type("mediumint", "mediumint")
      assert {:ok, :int} = PrimaryKey.resolve_pk_type("int", "int")
      assert {:ok, :bigint} = PrimaryKey.resolve_pk_type("bigint", "bigint")
    end

    test "unsigned integer family accepts (signedness read from COLUMN_TYPE)" do
      assert {:ok, :tinyint_unsigned} = PrimaryKey.resolve_pk_type("tinyint", "tinyint unsigned")
      assert {:ok, :int_unsigned} = PrimaryKey.resolve_pk_type("int", "int unsigned")
      assert {:ok, :bigint_unsigned} = PrimaryKey.resolve_pk_type("bigint", "bigint unsigned")
    end

    test "BINARY / VARBINARY accept" do
      assert {:ok, :binary} = PrimaryKey.resolve_pk_type("binary", "binary(16)")
      assert {:ok, :varbinary} = PrimaryKey.resolve_pk_type("varbinary", "varbinary(255)")
    end

    test "the collation-ordered STRING family refuses — both _bin and _ci report varchar/char" do
      # A `_bin` collation and a `_ci` collation BOTH report data_type varchar/char, so the
      # collation cannot distinguish them: the whole string family fails closed (tripwire 7
      # collation-string arm).
      assert {:error, :snapshot_pk_unsupported_type} =
               PrimaryKey.resolve_pk_type("varchar", "varchar(255)")

      assert {:error, :snapshot_pk_unsupported_type} =
               PrimaryKey.resolve_pk_type("char", "char(36)")

      assert {:error, :snapshot_pk_unsupported_type} =
               PrimaryKey.resolve_pk_type("text", "text")

      assert {:error, :snapshot_pk_unsupported_type} =
               PrimaryKey.resolve_pk_type("enum", "enum('a','b')")
    end

    test "DECIMAL / DOUBLE / FLOAT / temporal refuse" do
      assert {:error, :snapshot_pk_unsupported_type} =
               PrimaryKey.resolve_pk_type("decimal", "decimal(10,2)")

      assert {:error, :snapshot_pk_unsupported_type} =
               PrimaryKey.resolve_pk_type("double", "double")

      assert {:error, :snapshot_pk_unsupported_type} =
               PrimaryKey.resolve_pk_type("float", "float")

      assert {:error, :snapshot_pk_unsupported_type} = PrimaryKey.resolve_pk_type("date", "date")

      assert {:error, :snapshot_pk_unsupported_type} =
               PrimaryKey.resolve_pk_type("datetime", "datetime")

      assert {:error, :snapshot_pk_unsupported_type} = PrimaryKey.resolve_pk_type("bit", "bit(8)")
    end
  end

  ## ---------------------------------------------------------------------------
  ## resolve_key/2 — the pure key resolution over constructed information_schema rows.
  ##
  ## The RED-capable seam: introspect/2 fetches the two resultsets and delegates here,
  ## so the PK / unique-key fallback / type refusal logic is exercised without a server.
  ## Rows mirror the live text-protocol shape (all information_schema columns are NOT NULL).
  ## ---------------------------------------------------------------------------

  describe "resolve_key/2 — PK present" do
    test "a single INT PK resolves to pk_columns + pk_types" do
      index_rows = [["PRIMARY", "1", "id", "0"]]
      column_rows = [["id", "int", "int", "NO"]]

      assert {:ok, %{pk_columns: ["id"], pk_types: [:int]}} =
               PrimaryKey.resolve_key(index_rows, column_rows)
    end

    test "a BIGINT UNSIGNED PK resolves to :bigint_unsigned (tripwire 18 type arm)" do
      index_rows = [["PRIMARY", "1", "id", "0"]]
      column_rows = [["id", "bigint", "bigint unsigned", "NO"]]

      assert {:ok, %{pk_columns: ["id"], pk_types: [:bigint_unsigned]}} =
               PrimaryKey.resolve_key(index_rows, column_rows)
    end

    test "a composite (INT, VARBINARY) PK preserves ordinal order (tripwire 7 composite arm)" do
      # SEQ_IN_INDEX out of natural order in the resultset — resolve_key must sort by it.
      index_rows = [
        ["PRIMARY", "2", "region", "0"],
        ["PRIMARY", "1", "id", "0"]
      ]

      column_rows = [
        ["id", "int", "int", "NO"],
        ["region", "varbinary", "varbinary(8)", "NO"]
      ]

      assert {:ok, %{pk_columns: ["id", "region"], pk_types: [:int, :varbinary]}} =
               PrimaryKey.resolve_key(index_rows, column_rows)
    end

    test "a VARCHAR PK fails closed :snapshot_pk_unsupported_type (tripwire 7 collation arm)" do
      index_rows = [["PRIMARY", "1", "code", "0"]]
      column_rows = [["code", "varchar", "varchar(255)", "NO"]]

      assert {:error, :snapshot_pk_unsupported_type} =
               PrimaryKey.resolve_key(index_rows, column_rows)
    end

    test "a DECIMAL PK fails closed :snapshot_pk_unsupported_type" do
      index_rows = [["PRIMARY", "1", "amount", "0"]]
      column_rows = [["amount", "decimal", "decimal(20,4)", "NO"]]

      assert {:error, :snapshot_pk_unsupported_type} =
               PrimaryKey.resolve_key(index_rows, column_rows)
    end

    test "one unsupported column in a composite PK refuses the whole key" do
      index_rows = [
        ["PRIMARY", "1", "id", "0"],
        ["PRIMARY", "2", "label", "0"]
      ]

      column_rows = [
        ["id", "int", "int", "NO"],
        ["label", "varchar", "varchar(64)", "NO"]
      ]

      assert {:error, :snapshot_pk_unsupported_type} =
               PrimaryKey.resolve_key(index_rows, column_rows)
    end
  end

  describe "resolve_key/2 — no PK (tripwire 6 / tripwire 19)" do
    test "no PK and no unique key → :snapshot_table_no_primary_key (tripwire 6)" do
      # A non-unique secondary index does NOT substitute for a PK.
      index_rows = [["idx_created", "1", "created_at", "1"]]
      column_rows = [["created_at", "datetime", "datetime", "YES"]]

      assert {:error, :snapshot_table_no_primary_key} =
               PrimaryKey.resolve_key(index_rows, column_rows)
    end

    test "a NOT-NULL-complete unique key substitutes for a missing PK" do
      index_rows = [["uq_email", "1", "email_id", "0"]]
      column_rows = [["email_id", "bigint", "bigint", "NO"]]

      assert {:ok, %{pk_columns: ["email_id"], pk_types: [:bigint]}} =
               PrimaryKey.resolve_key(index_rows, column_rows)
    end

    test "a unique key with a NULLABLE column is refused :snapshot_table_no_primary_key (tripwire 19)" do
      # A nullable unique-key column admits NULL-keyed rows that `WHERE k > cursor` never
      # selects → silent gap. RED: accept it and NULL-keyed rows escape the backfill (Ch3.3).
      index_rows = [["uq_ext", "1", "ext_id", "0"]]
      column_rows = [["ext_id", "int", "int", "YES"]]

      assert {:error, :snapshot_table_no_primary_key} =
               PrimaryKey.resolve_key(index_rows, column_rows)
    end

    test "a composite unique key with ANY nullable column is refused (tripwire 19)" do
      index_rows = [
        ["uq_pair", "1", "a", "0"],
        ["uq_pair", "2", "b", "0"]
      ]

      column_rows = [
        ["a", "int", "int", "NO"],
        ["b", "int", "int", "YES"]
      ]

      assert {:error, :snapshot_table_no_primary_key} =
               PrimaryKey.resolve_key(index_rows, column_rows)
    end

    test "a NOT-NULL-complete unique key with an unsupported type refuses :snapshot_pk_unsupported_type" do
      index_rows = [["uq_code", "1", "code", "0"]]
      column_rows = [["code", "varchar", "varchar(64)", "NO"]]

      assert {:error, :snapshot_pk_unsupported_type} =
               PrimaryKey.resolve_key(index_rows, column_rows)
    end

    test "a nullable unique key alongside a NOT-NULL unique key picks the NOT-NULL one" do
      index_rows = [
        ["uq_nullable", "1", "maybe", "0"],
        ["uq_solid", "1", "solid_id", "0"]
      ]

      column_rows = [
        ["maybe", "int", "int", "YES"],
        ["solid_id", "int", "int", "NO"]
      ]

      assert {:ok, %{pk_columns: ["solid_id"], pk_types: [:int]}} =
               PrimaryKey.resolve_key(index_rows, column_rows)
    end
  end

  ## ---------------------------------------------------------------------------
  ## canonical/2 — the equality form for the sink ledger (Ch4).
  ##
  ## Canonicalization is in Elixir (equality), NOT MySQL collation (chunking orders in
  ## MySQL). The text-protocol string form and the binlog-decoded integer form of the
  ## SAME PK must canonicalize equal so the ledger reconciles them.
  ## ---------------------------------------------------------------------------

  describe "canonical/2 — single-column and composite forms" do
    test "a single integer canonicalizes to a bare integer" do
      assert PrimaryKey.canonical([:int], [5]) == 5
      assert PrimaryKey.canonical([:int], ["5"]) == 5
    end

    test "the text-protocol string form and the decoded integer form are equal (ledger reconciliation)" do
      assert PrimaryKey.canonical([:int], ["42"]) == PrimaryKey.canonical([:int], [42])
    end

    test "a single binary canonicalizes to its bytes" do
      assert PrimaryKey.canonical([:varbinary], [<<0xDE, 0xAD>>]) == <<0xDE, 0xAD>>
      assert PrimaryKey.canonical([:binary], [<<1, 2, 3>>]) == <<1, 2, 3>>
    end

    test "a composite canonicalizes to a tuple in column order" do
      assert PrimaryKey.canonical([:int, :varbinary], [5, <<0xAB>>]) == {5, <<0xAB>>}
      assert PrimaryKey.canonical([:int, :varbinary], ["5", <<0xAB>>]) == {5, <<0xAB>>}
    end
  end

  ## ---------------------------------------------------------------------------
  ## BIGINT UNSIGNED across 2^63 (Ch3.1, tripwire 18).
  ##
  ## RED: a signed-wrap decoder yields the true value only below 2^63; at/above it it
  ## produces a negative, so canonical([:bigint_unsigned], [2^63]) would be -2^63 and
  ## the compare order collapses. Assert the TRUE unsigned value across the boundary.
  ## ---------------------------------------------------------------------------

  describe "canonical/2 + compare/2 — BIGINT UNSIGNED across 2^63 (tripwire 18)" do
    @two63 9_223_372_036_854_775_808
    @two63m1 9_223_372_036_854_775_807
    @two64m1 18_446_744_073_709_551_615

    test "the text form decodes to the TRUE unsigned value across 2^63" do
      assert PrimaryKey.canonical([:bigint_unsigned], ["0"]) == 0
      assert PrimaryKey.canonical([:bigint_unsigned], ["#{@two63m1}"]) == @two63m1
      assert PrimaryKey.canonical([:bigint_unsigned], ["#{@two63}"]) == @two63
      assert PrimaryKey.canonical([:bigint_unsigned], ["#{@two64m1}"]) == @two64m1
    end

    test "a signed-wrapped raw integer is normalized to its true unsigned value" do
      # A signed-wrap decoder would present 2^63 as -2^63 and 2^64-1 as -1; canonical must
      # unwrap them for an UNSIGNED column (the RED-capable guard for tripwire 18).
      assert PrimaryKey.canonical([:bigint_unsigned], [-1]) == @two64m1
      assert PrimaryKey.canonical([:bigint_unsigned], [-@two63]) == @two63
    end

    test "a SIGNED bigint keeps a legitimate negative value (no spurious unwrap)" do
      assert PrimaryKey.canonical([:bigint], [-1]) == -1
      assert PrimaryKey.canonical([:bigint], ["-1"]) == -1
    end

    test "compare/2 orders {0, 2^63-1, 2^63, 2^64-1} strictly ascending" do
      ascending = [0, @two63m1, @two63, @two64m1]

      # Each consecutive pair is strictly increasing under compare/2.
      Enum.zip(ascending, tl(ascending))
      |> Enum.each(fn {a, b} -> assert PrimaryKey.compare(a, b) == :lt end)

      # Sorting canonical unsigned values via compare/2 reproduces the ascending order;
      # a signed-wrap decoder would reorder 2^63… before 0… and break this.
      canon =
        ["#{@two63}", "0", "#{@two64m1}", "#{@two63m1}"]
        |> Enum.map(&PrimaryKey.canonical([:bigint_unsigned], [&1]))

      sorted = Enum.sort(canon, fn a, b -> PrimaryKey.compare(a, b) != :gt end)
      assert sorted == ascending
    end
  end

  ## ---------------------------------------------------------------------------
  ## compare/2 — the cursor ordering (must match MySQL ORDER BY for the allowlist).
  ## ---------------------------------------------------------------------------

  describe "compare/2 — integer and binary ordering" do
    test "integers order numerically" do
      assert PrimaryKey.compare(1, 2) == :lt
      assert PrimaryKey.compare(2, 2) == :eq
      assert PrimaryKey.compare(3, 2) == :gt
    end

    test "binaries order byte-wise, shorter-prefix first (matches MySQL VARBINARY)" do
      assert PrimaryKey.compare(<<0x61, 0x62>>, <<0x61, 0x63>>) == :lt
      assert PrimaryKey.compare(<<0x61, 0x62>>, <<0x61, 0x62, 0x63>>) == :lt
      assert PrimaryKey.compare(<<0x61, 0x62>>, <<0x61, 0x62>>) == :eq
    end
  end

  describe "compare/2 — composite (INT, BINARY) tuple order matches MySQL ORDER BY (tripwire 7)" do
    test "orders by the first column, then the second (lexicographic row-value order)" do
      # MySQL `ORDER BY id, region` on (INT, BINARY): primary key column first, then the
      # binary column byte-wise. compare/2 over canonical tuples must reproduce it exactly.
      rows = [
        {2, <<0x00>>},
        {1, <<0xFF>>},
        {1, <<0x01>>},
        {2, <<0x00, 0x00>>},
        {1, <<0x01, 0x00>>}
      ]

      canon = Enum.map(rows, fn {i, b} -> PrimaryKey.canonical([:int, :binary], [i, b]) end)
      sorted = Enum.sort(canon, fn a, b -> PrimaryKey.compare(a, b) != :gt end)

      expected = [
        {1, <<0x01>>},
        {1, <<0x01, 0x00>>},
        {1, <<0xFF>>},
        {2, <<0x00>>},
        {2, <<0x00, 0x00>>}
      ]

      assert sorted == expected
    end
  end

  ## ===========================================================================
  ## Live marquee — real information_schema shape, excluded by default
  ## (mix test --only live). Proves each classification against the LIVE server's
  ## actual resultset, not test literals.
  ## ===========================================================================

  describe "live — introspect/2 against mysql-cdc-probe information_schema" do
    @describetag :live

    setup do
      {:ok, q} =
        Query.establish(connection: live_conn(), connect_fun: &Query.default_connect/1)

      on_exit(fn -> Query.close(q) end)
      {:ok, query: q}
    end

    test "an INT PK introspects to [:int]", %{query: q} do
      assert {:ok, %{pk_columns: ["id"], pk_types: [:int]}} =
               PrimaryKey.introspect(q, {@schema, "int_pk"})
    end

    test "a BIGINT UNSIGNED PK introspects to [:bigint_unsigned]", %{query: q} do
      assert {:ok, %{pk_columns: ["id"], pk_types: [:bigint_unsigned]}} =
               PrimaryKey.introspect(q, {@schema, "ubigint_pk"})
    end

    test "a composite (INT, VARBINARY) PK introspects order-faithfully", %{query: q} do
      assert {:ok, %{pk_columns: ["id", "region"], pk_types: [:int, :varbinary]}} =
               PrimaryKey.introspect(q, {@schema, "composite_pk"})
    end

    test "a VARCHAR PK is refused :snapshot_pk_unsupported_type", %{query: q} do
      assert {:error, :snapshot_pk_unsupported_type} =
               PrimaryKey.introspect(q, {@schema, "varchar_pk"})
    end

    test "a nullable-unique-key table (no PK) is refused :snapshot_table_no_primary_key", %{
      query: q
    } do
      assert {:error, :snapshot_table_no_primary_key} =
               PrimaryKey.introspect(q, {@schema, "nullable_unique"})
    end

    test "compare/2 reproduces the live MySQL `ORDER BY pk` for a composite key", %{query: q} do
      # Populate the composite table, read the live MySQL ordering, and assert compare/2
      # sorts the same canonical rows identically — proven against REAL bytes, not literals.
      assert {:ok, %{pk_columns: cols, pk_types: types}} =
               PrimaryKey.introspect(q, {@schema, "composite_pk"})

      select_cols = Enum.map_join(cols, ", ", &"`#{&1}`")

      assert {:ok, ordered_rows} =
               Query.query(
                 q,
                 "SELECT #{select_cols} FROM `#{@schema}`.`composite_pk` ORDER BY #{select_cols}"
               )

      mysql_order = Enum.map(ordered_rows, &PrimaryKey.canonical(types, &1))

      compare_order =
        Enum.sort(mysql_order, fn a, b -> PrimaryKey.compare(a, b) != :gt end)

      assert compare_order == mysql_order
      # Non-vacuity: there is more than one distinct row to order.
      assert length(Enum.uniq(mysql_order)) > 1
    end
  end

  ## ---------------------------------------------------------------------------
  ## live setup — throwaway schema built by root (native), dropped in teardown
  ## ---------------------------------------------------------------------------

  setup_all do
    if :live in ExUnit.configuration()[:include] do
      socket = live_root_socket()

      try do
        run!(socket, "DROP DATABASE IF EXISTS #{@schema}")
        run!(socket, "CREATE DATABASE #{@schema}")

        run!(socket, "CREATE TABLE #{@schema}.int_pk (id INT PRIMARY KEY, payload INT)")

        run!(
          socket,
          "CREATE TABLE #{@schema}.ubigint_pk (id BIGINT UNSIGNED PRIMARY KEY, payload INT)"
        )

        run!(
          socket,
          "CREATE TABLE #{@schema}.composite_pk " <>
            "(id INT NOT NULL, region VARBINARY(8) NOT NULL, payload INT, PRIMARY KEY (id, region))"
        )

        run!(
          socket,
          "INSERT INTO #{@schema}.composite_pk (id, region, payload) VALUES " <>
            "(2, 0x00, 1), (1, 0xFF, 2), (1, 0x01, 3), (2, 0x0000, 4), (1, 0x0100, 5)"
        )

        run!(
          socket,
          "CREATE TABLE #{@schema}.varchar_pk (code VARCHAR(64) PRIMARY KEY, payload INT)"
        )

        # A table with NO primary key whose only unique key has a NULLABLE column.
        run!(
          socket,
          "CREATE TABLE #{@schema}.nullable_unique (ext_id INT NULL, payload INT, UNIQUE KEY uq_ext (ext_id))"
        )

        ensure_sha2_user!(socket)

        on_exit(fn ->
          teardown = live_root_socket()

          try do
            run!(teardown, "DROP DATABASE IF EXISTS #{@schema}")
          after
            close_socket(teardown)
          end
        end)
      after
        close_socket(socket)
      end
    end

    :ok
  end

  ## ---------------------------------------------------------------------------
  ## helpers — live substrate
  ## ---------------------------------------------------------------------------

  defp live_conn do
    [
      host: "127.0.0.1",
      port: 5633,
      username: "capstan_sha2",
      password: "capstan_sha2_pw",
      database: @schema,
      ssl: true,
      ssl_opts: [verify: :verify_none]
    ]
  end

  defp live_root_socket do
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
      {:error, reason} -> raise "primary_key_test: #{sql} failed #{inspect(reason)}"
    end
  end

  defp close_socket({:gen_tcp, s}), do: :gen_tcp.close(s)
  defp close_socket({:ssl, s}), do: :ssl.close(s)
end

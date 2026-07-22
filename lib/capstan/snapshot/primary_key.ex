defmodule Capstan.Snapshot.PrimaryKey do
  @moduledoc """
  The order-faithful primary-key core of the initial snapshot (C2, design Ch3/Ch4).

  A chunk pages by `ORDER BY pk` in MySQL while the cursor-gate classifies streamed
  changes by `k ≤ cursor` in Elixir. For that pairing to be correct — no silently
  mis-classified change (delivered vs suppressed → gap/dup) — the PK's Elixir term-order
  MUST provably match MySQL's `ORDER BY`. This module enforces that with a **positive type
  allowlist** and supplies the two operations the coordinator needs on PK values:

    * `canonical/2` — the **equality** form for the sink ledger. Canonicalization happens
      in Elixir (reconciliation-equality), NOT MySQL collation. The text-protocol string
      form of a PK (a chunk read) and the binlog-decoded integer/binary form (a streamed
      change) canonicalize to the SAME term, so the ledger reconciles a chunk row against a
      streamed change to the same key.
    * `compare/2` — the **ordering** for the cursor-gate (`k ≤ cursor`). Over the allowlist
      it is exactly MySQL `ORDER BY`: integers order numerically, `BINARY`/`VARBINARY` order
      byte-wise (shorter prefix first), and composites order lexicographically as tuples
      (matching MySQL row-value ORDER BY).

  ## The positive allowlist (Ch3.2 / Ch4)

  ACCEPT only the types whose Elixir order provably matches MySQL `ORDER BY`:

    * signed/unsigned integer — `TINYINT`/`SMALLINT`/`MEDIUMINT`/`INT`/`BIGINT`
    * `BINARY` / `VARBINARY` (byte-ordered)
    * composites of the above (tuple compare)

  REFUSE everything else with `:snapshot_pk_unsupported_type`:

    * the collation-ordered STRING family — `CHAR`/`VARCHAR`/`TEXT` (both a `_bin` and a
      `_ci` collation report `data_type` `char`/`varchar`, so collation cannot distinguish
      them; the whole family is refused). A collation order Elixir cannot reproduce would
      let the cursor-gate mis-classify a change.
    * `DECIMAL`/`DOUBLE`/`FLOAT`/`DATE`/`DATETIME`/temporal and any other type — their
      Elixir term-order diverges from MySQL.

  ## BIGINT UNSIGNED across 2^63 (Ch3.1)

  An unsigned 64-bit PK spans `[0, 2^64)`. A signed-wrap decoder reads the high half as
  negative (`2^63 → -2^63`, `2^64-1 → -1`), so it would order `2^63…` *before* `0…` and the
  cursor-gate would mis-classify a high-half key. `canonical/2` decodes to the TRUE unsigned
  value: a text form parses directly, and a raw integer that arrived signed-wrapped (a
  negative for an unsigned column) is normalized back into `[0, 2^width)`.

  ## No PK → fail closed

  `introspect/2` reads `information_schema` for the PRIMARY KEY. A table with no PK accepts
  a UNIQUE key fallback ONLY IF **every** column of that key is `NOT NULL` — a nullable
  unique-key column admits NULL-keyed rows that `WHERE k > cursor` never selects (a silent
  gap, Ch3.3), so such a key is refused `:snapshot_table_no_primary_key`. A table with
  neither is refused the same way.

  ## Rule 1

  No PK value is ever logged or telemetered here. Errors are value-free atoms. The
  `information_schema` queries embed only the schema/table (structural identity — the same
  fields telemetry may carry), never a row value, and are escaped as SQL string literals.
  """

  import Bitwise

  alias Capstan.Query

  # The integer families and their bit widths (used to normalize a signed-wrapped unsigned).
  @signed_bits %{tinyint: 8, smallint: 16, mediumint: 24, int: 32, bigint: 64}
  @unsigned_bits %{
    tinyint_unsigned: 8,
    smallint_unsigned: 16,
    mediumint_unsigned: 24,
    int_unsigned: 32,
    bigint_unsigned: 64
  }

  @integer_data_types ~w(tinyint smallint mediumint int bigint)

  @typedoc "An order-faithful PK column type atom (integer family, `:binary`, `:varbinary`)."
  @type pk_type ::
          :tinyint
          | :tinyint_unsigned
          | :smallint
          | :smallint_unsigned
          | :mediumint
          | :mediumint_unsigned
          | :int
          | :int_unsigned
          | :bigint
          | :bigint_unsigned
          | :binary
          | :varbinary

  @typedoc "The introspected key: the ordered PK columns and their order-faithful types."
  @type key :: %{pk_columns: [String.t()], pk_types: [pk_type()]}

  @typedoc "A canonical PK: a single value (single-column PK) or a tuple (composite PK)."
  @type canonical_pk :: integer() | binary() | tuple()

  ## ---------------------------------------------------------------------------
  ## introspect/2 — read information_schema for the PK / NOT-NULL-complete unique key
  ## ---------------------------------------------------------------------------

  @doc """
  Introspects the PRIMARY KEY (or a NOT-NULL-complete UNIQUE key fallback) of
  `{schema, table}` over a `Capstan.Query` handle.

  Returns `{:ok, %{pk_columns: [...], pk_types: [...]}}` for an order-faithful key, or a
  value-free `{:error, reason}`: `:snapshot_pk_unsupported_type` when a key column's type is
  outside the allowlist, `:snapshot_table_no_primary_key` when there is no PK and no
  NOT-NULL-complete unique key. A transport/query fault propagates its value-free reason
  from `Capstan.Query.query/2`.
  """
  @spec introspect(Query.t(), {String.t(), String.t()}) ::
          {:ok, key()} | {:error, atom()}
  def introspect(%Query{} = query, {schema, table}) do
    with {:ok, index_rows} <- Query.query(query, index_sql(schema, table)),
         {:ok, column_rows} <- Query.query(query, column_sql(schema, table)) do
      resolve_key(index_rows, column_rows)
    end
  end

  @doc """
  The pure key resolution over the two `information_schema` resultsets — the RED-capable
  seam `introspect/2` delegates to (so the PK / unique-key fallback / type-refusal logic is
  provable without a live server).

    * `index_rows` — `STATISTICS` rows `[INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME, NON_UNIQUE]`
    * `column_rows` — `COLUMNS` rows `[COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE]`

  (all columns are `NOT NULL` in `information_schema`, so no cell is `nil`). Returns the same
  `{:ok, key()} | {:error, reason}` contract as `introspect/2`.
  """
  @spec resolve_key([[binary() | nil]], [[binary() | nil]]) :: {:ok, key()} | {:error, atom()}
  def resolve_key(index_rows, column_rows) do
    columns = columns_by_name(column_rows)

    case pk_columns(index_rows) do
      [] -> resolve_unique_fallback(index_rows, columns)
      pk_cols -> build_key(pk_cols, columns)
    end
  end

  ## ---------------------------------------------------------------------------
  ## resolve_pk_type/2 — the positive allowlist (the RED-capable type arm)
  ## ---------------------------------------------------------------------------

  @doc """
  Classifies a single column's `(DATA_TYPE, COLUMN_TYPE)` to an order-faithful `pk_type()`,
  or refuses `:snapshot_pk_unsupported_type`.

  Signedness rides `COLUMN_TYPE` (which carries the `unsigned` attribute); `DATA_TYPE` alone
  cannot distinguish it. The whole string family (`char`/`varchar`/`text`/`enum`/…) and every
  non-integer, non-binary type are refused (their Elixir order diverges from MySQL).
  """
  @spec resolve_pk_type(binary(), binary()) ::
          {:ok, pk_type()} | {:error, :snapshot_pk_unsupported_type}
  def resolve_pk_type(data_type, column_type)
      when is_binary(data_type) and is_binary(column_type) do
    dt = String.downcase(data_type)

    cond do
      dt in @integer_data_types -> {:ok, integer_type(dt, unsigned?(column_type))}
      dt == "binary" -> {:ok, :binary}
      dt == "varbinary" -> {:ok, :varbinary}
      true -> {:error, :snapshot_pk_unsupported_type}
    end
  end

  ## ---------------------------------------------------------------------------
  ## canonical/2 — the equality form (Ch4)
  ## ---------------------------------------------------------------------------

  @doc """
  Builds the canonical equality form of a PK from `pk_types` and the raw column values
  (`raw_values`, one per PK column in order).

  A raw value is either a text-protocol string (a chunk read) or a binlog-decoded term (a
  streamed change); both forms of the SAME PK canonicalize equal. Integers normalize to
  their true value (an unsigned column's signed-wrapped negative is unwrapped into
  `[0, 2^width)`); binaries pass through their bytes. A single-column PK returns a bare value;
  a composite returns a tuple in column order.
  """
  @spec canonical([pk_type()], [binary() | integer()]) :: canonical_pk()
  def canonical(pk_types, raw_values)
      when is_list(pk_types) and is_list(raw_values) and length(pk_types) == length(raw_values) do
    case Enum.zip_with(pk_types, raw_values, &canonical_one/2) do
      [single] -> single
      many -> List.to_tuple(many)
    end
  end

  ## ---------------------------------------------------------------------------
  ## compare/2 — the cursor ordering (Ch4 enforcement)
  ## ---------------------------------------------------------------------------

  @doc """
  Compares two canonical PK values (from `canonical/2`): `:lt`, `:eq`, or `:gt`.

  Over the allowlist this is exactly MySQL `ORDER BY`: Erlang term order coincides with MySQL
  numeric order for integers, byte-wise order for `BINARY`/`VARBINARY`, and — for same-arity
  composites — lexicographic tuple order for row-value ORDER BY. Both arguments must be
  canonical forms; the caller compares like against like (a table's PK shape is fixed).
  """
  @spec compare(canonical_pk(), canonical_pk()) :: :lt | :eq | :gt
  def compare(a, b) when a < b, do: :lt
  def compare(a, b) when a > b, do: :gt
  def compare(_a, _b), do: :eq

  ## ---------------------------------------------------------------------------
  ## key resolution internals
  ## ---------------------------------------------------------------------------

  # The PK columns in ordinal order (empty when the table has no PRIMARY KEY).
  defp pk_columns(index_rows) do
    index_rows
    |> Enum.filter(fn [name | _] -> name == "PRIMARY" end)
    |> order_by_seq()
  end

  # NOT-NULL-complete unique-key fallback: among the unique keys whose every column is
  # NOT NULL (a nullable column admits NULL-keyed rows `WHERE k > cursor` never selects —
  # Ch3.3), pick the first (by index name) whose types are all order-faithful. If a
  # NOT-NULL-complete unique key exists but none is type-supported, that is an unsupported
  # type; if none is NOT-NULL-complete, the table has no usable primary key.
  defp resolve_unique_fallback(index_rows, columns) do
    candidates =
      index_rows
      |> unique_keys()
      |> Enum.filter(fn {_name, cols} -> Enum.all?(cols, &not_nullable?(&1, columns)) end)

    case candidates do
      [] -> {:error, :snapshot_table_no_primary_key}
      keys -> first_supported_key(keys, columns)
    end
  end

  # The first candidate unique key (already NOT-NULL-complete) whose types are all
  # order-faithful; else :snapshot_pk_unsupported_type (a usable key exists but its type
  # is not supported).
  defp first_supported_key(keys, columns) do
    Enum.reduce_while(keys, {:error, :snapshot_pk_unsupported_type}, fn {_name, cols}, _acc ->
      case build_key(cols, columns) do
        {:ok, _} = ok -> {:halt, ok}
        {:error, _} -> {:cont, {:error, :snapshot_pk_unsupported_type}}
      end
    end)
  end

  # The unique (NON_UNIQUE = "0") non-PRIMARY indexes, each as `{name, ordered_columns}`,
  # sorted by index name for a deterministic pick.
  defp unique_keys(index_rows) do
    index_rows
    |> Enum.filter(fn [name, _seq, _col, non_unique] ->
      non_unique == "0" and name != "PRIMARY"
    end)
    |> Enum.group_by(fn [name | _] -> name end)
    |> Enum.map(fn {name, rows} -> {name, order_by_seq(rows)} end)
    |> Enum.sort_by(fn {name, _cols} -> name end)
  end

  # Order an index's rows by SEQ_IN_INDEX (an integer, so "10" sorts after "2") → column names.
  defp order_by_seq(rows) do
    rows
    |> Enum.sort_by(fn [_name, seq | _] -> String.to_integer(seq) end)
    |> Enum.map(fn [_name, _seq, col | _] -> col end)
  end

  defp columns_by_name(column_rows) do
    Map.new(column_rows, fn [name, data_type, column_type, is_nullable] ->
      {name, %{data_type: data_type, column_type: column_type, nullable?: is_nullable == "YES"}}
    end)
  end

  # A column is NOT NULL only when it is present and IS_NULLABLE = "NO"; an absent column
  # (a key referencing a column the COLUMNS resultset lacks) fails closed as "nullable" so
  # the key is not accepted.
  defp not_nullable?(col, columns) do
    case Map.get(columns, col) do
      %{nullable?: nullable?} -> not nullable?
      nil -> false
    end
  end

  # Resolve every key column's type through the allowlist; any unsupported (or missing)
  # column refuses the whole key.
  defp build_key(cols, columns) do
    cols
    |> Enum.reduce_while({:ok, []}, fn col, {:ok, acc} ->
      with %{data_type: dt, column_type: ct} <- Map.get(columns, col),
           {:ok, type} <- resolve_pk_type(dt, ct) do
        {:cont, {:ok, [type | acc]}}
      else
        _ -> {:halt, {:error, :snapshot_pk_unsupported_type}}
      end
    end)
    |> case do
      {:ok, rev_types} -> {:ok, %{pk_columns: cols, pk_types: Enum.reverse(rev_types)}}
      {:error, _} = error -> error
    end
  end

  ## ---------------------------------------------------------------------------
  ## type / value helpers
  ## ---------------------------------------------------------------------------

  defp unsigned?(column_type),
    do: column_type |> String.downcase() |> String.contains?("unsigned")

  defp integer_type("tinyint", false), do: :tinyint
  defp integer_type("tinyint", true), do: :tinyint_unsigned
  defp integer_type("smallint", false), do: :smallint
  defp integer_type("smallint", true), do: :smallint_unsigned
  defp integer_type("mediumint", false), do: :mediumint
  defp integer_type("mediumint", true), do: :mediumint_unsigned
  defp integer_type("int", false), do: :int
  defp integer_type("int", true), do: :int_unsigned
  defp integer_type("bigint", false), do: :bigint
  defp integer_type("bigint", true), do: :bigint_unsigned

  defp canonical_one(:binary, value) when is_binary(value), do: value
  defp canonical_one(:varbinary, value) when is_binary(value), do: value

  defp canonical_one(type, value) do
    cond do
      Map.has_key?(@signed_bits, type) -> to_integer(value)
      Map.has_key?(@unsigned_bits, type) -> to_unsigned(value, Map.fetch!(@unsigned_bits, type))
      true -> raise ArgumentError, "capstan: non-order-faithful pk type #{inspect(type)}"
    end
  end

  defp to_integer(value) when is_integer(value), do: value
  defp to_integer(value) when is_binary(value), do: String.to_integer(value)

  # Normalize to the true unsigned value: a signed-wrapped raw integer (negative for an
  # unsigned column) is folded back into `[0, 2^bits)`; a non-negative value is already true.
  defp to_unsigned(value, bits) do
    case to_integer(value) do
      n when n < 0 -> n + (1 <<< bits)
      n -> n
    end
  end

  ## ---------------------------------------------------------------------------
  ## information_schema SQL (schema/table are structural identity; escaped as literals)
  ## ---------------------------------------------------------------------------

  defp index_sql(schema, table) do
    "SELECT INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME, NON_UNIQUE " <>
      "FROM information_schema.STATISTICS " <>
      "WHERE TABLE_SCHEMA = #{literal(schema)} AND TABLE_NAME = #{literal(table)} " <>
      "ORDER BY INDEX_NAME, SEQ_IN_INDEX"
  end

  defp column_sql(schema, table) do
    "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE " <>
      "FROM information_schema.COLUMNS " <>
      "WHERE TABLE_SCHEMA = #{literal(schema)} AND TABLE_NAME = #{literal(table)}"
  end

  # A single-quoted SQL string literal; backslash and single-quote escaped for the MySQL
  # default sql_mode. Guards against a schema/table name that would otherwise break the query.
  defp literal(value) do
    escaped = value |> String.replace("\\", "\\\\") |> String.replace("'", "''")
    "'" <> escaped <> "'"
  end
end

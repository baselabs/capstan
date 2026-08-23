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

  ## The positive allowlist (Ch3.2 / Ch4; the string arm is ADR-0012)

  ACCEPT the types whose comparison against MySQL `ORDER BY` is provably faithful:

    * signed/unsigned integer — `TINYINT`/`SMALLINT`/`MEDIUMINT`/`INT`/`BIGINT`
    * `BINARY` / `VARBINARY` (byte-ordered)
    * `CHAR` / `VARCHAR` — collation-ordered, with **the server as the only collation
      oracle**: a string PK's canonical form is its collation WEIGHT BYTES (probe-proven:
      weight-byte order == `ORDER BY` for `ai_ci`, both `_bin` weight forms, PAD SPACE over
      distinct keys, multi-level `as_cs`, and composites). The chunk read selects
      `WEIGHT_STRING(pk)` alongside the row; the stream side resolves weights through
      `resolve_weights/4` (a COLLATE-pinned `CONVERT(X'..' USING charset)` introducer over the
      binlog's raw column bytes — an unpinned introducer computes the charset-DEFAULT
      collation's weights, a different order space). Distinct PK values are collation-distinct
      by the PK constraint itself, so weights are a faithful key identity.
    * composites of the above (tuple compare; a string position compares by its weight)

  REFUSE everything else with `:snapshot_pk_unsupported_type`:

    * the TEXT family — order semantics are consistent, but a TEXT prefix PK pays a measured
      **filesort per chunk page** (`Using filesort` vs VARCHAR's `Using index`;
      `probe/collation_weight_probe.exs` Q6b) — superlinear backfill.
    * `ENUM`/`SET` — the column's `ORDER BY` is member-position order and its column weights
      are position-based, while any stream-side introducer computes STRING weights — two
      disagreeing order spaces, no uniform mechanism (probe Q13).
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

  # The stream-side weight resolution is CHUNK-BOUNDED: one SELECT-list term per distinct raw
  # value, at most this many per statement, so a bulk-load transaction cannot exceed
  # max_allowed_packet and deterministically burn the retry budget (ADR-0012, adversarial
  # review finding 3).
  @weight_batch_size 128

  @typedoc "An order-faithful PK column type atom (integer family, `:binary`, `:varbinary`, `:char`, `:varchar`)."
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
          | :char
          | :varchar

  @typedoc """
  The introspected key: the ordered PK columns, their order-faithful types, and — for the
  string columns — the charset/collation pair that pins every weight and cursor literal
  (ADR-0012). A non-string column's charset/collation entry is `nil` (mirroring
  `information_schema.COLUMNS`, where `CHARACTER_SET_NAME`/`COLLATION_NAME` are NULL for
  non-string columns).
  """
  @type key :: %{
          pk_columns: [String.t()],
          pk_types: [pk_type()],
          pk_charsets: [nil | String.t()],
          pk_collations: [nil | String.t()]
        }

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
    * `column_rows` — `COLUMNS` rows
      `[COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE, CHARACTER_SET_NAME, COLLATION_NAME]`

  (the four `STATISTICS` cells are `NOT NULL`; the charset/collation cells are NULL for
  non-string columns, so those two cells may be `nil`). Returns the same
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
  cannot distinguish it. `CHAR`/`VARCHAR` accept (ADR-0012 — the weight path carries their
  ordering); the TEXT family (measured per-page filesort) and `ENUM`/`SET` (position-based
  order no introducer weight path reproduces) refuse, as does every non-integer, non-binary,
  non-char/varchar type (its Elixir order diverges from MySQL).
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
      dt == "char" -> {:ok, :char}
      dt == "varchar" -> {:ok, :varchar}
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
  `[0, 2^width)`); binaries pass through their bytes; a string column's value is the
  `{raw, weight}` pair its caller assembled (the weight being the canonical bytes —
  ADR-0012; a bare binary for a string column raises, value-free, rather than silently
  comparing raws). A single-column PK returns a bare value; a composite returns a tuple in
  column order.
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
  ## weight resolution (ADR-0012) — the server as the only collation oracle
  ## ---------------------------------------------------------------------------

  @doc """
  Builds the COLLATE-pinned weight-resolution SQL for a batch of raw column values.

  One `WEIGHT_STRING(CONVERT(X'<hex>' USING charset) COLLATE collation)` term per raw, in
  order — the result row's columns pair positionally with `raws`. The `COLLATE` pin is
  LOAD-BEARING: without it the `CONVERT` result carries the charset-DEFAULT collation, whose
  weights are a different order space (probed: a column's `as_cs` multi-level weight vs the
  unpinned `ai_ci` primary-only weight), and the unpinned form in a `WHERE` additionally
  raises ERROR 1267 on non-default collations. Hex literals carry arbitrary bytes (NULs,
  quotes, backslashes, empty) with no string-literal escaping surface.
  """
  @spec weight_sql(String.t(), String.t(), [binary()]) :: String.t()
  def weight_sql(charset, collation, raws)
      when is_binary(charset) and is_binary(collation) and is_list(raws) do
    terms =
      Enum.map_join(raws, ", ", fn raw ->
        "WEIGHT_STRING(CONVERT(X'#{Base.encode16(raw)}' USING #{charset}) COLLATE #{collation})"
      end)

    "SELECT #{terms}"
  end

  @doc """
  Resolves the collation weight bytes for a batch of raw PK-column values over a
  `Capstan.Query` handle — `{:ok, %{raw => weight}}`, or a value-free `{:error, reason}`.

  The stream-side arm of ADR-0012: a binlog-decoded string PK arrives as the column's raw
  bytes (the row event carries no charset), and its canonical form is the weight the SERVER
  computes for those bytes under the column's charset + collation. The batch is chunk-bounded
  (#{@weight_batch_size} values per statement) so one bulk-load transaction cannot exceed
  `max_allowed_packet`; a query fault surfaces as the scrubbed `Capstan.Query` atom for the
  caller to budget and halt on.
  """
  @spec resolve_weights(Query.t(), String.t(), String.t(), [binary()]) ::
          {:ok, %{binary() => binary()}} | {:error, atom()}
  def resolve_weights(%Query{} = query, charset, collation, raws) do
    raws
    |> Enum.chunk_every(@weight_batch_size)
    |> Enum.reduce_while({:ok, %{}}, fn batch, {:ok, acc} ->
      case Query.query(query, weight_sql(charset, collation, batch)) do
        {:ok, [row]} when length(row) == length(batch) ->
          {:cont, {:ok, Map.merge(acc, Map.new(Enum.zip(batch, row)))}}

        {:ok, _malformed} ->
          # A weight row must have exactly one column per raw — anything else is a broken
          # resultset shape, classified with the query faults (value-free).
          {:halt, {:error, :snapshot_pk_weight_failed}}

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
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

  defp columns_by_name(column_rows) do
    Map.new(column_rows, fn [name, data_type, column_type, is_nullable, charset, collation] ->
      {name,
       %{
         data_type: data_type,
         column_type: column_type,
         nullable?: is_nullable == "YES",
         charset: charset,
         collation: collation
       }}
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
  # column refuses the whole key. A string column additionally requires its charset AND
  # collation from the resultset — the pin every weight/cursor literal is built from — and a
  # nil one (a broken introspection shape) refuses the key fail-closed.
  defp build_key(cols, columns) do
    cols
    |> Enum.reduce_while({:ok, []}, fn col, {:ok, acc} ->
      with %{data_type: dt, column_type: ct, charset: cs, collation: coll} <-
             Map.get(columns, col),
           {:ok, type} <- resolve_pk_type(dt, ct),
           :ok <- string_pin_ok?(type, cs, coll) do
        {:cont, {:ok, [{type, cs, coll} | acc]}}
      else
        _ -> {:halt, {:error, :snapshot_pk_unsupported_type}}
      end
    end)
    |> case do
      {:ok, rev} ->
        {types, charsets, collations} = unzip_key(rev)

        {:ok,
         %{
           pk_columns: cols,
           pk_types: types,
           pk_charsets: charsets,
           pk_collations: collations
         }}

      {:error, _} = error ->
        error
    end
  end

  # A string PK column must carry both a charset and a collation; anything else is a broken
  # introspection shape and refuses the key (never an unpinnable literal).
  defp string_pin_ok?(type, charset, collation) when type in [:char, :varchar] do
    if is_binary(charset) and charset != "" and is_binary(collation) and collation != "",
      do: :ok,
      else: {:error, :snapshot_pk_unsupported_type}
  end

  defp string_pin_ok?(_type, _charset, _collation), do: :ok

  defp unzip_key(rev) do
    entries = Enum.reverse(rev)

    {
      Enum.map(entries, fn {type, _cs, _coll} -> type end),
      Enum.map(entries, fn {_type, cs, _coll} -> cs end),
      Enum.map(entries, fn {_type, _cs, coll} -> coll end)
    }
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

  # A string PK column's canonical form is its WEIGHT BYTES — the server-computed collation
  # sort key (ADR-0012). Callers that hold both halves pass the {raw, weight} pair (the
  # cursor-gate via its spec-carried weights; the chunk reader via the appended
  # WEIGHT_STRING columns). A bare binary here means the weight was never resolved — a
  # caller bug, not a data condition: fail LOUD and VALUE-FREE (Rule 1 — the message carries
  # no value), never silently fall back to comparing raws (that is exactly the byte-order
  # divergence C2a retires).
  defp canonical_one(type, {_raw, weight}) when type in [:char, :varchar] and is_binary(weight),
    do: weight

  defp canonical_one(type, _raw_without_weight) when type in [:char, :varchar] do
    raise ArgumentError,
          "capstan: #{type} primary-key column canonicalized without a collation weight " <>
            "(the server-computed weight must be resolved before canonicalization)"
  end

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
    "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE, CHARACTER_SET_NAME, COLLATION_NAME " <>
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

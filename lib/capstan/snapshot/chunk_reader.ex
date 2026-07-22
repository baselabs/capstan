defmodule Capstan.Snapshot.ChunkReader do
  @moduledoc """
  The brief-lock exact-`G` capture + PK-range paging + structural fingerprint (C2 Task 5,
  design Ch1 — THE linchpin).

  A `ChunkReader` owns a `Capstan.Query` connection and, per chunk of a snapshot table `T`,
  captures the chunk rows paired with an **exact GTID position `G` that is a provable lower
  bound on the chunk's read view for `T`**. That pairing is the correctness property the whole
  snapshot rests on: a lock-free `@@gtid_executed` read LEADS InnoDB row-visibility under
  concurrent commit (probe-confirmed: 300000/300000 under 3 writers), so a bare read would let
  a stale chunk row overwrite a fresh streamed row — silent corruption. A brief `LOCK TABLES …
  READ` quiesces `T`'s writes while `G` and a `START TRANSACTION WITH CONSISTENT SNAPSHOT` view
  are captured together, making `G` exact.

  ## The pinned capture sequence (design § Pinned decisions #2)

  Per chunk of `T` from `cursor`, EXACTLY:

      (i)   SET SESSION lock_wait_timeout = <bounded>   -- MDL default ~1yr; a bound turns a
                                                        --   contended lock into a HALT, not a wedge
      (ii)  LOCK TABLES `T` READ                        -- waits for in-flight T-writes, blocks new ones
      (iii) G = SELECT @@global.gtid_executed           -- EXACT: every T-txn <= G is committed AND visible
      (iv)  START TRANSACTION WITH CONSISTENT SNAPSHOT   -- pins the read view V; for T, V == state as-of G
      (v)   UNLOCK TABLES                                -- release; the pinned view V survives the unlock
      (vi)  SELECT <projected cols> FROM `T`             -- as-of V, lock-free
              WHERE pk > cursor ORDER BY pk LIMIT chunk_size
      (vii) COMMIT

  Step (iii) uses `@@global.gtid_executed`; `gtid_executed` is a global-scoped variable, so
  `@@global.gtid_executed` and the pinned decision's shorthand `@@gtid_executed` read the same
  value. The projection is EXPLICIT (never `SELECT *`) so the chunk carries the full row image
  and the per-chunk fingerprint is well-defined.

  ## Halts (value-free atoms; design § Preconditions)

  Faults are classified POSITIONALLY, by which phase of the pinned sequence failed — this is
  how the reader honours the Task-4 forward-finding that `Capstan.Query.query/2` scrubs the
  MySQL error CODE (so a lock-wait timeout `1205` and a chunk-read fault both return the same
  value-free atom and cannot be told apart from the return value alone). The *statement that
  failed* is the distinct signal instead:

    * **`:snapshot_lock_unavailable`** — a fault while ACQUIRING the lock: the bounding
      `SET SESSION lock_wait_timeout` or the `LOCK TABLES … READ` itself. A lock-wait timeout
      (bounded by step (i)) and a missing `LOCK TABLES` privilege both surface here — both are
      "could not obtain the lock". Budgeted, then halts.
    * **`:snapshot_chunk_read_failed`** — a fault ONCE THE LOCK IS HELD or the view is being
      read: `SELECT @@global.gtid_executed`, `START TRANSACTION WITH CONSISTENT SNAPSHOT`,
      `UNLOCK TABLES`, the chunk `SELECT`, or `COMMIT`. The chunk `SELECT` is the canonical
      case; the surrounding view statements share its fate (the chunk could not be read
      consistently). Budgeted, then halts.
    * **`:snapshot_schema_drifted`** — the per-chunk structural fingerprint changed from the
      baseline (an in-flight DDL). PERMANENT (a drifted schema does not un-drift on retry), so
      it halts immediately without spending the budget. (The other drift arm — an observed
      `%Capstan.SchemaChange{}` on the table — is the coordinator's, Task 8; the reader owns the
      fingerprint arm.)

  Budgeted faults reuse `Capstan.CheckpointStore.retry_decision/2` + `permanent_reason?/1` — the
  shared counter, never re-derived — so retry semantics cannot drift from C1. The reader retries
  the whole capture on its OWN connection; it does NOT reconnect (a reconnect re-verifies source
  identity, Ch8, and is the coordinator's lifecycle concern) — a dead connection burns the budget
  and halts fail-closed, which the coordinator then propagates.

  ## Rule 1 (design § Rule 1, tripwire 13)

  The `cursor` is a row VALUE embedded as a SQL literal in `WHERE pk > <cursor>`. On a
  `:snapshot_chunk_read_failed` the failing SQL is NEVER logged or returned: `Capstan.Query`
  never logs SQL and returns a value-free atom, and this module maps faults to bare atoms and
  discards every raw fault term — so the cursor appears in no log, error, or crash report. The
  produced `%Capstan.Snapshot.Chunk{}` derives a value-eliding `Inspect` (`rows`/`max_pk` are
  never rendered). No row value or cursor is ever logged or telemetered here.
  """

  alias Capstan.CheckpointStore
  alias Capstan.Query
  alias Capstan.Snapshot.Chunk
  alias Capstan.Snapshot.PrimaryKey

  @default_chunk_size 4096
  @default_lock_wait_timeout 5

  @gtid_sql "SELECT @@global.gtid_executed"
  @start_txn_sql "START TRANSACTION WITH CONSISTENT SNAPSHOT"
  @unlock_sql "UNLOCK TABLES"
  @commit_sql "COMMIT"
  @rollback_sql "ROLLBACK"

  @typedoc "The paging cursor: a canonical PK (the previous chunk's `max_pk`) or `:start`."
  @type cursor :: PrimaryKey.canonical_pk() | :start

  @type t :: %__MODULE__{
          query: Query.t(),
          schema: String.t(),
          table: String.t(),
          pk_columns: [String.t()],
          pk_types: [PrimaryKey.pk_type()],
          pk_indices: [non_neg_integer()],
          projection: [String.t()],
          fingerprint: String.t(),
          chunk_size: pos_integer(),
          lock_wait_timeout: pos_integer(),
          max_retries: non_neg_integer(),
          seq: non_neg_integer()
        }

  defstruct [
    :query,
    :schema,
    :table,
    :pk_columns,
    :pk_types,
    :pk_indices,
    :projection,
    :fingerprint,
    :chunk_size,
    :lock_wait_timeout,
    :max_retries,
    :seq
  ]

  @typedoc """
  The result of `read_chunk/2`:

    * `{:ok, chunk, final?, reader}` — a chunk of 1..chunk_size rows; `final?` is `true` when
      it held fewer than `chunk_size` rows (no further chunk exists). `reader` carries the
      incremented sequence number.
    * `{:done, reader}` — the previous chunk was exactly `chunk_size` rows and this page is
      empty; the table is fully paged.
    * `{:error, reason}` — a value-free halt atom.
  """
  @type read_result ::
          {:ok, Chunk.t(), boolean(), t()}
          | {:done, t()}
          | {:error, atom()}

  ## ---------------------------------------------------------------------------
  ## open/3 — introspect the PK, capture the baseline projection + fingerprint
  ## ---------------------------------------------------------------------------

  @doc """
  Opens a reader for `{schema, table}` over an established `Capstan.Query` handle.

  Introspects the order-faithful PK (via `Capstan.Snapshot.PrimaryKey.introspect/2`) and reads
  the table's ordinal column list to fix the EXPLICIT projection and the baseline structural
  fingerprint. Options:

    * `:chunk_size` — rows per chunk (default `#{@default_chunk_size}`).
    * `:lock_wait_timeout` — the bounded MDL wait, in seconds (default `#{@default_lock_wait_timeout}`).
    * `:max_retries` — the budgeted-fault retry ceiling (default
      `CheckpointStore.default_max_retries/0`).
    * `:seq` — the resume chunk sequence number (default `0`).
    * `:fingerprint` — a baseline fingerprint from durable state; when set, the first chunk's
      fingerprint is compared against IT (so a schema drift across a resume is caught on chunk 1)
      rather than against the freshly-read columns.

  Returns `{:ok, t()}` or a value-free `{:error, reason}` — an introspection halt
  (`:snapshot_table_no_primary_key`, `:snapshot_pk_unsupported_type`) surfaces unchanged; a
  transient fault reading `information_schema` (the PK introspection or the column list) surfaces
  `:snapshot_chunk_read_failed`.
  """
  @spec open(Query.t(), {String.t(), String.t()}, keyword()) :: {:ok, t()} | {:error, atom()}
  def open(%Query{} = query, {schema, table}, opts \\ [])
      when is_binary(schema) and is_binary(table) do
    with {:ok, key} <- introspect_key(query, {schema, table}),
         {:ok, column_rows} <- read_columns_for_open(query, schema, table) do
      projection = projection_of(column_rows)

      {:ok,
       %__MODULE__{
         query: query,
         schema: schema,
         table: table,
         pk_columns: key.pk_columns,
         pk_types: key.pk_types,
         pk_indices: pk_indices(key.pk_columns, projection),
         projection: projection,
         fingerprint: Keyword.get(opts, :fingerprint) || fingerprint_of(column_rows),
         chunk_size: Keyword.get(opts, :chunk_size, @default_chunk_size),
         lock_wait_timeout: Keyword.get(opts, :lock_wait_timeout, @default_lock_wait_timeout),
         max_retries: Keyword.get(opts, :max_retries, CheckpointStore.default_max_retries()),
         seq: Keyword.get(opts, :seq, 0)
       }}
    end
  end

  # A PK-classification halt (permanent, named) passes through unchanged; a transient
  # introspection read fault (a scrubbed `Query` reason) becomes the named `:snapshot_chunk_read_failed`.
  defp introspect_key(query, table) do
    case PrimaryKey.introspect(query, table) do
      {:ok, key} ->
        {:ok, key}

      {:error, reason}
      when reason in [:snapshot_table_no_primary_key, :snapshot_pk_unsupported_type] ->
        {:error, reason}

      {:error, _transient} ->
        {:error, :snapshot_chunk_read_failed}
    end
  end

  defp read_columns_for_open(query, schema, table) do
    case read_columns(query, schema, table) do
      {:ok, column_rows} -> {:ok, column_rows}
      {:error, _reason} -> {:error, :snapshot_chunk_read_failed}
    end
  end

  ## ---------------------------------------------------------------------------
  ## read_chunk/2 — the brief-lock exact-G capture for one chunk from `cursor`
  ## ---------------------------------------------------------------------------

  @doc """
  Reads the next chunk of rows with `pk > cursor` (or from the start when `cursor` is `:start`),
  paired with the chunk's exact GTID position `G`, under the pinned brief-lock capture sequence.

  Returns a `#{inspect(__MODULE__)}.read_result/0`. The produced `%Capstan.Snapshot.Chunk{}`
  carries `g` (the exact lower bound), `rows` (the full row images as `column => value` maps),
  and `max_pk` (the canonical PK of the last row — the value the cursor advances to). Faults are
  classified positionally to `:snapshot_lock_unavailable` / `:snapshot_chunk_read_failed`; a
  fingerprint change halts `:snapshot_schema_drifted`.
  """
  @spec read_chunk(t(), cursor()) :: read_result()
  def read_chunk(%__MODULE__{} = reader, cursor) do
    case attempt_with_budget(reader, cursor, 0) do
      {:ok, %Chunk{rows: []}} ->
        {:done, reader}

      {:ok, %Chunk{} = chunk} ->
        final? = length(chunk.rows) < reader.chunk_size
        {:ok, chunk, final?, %{reader | seq: reader.seq + 1}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  ## ---------------------------------------------------------------------------
  ## budgeted retry over one capture attempt
  ## ---------------------------------------------------------------------------

  # A permanent outcome (schema drift) breaks out immediately; a budgeted phase fault retries
  # the WHOLE capture (a partial capture is cleaned up first) until the shared counter halts.
  defp attempt_with_budget(reader, cursor, attempt) do
    case attempt(reader, cursor) do
      {:ok, chunk} ->
        {:ok, chunk}

      {:permanent, reason} ->
        {:error, reason}

      {:fault, phase} ->
        cleanup(reader)

        case CheckpointStore.retry_decision(attempt, reader.max_retries) do
          :retry -> attempt_with_budget(reader, cursor, attempt + 1)
          :halt -> {:error, halt_reason(phase)}
        end
    end
  end

  # One capture attempt: the per-chunk fingerprint check (permanent drift halt), then the pinned
  # brief-lock sequence. Returns `{:ok, chunk}` | `{:permanent, reason}` | `{:fault, phase}`.
  defp attempt(reader, cursor) do
    with :ok <- check_fingerprint(reader),
         {:ok, g, rows} <- run_capture(reader, cursor) do
      {:ok, build_chunk(reader, g, rows)}
    end
  end

  # Re-read the ordinal column list and compare its fingerprint to the baseline (per-chunk drift
  # guard, tripwire 8). A read fault here is a read-phase fault (budgeted); a CHANGED fingerprint
  # is a permanent drift halt.
  defp check_fingerprint(reader) do
    case read_columns(reader.query, reader.schema, reader.table) do
      {:ok, column_rows} ->
        if fingerprint_of(column_rows) == reader.fingerprint,
          do: :ok,
          else: {:permanent, :snapshot_schema_drifted}

      {:error, _reason} ->
        {:fault, :read}
    end
  end

  # The pinned capture sequence (design § Pinned decisions #2), byte-exact in order. The `with`
  # tags each step with its phase so the `else` maps the fault positionally: the lock-acquisition
  # steps ((i) SET, (ii) LOCK) → `:lock`; the view/read steps ((iii)-(vii)) → `:read`.
  defp run_capture(reader, cursor) do
    q = reader.query

    with {:lock, {:ok, _}} <- {:lock, Query.query(q, set_timeout_sql(reader))},
         {:lock, {:ok, _}} <- {:lock, Query.query(q, lock_sql(reader))},
         {:read, {:ok, [[g]]}} when is_binary(g) <- {:read, Query.query(q, @gtid_sql)},
         {:read, {:ok, _}} <- {:read, Query.query(q, @start_txn_sql)},
         {:read, {:ok, _}} <- {:read, Query.query(q, @unlock_sql)},
         {:read, {:ok, rows}} <- {:read, Query.query(q, chunk_sql(reader, cursor))},
         {:read, {:ok, _}} <- {:read, Query.query(q, @commit_sql)} do
      {:ok, g, rows}
    else
      {:lock, _fault} -> {:fault, :lock}
      {:read, _fault} -> {:fault, :read}
    end
  end

  # Best-effort teardown after a failed attempt: end any open snapshot txn, then release any held
  # table lock. Both are value-free and ignore their result (the socket may already be gone). A
  # subsequent retry re-issues LOCK TABLES, which itself implicitly releases prior locks.
  defp cleanup(reader) do
    _ = Query.query(reader.query, @rollback_sql)
    _ = Query.query(reader.query, @unlock_sql)
    :ok
  end

  defp halt_reason(:lock), do: :snapshot_lock_unavailable
  defp halt_reason(:read), do: :snapshot_chunk_read_failed

  ## ---------------------------------------------------------------------------
  ## chunk construction
  ## ---------------------------------------------------------------------------

  defp build_chunk(reader, g, rows) do
    %Chunk{
      table: {reader.schema, reader.table},
      seq: reader.seq,
      g: g,
      rows: Enum.map(rows, &row_record(reader, &1)),
      max_pk: max_pk(reader, rows)
    }
  end

  # A full row image as a `column_name => value` map (the projection zipped with the row values).
  defp row_record(reader, row), do: Map.new(Enum.zip(reader.projection, row))

  defp max_pk(_reader, []), do: nil

  defp max_pk(reader, rows) do
    last = List.last(rows)
    pk_values = Enum.map(reader.pk_indices, &Enum.at(last, &1))
    PrimaryKey.canonical(reader.pk_types, pk_values)
  end

  ## ---------------------------------------------------------------------------
  ## SQL builders (identifiers backtick-quoted; the cursor is a value literal — Rule 1)
  ## ---------------------------------------------------------------------------

  defp set_timeout_sql(reader),
    do: "SET SESSION lock_wait_timeout = #{reader.lock_wait_timeout}"

  defp lock_sql(reader), do: "LOCK TABLES #{qualified(reader)} READ"

  defp chunk_sql(reader, cursor) do
    "SELECT #{select_list(reader.projection)} FROM #{qualified(reader)}" <>
      where_clause(reader, cursor) <>
      " ORDER BY #{order_list(reader.pk_columns)}" <>
      " LIMIT #{reader.chunk_size}"
  end

  # `:start` reads from the beginning (no lower bound); else a keyset predicate `pk > cursor`.
  # For a composite PK the row-value form `(c1, c2) > (v1, v2)` matches MySQL row-value ORDER BY
  # (and hence `PrimaryKey.compare/2`'s tuple order).
  defp where_clause(_reader, :start), do: ""

  defp where_clause(%{pk_columns: [col], pk_types: [type]}, cursor),
    do: " WHERE #{ident(col)} > #{scalar_literal(type, cursor)}"

  defp where_clause(%{pk_columns: cols, pk_types: types}, cursor) do
    values = Tuple.to_list(cursor)
    lhs = "(" <> Enum.map_join(cols, ", ", &ident/1) <> ")"

    rhs =
      "(" <>
        (types
         |> Enum.zip(values)
         |> Enum.map_join(", ", fn {type, value} -> scalar_literal(type, value) end)) <>
        ")"

    " WHERE #{lhs} > #{rhs}"
  end

  # Integer PKs → a decimal literal; BINARY/VARBINARY → a hex string literal `X'..'` (byte-wise,
  # matching MySQL binary comparison). These are the only allowlisted PK value shapes.
  defp scalar_literal(type, value) when type in [:binary, :varbinary] and is_binary(value),
    do: "X'" <> Base.encode16(value) <> "'"

  defp scalar_literal(_type, value) when is_integer(value), do: Integer.to_string(value)

  defp select_list(columns), do: Enum.map_join(columns, ", ", &ident/1)
  defp order_list(columns), do: Enum.map_join(columns, ", ", &ident/1)

  defp qualified(reader), do: "#{ident(reader.schema)}.#{ident(reader.table)}"

  # A backtick-quoted identifier with embedded backticks doubled (the MySQL escape) — guards a
  # schema/table/column name that would otherwise break the statement.
  defp ident(name), do: "`" <> String.replace(name, "`", "``") <> "`"

  ## ---------------------------------------------------------------------------
  ## column introspection — the explicit projection + the structural fingerprint
  ## ---------------------------------------------------------------------------

  defp read_columns(query, schema, table) do
    Query.query(query, columns_sql(schema, table))
  end

  defp columns_sql(schema, table) do
    "SELECT COLUMN_NAME, COLUMN_TYPE, ORDINAL_POSITION " <>
      "FROM information_schema.COLUMNS " <>
      "WHERE TABLE_SCHEMA = #{literal(schema)} AND TABLE_NAME = #{literal(table)} " <>
      "ORDER BY ORDINAL_POSITION"
  end

  # The ordinal column names — the EXPLICIT projection (never `SELECT *`), so the chunk carries
  # the full row image in a stable order.
  defp projection_of(column_rows) do
    column_rows
    |> Enum.sort_by(fn [_name, _type, ordinal] -> String.to_integer(ordinal) end)
    |> Enum.map(fn [name, _type, _ordinal] -> name end)
  end

  # A stable structural fingerprint over the ordinal `(name, column_type)` pairs. Any add / drop
  # / rename / retype changes it, so a per-chunk re-read that differs from the baseline is a drift.
  defp fingerprint_of(column_rows) do
    material =
      column_rows
      |> Enum.sort_by(fn [_name, _type, ordinal] -> String.to_integer(ordinal) end)
      |> Enum.map_join("\n", fn [name, type, ordinal] -> "#{ordinal}|#{name}|#{type}" end)

    :sha256
    |> :crypto.hash(material)
    |> Base.encode16()
  end

  defp pk_indices(pk_columns, projection) do
    Enum.map(pk_columns, fn col -> Enum.find_index(projection, &(&1 == col)) end)
  end

  # A single-quoted SQL string literal (schema/table are structural identity, not a row value);
  # backslash and single-quote escaped for the default sql_mode. Mirrors `PrimaryKey.literal/1`.
  defp literal(value) do
    escaped = value |> String.replace("\\", "\\\\") |> String.replace("'", "''")
    "'" <> escaped <> "'"
  end
end

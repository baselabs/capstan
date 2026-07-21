defmodule Capstan.Binlog.TableMap do
  @moduledoc """
  A fully-decoded MySQL `TABLE_MAP_EVENT` body.

  Design F12 pins ownership: `Capstan.Binlog.Decoder` parses the **entire**
  `TABLE_MAP` body — including the optional-metadata TLVs — into this struct. Task
  10's registry stores and resolves these; Task 11 casts row values against them.
  Neither re-parses the body, so this struct must carry everything a later
  value-decode needs.

  ## Fields

    * `table_id` — the numeric table identity a row event references. Unstable across
      DDL and reused, so resolution is by this id via the registry, never by "most
      recent map" (design Q3).
    * `schema`, `table` — the qualified table name.
    * `column_types` — the raw MySQL type byte per column, in order (e.g. `3` = LONG,
      `254` = STRING/ENUM/SET). ENUM and SET both arrive as `254` and are told apart
      only via the metadata/TLVs (F3).
    * `column_metadata` — the type-specific metadata blob, stored **RAW**. It is
      length-prefixed in the body and consumed here as a single unit; the per-column
      split needs type-aware widths and belongs to Task 11's `parse_col_meta`, **not**
      here. Splitting it per-column in this module would cross the Task 9/11 boundary.
    * `null_bitmap` — the nullability bitmap over the columns.
    * `signedness` — optional-metadata TLV type 1: a bitmap over the NUMERIC columns,
      `1` = UNSIGNED (F3). Signedness is **not** in the type byte; without this a
      `BIGINT UNSIGNED` of `18446744073709551615` would decode as `-1`. Retained raw
      because interpreting the bitmap requires the numeric-column ordering Task 11 owns.
    * `default_charset` — optional-metadata TLV type 2, retained raw.
    * `column_names` — optional-metadata TLV type 4, in column order (present under
      `binlog_row_metadata=FULL`).
    * `set_str_values` — optional-metadata TLV type 5: the allowed string values per
      `SET` column, in order. C1 must be able to detect `SET` (both it and `ENUM` are
      type `254`) to fail closed later, so these are unpacked here.
    * `enum_str_values` — optional-metadata TLV type 6: the allowed string values per
      `ENUM` column, in order.

  Unknown optional-metadata TLV types (e.g. type 12 `COLUMN_VISIBILITY`, emitted by
  8.0.46) are scanned past by their length and never cause a failure (F3).
  """

  @typedoc "A decoded `TABLE_MAP_EVENT` body."
  @type t :: %__MODULE__{
          table_id: non_neg_integer(),
          schema: String.t(),
          table: String.t(),
          column_types: [byte()],
          column_metadata: binary(),
          null_bitmap: binary(),
          signedness: binary() | nil,
          default_charset: binary() | nil,
          column_names: [String.t()],
          set_str_values: [[String.t()]],
          enum_str_values: [[String.t()]]
        }

  defstruct [
    :table_id,
    :schema,
    :table,
    :column_types,
    :column_metadata,
    :null_bitmap,
    :signedness,
    :default_charset,
    :column_names,
    :set_str_values,
    :enum_str_values
  ]
end

defmodule Capstan.Binlog.Decoder do
  @moduledoc """
  Per-type binlog event body decoder — the second decode layer, over
  `Capstan.Binlog.Event`.

  `Capstan.Binlog.Event.parse/1` has already parsed the 19-byte header and verified
  and stripped the CRC32 trailer. This module dispatches on the event **type byte**
  and decodes `event.body` into a per-type term. It does **not** re-parse the header
  or re-verify the CRC.

  ## Return contract

  `decode/1` returns one of three shapes:

    * `{:ok, decoded}` — a recognised event, decoded to its per-type term.
    * `{:halt, :unsupported_transaction_shape}` — an `XA_PREPARE_LOG_EVENT` (type 38).
      This is a **fail-closed control signal**, distinct from both success and a
      decode error, so a consumer that omits a `{:halt, _}` clause crashes loudly
      rather than silently treating XA rows as a normal event (design Q13, F9).
    * `{:error, reason}` — an event C1 refuses: a compressed transaction payload, or
      an unknown type byte. An unknown type **fails closed** and is never silently
      skipped — a dropped event of an unrecognised shape could hide a condition
      capstan must halt on.

  ## Two silent-failure safety branches (the reason this module is high-Risk)

    * **`ROWS_QUERY_LOG_EVENT` (29)** carries the complete original SQL of the row
      change with every literal value — a total Rule-1 leak if surfaced. The header
      is recognised and the body is **discarded**: `decode/1` returns
      `{:rows_query, :discarded}` and the SQL text never enters the returned term
      (design Q16).
    * **`XA_PREPARE_LOG_EVENT` (38)** halts fail-closed. Its row images ride the
      *prepare* event and are committed later by a separate `XID` — or discarded by
      `XA ROLLBACK` — so treating them as committed delivers rolled-back rows
      effect-once. The halt is keyed purely on the type byte (design Q13).

  ## Rule-1 note on QUERY text

  For `QUERY` (2) the SQL text **is** returned — Task 12 inspects it to detect
  `BEGIN`/`COMMIT` and self-committing DDL terminators. That raw text is
  Rule-1-sensitive (DDL routinely embeds literals like `DEFAULT 'secret'`) and must
  never be logged. DDL redaction happens later, at the `%Capstan.SchemaChange{}`
  boundary (design Q15), not in this module.
  """

  alias Capstan.Binlog.{Event, TableMap}
  alias Capstan.Protocol.Packet

  # Event type bytes — ported from probe/mysql_binlog_probe.exs:161, plus the C1
  # additions (STOP, ROWS_QUERY, XA_PREPARE, TRANSACTION_PAYLOAD). `anonymous_gtid`
  # (34) is deliberately absent: C1 requires gtid_mode=ON, under which the server
  # never emits it, so it correctly falls through to the fail-closed unknown path.
  @stop 3
  @query 2
  @rotate 4
  @format_description 15
  @xid 16
  @table_map 19
  @heartbeat 27
  @rows_query 29
  @write_rows 30
  @update_rows 31
  @delete_rows 32
  @gtid 33
  @previous_gtids 35
  @xa_prepare 38
  @transaction_payload 40

  # Optional-metadata TLV type codes retained per F3. All other TLV types are
  # tolerated (scanned past by their length), never failed on.
  @tlv_signedness 1
  @tlv_default_charset 2
  @tlv_column_name 4
  @tlv_set_str_value 5
  @tlv_enum_str_value 6

  @typedoc "A single GTID: `{server_uuid, transaction_sequence_number}`."
  @type gtid :: {String.t(), pos_integer()}

  @typedoc "The structural decode of a row event body: identity, present bitmap(s), raw image bytes."
  @type rows ::
          {:write_rows, non_neg_integer(), binary(), binary()}
          | {:delete_rows, non_neg_integer(), binary(), binary()}
          | {:update_rows, non_neg_integer(), binary(), binary(), binary()}

  @typedoc "A successfully decoded event body."
  @type decoded ::
          {:rotate, String.t(), non_neg_integer()}
          | {:format_description, non_neg_integer(), String.t()}
          | :previous_gtids
          | {:gtid, gtid()}
          | {:query, %{schema: String.t(), sql: String.t()}}
          | TableMap.t()
          | rows()
          | {:xid, non_neg_integer()}
          | :stop
          | :heartbeat
          | {:rows_query, :discarded}

  @typedoc "A reason C1 refuses an event outright."
  @type error_reason :: :compressed_payload_unsupported | {:unknown_event_type, byte()}

  @doc """
  Decodes one `%Capstan.Binlog.Event{}` body to its per-type term.

  See the module doc for the `{:ok, _}` / `{:halt, _}` / `{:error, _}` contract and
  the two safety branches (ROWS_QUERY discard, XA_PREPARE halt).
  """
  @spec decode(Event.t()) ::
          {:ok, decoded()}
          | {:halt, :unsupported_transaction_shape}
          | {:error, error_reason()}
  def decode(%Event{type: type, body: body}), do: do_decode(type, body)

  defp do_decode(@rotate, body), do: {:ok, decode_rotate(body)}
  defp do_decode(@format_description, body), do: {:ok, decode_format_description(body)}
  defp do_decode(@previous_gtids, _body), do: {:ok, :previous_gtids}
  defp do_decode(@gtid, body), do: {:ok, decode_gtid(body)}
  defp do_decode(@query, body), do: {:ok, decode_query(body)}
  defp do_decode(@table_map, body), do: {:ok, decode_table_map(body)}
  defp do_decode(@write_rows, body), do: {:ok, decode_rows(:write_rows, body)}
  defp do_decode(@update_rows, body), do: {:ok, decode_rows(:update_rows, body)}
  defp do_decode(@delete_rows, body), do: {:ok, decode_rows(:delete_rows, body)}
  defp do_decode(@xid, body), do: {:ok, decode_xid(body)}
  defp do_decode(@stop, _body), do: {:ok, :stop}
  defp do_decode(@heartbeat, _body), do: {:ok, :heartbeat}

  # Q16 SAFETY: recognise the header, DISCARD the body. Returning `event.body` here
  # would leak the complete original SQL — with every literal — a total Rule-1 breach.
  defp do_decode(@rows_query, _body), do: {:ok, {:rows_query, :discarded}}

  # Q13/F9 SAFETY: XA prepare rows may later be rolled back. Halt fail-closed on the
  # type byte alone; the body is never inspected.
  defp do_decode(@xa_prepare, _body), do: {:halt, :unsupported_transaction_shape}

  defp do_decode(@transaction_payload, _body), do: {:error, :compressed_payload_unsupported}

  # Fail closed: an unrecognised event type is refused, never silently skipped.
  defp do_decode(type, _body), do: {:error, {:unknown_event_type, type}}

  ## per-type body decoders

  defp decode_rotate(<<position::64-little, next_name::binary>>) do
    {:rotate, next_name, position}
  end

  defp decode_format_description(
         <<binlog_version::16-little, server_version::binary-size(50), _rest::binary>>
       ) do
    version = server_version |> :binary.split(<<0>>) |> hd()
    {:format_description, binlog_version, version}
  end

  defp decode_gtid(<<_flags::8, sid::binary-size(16), gno::64-little-signed, _rest::binary>>) do
    {:gtid, {format_uuid(sid), gno}}
  end

  defp decode_query(
         <<_thread_id::32-little, _exec_time::32-little, schema_len::8, _error_code::16-little,
           status_vars_len::16-little, rest::binary>>
       ) do
    <<_status_vars::binary-size(status_vars_len), schema::binary-size(schema_len), 0,
      sql::binary>> =
      rest

    {:query, %{schema: schema, sql: sql}}
  end

  defp decode_xid(<<xid::64-little>>), do: {:xid, xid}

  # ROWS_v2 body: table_id(6) flags(2) extra_len(2) extra(extra_len-2) ncols(lenenc)
  # then the present bitmap(s) then the raw row-image bytes. WRITE/DELETE carry one
  # present bitmap; UPDATE carries a before- and an after-image bitmap. Row VALUES are
  # NOT decoded here — Task 11 casts the raw bytes against the %TableMap{}.
  defp decode_rows(
         op,
         <<table_id::48-little, _flags::16-little, extra_len::16-little, rest::binary>>
       ) do
    extra_size = extra_len - 2
    <<_extra::binary-size(extra_size), rest::binary>> = rest
    {ncols, rest} = Packet.lenenc_int(rest)
    present_bytes = div(ncols + 7, 8)
    split_row_images(op, table_id, present_bytes, rest)
  end

  defp split_row_images(:update_rows, table_id, present_bytes, rest) do
    <<before_cols::binary-size(present_bytes), after_cols::binary-size(present_bytes),
      rows::binary>> =
      rest

    {:update_rows, table_id, before_cols, after_cols, rows}
  end

  defp split_row_images(op, table_id, present_bytes, rest) do
    <<present::binary-size(present_bytes), rows::binary>> = rest
    {op, table_id, present, rows}
  end

  ## TABLE_MAP body (owns the WHOLE body incl. optional-metadata TLVs — F12)

  defp decode_table_map(body) do
    <<table_id::48-little, _flags::16-little, rest::binary>> = body
    {schema, rest} = read_str8_z(rest)
    {table, rest} = read_str8_z(rest)
    {ncols, rest} = Packet.lenenc_int(rest)
    <<column_types::binary-size(ncols), rest::binary>> = rest
    {metadata_len, rest} = Packet.lenenc_int(rest)
    # RAW blob consumed as a single unit — NOT split per-column (that is Task 11).
    <<column_metadata::binary-size(metadata_len), rest::binary>> = rest
    null_bytes = div(ncols + 7, 8)
    <<null_bitmap::binary-size(null_bytes), optional_metadata::binary>> = rest

    tlvs = scan_tlv(optional_metadata, %{})

    %TableMap{
      table_id: table_id,
      schema: schema,
      table: table,
      column_types: :erlang.binary_to_list(column_types),
      column_metadata: column_metadata,
      null_bitmap: null_bitmap,
      signedness: Map.get(tlvs, :signedness),
      default_charset: Map.get(tlvs, :default_charset),
      column_names: Map.get(tlvs, :column_names, []),
      set_str_values: Map.get(tlvs, :set_str_values, []),
      enum_str_values: Map.get(tlvs, :enum_str_values, [])
    }
  end

  # A length-prefixed (1 byte) string followed by a NUL terminator, per the
  # TABLE_MAP schema/table encoding.
  defp read_str8_z(<<len::8, string::binary-size(len), 0, rest::binary>>), do: {string, rest}

  # Optional-metadata TLV scan. Each entry is `type(1) length(lenenc) value(length)`.
  # Retain the F3 fields; tolerate every other type by skipping its value.
  defp scan_tlv(<<>>, acc), do: acc

  defp scan_tlv(<<type::8, rest::binary>>, acc) do
    {len, rest} = Packet.lenenc_int(rest)
    <<value::binary-size(len), rest2::binary>> = rest
    scan_tlv(rest2, store_tlv(type, value, acc))
  end

  defp store_tlv(@tlv_signedness, value, acc), do: Map.put(acc, :signedness, value)
  defp store_tlv(@tlv_default_charset, value, acc), do: Map.put(acc, :default_charset, value)

  defp store_tlv(@tlv_column_name, value, acc),
    do: Map.put(acc, :column_names, read_str_list(value))

  defp store_tlv(@tlv_set_str_value, value, acc),
    do: Map.put(acc, :set_str_values, read_nested_str_list(value))

  defp store_tlv(@tlv_enum_str_value, value, acc),
    do: Map.put(acc, :enum_str_values, read_nested_str_list(value))

  defp store_tlv(_other, _value, acc), do: acc

  # A flat sequence of length-encoded strings (TLV type 4, COLUMN_NAME).
  defp read_str_list(<<>>), do: []

  defp read_str_list(bin) do
    {string, rest} = Packet.lenenc_str(bin)
    [string | read_str_list(rest)]
  end

  # Per column: a lenenc count then that many length-encoded strings (TLV types 5/6,
  # SET_STR_VALUE / ENUM_STR_VALUE) — one inner list per SET/ENUM column, in order.
  defp read_nested_str_list(<<>>), do: []

  defp read_nested_str_list(bin) do
    {count, rest} = Packet.lenenc_int(bin)
    {values, rest} = take_strings(count, rest, [])
    [values | read_nested_str_list(rest)]
  end

  defp take_strings(0, bin, acc), do: {Enum.reverse(acc), bin}

  defp take_strings(count, bin, acc) do
    {string, rest} = Packet.lenenc_str(bin)
    take_strings(count - 1, rest, [string | acc])
  end

  defp format_uuid(
         <<a::binary-size(4), b::binary-size(2), c::binary-size(2), d::binary-size(2),
           e::binary-size(6)>>
       ) do
    Enum.map_join([a, b, c, d, e], "-", &Base.encode16(&1, case: :lower))
  end
end

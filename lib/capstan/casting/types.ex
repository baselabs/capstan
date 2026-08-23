defmodule Capstan.Casting.Types do
  @moduledoc """
  Casts one column's raw row-image bytes to an Elixir term, and owns the per-column
  metadata-width split of a `TABLE_MAP`'s raw `column_metadata` blob.

  This is the silent-wrong-value surface of C1, so the whole module is built around
  *never emitting a plausible-but-wrong value*. Three failure modes get first-class
  defence:

    * **Signedness.** Integer width comes from the wire type byte, but *signedness
      does not* — it rides `TABLE_MAP` optional-metadata TLV type 1. `cast/4` takes the
      resolved signedness bit as an explicit argument; a `BIGINT UNSIGNED` of
      `18446744073709551615` must never decode to `-1`.
    * **`SET` masquerading as `ENUM`.** Both are wire type `254` (`MYSQL_TYPE_STRING`)
      and are distinguishable only by the STRING metadata pair (byte 0 real-type:
      `247` = `ENUM`, `248` = `SET`). `SET`'s row image is packed as a bitfield, not an
      index, so an enum-style decode would be silently wrong. C1 defers `SET`
      (roadmap row C4): `parse_col_meta/2` unpacks the pair to *detect* it and `cast/4`
      **fails closed** with `:unsupported_column_type`.
    * **Meta-driven widths.** Fractional-second width, decimal precision/scale, blob
      length-prefix width and string length all come from the metadata, not the type
      byte. A fixed-width assumption both mis-values the column *and* desynchronises
      every column after it. `parse_col_meta/2` enumerates every supported type's meta
      width **explicitly** and fails closed on the rest, rather than guessing with a
      1-byte fallback (the bug in `probe/mysql_binlog_probe.exs:301-311`).

  ## Fail-closed contract

  Both entry points return `{:error, {:unsupported_column_type, detail}}` rather than a
  value whenever the type, its metadata, or a decoded temporal component is one C1 does
  not faithfully support. The owner (`Capstan.Assembler`) turns that into a stream halt, exactly as
  it does for the registry's `{:error, :unmapped_table_id}` — a column C1 cannot decode
  is never silently dropped or passed through raw. `detail` carries only schema-level
  facts (wire type byte, column position); never a column value (Rule 1).
  """

  import Bitwise

  # ---- MySQL wire type bytes (subset C1 supports; others fail closed) -------------
  @type_tiny 1
  @type_short 2
  @type_long 3
  @type_longlong 8
  @type_int24 9
  @type_date 10
  @type_timestamp2 17
  @type_datetime2 18
  @type_time2 19
  @type_varchar 15
  @type_json 245
  @type_newdecimal 246
  @type_blob 252
  @type_var_string 253
  @type_string 254

  # STRING (254) real-type discriminators, read from the metadata pair.
  @real_enum 247
  @real_set 248

  @typedoc """
  A single column's parsed metadata — self-describing enough for `cast/4` to decode the
  value, produced by `parse_col_meta/2`.
  """
  @type col_meta ::
          {:int, 1 | 2 | 3 | 4 | 8}
          | {:decimal, non_neg_integer(), non_neg_integer()}
          | {:varstring, non_neg_integer()}
          | {:char, non_neg_integer()}
          | {:enum, 1 | 2}
          | {:set, 1 | 2}
          | {:blob, 1..4}
          | {:datetime, 0..6}
          | {:timestamp, 0..6}
          | {:time, 0..6}
          | :date
          | {:json, 1..4}

  @typedoc "The fail-closed signal: a type/meta/value C1 refuses rather than mis-decode."
  @type unsupported :: {:unsupported_column_type, map()}

  # dig2bytes[n] = bytes needed to hold n leftover decimal digits (MySQL decimal.c).
  @dig2bytes {0, 1, 1, 2, 2, 3, 3, 4, 4, 4}
  @digits_per_group 9

  @doc """
  Splits a `TABLE_MAP`'s raw `column_metadata` blob into one `t:col_meta/0` per column.

  `column_types` is the wire type byte per column (in order); `column_metadata` is the
  raw, unsplit blob the decoder stored verbatim. Each type consumes a *type-specific*
  number of metadata bytes — enumerated explicitly here. The first column whose type C1
  does not support **fails closed**: an unknown meta width cannot be skipped without
  desynchronising every later column, so the whole table is refused.

  Returns `{:ok, [col_meta]}` aligned with `column_types`, or
  `{:error, {:unsupported_column_type, detail}}`.
  """
  @spec parse_col_meta([byte()], binary()) :: {:ok, [col_meta()]} | {:error, unsupported()}
  def parse_col_meta(column_types, column_metadata)
      when is_list(column_types) and is_binary(column_metadata) do
    parse_col_meta(column_types, column_metadata, 0, [])
  end

  defp parse_col_meta([], _blob, _index, acc), do: {:ok, Enum.reverse(acc)}

  defp parse_col_meta([type | rest], blob, index, acc) do
    case take_meta(type, blob) do
      {:ok, meta, blob_rest} -> parse_col_meta(rest, blob_rest, index + 1, [meta | acc])
      {:error, reason} -> {:error, {:unsupported_column_type, meta_detail(type, index, reason)}}
    end
  end

  # 0-metadata-byte integer types. Width is implied by the type byte.
  defp take_meta(@type_tiny, blob), do: {:ok, {:int, 1}, blob}
  defp take_meta(@type_short, blob), do: {:ok, {:int, 2}, blob}
  defp take_meta(@type_int24, blob), do: {:ok, {:int, 3}, blob}
  defp take_meta(@type_long, blob), do: {:ok, {:int, 4}, blob}
  defp take_meta(@type_longlong, blob), do: {:ok, {:int, 8}, blob}
  defp take_meta(@type_date, blob), do: {:ok, :date, blob}

  # NEWDECIMAL: 2 bytes = precision, scale.
  defp take_meta(@type_newdecimal, <<precision, scale, rest::binary>>),
    do: {:ok, {:decimal, precision, scale}, rest}

  # VARCHAR / VAR_STRING: 2-byte little-endian max byte length.
  defp take_meta(@type_varchar, <<max_len::16-little, rest::binary>>),
    do: {:ok, {:varstring, max_len}, rest}

  defp take_meta(@type_var_string, <<max_len::16-little, rest::binary>>),
    do: {:ok, {:varstring, max_len}, rest}

  # STRING (254): 2-byte pair that also carries the real type. CHAR vs ENUM vs SET.
  defp take_meta(@type_string, <<byte0, byte1, rest::binary>>),
    do: {:ok, string_meta(byte0, byte1), rest}

  # BLOB / TEXT: 1 byte = number of length-prefix bytes (1..4).
  defp take_meta(@type_blob, <<length_bytes, rest::binary>>) when length_bytes in 1..4,
    do: {:ok, {:blob, length_bytes}, rest}

  # JSON: 1 byte = number of length-prefix bytes (MySQL emits 4).
  defp take_meta(@type_json, <<length_bytes, rest::binary>>) when length_bytes in 1..4,
    do: {:ok, {:json, length_bytes}, rest}

  # DATETIME2 / TIMESTAMP2 / TIME2: 1 byte fractional-seconds precision (fsp, 0..6).
  defp take_meta(@type_datetime2, <<fsp, rest::binary>>) when fsp in 0..6,
    do: {:ok, {:datetime, fsp}, rest}

  defp take_meta(@type_timestamp2, <<fsp, rest::binary>>) when fsp in 0..6,
    do: {:ok, {:timestamp, fsp}, rest}

  defp take_meta(@type_time2, <<fsp, rest::binary>>) when fsp in 0..6,
    do: {:ok, {:time, fsp}, rest}

  # Every other type byte (FLOAT, DOUBLE, BIT, YEAR, GEOMETRY, old DECIMAL, ...) is
  # refused: its meta width is not enumerated, so decoding cannot continue safely.
  defp take_meta(_type, _blob), do: {:error, :unknown_wire_type}

  # STRING metadata decode (MySQL log_event.cc). For a long CHAR the high length bits
  # are packed into byte0; ENUM/SET/short-CHAR fall in the simple branch where byte0 is
  # the real type directly. `length` is the packed byte length / index width.
  defp string_meta(byte0, byte1) do
    if (byte0 &&& 0x30) != 0x30 do
      length = byte1 ||| bxor(byte0 &&& 0x30, 0x30) <<< 4
      classify_string(byte0 ||| 0x30, length)
    else
      classify_string(byte0, byte1)
    end
  end

  defp classify_string(@real_enum, pack_len), do: {:enum, pack_len}
  defp classify_string(@real_set, pack_len), do: {:set, pack_len}
  defp classify_string(_char, length), do: {:char, length}

  @doc """
  Casts one column's leading bytes of a row image to an Elixir term.

  Arguments:

    * `col_meta` — the column's `t:col_meta/0` from `parse_col_meta/2`.
    * `signed?` — the resolved signedness bit (`true` = signed). Consumed only by
      integer casts; supplied by the caller from TLV type 1.
    * `str_values` — the allowed member strings for an `ENUM` column, `[]` otherwise.
    * `bytes` — the remaining row-image bytes, positioned at this column's value.

  Returns `{:ok, value, rest}` with the bytes this column consumed removed, or
  `{:error, {:unsupported_column_type, detail}}` (`SET`, JSON opaque, or an
  out-of-range temporal/enum) — never a wrong value and never a raw passthrough.
  """
  @spec cast(col_meta(), boolean(), [String.t()], binary()) ::
          {:ok, term(), binary()} | {:error, unsupported()}
  def cast(col_meta, signed?, str_values, bytes)

  def cast({:int, width}, signed?, _str, bytes), do: cast_int(width, signed?, bytes)

  def cast({:decimal, precision, scale}, _signed?, _str, bytes),
    do: cast_decimal(precision, scale, bytes)

  def cast({:varstring, max_len}, _signed?, _str, bytes),
    do: cast_var_length(prefix_bytes(max_len), bytes)

  def cast({:char, length}, _signed?, _str, bytes),
    do: cast_var_length(prefix_bytes(length), bytes)

  def cast({:blob, length_bytes}, _signed?, _str, bytes),
    do: cast_var_length(length_bytes, bytes)

  def cast({:enum, pack_len}, _signed?, str_values, bytes),
    do: cast_enum(pack_len, str_values, bytes)

  def cast({:set, pack_len}, _signed?, _str, _bytes),
    do: {:error, {:unsupported_column_type, %{reason: :set_deferred, pack_len: pack_len}}}

  def cast(:date, _signed?, _str, <<raw::24-little, rest::binary>>), do: cast_date(raw, rest)

  def cast({:datetime, fsp}, _signed?, _str, bytes), do: cast_datetime(fsp, bytes)
  def cast({:timestamp, fsp}, _signed?, _str, bytes), do: cast_timestamp(fsp, bytes)
  def cast({:time, fsp}, _signed?, _str, bytes), do: cast_time(fsp, bytes)

  def cast({:json, length_bytes}, _signed?, _str, bytes),
    do: cast_json(length_bytes, bytes)

  # ---- integers -------------------------------------------------------------------

  defp cast_int(width, true, bytes) do
    bits = width * 8
    <<value::size(^bits)-little-signed, rest::binary>> = bytes
    {:ok, value, rest}
  end

  defp cast_int(width, false, bytes) do
    bits = width * 8
    <<value::size(^bits)-little-unsigned, rest::binary>> = bytes
    {:ok, value, rest}
  end

  # ---- variable-length strings / blobs --------------------------------------------

  # `prefix_size` = 1 or 2 (VARCHAR/CHAR) or 1..4 (BLOB) length-prefix bytes, LE.
  defp cast_var_length(prefix_size, bytes) do
    prefix_bits = prefix_size * 8
    <<length::size(^prefix_bits)-little, value::binary-size(length), rest::binary>> = bytes
    {:ok, value, rest}
  end

  # VARCHAR/CHAR use a 1-byte length prefix when the max byte length fits in a byte,
  # a 2-byte prefix otherwise.
  defp prefix_bytes(max_len) when max_len < 256, do: 1
  defp prefix_bytes(_max_len), do: 2

  # ---- ENUM -----------------------------------------------------------------------

  defp cast_enum(pack_len, str_values, bytes) do
    bits = pack_len * 8
    <<index::size(^bits)-little, rest::binary>> = bytes
    resolve_enum(index, str_values, rest, pack_len)
  end

  # Index 0 is MySQL's empty/invalid member (''); 1..n select the nth member.
  defp resolve_enum(0, _str_values, rest, _pack_len), do: {:ok, "", rest}

  defp resolve_enum(index, str_values, rest, pack_len) do
    case Enum.at(str_values, index - 1) do
      nil ->
        {:error,
         {:unsupported_column_type, %{reason: :enum_index_out_of_range, pack_len: pack_len}}}

      member ->
        {:ok, member, rest}
    end
  end

  # ---- DECIMAL / NEWDECIMAL (packed BCD, MySQL decimal.c bin2decimal) --------------

  defp cast_decimal(precision, scale, bytes) do
    intg = precision - scale
    uncomp_intg = div(intg, @digits_per_group)
    uncomp_frac = div(scale, @digits_per_group)
    comp_intg = intg - uncomp_intg * @digits_per_group
    comp_frac = scale - uncomp_frac * @digits_per_group

    bin_size =
      uncomp_intg * 4 + elem(@dig2bytes, comp_intg) +
        uncomp_frac * 4 + elem(@dig2bytes, comp_frac)

    <<buf::binary-size(^bin_size), rest::binary>> = bytes
    <<first, tail::binary>> = buf
    # Sign bit set => positive (mask 0); clear => negative (mask 0xFF, one's complement).
    {sign, mask} = if (first &&& 0x80) != 0, do: {1, 0}, else: {-1, 0xFF}
    digits = :binary.bin_to_list(<<bxor(first, 0x80), tail::binary>>)

    {coefficient, digits} = read_group(digits, elem(@dig2bytes, comp_intg), comp_intg, mask, 0)
    {coefficient, digits} = read_full_groups(digits, uncomp_intg, mask, coefficient)
    {coefficient, digits} = read_full_groups(digits, uncomp_frac, mask, coefficient)

    {coefficient, _digits} =
      read_group(digits, elem(@dig2bytes, comp_frac), comp_frac, mask, coefficient)

    {:ok, Decimal.new(sign, coefficient, -scale), rest}
  end

  defp read_full_groups(digits, 0, _mask, coefficient), do: {coefficient, digits}

  defp read_full_groups(digits, count, mask, coefficient) do
    {coefficient, digits} = read_group(digits, 4, @digits_per_group, mask, coefficient)
    read_full_groups(digits, count - 1, mask, coefficient)
  end

  # Reads `nbytes` big-endian (XORed with `mask`) as `digit_count` decimal digits and
  # folds them into the running base-10 coefficient.
  defp read_group(digits, 0, _digit_count, _mask, coefficient), do: {coefficient, digits}

  defp read_group(digits, nbytes, digit_count, mask, coefficient) do
    {group_bytes, rest} = Enum.split(digits, nbytes)
    value = Enum.reduce(group_bytes, 0, fn byte, acc -> acc * 256 + bxor(byte, mask) end)
    {coefficient * pow10(digit_count) + value, rest}
  end

  defp pow10(0), do: 1
  defp pow10(n), do: 10 * pow10(n - 1)

  # ---- temporals (meta-driven fractional precision) -------------------------------

  # DATE: 3-byte little-endian bit pack `day | month<<5 | year<<9`.
  defp cast_date(raw, rest) do
    day = raw &&& 0x1F
    month = raw >>> 5 &&& 0x0F
    year = raw >>> 9

    case Date.new(year, month, day) do
      {:ok, date} -> {:ok, date, rest}
      {:error, reason} -> {:error, {:unsupported_column_type, %{reason: {:invalid_date, reason}}}}
    end
  end

  # DATETIME2: 5-byte big-endian integer part (biased) + fsp-driven fractional bytes.
  defp cast_datetime(fsp, bytes) do
    <<int_part::40-big, rest::binary>> = bytes
    {microsecond, rest} = read_fractional(fsp, rest)
    ymdhms = int_part - 0x8000_00_00_00
    hms = ymdhms &&& 0x1FFFF
    ymd = ymdhms >>> 17
    ym = ymd >>> 5

    case NaiveDateTime.new(
           div(ym, 13),
           rem(ym, 13),
           ymd &&& 0x1F,
           hms >>> 12,
           hms >>> 6 &&& 0x3F,
           hms &&& 0x3F,
           {microsecond, fsp}
         ) do
      {:ok, ndt} ->
        {:ok, ndt, rest}

      {:error, reason} ->
        {:error, {:unsupported_column_type, %{reason: {:invalid_datetime, reason}}}}
    end
  end

  # TIMESTAMP2: 4-byte big-endian Unix seconds (UTC) + fsp-driven fractional bytes.
  defp cast_timestamp(fsp, bytes) do
    <<seconds::32-big, rest::binary>> = bytes
    {microsecond, rest} = read_fractional(fsp, rest)

    case DateTime.from_unix(seconds * 1_000_000 + microsecond, :microsecond) do
      {:ok, dt} ->
        {:ok, %{dt | microsecond: {microsecond, fsp}}, rest}

      {:error, reason} ->
        {:error, {:unsupported_column_type, %{reason: {:invalid_timestamp, reason}}}}
    end
  end

  # TIME2: 3-byte big-endian integer part for fsp 0..4, whole 6-byte read for fsp 5/6.
  defp cast_time(fsp, bytes) when fsp <= 4 do
    <<int_part::24-big, rest::binary>> = bytes
    {frac, rest} = read_fractional(fsp, rest)
    packed = (int_part - 0x80_00_00) <<< 24 ||| frac
    build_time(packed, frac, fsp, rest)
  end

  defp cast_time(fsp, bytes) do
    <<packed_biased::48-big, rest::binary>> = bytes
    packed = packed_biased - 0x8000_0000_0000
    build_time(packed, packed &&& 0xFFFFFF, fsp, rest)
  end

  defp build_time(packed, microsecond, fsp, rest) do
    hms = packed >>> 24

    case Time.new(hms >>> 12, hms >>> 6 &&& 0x3F, hms &&& 0x3F, {microsecond, fsp}) do
      {:ok, time} -> {:ok, time, rest}
      {:error, reason} -> {:error, {:unsupported_column_type, %{reason: {:invalid_time, reason}}}}
    end
  end

  # Fractional-second bytes per fsp: 0 → none, 1/2 → 1 byte (×10000), 3/4 → 2 bytes
  # (×100), 5/6 → 3 bytes (×1). Returns microseconds and the remaining bytes.
  defp read_fractional(0, bytes), do: {0, bytes}

  defp read_fractional(fsp, <<centi, rest::binary>>) when fsp in 1..2,
    do: {centi * 10_000, rest}

  defp read_fractional(fsp, <<value::16-big, rest::binary>>) when fsp in 3..4,
    do: {value * 100, rest}

  defp read_fractional(fsp, <<value::24-big, rest::binary>>) when fsp in 5..6,
    do: {value, rest}

  # ---- JSON (MySQL internal binary format, json_binary.cc) ------------------------

  defp cast_json(length_bytes, bytes) do
    bits = length_bytes * 8
    <<length::size(^bits)-little, doc::binary-size(length), rest::binary>> = bytes

    try do
      {:ok, json_doc(doc), rest}
    catch
      {:json_unsupported, detail} -> {:error, {:unsupported_column_type, detail}}
    end
  end

  # Top level: a 1-byte type tag followed by the value's own binary.
  defp json_doc(<<type, payload::binary>>), do: json_value(type, payload)
  defp json_doc(<<>>), do: nil

  defp json_value(0x00, data), do: json_object(data, 2)
  defp json_value(0x01, data), do: json_object(data, 4)
  defp json_value(0x02, data), do: json_array(data, 2)
  defp json_value(0x03, data), do: json_array(data, 4)
  defp json_value(0x04, <<literal, _::binary>>), do: json_literal(literal)
  defp json_value(0x05, <<v::16-little-signed, _::binary>>), do: v
  defp json_value(0x06, <<v::16-little-unsigned, _::binary>>), do: v
  defp json_value(0x07, <<v::32-little-signed, _::binary>>), do: v
  defp json_value(0x08, <<v::32-little-unsigned, _::binary>>), do: v
  defp json_value(0x09, <<v::64-little-signed, _::binary>>), do: v
  defp json_value(0x0A, <<v::64-little-unsigned, _::binary>>), do: v
  defp json_value(0x0B, <<v::float-little-size(64), _::binary>>), do: v
  defp json_value(0x0C, data), do: json_string(data)
  # Opaque (0x0F) embeds a typed MySQL value (DECIMAL, temporal, ...) whose mis-decode
  # would be silently wrong. C1 defers it and fails closed.
  defp json_value(type, _data), do: throw({:json_unsupported, %{reason: {:json_type, type}}})

  defp json_literal(0), do: nil
  defp json_literal(1), do: true
  defp json_literal(2), do: false

  # Object binary: element-count, total-size, key-entries, value-entries, keys, values.
  # `offset_size` is 2 (small) or 4 (large); all offsets are relative to `data`'s start.
  defp json_object(data, offset_size) do
    <<count::little-size(^offset_size * 8), _size::little-size(^offset_size * 8), body::binary>> =
      data

    {key_entries, body} = json_take(count, body, &json_key_entry(&1, offset_size))
    {value_entries, _body} = json_take(count, body, &json_value_entry(&1, offset_size))

    keys = Enum.map(key_entries, fn {offset, length} -> binary_part(data, offset, length) end)
    values = Enum.map(value_entries, &json_resolve(&1, data, offset_size))
    Map.new(Enum.zip(keys, values))
  end

  defp json_array(data, offset_size) do
    <<count::little-size(^offset_size * 8), _size::little-size(^offset_size * 8), body::binary>> =
      data

    {value_entries, _body} = json_take(count, body, &json_value_entry(&1, offset_size))
    Enum.map(value_entries, &json_resolve(&1, data, offset_size))
  end

  defp json_key_entry(bin, offset_size) do
    <<offset::little-size(^offset_size * 8), length::16-little, rest::binary>> = bin
    {{offset, length}, rest}
  end

  defp json_value_entry(bin, offset_size) do
    <<type, field::binary-size(^offset_size), rest::binary>> = bin
    {{type, field}, rest}
  end

  # A value entry either inlines its value in the offset field or points at it. Inline
  # types: literal/int16/uint16 (both sizes) plus int32/uint32 for large containers.
  defp json_resolve({type, field}, _data, _offset_size) when type in [0x04, 0x05, 0x06],
    do: json_value(type, field)

  defp json_resolve({type, <<_::32>> = field}, _data, 4) when type in [0x07, 0x08],
    do: json_value(type, field)

  defp json_resolve({type, field}, data, offset_size) do
    <<offset::little-size(^offset_size * 8)>> = field
    json_value(type, binary_part(data, offset, byte_size(data) - offset))
  end

  # JSON string: a var-int (7 bits/byte, high bit = continuation) length then the bytes.
  defp json_string(data) do
    {length, rest} = json_varint(data, 0, 0)
    binary_part(rest, 0, length)
  end

  defp json_varint(<<byte, rest::binary>>, shift, acc) do
    acc = acc ||| (byte &&& 0x7F) <<< shift
    if (byte &&& 0x80) != 0, do: json_varint(rest, shift + 7, acc), else: {acc, rest}
  end

  defp json_take(0, bin, _fun), do: {[], bin}

  defp json_take(count, bin, fun) do
    {item, rest} = fun.(bin)
    {items, rest} = json_take(count - 1, rest, fun)
    {[item | items], rest}
  end

  # ---- fail-closed detail (schema-only, never a value; Rule 1) --------------------

  defp meta_detail(type, index, reason),
    do: %{wire_type: type, column_index: index, reason: reason}
end

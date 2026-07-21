defmodule Capstan.Binlog.Rows do
  @moduledoc """
  Decodes a full row image from the `Capstan.Binlog.Decoder`'s raw tuple and the
  resolved `%Capstan.Binlog.TableMap{}`.

  The decoder splits a `ROWS` event into `{op, table_id, present_bitmap(s), raw}` but
  leaves every value byte untouched (F12). This module turns `raw` into decoded rows:

    * `WRITE`/`DELETE` carry one image per row; `UPDATE` carries a before/after image
      **pair** per row. A single event carries **many** rows (e.g. a multi-row `INSERT`
      or a multi-table `UPDATE`), so `raw` is walked until it is exhausted.
    * Each image starts with a per-row NULL bitmap over the columns *present* in the
      image (its width is driven by the columns-PRESENT bitmap, not the full column
      count — `probe:339-351` assumes all columns present, which is wrong in general).
    * Each present, non-null column is cast via `Capstan.Casting.Types.cast/4`; a
      present-null column is recorded as `nil` and consumes no value bytes.

  Rows are returned as `column_name => value` maps, the shape `Capstan.Change` consumes.

  ## Fail closed, never silently wrong

  The single biggest failure mode here is a *plausible-but-wrong* value, so the decode
  is built to fail closed at every point one could arise:

    * **Wrong schema (Q3).** A `table_id` is resolved to a `%TableMap{}` by the owner;
      this module additionally asserts the event's `table_id` equals the map's, so a
      caller that pairs `ta`'s rows with `tb`'s map is refused rather than casting
      against the wrong columns.
    * **Signedness (F3).** The per-column signedness bit is resolved from TLV type 1 and
      passed to the caster. A numeric column with no signedness bitmap fails closed
      rather than defaulting to signed.
    * **Unsupported column (`SET`, JSON opaque, unknown type/meta).** Any caster error
      aborts the whole decode; a partial or raw-passthrough row is never emitted.
    * **Truncated image.** If the raw bytes are too short to hold even a row's NULL
      bitmap, the decode returns `{:error, {:truncated_row_image, _}}` rather than
      raising — a clean halt the pipeline can dispatch (`Event.parse/1`'s CRC check
      makes a truncation here a decode-desync signal, not silent corruption).

  All error tuples carry only schema-level facts, never a column value (Rule 1).
  """

  import Bitwise

  alias Capstan.Binlog.{Decoder, TableMap}
  alias Capstan.Casting.Types

  @typedoc "A decoded row: present columns keyed by name, present-null columns as `nil`."
  @type row :: %{optional(String.t()) => term()}

  @typedoc "The decoded event: the semantic op plus its rows (UPDATE as before/after pairs)."
  @type decoded ::
          {:insert, [row()]}
          | {:delete, [row()]}
          | {:update, [{row(), row()}]}

  @typedoc "A fail-closed refusal — an unsupported column, a schema mismatch, missing metadata, or a truncated image."
  @type error ::
          {:unsupported_column_type, map()}
          | {:table_id_mismatch, map()}
          | {:missing_column_names, map()}
          | {:truncated_row_image, map()}

  # A precomputed per-image view: the present columns' plan entries + the NULL-bitmap
  # width in bytes (constant across every row in the event).
  @typep view :: {[plan_entry()], non_neg_integer()}
  @typep plan_entry :: {String.t(), Types.col_meta(), boolean(), [String.t()]}

  @doc """
  Decodes a `Capstan.Binlog.Decoder` row tuple against its resolved `%TableMap{}`.

  Returns `{:ok, {:insert | :delete, [row]}}`, `{:ok, {:update, [{before, after}]}}`, or
  `{:error, reason}` (fail closed — see the module doc).
  """
  @spec decode(Decoder.rows(), TableMap.t()) :: {:ok, decoded()} | {:error, error()}
  def decode({:write_rows, table_id, present, raw}, %TableMap{} = table_map),
    do: decode_images(:insert, table_id, present, raw, table_map)

  def decode({:delete_rows, table_id, present, raw}, %TableMap{} = table_map),
    do: decode_images(:delete, table_id, present, raw, table_map)

  def decode(
        {:update_rows, table_id, before_present, after_present, raw},
        %TableMap{} = table_map
      ) do
    with :ok <- check_table_id(table_id, table_map),
         {:ok, plan} <- build_plan(table_map),
         before_view = build_view(plan, before_present),
         after_view = build_view(plan, after_present),
         {:ok, pairs} <- walk_update_pairs(before_view, after_view, raw, []) do
      {:ok, {:update, pairs}}
    end
  end

  # ---- WRITE / DELETE: a list of single images ------------------------------------

  defp decode_images(op, table_id, present, raw, table_map) do
    with :ok <- check_table_id(table_id, table_map),
         {:ok, plan} <- build_plan(table_map),
         view = build_view(plan, present),
         {:ok, rows} <- walk_images(view, raw, []) do
      {:ok, {op, rows}}
    end
  end

  defp walk_images(_view, <<>>, acc), do: {:ok, Enum.reverse(acc)}

  defp walk_images(view, bytes, acc) do
    case decode_image(view, bytes) do
      {:ok, row, rest} -> walk_images(view, rest, [row | acc])
      {:error, _reason} = error -> error
    end
  end

  # ---- UPDATE: a list of {before, after} pairs ------------------------------------

  defp walk_update_pairs(_before_view, _after_view, <<>>, acc), do: {:ok, Enum.reverse(acc)}

  defp walk_update_pairs(before_view, after_view, bytes, acc) do
    with {:ok, before_row, rest} <- decode_image(before_view, bytes),
         {:ok, after_row, rest} <- decode_image(after_view, rest) do
      walk_update_pairs(before_view, after_view, rest, [{before_row, after_row} | acc])
    end
  end

  # ---- one image ------------------------------------------------------------------

  @spec decode_image(view(), binary()) :: {:ok, row(), binary()} | {:error, error()}
  defp decode_image({present_plan, null_bytes}, bytes) do
    case bytes do
      <<null_bitmap::binary-size(null_bytes), value_bytes::binary>> ->
        walk_columns(present_plan, null_bitmap, 0, value_bytes, %{})

      _ ->
        {:error, {:truncated_row_image, %{need_null_bytes: null_bytes, have: byte_size(bytes)}}}
    end
  end

  defp walk_columns([], _null_bitmap, _index, value_bytes, acc), do: {:ok, acc, value_bytes}

  defp walk_columns([{name, meta, signed?, str_values} | rest], null_bitmap, index, bytes, acc) do
    if bit_set?(null_bitmap, index) do
      walk_columns(rest, null_bitmap, index + 1, bytes, Map.put(acc, name, nil))
    else
      case Types.cast(meta, signed?, str_values, bytes) do
        {:ok, value, value_rest} ->
          walk_columns(rest, null_bitmap, index + 1, value_rest, Map.put(acc, name, value))

        {:error, _reason} = error ->
          error
      end
    end
  end

  # ---- the per-image view (present columns + null-bitmap width) -------------------

  @spec build_view([plan_entry()], binary()) :: view()
  defp build_view(plan, present_bitmap) do
    present_plan =
      plan
      |> Enum.with_index()
      |> Enum.filter(fn {_entry, index} -> bit_set?(present_bitmap, index) end)
      |> Enum.map(fn {entry, _index} -> entry end)

    {present_plan, div(length(present_plan) + 7, 8)}
  end

  # LSB-first bit read shared by the present bitmap and the per-row NULL bitmap.
  defp bit_set?(bitmap, index),
    do: (:binary.at(bitmap, div(index, 8)) >>> rem(index, 8) &&& 1) == 1

  # ---- the per-table plan (parsed once, reused across every row) ------------------

  defp build_plan(%TableMap{column_types: types, column_metadata: blob} = table_map) do
    with {:ok, metas} <- Types.parse_col_meta(types, blob),
         :ok <- check_column_names(table_map.column_names, types),
         {:ok, signed_flags} <- signedness_flags(metas, table_map.signedness) do
      str_lists = enum_set_values(metas, table_map.enum_str_values, table_map.set_str_values)
      {:ok, Enum.zip([table_map.column_names, metas, signed_flags, str_lists])}
    end
  end

  defp check_column_names(names, types) when length(names) == length(types), do: :ok

  defp check_column_names(names, types),
    do: {:error, {:missing_column_names, %{names: length(names), columns: length(types)}}}

  # Resolve each column's signedness bit from TLV type 1: a bitmap over the numeric
  # columns, MSB-first, 1 = UNSIGNED (F3). A numeric column with no bit fails closed.
  defp signedness_flags(metas, signedness), do: signedness_flags(metas, signedness || <<>>, 0, [])

  defp signedness_flags([], _signedness, _numeric_index, acc), do: {:ok, Enum.reverse(acc)}

  defp signedness_flags([meta | rest], signedness, numeric_index, acc) do
    if numeric?(meta) do
      case signedness_bit(signedness, numeric_index) do
        {:ok, unsigned?} ->
          signedness_flags(rest, signedness, numeric_index + 1, [not unsigned? | acc])

        :error ->
          {:error,
           {:unsupported_column_type,
            %{reason: :missing_signedness, numeric_index: numeric_index}}}
      end
    else
      # Non-numeric columns carry no signedness; the flag is unused by their caster.
      signedness_flags(rest, signedness, numeric_index, [true | acc])
    end
  end

  defp numeric?({:int, _width}), do: true
  defp numeric?({:decimal, _precision, _scale}), do: true
  defp numeric?(_meta), do: false

  defp signedness_bit(signedness, numeric_index) do
    byte_index = div(numeric_index, 8)

    if byte_index < byte_size(signedness) do
      bit = 7 - rem(numeric_index, 8)
      {:ok, (:binary.at(signedness, byte_index) >>> bit &&& 1) == 1}
    else
      :error
    end
  end

  # Map ENUM columns to their member-string list (TLV 6) and SET columns to theirs
  # (TLV 5), each in column order. Non-string columns get `[]`.
  defp enum_set_values(metas, enum_values, set_values),
    do: enum_set_values(metas, enum_values, set_values, [])

  defp enum_set_values([], _enum_values, _set_values, acc), do: Enum.reverse(acc)

  defp enum_set_values([{:enum, _pack} | rest], [values | enum_rest], set_values, acc),
    do: enum_set_values(rest, enum_rest, set_values, [values | acc])

  defp enum_set_values([{:set, _pack} | rest], enum_values, [values | set_rest], acc),
    do: enum_set_values(rest, enum_values, set_rest, [values | acc])

  defp enum_set_values([_meta | rest], enum_values, set_values, acc),
    do: enum_set_values(rest, enum_values, set_values, [[] | acc])

  # ---- Q3 wrong-schema guard ------------------------------------------------------

  defp check_table_id(table_id, %TableMap{table_id: table_id}), do: :ok

  defp check_table_id(table_id, %TableMap{table_id: expected}),
    do: {:error, {:table_id_mismatch, %{event_table_id: table_id, table_map_table_id: expected}}}
end

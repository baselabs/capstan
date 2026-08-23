defmodule Capstan.Binlog.TransactionPayload do
  @moduledoc """
  Decodes a `TRANSACTION_PAYLOAD_EVENT` (type 40) body into the inner event
  stream of the transaction it wraps — the `binlog_transaction_compression`
  consumer arm (ADR-0011).

  Body layout (MySQL server source, `libbinlogevents/src/codecs/binary.cpp`,
  read first-hand): a `net_field_length` TLV header — triples of
  `(type, value_length, value)` closed by a bare `0` end mark — followed by the
  payload bytes. Field types: `0` end mark, `1` payload size, `2` compression
  type, `3` uncompressed size; unrecognized fields are skipped by their length.
  The compression type value is `0` for ZSTD and `255` for NONE
  (`compression/base.h`) — only ZSTD is consumed; anything else fails closed.

  The payload is one or more zstd frames (`Capstan.Zstd`). The inflated bytes
  are the transaction's events — `QUERY(BEGIN)`, `TABLE_MAP`, row events,
  terminator — each a plain 19-byte header + body with **no per-event CRC
  trailer** (the outer type-40 event's CRC32 covers the compressed payload as a
  whole; observed live, the inner headers' `event_size` values sum exactly to
  the inflated length). Splitting is driven by each inner header's `event_size`.

  Every divergence from the declared shape — a missing field, a size that does
  not fit, an unknown compression type, a zstd corruption signal, an
  uncompressed-size mismatch, a malformed inner header — is `{:error, reason}`,
  never a guess. All reasons are value-free (Rule 1): the payload and inner
  bytes carry row values and DDL text and never enter an error term.
  """

  alias Capstan.Binlog.Event
  alias Capstan.Zstd

  @header_size 19

  @otw_end 0
  @otw_payload_size 1
  @otw_compression_type 2
  @otw_uncompressed_size 3

  @compression_zstd 0

  # An inflation ceiling: a single compressed payload can never legitimately
  # exceed what the pipeline could buffer as an in-memory transaction anyway,
  # and a source that declares (or inflates toward) more is either hostile or
  # beyond capstan's operating envelope — refuse it BEFORE inflating when the
  # server declares the size, and after when it does not (never a partial
  # guess; a zip-bomb frame must not exhaust the BEAM).
  @max_inflated_bytes 1024 * 1024 * 1024

  @doc """
  Decodes a type-40 body to its inner event list: parses the TLV header,
  inflates the ZSTD payload, and splits the inner event stream.

  The uncompressed size, when the server declares it, is checked against the
  inflated length — a server-declared oracle over the decompressor's output.
  """
  @spec decode(binary()) :: {:ok, [Event.t()]} | {:error, term()}
  def decode(body) when is_binary(body) do
    with {:ok, tlv, payload_offset} <- walk_tlv(body, 0, %{}),
         {:ok, size} <- require_size(tlv),
         :ok <- require_zstd(tlv),
         :ok <- declared_size_cap(tlv),
         {:ok, frame} <- slice_payload(body, payload_offset, size),
         {:ok, inner} <- inflate(frame),
         :ok <- uncompressed_size_check(tlv, byte_size(inner)),
         :ok <- inflated_size_cap(byte_size(inner)) do
      split_events(inner, [])
    end
  end

  ## -- TLV header ----------------------------------------------------------------

  defp walk_tlv(<<@otw_end, rest::binary>>, consumed, acc),
    do: {:ok, Map.put(acc, :payload, rest), consumed + 1}

  defp walk_tlv(<<type, rest::binary>>, consumed, acc) do
    case net_field_length(rest) do
      {len, rest2} ->
        case rest2 do
          <<value::binary-size(^len), rest3::binary>> ->
            consumed2 = consumed + 1 + (byte_size(rest) - byte_size(rest2)) + len
            store_tlv(type, value, rest3, consumed2, acc)

          _ ->
            {:error, {:payload_header, :truncated_header}}
        end

      :error ->
        {:error, {:payload_header, :truncated_header}}
    end
  end

  defp walk_tlv(<<>>, _consumed, _acc), do: {:error, {:payload_header, :missing_end_mark}}

  # Field values are themselves net_field_length integers; unknown field types
  # are skipped by their declared length (forward compatibility, exactly as the
  # server codec does).
  defp store_tlv(type, value, rest, consumed, acc)
       when type in [@otw_payload_size, @otw_compression_type, @otw_uncompressed_size] do
    case net_field_length(value) do
      {v, <<>>} -> walk_tlv(rest, consumed, Map.put(acc, field_key(type), v))
      _ -> {:error, {:payload_header, {:malformed_field, type}}}
    end
  end

  defp store_tlv(_unknown, _value, rest, consumed, acc), do: walk_tlv(rest, consumed, acc)

  defp field_key(@otw_payload_size), do: :payload_size
  defp field_key(@otw_compression_type), do: :compression_type
  defp field_key(@otw_uncompressed_size), do: :uncompressed_size

  defp require_size(%{payload_size: size}), do: {:ok, size}
  defp require_size(_), do: {:error, {:payload_header, :missing_payload_size}}

  defp require_zstd(%{compression_type: @compression_zstd}), do: :ok

  defp require_zstd(%{compression_type: t}) when is_integer(t),
    do: {:error, {:payload_header, {:unsupported_payload_compression, t}}}

  defp require_zstd(_), do: {:error, {:payload_header, :missing_compression_type}}

  defp slice_payload(body, offset, size) do
    # By construction the payload runs to the END of the event body (the
    # server codec writes payload_size = the remainder): trailing bytes after
    # the declared size are a malformed event, never silently skipped.
    if offset + size > byte_size(body) do
      {:error, {:payload_header, :payload_size_exceeded}}
    else
      if offset + size < byte_size(body) do
        {:error, {:payload_header, :payload_trailing_bytes}}
      else
        {:ok, binary_part(body, offset, size)}
      end
    end
  end

  # The DECLARED uncompressed size gates BEFORE any inflation — a hostile
  # frame's declared size is refused without allocating a byte of output.
  defp declared_size_cap(%{uncompressed_size: declared})
       when is_integer(declared) and declared > @max_inflated_bytes,
       do: {:error, {:payload_header, :payload_too_large}}

  defp declared_size_cap(_tlv), do: :ok

  defp inflated_size_cap(actual) when actual > @max_inflated_bytes,
    do: {:error, {:payload_header, :payload_too_large}}

  defp inflated_size_cap(_actual), do: :ok

  defp inflate(frame) do
    case Zstd.decompress(frame) do
      {:ok, inner} -> {:ok, inner}
      {:error, reason} -> {:error, {:payload_inflate, reason}}
    end
  end

  defp uncompressed_size_check(%{uncompressed_size: declared}, actual)
       when is_integer(declared) and declared != actual,
       do: {:error, {:payload_header, :uncompressed_size_mismatch}}

  defp uncompressed_size_check(_tlv, _actual), do: :ok

  ## -- inner event stream ----------------------------------------------------------

  defp split_events(<<>>, acc), do: {:ok, Enum.reverse(acc)}

  defp split_events(
         <<timestamp::32-little, type::8, server_id::32-little, event_size::32-little,
           log_pos::32-little, flags::16-little, rest::binary>>,
         acc
       ) do
    body_size = event_size - @header_size

    cond do
      event_size < @header_size ->
        {:error, {:payload_inner, :inner_event_malformed}}

      body_size > byte_size(rest) ->
        {:error, {:payload_inner, :inner_event_truncated}}

      type == 40 ->
        # Nested payloads are a shape MySQL never produces; refuse rather than
        # recurse on attacker-shaped bytes.
        {:error, {:payload_inner, :nested_payload_unsupported}}

      true ->
        <<body::binary-size(^body_size), rest2::binary>> = rest

        split_events(rest2, [
          %Event{
            type: type,
            timestamp: timestamp,
            server_id: server_id,
            event_size: event_size,
            log_pos: log_pos,
            flags: flags,
            body: body
          }
          | acc
        ])
    end
  end

  defp split_events(_truncated, _acc), do: {:error, {:payload_inner, :inner_event_truncated}}

  ## -- net_field_length (MySQL protocol) -------------------------------------------

  defp net_field_length(<<b, rest::binary>>) when b < 251, do: {b, rest}
  defp net_field_length(<<252, v::16-little, rest::binary>>), do: {v, rest}
  defp net_field_length(<<253, v::24-little, rest::binary>>), do: {v, rest}
  defp net_field_length(<<254, v::64-little, rest::binary>>), do: {v, rest}
  defp net_field_length(_), do: :error
end

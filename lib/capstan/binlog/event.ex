defmodule Capstan.Binlog.Event do
  @moduledoc """
  Binlog event header parse + CRC32 integrity check — the first decode layer.

  Every binlog event is a 19-byte header followed by a body:

      <<timestamp::32-little, type::8, server_id::32-little, event_size::32-little,
        log_pos::32-little, flags::16-little, body::binary>>

  ## CRC32 posture

  The C1 substrate runs with `binlog_checksum=CRC32` and the dump negotiates it
  (`SET @master_binlog_checksum`), so this module assumes **every** event carries
  a trailing 4-byte CRC32 checksum over the whole event minus those 4 bytes. A
  `FORMAT_DESCRIPTION` event declares the checksum algorithm on the wire, but C1
  does not need to read that byte — the precondition + negotiation already
  guarantee CRC32.

  `parse/1` verifies the trailer and, on success, returns the header fields plus
  the CRC-stripped body. A mismatch fails **loudly**: `{:error, :crc_mismatch}`,
  never a usable event — an integrity failure must never be silently swallowed
  into something that looks valid. A too-short input returns
  `{:error, :truncated}`.

  This module does not interpret the body or map `type` to a name — that is
  the decoder's job (`Capstan.Binlog.Decoder`), which owns the type-byte → name map and
  per-type body decode. `type` here is the raw event-type byte (0..255).

  The artificial `ROTATE` that opens every dump (`timestamp=0`, `log_pos=0`) is
  a normal, CRC-valid event and parses like any other — callers decide what to
  do with it.
  """

  @header_size 19
  @crc_size 4

  @typedoc "Binlog event header fields plus the CRC-verified, CRC-stripped body."
  @type t :: %__MODULE__{
          type: 0..255,
          timestamp: non_neg_integer(),
          server_id: non_neg_integer(),
          event_size: non_neg_integer(),
          log_pos: non_neg_integer(),
          flags: non_neg_integer(),
          body: binary()
        }

  defstruct [:type, :timestamp, :server_id, :event_size, :log_pos, :flags, :body]

  @doc """
  Parses one binlog event: the 19-byte header, splits off the trailing 4-byte
  CRC32 checksum, and verifies it against the header + body.

  Returns `{:ok, %Event{}}` with the CRC-stripped body on a verified event,
  `{:error, :crc_mismatch}` on a checksum mismatch (fail closed), or
  `{:error, :truncated}` when `event` is too short to hold a header and
  checksum.
  """
  @spec parse(binary()) :: {:ok, t()} | {:error, :truncated | :crc_mismatch}
  def parse(event) when byte_size(event) < @header_size + @crc_size do
    {:error, :truncated}
  end

  def parse(event) do
    <<timestamp::32-little, type::8, server_id::32-little, event_size::32-little,
      log_pos::32-little, flags::16-little, body_with_checksum::binary>> = event

    body_size = byte_size(body_with_checksum) - @crc_size
    <<body::binary-size(^body_size), checksum::32-little>> = body_with_checksum

    checked_size = byte_size(event) - @crc_size
    <<checked::binary-size(^checked_size), _checksum_bytes::binary>> = event

    if :erlang.crc32(checked) == checksum do
      {:ok,
       %__MODULE__{
         type: type,
         timestamp: timestamp,
         server_id: server_id,
         event_size: event_size,
         log_pos: log_pos,
         flags: flags,
         body: body
       }}
    else
      {:error, :crc_mismatch}
    end
  end
end

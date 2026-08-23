defmodule Capstan.Protocol.Packet do
  @moduledoc """
  MySQL client/server protocol framing.

  Every MySQL wire packet is a 4-byte header — `<<length::24-little, sequence_id::8>>` —
  followed by exactly `length` payload bytes.

  ## Split packets (>= 16 MiB)

  A logical payload of `0xFFFFFF` bytes or more does not fit the 24-bit length field, so
  the peer splits it: each `0xFFFFFF`-byte packet means *more follows*, and the sequence
  terminates with the first packet shorter than `0xFFFFFF` — which is **zero-length** when
  the logical payload is an exact multiple of `0xFFFFFF`.

  Reading a continuation packet as a fresh header does not fail loudly: the reader
  desynchronises mid-payload and every subsequent frame is garbage. `read_packet/2`
  therefore reassembles the whole logical payload before returning, and **raises** on a
  short or truncated read rather than returning a partial payload.

  Errors raised here never include payload bytes — only lengths, frame indices and the
  transport reason.
  """

  @max_payload 0xFFFFFF
  @default_timeout 20_000

  @typedoc """
  A socket tagged with its transport module, so the same framing serves both the
  plaintext and the TLS-upgraded connection.
  """
  @type socket :: {:gen_tcp, :gen_tcp.socket()} | {:ssl, :ssl.sslsocket()}

  @typedoc "MySQL packet sequence id — wraps at 255."
  @type sequence_id :: 0..255

  @doc """
  Reads one logical packet, reassembling a split (>= 16 MiB) payload.

  Returns `{sequence_id, payload}` where `sequence_id` is the id of the **last** frame of
  the sequence. Raises if the stream ends or stalls mid-packet.
  """
  @spec read_packet(socket(), timeout()) :: {sequence_id(), binary()}
  def read_packet(socket, timeout \\ @default_timeout) do
    read_frames(socket, timeout, 1, [])
  end

  defp read_frames(socket, timeout, frame, acc) do
    {len, seq} = read_header(socket, timeout, frame)
    acc = [read_payload(socket, len, timeout, frame) | acc]

    if len == @max_payload do
      read_frames(socket, timeout, frame + 1, acc)
    else
      {seq, acc |> Enum.reverse() |> IO.iodata_to_binary()}
    end
  end

  defp read_header(socket, timeout, frame) do
    case recv(socket, 4, timeout) do
      {:ok, <<len::24-little, seq::8>>} ->
        {len, seq}

      {:error, reason} ->
        raise "capstan: truncated MySQL packet header at frame #{frame} (#{inspect(reason)})"
    end
  end

  # `:gen_tcp.recv/3` with a length of 0 returns whatever is buffered, so an
  # empty packet must never reach the socket.
  defp read_payload(_socket, 0, _timeout, _frame), do: <<>>

  defp read_payload(socket, len, timeout, frame) do
    case recv(socket, len, timeout) do
      {:ok, payload} ->
        payload

      {:error, reason} ->
        raise "capstan: truncated MySQL packet payload at frame #{frame}, " <>
                "expected #{len} bytes (#{inspect(reason)})"
    end
  end

  @doc """
  Frames `payload` under `sequence_id`, splitting it when it reaches 16 MiB.

  A payload that is an exact multiple of `0xFFFFFF` bytes is terminated with a
  zero-length packet, mirroring what `read_packet/2` expects to read. Sequence ids
  increment per frame and wrap at 255.
  """
  @spec encode(binary(), sequence_id()) :: iodata()
  def encode(payload, sequence_id)
      when is_binary(payload) and is_integer(sequence_id) and sequence_id in 0..255 do
    encode_frames(payload, sequence_id, [])
  end

  defp encode_frames(payload, seq, acc) when byte_size(payload) >= @max_payload do
    <<chunk::binary-size(@max_payload), rest::binary>> = payload
    frame = [<<@max_payload::24-little, seq::8>>, chunk]
    encode_frames(rest, rem(seq + 1, 256), [frame | acc])
  end

  defp encode_frames(payload, seq, acc) do
    frame = [<<byte_size(payload)::24-little, seq::8>>, payload]
    Enum.reverse([frame | acc])
  end

  @doc """
  Frames `payload` under `sequence_id` and writes it to `socket`.
  """
  @spec send_packet(socket(), binary(), sequence_id()) :: :ok | {:error, term()}
  def send_packet({:gen_tcp, sock}, payload, sequence_id),
    do: :gen_tcp.send(sock, encode(payload, sequence_id))

  def send_packet({:ssl, sock}, payload, sequence_id),
    do: :ssl.send(sock, encode(payload, sequence_id))

  @doc """
  Reads a length-encoded integer, returning `{value, rest}`.
  """
  @spec lenenc_int(binary()) :: {non_neg_integer(), binary()}
  def lenenc_int(<<n, rest::binary>>) when n < 0xFB, do: {n, rest}
  def lenenc_int(<<0xFC, n::16-little, rest::binary>>), do: {n, rest}
  def lenenc_int(<<0xFD, n::24-little, rest::binary>>), do: {n, rest}
  def lenenc_int(<<0xFE, n::64-little, rest::binary>>), do: {n, rest}

  @doc """
  Reads a length-encoded string, returning `{value, rest}`.
  """
  @spec lenenc_str(binary()) :: {binary(), binary()}
  def lenenc_str(bin) do
    {n, rest} = lenenc_int(bin)
    <<s::binary-size(^n), rest2::binary>> = rest
    {s, rest2}
  end

  defp recv({:gen_tcp, sock}, n, timeout), do: :gen_tcp.recv(sock, n, timeout)
  defp recv({:ssl, sock}, n, timeout), do: :ssl.recv(sock, n, timeout)
end

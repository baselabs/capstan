defmodule Capstan.Protocol.Command do
  @moduledoc """
  MySQL replication commands and text-resultset decoding.

  Builds the request payloads capstan sends over an authenticated
  `Capstan.Protocol.Packet` socket — `COM_QUERY`, `COM_REGISTER_SLAVE`, and the
  resume-carrying `COM_BINLOG_DUMP_GTID` — and decodes the text resultset a
  `COM_QUERY` returns. Framing, the >16 MiB split, and length-encoded primitives all
  live in `Capstan.Protocol.Packet`; GTID-set algebra lives in `Capstan.Gtid`.

  ## The resume encoding (F2 — silent-loss class)

  `COM_BINLOG_DUMP_GTID` carries the checkpoint the client has already applied, and
  the server resumes streaming from the first transaction *not* in it. The wire
  interval end is **EXCLUSIVE** while `Capstan.Gtid` is **INCLUSIVE**: a checkpoint of
  `uuid:1-11` (GTIDs 1..11 applied) encodes as `start = 1, end = 12`. The `high + 1`
  conversion lives here, in `com_binlog_dump_gtid/2`, and nowhere else. An off-by-one
  skips or replays exactly one transaction per interval on every restart — silently,
  with no server error — so it is pinned by round-trip byte tests and a live tripwire.

  ## NULL in a text resultset

  A NULL column value is the single byte `0xFB`, which is *not* a length. `Packet`'s
  `lenenc_int/1` has no `0xFB` clause by design, so the row decoder must detect NULL
  and yield `nil` before delegating to `lenenc_str/1` — otherwise a NULL column would
  raise `FunctionClauseError`.
  """

  alias Capstan.Gtid
  alias Capstan.Protocol.Packet

  @com_query 0x03
  @com_register_slave 0x15
  @com_binlog_dump_gtid 0x1E

  # COM_BINLOG_DUMP_GTID flags: BINLOG_THROUGH_GTID selects GTID-based positioning.
  @binlog_through_gtid 0x04
  # The magic binlog start offset; the binlog name is empty in GTID mode.
  @binlog_start_pos 4

  @default_timeout 20_000

  @typedoc "A row of a text resultset; a NULL column is `nil`."
  @type row :: [binary() | nil]

  @doc """
  Builds a `COM_QUERY` request: the command byte `0x03` followed by the SQL text.
  """
  @spec com_query(binary()) :: binary()
  def com_query(sql) when is_binary(sql), do: <<@com_query, sql::binary>>

  @doc """
  Builds a `COM_REGISTER_SLAVE` request identifying this client to the source.

  Hostname, user and password are advertised empty and the replication rank and
  master-id are zero — capstan positions with `COM_BINLOG_DUMP_GTID`, so only the
  `server_id` is meaningful.
  """
  @spec com_register_slave(non_neg_integer()) :: binary()
  def com_register_slave(server_id) when is_integer(server_id) and server_id >= 0 do
    <<@com_register_slave, server_id::32-little, 0::8, 0::8, 0::8, 0::16-little, 0::32-little,
      0::32-little>>
  end

  @doc """
  Builds a `COM_BINLOG_DUMP_GTID` request that resumes from `gtid_set`.

  The payload is the command byte, `flags` (BINLOG_THROUGH_GTID), `server_id`, an
  empty binlog name (length 0), the start position (4), then the `data_size`-prefixed
  GTID-set block: `n_sids`, and per source the 16 raw UUID bytes, its interval count,
  and each `{start, end}` pair. **Each INCLUSIVE `high` is encoded as the EXCLUSIVE
  wire `high + 1`**. An empty set encodes as `n_sids = 0` and streams everything
  the server still retains.
  """
  @spec com_binlog_dump_gtid(non_neg_integer(), Gtid.t()) :: binary()
  def com_binlog_dump_gtid(server_id, gtid_set) when is_integer(server_id) and server_id >= 0 do
    gtid_data = encode_gtid_set(gtid_set)

    <<@com_binlog_dump_gtid, @binlog_through_gtid::16-little, server_id::32-little, 0::32-little,
      @binlog_start_pos::64-little, byte_size(gtid_data)::32-little, gtid_data::binary>>
  end

  @doc """
  Sends `sql` as a `COM_QUERY` on `socket` and reads the response.

  Returns `:ok` for a statement with no resultset (an OK packet — e.g. `SET`),
  `{:ok, rows}` for a text resultset (each row a list of column values, `nil` for a
  NULL column), and `{:error, {:query_error, code}}` for a server error. A transport
  failure on send is `{:error, {:transport, reason}}`.
  """
  @spec query(Packet.socket(), binary(), timeout()) ::
          :ok
          | {:ok, [row()]}
          | {:error, {:query_error, non_neg_integer()} | {:transport, term()}}
  def query(socket, sql, timeout \\ @default_timeout) when is_binary(sql) do
    case Packet.send_packet(socket, com_query(sql), 0) do
      :ok -> read_query_response(socket, timeout)
      {:error, reason} -> {:error, {:transport, reason}}
    end
  end

  ## ---------------------------------------------------------------------------
  ## COM_BINLOG_DUMP_GTID payload
  ## ---------------------------------------------------------------------------

  defp encode_gtid_set(gtid_set) do
    sources = Gtid.sources(gtid_set)
    sids = for {uuid, intervals} <- sources, into: <<>>, do: encode_sid(uuid, intervals)
    <<length(sources)::64-little, sids::binary>>
  end

  defp encode_sid(uuid, intervals) do
    uuid_bytes = uuid |> String.replace("-", "") |> Base.decode16!(case: :mixed)

    encoded_intervals =
      for {low, high} <- intervals, into: <<>> do
        # INCLUSIVE high -> EXCLUSIVE wire end. An off-by-one here silently skips or
        # replays exactly one transaction per interval on every restart (F2).
        <<low::64-little, high + 1::64-little>>
      end

    <<uuid_bytes::binary, length(intervals)::64-little, encoded_intervals::binary>>
  end

  ## ---------------------------------------------------------------------------
  ## text-resultset decode
  ## ---------------------------------------------------------------------------

  defp read_query_response(socket, timeout) do
    case Packet.read_packet(socket, timeout) do
      {_seq, <<0x00, _rest::binary>>} ->
        :ok

      {_seq, <<0xFF, code::16-little, _rest::binary>>} ->
        {:error, {:query_error, code}}

      {_seq, column_count_packet} ->
        {ncols, <<>>} = Packet.lenenc_int(column_count_packet)
        skip_column_defs(socket, ncols, timeout)
        {:ok, read_rows(socket, timeout, [])}
    end
  end

  # With CLIENT_DEPRECATE_EOF there is no EOF after the column definitions, so read
  # exactly `ncols` definition packets and discard them.
  defp skip_column_defs(_socket, 0, _timeout), do: :ok

  defp skip_column_defs(socket, remaining, timeout) do
    {_seq, _column_def} = Packet.read_packet(socket, timeout)
    skip_column_defs(socket, remaining - 1, timeout)
  end

  # Canonical DEPRECATE_EOF rule: a `0xFE`-led packet is the terminating OK packet iff
  # its TOTAL length is < 9 bytes (`1 + byte_size(rest) < 9`, i.e. `rest < 8`). A row
  # that legitimately starts with `0xFE` uses the 8-byte lenenc length form, so it is
  # >= 9 bytes total (`rest >= 8`) and is never mistaken for the terminator.
  defp read_rows(socket, timeout, acc) do
    case Packet.read_packet(socket, timeout) do
      {_seq, <<0xFE, rest::binary>>} when byte_size(rest) < 8 ->
        Enum.reverse(acc)

      {_seq, row} ->
        read_rows(socket, timeout, [decode_text_row(row, []) | acc])
    end
  end

  defp decode_text_row(<<>>, acc), do: Enum.reverse(acc)

  # 0xFB is the NULL marker, not a length; handle it before touching lenenc_str/1.
  defp decode_text_row(<<0xFB, rest::binary>>, acc), do: decode_text_row(rest, [nil | acc])

  defp decode_text_row(bin, acc) do
    {value, rest} = Packet.lenenc_str(bin)
    decode_text_row(rest, [value | acc])
  end
end

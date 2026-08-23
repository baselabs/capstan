defmodule Capstan.Binlog.TransactionPayloadTest do
  @moduledoc """
  Conformance for the TRANSACTION_PAYLOAD consumer arm against REAL captured
  compressed transactions (`zstd_*` fixtures, `binlog_transaction_compression=ON`
  substrate). The `.inner` oracle files are the same frames inflated by the
  REFERENCE `zstd` binary at capture time — the arm's decode must reproduce
  them byte-for-byte through its Event list, and the assembler must fold a
  bare-GTID + payload pair exactly as it folds the oracle's inner events.
  """

  use ExUnit.Case, async: true

  alias Capstan.Assembler
  alias Capstan.Binlog.{Event, TransactionPayload}
  alias Capstan.Position

  @fixtures Path.expand("../../fixtures/binlog", __DIR__)

  defp payload_event(dir, file) do
    path = Path.join([@fixtures, dir, file])
    {:ok, event} = Event.parse(File.read!(path))
    event
  end

  # Re-serializes the decoded inner Events to bytes; compared against `.inner`.
  defp serialize(%Event{} = e) do
    <<e.timestamp::32-little, e.type::8, e.server_id::32-little, e.event_size::32-little,
      e.log_pos::32-little, e.flags::16-little, e.body::binary>>
  end

  test "every captured payload decodes to exactly the reference-inflated bytes" do
    bins = Path.wildcard(Path.join(@fixtures, "zstd_*/*transaction_payload.bin"))
    assert length(bins) >= 9

    for bin <- bins do
      {:ok, event} = Event.parse(File.read!(bin))
      assert {:ok, inner} = TransactionPayload.decode(event.body), "decode failed: #{bin}"

      assert IO.iodata_to_binary(Enum.map(inner, &serialize/1)) ==
               File.read!(String.replace_suffix(bin, ".bin", ".inner")),
             "inner bytes differ from the reference oracle: #{bin}"
    end
  end

  test "zstd_rows payloads wrap INSERT/UPDATE/DELETE row events" do
    for {file, expected} <- [
          {"06-transaction_payload.bin", [2, 19, 30, 16]},
          {"08-transaction_payload.bin", [2, 19, 31, 16]},
          {"10-transaction_payload.bin", [2, 19, 32, 16]}
        ] do
      event = payload_event("zstd_rows", file)
      assert {:ok, inner} = TransactionPayload.decode(event.body)
      assert Enum.map(inner, & &1.type) == expected
    end
  end

  test "the large multi-block payload folds to a full transaction" do
    gtid = payload_event("zstd_large", "05-gtid.bin")
    payload = payload_event("zstd_large", "06-transaction_payload.bin")

    assert {:ok, [txn], _pos} = Assembler.run([gtid, payload], %Position{gtid_set: ""})
    assert [%Capstan.Change{} | _] = txn.changes
    assert length(txn.changes) == 3
  end

  describe "fold equivalence: payload vs oracle inner events" do
    test "zstd_small delivers the same transaction as its uncompressed inner stream" do
      gtid = payload_event("zstd_small", "05-gtid.bin")
      payload = payload_event("zstd_small", "06-transaction_payload.bin")
      oracle_inner = oracle_events("zstd_small/06-transaction_payload.inner")

      assert {:ok, [via_payload], _} = Assembler.run([gtid, payload], %Position{gtid_set: ""})
      assert {:ok, [via_inner], _} = Assembler.run([gtid | oracle_inner], %Position{gtid_set: ""})
      assert via_payload == via_inner
    end

    test "zstd_rows (all three payloads) delivers identical transactions" do
      gtid_insert = payload_event("zstd_rows", "05-gtid.bin")
      payload_insert = payload_event("zstd_rows", "06-transaction_payload.bin")

      gtid_update = payload_event("zstd_rows", "07-gtid.bin")
      payload_update = payload_event("zstd_rows", "08-transaction_payload.bin")

      gtid_delete = payload_event("zstd_rows", "09-gtid.bin")
      payload_delete = payload_event("zstd_rows", "10-transaction_payload.bin")

      start = %Position{gtid_set: ""}

      assert {:ok, via_payload, _} =
               Assembler.run(
                 [
                   gtid_insert,
                   payload_insert,
                   gtid_update,
                   payload_update,
                   gtid_delete,
                   payload_delete
                 ],
                 start
               )

      assert {:ok, via_inner, _} =
               Assembler.run(
                 [
                   gtid_insert
                   | oracle_events("zstd_rows/06-transaction_payload.inner")
                 ] ++
                   [
                     gtid_update
                     | oracle_events("zstd_rows/08-transaction_payload.inner")
                   ] ++
                   [gtid_delete | oracle_events("zstd_rows/10-transaction_payload.inner")],
                 start
               )

      assert via_payload == via_inner
      assert length(via_payload) == 3
    end
  end

  describe "fail-closed tripwires (protected mutations)" do
    test "a non-ZSTD compression type value is refused" do
      event = payload_event("zstd_small", "06-transaction_payload.bin")
      tampered = replace_tlv_value(event.body, 2, fn _v -> 1 end)

      assert {:error, {:payload_header, {:unsupported_payload_compression, 1}}} =
               TransactionPayload.decode(tampered)
    end

    test "a corrupted zstd payload is refused, never mis-decoded" do
      event = payload_event("zstd_small", "06-transaction_payload.bin")
      {triples, frame} = parse_tlv(event.body)
      frame_at = byte_size(event.body) - byte_size(frame)
      _ = triples
      <<pre::binary-size(^frame_at), _magic::4-binary, frame_rest::binary>> = event.body
      tampered = <<pre::binary, 0, 0, 0, 0, frame_rest::binary>>

      assert {:error, {:payload_inflate, _}} = TransactionPayload.decode(tampered)
    end

    test "an uncompressed-size mismatch is refused" do
      event = payload_event("zstd_small", "06-transaction_payload.bin")
      tampered = replace_tlv_value(event.body, 3, fn _v -> 200 end)

      assert {:error, {:payload_header, :uncompressed_size_mismatch}} =
               TransactionPayload.decode(tampered)
    end

    test "a truncated body is refused" do
      event = payload_event("zstd_small", "06-transaction_payload.bin")
      <<truncated::20-binary, _::binary>> = event.body
      assert {:error, {:payload_header, _}} = TransactionPayload.decode(truncated)
    end

    test "a payload size exceeding the body is refused" do
      event = payload_event("zstd_small", "06-transaction_payload.bin")
      tampered = replace_tlv_value(event.body, 1, fn v -> v + 10_000 end)

      assert {:error, {:payload_header, _}} = TransactionPayload.decode(tampered)
    end
  end

  # -- TLV test helpers (value-length-aware; the triples' widths vary) ----------

  # Returns {value_offset, decoded_value_int} for a field type in the TLV prefix.
  defp tlv_field(bin, want) do
    {triples, _rest} = parse_tlv(bin)

    {offset, value} =
      Enum.reduce_while(triples, {0, nil}, fn {type, value}, {off, _} ->
        if type == want,
          do: {:halt, {off + 1, decode_lenenc(value)}},
          else: {:cont, {off + 1 + lenenc_size(byte_size(value)) + byte_size(value), nil}}
      end)

    {offset, value}
  end

  # Rebuilds the body with one field's VALUE substituted (minimal lenenc). A
  # triple is (type, lenenc length, value bytes); the value bytes are the
  # lenenc encoding of the field's integer.
  defp replace_tlv_value(bin, want, fun) do
    {triples, rest} = parse_tlv(bin)

    rebuilt =
      Enum.map(triples, fn {type, value} ->
        if type == want do
          value_bytes = lenenc(fun.(decode_lenenc(value)))
          <<type>> <> lenenc(byte_size(value_bytes)) <> value_bytes
        else
          <<type>> <> lenenc(byte_size(value)) <> value
        end
      end)

    IO.iodata_to_binary(rebuilt) <> <<0>> <> rest
  end

  defp parse_tlv(bin), do: do_parse_tlv(bin, [])

  defp do_parse_tlv(<<0, rest::binary>>, acc), do: {Enum.reverse(acc), rest}

  defp do_parse_tlv(<<type, rest::binary>>, acc) do
    {len, rest2} = split_lenenc(rest)
    <<value::binary-size(^len), rest3::binary>> = rest2
    do_parse_tlv(rest3, [{type, value} | acc])
  end

  defp split_lenenc(<<b, rest::binary>>) when b < 251, do: {b, rest}
  defp split_lenenc(<<252, v::16-little, rest::binary>>), do: {v, rest}
  defp split_lenenc(<<253, v::24-little, rest::binary>>), do: {v, rest}
  defp split_lenenc(<<254, v::64-little, rest::binary>>), do: {v, rest}

  defp lenenc_size(n) when n < 251, do: 1
  defp lenenc_size(n) when n < 65_536, do: 3
  defp lenenc_size(n) when n < 16_777_216, do: 4
  defp lenenc_size(_n), do: 9

  defp lenenc(v) when v < 251, do: <<v>>
  defp lenenc(v) when v < 65_536, do: <<252, v::16-little>>
  defp lenenc(v) when v < 16_777_216, do: <<253, v::24-little>>
  defp lenenc(v), do: <<254, v::64-little>>

  defp decode_lenenc(<<b>>) when b < 251, do: b
  defp decode_lenenc(<<252, v::16-little>>), do: v
  defp decode_lenenc(<<253, v::24-little>>), do: v
  defp decode_lenenc(<<254, v::64-little>>), do: v

  # Parses a `.inner` oracle (19-byte headers, no CRC trailers) into Events —
  # an independent path from TransactionPayload's own splitter.
  defp oracle_events(rel) do
    bin = File.read!(Path.join(@fixtures, rel))

    Stream.unfold(bin, fn
      <<>> ->
        nil

      <<ts::32-little, type::8, sid::32-little, size::32-little, lp::32-little, fl::16-little,
        rest::binary>> ->
        body_size = size - 19
        <<body::binary-size(^body_size), rest2::binary>> = rest

        event = %Event{
          type: type,
          timestamp: ts,
          server_id: sid,
          event_size: size,
          log_pos: lp,
          flags: fl,
          body: body
        }

        {event, rest2}
    end)
    |> Enum.to_list()
  end
end

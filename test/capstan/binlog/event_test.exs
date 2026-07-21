defmodule Capstan.Binlog.EventTest do
  use ExUnit.Case, async: true

  alias Capstan.Binlog.Event

  @fixtures_root Path.join([__DIR__, "..", "..", "fixtures", "binlog"])

  defp fixture(scenario, filename) do
    [@fixtures_root, scenario, filename]
    |> Path.join()
    |> File.read!()
  end

  # Fixture files are named "<NN>-<event_type>.bin"; the numeric prefix ordering
  # differs per scenario, but every scenario's dump opens with this sequence.
  @representative %{
    "01-rotate.bin" => 4,
    "02-format_description.bin" => 15,
    "03-previous_gtids.bin" => 35,
    "04-heartbeat.bin" => 27,
    "05-gtid.bin" => 33,
    "06-query.bin" => 2,
    "07-table_map.bin" => 19,
    "08-write_rows.bin" => 30,
    "09-xid.bin" => 16
  }

  describe "parse/1 against Task 7 fixtures" do
    for {filename, expected_type} <- @representative do
      test "#{filename} parses with type #{expected_type}, CRC verified, non-empty body" do
        event = fixture("simple_dml", unquote(filename))

        assert {:ok, %Event{} = parsed} = Event.parse(event)
        assert parsed.type == unquote(expected_type)
        assert byte_size(parsed.body) > 0
      end
    end

    test "the artificial ROTATE that opens every dump has ts=0, log_pos=0, and still verifies" do
      event = fixture("simple_dml", "01-rotate.bin")

      assert {:ok, %Event{} = parsed} = Event.parse(event)
      assert parsed.type == 4
      assert parsed.timestamp == 0
      assert parsed.log_pos == 0
    end

    test "HEARTBEAT (type 27) parses like any other event" do
      event = fixture("simple_dml", "04-heartbeat.bin")

      assert {:ok, %Event{type: 27}} = Event.parse(event)
    end

    test "every captured fixture across every scenario parses and verifies" do
      fixtures = Path.wildcard(Path.join(@fixtures_root, "*/*.bin"))
      assert fixtures != []

      for path <- fixtures do
        event = File.read!(path)

        assert {:ok, %Event{} = parsed} = Event.parse(event),
               "expected #{path} to parse and CRC-verify"

        assert byte_size(parsed.body) > 0, "expected #{path} to have a non-empty body"
      end
    end
  end

  describe "parse/1 tamper tripwire" do
    test "flipping one byte in the body rejects with :crc_mismatch" do
      original = fixture("simple_dml", "09-xid.bin")

      # 09-xid.bin: 19-byte header + 8-byte body (the XID) + 4-byte CRC32 trailer.
      # Flip the first body byte (offset 19) — inside the body, not the header,
      # and not touching event_size (a header field).
      <<header::binary-size(19), body_byte, rest::binary>> = original
      tampered_byte = Bitwise.bxor(body_byte, 0xFF)
      tampered = <<header::binary, tampered_byte, rest::binary>>

      assert byte_size(tampered) == byte_size(original)
      assert tampered != original

      assert {:error, :crc_mismatch} = Event.parse(tampered)
    end
  end

  describe "parse/1 truncation" do
    test "an input shorter than header + CRC size is rejected as truncated" do
      assert {:error, :truncated} = Event.parse(<<0::8*10>>)
    end

    test "an empty input is rejected as truncated" do
      assert {:error, :truncated} = Event.parse(<<>>)
    end
  end
end

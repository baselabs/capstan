defmodule Capstan.Protocol.PacketTest do
  use ExUnit.Case, async: true

  alias Capstan.Protocol.Packet

  @max 0xFFFFFF
  @loopback {127, 0, 0, 1}

  describe "lenenc_int/1" do
    test "1-byte width covers 0..0xFA" do
      assert {0, "rest"} = Packet.lenenc_int(<<0, "rest">>)
      assert {1, ""} = Packet.lenenc_int(<<1>>)
      assert {0xFA, "rest"} = Packet.lenenc_int(<<0xFA, "rest">>)
    end

    test "0xFC marks the 2-byte width" do
      assert {251, "rest"} = Packet.lenenc_int(<<0xFC, 251::16-little, "rest">>)
      assert {0xFFFF, ""} = Packet.lenenc_int(<<0xFC, 0xFFFF::16-little>>)
    end

    test "0xFD marks the 3-byte width" do
      assert {0x10000, "rest"} = Packet.lenenc_int(<<0xFD, 0x10000::24-little, "rest">>)
      assert {0xFFFFFF, ""} = Packet.lenenc_int(<<0xFD, 0xFFFFFF::24-little>>)
    end

    test "0xFE marks the 8-byte width" do
      assert {0x1000000, "rest"} = Packet.lenenc_int(<<0xFE, 0x1000000::64-little, "rest">>)

      assert {0xFFFFFFFFFFFFFFFF, ""} =
               Packet.lenenc_int(<<0xFE, 0xFFFFFFFFFFFFFFFF::64-little>>)
    end

    test "the width markers are decoded as markers, never as 1-byte values" do
      # 0xFC/0xFD/0xFE as a bare first byte would decode to 252/253/254 if the
      # width clauses were missing or mis-ordered.
      refute match?({252, _}, Packet.lenenc_int(<<0xFC, 7::16-little>>))
      refute match?({253, _}, Packet.lenenc_int(<<0xFD, 7::24-little>>))
      refute match?({254, _}, Packet.lenenc_int(<<0xFE, 7::64-little>>))
    end
  end

  describe "lenenc_str/1" do
    test "reads a 1-byte-width string and returns the remainder" do
      assert {"abc", "tail"} = Packet.lenenc_str(<<3, "abc", "tail">>)
    end

    test "reads a zero-length string" do
      assert {"", "tail"} = Packet.lenenc_str(<<0, "tail">>)
    end

    test "reads a 2-byte-width string" do
      body = :binary.copy(<<0x41>>, 300)
      assert {^body, "tail"} = Packet.lenenc_str(<<0xFC, 300::16-little, body::binary, "tail">>)
    end
  end

  describe "read_packet/2 — single-frame header framing" do
    test "reads a payload of the declared length and returns the sequence id" do
      sock = packet_source([<<5::24-little, 7::8, "hello">>])
      assert {7, "hello"} = Packet.read_packet(sock, 5_000)
    end

    test "reads a zero-length packet without consuming the following bytes" do
      sock = packet_source([<<0::24-little, 3::8>>, <<2::24-little, 4::8, "hi">>])
      assert {3, ""} = Packet.read_packet(sock, 5_000)
      assert {4, "hi"} = Packet.read_packet(sock, 5_000)
    end

    test "reads consecutive packets in order" do
      sock =
        packet_source([
          <<1::24-little, 0::8, "a">>,
          <<2::24-little, 1::8, "bc">>
        ])

      assert {0, "a"} = Packet.read_packet(sock, 5_000)
      assert {1, "bc"} = Packet.read_packet(sock, 5_000)
    end
  end

  describe "read_packet/2 — >16 MiB split-packet reassembly" do
    test "a 0xFFFFFF-byte packet is followed by a continuation and reassembles" do
      head = :binary.copy(<<0xAB>>, @max)
      tail = :binary.copy(<<0xCD>>, 10)

      sock =
        packet_source([
          <<@max::24-little, 0::8, head::binary>>,
          <<10::24-little, 1::8, tail::binary>>
        ])

      assert {seq, payload} = Packet.read_packet(sock, 30_000)
      assert seq == 1, "the LAST sequence id of the split packet must be returned"
      assert byte_size(payload) == @max + 10
      assert payload == head <> tail
    end

    test "a payload that is an exact multiple of 0xFFFFFF ends with a zero-length packet" do
      head = :binary.copy(<<0xAB>>, @max)

      sock =
        packet_source([
          <<@max::24-little, 0::8, head::binary>>,
          <<0::24-little, 1::8>>
        ])

      assert {1, payload} = Packet.read_packet(sock, 30_000)
      assert byte_size(payload) == @max
      assert payload == head
    end

    test "a payload spanning THREE frames reassembles in order" do
      # Two-frame cases cannot distinguish an n-ary reassembly loop from one capped
      # at a single continuation. A >32 MiB payload is reachable in practice (a large
      # WRITE_ROWS batch, a BLOB column), and a capped loop would truncate it SILENTLY
      # — the F15 desync class this module exists to prevent.
      a = :binary.copy(<<0xA1>>, @max)
      b = :binary.copy(<<0xB2>>, @max)
      c = :binary.copy(<<0xC3>>, 7)

      sock =
        packet_source([
          <<@max::24-little, 0::8, a::binary>>,
          <<@max::24-little, 1::8, b::binary>>,
          <<7::24-little, 2::8, c::binary>>
        ])

      assert {seq, payload} = Packet.read_packet(sock, 30_000)
      assert seq == 2, "the LAST sequence id across all three frames must be returned"
      assert byte_size(payload) == 2 * @max + 7
      assert payload == a <> b <> c, "frames must reassemble in wire order"
    end

    test "the packet after a completed split sequence is framed independently" do
      head = :binary.copy(<<0xAB>>, @max)

      sock =
        packet_source([
          <<@max::24-little, 0::8, head::binary>>,
          <<0::24-little, 1::8>>,
          <<3::24-little, 2::8, "nxt">>
        ])

      assert {1, ^head} = Packet.read_packet(sock, 30_000)
      assert {2, "nxt"} = Packet.read_packet(sock, 5_000)
    end
  end

  describe "read_packet/2 — desync tripwires" do
    test "a continuation whose payload is short RAISES, never returns a short payload" do
      head = :binary.copy(<<0xAB>>, @max)

      sock =
        packet_source([
          <<@max::24-little, 0::8, head::binary>>,
          # declares 10 bytes, delivers 4, then the peer closes
          <<10::24-little, 1::8, "abcd">>
        ])

      assert_raise RuntimeError, ~r/truncated/, fn -> Packet.read_packet(sock, 30_000) end
    end

    test "a truncated continuation HEADER raises" do
      head = :binary.copy(<<0xAB>>, @max)

      sock =
        packet_source([
          <<@max::24-little, 0::8, head::binary>>,
          <<10::16-little>>
        ])

      assert_raise RuntimeError, ~r/truncated/, fn -> Packet.read_packet(sock, 30_000) end
    end

    test "a truncated first payload raises" do
      sock = packet_source([<<9::24-little, 0::8, "abc">>])

      assert_raise RuntimeError, ~r/truncated/, fn -> Packet.read_packet(sock, 5_000) end
    end

    test "the raise message carries no payload bytes (Rule 1)" do
      sock = packet_source([<<64::24-little, 0::8, "s3cr3tval">>])

      error = assert_raise(RuntimeError, fn -> Packet.read_packet(sock, 5_000) end)

      refute Exception.message(error) =~ "s3cr3t"
    end
  end

  describe "encode/2" do
    test "prefixes the 4-byte header" do
      assert IO.iodata_to_binary(Packet.encode("hello", 7)) == <<5::24-little, 7::8, "hello">>
    end

    test "encodes a zero-length payload" do
      assert IO.iodata_to_binary(Packet.encode("", 0)) == <<0::24-little, 0::8>>
    end

    test "splits a 0xFFFFFF-byte payload and terminates with a zero-length packet" do
      payload = :binary.copy(<<0xAB>>, @max)
      encoded = IO.iodata_to_binary(Packet.encode(payload, 0))

      assert encoded == <<@max::24-little, 0::8, payload::binary, 0::24-little, 1::8>>
    end

    test "splits a payload spanning THREE frames, incrementing the sequence id per frame" do
      # Mirrors the read-side three-frame case: a split loop capped at one
      # continuation would truncate a >32 MiB write SILENTLY.
      full = :binary.copy(<<0xAB>>, @max)
      remainder = :binary.copy(<<0xAB>>, 7)
      encoded = IO.iodata_to_binary(Packet.encode(full <> full <> remainder, 0))

      frame_0 = <<@max::24-little, 0::8, full::binary>>
      frame_1 = <<@max::24-little, 1::8, full::binary>>
      frame_2 = <<7::24-little, 2::8, remainder::binary>>

      assert encoded == frame_0 <> frame_1 <> frame_2
    end

    test "splits an over-length payload with incrementing sequence ids" do
      payload = :binary.copy(<<0xAB>>, @max + 3)
      encoded = IO.iodata_to_binary(Packet.encode(payload, 5))
      head = :binary.copy(<<0xAB>>, @max)
      tail = :binary.copy(<<0xAB>>, 3)

      assert encoded ==
               <<@max::24-little, 5::8, head::binary, 3::24-little, 6::8, tail::binary>>
    end

    test "the sequence id wraps at 255" do
      assert IO.iodata_to_binary(Packet.encode("x", 255)) == <<1::24-little, 255::8, "x">>

      payload = :binary.copy(<<0xAB>>, @max)
      encoded = IO.iodata_to_binary(Packet.encode(payload, 255))
      assert <<_::binary-size(4), _::binary-size(@max), 0::24-little, 0::8>> = encoded
    end
  end

  describe "send_packet/3 + read_packet/2 round trip" do
    test "a >16 MiB payload written by send_packet reassembles byte-identically" do
      payload = :binary.copy(<<0x5A>>, @max + 128)
      sock = packet_source([Packet.encode(payload, 4)])

      assert {5, ^payload} = Packet.read_packet(sock, 30_000)
    end

    test "send_packet writes the framed bytes to the socket" do
      {client, server} = socket_pair()
      assert :ok = Packet.send_packet(server, "hello", 2)
      assert {2, "hello"} = Packet.read_packet(client, 5_000)
    end
  end

  ## helpers

  # Serves `chunks` (raw bytes, already framed) to the returned client socket
  # from a separate process, then closes — so a deliberately short chunk list
  # surfaces as a truncated read rather than a hang.
  defp packet_source(chunks) do
    {:ok, listen} =
      :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true, ip: @loopback])

    {:ok, port} = :inet.port(listen)

    spawn_link(fn ->
      {:ok, server} = :gen_tcp.accept(listen, 5_000)
      Enum.each(chunks, fn chunk -> :ok = :gen_tcp.send(server, chunk) end)
      :gen_tcp.close(server)
      :gen_tcp.close(listen)
    end)

    {:ok, client} = :gen_tcp.connect(@loopback, port, [:binary, active: false], 5_000)
    on_exit(fn -> :gen_tcp.close(client) end)

    {:gen_tcp, client}
  end

  defp socket_pair do
    {:ok, listen} =
      :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true, ip: @loopback])

    {:ok, port} = :inet.port(listen)
    {:ok, client} = :gen_tcp.connect(@loopback, port, [:binary, active: false], 5_000)
    {:ok, server} = :gen_tcp.accept(listen, 5_000)
    :gen_tcp.close(listen)

    on_exit(fn ->
      :gen_tcp.close(client)
      :gen_tcp.close(server)
    end)

    {{:gen_tcp, client}, {:gen_tcp, server}}
  end
end

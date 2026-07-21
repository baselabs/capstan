defmodule Capstan.AssemblerServerTest.RecordingSink do
  @moduledoc false
  # A test sink recording every callback to a configured `report_to` pid and returning
  # a configured result. Config lives in `:persistent_term` (the tests are `async: false`,
  # so there is exactly one live config); the sink runs inside the server process, so a
  # pid-carrying callback argument is not available.
  @behaviour Capstan.Sink

  @key {__MODULE__, :config}

  def configure(config), do: :persistent_term.put(@key, config)
  def clear, do: :persistent_term.erase(@key)
  defp config, do: :persistent_term.get(@key)

  @impl Capstan.Sink
  def handle_transaction(transaction) do
    cfg = config()
    send(cfg.report_to, {:handle_transaction, transaction})
    cfg.on_transaction.(transaction)
  end

  @impl Capstan.Sink
  def handle_schema_change(schema_change, position) do
    cfg = config()
    send(cfg.report_to, {:handle_schema_change, schema_change, position})
    cfg.on_schema_change.(schema_change, position)
  end
end

defmodule Capstan.AssemblerServerTest.FailingStore do
  @moduledoc false
  # A checkpoint store whose write always fails transiently, counting attempts so a test
  # can assert the retry budget is actually consulted (not "halt now", not unbounded).
  @behaviour Capstan.CheckpointStore

  def start_link, do: Agent.start_link(fn -> 0 end)
  def write_count(agent), do: Agent.get(agent, & &1)

  @impl Capstan.CheckpointStore
  def read(_agent), do: {:ok, nil}

  @impl Capstan.CheckpointStore
  def write(agent, gtid_set) when is_binary(gtid_set) do
    Agent.update(agent, &(&1 + 1))
    {:error, :store_unavailable}
  end
end

defmodule Capstan.AssemblerServerTest.ReadFailStore do
  @moduledoc false
  # A checkpoint store whose READ fails: the server must fail closed at startup rather
  # than resume from a guessed position.
  @behaviour Capstan.CheckpointStore

  @impl Capstan.CheckpointStore
  def read(_store), do: {:error, :store_down}

  @impl Capstan.CheckpointStore
  def write(_store, _gtid_set), do: :ok
end

defmodule Capstan.AssemblerServerTest do
  use ExUnit.Case, async: false

  alias Capstan.{
    AssemblerServer,
    Change,
    CheckpointStore,
    Gtid,
    Position,
    SchemaChange,
    Transaction
  }

  alias Capstan.AssemblerServerTest.{FailingStore, ReadFailStore, RecordingSink}
  alias Capstan.Binlog.{Decoder, Event}

  @in_memory CheckpointStore.InMemory
  @fixtures_root Path.join([__DIR__, "..", "fixtures", "binlog"])

  # The first (XID-terminated) transaction of the simple_dml fixture: an INSERT into
  # probe_db.widgets.
  @dml_txn [
    "01-rotate.bin",
    "02-format_description.bin",
    "03-previous_gtids.bin",
    "04-heartbeat.bin",
    "05-gtid.bin",
    "06-query.bin",
    "07-table_map.bin",
    "08-write_rows.bin",
    "09-xid.bin"
  ]

  # The alter_ddl fixture: GTID -> ALTER (no BEGIN, no XID) -> self-committing DDL.
  @ddl_txn [
    "01-rotate.bin",
    "02-format_description.bin",
    "03-previous_gtids.bin",
    "04-heartbeat.bin",
    "05-gtid.bin",
    "06-query.bin"
  ]

  # A fixed 16-byte source UUID for the synthesised empty-transaction stream (F10).
  @sid <<0x3E, 0x11, 0xFA, 0x47, 0x71, 0xCA, 0x11, 0xE1, 0x9E, 0x33, 0xC8, 0x0A, 0xA9, 0x42, 0x95,
         0x62>>
  @sid_uuid "3e11fa47-71ca-11e1-9e33-c80aa9429562"

  ## ---------------------------------------------------------------------------
  ## delivery + checkpoint-after-{:ok, _} (lib-owned mode)
  ## ---------------------------------------------------------------------------

  describe "delivery + checkpoint — written ONLY after handle_transaction/1 -> {:ok, _}" do
    test "a delivered transaction advances the checkpoint to txn.position after {:ok, _}" do
      {:ok, store} = @in_memory.start_link()
      configure_sink()
      attach_telemetry([[:capstan, :transaction, :committed]])
      server = start_server(store: store)

      {uuid, gno} = gtid_of("simple_dml", "05-gtid.bin")
      feed(server, raws("simple_dml", @dml_txn))

      assert_receive {:handle_transaction, %Transaction{} = txn}, 2000
      assert txn.gtid == "#{uuid}:#{gno}"
      assert [%Change{op: :insert, table: "widgets"}] = txn.changes

      # The committed telemetry fires AFTER the checkpoint write — a reliable barrier.
      gtid = txn.gtid
      assert_receive {:telemetry, [:capstan, :transaction, :committed], %{}, %{gtid: ^gtid}}, 2000

      assert {:ok, %Position{gtid_set: set}} = read_checkpoint(store)
      assert Gtid.member?(Gtid.parse(set), {uuid, gno})
    end

    test "a sink {:error, _} HALTS the pipeline and does NOT advance the checkpoint" do
      # RED-before-green for the checkpoint-after-{:ok, _} ORDERING: a tamper that writes
      # the checkpoint before / regardless of the sink result leaves the store non-nil
      # here. The failed txn is re-delivered on restart (at-least-once).
      {:ok, store} = @in_memory.start_link()
      configure_sink(on_transaction: fn _txn -> {:error, :sink_boom} end)
      server = start_server(store: store)
      ref = Process.monitor(server)

      feed(server, raws("simple_dml", @dml_txn))

      assert_receive {:handle_transaction, %Transaction{}}, 2000

      assert_receive {:DOWN, ^ref, :process, ^server,
                      {:shutdown, {:halt, {:sink_error, :sink_boom}}}},
                     2000

      assert {:ok, nil} = read_checkpoint(store)
    end
  end

  ## ---------------------------------------------------------------------------
  ## dedup — skip an already-processed GTID via Gtid.member?/2
  ## ---------------------------------------------------------------------------

  describe "dedup — an already-processed GTID is SKIPPED, the sink is not called" do
    test "a duplicate transaction emits :already_processed and never reaches the sink" do
      # RED-before-green: a no-dedup impl re-delivers the duplicate (calls the sink and
      # emits :committed instead of :skipped).
      {uuid, gno} = gtid_of("simple_dml", "05-gtid.bin")
      {:ok, store} = @in_memory.start_link()
      # Seed the durable checkpoint so this GTID is already processed on resume.
      :ok = write_checkpoint(store, %Position{gtid_set: "#{uuid}:#{gno}"})
      configure_sink()
      attach_telemetry([[:capstan, :transaction, :skipped]])
      server = start_server(store: store)

      feed(server, raws("simple_dml", @dml_txn))

      assert_receive {:telemetry, [:capstan, :transaction, :skipped], %{},
                      %{gtid: gtid, reason: :already_processed}},
                     2000

      assert gtid == "#{uuid}:#{gno}"
      refute_receive {:handle_transaction, _}, 300

      assert {:ok, %Position{gtid_set: set}} = read_checkpoint(store)
      assert set == "#{uuid}:#{gno}"
    end
  end

  ## ---------------------------------------------------------------------------
  ## filtered / empty transactions advance the watermark with no sink call (Q14/F10)
  ## ---------------------------------------------------------------------------

  describe "filtered transaction — advances the checkpoint with NO sink call (Q14)" do
    test "a fully-filtered transaction advances the watermark and never calls the sink" do
      # RED-before-green: an impl that only checkpoints DELIVERED transactions leaves the
      # watermark unmoved here (and emits no :filtered).
      {uuid, gno} = gtid_of("simple_dml", "05-gtid.bin")
      {:ok, store} = @in_memory.start_link()
      configure_sink()
      attach_telemetry([[:capstan, :transaction, :filtered]])
      # Allowlist a table the fixture never touches -> every row is filtered before decode.
      server = start_server(store: store, tables: [{"probe_db", "some_other_table"}])

      feed(server, raws("simple_dml", @dml_txn))

      assert_receive {:telemetry, [:capstan, :transaction, :filtered], %{}, %{gtid: gtid}}, 2000
      assert gtid == "#{uuid}:#{gno}"
      refute_receive {:handle_transaction, _}, 300

      assert {:ok, %Position{gtid_set: set}} = read_checkpoint(store)
      assert Gtid.member?(Gtid.parse(set), {uuid, gno})
    end

    test "a long run of filtered/empty transactions keeps advancing — no stall (F10)" do
      # RED-before-green: an impl that stalls on a filtered quiet period never advances the
      # watermark to the full range. Distinct GNOs 1..n prove the position keeps MOVING.
      n = 50
      {:ok, store} = @in_memory.start_link()
      configure_sink()
      attach_telemetry([[:capstan, :transaction, :filtered]])
      server = start_server(store: store)

      # Each empty transaction (GTID -> XID, no rows) has empty changes.
      for i <- 1..n, do: feed(server, [gtid_raw(i), xid_raw(i)])

      for i <- 1..n do
        expected = "#{@sid_uuid}:#{i}"

        assert_receive {:telemetry, [:capstan, :transaction, :filtered], %{}, %{gtid: ^expected}},
                       2000
      end

      refute_receive {:handle_transaction, _}, 200

      # The persisted watermark reached the full contiguous range: no stall, and it stays a
      # single coalesced interval (the design's compactness tripwire).
      assert {:ok, %Position{gtid_set: set}} = read_checkpoint(store)
      assert set == "#{@sid_uuid}:1-#{n}"
    end
  end

  ## ---------------------------------------------------------------------------
  ## halt dispatch — fail closed, never checkpoint past a halt
  ## ---------------------------------------------------------------------------

  describe "halt dispatch — fail closed without advancing the checkpoint" do
    test "a {:capstan_halt, reason} from the Connection stops the pipeline, no checkpoint" do
      {:ok, store} = @in_memory.start_link()
      configure_sink()
      server = start_server(store: store)
      ref = Process.monitor(server)

      send(server, {:capstan_halt, :data_gap})

      assert_receive {:DOWN, ^ref, :process, ^server, {:shutdown, {:halt, :data_gap}}}, 2000
      assert {:ok, nil} = read_checkpoint(store)
    end

    test "an Assembler {:halt, _} (XA_PREPARE) stops fail-closed without checkpointing" do
      {:ok, store} = @in_memory.start_link()
      configure_sink()
      server = start_server(store: store)
      ref = Process.monitor(server)

      send(server, {:binlog_event, xa_prepare_raw()})

      assert_receive {:DOWN, ^ref, :process, ^server,
                      {:shutdown, {:halt, :unsupported_transaction_shape}}},
                     2000

      assert {:ok, nil} = read_checkpoint(store)
    end

    test "an Assembler {:error, _} (unknown event type) stops fail-closed" do
      {:ok, store} = @in_memory.start_link()
      configure_sink()
      server = start_server(store: store)
      ref = Process.monitor(server)

      send(server, {:binlog_event, unknown_raw()})

      assert_receive {:DOWN, ^ref, :process, ^server,
                      {:shutdown, {:halt, {:assembler_error, {:unknown_event_type, 99}}}}},
                     2000

      assert {:ok, nil} = read_checkpoint(store)
    end

    test "a CRC-mismatched event stops fail-closed (integrity failure never skipped)" do
      {:ok, store} = @in_memory.start_link()
      configure_sink()
      server = start_server(store: store)
      ref = Process.monitor(server)

      send(server, {:binlog_event, crc_mismatch_raw()})

      assert_receive {:DOWN, ^ref, :process, ^server,
                      {:shutdown, {:halt, {:event_parse_failed, :crc_mismatch}}}},
                     2000

      assert {:ok, nil} = read_checkpoint(store)
    end
  end

  ## ---------------------------------------------------------------------------
  ## schema change — self-committing DDL advances the checkpoint (Q13)
  ## ---------------------------------------------------------------------------

  describe "schema change — handle_schema_change/2 advances the checkpoint (Q13)" do
    test "a SchemaChange is delivered and advances the checkpoint" do
      # RED-before-green: an impl that does not checkpoint DDL leaves the watermark unmoved.
      {uuid, gno} = gtid_of("alter_ddl", "05-gtid.bin")
      {:ok, store} = @in_memory.start_link()
      configure_sink()
      attach_telemetry([[:capstan, :schema_change, :received]])
      server = start_server(store: store)

      feed(server, raws("alter_ddl", @ddl_txn))

      assert_receive {:handle_schema_change,
                      %SchemaChange{schema: "probe_db", table: "widgets_ddl", kind: :alter_table} =
                        sc, %Position{} = pos},
                     2000

      assert sc.gtid == "#{uuid}:#{gno}"
      assert Gtid.member?(Gtid.parse(pos.gtid_set), {uuid, gno})

      assert_receive {:telemetry, [:capstan, :schema_change, :received], %{},
                      %{schema: "probe_db", table: "widgets_ddl", kind: :alter_table}},
                     2000

      assert {:ok, %Position{gtid_set: set}} = read_checkpoint(store)
      assert Gtid.member?(Gtid.parse(set), {uuid, gno})
    end

    test "a handle_schema_change/2 {:error, _} halts fail-closed without advancing" do
      {:ok, store} = @in_memory.start_link()
      configure_sink(on_schema_change: fn _sc, _pos -> {:error, :ddl_boom} end)
      server = start_server(store: store)
      ref = Process.monitor(server)

      feed(server, raws("alter_ddl", @ddl_txn))

      assert_receive {:handle_schema_change, %SchemaChange{}, %Position{}}, 2000

      assert_receive {:DOWN, ^ref, :process, ^server,
                      {:shutdown, {:halt, {:sink_error, :ddl_boom}}}},
                     2000

      assert {:ok, nil} = read_checkpoint(store)
    end
  end

  ## ---------------------------------------------------------------------------
  ## checkpoint-write faults — retry per the budget, then halt fail-closed
  ## ---------------------------------------------------------------------------

  describe "checkpoint write fault — retried per the budget, then halts" do
    test "a transient write fault is retried max_retries times, then halts fail-closed" do
      {:ok, counter} = FailingStore.start_link()
      configure_sink()
      max = 2

      {:ok, server} =
        GenServer.start(AssemblerServer,
          sink: RecordingSink,
          checkpoint_store: {FailingStore, counter},
          max_retries: max
        )

      on_exit(fn -> if Process.alive?(server), do: safe_stop(server) end)
      ref = Process.monitor(server)

      feed(server, raws("simple_dml", @dml_txn))

      assert_receive {:handle_transaction, %Transaction{}}, 2000

      assert_receive {:DOWN, ^ref, :process, ^server,
                      {:shutdown, {:halt, {:checkpoint_write_failed, :store_unavailable}}}},
                     2000

      # The budget was actually consulted: max_retries + 1 write attempts (not 1 = halt-now,
      # not unbounded).
      assert FailingStore.write_count(counter) == max + 1
    end

    test "a checkpoint READ fault at startup fails closed (never resumes from a guess)" do
      configure_sink()

      assert {:error, {:shutdown, {:halt, {:checkpoint_read_failed, :store_down}}}} =
               GenServer.start(AssemblerServer,
                 sink: RecordingSink,
                 checkpoint_store: {ReadFailStore, :ignored}
               )
    end
  end

  ## ---------------------------------------------------------------------------
  ## helpers
  ## ---------------------------------------------------------------------------

  defp start_server(opts) do
    store = Keyword.fetch!(opts, :store)

    server_opts = [
      sink: RecordingSink,
      checkpoint_store: {@in_memory, store},
      tables: Keyword.get(opts, :tables, :all)
    ]

    {:ok, pid} = GenServer.start(AssemblerServer, server_opts)
    on_exit(fn -> if Process.alive?(pid), do: safe_stop(pid) end)
    pid
  end

  defp safe_stop(pid) do
    GenServer.stop(pid, :normal, 1000)
  catch
    _, _ -> :ok
  end

  defp configure_sink(overrides \\ []) do
    base = %{
      report_to: self(),
      on_transaction: fn txn -> {:ok, txn.position} end,
      on_schema_change: fn _sc, _pos -> :ok end
    }

    config = Enum.reduce(overrides, base, fn {k, v}, acc -> Map.put(acc, k, v) end)
    RecordingSink.configure(config)
    on_exit(&RecordingSink.clear/0)
  end

  # A module-function handler (not a local capture) keeps :telemetry from logging a
  # per-attach performance warning into the test output. The test pid rides in `config`.
  def forward_telemetry(event, measurements, metadata, %{test: test}) do
    send(test, {:telemetry, event, measurements, metadata})
  end

  defp attach_telemetry(events) do
    handler_id = {__MODULE__, make_ref()}

    :telemetry.attach_many(
      handler_id,
      events,
      &__MODULE__.forward_telemetry/4,
      %{test: self()}
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  defp read_checkpoint(store), do: CheckpointStore.read_position(@in_memory, store)

  defp write_checkpoint(store, %Position{} = position),
    do: CheckpointStore.write_position(@in_memory, store, position)

  defp feed(server, raws), do: Enum.each(raws, &send(server, {:binlog_event, &1}))

  defp raw!(scenario, filename),
    do: File.read!(Path.join([@fixtures_root, scenario, filename]))

  defp raws(scenario, filenames), do: Enum.map(filenames, &raw!(scenario, &1))

  defp gtid_of(scenario, filename) do
    {:ok, event} = Event.parse(raw!(scenario, filename))
    {:ok, {:gtid, {uuid, gno}}} = Decoder.decode(event)
    {uuid, gno}
  end

  # Builds a valid raw binlog event (19-byte header + body + CRC32 over header+body),
  # exactly what Capstan.Binlog.Event.parse/1 consumes.
  defp raw_event(type, body, log_pos) do
    payload =
      <<0::32-little, type::8, 1::32-little, byte_size(body) + 23::32-little, log_pos::32-little,
        0::16-little, body::binary>>

    <<payload::binary, :erlang.crc32(payload)::32-little>>
  end

  # A GTID event body: flags(1) + source-id(16) + gno(8, signed) + (ignored rest).
  defp gtid_raw(gno) do
    sid = @sid
    raw_event(33, <<0::8, sid::binary-size(16), gno::64-little-signed>>, gno)
  end

  # An XID event body: the 8-byte transaction id.
  defp xid_raw(xid), do: raw_event(16, <<xid::64-little>>, xid)

  # An XA_PREPARE (type 38): the Decoder halts on the type byte alone.
  defp xa_prepare_raw, do: raw_event(38, <<>>, 0)

  # An unknown event type: the Decoder fails closed with {:unknown_event_type, 99}.
  defp unknown_raw, do: raw_event(99, <<0, 0, 0, 0>>, 0)

  # A well-formed-length event carrying a WRONG CRC trailer: Event.parse -> :crc_mismatch.
  defp crc_mismatch_raw do
    sid = @sid
    body = <<0::8, sid::binary-size(16), 1::64-little-signed>>

    payload =
      <<0::32-little, 33::8, 1::32-little, byte_size(body) + 23::32-little, 0::32-little,
        0::16-little, body::binary>>

    <<payload::binary, :erlang.crc32(payload) + 1::32-little>>
  end
end

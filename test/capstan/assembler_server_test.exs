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

  import ExUnit.CaptureLog

  alias Capstan.{
    AssemblerServer,
    Change,
    CheckpointStore,
    Error,
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
      # Measurements: the change count (computed on the concrete pre-delivery list, never
      # by consuming the sink's enumerable) and the monotonic sink-call duration.
      gtid = txn.gtid

      assert_receive {:telemetry, [:capstan, :transaction, :committed],
                      %{change_count: 1, sink_ms: sink_ms}, %{gtid: ^gtid}},
                     2000

      assert is_number(sink_ms) and sink_ms >= 0

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
  ## decode crash — a malformed event halts value-free, never leaks bytes (B1 / Rule 1)
  ## ---------------------------------------------------------------------------

  describe "decode crash — a CRC-valid but malformed event halts value-free (B1)" do
    test "halts {:event_decode_crashed, %Error{}} with NO byte leak, no checkpoint advance" do
      # A CRC-valid TABLE_MAP whose schema-length byte far exceeds the bytes that follow, so
      # the decoder's hard-match RAISES a FunctionClauseError (no matching `read_str8_z/1`
      # clause) whose args carry the raw body — including `sentinel`. WITHOUT the step_guarded
      # rescue this raise is uncaught: the exit reason is the exception (not a clean shutdown)
      # and the OTP crash report logs the embedded bytes (a Rule-1 breach). WITH the rescue the
      # halt is a value-free {:event_decode_crashed, %Capstan.Error{}} and the bytes reach no
      # channel.
      sentinel = "capstan_b1_decode_leak_sentinel_5d2f9a4c"
      {:ok, store} = @in_memory.start_link()
      configure_sink()
      attach_telemetry([[:capstan, :assembler, :halt]])
      server = start_server(store: store)
      ref = Process.monitor(server)

      log =
        capture_log(fn ->
          send(server, {:binlog_event, malformed_table_map_raw(sentinel)})

          assert_receive {:DOWN, ^ref, :process, ^server,
                          {:shutdown, {:halt, {:event_decode_crashed, %Error{}}}}},
                         2000
        end)

      # Rule 1 — the LEAK CHANNEL is decimal, not ASCII: if the raw bytes reached an OTP
      # crash report they render as a decimal byte list (`<<255, 99, 97, ...>>`), NEVER the
      # ASCII sentinel — so `String.contains?(log, sentinel)` alone is VACUOUS (it can never
      # match). Scan for the decimal encoding of a distinctive sentinel slice (the actual
      # channel), which IS present in the unguarded crash report and absent once the rescue
      # turns the crash into a clean shutdown-halt with no report.
      decimal_leak =
        sentinel |> binary_part(0, 16) |> :binary.bin_to_list() |> Enum.join(", ")

      refute String.contains?(log, decimal_leak),
             "Rule 1 violation: decode-crash bytes leaked (decimal-encoded) into a log line:\n#{log}"

      refute String.contains?(log, sentinel),
             "Rule 1 violation: decode-crash sentinel leaked (ASCII) into a log line:\n#{log}"

      # The halt telemetry is value-free — a bare reason atom, the compound reason's payload
      # (which held the bytes) scrubbed away.
      assert_received {:telemetry, [:capstan, :assembler, :halt], %{},
                       %{reason: :event_decode_crashed} = meta}

      refute String.contains?(inspect(meta), sentinel)

      # Fail-closed: the checkpoint never advanced past the crash.
      assert {:ok, nil} = read_checkpoint(store)
    end

    test "a sink that RAISES on delivery halts value-free, no crash-report leak (F2)" do
      # A sink that raises (instead of returning `{:error, _}`) must NOT crash the server: a
      # crash would put BOTH the raise message AND the in-flight `{:binlog_event, raw}`
      # message (the raw event bytes) into the OTP crash report (Rule 1). The handle_info
      # rescue turns it into a value-free {:event_processing_crashed, %Error{}} halt with no
      # report at all. Non-vacuity: without the rescue the exit reason is the RuntimeError
      # (not a shutdown-halt) and the crash report carries the sentinel + "Last message".
      sentinel = "capstan_f2_sink_raise_sentinel_8e1b3d"
      {:ok, store} = @in_memory.start_link()
      configure_sink(on_transaction: fn _txn -> raise "sink exploded: #{sentinel}" end)
      server = start_server(store: store)
      ref = Process.monitor(server)

      log =
        capture_log(fn ->
          feed(server, raws("simple_dml", @dml_txn))

          assert_receive {:DOWN, ^ref, :process, ^server,
                          {:shutdown, {:halt, {:event_processing_crashed, %Error{}}}}},
                         2000
        end)

      # No crash report fired (a clean shutdown-halt), so neither the raise message nor the
      # in-flight raw event bytes reached the log.
      refute String.contains?(log, sentinel),
             "Rule 1 violation: sink-raise message leaked into a log line:\n#{log}"

      refute String.contains?(log, "Last message"),
             "a GenServer crash report fired (leaking the in-flight raw event bytes):\n#{log}"

      assert {:ok, nil} = read_checkpoint(store)
    end
  end

  ## ---------------------------------------------------------------------------
  ## assembler halt telemetry — every AssemblerServer-detected halt is observable (S3)
  ## ---------------------------------------------------------------------------

  describe "assembler halt telemetry — detected halts emit [:capstan, :assembler, :halt] (S3)" do
    test "a sink {:error, _} halt emits assembler-halt telemetry with a value-free reason" do
      # Non-vacuity: the reason is scrubbed to the OUTER atom (`:sink_error`), NOT the raw
      # compound `{:sink_error, :sink_boom}` — a leak-past-the-allowlist would carry the
      # payload through here.
      {:ok, store} = @in_memory.start_link()
      configure_sink(on_transaction: fn _txn -> {:error, :sink_boom} end)
      attach_telemetry([[:capstan, :assembler, :halt]])
      server = start_server(store: store)
      ref = Process.monitor(server)

      feed(server, raws("simple_dml", @dml_txn))

      assert_receive {:DOWN, ^ref, :process, ^server,
                      {:shutdown, {:halt, {:sink_error, :sink_boom}}}},
                     2000

      assert_received {:telemetry, [:capstan, :assembler, :halt], %{}, %{reason: :sink_error}}
    end

    test "a propagated {:capstan_halt, _} does NOT double-emit assembler-halt telemetry" do
      # The Connection already surfaced this halt via its own connection.halt payload; the
      # AssemblerServer stops via stop_halt/2 (no emit) so the same halt is not double-reported.
      {:ok, store} = @in_memory.start_link()
      configure_sink()
      attach_telemetry([[:capstan, :assembler, :halt]])
      server = start_server(store: store)
      ref = Process.monitor(server)

      send(server, {:capstan_halt, :data_gap})

      assert_receive {:DOWN, ^ref, :process, ^server, {:shutdown, {:halt, :data_gap}}}, 2000
      refute_received {:telemetry, [:capstan, :assembler, :halt], _measurements, _meta}
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
  ## C2 spine hook (Ch6) — :watermark_observer fires on EVERY advance; byte-identical
  ## when absent. Task 7 (a)/(b).
  ## ---------------------------------------------------------------------------

  describe ":watermark_observer — byte-identical when absent (Task 7 (a))" do
    test "a DELIVERED advance sends NO watermark when no observer is configured" do
      {:ok, store} = @in_memory.start_link()
      configure_sink()
      server = start_server(store: store)

      feed(server, raws("simple_dml", @dml_txn))

      # The delivery happened (so an advance happened) — yet no observer notify fires.
      assert_receive {:handle_transaction, %Transaction{}}, 2000
      refute_receive {:capstan_watermark, _}, 300
    end

    test "a FILTERED (changes: []) advance sends NO watermark when no observer is configured" do
      {:ok, store} = @in_memory.start_link()
      configure_sink()
      attach_telemetry([[:capstan, :transaction, :filtered]])
      server = start_server(store: store, tables: [{"probe_db", "some_other_table"}])

      feed(server, raws("simple_dml", @dml_txn))

      # The filtered advance happened (telemetry proves it) — yet no observer notify fires.
      assert_receive {:telemetry, [:capstan, :transaction, :filtered], %{}, %{gtid: _}}, 2000
      refute_receive {:capstan_watermark, _}, 300
    end

    test "a self-committing DDL advance sends NO watermark when no observer is configured" do
      {:ok, store} = @in_memory.start_link()
      configure_sink()
      server = start_server(store: store)

      feed(server, raws("alter_ddl", @ddl_txn))

      assert_receive {:handle_schema_change, %SchemaChange{}, %Position{}}, 2000
      refute_receive {:capstan_watermark, _}, 300
    end
  end

  describe ":watermark_observer — fires on EVERY advance path (Ch6, Task 7 (b), tripwire 10)" do
    test "notifies on a DELIVERED advance; only the canonical GTID STRING rides (Rule 1)" do
      {:ok, store} = @in_memory.start_link()
      configure_sink()
      {uuid, gno} = gtid_of("simple_dml", "05-gtid.bin")
      server = start_server(store: store, watermark_observer: self())

      feed(server, raws("simple_dml", @dml_txn))

      assert_receive {:capstan_watermark, set}, 2000
      assert is_binary(set)
      assert Gtid.member?(Gtid.parse(set), {uuid, gno})
      # Rule 1: the payload is the canonical GTID-set STRING (uuid:range), never a row value
      # from the INSERTed widgets record.
      assert set =~ ~r/^[0-9a-fA-F-]+:[0-9:-]+$/
    end

    test "notifies on a FILTERED (changes: []) advance — tripwire 10: hooking only delivered omits this" do
      # RED under the tripwire-10 mutation (notify only in the delivered dispatch): a fully
      # filtered transaction advances the watermark with NO sink call, so a delivered-only hook
      # never fires here → the advance gate would stall.
      {:ok, store} = @in_memory.start_link()
      configure_sink()
      {uuid, gno} = gtid_of("simple_dml", "05-gtid.bin")

      server =
        start_server(
          store: store,
          tables: [{"probe_db", "some_other_table"}],
          watermark_observer: self()
        )

      feed(server, raws("simple_dml", @dml_txn))

      assert_receive {:capstan_watermark, set}, 2000
      assert Gtid.member?(Gtid.parse(set), {uuid, gno})
      refute_receive {:handle_transaction, _}, 200
    end

    test "notifies on a self-committing DDL advance — tripwire 10: hooking only delivered omits this" do
      # RED under the tripwire-10 mutation: a DDL advances via handle_schema_change/2, not
      # handle_transaction/1, so a delivered-only hook never fires → the advance gate stalls
      # behind a DDL on a non-snapshot table.
      {:ok, store} = @in_memory.start_link()
      configure_sink()
      {uuid, gno} = gtid_of("alter_ddl", "05-gtid.bin")
      server = start_server(store: store, watermark_observer: self())

      feed(server, raws("alter_ddl", @ddl_txn))

      assert_receive {:handle_schema_change, %SchemaChange{}, %Position{}}, 2000
      assert_receive {:capstan_watermark, set}, 2000
      assert Gtid.member?(Gtid.parse(set), {uuid, gno})
    end
  end

  ## ---------------------------------------------------------------------------
  ## C2 spine hook — coordinator monitor (:snapshot_coordinator_down). Mirrors the
  ## connection.ex :receiver_down monitor. Task 7 (c)/(d).
  ## ---------------------------------------------------------------------------

  describe "coordinator monitor — a silent coordinator death halts loud (Task 7 (c))" do
    test "attach_coordinator/2 arms a monitor; a silent death halts :snapshot_coordinator_down" do
      # RED with no monitor: the coordinator's death is unobserved, the AssemblerServer stays
      # up and would stream into a dead sink → a silently stranded backfill (a gap).
      {:ok, store} = @in_memory.start_link()
      configure_sink()
      attach_telemetry([[:capstan, :assembler, :halt]])
      server = start_server(store: store)

      coordinator = spawn(fn -> Process.sleep(:infinity) end)
      :ok = AssemblerServer.attach_coordinator(server, coordinator)
      # Barrier: force the cast that arms Process.monitor to be processed BEFORE the kill,
      # so there is no monitor-vs-death race.
      _ = :sys.get_state(server)

      ref = Process.monitor(server)
      # A SILENT death (Process.exit/:kill is untrappable) — NOT a {:capstan_halt, _} message.
      Process.exit(coordinator, :kill)

      assert_receive {:DOWN, ^ref, :process, ^server,
                      {:shutdown, {:halt, :snapshot_coordinator_down}}},
                     2000

      # The EMITTING halt/2 was used (visible to monitoring), not the silent stop_halt/2.
      assert_received {:telemetry, [:capstan, :assembler, :halt], %{},
                       %{reason: :snapshot_coordinator_down}}

      # Fail-closed: no delivery occurred, so the checkpoint never advanced.
      assert {:ok, nil} = read_checkpoint(store)
    end
  end

  describe "coordinator monitor — no coordinator attached ⇒ no monitor armed (Task 7 (d))" do
    test "a spurious {:DOWN} for an un-armed ref is a passthrough — the server keeps running" do
      {:ok, store} = @in_memory.start_link()
      configure_sink()
      {uuid, gno} = gtid_of("simple_dml", "05-gtid.bin")
      server = start_server(store: store)

      # A DOWN for a reference the server never monitored (no coordinator attached).
      send(server, {:DOWN, make_ref(), :process, self(), :noproc})

      # It did not halt: a subsequent transaction is still delivered normally.
      feed(server, raws("simple_dml", @dml_txn))
      assert_receive {:handle_transaction, %Transaction{} = txn}, 2000
      assert txn.gtid == "#{uuid}:#{gno}"
      assert Process.alive?(server)
    end
  end

  ## ---------------------------------------------------------------------------
  ## helpers
  ## ---------------------------------------------------------------------------

  defp start_server(opts) do
    store = Keyword.fetch!(opts, :store)

    # The `:watermark_observer` key is passed ONLY when the test sets it, so a test that
    # omits it exercises the byte-identical (option-absent) C1 path.
    observer_opts =
      case Keyword.get(opts, :watermark_observer) do
        nil -> []
        pid -> [watermark_observer: pid]
      end

    server_opts =
      [
        sink: RecordingSink,
        checkpoint_store: {@in_memory, store},
        tables: Keyword.get(opts, :tables, :all)
      ] ++ observer_opts

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

  # A CRC-VALID TABLE_MAP (type 19) whose body is structurally malformed: after the
  # table_id(6) + flags(2) header, the schema length byte claims 255 bytes but far fewer
  # follow, so decode_table_map's `read_str8_z` hard-match RAISES a FunctionClauseError (no
  # matching clause) whose args carry the raw remainder — including `sentinel`. Event.parse
  # succeeds (the CRC is valid), so the raise happens inside the decode, exactly the B1
  # decode-crash path.
  defp malformed_table_map_raw(sentinel) do
    body = <<1::48-little, 0::16-little, 255::8, sentinel::binary>>
    raw_event(19, body, 0)
  end

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

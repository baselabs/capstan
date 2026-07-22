defmodule Capstan.Snapshot.CoordinatorTest.StubReader do
  @moduledoc false
  # A stubbed `Capstan.Snapshot.ChunkReader` whose behaviour is scripted IN the reader handle:
  # the handle is `%{script: [step, ...]}` and each `read_chunk/2` pops the head. This lets a
  # unit test drive the coordinator's advance gate without a live MySQL query connection while
  # the REAL `CursorGate`/`PrimaryKey`/`SnapshotStore.InMemory` do their real work.
  #
  #   * `{:chunk, %Chunk{}, final?}` -> `{:ok, chunk, final?, next_reader}`
  #   * `:done`                       -> `{:done, next_reader}`
  #   * `{:error, reason}`            -> `{:error, reason}`
  def read_chunk(%{script: [step | rest]}, _cursor) do
    next = %{script: rest}

    case step do
      {:chunk, chunk, final?} -> {:ok, chunk, final?, next}
      :done -> {:done, next}
      {:error, reason} -> {:error, reason}
    end
  end

  def read_chunk(%{script: []} = reader, _cursor), do: {:done, reader}
end

defmodule Capstan.Snapshot.CoordinatorTest.RecordingSink do
  @moduledoc false
  # The REAL (downstream) sink the coordinator forwards to / emits chunks through. Records each
  # callback to a `report_to` pid and returns a configured result. Config lives in
  # `:persistent_term` (the tests are `async: false`, so there is exactly one live config), and
  # the sink runs inside the coordinator process, so a pid-carrying argument is not available.
  @behaviour Capstan.Sink

  @key {__MODULE__, :config}

  def configure(config), do: :persistent_term.put(@key, Map.new(config))
  def clear, do: :persistent_term.erase(@key)
  defp config, do: :persistent_term.get(@key)

  @impl Capstan.Sink
  def handle_transaction(txn) do
    cfg = config()
    send(cfg.report_to, {:handle_transaction, txn})
    Map.get(cfg, :on_transaction, fn t -> {:ok, t.position} end).(txn)
  end

  @impl Capstan.Sink
  def handle_schema_change(schema_change, position) do
    cfg = config()
    send(cfg.report_to, {:handle_schema_change, schema_change, position})
    Map.get(cfg, :on_schema_change, fn _sc, _pos -> :ok end).(schema_change, position)
  end

  @impl Capstan.Sink
  def handle_snapshot(changes, meta) do
    cfg = config()
    send(cfg.report_to, {:handle_snapshot, changes, meta})
    Map.get(cfg, :on_snapshot, fn _changes, _meta -> :ok end).(changes, meta)
  end
end

defmodule Capstan.Snapshot.CoordinatorTest.ReportingStore do
  @moduledoc false
  # A `Capstan.SnapshotStore` that REPORTS every `write/2` to a pid before persisting — the
  # emit-before-persist ordering proof (tripwire 16). Backed by an `Agent`.
  @behaviour Capstan.SnapshotStore

  def start_link(report_to), do: Agent.start_link(fn -> %{report_to: report_to, state: nil} end)

  @impl Capstan.SnapshotStore
  def read(agent), do: {:ok, Agent.get(agent, & &1.state)}

  @impl Capstan.SnapshotStore
  def write(agent, state) do
    send(Agent.get(agent, & &1.report_to), {:store_write, state})
    Agent.update(agent, &%{&1 | state: state})
    :ok
  end
end

defmodule Capstan.Snapshot.CoordinatorTest.GatedStore do
  @moduledoc false
  # A `Capstan.SnapshotStore` whose `write/2` BLOCKS after announcing itself — so a test can
  # kill the coordinator in the exact window between the sink's `handle_snapshot` `{:ok}` and
  # the durable cursor persist (tripwire 16, the at-least-once crash boundary). Because the
  # write is killed before it reaches `Agent.update`, the durable state stays un-advanced.
  @behaviour Capstan.SnapshotStore

  def start_link(gate), do: Agent.start_link(fn -> %{gate: gate, state: nil} end)
  def durable(agent), do: Agent.get(agent, & &1.state)

  @impl Capstan.SnapshotStore
  def read(agent), do: {:ok, Agent.get(agent, & &1.state)}

  @impl Capstan.SnapshotStore
  def write(agent, state) do
    send(Agent.get(agent, & &1.gate), {:write_reached, self()})

    receive do
      :proceed ->
        Agent.update(agent, &%{&1 | state: state})
        :ok
    after
      5000 -> :ok
    end
  end
end

defmodule Capstan.Snapshot.CoordinatorTest.CountingFaultyStore do
  @moduledoc false
  # A `Capstan.SnapshotStore` whose `write/2` fails a configured number of times (or `:always`),
  # COUNTING every attempt — the store-write RETRY-BUDGET proof (design § Preconditions: a store
  # fault is budgeted, then halts). `fail_until: n` fails attempts 1..n then succeeds; `:always`
  # never succeeds. Backed by an `Agent`.
  @behaviour Capstan.SnapshotStore

  def start_link(opts),
    do: Agent.start_link(fn -> Map.merge(%{count: 0, state: nil}, Map.new(opts)) end)

  def count(agent), do: Agent.get(agent, & &1.count)

  @impl Capstan.SnapshotStore
  def read(agent), do: {:ok, Agent.get(agent, & &1.state)}

  @impl Capstan.SnapshotStore
  def write(agent, state) do
    n = Agent.get_and_update(agent, fn s -> {s.count + 1, %{s | count: s.count + 1}} end)
    fail_until = Agent.get(agent, & &1.fail_until)

    if fail_until == :always or n <= fail_until do
      {:error, Agent.get(agent, &Map.get(&1, :error_reason, :store_blip))}
    else
      Agent.update(agent, &%{&1 | state: state})
      :ok
    end
  end
end

defmodule Capstan.Snapshot.CoordinatorTest do
  @moduledoc """
  Task 8 (C2) — `Capstan.Snapshot.Coordinator`, the sink-interposing GenServer that runs the
  cursor-gate, buffers one chunk, runs the advance gate, emits via `handle_snapshot/2`, and
  propagates fail-closed halts. Unit tests with a STUBBED `ChunkReader` + REAL
  `CursorGate`/`PrimaryKey`/`SnapshotStore.InMemory`, pinning the named tripwires.
  """
  use ExUnit.Case, async: false

  alias Capstan.Change
  alias Capstan.Position
  alias Capstan.SchemaChange
  alias Capstan.SnapshotStore
  alias Capstan.Transaction

  alias Capstan.Snapshot.{Chunk, Coordinator, State}

  alias Capstan.Snapshot.CoordinatorTest.{
    CountingFaultyStore,
    GatedStore,
    RecordingSink,
    ReportingStore,
    StubReader
  }

  @uuid "3e11fa47-71ca-11e1-9e33-c80aa9429562"
  @p0 "#{@uuid}:1-100"
  @in_memory SnapshotStore.InMemory

  setup do
    RecordingSink.configure(report_to: self())
    on_exit(&RecordingSink.clear/0)
    :ok
  end

  ## ===========================================================================
  ## handle_transaction/1 — the cursor-gate
  ## ===========================================================================

  describe "handle_transaction/1 — the cursor-gate (tripwire 4)" do
    test "forwards ONLY the surviving subset (k <= cursor) to the real sink" do
      _coord = gate_coordinator(10)
      t = txn([insert(5), insert(15), delete(8), delete(20)])

      # 5,8 <= 10 forward; 15,20 > 10 suppress.
      assert Coordinator.handle_transaction(t) == {:ok, t.position}
      assert_receive {:handle_transaction, forwarded}
      assert forwarded.changes == [insert(5), delete(8)]
      assert forwarded.position == t.position
      assert forwarded.gtid == t.gtid
    end

    test "a fully-suppressed txn returns {:ok, position} WITHOUT calling the real sink (no deadlock)" do
      # Every change is > cursor, so the surviving subset is empty. The coordinator must NOT
      # call the real sink (a zero-change forward), and must still return the txn's position so
      # the AssemblerServer advances the watermark — otherwise a fully-suppressed run stalls the
      # advance gate forever (deadlock). RED: returning {:error, _} or an empty forward.
      _coord = gate_coordinator(10)
      t = txn([insert(15), insert(20), delete(30)])

      assert Coordinator.handle_transaction(t) == {:ok, t.position}
      refute_received {:handle_transaction, _}
    end

    test "DELETE during backfill (tripwire 9): a > cursor delete is suppressed; a <= cursor delete streams" do
      coord = gate_coordinator(10)

      # > cursor: suppressed (its <= G delete already removed the row from the chunk -> no phantom).
      assert Coordinator.handle_transaction(txn([delete(15)])) == {:ok, pos()}
      refute_received {:handle_transaction, _}

      # <= cursor: streamed (the row was in an already-emitted chunk; the stream now deletes it).
      assert Process.alive?(coord)
      t = txn([delete(5)])
      assert Coordinator.handle_transaction(t) == {:ok, t.position}
      assert_receive {:handle_transaction, forwarded}
      assert forwarded.changes == [delete(5)]
    end

    test "a change on a NON-snapshot table passes through unchanged" do
      _coord = gate_coordinator(10)
      foreign = foreign_insert(99_999)
      # foreign passes through; insert(15) is suppressed on the snapshot table.
      t = txn([foreign, insert(15)])

      assert Coordinator.handle_transaction(t) == {:ok, t.position}
      assert_receive {:handle_transaction, forwarded}
      assert forwarded.changes == [foreign]
    end

    test "a change on a DONE snapshot table passes through (the stream is authoritative)" do
      # cursor 5, done? true -> a change at id 9_999 (> cursor) is NOT suppressed.
      _coord =
        start_coordinator(store_backed(), snap_state(%{table() => int_table(5, true)}),
          readers: %{},
          table_order: []
        )

      big = insert(9_999)
      t = txn([big])
      assert Coordinator.handle_transaction(t) == {:ok, t.position}
      assert_receive {:handle_transaction, forwarded}
      assert forwarded.changes == [big]
    end
  end

  describe "handle_transaction/1 — PK-changing UPDATE straddle (tripwire 17)" do
    test "sub-case (a): k_old <= cursor < k_new -> forward delete(k_old), suppress upsert(k_new)" do
      _coord = gate_coordinator(10)
      t = txn([pk_update(5, 15)])

      assert Coordinator.handle_transaction(t) == {:ok, t.position}
      assert_receive {:handle_transaction, forwarded}

      assert forwarded.changes == [
               %Change{
                 op: :delete,
                 schema: "s",
                 table: "t",
                 record: nil,
                 old_record: %{"id" => 5}
               }
             ]
    end

    test "sub-case (b): k_new <= cursor < k_old -> suppress delete(k_old), forward upsert(k_new)" do
      _coord = gate_coordinator(10)
      t = txn([pk_update(15, 5)])

      assert Coordinator.handle_transaction(t) == {:ok, t.position}
      assert_receive {:handle_transaction, forwarded}

      assert forwarded.changes == [
               %Change{
                 op: :insert,
                 schema: "s",
                 table: "t",
                 record: %{"id" => 5},
                 old_record: nil
               }
             ]
    end
  end

  describe "the coordinator IS the AssemblerServer's registered sink" do
    test "name-registered so the module-sink dispatch (`state.sink.handle_transaction/1`) reaches it" do
      coord = gate_coordinator(10)
      assert Process.whereis(Coordinator) == coord

      # The exact call the AssemblerServer makes on its `state.sink` MODULE.
      t = txn([insert(5)])
      assert Coordinator.handle_transaction(t) == {:ok, t.position}
      assert_receive {:handle_transaction, _}
    end
  end

  ## ===========================================================================
  ## handle_schema_change/2
  ## ===========================================================================

  describe "handle_schema_change/2" do
    test "a DDL on a snapshot-ACTIVE table halts :snapshot_schema_drifted (loud + telemetry)" do
      attach_telemetry([[:capstan, :snapshot, :halt]])
      coord = gate_coordinator(10)
      ref = Process.monitor(coord)

      sc = %SchemaChange{schema: "s", table: "t", kind: :alter_table, gtid: "#{@uuid}:200"}
      assert Coordinator.handle_schema_change(sc, pos()) == {:error, :snapshot_schema_drifted}

      # Loud: the coordinator sends the SPECIFIC reason to the AssemblerServer (assembler=self()).
      assert_receive {:capstan_halt, :snapshot_schema_drifted}
      # And stops fail-closed with the pinned halt shape.
      assert_receive {:DOWN, ^ref, :process, ^coord,
                      {:shutdown, {:halt, :snapshot_schema_drifted}}}

      assert_receive {:telemetry, [:capstan, :snapshot, :halt], %{},
                      %{reason: :snapshot_schema_drifted}}
    end

    test "a DDL on a NON-snapshot table forwards to the real sink and does NOT halt" do
      coord = gate_coordinator(10)
      sc = %SchemaChange{schema: "other", table: "x", kind: :create_table, gtid: "#{@uuid}:201"}

      assert Coordinator.handle_schema_change(sc, pos()) == :ok
      assert_receive {:handle_schema_change, ^sc, _pos}
      assert Process.alive?(coord)
    end

    test "a DDL on a DONE snapshot table forwards (not a drift — the stream is authoritative)" do
      coord =
        start_coordinator(store_backed(), snap_state(%{table() => int_table(5, true)}),
          readers: %{},
          table_order: []
        )

      sc = %SchemaChange{schema: "s", table: "t", kind: :alter_table, gtid: "#{@uuid}:202"}
      assert Coordinator.handle_schema_change(sc, pos()) == :ok
      assert_receive {:handle_schema_change, ^sc, _pos}
      assert Process.alive?(coord)
    end

    test "a real-sink {:error, _} on a non-snapshot DDL is relayed (the AssemblerServer halts)" do
      _coord = gate_coordinator(10)

      RecordingSink.configure(
        report_to: self(),
        on_schema_change: fn _sc, _pos -> {:error, :boom} end
      )

      sc = %SchemaChange{schema: "other", table: "x", kind: :create_table, gtid: "#{@uuid}:203"}

      assert Coordinator.handle_schema_change(sc, pos()) == {:error, :boom}
    end
  end

  ## ===========================================================================
  ## the advance gate — buffer one chunk + G; emit only once processed_set covers G
  ## ===========================================================================

  describe "the advance gate (tripwire 5)" do
    test "buffers one chunk, emits via handle_snapshot only once advance?(G), advances the cursor + persists, at most one buffered" do
      {:ok, store} = @in_memory.start_link()
      chunk1 = chunk(0, "#{@uuid}:1-5", [%{"id" => 1}, %{"id" => 2}], 2)
      chunk2 = chunk(1, "#{@uuid}:1-8", [%{"id" => 3}], 3)

      coord =
        start_coordinator({@in_memory, store}, snap_state(%{table() => int_table(:start)}),
          readers: %{table() => %{script: [{:chunk, chunk1, false}, {:chunk, chunk2, true}]}}
        )

      # Bootstrap has read chunk1 but the empty processed_set does not cover G1 -> buffered, not emitted.
      assert %Chunk{seq: 0} = :sys.get_state(coord).buffered
      refute_received {:handle_snapshot, _, _}

      # A watermark covering G1 emits chunk1 (op: :snapshot, upsert semantics), advances the
      # cursor to max_pk, persists, and buffers chunk2 (whose fresh G is not yet covered).
      send(coord, {:capstan_watermark, "#{@uuid}:1-6"})
      assert_receive {:handle_snapshot, c1, m1}
      assert Enum.all?(c1, &(&1.op == :snapshot))
      assert Enum.map(c1, & &1.record) == [%{"id" => 1}, %{"id" => 2}]
      assert %{g: "#{@uuid}:1-5", final_chunk?: false, chunk_seq: 0, schema: "s", table: "t"} = m1

      # AT MOST ONE buffered: chunk2 REPLACED chunk1 (a single field, seq 1) — never accumulated.
      assert %Chunk{seq: 1} = :sys.get_state(coord).buffered
      {:ok, s1} = SnapshotStore.read(@in_memory, store)
      assert s1.tables[table()].pk_cursor == 2
      assert s1.status == :snapshotting

      # chunk2's G is NOT covered by the first watermark -> still buffered, not emitted.
      refute_received {:handle_snapshot, _, _}

      # A watermark covering G2 emits chunk2 (final), advances + marks done + completes.
      send(coord, {:capstan_watermark, "#{@uuid}:1-8"})
      assert_receive {:handle_snapshot, c2, m2}
      assert Enum.map(c2, & &1.record) == [%{"id" => 3}]
      assert %{g: "#{@uuid}:1-8", final_chunk?: true, chunk_seq: 1} = m2

      assert :sys.get_state(coord).buffered == nil
      {:ok, s2} = SnapshotStore.read(@in_memory, store)
      assert s2.tables[table()].pk_cursor == 3
      assert s2.tables[table()].done? == true
      assert s2.status == :complete
    end

    test "does NOT emit while G is uncovered — the exact ordering the gate exists for" do
      # RED anchor for tripwire 5: emitting before advance?(G) lets a <= G change be delivered by
      # BOTH the stream and the chunk.
      {:ok, store} = @in_memory.start_link()
      chunk1 = chunk(0, "#{@uuid}:1-20", [%{"id" => 1}], 1)

      coord =
        start_coordinator({@in_memory, store}, snap_state(%{table() => int_table(:start)}),
          readers: %{table() => %{script: [{:chunk, chunk1, true}]}}
        )

      # A watermark that does NOT cover G (1-20) must not emit.
      send(coord, {:capstan_watermark, "#{@uuid}:1-10"})
      refute_receive {:handle_snapshot, _, _}, 200
      assert %Chunk{seq: 0} = :sys.get_state(coord).buffered
    end
  end

  ## ===========================================================================
  ## at-least-once crash boundary (tripwire 16) — emit BEFORE persist; re-emit on the window
  ## ===========================================================================

  describe "at-least-once crash boundary (tripwire 16)" do
    test "the sink emit STRICTLY precedes the durable cursor persist" do
      # If persist landed before emit, a crash between persist and emit would ADVANCE the cursor
      # without emitting -> the chunk is LOST (at-most-once, a gap). Emitting first makes the
      # crash a bounded DUP instead. Same-sender (coordinator) message order proves it.
      {:ok, agent} = ReportingStore.start_link(self())
      chunk1 = chunk(0, "#{@uuid}:1-5", [%{"id" => 1}], 1)

      coord =
        start_coordinator({ReportingStore, agent}, snap_state(%{table() => int_table(:start)}),
          readers: %{table() => %{script: [{:chunk, chunk1, true}]}}
        )

      send(coord, {:capstan_watermark, "#{@uuid}:1-6"})

      assert_receive first
      assert_receive second

      assert match?({:handle_snapshot, _c, _m}, first),
             "expected the sink emit to PRECEDE the durable persist, got first: #{inspect(first)}"

      assert match?({:store_write, _state}, second)
    end

    test "a kill in the emit->persist window leaves the durable cursor un-advanced -> the chunk re-emits" do
      {:ok, gate_agent} = GatedStore.start_link(self())
      chunk1 = chunk(0, "#{@uuid}:1-5", [%{"id" => 1}, %{"id" => 2}], 2)

      coord1 =
        start_coordinator({GatedStore, gate_agent}, snap_state(%{table() => int_table(:start)}),
          readers: %{table() => %{script: [{:chunk, chunk1, false}]}}
        )

      ref = Process.monitor(coord1)
      send(coord1, {:capstan_watermark, "#{@uuid}:1-6"})

      # Emit happened (bounded dup #1)...
      assert_receive {:handle_snapshot, first_emit, _meta}
      assert Enum.map(first_emit, & &1.record) == [%{"id" => 1}, %{"id" => 2}]
      # ...and the durable persist is IN-FLIGHT, blocked before it can advance the cursor.
      assert_receive {:write_reached, ^coord1}

      # KILL in the window. The write never reached Agent.update -> the durable state is un-advanced.
      Process.exit(coord1, :kill)
      assert_receive {:DOWN, ^ref, :process, ^coord1, :killed}
      assert GatedStore.durable(gate_agent) == nil

      # Restart: a fresh coordinator resumes from the (un-advanced) cursor :start and RE-EMITS
      # the same chunk — the bounded, upsert-idempotent dup C1's posture accepts.
      {:ok, store2} = @in_memory.start_link()

      coord2 =
        start_coordinator({@in_memory, store2}, snap_state(%{table() => int_table(:start)}),
          readers: %{table() => %{script: [{:chunk, chunk1, false}]}}
        )

      send(coord2, {:capstan_watermark, "#{@uuid}:1-6"})
      assert_receive {:handle_snapshot, second_emit, _meta}
      assert Enum.map(second_emit, & &1.record) == [%{"id" => 1}, %{"id" => 2}]
      assert first_emit == second_emit
    end
  end

  ## ===========================================================================
  ## halt propagation — symmetric with C1
  ## ===========================================================================

  describe "halt propagation" do
    test "handle_snapshot {:error, _} halts {:snapshot_sink_error, _} (loud) + value-free telemetry" do
      attach_telemetry([[:capstan, :snapshot, :halt]])
      {:ok, store} = @in_memory.start_link()
      chunk1 = chunk(0, "#{@uuid}:1-5", [%{"id" => 1}], 1)

      RecordingSink.configure(
        report_to: self(),
        on_snapshot: fn _c, _m -> {:error, :downstream_boom} end
      )

      coord =
        start_coordinator({@in_memory, store}, snap_state(%{table() => int_table(:start)}),
          readers: %{table() => %{script: [{:chunk, chunk1, true}]}}
        )

      ref = Process.monitor(coord)
      send(coord, {:capstan_watermark, "#{@uuid}:1-6"})

      assert_receive {:capstan_halt, {:snapshot_sink_error, :downstream_boom}}

      assert_receive {:DOWN, ^ref, :process, ^coord,
                      {:shutdown, {:halt, {:snapshot_sink_error, :downstream_boom}}}}

      # Telemetry scrubs the compound reason to its OUTER atom (the allowlist gates keys).
      assert_receive {:telemetry, [:capstan, :snapshot, :halt], %{},
                      %{reason: :snapshot_sink_error}}
    end

    test "a raise in emit is scrubbed value-free {:snapshot_processing_crashed, %Error{}} — NO sentinel leaks" do
      attach_telemetry([[:capstan, :snapshot, :halt]])
      {:ok, store} = @in_memory.start_link()
      chunk1 = chunk(0, "#{@uuid}:1-5", [%{"id" => 1}], 1)

      RecordingSink.configure(
        report_to: self(),
        on_snapshot: fn _c, _m -> raise "SECRET_ROW_9f3a2b" end
      )

      coord =
        start_coordinator({@in_memory, store}, snap_state(%{table() => int_table(:start)}),
          readers: %{table() => %{script: [{:chunk, chunk1, true}]}}
        )

      ref = Process.monitor(coord)
      send(coord, {:capstan_watermark, "#{@uuid}:1-6"})

      assert_receive {:capstan_halt, halt_reason}
      assert {:snapshot_processing_crashed, %Capstan.Error{reason: :unknown}} = halt_reason
      # The raise message (a stand-in for a leaked row value) is DISCARDED by Error.from/1.
      refute inspect(halt_reason) =~ "SECRET"

      assert_receive {:DOWN, ^ref, :process, ^coord,
                      {:shutdown, {:halt, {:snapshot_processing_crashed, _}}} = down}

      refute inspect(down) =~ "SECRET"

      assert_receive {:telemetry, [:capstan, :snapshot, :halt], %{},
                      %{reason: :snapshot_processing_crashed}}
    end

    test "a reader {:error, reason} halts fail-closed with that reason (loud)" do
      {:ok, store} = @in_memory.start_link()

      coord =
        start_coordinator({@in_memory, store}, snap_state(%{table() => int_table(:start)}),
          readers: %{table() => %{script: [{:error, :snapshot_chunk_read_failed}]}}
        )

      # The halt fires during bootstrap (before we can monitor), so the LOUD `{:capstan_halt, _}`
      # is the reliable signal; the monitor then confirms the process is gone (reason is the
      # shutdown tuple if we beat the stop, else `:noproc`).
      ref = Process.monitor(coord)
      assert_receive {:capstan_halt, :snapshot_chunk_read_failed}
      assert_receive {:DOWN, ^ref, :process, ^coord, _reason}
    end
  end

  ## ===========================================================================
  ## Rule 1 (tripwire 12) — no PK/cursor in telemetry; op: :snapshot elides the record
  ## ===========================================================================

  describe "Rule 1 (tripwire 12)" do
    test "snapshot telemetry carries counts in MEASUREMENTS and only structural ids in METADATA" do
      attach_telemetry([
        [:capstan, :snapshot, :started],
        [:capstan, :snapshot, :chunk_completed],
        [:capstan, :snapshot, :completed]
      ])

      {:ok, store} = @in_memory.start_link()
      chunk1 = chunk(0, "#{@uuid}:1-5", [%{"id" => 1}, %{"id" => 2}], 2)

      coord =
        start_coordinator({@in_memory, store}, snap_state(%{table() => int_table(:start)}),
          readers: %{table() => %{script: [{:chunk, chunk1, true}]}}
        )

      assert_receive {:telemetry, [:capstan, :snapshot, :started], started_meas, started_meta}
      assert started_meta == %{}
      assert started_meas.table_count == 1

      send(coord, {:capstan_watermark, "#{@uuid}:1-6"})

      assert_receive {:telemetry, [:capstan, :snapshot, :chunk_completed], meas, meta}
      # METADATA is structural identity ONLY — never a PK/cursor (the max_pk 2 must not appear).
      assert Enum.sort(Map.keys(meta)) == [:schema, :table]
      assert meta == %{schema: "s", table: "t"}
      refute Enum.any?(Map.values(meta), &(&1 == 2))
      # Counts live in (ungated) MEASUREMENTS.
      assert meas.row_count == 2
      assert meas.chunk_seq == 0

      assert_receive {:telemetry, [:capstan, :snapshot, :completed], _meas, comp_meta}
      assert comp_meta == %{}
    end

    test "an op: :snapshot Change elides the record value under inspect/1" do
      change = %Change{
        op: :snapshot,
        schema: "s",
        table: "t",
        record: %{"card_number" => "SENTINEL_4242"},
        old_record: nil
      }

      rendered = inspect(change)
      assert rendered =~ ":snapshot"
      refute rendered =~ "SENTINEL_4242"
      refute rendered =~ "card_number"
    end
  end

  ## ===========================================================================
  ## a SnapshotStore write fault is BUDGETED then halts (design § Preconditions)
  ## ===========================================================================

  describe "SnapshotStore write fault — budgeted retry then fail-closed halt" do
    test "a PERSISTENT store-write fault retries the shared budget then halts :snapshot_state_write_failed" do
      # design § Preconditions: a SnapshotStore fault is BUDGETED, then halts. On a chunk emit the
      # coordinator persists the advanced cursor; a persistent write fault must retry the shared
      # CheckpointStore counter (max_retries + 1 attempts), NOT halt on the first fault.
      {:ok, fstore} = CountingFaultyStore.start_link(fail_until: :always)
      g = "#{@uuid}:1-6"
      c = chunk(0, g, [%{"id" => 42}], 42)

      coord =
        start_coordinator(
          {CountingFaultyStore, fstore},
          snap_state(%{table() => int_table(:start)}),
          readers: %{table() => %{script: [{:chunk, c, true}]}},
          max_retries: 2
        )

      # A watermark covering G emits the chunk (handle_snapshot :ok), then persist fails.
      send(coord, {:capstan_watermark, g})

      assert_receive {:capstan_halt, :snapshot_state_write_failed}
      # BUDGET proof: max_retries(2) + 1 = 3 write attempts. RED (no budget / immediate halt): 1.
      assert CountingFaultyStore.count(fstore) == 3
    end

    test "a PERMANENT store reason (:config_invalid) halts IMMEDIATELY without spending the budget" do
      # permanent_reason?/1 short-circuits the budget: a mis-shaped store never un-breaks on
      # retry, so it halts on the first fault. RED (no permanent_reason?/1 check): it retries
      # max_retries times first (count == 6), wasting the budget on an unrecoverable fault.
      {:ok, fstore} =
        CountingFaultyStore.start_link(fail_until: :always, error_reason: :config_invalid)

      g = "#{@uuid}:1-6"
      c = chunk(0, g, [%{"id" => 42}], 42)

      coord =
        start_coordinator(
          {CountingFaultyStore, fstore},
          snap_state(%{table() => int_table(:start)}),
          readers: %{table() => %{script: [{:chunk, c, true}]}},
          max_retries: 5
        )

      send(coord, {:capstan_watermark, g})

      assert_receive {:capstan_halt, :snapshot_state_write_failed}
      assert CountingFaultyStore.count(fstore) == 1
    end

    test "a TRANSIENT store-write fault (within budget, then succeeds) is survived — no halt" do
      # Fail the first persist attempt, then succeed: the budget rides through the blip and the
      # coordinator stays alive rather than tearing the pipeline down on a momentary store hiccup.
      {:ok, fstore} = CountingFaultyStore.start_link(fail_until: 1)
      g = "#{@uuid}:1-6"
      c = chunk(0, g, [%{"id" => 42}], 42)

      coord =
        start_coordinator(
          {CountingFaultyStore, fstore},
          snap_state(%{table() => int_table(:start)}),
          readers: %{table() => %{script: [{:chunk, c, true}, :done]}},
          max_retries: 3
        )

      send(coord, {:capstan_watermark, g})

      assert_receive {:handle_snapshot, _changes, _meta}
      refute_receive {:capstan_halt, _}, 200
      assert Process.alive?(coord)
    end
  end

  ## ---------------------------------------------------------------------------
  ## helpers
  ## ---------------------------------------------------------------------------

  defp table, do: {"s", "t"}

  defp int_table(cursor, done? \\ false) do
    %{fingerprint: "fp", pk_columns: ["id"], pk_types: [:int], pk_cursor: cursor, done?: done?}
  end

  defp snap_state(tables), do: %State{status: :snapshotting, p0: @p0, tables: tables}

  defp store_backed do
    {:ok, store} = @in_memory.start_link()
    {@in_memory, store}
  end

  defp chunk(seq, g, rows, max_pk),
    do: %Chunk{table: table(), seq: seq, g: g, rows: rows, max_pk: max_pk}

  defp pos, do: %Position{gtid_set: "#{@uuid}:1-101", file: nil, pos: nil}

  defp txn(changes) do
    %Transaction{
      gtid: "#{@uuid}:101",
      position: pos(),
      changes: changes,
      commit_ts: ~U[2026-07-21 00:00:00Z]
    }
  end

  defp insert(id),
    do: %Change{op: :insert, schema: "s", table: "t", record: %{"id" => id}, old_record: nil}

  defp delete(id),
    do: %Change{op: :delete, schema: "s", table: "t", record: nil, old_record: %{"id" => id}}

  defp pk_update(old_id, new_id) do
    %Change{
      op: :update,
      schema: "s",
      table: "t",
      record: %{"id" => new_id},
      old_record: %{"id" => old_id}
    }
  end

  defp foreign_insert(id),
    do: %Change{op: :insert, schema: "other", table: "x", record: %{"id" => id}, old_record: nil}

  # A coordinator whose one snapshot table stays ACTIVE (a single never-covered chunk buffers
  # forever), so `handle_transaction/1` gating can be exercised against a fixed cursor without
  # driving any real backfill.
  defp gate_coordinator(cursor) do
    buffered = chunk(0, "#{@uuid}:9000-9001", [%{"id" => 999}], 999)

    start_coordinator(store_backed(), snap_state(%{table() => int_table(cursor)}),
      readers: %{table() => %{script: [{:chunk, buffered, false}]}}
    )
  end

  # The coordinator is started UNLINKED (`GenServer.start`, not `start_link`): a fail-closed
  # halt exits `{:shutdown, {:halt, _}}`, which — through a link — would propagate to and kill
  # the (non-trapping) test process. Unlinked + `on_exit` cleanup keeps every halt/crash test
  # isolated. The registered name is `Coordinator` (fixed, per § Pinned decisions #4), so the
  # tests are `async: false` and each start reuses the freed name.
  defp start_coordinator(snapshot_store, snapshot_state, opts) do
    coord_opts =
      [
        sink: RecordingSink,
        assembler: self(),
        snapshot_store: snapshot_store,
        snapshot_state: snapshot_state,
        chunk_reader: Keyword.get(opts, :chunk_reader, StubReader),
        processed_set: Keyword.get(opts, :processed_set, ""),
        readers: Keyword.fetch!(opts, :readers)
      ] ++ table_order_opt(opts) ++ Keyword.take(opts, [:max_retries])

    {:ok, pid} = GenServer.start(Coordinator, coord_opts, name: Coordinator)
    # Best-effort teardown: a halt/crash test leaves the coordinator already dead, so catch the
    # `:noproc`/shutdown exit rather than race `Process.alive?/1` against `GenServer.stop/2`.
    on_exit(fn ->
      try do
        GenServer.stop(pid, :normal, 200)
      catch
        :exit, _ -> :ok
      end
    end)

    pid
  end

  defp table_order_opt(opts) do
    case Keyword.fetch(opts, :table_order) do
      {:ok, order} -> [table_order: order]
      :error -> []
    end
  end

  @doc false
  def forward_telemetry(name, measurements, metadata, %{test: test}) do
    send(test, {:telemetry, name, measurements, metadata})
  end

  defp attach_telemetry(events) do
    id = {:coordinator_test, System.unique_integer()}
    :telemetry.attach_many(id, events, &__MODULE__.forward_telemetry/4, %{test: self()})
    on_exit(fn -> :telemetry.detach(id) end)
    id
  end
end

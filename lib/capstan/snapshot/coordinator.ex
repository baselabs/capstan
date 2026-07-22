defmodule Capstan.Snapshot.Coordinator do
  @moduledoc """
  The initial-snapshot orchestration core (C2 Task 8): a GenServer that interposes as the
  `Capstan.AssemblerServer`'s sink and its `:watermark_observer`, runs the cursor-gate over
  the live stream, drives the `Capstan.Snapshot.ChunkReader`, and emits each backfill chunk
  through the real sink's `c:Capstan.Sink.handle_snapshot/2` once the stream's processed
  watermark covers the chunk's exact GTID position `G`.

  ## Two roles, two message paths

    * **As the AssemblerServer's sink.** The coordinator implements the `Capstan.Sink`
      behaviour and is name-registered under `#{inspect(__MODULE__)}`. The AssemblerServer's
      `state.sink` is this MODULE, so its `handle_transaction/1` and `handle_schema_change/2`
      dispatch here; each is a thin module function that `GenServer.call`s the registered
      coordinator (resolved at call time, not start time — this is what breaks the start-order
      cycle, design § Pinned decisions #4). The coordinator does NOT edit `assembler_server.ex`;
      Task 7 supplies the `attach_coordinator/2` injection + the silent-death monitor.

    * **As the `:watermark_observer`.** After a deferred `attach_coordinator/2`, the
      AssemblerServer sends `{:capstan_watermark, gtid_set_string}` on EVERY checkpoint advance
      (delivered, filtered, AND self-committing DDL — the Ch6 choke-point guarantee). The
      coordinator feeds that string into the advance gate.

  ## The cursor-gate (`handle_transaction/1`)

  For each streamed `%Capstan.Change{}` on a snapshot-active table (in the snapshot set and
  not yet done), the coordinator applies `Capstan.Snapshot.CursorGate.classify/3` per
  row-image with the per-table cursor + PK shape, and forwards the surviving subset to the
  real sink as a transaction carrying only those changes. A change on a NON-snapshot table —
  or on a table whose backfill is already done (the stream is authoritative) — passes through
  unchanged.

  When ALL changes are suppressed, the coordinator returns `{:ok, position}` WITHOUT calling
  the real sink: a fully-suppressed transaction is exactly a *filtered* transaction from the
  real sink's view (ADR-0003), and the AssemblerServer still advances the watermark for it
  (`assembler_server.ex:246`/`:257`). Returning the txn's position keeps a fully-suppressed
  run from stalling the advance gate → deadlock.

  ## The advance gate

  The coordinator holds AT MOST ONE buffered chunk (its `rows` + its exact `G`). When the
  processed watermark covers `G` (`CursorGate.advance?/2`), it emits the chunk as a list of
  `%Change{op: :snapshot}` through the real sink's `handle_snapshot/2` with a value-free
  `%Capstan.Snapshot.Meta{}`, THEN advances the per-table cursor to `chunk.max_pk`, persists
  the `%Capstan.Snapshot.State{}` via `Capstan.SnapshotStore`, and drives the reader for the
  next chunk. The emit-BEFORE-persist ordering is the at-least-once boundary (tripwire 16): a
  crash between the sink's `{:ok}` and the durable cursor persist re-emits the one chunk
  (bounded dup, C1's posture; an upsert-by-PK sink converges).

  ## Fail-closed halts (symmetric with C1)

  A coordinator fault sends `{:capstan_halt, reason}` to the AssemblerServer (the LOUD path)
  and stops with `{:shutdown, {:halt, reason}}` (`restart: :temporary`, never restarted). The
  SILENT-death path — a coordinator that dies without messaging — is the AssemblerServer's
  `Process.monitor` (`:snapshot_coordinator_down`, Task 7). A raise in the emit/reconcile path
  is scrubbed value-free to `{:snapshot_processing_crashed, Capstan.Error.from(exc)}` exactly
  as the AssemblerServer scrubs a delivery-path raise (`assembler_server.ex:148-158`); a
  `handle_snapshot/2` `{:error, _}` halts `{:snapshot_sink_error, _}` (the outer atom only in
  telemetry). A DDL on a snapshot-active table halts `:snapshot_schema_drifted`.

  ## Rule 1

  Row values (chunk rows, canonical PKs, the per-table cursor) travel ONLY in the delivered
  `%Change{op: :snapshot}` list and in in-memory state; they are never logged or telemetered.
  Telemetry (`[:capstan, :snapshot, :started | :chunk_completed | :completed | :halt]`) carries
  counts in MEASUREMENTS and only structural identity (schema/table/reason) in METADATA, gated
  by `Capstan.Telemetry`'s value-free allowlist (a stray PK/cursor raises). The coordinator's
  own struct holds `%Capstan.Snapshot.Chunk{}` / `%Capstan.Snapshot.State{}`, each of which
  derives a value-eliding `Inspect`, so even an incidental inspect of the state never surfaces
  a value; and every halt is a graceful `{:shutdown, _}` exit (never an abnormal crash), so the
  in-flight message is never dumped to a crash report.
  """

  use GenServer, restart: :temporary

  @behaviour Capstan.Sink

  alias Capstan.Change
  alias Capstan.CheckpointStore
  alias Capstan.Error
  alias Capstan.Snapshot.Chunk
  alias Capstan.Snapshot.CursorGate
  alias Capstan.Snapshot.Meta
  alias Capstan.Snapshot.State, as: SnapState
  alias Capstan.SnapshotStore
  alias Capstan.Telemetry

  @typedoc "The `{schema, table}` identity of a snapshot table."
  @type table_key :: {String.t(), String.t()}

  @typedoc "A durable snapshot store as `{callback_module, store_handle}`."
  @type store :: {module(), term()}

  defstruct [
    # the real (downstream) sink module the coordinator forwards to / emits chunks through
    :sink,
    # the AssemblerServer pid/name the coordinator sends `{:capstan_halt, reason}` to
    :assembler,
    # the durable snapshot store `{impl, handle}`
    :store,
    # the ChunkReader module (injectable so unit tests can stub the reader/query)
    :chunk_reader,
    # per-table opened reader handles: `%{table_key => reader}`
    :readers,
    # the durable `%Capstan.Snapshot.State{}` (per-table cursor / PK shape / done flag)
    :snapshot_state,
    # the stream's processed watermark as a canonical GTID-set STRING (fed by the observer)
    :processed_set,
    # the not-yet-done tables in backfill order; head is the table currently being paged
    :pending,
    # AT MOST ONE buffered chunk awaiting the advance gate (`%Chunk{}` or `nil`)
    :buffered,
    # whether the buffered chunk is its table's final chunk
    :buffered_final?,
    # the budgeted-fault retry ceiling for a SnapshotStore write (shared C1 counter)
    :max_retries
  ]

  ## ---------------------------------------------------------------------------
  ## public API
  ## ---------------------------------------------------------------------------

  @doc """
  Starts the coordinator, registered under `#{inspect(__MODULE__)}` so the AssemblerServer's
  module-sink dispatch (`state.sink.handle_transaction/1` etc.) resolves it by name.

  Options:

    * `:sink` (required) — the real (downstream) `Capstan.Sink` module.
    * `:assembler` (required) — the AssemblerServer pid/name for `{:capstan_halt, _}`.
    * `:snapshot_store` (required) — `{impl_module, store_handle}`.
    * `:snapshot_state` (required) — the initial `%Capstan.Snapshot.State{}`.
    * `:readers` (required) — `%{{schema, table} => reader_handle}` (opened `ChunkReader`s).
    * `:chunk_reader` — the reader module (default `Capstan.Snapshot.ChunkReader`).
    * `:processed_set` — the initial processed watermark string (default `""`).
    * `:table_order` — the backfill order (default: not-done tables, sorted).
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) when is_list(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  ## ---------------------------------------------------------------------------
  ## Capstan.Sink callbacks — dispatched by the AssemblerServer to the registered name
  ## ---------------------------------------------------------------------------

  @doc """
  The AssemblerServer's `handle_transaction/1` sink hook: routes the call to the registered
  coordinator, which applies the cursor-gate and forwards the surviving subset. Returns
  `{:ok, position}` (the txn's position) — even when every change is suppressed.
  """
  @impl Capstan.Sink
  @spec handle_transaction(Capstan.Transaction.t()) ::
          {:ok, Capstan.Position.t()} | {:error, term()}
  def handle_transaction(txn) do
    GenServer.call(__MODULE__, {:handle_transaction, txn})
  end

  @doc """
  The AssemblerServer's `handle_schema_change/2` sink hook: a DDL on a snapshot-active table
  halts `:snapshot_schema_drifted`; any other DDL forwards to the real sink.
  """
  @impl Capstan.Sink
  @spec handle_schema_change(Capstan.SchemaChange.t(), Capstan.Position.t()) ::
          :ok | {:error, term()}
  def handle_schema_change(schema_change, position) do
    GenServer.call(__MODULE__, {:handle_schema_change, schema_change, position})
  end

  ## ---------------------------------------------------------------------------
  ## GenServer callbacks
  ## ---------------------------------------------------------------------------

  @impl GenServer
  def init(opts) do
    snapshot_state = Keyword.fetch!(opts, :snapshot_state)

    state = %__MODULE__{
      sink: Keyword.fetch!(opts, :sink),
      assembler: Keyword.fetch!(opts, :assembler),
      store: Keyword.fetch!(opts, :snapshot_store),
      snapshot_state: snapshot_state,
      readers: Keyword.fetch!(opts, :readers),
      chunk_reader: Keyword.get(opts, :chunk_reader, Capstan.Snapshot.ChunkReader),
      processed_set: Keyword.get(opts, :processed_set, ""),
      pending: Keyword.get(opts, :table_order) || default_pending(snapshot_state),
      buffered: nil,
      buffered_final?: false,
      max_retries: Keyword.get(opts, :max_retries, CheckpointStore.default_max_retries())
    }

    {:ok, state, {:continue, :bootstrap}}
  end

  # Emit the value-free `:started` event, then drive the first chunk. Wrapped so a raise while
  # opening the first chunk halts value-free rather than crashing (Rule 1).
  @impl GenServer
  def handle_continue(:bootstrap, state) do
    emit_started(state)
    drive(state)
  rescue
    exception -> crash_halt(state, exception)
  catch
    _kind, _reason -> crash_halt(state, :unknown)
  end

  # The cursor-gate. Forward the surviving subset (or nothing when fully suppressed); always
  # return the txn's own position so the watermark advances (no advance-gate deadlock). A raise
  # in gating/forwarding is scrubbed value-free — the in-flight txn (row values) never leaks.
  @impl GenServer
  def handle_call({:handle_transaction, txn}, _from, state) do
    case gate_changes(txn.changes, state) do
      [] ->
        {:reply, {:ok, txn.position}, state}

      surviving ->
        case state.sink.handle_transaction(%{txn | changes: surviving}) do
          {:ok, _position} -> {:reply, {:ok, txn.position}, state}
          {:error, reason} -> {:reply, {:error, reason}, state}
        end
    end
  rescue
    exception -> crash_halt_call(state, exception)
  catch
    _kind, _reason -> crash_halt_call(state, :unknown)
  end

  def handle_call({:handle_schema_change, schema_change, position}, _from, state) do
    key = {schema_change.schema, schema_change.table}

    if snapshot_active?(state, key) do
      # A DDL hit a table still being backfilled — the backfill is now inconsistent. Halt the
      # named drift reason (loud), replying `{:error, _}` so the caller's `GenServer.call`
      # returns rather than blocks.
      halt_reply(state, :snapshot_schema_drifted, {:error, :snapshot_schema_drifted})
    else
      case state.sink.handle_schema_change(schema_change, position) do
        :ok -> {:reply, :ok, state}
        {:error, reason} -> {:reply, {:error, reason}, state}
      end
    end
  rescue
    exception -> crash_halt_call(state, exception)
  catch
    _kind, _reason -> crash_halt_call(state, :unknown)
  end

  # A watermark advance from the AssemblerServer: update the processed set, then try the
  # advance gate. Wrapped value-free (an emit/reconcile raise must not leak a row value).
  @impl GenServer
  def handle_info({:capstan_watermark, gtid_set}, state) when is_binary(gtid_set) do
    try_advance(%{state | processed_set: gtid_set})
  rescue
    exception -> crash_halt(state, exception)
  catch
    _kind, _reason -> crash_halt(state, :unknown)
  end

  def handle_info(_message, state), do: {:noreply, state}

  # Release the shared `Capstan.Query` connection the readers hold. The bootstrap opens ONE query
  # connection (all readers share it, `chunk_reader.ex`); the coordinator owns it for the
  # backfill's life, so it must be closed when the coordinator stops — a fail-closed halt or a
  # `Supervisor.stop` — else the authenticated source connection leaks. Called for every stop
  # reason (a `:temporary` coordinator's `terminate/2` still runs on a `{:shutdown, _}` exit).
  # `Query.close` on an already-closed socket is a harmless no-op, so a double-close (the
  # supervisor also closes on a pre-start wiring abort) is safe.
  @impl GenServer
  def terminate(_reason, %__MODULE__{readers: readers}) do
    close_query(readers)
    :ok
  end

  def terminate(_reason, _state), do: :ok

  defp close_query(readers) when is_map(readers) do
    case Map.values(readers) do
      [%{query: %Capstan.Query{} = query} | _] -> Capstan.Query.close(query)
      _ -> :ok
    end
  end

  defp close_query(_readers), do: :ok

  ## ---------------------------------------------------------------------------
  ## the cursor-gate (per streamed change)
  ## ---------------------------------------------------------------------------

  # Classify each streamed change: an ACTIVE snapshot table (in the set, not done) routes
  # through `CursorGate.classify/3` (which suppresses `k > cursor` and splits a PK-changing
  # update); a non-snapshot table OR a done table (the stream is authoritative) passes through
  # unchanged. `Enum.flat_map/2` consumes `changes` exactly once (never `length/1`).
  defp gate_changes(changes, state) do
    Enum.flat_map(changes, fn change ->
      case Map.get(state.snapshot_state.tables, {change.schema, change.table}) do
        %{done?: false} = progress ->
          CursorGate.classify(change, progress.pk_cursor, table_spec(progress))

        _done_or_absent ->
          [change]
      end
    end)
  end

  defp table_spec(progress) do
    %{pk_columns: progress.pk_columns, pk_types: progress.pk_types, complete?: false}
  end

  # A table is snapshot-active — DDL on it is a drift — iff it is in the snapshot set AND its
  # backfill is not yet done (once done, the stream is authoritative and a DDL forwards).
  defp snapshot_active?(state, key) do
    match?(%{done?: false}, Map.get(state.snapshot_state.tables, key))
  end

  ## ---------------------------------------------------------------------------
  ## driving the reader + the advance gate
  ## ---------------------------------------------------------------------------

  # No more tables to page — the whole backfill is complete.
  defp drive(%__MODULE__{pending: []} = state), do: complete(state)

  # Read the next chunk of the head table; buffer it and try the gate, advance to the next
  # table on an empty page, or halt on a read fault.
  defp drive(%__MODULE__{pending: [key | _]} = state) do
    reader = Map.fetch!(state.readers, key)

    case state.chunk_reader.read_chunk(reader, table_cursor(state, key)) do
      {:ok, chunk, final?, reader2} ->
        state = %{put_reader(state, key, reader2) | buffered: chunk, buffered_final?: final?}
        try_advance(state)

      {:done, reader2} ->
        state = state |> put_reader(key, reader2) |> mark_done_and_pop(key)

        case persist(state) do
          {:ok, state} -> drive(state)
          {:halt, reason} -> coordinator_halt(state, reason)
        end

      {:error, reason} ->
        coordinator_halt(state, reason)
    end
  end

  # Emit the buffered chunk iff the processed watermark now covers its exact `G`; else wait.
  defp try_advance(%__MODULE__{buffered: nil} = state), do: {:noreply, state}

  defp try_advance(%__MODULE__{buffered: %Chunk{} = chunk} = state) do
    if CursorGate.advance?(chunk.g, state.processed_set) do
      emit_chunk(state, chunk)
    else
      {:noreply, state}
    end
  end

  # Emit FIRST (at-least-once), then advance the cursor + persist + telemetry + drive the next
  # chunk. A `handle_snapshot/2` `{:error, _}` halts `{:snapshot_sink_error, _}`.
  defp emit_chunk(state, %Chunk{} = chunk) do
    {schema, table} = chunk.table

    changes =
      Enum.map(chunk.rows, fn row ->
        %Change{op: :snapshot, schema: schema, table: table, record: row, old_record: nil}
      end)

    meta = %Meta{
      schema: schema,
      table: table,
      chunk_seq: chunk.seq,
      g: chunk.g,
      final_chunk?: state.buffered_final?
    }

    case state.sink.handle_snapshot(changes, meta) do
      :ok -> after_emit(state, chunk)
      {:error, reason} -> coordinator_halt(state, {:snapshot_sink_error, reason})
    end
  end

  # Post-emit: advance the cursor to `chunk.max_pk`, mark the table done on its final chunk,
  # persist the durable state, emit `:chunk_completed`, clear the buffer, and drive the next.
  defp after_emit(state, %Chunk{} = chunk) do
    state =
      state
      |> advance_cursor(chunk.table, chunk.max_pk)
      |> maybe_mark_done(chunk.table, state.buffered_final?)
      |> Map.merge(%{buffered: nil, buffered_final?: false})

    case persist(state) do
      {:ok, state} ->
        emit_chunk_completed(chunk)
        drive(state)

      {:halt, reason} ->
        coordinator_halt(state, reason)
    end
  end

  # Every table paged — mark the whole snapshot complete, persist, and announce it. The
  # coordinator stays alive as the (now pass-through) sink for the rest of this run.
  defp complete(state) do
    state = %{state | snapshot_state: %{state.snapshot_state | status: :complete}}

    case persist(state) do
      {:ok, state} ->
        emit_completed(state)
        {:noreply, state}

      {:halt, reason} ->
        coordinator_halt(state, reason)
    end
  end

  ## ---------------------------------------------------------------------------
  ## durable state helpers
  ## ---------------------------------------------------------------------------

  # A store-write fault is BUDGETED (design § Preconditions: "budgeted, then halt") via the
  # SHARED `CheckpointStore` retry family — the same posture C1 uses for a checkpoint-store write
  # (`assembler_server.ex` do_checkpoint), so a transient store blip is survived rather than
  # tearing down the pipeline. A PERMANENT reason (`permanent_reason?/1`, e.g. `:config_invalid`
  # — a mis-shaped store) halts IMMEDIATELY without spending the budget; every other fault is
  # budgeted, then halts fail-closed. The terminal halt is always the value-free
  # `:snapshot_state_write_failed` (the raw reason is discarded — Rule 1). Mirrors
  # `assembler_server.ex` do_checkpoint's `cond` exactly.
  defp persist(state), do: persist(state, 0)

  defp persist(%__MODULE__{store: {impl, store}} = state, attempt) do
    case SnapshotStore.write(impl, store, state.snapshot_state) do
      :ok ->
        {:ok, state}

      {:error, reason} ->
        cond do
          CheckpointStore.permanent_reason?(reason) ->
            {:halt, :snapshot_state_write_failed}

          CheckpointStore.retry_decision(attempt, state.max_retries) == :retry ->
            persist(state, attempt + 1)

          true ->
            {:halt, :snapshot_state_write_failed}
        end
    end
  end

  defp table_cursor(state, key), do: Map.fetch!(state.snapshot_state.tables, key).pk_cursor

  defp advance_cursor(state, key, max_pk) do
    update_table(state, key, fn progress -> %{progress | pk_cursor: max_pk} end)
  end

  defp maybe_mark_done(state, _key, false), do: state
  defp maybe_mark_done(state, key, true), do: mark_done_and_pop(state, key)

  defp mark_done_and_pop(state, key) do
    state
    |> update_table(key, fn progress -> %{progress | done?: true} end)
    |> Map.update!(:pending, &List.delete(&1, key))
  end

  defp update_table(state, key, fun) do
    tables = Map.update!(state.snapshot_state.tables, key, fun)
    %{state | snapshot_state: %{state.snapshot_state | tables: tables}}
  end

  defp put_reader(state, key, reader) do
    %{state | readers: Map.put(state.readers, key, reader)}
  end

  # The not-yet-done tables in a deterministic (sorted) backfill order.
  defp default_pending(%SnapState{tables: tables}) do
    tables
    |> Enum.reject(fn {_key, progress} -> progress.done? end)
    |> Enum.map(fn {key, _progress} -> key end)
    |> Enum.sort()
  end

  ## ---------------------------------------------------------------------------
  ## fail-closed halts — send the loud `{:capstan_halt, _}`, then stop `{:shutdown, {:halt, _}}`
  ## ---------------------------------------------------------------------------

  # A raise on the emit/reconcile path (a `handle_info`/`handle_continue` boundary): scrub
  # value-free exactly as `assembler_server.ex:148-158` and halt without a reply.
  defp crash_halt(state, exception) do
    coordinator_halt(state, {:snapshot_processing_crashed, Error.from(exception)})
  end

  # The same scrub inside a `handle_call` — reply the value-free reason so the caller unblocks.
  defp crash_halt_call(state, exception) do
    reason = {:snapshot_processing_crashed, Error.from(exception)}
    halt_reply(state, reason, {:error, reason})
  end

  defp coordinator_halt(state, reason) do
    announce_halt(state, reason)
    {:stop, {:shutdown, {:halt, reason}}, state}
  end

  defp halt_reply(state, reason, reply) do
    announce_halt(state, reason)
    {:stop, {:shutdown, {:halt, reason}}, reply, state}
  end

  # Emit the coordinator's own value-free halt telemetry, then send the LOUD, specific
  # `{:capstan_halt, reason}` to the AssemblerServer (the SILENT-death backstop is the
  # AssemblerServer's monitor → `:snapshot_coordinator_down`, Task 7). The `{:capstan_halt, _}`
  # message is enqueued before this process terminates, so the AssemblerServer sees the
  # specific reason ahead of any `{:DOWN, _}`.
  defp announce_halt(state, reason) do
    emit_halt(reason)
    send(state.assembler, {:capstan_halt, reason})
    :ok
  end

  ## ---------------------------------------------------------------------------
  ## telemetry — value-free; counts in MEASUREMENTS, structural identity in METADATA
  ## ---------------------------------------------------------------------------

  defp emit_started(state) do
    Telemetry.event([:capstan, :snapshot, :started], %{table_count: length(state.pending)}, %{})
  end

  defp emit_chunk_completed(%Chunk{table: {schema, table}} = chunk) do
    Telemetry.event(
      [:capstan, :snapshot, :chunk_completed],
      %{row_count: length(chunk.rows), chunk_seq: chunk.seq},
      %{schema: schema, table: table}
    )
  end

  defp emit_completed(state) do
    Telemetry.event(
      [:capstan, :snapshot, :completed],
      %{table_count: map_size(state.snapshot_state.tables)},
      %{}
    )
  end

  # The reason is scrubbed to its value-free OUTER atom via `Error.from/1` (the allowlist gates
  # KEYS, not values): `{:snapshot_sink_error, <raw>}` → `:snapshot_sink_error`,
  # `{:snapshot_processing_crashed, %Error{}}` → `:snapshot_processing_crashed`.
  defp emit_halt(reason) do
    Telemetry.event([:capstan, :snapshot, :halt], %{}, %{reason: Error.from(reason).reason})
  end
end

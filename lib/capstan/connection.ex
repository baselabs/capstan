defmodule Capstan.Connection do
  @moduledoc """
  The socket-owning GenServer: handshake, auth, the fail-closed gap gate, the dump,
  and frame forwarding.

  `Connection` owns ONLY the TCP/TLS socket. It never delivers to a sink and never
  **writes** the checkpoint — `Capstan.AssemblerServer` is the sole checkpoint writer, so
  there is no split-brain over the durable position. It does **read** the
  durable resume position at each (re)establish (see "Resume position" below); reading is
  not writing, and it is what makes a reconnect resume from the current watermark. That
  split keeps the reconnect seam auditable: every socket-lifetime primitive (the reconnect
  timer, the streaming reader) lives in this one module and is torn down with the
  connection it serves.

  ## Resume position

  The dump resumes from the durable checkpoint, RE-READ from the checkpoint store on every
  establish — the initial connect AND every reconnect. Freezing the resume position at
  start-up would be a correctness bug: after a transient drop the `AssemblerServer` has
  advanced the durable watermark, so a reconnect that replayed from the frozen start-up
  position would (a) needlessly re-stream everything since start-up (the `AssemblerServer`
  dedups it, but at O(run-length) cost) and, worse, (b) once source retention purged past
  that stale position, feed `gap_check/3` a checkpoint whose unapplied remainder intersects
  `gtid_purged` — a **false** `:data_gap` halt on a perfectly healthy pipeline, re-opening
  the very silent-loss vector the gate exists to close. Re-reading makes the resume correct
  by construction. The `Connection` is given the store as a `{impl, handle}` and calls
  `Capstan.CheckpointStore.read_position/2` (read-only — the single-writer invariant holds).
  A store read fault on refresh keeps the last-known position and proceeds (never worse than
  the frozen behaviour; the next reconnect refreshes); a store is optional (its absence — in
  a unit test wired only with `:start_position` — keeps the injected position).

  ## Lifecycle

      connect + auth  (via the injected connect_fun; default: gen_tcp + Handshake)
        -> Config.check_preconditions/1        (fail-closed server gate, ADR-0002)
        -> read @@gtid_executed / @@gtid_purged
        -> gap_check/3                          (proactive retention gap)
        -> SET @master_binlog_checksum          (required BEFORE the dump)
        -> SET @master_heartbeat_period         (liveness — required BEFORE the dump)
        -> COM_BINLOG_DUMP_GTID                 (resume from the start position)
        -> stream frames -> receiver

  ## Streaming liveness (half-open partition detection)

  A silent half-open partition mid-stream would otherwise hang the pipeline forever — the
  reader blocks on `recv` and MySQL only heartbeats an IDLE stream. Two mechanisms bound the
  detection window: (1) `SET @master_heartbeat_period` asks the master to emit a
  `HEARTBEAT_LOG_EVENT` after `:heartbeat_period_ms` of idleness (self-contained — not
  dependent on the server's `slave_net_timeout`), so even a quiet-but-healthy stream keeps
  delivering frames; (2) a parent-side **liveness timer** fires if NO frame (event OR
  heartbeat) arrives within `:stream_timeout_ms` (default 4× the heartbeat period, tolerating
  3 missed heartbeats) — on fire it emits `[:capstan, :connection, :stream_timeout]` (the stall
  is no longer silent), kills the recv-blocked reader, and reconnects; a persistent partition
  halts `:stream_stalled` via the established-then-dropped budget. `keepalive: true` on the
  socket is the OS backstop for a fully-dead peer (its ~2h default idle is too slow to be
  primary). The reader itself keeps reading with no recv timeout; liveness is the parent's job.

  ## Receiver contract

  Each streamed binlog event is forwarded as `{:binlog_event, raw_event_bytes}` where
  `raw_event_bytes` is the 19-byte header + body + CRC — exactly what
  `Capstan.Binlog.Event.parse/1` consumes (this module never decodes; that is the
  assembler's job). A fail-closed halt is delivered as `{:capstan_halt, reason}` and
  then the process stops with `{:shutdown, {:halt, reason}}`.

  The forwarding is a bare `send/2`, so it never raises when the receiver is dead — but a
  send into a dead receiver is a silent no-op, and the receiver's OWN fail-closed halt does
  not message back here. So this module **monitors** the receiver: on its `:DOWN` the
  Connection stops fail-closed (`:receiver_down`) rather than keep streaming events into a
  dead pid. Monitoring is not linking — a `:shutdown` stop of a `:temporary` child never
  restarts, so this closes the silent stall without the restart livelock the plain-`send`
  wiring exists to avoid.

  ## Two independent counters

    * **Command budget** (`max_command_retries`, default 5): counts failures that occur
      **before** the connection establishes (connect/auth/query/dump-send errors). It
      is RESET when a frame arrives — replicant's reset-on-frame policy, correct for
      transient pre-establish faults. Halts `:command_retries_exhausted` on the `max + 1`-th
      failure.
    * **Cycle counter**: counts **established-then-dropped** cycles. It is **NOT** reset
      by frame arrival, and is reset only by clean shutdown. A duplicate `server_id`
      makes MySQL evict this replica after each establish — authenticating and receiving
      frames before dying — so a frame-reset counter would livelock
      forever while emitting healthy `:established` telemetry. Halts `:server_id_conflict`
      on the `max + 1`-th cycle.

  ## Error 1236 is OVERLOADED (ADR-0003)

  A dump refusal (error 1236) is discriminated on its message text: checksum-negotiation
  text -> `:checksum_negotiation_failed`; purged-logs text (with OR without a named
  range) -> `:data_gap`; a duplicate-replica message (`server_uuid`/`server_id`) ->
  `:server_id_conflict` (MySQL 8.0.x evicts a same-`server_id` replica via 1236, not
  a socket drop); anything else -> `:unrecognized_dump_error`, **never** `:data_gap` —
  mapping 1236 unconditionally to a gap would tell an operator to reprovision past what is
  actually a config bug, manufacturing the silent loss the gate exists to prevent.
  """

  use GenServer

  alias Capstan.CheckpointStore
  alias Capstan.Config
  alias Capstan.Gtid
  alias Capstan.Position
  alias Capstan.Protocol.Command
  alias Capstan.Protocol.Handshake
  alias Capstan.Protocol.Packet
  alias Capstan.Telemetry

  @default_max_command_retries 5
  @default_reconnect_backoff 1_000
  @default_connect_timeout 20_000

  # Streaming liveness (half-open partition detection). The master sends a HEARTBEAT_LOG_EVENT
  # after `@master_heartbeat_period` of stream idleness; capstan sets it explicitly so the
  # liveness signal does not depend on the server's `slave_net_timeout`. The parent's liveness
  # timer fires if NO frame (event or heartbeat) arrives within `stream_timeout_ms`; the default
  # is 4× the heartbeat period, tolerating 3 consecutive missed heartbeats before declaring the
  # stream dead. `@master_heartbeat_period` is set in NANOSECONDS (verified live: N=1e9 → ~1s).
  @default_heartbeat_period_ms 15_000
  @default_stream_timeout_ms 60_000
  # Mirrors Capstan.Config's ceiling: the reconnect + liveness timers schedule via
  # Process.send_after/3, which raises past 2^32-1 ms.
  @max_timer_ms 4_294_967_295

  @gtid_query "SELECT @@global.gtid_executed, @@global.gtid_purged"
  @set_checksum_query "SET @master_binlog_checksum = @@global.binlog_checksum"

  # A precondition VIOLATION cannot be cured by reconnecting, so it halts fail-closed
  # rather than spending the command budget on a doomed retry.
  @precondition_halts [
    :binlog_format_not_row,
    :binlog_row_image_not_full,
    :binlog_row_metadata_not_full,
    :binlog_row_value_options_not_empty,
    :gtid_mode_not_on,
    :binlog_transaction_compression_on,
    :precondition_query_failed
  ]

  defstruct [
    :server_id,
    :connection,
    :max_command_retries,
    :receiver,
    :receiver_ref,
    :checkpoint_str,
    :checkpoint_store,
    :connect_fun,
    :reconnect_backoff,
    :heartbeat_period_ms,
    :stream_timeout_ms,
    :socket,
    :server_info,
    :connect_started_at,
    :reader,
    :reconnect_timer,
    :liveness_timer,
    command_failures: 0,
    cycle_count: 0,
    liveness_epoch: 0
  ]

  ## ---------------------------------------------------------------------------
  ## public API
  ## ---------------------------------------------------------------------------

  @doc """
  Starts the connection.

  Options: `:server_id` (required), `:connection` (keyword passed to the connect
  function), `:receiver` (pid/name that frames and halts are sent to), `:start_position`
  (`%Capstan.Position{}` or `nil` for a fresh start — the resume position used until the
  first store refresh, and the only source when no store is given), `:checkpoint_store`
  (an optional `{impl_module, handle}` re-read for the current resume position on every
  establish; see "Resume position"), `:max_command_retries` (default 5), `:connect_fun` (a
  1-arg override for the connect+auth step, default the real `gen_tcp` + `Handshake` path),
  `:reconnect_backoff` ms, `:heartbeat_period_ms` (default 15_000 — the master heartbeat
  interval; see "Streaming liveness"), `:stream_timeout_ms` (default 60_000 — the liveness
  window; MUST be `> :heartbeat_period_ms` or start-up fails closed), and `:name`. Every
  liveness/backoff value must be a positive integer within the schedulable
  `Process.send_after` ceiling (2^32-1 ms), mirroring `Capstan.Config.validate/1`.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) when is_list(opts) do
    {name, opts} = Keyword.pop(opts, :name)
    GenServer.start_link(__MODULE__, opts, if(name, do: [name: name], else: []))
  end

  @doc """
  The proactive retention-gap predicate.

  Given the server's `gtid_executed` and `gtid_purged` and the resume `checkpoint`
  (all canonical GTID-set strings), returns `:ok` when the pipeline is healthy, or a
  fail-closed halt. The predicate is on the **unapplied remainder**, not the checkpoint
  itself:

    * a checkpoint carrying GTIDs the server never executed -> `:source_identity_mismatch`
    * `(gtid_executed − checkpoint) ∩ gtid_purged ≠ ∅` -> `:data_gap`

  An **empty** checkpoint is a fresh start with no durable position to lose, so it never
  halts — otherwise every fresh start against a server that has ever purged (i.e. every
  real server) would falsely halt `:data_gap`, an over-rejection this gate must not commit.
  """
  @spec gap_check(String.t(), String.t(), String.t()) ::
          :ok | {:halt, :data_gap | :source_identity_mismatch}
  def gap_check(gtid_executed, gtid_purged, checkpoint)
      when is_binary(gtid_executed) and is_binary(gtid_purged) and is_binary(checkpoint) do
    checkpoint_set = Gtid.parse(checkpoint)

    if Gtid.sources(checkpoint_set) == [] do
      :ok
    else
      executed_set = Gtid.parse(gtid_executed)
      purged_set = Gtid.parse(gtid_purged)
      remainder = Gtid.subtract(executed_set, checkpoint_set)

      cond do
        not Gtid.subset?(checkpoint_set, executed_set) -> {:halt, :source_identity_mismatch}
        not Gtid.disjoint?(remainder, purged_set) -> {:halt, :data_gap}
        true -> :ok
      end
    end
  end

  @doc """
  Discriminates a dump-refusal error into a halt reason (ADR-0003).

  Error 1236 is overloaded: a checksum-negotiation message -> `:checksum_negotiation_failed`;
  a purged-logs message (with or without a named range) -> `:data_gap`; a duplicate-replica
  message (`server_uuid`/`server_id`) -> `:server_id_conflict`; any other 1236 ->
  `:unrecognized_dump_error` (never `:data_gap`). A non-1236 code is surfaced as
  `{:dump_failed, code}`.
  """
  @spec classify_dump_error(non_neg_integer(), binary()) ::
          :checksum_negotiation_failed
          | :data_gap
          | :server_id_conflict
          | :unrecognized_dump_error
          | {:dump_failed, non_neg_integer()}
  def classify_dump_error(1236, message) when is_binary(message) do
    cond do
      bin_contains?(message, "checksum") -> :checksum_negotiation_failed
      bin_contains?(message, "purged") -> :data_gap
      # MySQL 8.0.x evicts a duplicate replica with a 1236 whose message names
      # `server_uuid`/`server_id` ("A replica with the same server_uuid/server_id …"),
      # NOT a bare socket drop — so the `Connection` cycle counter never sees it. Map it
      # to the design's `:server_id_conflict` (Q8) so the halt is actionable, not generic.
      bin_contains?(message, "server_uuid") -> :server_id_conflict
      true -> :unrecognized_dump_error
    end
  end

  def classify_dump_error(code, _message) when is_integer(code), do: {:dump_failed, code}

  ## ---------------------------------------------------------------------------
  ## GenServer callbacks
  ## ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    # Trap exits so the linked streaming reader's death is delivered as a message and
    # handled as a drop, instead of crashing this process.
    Process.flag(:trap_exit, true)

    heartbeat_period_ms = Keyword.get(opts, :heartbeat_period_ms, @default_heartbeat_period_ms)
    stream_timeout_ms = Keyword.get(opts, :stream_timeout_ms, @default_stream_timeout_ms)
    reconnect_backoff = Keyword.get(opts, :reconnect_backoff, @default_reconnect_backoff)

    # The SAME shape Config.validate/1 enforces on the public path — positive integers
    # within the schedulable timer ceiling, window strictly above the heartbeat — so the
    # direct-wiring constructor cannot bypass the public refusal and crash
    # Process.send_after/3 (or silently disable master heartbeats with a zero period)
    # later instead. A liveness window at or below the heartbeat period would false-drop
    # a HEALTHY idle stream — a full heartbeat interval can elapse between frames. Fail
    # closed rather than run a liveness check that fires on a working pipeline.
    if schedulable_ms?(reconnect_backoff) and schedulable_ms?(heartbeat_period_ms) and
         schedulable_ms?(stream_timeout_ms) and stream_timeout_ms > heartbeat_period_ms do
      state = %__MODULE__{
        server_id: Keyword.fetch!(opts, :server_id),
        connection: Keyword.get(opts, :connection, []),
        max_command_retries:
          Keyword.get(opts, :max_command_retries, @default_max_command_retries),
        receiver: Keyword.get(opts, :receiver),
        checkpoint_str: checkpoint_string(Keyword.get(opts, :start_position)),
        checkpoint_store: Keyword.get(opts, :checkpoint_store),
        connect_fun: Keyword.get(opts, :connect_fun, &default_connect/1),
        reconnect_backoff: reconnect_backoff,
        heartbeat_period_ms: heartbeat_period_ms,
        stream_timeout_ms: stream_timeout_ms
      }

      {:ok, monitor_receiver(state), {:continue, :connect}}
    else
      {:stop, {:shutdown, {:halt, :invalid_liveness_config}}}
    end
  end

  # A liveness/backoff value Process.send_after/3 can actually schedule: a positive
  # integer within the 2^32-1 ms timer ceiling.
  defp schedulable_ms?(n) when is_integer(n) and n > 0 and n <= @max_timer_ms, do: true
  defp schedulable_ms?(_n), do: false

  # Monitor the receiver (the `AssemblerServer`) so its death is not silent. The receiver's
  # fail-closed halt stops IT but sends no message back here (the forwarding is a bare
  # `send/2`); without this monitor the Connection would keep streaming binlog events into a
  # dead pid — a silent no-op — while looking healthy, exactly the stall the fail-closed
  # design forbids. A `:DOWN` triggers a fail-closed stop (see the `{:DOWN, _}` handler). A
  # non-pid receiver (a registered name, or `nil` in a unit test) is left unmonitored.
  defp monitor_receiver(%__MODULE__{receiver: receiver} = state) when is_pid(receiver) do
    %{state | receiver_ref: Process.monitor(receiver)}
  end

  defp monitor_receiver(state), do: state

  @impl true
  def handle_continue(:connect, state), do: do_connect(state)

  @impl true
  def handle_info(:reconnect, state) do
    do_connect(%{state | reconnect_timer: nil})
  end

  # A streamed packet from the CURRENT reader.
  def handle_info({:frame, reader, payload}, %{reader: reader} = state) do
    handle_frame(payload, state)
  end

  # A frame from an already-torn-down reader — dropped, never acted on (no stale effect).
  def handle_info({:frame, _stale_reader, _payload}, state), do: {:noreply, state}

  # The current reader died (socket closed / truncated) — an established-then-dropped cycle.
  def handle_info({:EXIT, reader, _reason}, %{reader: reader} = state) do
    handle_drop(%{state | reader: nil})
  end

  def handle_info({:EXIT, _other, _reason}, state), do: {:noreply, state}

  # The liveness timer fired: no frame — event OR heartbeat — arrived within
  # `stream_timeout_ms`, so the stream is silently dead (a half-open partition). Only the
  # CURRENT epoch acts; a stale timeout queued before a reset/cancel is ignored. Make the stall
  # VISIBLE (it was a silent hang), then drop → reconnect, halting `:stream_stalled` if the
  # established-then-dropped budget is exhausted (a persistent half-open partition).
  def handle_info({:liveness_timeout, epoch}, %__MODULE__{liveness_epoch: epoch} = state) do
    emit_stream_timeout()
    state |> stop_reader() |> handle_drop(:stream_stalled)
  end

  def handle_info({:liveness_timeout, _stale}, state), do: {:noreply, state}

  # The monitored receiver (the `AssemblerServer`) died — a fail-closed halt it detected
  # (sink error, checkpoint budget, XA, decode crash) stops it WITHOUT messaging us. Stop the
  # whole pipeline fail-closed rather than keep streaming binlog events into a dead pid (the
  # silent stall). `:temporary` children mean neither restarts into a livelock.
  def handle_info({:DOWN, ref, :process, _pid, _reason}, %__MODULE__{receiver_ref: ref} = state) do
    halt(state, :receiver_down)
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    state
    |> stop_reader()
    |> cancel_liveness()
    |> cancel_reconnect_timer()
    |> close_current_socket()

    :ok
  end

  ## ---------------------------------------------------------------------------
  ## connect + establish
  ## ---------------------------------------------------------------------------

  defp do_connect(state) do
    state = %{state | connect_started_at: System.monotonic_time()}

    case state.connect_fun.(state.connection) do
      {:ok, socket, info} -> establish_guarded(%{state | socket: socket, server_info: info})
      {:error, _reason} -> note_command_failure(state)
    end
  end

  # `establish/1` can RAISE on untrusted server input: `Command.query` -> `Packet.read_packet`
  # raises on a transport error, and `gap_check/3` -> `Gtid.parse` raises `ArgumentError` on a
  # malformed `@@gtid_executed`/`@@gtid_purged`. Fail closed value-free instead of crashing the
  # `:temporary` Connection (which would neither restart nor emit a halt, and could leak the
  # server bytes via the OTP crash report). Treat it as a command failure — spend the budget,
  # reconnect, and halt `:command_retries_exhausted` if it persists. `state` carries the open
  # socket, so `note_command_failure/1` closes it (no fd leak); the exception is discarded.
  defp establish_guarded(state) do
    establish(state)
  rescue
    _exception -> note_command_failure(state)
  catch
    _kind, _reason -> note_command_failure(state)
  end

  defp establish(state) do
    # Re-read the durable resume position BEFORE the gap check and the dump, so a reconnect
    # resumes from the current watermark, not the frozen start-up position (see the
    # "Resume position" moduledoc — the false-`:data_gap`-after-purge correctness bug).
    state = refresh_checkpoint(state)

    with :ok <- preconditions(state.socket),
         {:ok, executed, purged} <- gtid_sets(state.socket),
         :ok <- gap_check(executed, purged, state.checkpoint_str),
         :ok <- set_checksum(state.socket),
         :ok <- set_heartbeat(state.socket, state.heartbeat_period_ms),
         :ok <- send_dump(state) do
      start_streaming(state)
    else
      {:halt, reason} -> halt(state, reason)
      {:command_error, _reason} -> note_command_failure(state)
    end
  end

  # Read-only refresh of the resume position from the checkpoint store (design Q7): the
  # store is the single source of truth for "where to resume", written only by the
  # `AssemblerServer`. No store configured (a unit test wired with `:start_position` alone)
  # keeps the injected position; a read fault keeps the last-known position and proceeds
  # (never worse than the frozen behaviour — the next reconnect refreshes).
  defp refresh_checkpoint(%__MODULE__{checkpoint_store: nil} = state), do: state

  defp refresh_checkpoint(%__MODULE__{checkpoint_store: {impl, store}} = state) do
    case CheckpointStore.read_position(impl, store) do
      {:ok, position} -> %{state | checkpoint_str: checkpoint_string(position)}
      {:error, _reason} -> state
    end
  end

  defp preconditions(socket) do
    case Config.check_preconditions(socket) do
      :ok -> :ok
      {:error, reason} when reason in @precondition_halts -> {:halt, reason}
      {:error, other} -> {:command_error, other}
    end
  end

  defp gtid_sets(socket) do
    case Command.query(socket, @gtid_query) do
      {:ok, [[executed, purged]]} when is_binary(executed) and is_binary(purged) ->
        {:ok, executed, purged}

      {:ok, _unexpected} ->
        {:command_error, :gtid_read_unexpected}

      :ok ->
        {:command_error, :gtid_read_unexpected}

      {:error, reason} ->
        {:command_error, reason}
    end
  end

  # F4: the checksum negotiation MUST precede the dump, or the server refuses with 1236.
  defp set_checksum(socket) do
    case Command.query(socket, @set_checksum_query) do
      :ok -> :ok
      {:ok, _rows} -> :ok
      {:error, reason} -> {:command_error, reason}
    end
  end

  # Ask the master to emit a HEARTBEAT_LOG_EVENT after `heartbeat_period_ms` of stream
  # idleness, so the parent's liveness timer has a signal to reset on even when no real events
  # flow. `@master_heartbeat_period` is in NANOSECONDS (verified live). Must precede the dump.
  defp set_heartbeat(socket, heartbeat_period_ms) do
    period_ns = heartbeat_period_ms * 1_000_000

    case Command.query(socket, "SET @master_heartbeat_period = #{period_ns}") do
      :ok -> :ok
      {:ok, _rows} -> :ok
      {:error, reason} -> {:command_error, reason}
    end
  end

  defp send_dump(state) do
    checkpoint_set = Gtid.parse(state.checkpoint_str)
    dump = Command.com_binlog_dump_gtid(state.server_id, checkpoint_set)

    case Packet.send_packet(state.socket, dump, 0) do
      :ok -> :ok
      {:error, reason} -> {:command_error, {:transport, reason}}
    end
  end

  defp start_streaming(state) do
    case start_reader(state) do
      {:ok, state} ->
        state = schedule_liveness(state)
        emit_established(state)
        {:noreply, state}

      {:error, _reason} ->
        note_command_failure(state)
    end
  end

  ## ---------------------------------------------------------------------------
  ## streaming
  ## ---------------------------------------------------------------------------

  # An event packet: strip the leading 0x00 OK marker and forward the raw event bytes.
  # A frame resets the command budget (A6) and the liveness timer (a heartbeat is a 0x00 frame
  # too, so an idle-but-healthy stream keeps resetting it), but NEVER the cycle counter (C8).
  defp handle_frame(<<0x00, event::binary>>, state) do
    notify_receiver(state, {:binlog_event, event})
    {:noreply, %{reset_liveness(state) | command_failures: 0}}
  end

  # A mid-stream error packet (the dump refusal surfaces here as the first frame).
  defp handle_frame(<<0xFF, code::16-little, message::binary>>, state) do
    state |> stop_reader() |> halt(classify_dump_error(code, message))
  end

  # A clean EOF is still an established-then-dropped cycle.
  defp handle_frame(<<0xFE, _rest::binary>>, state) do
    state |> stop_reader() |> handle_drop()
  end

  defp handle_frame(_unexpected, state) do
    state |> stop_reader() |> halt(:unexpected_stream_packet)
  end

  # The reader owns the socket for the streaming phase (passive recv must run in the
  # owning process) and reads with no recv timeout — a recv-blocked reader on a half-open
  # partition is killed by the PARENT's liveness timer (see "Streaming liveness"). It dies —
  # signalling a drop — when the socket errors.
  defp start_reader(state) do
    parent = self()
    socket = state.socket

    reader =
      spawn_link(fn ->
        receive do
          {:go, ^parent} -> reader_loop(socket, parent)
        end
      end)

    case set_controlling_process(socket, reader) do
      :ok ->
        send(reader, {:go, parent})
        {:ok, %{state | reader: reader}}

      {:error, _reason} = error ->
        Process.unlink(reader)
        Process.exit(reader, :kill)
        error
    end
  end

  defp reader_loop(socket, parent) do
    case safe_read(socket) do
      {:ok, {_seq, payload}} ->
        send(parent, {:frame, self(), payload})
        reader_loop(socket, parent)

      {:error, _reason} ->
        # A socket close/truncation is a NORMAL drop signal, not a crash: return so the
        # process exits `:normal` (no error-log noise). The parent, trapping exits, sees
        # the `{:EXIT, reader, :normal}` and handles the established-then-dropped cycle.
        :ok
    end
  end

  defp safe_read(socket) do
    {:ok, Packet.read_packet(socket, :infinity)}
  rescue
    _ -> {:error, :closed}
  catch
    :exit, _ -> {:error, :closed}
  end

  ## ---------------------------------------------------------------------------
  ## drop / budget / halt
  ## ---------------------------------------------------------------------------

  # An established-then-dropped cycle (Q8 / C8). Frame arrival does NOT reach here, so
  # the cycle counter is never frame-reset; it grows until the livelock is broken. The
  # `exhausted_reason` is what the halt carries when the budget is spent: a clean EOF / socket
  # drop keeps `:server_id_conflict` (the 8.0.x duplicate-replica signature); a liveness-timeout
  # drop passes `:stream_stalled` so a persistent half-open partition is not misdiagnosed as a
  # duplicate replica.
  defp handle_drop(state, exhausted_reason \\ :server_id_conflict) do
    state = state |> cancel_liveness() |> close_current_socket()
    cycle_count = state.cycle_count + 1
    state = %{state | cycle_count: cycle_count}

    if cycle_count > state.max_command_retries do
      halt(state, exhausted_reason)
    else
      schedule_reconnect(state)
    end
  end

  # A failure BEFORE establishing spends the command budget (A6). The budget resets on
  # frame arrival; this counts only pre-establish faults.
  defp note_command_failure(state) do
    state = close_current_socket(state)
    failures = state.command_failures + 1
    state = %{state | command_failures: failures}

    if failures > state.max_command_retries do
      halt(state, :command_retries_exhausted)
    else
      schedule_reconnect(state)
    end
  end

  defp schedule_reconnect(state) do
    timer = Process.send_after(self(), :reconnect, state.reconnect_backoff)
    {:noreply, %{state | reconnect_timer: timer}}
  end

  defp halt(state, reason) do
    state =
      state
      |> stop_reader()
      |> cancel_liveness()
      |> cancel_reconnect_timer()
      |> close_current_socket()

    emit_halt(reason)
    notify_receiver(state, {:capstan_halt, reason})
    {:stop, {:shutdown, {:halt, reason}}, state}
  end

  ## ---------------------------------------------------------------------------
  ## socket / reader / timer teardown
  ## ---------------------------------------------------------------------------

  defp stop_reader(%__MODULE__{reader: nil} = state), do: state

  defp stop_reader(%__MODULE__{reader: reader} = state) do
    # Unlink before the kill so the exit signal is not delivered back as an {:EXIT},
    # and any {:frame, reader, _} still in flight is ignored by the stale-pid guard.
    Process.unlink(reader)
    Process.exit(reader, :kill)
    %{state | reader: nil}
  end

  defp cancel_reconnect_timer(%__MODULE__{reconnect_timer: nil} = state), do: state

  defp cancel_reconnect_timer(%__MODULE__{reconnect_timer: timer} = state) do
    Process.cancel_timer(timer)
    %{state | reconnect_timer: nil}
  end

  # The streaming-liveness timer (half-open partition detection). Scheduling and resetting both
  # go through `schedule_liveness/1`, which cancels any live timer and bumps the epoch so a
  # timeout already queued for a prior window is ignored (`{:liveness_timeout, stale}`). Reset
  # runs on every frame; cancel runs on every drop/halt/teardown.
  defp schedule_liveness(state) do
    state = cancel_liveness(state)
    epoch = state.liveness_epoch
    timer = Process.send_after(self(), {:liveness_timeout, epoch}, state.stream_timeout_ms)
    %{state | liveness_timer: timer}
  end

  defp reset_liveness(state), do: schedule_liveness(state)

  defp cancel_liveness(%__MODULE__{liveness_timer: timer, liveness_epoch: epoch} = state) do
    if timer, do: Process.cancel_timer(timer)
    %{state | liveness_timer: nil, liveness_epoch: epoch + 1}
  end

  defp close_current_socket(%__MODULE__{socket: nil} = state), do: state

  defp close_current_socket(%__MODULE__{socket: socket} = state) do
    close_socket(socket)
    %{state | socket: nil}
  end

  defp close_socket({:gen_tcp, sock}), do: :gen_tcp.close(sock)
  defp close_socket({:ssl, sock}), do: :ssl.close(sock)

  defp set_controlling_process({:gen_tcp, sock}, pid), do: :gen_tcp.controlling_process(sock, pid)
  defp set_controlling_process({:ssl, sock}, pid), do: :ssl.controlling_process(sock, pid)

  ## ---------------------------------------------------------------------------
  ## receiver + telemetry
  ## ---------------------------------------------------------------------------

  defp notify_receiver(%__MODULE__{receiver: nil}, _message), do: :ok
  defp notify_receiver(%__MODULE__{receiver: receiver}, message), do: send(receiver, message)

  # Routed through `Capstan.Telemetry.event/3` so the value-free metadata allowlist gates
  # every payload at runtime (Rule 1 completion, F11): a future emitter attaching a row
  # value or password raises rather than shipping it.
  defp emit_established(state) do
    establish_ms = monotonic_ms_since(state)

    Telemetry.event(
      [:capstan, :connection, :established],
      %{establish_ms: establish_ms},
      %{
        server_version: server_info(state, :server_version),
        tls: server_info(state, :tls)
      }
    )
  end

  defp monotonic_ms_since(%__MODULE__{connect_started_at: started})
       when is_integer(started) do
    System.convert_time_unit(System.monotonic_time() - started, :native, :millisecond)
  end

  defp emit_halt(reason) do
    Telemetry.event([:capstan, :connection, :halt], %{}, %{reason: reason})
  end

  # A stream stall (the liveness timer fired): the pipeline was hung on a silently-dead stream
  # and is now reconnecting. Value-free (a bare reason atom) — makes the previously-silent hang
  # visible to an operator's monitoring.
  defp emit_stream_timeout do
    Telemetry.event([:capstan, :connection, :stream_timeout], %{}, %{reason: :stream_stalled})
  end

  defp server_info(%__MODULE__{server_info: info}, key) when is_map(info),
    do: Map.get(info, key)

  defp server_info(_state, _key), do: nil

  ## ---------------------------------------------------------------------------
  ## default connect (real gen_tcp + Handshake)
  ## ---------------------------------------------------------------------------

  defp default_connect(connection) do
    host = connection |> Keyword.fetch!(:host) |> normalize_host()
    port = Keyword.fetch!(connection, :port)
    timeout = Keyword.get(connection, :timeout, @default_connect_timeout)

    # `keepalive: true` is the OS-level backstop for a fully-dead peer; the parent's liveness
    # timer is the PRIMARY bounded detector (keepalive's default idle time is ~2h, far too slow).
    case :gen_tcp.connect(host, port, [:binary, active: false, keepalive: true], timeout) do
      {:ok, raw} -> authenticate(raw, connection)
      {:error, reason} -> {:error, reason}
    end
  end

  defp authenticate(raw, connection) do
    # Handshake.connect/2 owns the socket lifetime end-to-end: on any {:error, _} it
    # has already closed whatever socket it held — the raw {:gen_tcp, _} before a TLS
    # upgrade, or the {:ssl, _} after. Closing `raw` here would double-free the fd
    # and, on the post-upgrade path, orphan the live :ssl process (whose only handle
    # is the {:ssl, _} we never see), so we must NOT close on the error branch.
    case Handshake.connect({:gen_tcp, raw}, connection) do
      {:ok, %{socket: socket} = info} ->
        {:ok, socket, info}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp normalize_host(host) when is_binary(host), do: String.to_charlist(host)
  defp normalize_host(host) when is_list(host), do: host

  ## ---------------------------------------------------------------------------
  ## helpers
  ## ---------------------------------------------------------------------------

  defp checkpoint_string(nil), do: ""
  defp checkpoint_string(%Position{gtid_set: gtid_set}) when is_binary(gtid_set), do: gtid_set

  defp bin_contains?(haystack, needle), do: :binary.match(haystack, needle) != :nomatch
end

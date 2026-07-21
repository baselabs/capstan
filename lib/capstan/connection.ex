defmodule Capstan.Connection do
  @moduledoc """
  The socket-owning GenServer: handshake, auth, the fail-closed gap gate, the dump,
  and frame forwarding.

  `Connection` owns ONLY the TCP/TLS socket. It never delivers to a sink and never
  touches the checkpoint — `Capstan.AssemblerServer` owns those. That split keeps the
  reconnect seam auditable: every socket-lifetime primitive (the reconnect timer, the
  streaming reader) lives in this one module and is torn down with the connection it
  serves (design Q7).

  ## Lifecycle

      connect + auth  (via the injected connect_fun; default: gen_tcp + Handshake)
        -> Config.check_preconditions/1        (fail-closed server gate, Q5)
        -> read @@gtid_executed / @@gtid_purged
        -> gap_check/3                          (proactive retention gap, F1 / Q4)
        -> SET @master_binlog_checksum          (F4 — required BEFORE the dump)
        -> COM_BINLOG_DUMP_GTID                 (resume from the start position)
        -> stream frames -> receiver

  ## Receiver contract

  Each streamed binlog event is forwarded as `{:binlog_event, raw_event_bytes}` where
  `raw_event_bytes` is the 19-byte header + body + CRC — exactly what
  `Capstan.Binlog.Event.parse/1` consumes (this module never decodes; that is the
  assembler's job). A fail-closed halt is delivered as `{:capstan_halt, reason}` and
  then the process stops with `{:shutdown, {:halt, reason}}`.

  ## Two independent counters (design Q8)

    * **Command budget** (`max_command_retries`, default 5): counts failures that occur
      **before** the connection establishes (connect/auth/query/dump-send errors). It
      is RESET when a frame arrives — replicant's A6 policy, correct for transient
      pre-establish faults. Halts `:command_retries_exhausted` on the `max + 1`-th
      failure.
    * **Cycle counter**: counts **established-then-dropped** cycles. It is **NOT** reset
      by frame arrival, and is reset only by clean shutdown. A duplicate `server_id`
      makes MySQL evict this replica after each establish — authenticating and receiving
      frames before dying — so a frame-reset counter (A6 verbatim) would livelock
      forever while emitting healthy `:established` telemetry. Halts `:server_id_conflict`
      on the `max + 1`-th cycle.

  ## Error 1236 is OVERLOADED (design A2 / F5)

  A dump refusal (error 1236) is discriminated on its message text: checksum-negotiation
  text -> `:checksum_negotiation_failed`; purged-logs text (with OR without a named
  range) -> `:data_gap`; anything else -> `:unrecognized_dump_error`, **never**
  `:data_gap` — mapping 1236 unconditionally to a gap would tell an operator to
  reprovision past what is actually a config bug, manufacturing the silent loss the gate
  exists to prevent.
  """

  use GenServer

  alias Capstan.Config
  alias Capstan.Gtid
  alias Capstan.Position
  alias Capstan.Protocol.Command
  alias Capstan.Protocol.Handshake
  alias Capstan.Protocol.Packet

  @default_max_command_retries 5
  @default_reconnect_backoff 1_000
  @default_connect_timeout 20_000

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
    :precondition_query_failed
  ]

  defstruct [
    :server_id,
    :connection,
    :max_command_retries,
    :receiver,
    :checkpoint_str,
    :connect_fun,
    :reconnect_backoff,
    :socket,
    :server_info,
    :reader,
    :reconnect_timer,
    command_failures: 0,
    cycle_count: 0
  ]

  ## ---------------------------------------------------------------------------
  ## public API
  ## ---------------------------------------------------------------------------

  @doc """
  Starts the connection.

  Options: `:server_id` (required), `:connection` (keyword passed to the connect
  function), `:receiver` (pid/name that frames and halts are sent to), `:start_position`
  (`%Capstan.Position{}` or `nil` for a fresh start), `:max_command_retries`
  (default 5), `:connect_fun` (a 1-arg override for the connect+auth step, default the
  real `gen_tcp` + `Handshake` path), `:reconnect_backoff` ms, and `:name`.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) when is_list(opts) do
    {name, opts} = Keyword.pop(opts, :name)
    GenServer.start_link(__MODULE__, opts, if(name, do: [name: name], else: []))
  end

  @doc """
  The proactive retention-gap predicate (design Q4 / F1).

  Given the server's `gtid_executed` and `gtid_purged` and the resume `checkpoint`
  (all canonical GTID-set strings), returns `:ok` when the pipeline is healthy, or a
  fail-closed halt. The predicate is on the **unapplied remainder**, not the checkpoint
  itself:

    * a checkpoint carrying GTIDs the server never executed -> `:source_identity_mismatch`
    * `(gtid_executed − checkpoint) ∩ gtid_purged ≠ ∅` -> `:data_gap`

  An **empty** checkpoint is a fresh start with no durable position to lose, so it never
  halts — otherwise every fresh start against a server that has ever purged (i.e. every
  real server) would falsely halt `:data_gap`, the over-rejection Q4 forbids.
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
  Discriminates a dump-refusal error into a halt reason (design A2 / F5).

  Error 1236 is overloaded: a checksum-negotiation message -> `:checksum_negotiation_failed`;
  a purged-logs message (with or without a named range) -> `:data_gap`; any other 1236 ->
  `:unrecognized_dump_error` (never `:data_gap`). A non-1236 code is surfaced as
  `{:dump_failed, code}`.
  """
  @spec classify_dump_error(non_neg_integer(), binary()) ::
          :checksum_negotiation_failed
          | :data_gap
          | :unrecognized_dump_error
          | {:dump_failed, non_neg_integer()}
  def classify_dump_error(1236, message) when is_binary(message) do
    cond do
      bin_contains?(message, "checksum") -> :checksum_negotiation_failed
      bin_contains?(message, "purged") -> :data_gap
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

    state = %__MODULE__{
      server_id: Keyword.fetch!(opts, :server_id),
      connection: Keyword.get(opts, :connection, []),
      max_command_retries: Keyword.get(opts, :max_command_retries, @default_max_command_retries),
      receiver: Keyword.get(opts, :receiver),
      checkpoint_str: checkpoint_string(Keyword.get(opts, :start_position)),
      connect_fun: Keyword.get(opts, :connect_fun, &default_connect/1),
      reconnect_backoff: Keyword.get(opts, :reconnect_backoff, @default_reconnect_backoff)
    }

    {:ok, state, {:continue, :connect}}
  end

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

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    state
    |> stop_reader()
    |> cancel_reconnect_timer()
    |> close_current_socket()

    :ok
  end

  ## ---------------------------------------------------------------------------
  ## connect + establish
  ## ---------------------------------------------------------------------------

  defp do_connect(state) do
    case state.connect_fun.(state.connection) do
      {:ok, socket, info} -> establish(%{state | socket: socket, server_info: info})
      {:error, _reason} -> note_command_failure(state)
    end
  end

  defp establish(state) do
    with :ok <- preconditions(state.socket),
         {:ok, executed, purged} <- gtid_sets(state.socket),
         :ok <- gap_check(executed, purged, state.checkpoint_str),
         :ok <- set_checksum(state.socket),
         :ok <- send_dump(state) do
      start_streaming(state)
    else
      {:halt, reason} -> halt(state, reason)
      {:command_error, _reason} -> note_command_failure(state)
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
  # A frame resets the command budget (A6) but NEVER the cycle counter (C8).
  defp handle_frame(<<0x00, event::binary>>, state) do
    notify_receiver(state, {:binlog_event, event})
    {:noreply, %{state | command_failures: 0}}
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
  # owning process), reads with no timeout (heartbeats keep a healthy stream flowing),
  # and dies — signalling a drop — when the socket errors.
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
  # the cycle counter is never frame-reset; it grows until the livelock is broken.
  defp handle_drop(state) do
    state = close_current_socket(state)
    cycle_count = state.cycle_count + 1
    state = %{state | cycle_count: cycle_count}

    if cycle_count > state.max_command_retries do
      halt(state, :server_id_conflict)
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

  defp emit_established(state) do
    :telemetry.execute(
      [:capstan, :connection, :established],
      %{},
      %{
        server_version: server_info(state, :server_version),
        tls: server_info(state, :tls)
      }
    )
  end

  defp emit_halt(reason) do
    :telemetry.execute([:capstan, :connection, :halt], %{}, %{reason: reason})
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

    case :gen_tcp.connect(host, port, [:binary, active: false], timeout) do
      {:ok, raw} -> authenticate(raw, connection)
      {:error, reason} -> {:error, reason}
    end
  end

  defp authenticate(raw, connection) do
    case Handshake.connect({:gen_tcp, raw}, connection) do
      {:ok, %{socket: socket} = info} ->
        {:ok, socket, info}

      {:error, reason} ->
        :gen_tcp.close(raw)
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

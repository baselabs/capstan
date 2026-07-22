defmodule Capstan.AssemblerServer do
  @moduledoc """
  The composition point: the GenServer that turns a `Capstan.Connection`'s frame stream
  into durable, fail-closed, at-least-once delivery (lib-owned checkpoint mode — the
  checkpoint advances only after the sink's `{:ok, _}`, so a crash in between re-delivers;
  see `Capstan` and ADR-0004. Effect-once is the deferred sink-owned atomic path).

  `Connection` owns ONLY the socket and forwards `{:binlog_event, raw}` (the raw 19-byte
  header + body + CRC — exactly `Capstan.Binlog.Event.parse/1`'s input) and
  `{:capstan_halt, reason}`. This server owns everything downstream of the wire:
  the assembly fold, the sink delivery, the checkpoint, and every fail-closed halt.
  It never touches the socket. That split makes the reconnect path auditable — the
  recurring OTP-async-lifetime bug family lives exactly at a process owning both.

  ## Flow per binlog event

      Event.parse/1                       # header + CRC verify; a failure HALTS fail-closed
        -> Assembler.step/2               # the pure three-terminator fold (ADR-0003)
             {:cont, [output], next}      # for each output, the delivery + checkpoint below
             {:halt, reason}              # XA_PREPARE -> HALT, do not advance
             {:error, reason}            # any refused/desynced event -> HALT, do not advance

  A `%Capstan.Transaction{}` or `%Capstan.SchemaChange{}` output runs, in order:

    1. **Dedup** — if its GTID is already in the processed set (`Capstan.Gtid.member?/2`,
       never hand-rolled), the output is SKIPPED: the sink is not called and the
       checkpoint is not touched, emitting `:already_processed` (effect-once).
    2. **Filtered / empty transaction** (`changes: []`, ADR-0003) — advances the checkpoint
       with NO sink call. A long run of filtered transactions therefore never stalls the
       watermark.
    3. **Delivered transaction** — `Sink.handle_transaction/1` FIRST; the checkpoint is
       written ONLY after it returns `{:ok, _}`. A `{:error, _}` HALTS fail-closed
       WITHOUT advancing, so the failed transaction is re-delivered on restart
       (at-least-once). This is the checkpoint-after-`{:ok, _}` ordering.
    4. **Schema change** (self-committing DDL, Q13) — `Sink.handle_schema_change/2`, then
       the advanced watermark is checkpointed. `{:error, _}` HALTS without advancing.

  Every halt — a `{:capstan_halt, _}` from the Connection, an assembler `{:halt, _}` /
  `{:error, _}`, an event-parse failure, a sink error, or an exhausted checkpoint-write
  budget — stops the pipeline with `{:shutdown, {:halt, reason}}` and never checkpoints
  past the failure. Fail closed is the whole point: silently continuing past any of these
  is the data-loss class this server exists to prevent.

  ## Checkpoint mode

  C1 implements **lib-owned** checkpoint mode: a `Capstan.CheckpointStore` persists the
  processed `gtid_set`. The server resumes from `read_position/2` at startup (a read fault
  fails closed rather than resume from a guessed position; `nil` is a fresh empty start)
  and advances via `write_position/3`. A write fault is retried per the shared budget
  (`retry_decision/2`) and then halts fail-closed. Sink-owned checkpoint mode (the sink
  persisting the position atomically with its own write and returning it via
  `c:Capstan.Sink.checkpoint/0`) is not wired in this server.

  ## Why a separate processed set

  The pure `Capstan.Assembler` unions each transaction's GTID into its running fold and
  returns the transaction's position INCLUDING it, so the assembler's own set can never
  answer "was this already processed?". This server keeps the durable processed watermark
  separately: it is the resumed checkpoint, advanced only when a delivery is durably
  checkpointed, and it is what dedup consults BEFORE delivering.
  """

  use GenServer

  alias Capstan.Assembler
  alias Capstan.Binlog.Event
  alias Capstan.CheckpointStore
  alias Capstan.Error
  alias Capstan.Gtid
  alias Capstan.Position
  alias Capstan.SchemaChange
  alias Capstan.Telemetry
  alias Capstan.Transaction

  @type checkpoint_store :: {module(), CheckpointStore.store()}

  defstruct [
    :assembler,
    :sink,
    :checkpoint_impl,
    :checkpoint_store,
    :processed_set,
    :max_retries,
    # C2 snapshot spine hooks (both absent in all of C1 ⇒ byte-identical behavior):
    #   * `:watermark_observer` — an optional pid notified `{:capstan_watermark, gtid_set}`
    #     at the checkpoint choke point, so a co-running snapshot coordinator sees EVERY
    #     watermark advance (delivered, filtered, AND self-committing DDL) by construction.
    #   * `:coordinator_ref` — the `Process.monitor/1` ref of an attached snapshot coordinator;
    #     a silent coordinator death halts fail-closed (`:snapshot_coordinator_down`).
    :watermark_observer,
    :coordinator_ref
  ]

  ## ---------------------------------------------------------------------------
  ## public API
  ## ---------------------------------------------------------------------------

  @doc """
  Starts the assembler server.

  Options:

    * `:sink` (required) — the `Capstan.Sink` callback module.
    * `:checkpoint_store` (required) — `{impl_module, store_handle}` for lib-owned mode.
    * `:tables` — the table filter passed to `Capstan.Assembler` (`:all`, the default, or
      an allowlist of `{schema, table}` pairs).
    * `:max_retries` — the checkpoint-write retry budget (default
      `Capstan.CheckpointStore.default_max_retries/0`).
    * `:watermark_observer` — an optional pid notified `{:capstan_watermark, gtid_set}` on
      every checkpoint advance (default `nil` ⇒ no notify; the C2 snapshot coordinator's
      advance-gate feed). Also injectable post-start via `attach_coordinator/2`.
    * `:name` — an optional registered name.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) when is_list(opts) do
    {name, opts} = Keyword.pop(opts, :name)
    GenServer.start_link(__MODULE__, opts, if(name, do: [name: name], else: []))
  end

  @doc """
  Injects the snapshot coordinator AFTER both processes are up (deferred injection —
  design § Pinned decisions #4, mirrors the `Capstan.Connection` `:receiver_down` monitor).

  The cast (a) sets `coordinator_pid` as the `:watermark_observer`, so every subsequent
  checkpoint advance notifies it, and (b) arms a `Process.monitor/1` on it, so a silent
  coordinator death halts the pipeline fail-closed with `:snapshot_coordinator_down` rather
  than leaving the stream feeding a dead sink (a silently stranded backfill is a gap). Until
  this is called — and in all of C1 — both are absent, so behavior is byte-identical.
  """
  @spec attach_coordinator(GenServer.server(), pid()) :: :ok
  def attach_coordinator(server, coordinator_pid) when is_pid(coordinator_pid) do
    GenServer.cast(server, {:attach_coordinator, coordinator_pid})
  end

  ## ---------------------------------------------------------------------------
  ## GenServer callbacks
  ## ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    sink = Keyword.fetch!(opts, :sink)
    {impl, store} = Keyword.fetch!(opts, :checkpoint_store)
    tables = Keyword.get(opts, :tables, :all)
    max_retries = Keyword.get(opts, :max_retries, CheckpointStore.default_max_retries())
    watermark_observer = Keyword.get(opts, :watermark_observer)

    case CheckpointStore.read_position(impl, store) do
      {:ok, resumed} ->
        start_position = resumed || %Position{gtid_set: "", file: nil, pos: nil}

        state = %__MODULE__{
          assembler: Assembler.new(start_position, tables: tables),
          sink: sink,
          checkpoint_impl: impl,
          checkpoint_store: store,
          processed_set: Gtid.parse(start_position.gtid_set),
          max_retries: max_retries,
          watermark_observer: watermark_observer,
          coordinator_ref: nil
        }

        {:ok, state}

      {:error, reason} ->
        # Fail closed: never resume the pipeline from a guessed position.
        {:stop, {:shutdown, {:halt, {:checkpoint_read_failed, reason}}}}
    end
  end

  @impl true
  def handle_info({:binlog_event, raw}, state) when is_binary(raw) do
    case Event.parse(raw) do
      {:ok, event} -> apply_event(event, state)
      # A CRC mismatch / truncation is an integrity failure, never silently skipped.
      {:error, reason} -> halt(state, {:event_parse_failed, reason})
    end
  rescue
    # A raise from the DELIVERY path — a sink or store that raises instead of returning
    # `{:error, _}`. Fail closed value-free rather than crash: a crash here would put the
    # in-flight `{:binlog_event, raw}` message (the raw event bytes — row values / DDL SQL)
    # into the OTP crash report's "Last message", a Rule-1 breach of the same class the
    # decode guard closes. (The decode path itself is caught earlier by `step_guarded/2`
    # with the more specific `:event_decode_crashed` reason, so this is the delivery arm.)
    exception -> halt(state, {:event_processing_crashed, Error.from(exception)})
  catch
    _kind, _reason -> halt(state, {:event_processing_crashed, Error.from(:unknown)})
  end

  # A fail-closed halt propagated from the Connection (design Q7): stop the pipeline
  # WITHOUT advancing the checkpoint. The Connection already emitted its own
  # `connection.halt` telemetry, so this uses the non-emitting `stop_halt/2` — re-emitting
  # here would double-report the same halt.
  def handle_info({:capstan_halt, reason}, state), do: stop_halt(state, reason)

  # The monitored snapshot coordinator (attached via `attach_coordinator/2`) died silently —
  # a fault that stops IT without messaging us. Halt the pipeline fail-closed rather than keep
  # advancing the watermark into a dead sink (a stranded backfill is a gap). Uses the EMITTING
  # `halt/2` so the stop is visible to monitoring. Mirrors `connection.ex:337-338`'s
  # `:receiver_down`. A `{:DOWN, _}` whose ref is not the monitored coordinator's falls through
  # to the passthrough clause below (no coordinator attached ⇒ `coordinator_ref` is `nil`,
  # which never matches a real ref).
  def handle_info(
        {:DOWN, ref, :process, _pid, _reason},
        %__MODULE__{coordinator_ref: ref} = state
      ) do
    halt(state, :snapshot_coordinator_down)
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def handle_cast({:attach_coordinator, coordinator_pid}, state) do
    # Deferred injection (design § Pinned decisions #4): set the observer AND arm the monitor
    # in one step, so the coordinator sees every watermark advance and its silent death is loud.
    ref = Process.monitor(coordinator_pid)
    {:noreply, %{state | watermark_observer: coordinator_pid, coordinator_ref: ref}}
  end

  ## ---------------------------------------------------------------------------
  ## event application
  ## ---------------------------------------------------------------------------

  defp apply_event(event, state) do
    case step_guarded(state.assembler, event) do
      {:cont, outputs, next} ->
        deliver_outputs(outputs, %{state | assembler: next})

      # A transaction-shape halt (XA_PREPARE) — do not advance past it.
      {:halt, reason} ->
        halt(state, reason)

      # Any refused/desynced event — fail closed rather than checkpoint past the failure.
      {:error, reason} ->
        halt(state, {:assembler_error, reason})

      # A CRC-valid but structurally-malformed event whose per-type decode/cast RAISED — a
      # latent decoder/layout mismatch (an unsupported column/temporal/charset variant) or a
      # stream desync. Rule 1: the raw exception embeds row/DDL bytes (a
      # `%MatchError{term: <row bytes>}`, a negative binary-size `ArgumentError`); letting it
      # propagate would echo them into the OTP crash report. `step_guarded/2` has already
      # scrubbed it to a value-free `Capstan.Error`, and the fail-closed
      # `{:shutdown, {:halt, _}}` exit is never reported abnormally — so a decode crash halts
      # value-free rather than leaking.
      {:decode_crashed, error} ->
        halt(state, {:event_decode_crashed, error})
    end
  end

  # Guard the pure decode/assembly fold. A structurally-malformed event can RAISE inside
  # `Assembler.step/2` (the per-type binlog decoders hard-match on body layout; column
  # casting can raise on an unexpected shape). Convert any raise/throw/exit to a value-free
  # sentinel so this boundary honours the fail-closed `{:error, _}` / `{:halt, _}` contract
  # instead of crashing — a crash would both leak row bytes (via the OTP crash report) and,
  # under the pipeline's `:temporary` / plain-`send` wiring, strand the Connection. The
  # exception's message/`term` (the bytes) is dropped by `Capstan.Error.from/1`; the `catch`
  # arm discards its raw reason for the same Rule-1 reason.
  defp step_guarded(assembler, event) do
    Assembler.step(assembler, event)
  rescue
    exception -> {:decode_crashed, Error.from(exception)}
  catch
    _kind, _reason -> {:decode_crashed, Error.from(:unknown)}
  end

  defp deliver_outputs([], state), do: {:noreply, state}

  defp deliver_outputs([output | rest], state) do
    case deliver_one(output, state) do
      {:ok, state} -> deliver_outputs(rest, state)
      {:halt, reason} -> halt(state, reason)
    end
  end

  # Dedup FIRST, for every output type: an already-processed GTID is skipped effect-once,
  # never re-delivered and never re-checkpointed.
  defp deliver_one(output, state) do
    gtid = output_gtid(output)

    if Gtid.member?(state.processed_set, single_gtid(gtid)) do
      emit_skipped(gtid)
      {:ok, state}
    else
      dispatch(output, state)
    end
  end

  # A fully-filtered / empty transaction advances the watermark with NO sink call (Q14),
  # so a long filtered quiet period never stalls the position.
  #
  # COUPLING (C1-correct, guard for C3): this matches `changes` as a concrete `[]`. The
  # `Transaction` contract types `changes` as `Enumerable.t()`, and C3 will make it a
  # lazy single-pass enumerable — an empty `Stream` would NOT match `[]` and would fall
  # through to the delivered clause, calling the sink with zero changes (a Q14
  # regression). `Enum.empty?/1` is NOT the fix: it consumes the first element of a
  # single-pass enumerable. When C3 lands, the Assembler must emit an explicit
  # filtered/empty signal rather than have this site infer it from the representation.
  defp dispatch(%Transaction{changes: []} = txn, state) do
    with {:ok, state} <- checkpoint(state, txn.position) do
      emit_filtered(txn.gtid)
      {:ok, state}
    end
  end

  # A delivered transaction: the sink FIRST, the checkpoint ONLY after `{:ok, _}`
  # (at-least-once). A sink error halts fail-closed WITHOUT advancing.
  defp dispatch(%Transaction{} = txn, state) do
    case state.sink.handle_transaction(txn) do
      {:ok, _position} ->
        with {:ok, state} <- checkpoint(state, txn.position) do
          emit_committed(txn.gtid)
          {:ok, state}
        end

      {:error, reason} ->
        {:halt, {:sink_error, reason}}
    end
  end

  # A self-committing DDL schema change (Q13): deliver, then checkpoint the advanced
  # watermark. The `%SchemaChange{}` carries no position, so the watermark comes from the
  # assembler's post-step position.
  #
  # COUPLING (C1-correct, guard for a future multi-output step): reading the assembler's
  # POST-step position is exact only because `Assembler.step/2` emits at most ONE output
  # per event in C1 (every `apply_decoded` clause returns `[]` or a single element). If a
  # step ever emitted multiple outputs with a `%SchemaChange{}` not last, that position
  # would already include a later, not-yet-delivered output's GTID — silently advancing
  # the watermark past undelivered work (an effect-once violation). A future multi-output
  # Assembler must carry a per-output position (as `%Transaction{}` already does) rather
  # than have this site read the shared post-step position.
  defp dispatch(%SchemaChange{} = schema_change, state) do
    position = Assembler.position(state.assembler)

    case state.sink.handle_schema_change(schema_change, position) do
      :ok ->
        with {:ok, state} <- checkpoint(state, position) do
          emit_schema_change(schema_change)
          {:ok, state}
        end

      {:error, reason} ->
        {:halt, {:sink_error, reason}}
    end
  end

  ## ---------------------------------------------------------------------------
  ## checkpoint (lib-owned) — advance only on a durable write; budgeted retry
  ## ---------------------------------------------------------------------------

  defp checkpoint(state, position), do: do_checkpoint(state, position, 0)

  # A transient write fault is retried per the shared budget (`retry_decision/2`) and then
  # halts fail-closed; a permanent reason halts immediately without spending the budget.
  # The retry is synchronous (the store is local and the GenServer must not block on a
  # timer here) and bounded by `max_retries`.
  defp do_checkpoint(state, position, retries) do
    case CheckpointStore.write_position(state.checkpoint_impl, state.checkpoint_store, position) do
      :ok ->
        # The choke point ALL three advance paths route through (delivered, filtered, DDL), so
        # the watermark observer fires on EVERY advance by construction (Ch6). The payload is
        # the canonical GTID-set STRING — never a row value (Rule 1). `nil` observer ⇒ no send.
        notify_watermark(state.watermark_observer, position.gtid_set)
        {:ok, %{state | processed_set: Gtid.parse(position.gtid_set)}}

      {:error, reason} ->
        cond do
          CheckpointStore.permanent_reason?(reason) ->
            {:halt, {:checkpoint_write_failed, reason}}

          CheckpointStore.retry_decision(retries, state.max_retries) == :retry ->
            do_checkpoint(state, position, retries + 1)

          true ->
            {:halt, {:checkpoint_write_failed, reason}}
        end
    end
  end

  ## ---------------------------------------------------------------------------
  ## halt — fail closed, never checkpoint past this point
  ## ---------------------------------------------------------------------------

  # An AssemblerServer-DETECTED fail-closed halt: emit halt telemetry (so the stop is
  # visible to an operator's monitoring, not only the Connection-side halts), then stop.
  defp halt(state, reason) do
    emit_halt(reason)
    stop_halt(state, reason)
  end

  # Stop the pipeline fail-closed WITHOUT advancing the checkpoint and WITHOUT emitting halt
  # telemetry — used for a halt the Connection already surfaced via its own
  # `connection.halt` payload (the propagated `{:capstan_halt, _}` path).
  defp stop_halt(state, reason), do: {:stop, {:shutdown, {:halt, reason}}, state}

  ## ---------------------------------------------------------------------------
  ## watermark observer (Ch6) — notify a co-running snapshot coordinator on every advance
  ## ---------------------------------------------------------------------------

  # No observer configured (all of C1, every existing test) ⇒ no send: byte-identical.
  defp notify_watermark(nil, _gtid_set), do: :ok

  # The payload is the canonical GTID-set STRING only (Rule 1 — never a row value); a bare
  # `send/2` so a dead observer is a no-op, never a raise (the monitor makes the death loud).
  defp notify_watermark(observer, gtid_set) when is_pid(observer) do
    send(observer, {:capstan_watermark, gtid_set})
    :ok
  end

  ## ---------------------------------------------------------------------------
  ## GTID helpers
  ## ---------------------------------------------------------------------------

  defp output_gtid(%Transaction{gtid: gtid}), do: gtid
  defp output_gtid(%SchemaChange{gtid: gtid}), do: gtid

  # Turn a single `"uuid:gno"` string into the `{uuid, gno}` tuple `Gtid.member?/2` takes,
  # via the public parse + sources path (never hand-rolled interval arithmetic).
  defp single_gtid(gtid_string) do
    [{uuid, [{gno, _high} | _]} | _] = Gtid.sources(Gtid.parse(gtid_string))
    {uuid, gno}
  end

  ## ---------------------------------------------------------------------------
  ## telemetry — routed through Capstan.Telemetry.event/3 so the value-free metadata
  ## allowlist gates every payload at runtime (Rule 1 completion, F11): a stray row value
  ## or password attached to a payload raises rather than shipping.
  ## ---------------------------------------------------------------------------

  defp emit_committed(gtid) do
    Telemetry.event([:capstan, :transaction, :committed], %{}, %{gtid: gtid})
  end

  defp emit_filtered(gtid) do
    Telemetry.event([:capstan, :transaction, :filtered], %{}, %{gtid: gtid})
  end

  defp emit_skipped(gtid) do
    Telemetry.event(
      [:capstan, :transaction, :skipped],
      %{},
      %{gtid: gtid, reason: :already_processed}
    )
  end

  defp emit_schema_change(%SchemaChange{schema: schema, table: table, kind: kind}) do
    Telemetry.event(
      [:capstan, :schema_change, :received],
      %{},
      %{schema: schema, table: table, kind: kind}
    )
  end

  # Every AssemblerServer-side fail-closed halt (sink error, checkpoint-write budget, event
  # parse/decode failure, assembler desync, XA_PREPARE, unmapped table_id) emits here so the
  # most important data-integrity stops are visible to monitoring — not only the
  # Connection-side halts. The reason is scrubbed to its value-free OUTER atom via
  # `Capstan.Error.from/1`: a compound reason like `{:sink_error, <raw sink reason>}` could
  # otherwise carry user data past the metadata allowlist, which gates KEYS, not values.
  defp emit_halt(reason) do
    Telemetry.event([:capstan, :assembler, :halt], %{}, %{reason: Error.from(reason).reason})
  end
end

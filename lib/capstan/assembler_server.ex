defmodule Capstan.AssemblerServer do
  @moduledoc """
  The composition point: the GenServer that turns a `Capstan.Connection`'s frame stream
  into durable, effect-once delivery.

  `Connection` owns ONLY the socket and forwards `{:binlog_event, raw}` (the raw 19-byte
  header + body + CRC — exactly `Capstan.Binlog.Event.parse/1`'s input) and
  `{:capstan_halt, reason}`. This server owns everything downstream of the wire (design
  Q7): the assembly fold, the sink delivery, the checkpoint, and every fail-closed halt.
  It never touches the socket. That split makes the reconnect path auditable — the
  recurring OTP-async-lifetime bug family lives exactly at a process owning both.

  ## Flow per binlog event

      Event.parse/1                       # header + CRC verify; a failure HALTS fail-closed
        -> Assembler.step/2               # the pure three-terminator fold (Q13/Q14)
             {:cont, [output], next}      # for each output, the delivery + checkpoint below
             {:halt, reason}              # XA_PREPARE -> HALT, do not advance
             {:error, reason}            # any refused/desynced event -> HALT, do not advance

  A `%Capstan.Transaction{}` or `%Capstan.SchemaChange{}` output runs, in order:

    1. **Dedup** — if its GTID is already in the processed set (`Capstan.Gtid.member?/2`,
       never hand-rolled), the output is SKIPPED: the sink is not called and the
       checkpoint is not touched, emitting `:already_processed` (effect-once).
    2. **Filtered / empty transaction** (`changes: []`, Q14) — advances the checkpoint
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
  alias Capstan.Gtid
  alias Capstan.Position
  alias Capstan.SchemaChange
  alias Capstan.Transaction

  @type checkpoint_store :: {module(), CheckpointStore.store()}

  defstruct [
    :assembler,
    :sink,
    :checkpoint_impl,
    :checkpoint_store,
    :processed_set,
    :max_retries
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
    * `:name` — an optional registered name.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) when is_list(opts) do
    {name, opts} = Keyword.pop(opts, :name)
    GenServer.start_link(__MODULE__, opts, if(name, do: [name: name], else: []))
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

    case CheckpointStore.read_position(impl, store) do
      {:ok, resumed} ->
        start_position = resumed || %Position{gtid_set: "", file: nil, pos: nil}

        state = %__MODULE__{
          assembler: Assembler.new(start_position, tables: tables),
          sink: sink,
          checkpoint_impl: impl,
          checkpoint_store: store,
          processed_set: Gtid.parse(start_position.gtid_set),
          max_retries: max_retries
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
  end

  # A fail-closed halt propagated from the Connection (design Q7): stop the pipeline
  # WITHOUT advancing the checkpoint. The Connection already emitted its halt telemetry;
  # here the fail-closed action is to stop.
  def handle_info({:capstan_halt, reason}, state), do: halt(state, reason)

  def handle_info(_message, state), do: {:noreply, state}

  ## ---------------------------------------------------------------------------
  ## event application
  ## ---------------------------------------------------------------------------

  defp apply_event(event, state) do
    case Assembler.step(state.assembler, event) do
      {:cont, outputs, next} ->
        deliver_outputs(outputs, %{state | assembler: next})

      # A transaction-shape halt (XA_PREPARE) — do not advance past it.
      {:halt, reason} ->
        halt(state, reason)

      # Any refused/desynced event — fail closed rather than checkpoint past the failure.
      {:error, reason} ->
        halt(state, {:assembler_error, reason})
    end
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

  defp halt(state, reason), do: {:stop, {:shutdown, {:halt, reason}}, state}

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
  ## telemetry — value-free (Task 16 owns telemetry.ex; plain atoms/ints here)
  ## ---------------------------------------------------------------------------

  defp emit_committed(gtid) do
    :telemetry.execute([:capstan, :transaction, :committed], %{}, %{gtid: gtid})
  end

  defp emit_filtered(gtid) do
    :telemetry.execute([:capstan, :transaction, :filtered], %{}, %{gtid: gtid})
  end

  defp emit_skipped(gtid) do
    :telemetry.execute(
      [:capstan, :transaction, :skipped],
      %{},
      %{gtid: gtid, reason: :already_processed}
    )
  end

  defp emit_schema_change(%SchemaChange{schema: schema, table: table, kind: kind}) do
    :telemetry.execute(
      [:capstan, :schema_change, :received],
      %{},
      %{schema: schema, table: table, kind: kind}
    )
  end
end

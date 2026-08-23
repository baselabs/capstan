defmodule Capstan.Assembler do
  @moduledoc """
  The three-terminator fold — a **pure** assembler over a `%Capstan.Binlog.Event{}`
  stream (ADR-0003; the owning GenServer is `Capstan.AssemblerServer`).

  The fold turns raw binlog events into the committed units a `Sink` consumes:
  `%Capstan.Transaction{}` for DML, `%Capstan.SchemaChange{}` for self-committing
  DDL. It threads a running `Capstan.Position` (the processed GTID set) from a
  caller-supplied start, and a `Capstan.Binlog.TableRegistry` built as `TABLE_MAP`
  events arrive.

  ## Why it folds `%Event{}`, not decoded terms

  `Capstan.Binlog.Decoder.decode/1` returns only the per-type body term and **drops**
  the event header. But a `Transaction`'s `commit_ts` is the terminator event's
  header `timestamp` and its diagnostic `pos` is the header `log_pos`. So the fold
  decodes each `%Event{}` via `Decoder.decode/1` for the body term **and** reads the
  header off the same struct for `timestamp`/`log_pos`.

  ## The single biggest failure mode this fold is built AROUND (risk-first)

  A **phantom / rolled-back transaction delivered effect-once** — the silent one.
  Two shapes of it, both defended structurally rather than only by convention:

    * An `XA_PREPARE` (`Decoder` `{:halt, :unsupported_transaction_shape}`): its row
      images may later be `XA ROLLBACK`-ed, so the rows accumulated so far in that
      transaction must be **discarded**, not delivered (ADR-0003). The halt is keyed
      on the `Decoder`'s type-byte signal, never re-derived here.
    * Any mid-transaction failure — an unmapped `table_id`, a `Rows.decode/2` error,
      or a `Decoder` `{:error, _}` — aborts **fail-closed**. A partial transaction
      carrying only the rows decoded before the failure is never emitted.

  Both are guaranteed by the fold's shape: rows accumulate in the in-flight
  transaction buffer and are released **only** when a terminator is reached. A
  `{:halt, _}`/`{:error, _}` short-circuits with that buffer still un-released, so it
  is structurally impossible to leak. The happy path (row → `%Change{}`) is secondary.

  Stream **desyncs** fail closed the same way rather than being silently absorbed: a
  GTID arriving while a transaction is still open (`:gtid_within_open_transaction`),
  or a QUERY / row / terminator with no open GTID scope (`:begin_without_gtid`,
  `:query_without_gtid`, `:rows_without_transaction`, `:terminator_without_transaction`)
  — each aborts rather than dropping or overwriting an open buffer, since a silently
  absorbed desync can mask exactly the loss this fold exists to prevent.

  ## The three terminators (ADR-0003) — anchored on the proven `fixture_capture`
  `classify`/`apply_event` precedent so a terminator is never misclassified (itself a
  silent-loss risk: a `COMMIT` read as DDL would drop the transaction's rows):

    * `XID` — a transactional (InnoDB) commit.
    * `QUERY("COMMIT")` — a non-transactional (MyISAM) commit; the `COMMIT` is the
      terminator, not a change.
    * a self-committing DDL `QUERY` — a `QUERY` that is **not** `BEGIN`/`COMMIT` and
      arrives with **no open `BEGIN`**. Yields a `%SchemaChange{}` and advances the
      position. `BEGIN` opens a DML block; `COMMIT` closes it.

  ## Fully-filtered transactions keep the watermark moving (ADR-0003)

  The checkpoint is a **processed-GTID watermark**, so a committed transaction whose
  changes are all filtered (or which has none) still advances the position and is
  delivered as a `%Transaction{}` with **empty** `changes`. `Capstan.AssemblerServer`
  checkpoints it
  without calling the sink. A long run of filtered transactions therefore never
  stalls the position.

  `Capstan.Config` carries no table filter in C1, so the filter is a **parameter**:
  `tables: :all` (default — nothing filtered) or an allowlist of `{schema, table}`
  pairs, applied **before** row decode.

  ## Rule 1

  No `%Change{}` value, `%SchemaChange{}` DDL text, or error tuple ever carries a row
  value or statement literal. DDL is reduced to `schema`/`table`/`kind` here; the raw
  SQL is inspected only to classify and is then dropped.

  ## Interface

    * `new/2` + `step/2` + `position/1` — the incremental primitive the owning
      GenServer (`Capstan.AssemblerServer`) drives to checkpoint per transaction.
    * `run/3` — folds a whole event sequence, returning every output plus the final
      watermark; the batch entry the tests drive.
  """

  alias Capstan.Binlog.{Decoder, Event, Rows, TableMap, TableRegistry}
  alias Capstan.{Change, Gtid, Position, SchemaChange, Transaction}
  alias Capstan.Xa

  @typedoc "An emitted committed unit."
  @type output :: Transaction.t() | SchemaChange.t()

  @typedoc "The table filter: everything, or an allowlist of `{schema, table}` pairs."
  @type filter :: :all | MapSet.t({String.t(), String.t()})

  # The in-flight transaction buffer. Held only between a GTID and its terminator;
  # released to output at the terminator, discarded on any halt/error.
  @typep inflight :: %{
           uuid: String.t(),
           gno: pos_integer(),
           gtid: String.t(),
           changes: [Change.t()],
           begun?: boolean()
         }

  @type xa_policy :: :refuse | :track

  # A pooled prepare (ADR-0006): the buffered transaction awaiting its resolution,
  # keyed by the XID digest (`Capstan.Xa.Id`). Never emitted, never inspected.
  @typep pooled :: %{txn: inflight()}

  @type t :: %__MODULE__{
          gtid_set: Gtid.t(),
          file: String.t() | nil,
          pos: non_neg_integer() | nil,
          registry: TableRegistry.t(),
          filter: filter(),
          txn: inflight() | nil,
          xa: xa_policy(),
          prepared: %{optional(Capstan.Xa.Id.t()) => pooled()},
          max_prepared: pos_integer(),
          startup_xids: MapSet.t()
        }

  # Rule 1: `txn` carries row values and `prepared` carries row values + XID digests
  # (a digest is not emittable either — dictionary-reversible for low-entropy XIDs).
  @derive {Inspect, only: [:gtid_set, :file, :pos, :filter, :xa, :max_prepared]}
  defstruct [
    :gtid_set,
    :file,
    :pos,
    :registry,
    :filter,
    :txn,
    :xa,
    :max_prepared,
    :prepared,
    :startup_xids
  ]

  @typedoc "The XA-related fail-closed halts (`step/2` / `run/3`)."
  @type xa_halt ::
          :unsupported_transaction_shape
          | :xa_prepared_pool_exhausted
          | :xa_prepared_gtid_mismatch
          | :xa_commit_without_prepare
          | :xa_rollback_without_prepare

  @typedoc "The result of `step/2`."
  @type step_result ::
          {:cont, [output()], t()}
          | {:halt, xa_halt()}
          | {:error, term()}

  @typedoc """
  The result of `run/3`. On a `{:halt, _}`/`{:error, _}` short-circuit the
  already-COMMITTED outputs (and the watermark up to the failure) are still returned —
  a transaction that reached its terminator before the halt is genuinely committed and
  must not be lost, only the in-flight one is discarded.
  """
  @type run_result ::
          {:ok, [output()], Position.t()}
          | {:halt, xa_halt(), [output()], Position.t()}
          | {:error, term(), [output()], Position.t()}

  @default_max_prepared 10_000

  @doc """
  Builds a fresh assembler state resuming from `start_position`.

  `opts` accepts `tables: :all` (default) or `tables: [{schema, table}, ...]` — the
  allowlist of tables whose rows are delivered (everything else is filtered before row
  decode) — plus the XA policy (ADR-0006): `xa: :refuse` (default — a TWO-PHASE
  `XA_PREPARE` halts `:unsupported_transaction_shape`, the C1 posture; a `one_phase:
  true` prepare is an ordinary atomic commit and is DELIVERED in both modes — the one
  deliberate deviation from C1's unconditional type-38 halt, per ADR-0006 §2) or
  `xa: :track`; `max_prepared_transactions:` (positive integer, default
  #{@default_max_prepared}) bounds the in-memory prepared pool (halt
  `:xa_prepared_pool_exhausted` — never evict, eviction is the silent-loss class); and
  `startup_xids:` (a list of `Capstan.Xa.Id` digests from the connect-time `XA RECOVER`)
  pre-seeds the dangling-prepare discriminator so a pre-start prepare's resolution is a
  correct row-less watermark advance.
  """
  @spec new(Position.t(), keyword()) :: t()
  def new(%Position{} = start_position, opts \\ []) do
    %__MODULE__{
      gtid_set: Gtid.parse(start_position.gtid_set || ""),
      file: start_position.file,
      pos: start_position.pos,
      registry: TableRegistry.new(),
      filter: build_filter(Keyword.get(opts, :tables, :all)),
      txn: nil,
      xa: Keyword.get(opts, :xa, :refuse),
      prepared: %{},
      max_prepared: Keyword.get(opts, :max_prepared_transactions, @default_max_prepared),
      startup_xids: MapSet.new(Keyword.get(opts, :startup_xids, []))
    }
  end

  @doc """
  The running processed-GTID watermark as a `Capstan.Position`.

  `Capstan.AssemblerServer` reads this after a `step/2` to checkpoint a transaction — including a
  self-committing DDL or a fully-filtered transaction, whose position advances even
  though the emitted output carries none (or an empty `changes`).
  """
  @spec position(t()) :: Position.t()
  def position(%__MODULE__{gtid_set: set, file: file, pos: pos}) do
    %Position{gtid_set: Gtid.render(set), file: file, pos: pos}
  end

  @doc """
  Folds one `%Capstan.Binlog.Event{}` into the state.

  Returns `{:cont, outputs, state}` (`outputs` is `[]` for every event except a
  terminator), or short-circuits fail-closed with `{:halt,
  :unsupported_transaction_shape}` or `{:error, reason}` — in either case the
  in-flight transaction is discarded and nothing is emitted (see the module doc).
  """
  @spec step(t(), Event.t()) :: step_result()
  def step(%__MODULE__{} = state, %Event{} = event) do
    case Decoder.decode(event) do
      {:ok, decoded} -> apply_decoded(state, event, decoded)
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Folds an entire event sequence from `start_position`.

  Returns `{:ok, outputs, final_position}` when every event is consumed. On the first
  `{:halt, _}`/`{:error, _}` a `step/2` produces, returns `{:halt, reason, committed,
  watermark}` / `{:error, reason, committed, watermark}` — the transactions that
  already terminated before the failure are genuinely committed and returned; only the
  in-flight (e.g. XA-prepared) transaction is discarded. `opts` is as for `new/2`.
  """
  @spec run([Event.t()], Position.t(), keyword()) :: run_result()
  def run(events, %Position{} = start_position, opts \\ []) do
    do_run(events, new(start_position, opts), [])
  end

  defp do_run([], state, acc), do: {:ok, Enum.reverse(acc), position(state)}

  defp do_run([event | rest], state, acc) do
    case step(state, event) do
      {:cont, outputs, next_state} ->
        do_run(rest, next_state, Enum.reverse(outputs, acc))

      # The already-committed outputs and the watermark up to the failure survive; only
      # the in-flight transaction is dropped (see `run_result`).
      {:halt, reason} ->
        {:halt, reason, Enum.reverse(acc), position(state)}

      {:error, reason} ->
        {:error, reason, Enum.reverse(acc), position(state)}
    end
  end

  ## ---------------------------------------------------------------------------
  ## per-decoded-term dispatch
  ## ---------------------------------------------------------------------------

  @spec apply_decoded(t(), Event.t(), Decoder.decoded()) :: step_result()

  # A compressed transaction (TRANSACTION_PAYLOAD): the decoder inflated the
  # payload into the wrapped events; fold each through the same transition in
  # order. The bare GTID that opened the transaction arrived before the payload,
  # so the inner QUERY(BEGIN)/rows/terminator fold exactly as bare events. A
  # failure mid-payload propagates fail-closed with the in-flight buffer
  # discarded — structurally the same guarantee as a bare-stream failure.
  #
  # The inner headers carry `log_pos: 0` (the compressor zeroes them — observed
  # live), so each inner event is re-stamped with the OUTER payload event's
  # `log_pos` before folding: a terminator's log_pos is the transaction's
  # diagnostic `pos` (Rule 3: diagnostic only, never an ordering key — the GTID
  # set is the authority), and the payload event's own log_pos is exactly the
  # transaction's end position the XID's would have named.
  defp apply_decoded(state, event, {:transaction_payload, inner_events}) do
    fold_inner(Enum.map(inner_events, &%{&1 | log_pos: event.log_pos}), state, [])
  end

  # A GTID arriving while a transaction is still open is a stream desync — the prior
  # transaction never reached its terminator. Fail closed rather than silently
  # overwrite (and lose) the open buffer, which would be an undetected data loss.
  defp apply_decoded(%__MODULE__{txn: txn}, _event, {:gtid, _}) when not is_nil(txn) do
    {:error, :gtid_within_open_transaction}
  end

  # A GTID opens a fresh in-flight transaction. `begun?` stays false until a BEGIN
  # arrives; a self-committing DDL QUERY fires exactly when it is still false.
  defp apply_decoded(state, _event, {:gtid, {uuid, gno}}) do
    txn = %{uuid: uuid, gno: gno, gtid: "#{uuid}:#{gno}", changes: [], begun?: false}
    {:cont, [], %{state | txn: txn}}
  end

  defp apply_decoded(state, event, {:query, %{schema: schema, sql: sql}}) do
    on_query(state, event, schema, sql)
  end

  defp apply_decoded(state, _event, %TableMap{} = table_map) do
    {:cont, [], %{state | registry: TableRegistry.put(state.registry, table_map)}}
  end

  defp apply_decoded(state, _event, {:write_rows, table_id, _present, _raw} = rows),
    do: on_rows(state, table_id, rows)

  defp apply_decoded(state, _event, {:delete_rows, table_id, _present, _raw} = rows),
    do: on_rows(state, table_id, rows)

  defp apply_decoded(state, _event, {:update_rows, table_id, _before, _after, _raw} = rows),
    do: on_rows(state, table_id, rows)

  # XID — transactional commit terminator.
  defp apply_decoded(state, event, {:xid, _xid}), do: commit_dml(state, event)

  # XA_PREPARE (type 38). one_phase = true is an ordinary single-GTID commit (the
  # prepare IS the commit; no pool entry — ADR-0006 §2). Two-phase is the policy split:
  # :refuse keeps the C1 halt for TWO-PHASE prepares (a one-phase prepare is an
  # ordinary atomic commit, delivered in both modes — ADR-0006 §2); :track pools the
  # buffered transaction
  # under its XID digest and holds the watermark (the core invariant: G_p enters the
  # durable set ONLY in the same write as its resolver G_c).
  defp apply_decoded(state, event, {:xa_prepare, %{one_phase: true}}),
    do: commit_dml(state, event)

  defp apply_decoded(%__MODULE__{xa: :refuse}, _event, {:xa_prepare, _xid}),
    do: {:halt, :unsupported_transaction_shape}

  defp apply_decoded(%__MODULE__{xa: :track} = state, _event, {:xa_prepare, xid}),
    do: on_xa_prepare(state, xid)

  # A file boundary resets the table_id space: drop every binding so a stale id can
  # never resolve to the wrong table (design Q3). Neither event can occur mid-txn.
  defp apply_decoded(state, _event, {:rotate, next_name, _pos}) do
    {:cont, [], %{state | registry: TableRegistry.invalidate(state.registry), file: next_name}}
  end

  defp apply_decoded(state, _event, {:format_description, _binlog_v, _server_v}) do
    {:cont, [], %{state | registry: TableRegistry.invalidate(state.registry)}}
  end

  # Structural markers carry nothing the fold acts on. ROWS_QUERY's SQL was already
  # discarded by the Decoder (Rule 1), so it too is a no-op here.
  defp apply_decoded(state, _event, :previous_gtids), do: {:cont, [], state}
  defp apply_decoded(state, _event, :heartbeat), do: {:cont, [], state}
  defp apply_decoded(state, _event, :stop), do: {:cont, [], state}
  defp apply_decoded(state, _event, {:rows_query, :discarded}), do: {:cont, [], state}

  # The TRANSACTION_PAYLOAD sub-fold: each inner event steps the same transition
  # a bare event would; outputs accumulate in order and any halt/error
  # propagates with the in-flight buffer discarded.
  defp fold_inner([], state, acc), do: {:cont, acc, state}

  defp fold_inner([inner | rest], state, acc) do
    case step(state, inner) do
      {:cont, more, state2} -> fold_inner(rest, state2, acc ++ more)
      {kind, _} = failure when kind in [:halt, :error] -> failure
    end
  end

  ## ---------------------------------------------------------------------------
  ## QUERY classification (fixture_capture classify/apply_event precedent)
  ## ---------------------------------------------------------------------------

  # A QUERY outside any GTID scope is a desync: in GTID mode every transaction opens
  # with a GTID before its BEGIN/DDL, so this never occurs in a well-formed stream.
  # Fail closed rather than silently continue, naming BEGIN distinctly for diagnosis.
  defp on_query(%{txn: nil}, _event, _schema, sql) do
    if keyword?(sql, "BEGIN"),
      do: {:error, :begin_without_gtid},
      else: {:error, :query_without_gtid}
  end

  defp on_query(%__MODULE__{xa: :track} = state, event, schema, sql) do
    case Xa.Id.parse_resolution(sql) do
      {:ok, {verb, digest}} -> on_xa_resolution(state, event, verb, digest)
      :error -> on_query_default(state, event, schema, sql)
    end
  end

  defp on_query(state, event, schema, sql), do: on_query_default(state, event, schema, sql)

  defp on_query_default(state, event, schema, sql) do
    cond do
      keyword?(sql, "BEGIN") -> {:cont, [], %{state | txn: %{state.txn | begun?: true}}}
      # An `XA START`/`XA END` opens/continues an XA transaction block — it is NOT a
      # self-committing DDL. Treating it as one (the misclassification below) would emit a
      # spurious `%SchemaChange{}` and ADVANCE the checkpoint past the XA GTID, then fail on
      # the following rows — silently losing the transaction if the XA later commits. Open
      # the block so rows accumulate; the `XA_PREPARE` event (type 38) is the terminator
      # that halts `:unsupported_transaction_shape` fail-closed with the buffer discarded.
      xa_statement?(sql) -> {:cont, [], %{state | txn: %{state.txn | begun?: true}}}
      keyword?(sql, "COMMIT") -> commit_dml(state, event)
      # A QUERY inside an open BEGIN that is not COMMIT is an in-transaction statement
      # marker, not a terminator (mirrors fixture_capture's `_other -> state`).
      state.txn.begun? -> {:cont, [], state}
      # Neither BEGIN nor COMMIT, with no open BEGIN => self-committing DDL (Q13).
      true -> commit_ddl(state, event, schema, sql)
    end
  end

  defp keyword?(sql, word), do: sql |> String.trim() |> String.upcase() == word

  # `XA START`/`XA END`/`XA PREPARE`/`XA COMMIT`/`XA ROLLBACK` — the XA transaction-control
  # verbs. No DDL statement begins with `XA `, so a prefix match is unambiguous.
  defp xa_statement?(sql),
    do: sql |> String.trim() |> String.upcase() |> String.starts_with?("XA ")

  ## ---------------------------------------------------------------------------
  ## XA prepare pool + resolution (ADR-0006 — :track only)
  ## ---------------------------------------------------------------------------

  # A prepare with no open transaction is a desync, same as any other terminator.
  defp on_xa_prepare(%__MODULE__{txn: nil}, _xid),
    do: {:error, :terminator_without_transaction}

  defp on_xa_prepare(%__MODULE__{txn: txn} = state, xid) do
    digest = Xa.Id.digest(xid)

    cond do
      # A re-presented prepare for an already-pooled XID: same G_p = our own reconnect
      # resend (idempotent — the duplicate in-flight buffer is dropped, the pool entry
      # stands); a different G_p is a server anomaly we refuse to guess at.
      Map.has_key?(state.prepared, digest) ->
        if state.prepared[digest].txn.gtid == txn.gtid do
          {:cont, [], %{state | txn: nil}}
        else
          {:halt, :xa_prepared_gtid_mismatch}
        end

      # Never evict: evicting a prepare that later commits is exactly the silent loss
      # C5 exists to prevent (ADR-0006 rejected-alternatives).
      map_size(state.prepared) >= state.max_prepared ->
        {:halt, :xa_prepared_pool_exhausted}

      true ->
        # The held-out watermark: NO output, NO advance — G_p enters the durable set
        # only in the same checkpoint write as its resolver G_c.
        {:cont, [], %{state | prepared: Map.put(state.prepared, digest, %{txn: txn}), txn: nil}}
    end
  end

  # (A resolution QUERY can only arrive inside an open G_c transaction — the
  # nil-txn dispatch arm has already failed closed before we get here.)
  defp on_xa_resolution(%__MODULE__{txn: txn} = state, event, verb, digest) do
    case Map.pop(state.prepared, digest) do
      {nil, _prepared} ->
        # Not pooled. A pre-start dangling prepare (seen in the connect-time XA RECOVER)
        # resolves as a correct ROW-LESS advance — its rows predate the pipeline (the
        # snapshot's domain); anything else is a desync we refuse to guess at.
        if MapSet.member?(state.startup_xids, digest) do
          # CONSUMED, symmetric with the pool's Map.pop (claude-peer finding): a digest
          # that stays resident would silently absorb a LATER duplicate resolution for
          # the same XID (a re-used application XID whose second prepare was lost) as
          # another benign row-less advance — turning a loud desync halt silent.
          state = %{state | startup_xids: MapSet.delete(state.startup_xids, digest)}
          rowless_advance(state, event, txn, [])
        else
          {:halt, xa_desync(verb)}
        end

      {%{txn: pooled}, prepared} ->
        state = %{state | prepared: prepared}

        case verb do
          :commit ->
            # Deliver the pooled changes as one transaction; the position (and the
            # durable set) carries G_p ∪ G_c in a SINGLE advance — the held-out
            # watermark's payoff arm.
            {new_set, position} = advance_union(state, [txn_gtid(pooled), txn_gtid(txn)], event)

            transaction = %Transaction{
              gtid: pooled.gtid,
              position: position,
              changes: Enum.reverse(pooled.changes),
              commit_ts: DateTime.from_unix!(event.timestamp)
            }

            {:cont, [transaction], %{state | gtid_set: new_set, pos: event.log_pos, txn: nil}}

          :rollback ->
            # The rows are discarded, NEVER delivered; both GTIDs advance in one write.
            rowless_advance(state, event, txn, [txn_gtid(pooled)])
        end
    end
  end

  defp xa_desync(:commit), do: :xa_commit_without_prepare
  defp xa_desync(:rollback), do: :xa_rollback_without_prepare

  defp txn_gtid(txn), do: "#{txn.uuid}:#{txn.gno}"

  # A row-less watermark advance: an EMPTY-changes transaction (the AssemblerServer's
  # filtered path checkpoints it without calling the sink) over this GTID plus any
  # held-out extras — the rollback and the dangling-resolution arms.
  defp rowless_advance(state, event, txn, extra_gtids) do
    {new_set, position} = advance_union(state, [txn_gtid(txn) | extra_gtids], event)

    transaction = %Transaction{
      gtid: txn.gtid,
      position: position,
      changes: [],
      commit_ts: DateTime.from_unix!(event.timestamp)
    }

    {:cont, [transaction], %{state | gtid_set: new_set, pos: event.log_pos, txn: nil}}
  end

  ## ---------------------------------------------------------------------------
  ## terminators — the ONLY place the in-flight buffer is released
  ## ---------------------------------------------------------------------------

  # An XID with no open transaction is a desync — fail closed, never a silent no-op.
  defp commit_dml(%{txn: nil}, _event), do: {:error, :terminator_without_transaction}

  defp commit_dml(%{txn: txn} = state, event) do
    {new_set, position} = advance(state, txn, event)

    transaction = %Transaction{
      gtid: txn.gtid,
      position: position,
      changes: Enum.reverse(txn.changes),
      commit_ts: DateTime.from_unix!(event.timestamp)
    }

    {:cont, [transaction], %{state | gtid_set: new_set, pos: event.log_pos, txn: nil}}
  end

  defp commit_ddl(%{txn: txn} = state, event, default_schema, sql) do
    {new_set, _position} = advance(state, txn, event)
    {kind, schema, table} = classify_ddl(default_schema, sql)

    schema_change = %SchemaChange{schema: schema, table: table, kind: kind, gtid: txn.gtid}

    {:cont, [schema_change], %{state | gtid_set: new_set, pos: event.log_pos, txn: nil}}
  end

  # Union this transaction's GTID into the running processed set and build the
  # `%Position{}` that INCLUDES it. `pos` is the terminator's header `log_pos`
  # (diagnostic); the gtid_set is the authoritative watermark.
  defp advance(state, txn, event), do: advance_union(state, [txn_gtid(txn)], event)

  # Union one or more GTIDs into the running processed set in a SINGLE advance — the
  # XA resolution path carries G_p ∪ G_c atomically (the held-out watermark).
  defp advance_union(state, gtids, event) do
    new_set =
      Enum.reduce(gtids, state.gtid_set, fn gtid, acc ->
        Gtid.union(acc, Gtid.parse(gtid))
      end)

    position = %Position{gtid_set: Gtid.render(new_set), file: state.file, pos: event.log_pos}
    {new_set, position}
  end

  ## ---------------------------------------------------------------------------
  ## row events — resolve by OWN table_id (Q3), filter BEFORE decode (Q14), fail closed
  ## ---------------------------------------------------------------------------

  # Rows outside a transaction scope (no open GTID) are a desync — never produced by a
  # well-formed stream. Fail closed rather than silently drop them.
  defp on_rows(%{txn: nil}, _table_id, _rows), do: {:error, :rows_without_transaction}

  defp on_rows(state, table_id, rows) do
    case TableRegistry.resolve(state.registry, table_id) do
      {:error, :unmapped_table_id} ->
        # Fail closed: a row event with no live TABLE_MAP is a desync, never a guess.
        {:error, :unmapped_table_id}

      {:ok, %TableMap{schema: schema, table: table} = table_map} ->
        if allowed?(state.filter, schema, table) do
          decode_rows(state, rows, table_map)
        else
          # Filtered BEFORE row decode: the transaction still terminates and advances
          # the watermark (Q14); its rows simply never become changes.
          {:cont, [], state}
        end
    end
  end

  defp decode_rows(state, rows, %TableMap{schema: schema, table: table} = table_map) do
    case Rows.decode(rows, table_map) do
      {:ok, decoded_rows} ->
        changes = to_changes(decoded_rows, schema, table)

        {:cont, [],
         %{state | txn: %{state.txn | changes: Enum.reverse(changes, state.txn.changes)}}}

      {:error, _reason} = error ->
        # A caster/schema/truncation refusal aborts the whole transaction fail-closed.
        error
    end
  end

  defp to_changes({:insert, rows}, schema, table) do
    for record <- rows do
      %Change{op: :insert, schema: schema, table: table, record: record, old_record: nil}
    end
  end

  defp to_changes({:delete, rows}, schema, table) do
    for record <- rows do
      %Change{op: :delete, schema: schema, table: table, record: nil, old_record: record}
    end
  end

  defp to_changes({:update, pairs}, schema, table) do
    for {before_row, after_row} <- pairs do
      %Change{
        op: :update,
        schema: schema,
        table: table,
        record: after_row,
        old_record: before_row
      }
    end
  end

  ## ---------------------------------------------------------------------------
  ## filter
  ## ---------------------------------------------------------------------------

  defp build_filter(:all), do: :all
  defp build_filter(list) when is_list(list), do: MapSet.new(list)

  defp allowed?(:all, _schema, _table), do: true
  defp allowed?(allowlist, schema, table), do: MapSet.member?(allowlist, {schema, table})

  ## ---------------------------------------------------------------------------
  ## DDL classification — schema/table/kind ONLY (Rule 1: never the SQL text)
  ## ---------------------------------------------------------------------------

  # The raw SQL is inspected here to classify and is then dropped; only the
  # structured facts flow into `%SchemaChange{}`. Unrecognised DDL is `:other` with no
  # table — a safe default that guesses nothing and leaks nothing.
  #
  # This is a best-effort *metadata* extractor, not a SQL parser: it tokenises on
  # whitespace, so a quoted identifier containing whitespace (`` `my table` ``) is
  # truncated to a wrong `table` label. That is a diagnostic-label imperfection ONLY —
  # never a row value, never a Rule-1 leak, and with no effect on the data path or the
  # watermark. A quote-aware tokeniser is deferred until a real fixture demands it.
  @spec classify_ddl(String.t(), String.t()) :: {atom(), String.t() | nil, String.t() | nil}
  defp classify_ddl(default_schema, sql) do
    tokens = sql |> String.trim() |> String.split(~r/\s+/, trim: true)

    case Enum.map(Enum.take(tokens, 3), &String.upcase/1) do
      ["CREATE", "TABLE" | _] ->
        object(default_schema, :create_table, create_if_not_exists(Enum.drop(tokens, 2)))

      ["CREATE", "TEMPORARY", "TABLE"] ->
        object(default_schema, :create_table, create_if_not_exists(Enum.drop(tokens, 3)))

      ["ALTER", "TABLE" | _] ->
        object(default_schema, :alter_table, Enum.drop(tokens, 2))

      ["DROP", "TABLE" | _] ->
        object(default_schema, :drop_table, drop_if_exists(Enum.drop(tokens, 2)))

      ["DROP", "TEMPORARY", "TABLE"] ->
        object(default_schema, :drop_table, drop_if_exists(Enum.drop(tokens, 3)))

      ["TRUNCATE", "TABLE" | _] ->
        object(default_schema, :truncate, Enum.drop(tokens, 2))

      ["TRUNCATE" | _] ->
        object(default_schema, :truncate, Enum.drop(tokens, 1))

      _ ->
        {:other, nil, nil}
    end
  end

  defp object(default_schema, kind, [name | _]) do
    {schema, table} = split_qualified(name, default_schema)
    {kind, schema, table}
  end

  defp object(default_schema, kind, []), do: {kind, default_schema, nil}

  defp drop_if_exists([first, second | rest]) do
    if String.upcase(first) == "IF" and String.upcase(second) == "EXISTS",
      do: rest,
      else: [first, second | rest]
  end

  defp drop_if_exists(tokens), do: tokens

  # `CREATE TABLE` takes `IF NOT EXISTS` (three tokens), the mirror of `drop_if_exists`.
  # Without stripping it the object name would be read as `"IF"` — a silently-wrong
  # table on a very common statement.
  defp create_if_not_exists([first, second, third | rest]) do
    if Enum.map([first, second, third], &String.upcase/1) == ["IF", "NOT", "EXISTS"],
      do: rest,
      else: [first, second, third | rest]
  end

  defp create_if_not_exists(tokens), do: tokens

  # Split an optional `schema.table` qualifier and strip identifier quoting and any
  # glued punctuation (`(`, trailing `,`/`;`). A bare name uses the connection schema.
  defp split_qualified(token, default_schema) do
    cleaned =
      token
      |> String.split("(", parts: 2)
      |> hd()
      |> String.trim_trailing(";")
      |> String.trim_trailing(",")

    case String.split(cleaned, ".", parts: 2) do
      [table] -> {default_schema, unquote_ident(table)}
      [schema, table] -> {unquote_ident(schema), unquote_ident(table)}
    end
  end

  defp unquote_ident(identifier), do: identifier |> String.trim("`") |> String.trim("\"")
end

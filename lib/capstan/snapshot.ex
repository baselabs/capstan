defmodule Capstan.Snapshot do
  @moduledoc """
  Initial-snapshot public config validation + the **bootstrap orchestration** (C2 —
  the `P0` pre-seed that avoids C1b, design Q10 / Ch5).

  ## What the bootstrap does

  In snapshot mode `Capstan.Supervisor` calls `bootstrap/4` after starting the checkpoint +
  snapshot stores and BEFORE it reads the checkpoint position, so the pre-seed lands first:

    1. **Read the durable `%Capstan.Snapshot.State{}` + reconcile its table set against config.**
       The stored state binds the tables introspected at the fresh start; the configured
       `snapshot.tables` must still match it (`reconcile_tables/2`) or the bootstrap halts
       `:snapshot_config_drifted` — a table ADDED to config after a `:complete`/mid-snapshot state
       would otherwise be silently never backfilled (both the `:complete` short-circuit and
       `open_resume/3` key off the STORED set). `status: :complete` (with a matching set) ⇒ the
       backfill is already done — signal `:complete` so the supervisor wires the real sink directly
       (pure C1, no coordinator). No source connection is opened in this case (the reconcile is a
       pure key comparison).
    2. **Establish the transient query connection, source-identity pinned (Ch8).** The stream
       connection's `@@server_uuid` is read via `Capstan.Config.read_server_uuid/1` over an
       independent authenticated socket, and the `Capstan.Query` connection is established
       pinned to it (`:expected_server_uuid`). A query connection that lands on a DIFFERENT
       replica than the stream — a VIP failover between the two connects — is caught
       `:snapshot_source_mismatch` (the cross-connection source-identity check).
    3. **Resolve `P0`.** A FRESH start reads `P0 = @@global.gtid_executed` over the query
       connection (a read fault halts `:snapshot_bootstrap_gtid_read_failed`). A mid-snapshot
       RESUME uses the STORED `p0` from the durable `%State{}` — immune to `@@gtid_executed`
       drift across a bootstrap crash.
    4. **Seed the checkpoint store.** `P0` is written ONLY when the checkpoint is empty or
       already equals `p0` (`seed_checkpoint/2`) — it NEVER regresses a watermark the stream
       already advanced. This makes the dump AND the assembler watermark both resolve to `P0`
       through the sanctioned `start_position: :checkpoint` path (no C1b, no ADR-0004
       supersession).
    5. **Seed the durable `%State{}` + open the readers.** A `Capstan.Snapshot.ChunkReader` is
       opened per not-done snapshot table (introspecting the order-faithful PK + baseline
       fingerprint), and the `%State{}` (per-table `pk_cursor: :start`, `p0`) is persisted. On
       resume the durable per-table cursors are kept and each reader reopened with the stored
       fingerprint (a schema drift across the resume is caught on chunk 1).

  The supervisor then starts the `Capstan.Snapshot.Coordinator` with `processed_set` = the live
  watermark (`P0`) so a chunk whose `G ≤ P0` emits immediately, wires it as the
  assembler's sink by NAME, and injects the observer + monitor via
  `Capstan.AssemblerServer.attach_coordinator/2`.

  ## Retention purge racing the bootstrap (tripwire 11)

  The seed NEVER masks a retention gap. Across a bootstrap crash-window (the checkpoint seeded
  but the `%State{}` not yet written), a re-bootstrap reads a fresh `gtid_executed = p0'` yet
  `seed_checkpoint/2` LEAVES the earlier `P0` untouched (`p0' ≠ P0`). If the source has since
  PURGED past that `P0`, the stream resumes from it and the EXISTING C1 gap gate fires
  `:data_gap` — the bootstrap does not map error `1236` to `:ok`, nor overwrite the gapped
  checkpoint with the current position (which WOULD mask the loss).

  ## Rule 1

  `P0` and the `%State{}` `p0` are GTID-set STRINGS (`Capstan.Gtid` form) — structural, not row
  values. The per-table `pk_cursor`/`fingerprint` are user data; they live only in the durable
  `%State{}` (whose `Inspect` elides the `tables` map) and are never logged or telemetered here.
  Every failure is a value-free atom; the transient query connection's password never leaves the
  `Capstan.Query` handle.
  """

  alias Capstan.CheckpointStore
  alias Capstan.Config
  alias Capstan.Gtid
  alias Capstan.Pipeline
  alias Capstan.Position
  alias Capstan.Query
  alias Capstan.Snapshot.ChunkReader
  alias Capstan.Snapshot.State
  alias Capstan.Snapshot.Tables
  alias Capstan.SnapshotStore

  @gtid_executed_sql "SELECT @@global.gtid_executed"

  @typedoc "A checkpoint store handle as `{callback_module, store_handle}`."
  @type checkpoint_store :: {module(), CheckpointStore.store()}

  @typedoc "A snapshot store handle as `{callback_module, store_handle}`."
  @type snapshot_store :: {module(), SnapshotStore.store()}

  @typedoc "The per-table opened `ChunkReader` handles for the coordinator's `readers` map."
  @type readers :: %{optional({String.t(), String.t()}) => ChunkReader.t()}

  @typedoc """
  The bootstrap outcome:

    * `:complete` — the durable `%State{}` is `status: :complete`; wire pure C1 (no coordinator).
    * `{:snapshot, state, readers, processed_set}` — a fresh/mid-snapshot start: the seeded
      `%State{}`, the per-table readers, and the initial processed-watermark string (`P0`) the
      coordinator seeds its advance gate with.
    * `{:error, reason}` — a value-free bootstrap halt.
  """
  @type result ::
          :complete
          | {:snapshot, State.t(), readers(), String.t()}
          | {:error, atom()}

  ## ---------------------------------------------------------------------------
  ## public config validation
  ## ---------------------------------------------------------------------------

  @doc """
  Validates the snapshot configuration surface: normalises the `:snapshot` block
  (`Capstan.Config.validate_snapshot/1`) and enforces `snapshot_tables ⊆ captured`
  (`Capstan.Pipeline.validate_snapshot_tables/2`).

  Returns `{:ok, snapshot_config | nil}` (`nil` when `:snapshot` is absent — pure C1) or a
  value-free `{:error, reason}` (`:config_invalid`, `:snapshot_table_not_captured`). Composes
  the two existing validators so the snapshot config surface has one documented entry point.
  """
  @spec validate(keyword()) ::
          {:ok, Config.snapshot_config() | nil}
          | {:error, :config_invalid | :snapshot_table_not_captured}
  def validate(opts) when is_list(opts) do
    with {:ok, snapshot} <- Config.validate_snapshot(opts),
         :ok <- Pipeline.validate_snapshot_tables(opts, snapshot) do
      {:ok, snapshot}
    end
  end

  ## ---------------------------------------------------------------------------
  ## bootstrap orchestration
  ## ---------------------------------------------------------------------------

  @doc """
  Runs the snapshot bootstrap against the already-started `checkpoint_store` + `snapshot_store`.

  `opts` is the pipeline wiring (`:connection`, optional `:connect_fun` / `:max_command_retries`
  test seams); `snapshot` is the normalised `Capstan.Config.snapshot_config()`. Returns a
  `t:result/0`. Reads the durable `%State{}` FIRST (short-circuiting to `:complete` without
  opening any source connection); otherwise establishes the source-pinned query connection,
  resolves + seeds `P0`, and opens the per-table readers. See the moduledoc for the full
  sequence and the fail-closed halts.
  """
  @spec bootstrap(keyword(), Config.snapshot_config(), checkpoint_store(), snapshot_store()) ::
          result()
  def bootstrap(opts, snapshot, checkpoint_store, {simpl, sstore} = snapshot_store)
      when is_list(opts) and is_map(snapshot) do
    case SnapshotStore.read(simpl, sstore) do
      {:ok, nil} ->
        # A FRESH start — no durable state yet, so config IS the table set (nothing to reconcile).
        start_backfill(opts, snapshot, checkpoint_store, snapshot_store, nil)

      {:ok, %State{} = existing} ->
        with :ok <- reconcile_tables(snapshot, existing) do
          resume_or_complete(existing, opts, snapshot, checkpoint_store, snapshot_store)
        end

      {:error, _reason} ->
        {:error, :snapshot_state_read_failed}
    end
  rescue
    # A store that RAISES before any source connection opens (the initial `SnapshotStore.read`),
    # or an unexpected raise on the pre-query identity path — fail closed VALUE-FREE, never an
    # uncaught exception out of `start_link/1` (S-1 shape-gap). A raise on the query-open path is
    # already caught + query-closed by `finish_or_close/6`, so this fires only pre-query (no fd to
    # release here). The raised term is discarded (Rule 1).
    _exception -> {:error, :snapshot_bootstrap_crashed}
  catch
    _kind, _reason -> {:error, :snapshot_bootstrap_crashed}
  end

  # A durable `%State{}` binds its OWN table set (the tables introspected at the fresh start). The
  # `:complete` short-circuit and `open_resume/3` both key off THAT stored set and IGNORE the
  # configured `snapshot.tables`, so a table ADDED to config after a `:complete`/mid-snapshot state
  # would be SILENTLY never backfilled (and a REMOVED table leaves orphaned durable progress). Fail
  # closed on ANY divergence — the operator must resolve a config/state mismatch deliberately (drop
  # the durable snapshot state to re-backfill the new set, or restore the config). Pure key
  # comparison (no source connection), so the `:complete` no-connect property is preserved. A
  # configured `:all` set always reconciles: at a fresh start it RESOLVED to the stored concrete
  # set (C2b, `Capstan.Snapshot.Tables`), so the stored set IS the authority for what `:all` meant.
  defp reconcile_tables(%{tables: :all}, _existing), do: :ok

  defp reconcile_tables(%{tables: configured}, %State{tables: stored}) when is_list(configured) do
    if MapSet.new(configured) == MapSet.new(Map.keys(stored)),
      do: :ok,
      else: {:error, :snapshot_config_drifted}
  end

  # Post-reconciliation dispatch: a `:complete` state wires pure C1 (no coordinator, no source
  # connection); any other durable state resumes the backfill.
  defp resume_or_complete(%State{status: :complete}, _opts, _snapshot, _checkpoint_store, _ss),
    do: :complete

  defp resume_or_complete(existing, opts, snapshot, checkpoint_store, snapshot_store),
    do: start_backfill(opts, snapshot, checkpoint_store, snapshot_store, existing)

  @doc """
  Seeds the checkpoint store with `p0` ONLY when the checkpoint is empty or already equals `p0`.

  A checkpoint the stream has already advanced past `p0` (a resume, or a bootstrap crash-window
  re-read) is LEFT untouched — the seed never regresses a watermark, and never masks a retention
  gap by overwriting a stale-but-gapped checkpoint with the current position (tripwire 11).
  Equality is a GTID-set comparison (`Capstan.Gtid`), not a string compare. Returns `:ok` or a
  value-free store error.
  """
  @spec seed_checkpoint(checkpoint_store(), String.t()) :: :ok | {:error, term()}
  def seed_checkpoint({impl, store}, p0) when is_atom(impl) and is_binary(p0) do
    case CheckpointStore.read_position(impl, store) do
      {:ok, nil} -> write_checkpoint(impl, store, p0)
      {:ok, %Position{gtid_set: ""}} -> write_checkpoint(impl, store, p0)
      {:ok, %Position{gtid_set: current}} -> reseed_if_equal(impl, store, current, p0)
      {:error, _reason} = error -> error
    end
  end

  # An advanced watermark (`current ≠ p0`) is LEFT untouched — never regress, never mask a gap.
  # A checkpoint already equal to `p0` is (idempotently) re-written to the same value.
  defp reseed_if_equal(impl, store, current, p0) do
    if gtid_equal?(current, p0), do: write_checkpoint(impl, store, p0), else: :ok
  end

  ## ---------------------------------------------------------------------------
  ## backfill start (fresh or mid-snapshot resume)
  ## ---------------------------------------------------------------------------

  defp start_backfill(opts, snapshot, checkpoint_store, snapshot_store, existing) do
    connection = Keyword.fetch!(opts, :connection)
    connect_fun = Keyword.get(opts, :connect_fun, &Query.default_connect/1)
    max_retries = Keyword.get(opts, :max_command_retries, CheckpointStore.default_max_retries())

    with {:ok, stream_uuid} <- read_stream_identity(connection, connect_fun),
         {:ok, query} <- establish_query(connection, connect_fun, max_retries, stream_uuid) do
      finish_or_close(query, opts, snapshot, checkpoint_store, snapshot_store, existing)
    end
  end

  # The query connection is open past this point: on any failure it must be closed (no fd leak);
  # on success the readers own it for the backfill's life. `build_backfill/6` calls store ops
  # whose behaviour contract is `{:error, term()}`, but a real durable store (Ecto/DBConnection)
  # RAISES on a transient backing-DB outage rather than returning `{:error, _}`. Bootstrap runs
  # synchronously inside `Capstan.Supervisor.start_link/1` — NOT in a GenServer callback — so an
  # un-rescued raise here both leaks the authenticated query connection AND aborts start with an
  # uncaught exception instead of a value-free `{:error, _}` (a fail-closed-shape gap; under a
  # restart loop against a down store it exhausts source connections — review S-1). Close the
  # query and fail closed value-free; the raised term (a store's own exception, possibly
  # value-bearing) is DISCARDED (Rule 1), exactly as the coordinator scrubs an emit-path raise.
  defp finish_or_close(query, opts, snapshot, checkpoint_store, snapshot_store, existing) do
    case build_backfill(query, opts, snapshot, checkpoint_store, snapshot_store, existing) do
      {:snapshot, _state, _readers, _processed} = ok ->
        ok

      {:error, _reason} = error ->
        Query.close(query)
        error
    end
  rescue
    _exception ->
      Query.close(query)
      {:error, :snapshot_bootstrap_crashed}
  catch
    _kind, _reason ->
      Query.close(query)
      {:error, :snapshot_bootstrap_crashed}
  end

  defp build_backfill(query, _opts, snapshot, checkpoint_store, snapshot_store, existing) do
    with {:ok, p0} <- resolve_p0(query, existing),
         :ok <- seed_checkpoint(checkpoint_store, p0),
         {:ok, state, readers} <- open_tables(query, snapshot, existing, p0),
         :ok <- persist_state(snapshot_store, state),
         {:ok, processed} <- current_watermark(checkpoint_store) do
      {:snapshot, state, readers, processed}
    end
  end

  ## ---------------------------------------------------------------------------
  ## source identity (Ch8) + the transient query connection
  ## ---------------------------------------------------------------------------

  # Read the stream connection's `@@server_uuid` over an INDEPENDENT authenticated socket, so the
  # query connection can be pinned against it (the cross-connection check). The socket is closed
  # immediately — this read exists only to obtain the identity to pin.
  defp read_stream_identity(connection, connect_fun) do
    case connect_fun.(connection) do
      {:ok, socket, _info} ->
        read_and_close_identity(socket)

      {:error, _reason} ->
        {:error, :snapshot_query_connect_failed}
    end
  rescue
    # `connect_fun` itself raised — no socket was opened, so there is nothing to release here.
    _exception -> {:error, :snapshot_query_connect_failed}
  catch
    _kind, _reason -> {:error, :snapshot_query_connect_failed}
  end

  # Read `@@server_uuid` over the just-opened identity socket, then CLOSE it — on the success path
  # AND on a raise (the same fd-leak-on-raise posture as `finish_or_close/6`'s query close). The
  # socket is in scope here so the rescue can release it.
  defp read_and_close_identity(socket) do
    result = Config.read_server_uuid(socket)
    close_socket(socket)
    identity_result(result)
  rescue
    _exception ->
      close_socket(socket)
      {:error, :snapshot_query_connect_failed}
  catch
    _kind, _reason ->
      close_socket(socket)
      {:error, :snapshot_query_connect_failed}
  end

  defp identity_result({:ok, uuid}), do: {:ok, uuid}
  defp identity_result({:error, _reason}), do: {:error, :snapshot_query_connect_failed}

  defp establish_query(connection, connect_fun, max_retries, stream_uuid) do
    Query.establish(
      connection: connection,
      connect_fun: connect_fun,
      max_command_retries: max_retries,
      expected_server_uuid: stream_uuid
    )
  end

  ## ---------------------------------------------------------------------------
  ## P0 resolution
  ## ---------------------------------------------------------------------------

  # A mid-snapshot RESUME reuses the STORED p0 (drift-immune across a bootstrap crash); a FRESH
  # start (no durable state, or a state with no p0) reads the live `@@global.gtid_executed`.
  defp resolve_p0(_query, %State{p0: p0}) when is_binary(p0), do: {:ok, p0}
  defp resolve_p0(query, _fresh), do: read_p0(query)

  defp read_p0(query) do
    case Query.query(query, @gtid_executed_sql) do
      {:ok, [[gtid_set]]} when is_binary(gtid_set) -> {:ok, gtid_set}
      _other -> {:error, :snapshot_bootstrap_gtid_read_failed}
    end
  end

  ## ---------------------------------------------------------------------------
  ## opening the readers + building the durable %State{}
  ## ---------------------------------------------------------------------------

  # FRESH: introspect every snapshot table (via ChunkReader.open) and build the durable %State{}
  # with `pk_cursor: :start`. An `:all` snapshot set (which arises when the CAPTURE allowlist is
  # itself `:all`) resolves to the server's scoped base tables — `information_schema.TABLES`
  # enumeration excluding the system schemas and everything that is not a BASE TABLE (C2b,
  # `Capstan.Snapshot.Tables`) — so "snapshot everything" means a well-defined, scoped set, never
  # the system schemas and never a silent empty set. The resolved list is what the durable
  # `%State{}` binds, exactly as an explicit config list would.
  defp open_tables(query, %{tables: tables, chunk_size: chunk_size}, nil, p0)
       when is_list(tables) do
    open_fresh(query, tables, chunk_size, p0)
  end

  defp open_tables(query, %{tables: :all, chunk_size: chunk_size}, nil, p0) do
    case Tables.resolve_all(query) do
      {:ok, tables} -> open_fresh(query, tables, chunk_size, p0)
      {:error, _reason} = error -> error
    end
  end

  # RESUME: keep the durable per-table cursors and reopen a reader per NOT-DONE table with the
  # stored fingerprint (so a schema drift across the resume is caught on chunk 1).
  defp open_tables(query, %{chunk_size: chunk_size}, %State{} = state, _p0) do
    open_resume(query, state, chunk_size)
  end

  defp open_fresh(query, tables, chunk_size, p0) do
    case reduce_open(tables, query, fn _key -> [chunk_size: chunk_size] end) do
      {:ok, readers} ->
        {:ok, %State{status: :snapshotting, p0: p0, tables: tables_from_readers(readers)},
         readers}

      {:error, _reason} = error ->
        error
    end
  end

  defp open_resume(query, %State{tables: tables} = state, chunk_size) do
    not_done =
      tables
      |> Enum.reject(fn {_key, progress} -> progress.done? end)
      |> Enum.map(fn {key, _progress} -> key end)

    opts_for = fn key ->
      [chunk_size: chunk_size, fingerprint: Map.fetch!(tables, key).fingerprint]
    end

    case reduce_open(not_done, query, opts_for) do
      {:ok, readers} -> {:ok, state, readers}
      {:error, _reason} = error -> error
    end
  end

  # Open a reader per table (halting on the first introspection/read fault), accumulating the
  # `%{key => reader}` map. `opts_for` yields the per-table `ChunkReader.open/3` options.
  defp reduce_open(keys, query, opts_for) do
    Enum.reduce_while(keys, {:ok, %{}}, fn key, {:ok, acc} ->
      case ChunkReader.open(query, key, opts_for.(key)) do
        {:ok, reader} -> {:cont, {:ok, Map.put(acc, key, reader)}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  # The durable per-table progress derived from each freshly-opened reader (both the re-read
  # floor `pk_cursor` and the delivered high-water `delivered_pk` at `:start`).
  defp tables_from_readers(readers) do
    Map.new(readers, fn {key, reader} ->
      {key,
       %{
         fingerprint: reader.fingerprint,
         pk_columns: reader.pk_columns,
         pk_types: reader.pk_types,
         pk_cursor: :start,
         delivered_pk: :start,
         done?: false
       }}
    end)
  end

  ## ---------------------------------------------------------------------------
  ## durable state + checkpoint helpers
  ## ---------------------------------------------------------------------------

  defp persist_state({impl, store}, state) do
    case SnapshotStore.write(impl, store, state) do
      :ok -> :ok
      {:error, _reason} -> {:error, :snapshot_state_write_failed}
    end
  end

  # The processed watermark the coordinator seeds its advance gate with: the
  # checkpoint's current value AFTER the seed — `P0` on a fresh start, the advanced watermark on a
  # resume — so a chunk captured at `G ≤` it emits immediately.
  defp current_watermark({impl, store}) do
    case CheckpointStore.read_position(impl, store) do
      {:ok, nil} -> {:ok, ""}
      {:ok, %Position{gtid_set: gtid_set}} -> {:ok, gtid_set}
      {:error, _reason} = error -> error
    end
  end

  defp write_checkpoint(impl, store, p0) do
    CheckpointStore.write_position(impl, store, Position.from_persisted(p0))
  end

  # Set equality via mutual subset — the sanctioned `Capstan.Gtid` API (ADR-0001), robust to a
  # textual difference between two strings that denote the same GTID set.
  defp gtid_equal?(a, b) do
    parsed_a = Gtid.parse(a)
    parsed_b = Gtid.parse(b)
    Gtid.subset?(parsed_a, parsed_b) and Gtid.subset?(parsed_b, parsed_a)
  end

  defp close_socket({:gen_tcp, sock}), do: :gen_tcp.close(sock)
  defp close_socket({:ssl, sock}), do: :ssl.close(sock)
end

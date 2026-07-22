defmodule Capstan.Supervisor do
  @moduledoc """
  The pipeline supervisor: it owns one pipeline's
  checkpoint store, `Capstan.AssemblerServer`, and `Capstan.Connection`, wired so the
  `Connection`'s `:receiver` is the `AssemblerServer` PID.

  ## Fail-closed halts do not take down the host

  Every child is `restart: :temporary`. A fail-closed halt exits
  `{:shutdown, {:halt, reason}}` — which is **not** a crash — so a halted child is
  removed and **never restarted into a livelock**, and this supervisor (the host) stays
  up. A `:temporary` child never restarts, so the `AssemblerServer` PID baked into the
  `Connection`'s `:receiver` is stable for the pipeline's life; a send to a
  terminated `AssemblerServer` is a silent no-op, never a raise that could resurrect a
  fail-closed pipeline.

  The strategy is `:one_for_one`: the children coordinate their own fail-closed halts
  (the `Connection` forwards `{:capstan_halt, reason}` to the `AssemblerServer` before it
  stops), so no cross-restart coupling is needed, and none is wanted — a restart of a
  halted child is exactly the livelock the fail-closed design forbids.

  Children start in dependency order — store, then `AssemblerServer` (reads the store,
  takes its `{impl, handle}`), then `Connection` (takes the `AssemblerServer` PID) — via
  `Supervisor.start_child/2`, because the `Connection`'s receiver PID is only known once
  the `AssemblerServer` is running.
  """

  alias Capstan.AssemblerServer
  alias Capstan.CheckpointStore
  alias Capstan.Pipeline
  alias Capstan.Query
  alias Capstan.Snapshot
  alias Capstan.Snapshot.Coordinator

  @doc """
  Start a supervised, lib-owned pipeline from an already-validated wiring keyword
  (`:sink`, `:checkpoint_store`, `:connection`, `:server_id`, `:max_command_retries`,
  `:start_position`, `:tables`; plus optional `:connect_fun` / `:reconnect_backoff` test
  seams that fail closed to the real connect when absent).

  Returns `{:ok, supervisor_pid}`, or `{:error, reason}` — a value-free store-read or
  start-position refusal — after tearing the partially-started tree back down.
  """
  @spec start_link(keyword()) :: {:ok, pid()} | {:error, term()}
  def start_link(opts) when is_list(opts) do
    {:ok, sup} = Supervisor.start_link([], strategy: :one_for_one)

    case wire(sup, opts) do
      :ok ->
        {:ok, sup}

      {:error, reason} ->
        _ = Supervisor.stop(sup)
        {:error, reason}
    end
  end

  # Snapshot mode adds the P0 pre-seed + the coordinator/store children; an absent `:snapshot`
  # takes the byte-identical C1 path (`wire_c1/2`), so the full existing suite stays green.
  defp wire(sup, opts) do
    if Pipeline.snapshot_mode?(opts) do
      wire_snapshot(sup, opts)
    else
      wire_c1(sup, opts)
    end
  end

  # The C1 wiring — unchanged: store → checkpoint read → assembler → connection.
  defp wire_c1(sup, opts) do
    {impl, store_options} = Pipeline.checkpoint_store(opts)

    with {:ok, store} <- add_child(sup, Pipeline.store_spec(impl, store_options)),
         {:ok, resumed} <- CheckpointStore.read_position(impl, store),
         {:ok, start_position} <- Pipeline.resolve_start_position(opts, resumed),
         {:ok, assembler} <- add_child(sup, Pipeline.assembler_spec(opts, {impl, store})),
         {:ok, _connection} <-
           add_child(
             sup,
             Pipeline.connection_spec(opts, assembler, start_position, {impl, store})
           ) do
      :ok
    end
  end

  # The snapshot wiring: start the durable snapshot store, run the bootstrap P0 pre-seed BEFORE
  # the checkpoint read (so the assembler seeds its watermark from P0), resolve the start
  # position, then wire the mode-specific tail (`finish_wire/6`). `status: :complete` collapses to
  # the pure-C1 tail; a fresh/mid-snapshot start wires the coordinator.
  defp wire_snapshot(sup, opts) do
    {impl, store_options} = Pipeline.checkpoint_store(opts)
    snapshot = Keyword.fetch!(opts, :snapshot)
    {snap_impl, snap_options} = snapshot.store

    with {:ok, store} <- add_child(sup, Pipeline.store_spec(impl, store_options)),
         {:ok, snap_store} <-
           add_child(sup, Pipeline.snapshot_store_spec(snap_impl, snap_options)) do
      # `bootstrap/4` seeds the checkpoint BEFORE the `continue_wire/5` read below (so the
      # assembler seeds its watermark from P0), and returns `:complete | {:snapshot, ...} |
      # {:error, reason}` — dispatched by `continue_wire/5`, not the `{:ok, _}` this chain matches.
      boot = Snapshot.bootstrap(opts, snapshot, {impl, store}, {snap_impl, snap_store})
      continue_wire(sup, opts, boot, {impl, store}, {snap_impl, snap_store})
    end
  end

  # Abort on a bootstrap error; otherwise resolve the (now-seeded) start position and wire the
  # mode-specific tail.
  defp continue_wire(_sup, _opts, {:error, reason}, _checkpoint_store, _snapshot_store) do
    {:error, reason}
  end

  # A fresh/mid-snapshot boot holds the bootstrap's open query connection (in `readers`). If the
  # now-seeded checkpoint read (or start-position resolve) faults here, the coordinator never
  # starts — and a coordinator that never runs gets no `terminate/2` — so the query would leak;
  # close it before aborting (F1). Once `finish_wire` starts the coordinator, the coordinator
  # owns and closes the query.
  defp continue_wire(
         sup,
         opts,
         {:snapshot, _state, readers, _processed} = boot,
         {impl, store} = checkpoint_store,
         snapshot_store
       ) do
    with {:ok, resumed} <- CheckpointStore.read_position(impl, store),
         {:ok, start_position} <- Pipeline.resolve_start_position(opts, resumed) do
      finish_wire(sup, opts, boot, checkpoint_store, snapshot_store, start_position)
    else
      {:error, reason} ->
        close_readers_query(readers)
        {:error, reason}
    end
  end

  defp continue_wire(
         sup,
         opts,
         :complete = boot,
         {impl, store} = checkpoint_store,
         snapshot_store
       ) do
    with {:ok, resumed} <- CheckpointStore.read_position(impl, store),
         {:ok, start_position} <- Pipeline.resolve_start_position(opts, resumed) do
      finish_wire(sup, opts, boot, checkpoint_store, snapshot_store, start_position)
    end
  end

  # `status: :complete` ⇒ pure C1: the REAL sink is wired directly, no coordinator, no attach —
  # observably identical to the C1 stream.
  defp finish_wire(sup, opts, :complete, {impl, store}, _snapshot_store, start_position) do
    with {:ok, assembler} <- add_child(sup, Pipeline.assembler_spec(opts, {impl, store})),
         {:ok, _connection} <-
           add_child(
             sup,
             Pipeline.connection_spec(opts, assembler, start_position, {impl, store})
           ) do
      :ok
    end
  end

  # Fresh / mid-snapshot: the assembler's sink is the coordinator MODULE (name-resolved at call
  # time, so the assembler need not hold the coordinator pid at init — design § Pinned #4). The
  # coordinator starts with `processed_set` = the live watermark (P0, Task-8-F2); once it is up,
  # `attach_coordinator/2` injects the observer + arms the silent-death monitor. The coordinator
  # is started BEFORE the connection so the observer is attached before the stream advances the
  # watermark (no early-advance race; a missed pre-attach advance would recover anyway — the
  # watermark feed is a cumulative snapshot, not a delta).
  defp finish_wire(sup, opts, boot, {impl, store}, snapshot_store, start_position) do
    {:snapshot, snapshot_state, readers, processed_set} = boot
    assembler_opts = Keyword.put(opts, :sink, Coordinator)

    with {:ok, assembler} <-
           add_child(sup, Pipeline.assembler_spec(assembler_opts, {impl, store})),
         {:ok, coordinator} <-
           add_child(
             sup,
             Pipeline.coordinator_spec(opts,
               assembler: assembler,
               snapshot_store: snapshot_store,
               snapshot_state: snapshot_state,
               readers: readers,
               processed_set: processed_set
             )
           ) do
      # The coordinator is up and OWNS the query (its `terminate/2` closes it under
      # `Supervisor.stop`), so a failure below is released by the coordinator — not here.
      with :ok <- AssemblerServer.attach_coordinator(assembler, coordinator),
           {:ok, _connection} <-
             add_child(
               sup,
               Pipeline.connection_spec(opts, assembler, start_position, {impl, store})
             ) do
        :ok
      end
    else
      # The assembler/coordinator failed to start (a failed init gets no `terminate/2`), so the
      # query the coordinator would have owned leaks unless closed here (F1).
      {:error, reason} ->
        close_readers_query(readers)
        {:error, reason}
    end
  end

  # Close the shared `Capstan.Query` connection the bootstrap opened (all `readers` share it),
  # on a wiring abort BEFORE the coordinator started to own it. A `Query.close` on an
  # already-closed socket is a harmless no-op.
  defp close_readers_query(readers) do
    case Map.values(readers) do
      [%{query: %Query{} = query} | _] -> Query.close(query)
      _ -> :ok
    end
  end

  # Normalize `Supervisor.start_child/2`: a child whose start returns `{:ok, pid, info}`
  # would otherwise not match the `with` chain's `{:ok, pid}` and crash `start_link/1`
  # with a `CaseClauseError`. Every current child returns `{:ok, pid}`, so the 3-tuple
  # arm is defensive against a future child spec.
  defp add_child(sup, spec) do
    case Supervisor.start_child(sup, spec) do
      {:ok, pid} -> {:ok, pid}
      {:ok, pid, _info} -> {:ok, pid}
      {:error, _reason} = error -> error
    end
  end
end

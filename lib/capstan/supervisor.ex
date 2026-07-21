defmodule Capstan.Supervisor do
  @moduledoc """
  The pipeline supervisor (design § Architecture overview): it owns one pipeline's
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

  alias Capstan.CheckpointStore
  alias Capstan.Pipeline

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

  defp wire(sup, opts) do
    {impl, store_options} = Pipeline.checkpoint_store(opts)

    with {:ok, store} <- Supervisor.start_child(sup, Pipeline.store_spec(impl, store_options)),
         {:ok, resumed} <- CheckpointStore.read_position(impl, store),
         {:ok, start_position} <- Pipeline.resolve_start_position(opts, resumed),
         {:ok, assembler} <-
           Supervisor.start_child(sup, Pipeline.assembler_spec(opts, {impl, store})),
         {:ok, _connection} <-
           Supervisor.start_child(sup, Pipeline.connection_spec(opts, assembler, start_position)) do
      :ok
    end
  end
end

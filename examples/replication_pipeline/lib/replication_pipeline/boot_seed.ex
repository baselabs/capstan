defmodule ReplicationPipeline.BootSeed do
  @moduledoc """
  The one-shot first-boot seeder: write the checkpoint row from the SOURCE
  server's current `gtid_executed` if (and only if) no checkpoint exists yet.

  The seed runs SYNCHRONOUSLY in `start_link/1` (returning `:ignore`): the
  supervisor starts children in order, and the capstan child must not read the
  checkpoint store before the seed has committed — a racing seed leaves an
  empty checkpoint, and capstan then dumps the server's FULL retained history
  (observed: the source's init DDLs replayed as receipts).

  Capstan >= 1.2 also offers `start_position: :current`, but an explicit start
  position overrides a written checkpoint on EVERY boot — as a persistent
  child-spec option it would skip everything committed while the app was down,
  on every restart. Seed-once + default checkpoint resume remains the correct
  pattern for a durable pipeline.
  """

  @table "capstan_checkpoint"

  def child_spec(_arg) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [[]]}, restart: :temporary}
  end

  def start_link(_arg) do
    :ok = run()
    :ignore
  end

  def run do
    {:ok, src} = MyXQL.start_link(ReplicationPipeline.Application.source_opts())
    %MyXQL.Result{rows: [[gtid_executed]]} = MyXQL.query!(src, "SELECT @@global.gtid_executed")
    GenServer.stop(src)

    existing =
      MyXQL.query!(
        ReplicationPipeline.DestPool,
        "SELECT gtid_set FROM #{@table} WHERE pipeline_id = ?",
        [ReplicationPipeline.Application.pipeline_id()]
      )

    case existing.rows do
      [] ->
        MyXQL.query!(
          ReplicationPipeline.DestPool,
          "INSERT INTO #{@table} (pipeline_id, gtid_set) VALUES (?, ?) AS new " <>
            "ON DUPLICATE KEY UPDATE gtid_set = new.gtid_set",
          [ReplicationPipeline.Application.pipeline_id(), gtid_executed]
        )

        :ok

      [[_current]] ->
        :ok
    end
  end
end

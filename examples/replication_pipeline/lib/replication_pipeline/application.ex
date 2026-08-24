defmodule ReplicationPipeline.Application do
  @moduledoc """
  Supervision: destination pool → one-shot checkpoint seeder → capstan pipeline.

  All configuration is environment-driven (see README.md). On first boot —
  before the pipeline starts — `ReplicationPipeline.BootSeed` writes the
  checkpoint from the source's current `gtid_executed`, so the first dump
  delivers from now instead of requesting the server's full retained history
  (which a server that has purged refuses; capstan halts `:data_gap`).
  """

  use Application

  @source_tables [{"example_src", "orders"}]

  @impl true
  def start(_type, _args) do
    ReplicationPipeline.TelemetryLog.attach()

    children =
      [
        {MyXQL, dest_opts() |> Keyword.put(:name, ReplicationPipeline.DestPool)},
        ReplicationPipeline.BootSeed,
        {Capstan,
         connection: capstan_connection(),
         server_id: env!("CAPSTAN_SERVER_ID") |> String.to_integer(),
         sink: ReplicationPipeline.Sink,
         checkpoint_store: [
           module: ReplicationPipeline.CheckpointStore,
           options: [pool: ReplicationPipeline.DestPool, pipeline_id: pipeline_id()]
         ],
         tables: @source_tables}
      ]

    Supervisor.start_link(children, strategy: :one_for_one, name: ReplicationPipeline.Supervisor)
  end

  # MyXQL (the seeder + destination client) spells it `hostname:`; capstan's
  # connection block spells it `host:`. Two clients, two spellings — kept
  # explicit rather than shared.
  def source_opts do
    [
      hostname: env!("SOURCE_HOST"),
      port: String.to_integer(env!("SOURCE_PORT")),
      username: env!("SOURCE_USER"),
      password: env!("SOURCE_PASSWORD"),
      database: "example_src",
      ssl: false
    ]
  end

  def capstan_connection do
    [
      host: env!("SOURCE_HOST"),
      port: String.to_integer(env!("SOURCE_PORT")),
      username: env!("SOURCE_USER"),
      password: env!("SOURCE_PASSWORD"),
      ssl: false
    ]
  end

  def dest_opts do
    [
      hostname: env!("DEST_HOST"),
      port: String.to_integer(env!("DEST_PORT")),
      username: env!("DEST_USER"),
      password: env!("DEST_PASSWORD"),
      database: "example_dst"
    ]
  end

  def pipeline_id, do: env("PIPELINE_ID", "replication-pipeline-example")

  def source_tables, do: @source_tables

  defp env!(name), do: env(name, nil) || raise("missing required env #{name}")

  defp env(name, default), do: System.get_env(name) || default
end

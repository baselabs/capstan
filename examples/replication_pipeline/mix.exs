defmodule ReplicationPipeline.MixProject do
  use Mix.Project

  # The reference pipeline: capstan → value-free receipts + idempotent mirror
  # → destination MySQL with a durable GTID checkpoint. Repo-side example —
  # never shipped in the hex package (see the package files list in the root
  # mix.exs). Run it with docker compose; see README.md.
  def project do
    [
      app: :replication_pipeline,
      version: "0.1.0",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {ReplicationPipeline.Application, []}
    ]
  end

  defp deps do
    [
      # The CDC engine, from THIS checkout (the example is part of its repo).
      {:capstan, path: "../.."},
      # Destination + source-seeding client.
      {:myxql, "~> 0.7"}
    ]
  end
end

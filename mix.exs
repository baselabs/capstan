defmodule Capstan.MixProject do
  use Mix.Project

  @version "0.0.0-dev"
  @source_url "https://github.com/baselabs/capstan"

  def project do
    [
      app: :capstan,
      version: @version,
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      consolidate_protocols: Mix.env() != :test,
      deps: deps(),
      package: package(),
      docs: docs(),
      aliases: aliases(),
      name: "Capstan",
      description:
        "Framework-agnostic Elixir CDC consumer for MySQL binary-log replication (row-based " <>
          "binlog) with sink-owned, transaction-granularity exactly-once delivery.",
      source_url: @source_url,
      homepage_url: @source_url,
      dialyzer: [
        plt_add_apps: [:mix, :ex_unit, :decimal, :jason],
        plt_core_path: "priv/plts",
        plt_local_path: "priv/plts"
      ]
    ]
  end

  def cli do
    [preferred_envs: [credo: :test, dialyzer: :test]]
  end

  def application do
    [extra_applications: [:logger, :crypto, :ssl]]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      # Runtime. The binlog protocol client is implemented in-library over :gen_tcp
      # (probe-proven 2026-07-20); there is no MySQL replication-protocol dependency
      # to inherit. MyXQL is NOT a dependency — it is the Apache-2.0 reference for
      # the connection handshake, read at authoring time, never linked.
      {:decimal, "~> 3.1"},
      {:jason, "~> 1.4"},
      {:telemetry, "~> 1.0"},
      # Dev/Test
      {:ex_doc, "~> 0.31", only: :dev, runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:mix_audit, "~> 2.1", only: [:dev, :test], runtime: false}
    ]
  end

  defp package do
    [
      maintainers: ["rjpalermo"],
      files: ~w(lib .formatter.exs mix.exs README* LICENSE* CHANGELOG* usage-rules.md),
      licenses: ["MIT"],
      links: %{
        "GitHub" => @source_url,
        "Changelog" => "#{@source_url}/blob/main/CHANGELOG.md",
        "Documentation" => "https://hexdocs.pm/capstan"
      }
    ]
  end

  defp docs do
    [
      main: "readme",
      source_ref: "v#{@version}",
      source_url: @source_url,
      extras: ["README.md", "CHANGELOG.md"]
    ]
  end

  defp aliases do
    [
      quality: ["format --check-formatted", "credo --strict", "dialyzer"],
      audit: ["deps.unlock --check-unused", "hex.audit", "deps.audit"]
    ]
  end
end

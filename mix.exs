defmodule Capstan.MixProject do
  use Mix.Project

  @version "1.2.2"
  @source_url "https://github.com/baselabs/capstan"

  def project do
    [
      app: :capstan,
      version: @version,
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      consolidate_protocols: Mix.env() != :test,
      # Coverage measures the SHIPPED library — the test-support harness (the manual fixture
      # capture tool, the substrate case scaffolding, the value-free sweep) is scaffolding,
      # not product. The threshold stays Mix's default 90 and is enforced by the CI cover step.
      test_coverage: [
        ignore_modules: [~r/^Capstan\.(FixtureCapture|MysqlCase|ValueFree)/]
      ],
      deps: deps(),
      package: package(),
      docs: docs(),
      aliases: aliases(),
      name: "Capstan",
      description:
        "Framework-agnostic Elixir CDC consumer for MySQL binary-log replication (row-based " <>
          "binlog) — streams committed transactions to a pluggable sink, fail-closed, " <>
          "at-least-once (sink-owned effect-once mode planned).",
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
      # dotenvy loads the dev MySQL substrate tunables from .env in config/runtime.exs (Base-family
      # pattern). Scoped :dev/:test — capstan is a published library and must not force it on
      # consumers; config/ is not shipped to Hex either (see `package.files`).
      {:dotenvy, "~> 1.1", only: [:dev, :test]},
      {:ex_doc, "~> 0.31", only: :dev, runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:stream_data, "~> 1.0", only: [:dev, :test]},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:mix_audit, "~> 2.1", only: [:dev, :test], runtime: false}
    ]
  end

  defp package do
    [
      maintainers: ["rjpalermo"],
      # The tarball ships the decision record WITH the code it governs (ADRs + the
      # authored roadmap); contributor-workflow docs (testing.md -> scripts/, compose)
      # reference repo-only files and stay repo-side.
      files:
        ~w(lib .formatter.exs mix.exs README* LICENSE* NOTICE CHANGELOG* usage-rules.md docs/adr/* docs/ROADMAP.md),
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
      groups_for_extras: [
        Guides: ["docs/recipes.md", "docs/telemetry.md"]
      ],
      main: "readme",
      source_ref: "v#{@version}",
      source_url: @source_url,
      extras: [
        "README.md",
        "usage-rules.md",
        "CHANGELOG.md",
        {"docs/recipes.md", title: "Recipes"},
        {"docs/telemetry.md", title: "Telemetry Reference"},
        "docs/adr/0001-position-and-dedup-model.md",
        "docs/adr/0002-fail-closed-server-preconditions.md",
        "docs/adr/0003-transaction-shape-and-checkpoint-semantics.md",
        "docs/adr/0004-c1-scope-lib-owned-checkpoint-only.md",
        "docs/adr/0005-initial-snapshot-cursor-gate-brief-lock.md",
        "docs/adr/0006-xa-prepare-commit-tracking.md",
        "docs/adr/0007-value-free-boundary-rule-1.md",
        "docs/adr/0008-pure-elixir-protocol-client.md",
        "docs/adr/0009-fail-closed-supervision-and-streaming-liveness.md",
        "docs/adr/0010-exclusive-end-bound-of-com-binlog-dump-gtid.md",
        "docs/adr/0011-transaction-compression-precondition.md"
      ],
      groups_for_modules: [
        "Consumer API": [
          Capstan,
          Capstan.Sink,
          Capstan.CheckpointStore,
          Capstan.CheckpointStore.InMemory,
          Capstan.Transaction,
          Capstan.Change,
          Capstan.SchemaChange,
          Capstan.Position,
          Capstan.Gtid,
          Capstan.Error,
          Capstan.SnapshotStore,
          Capstan.SnapshotStore.InMemory,
          Capstan.Snapshot.Meta,
          Capstan.Snapshot.State
        ],
        Internals: [
          Capstan.Assembler,
          Capstan.AssemblerServer,
          Capstan.Config,
          Capstan.Connection,
          Capstan.Pipeline,
          Capstan.Supervisor,
          Capstan.Telemetry,
          Capstan.Binlog.Decoder,
          Capstan.Binlog.Event,
          Capstan.Binlog.Rows,
          Capstan.Binlog.TableMap,
          Capstan.Binlog.TableRegistry,
          Capstan.Casting.Types,
          Capstan.Protocol.Command,
          Capstan.Protocol.Handshake,
          Capstan.Protocol.Packet,
          Capstan.Snapshot,
          Capstan.Snapshot.Coordinator,
          Capstan.Snapshot.CursorGate,
          Capstan.Snapshot.Chunk,
          Capstan.Snapshot.ChunkReader,
          Capstan.Snapshot.PrimaryKey
        ]
      ]
    ]
  end

  defp aliases do
    [
      quality: ["format --check-formatted", "credo --strict", "dialyzer"],
      audit: ["deps.unlock --check-unused", "hex.audit", "deps.audit"]
    ]
  end
end

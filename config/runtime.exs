import Config

# Dev/test only: load the dev MySQL substrate tunables from .env via Dotenvy, mirroring the
# Base-family pattern (sirtify/config/runtime.exs). capstan is a library — dotenvy is a :dev/:test-only
# dep and this config is never shipped to consumers (mix.exs `package.files`) — so the block is guarded
# to :dev/:test (in :prod the dotenvy module is absent and there is no substrate to configure).
#
# Order matters: `System.get_env()` is sourced LAST, so a real environment variable always wins over a
# .env file value. That is what lets CI (which exports MYSQL_PORT_80/… and ships no .env) and any
# shell override work unchanged. `require_files: false` → a missing .env is fine.
if config_env() in [:dev, :test] do
  import Dotenvy

  source!(
    [
      Path.expand("../.env", __DIR__),
      Path.expand("../.env.#{config_env()}", __DIR__),
      System.get_env()
    ],
    require_files: false
  )

  # The single source of the dev MySQL substrate port. NOTHING in lib/ or test/ hard-codes a port —
  # every reader goes through this app env (Capstan.MysqlCase.shared_port/0). The defaults below are
  # the committed random high ports (also in .env.example); .env or a real env var overrides them.
  config :capstan, :mysql_substrate,
    port_80: env!("MYSQL_PORT_80", :integer, 11619),
    port_84: env!("MYSQL_PORT_84", :integer, 15401),
    # The 8.0 substrate's CA (for the TLS handshake test) when it does not run as the
    # `mysql-cdc-probe` Compose container; unset reads it from that container.
    ca_file: env!("CAPSTAN_SUBSTRATE_CA_FILE", :string, nil),
    # The disposable MySQL 8.0 the destructive (:disposable_mysql) marquees reset; unset (the
    # default) leaves those marquees unable to run, and they are excluded unless selected.
    disposable_port: env!("CAPSTAN_DISPOSABLE_MYSQL_PORT", :integer?, nil)

  # ADR-0013: the Amazon Aurora MySQL cluster the `:aurora_mysql` marquees run against
  # (excluded unless selected — like the disposable tier, never a silent pass). The base
  # five keys are REQUIRED for that tier (MysqlCase.aurora_connection!/0 raises naming
  # the missing ones); cluster_id/reader_host/misconfig are per-marquee prerequisites the
  # marquee itself names. No value here is ever logged or telemetered (Rule 1).
  config :capstan, :aurora_substrate,
    host: env!("AURORA_MYSQL_HOST", :string, nil),
    port: env!("AURORA_MYSQL_PORT", :integer?, nil),
    user: env!("AURORA_MYSQL_USER", :string, nil),
    password: env!("AURORA_MYSQL_PASSWORD", :string, nil),
    ca_file: env!("AURORA_MYSQL_CA_FILE", :string, nil),
    cluster_id: env!("AURORA_MYSQL_CLUSTER_ID", :string, nil),
    reader_host: env!("AURORA_MYSQL_READER_HOST", :string, nil),
    misconfig: env!("AURORA_MYSQL_MISCONFIG", :string, nil)

  # ADR-0013's simulator arm: the `:aurora_sim` marquees run against the local
  # `scripts/aurora-sim/` stack (three real MySQL 8.0 nodes behind an HAProxy endpoint).
  # Everything is local throwaway (127.0.0.1, root/probe), so the defaults ARE the
  # compose stack's ports — the tier is excluded unless selected and fails loudly if
  # nothing listens.
  config :capstan, :aurora_sim,
    host: env!("AURORA_SIM_HOST", :string, "127.0.0.1"),
    user: env!("AURORA_SIM_USER", :string, "root"),
    password: env!("AURORA_SIM_PASSWORD", :string, "probe"),
    endpoint_port: env!("AURORA_SIM_ENDPOINT_PORT", :integer, 37521),
    writer_port: env!("AURORA_SIM_WRITER_PORT", :integer, 37524),
    promotable_port: env!("AURORA_SIM_PROMOTABLE_PORT", :integer, 37525),
    reader_port: env!("AURORA_SIM_READER_PORT", :integer, 37527),
    admin_port: env!("AURORA_SIM_ADMIN_PORT", :integer, 37528)
end

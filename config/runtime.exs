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
end

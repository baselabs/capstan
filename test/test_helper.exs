# `:live` and `:integration` need the shared mysql-cdc-probe; `:disposable_mysql` needs the
# disposable MySQL (CAPSTAN_DISPOSABLE_MYSQL_PORT, a server the suite may reset); `:aurora_mysql`
# needs a real Aurora MySQL cluster (the AURORA_MYSQL_* environment, ADR-0013); `:aurora_sim`
# needs the local `scripts/aurora-sim/` stack (real MySQL nodes behind a flipping endpoint —
# no AWS account). All five are excluded by default so `mix test` runs only the pure-unit
# suite. When a run does NOT select a tag, its marquees show as a genuine ExUnit "excluded"
# count, never a spurious pass. CI opts each class in with `--only <tag>`; `--only` exits
# non-zero if a tag matches zero tests, so a mis-tag can never silently drop a marquee.
ExUnit.start(exclude: [:live, :integration, :disposable_mysql, :aurora_mysql, :aurora_sim])

# `:live` and `:integration` need the shared mysql-cdc-probe; `:requires_docker` needs a
# throwaway container (the docker-gated marquees). All three are excluded by default so
# `mix test` runs only the pure-unit suite. When a run does NOT select `:requires_docker`,
# those marquees show as a genuine ExUnit "excluded" count — never a spurious pass. CI opts
# each class in with `--only <tag>`; `--only` exits non-zero if a tag matches zero tests, so a
# mis-tag can never silently drop a marquee.
ExUnit.start(exclude: [:live, :integration, :requires_docker])

# The reference pipeline as one OTP release in one container.
# Build context is the REPO ROOT (the example path-depends on capstan):
#   docker compose build   (from examples/replication_pipeline)
#
# Builder and runtime use the same base so the release's ERTS always matches.

FROM hexpm/elixir:1.15.8-erlang-26.2.5.21-debian-bookworm-20260803-slim AS builder

RUN apt-get update -y && apt-get install -y --no-install-recommends build-essential git ca-certificates && apt-get clean

WORKDIR /build

# capstan (path dep) is COPYed first so dep layers cache independently.
COPY mix.exs mix.lock README.md LICENSE ./
COPY lib ./lib
COPY examples/replication_pipeline/mix.exs ./examples/replication_pipeline/mix.exs
COPY examples/replication_pipeline/lib ./examples/replication_pipeline/lib
COPY examples/replication_pipeline/priv ./examples/replication_pipeline/priv

WORKDIR /build/examples/replication_pipeline

ENV MIX_ENV=prod

RUN mix local.hex --force && mix local.rebar --force \
    && mix deps.get --only prod \
    && mix compile --warnings-as-errors

RUN mix release

FROM hexpm/elixir:1.15.8-erlang-26.2.5.21-debian-bookworm-20260803-slim

RUN apt-get update -y && apt-get install -y --no-install-recommends libssl3 ca-certificates && apt-get clean

WORKDIR /app
COPY --from=builder /build/examples/replication_pipeline/_build/prod/rel/replication_pipeline ./

CMD ["/app/bin/replication_pipeline", "start"]

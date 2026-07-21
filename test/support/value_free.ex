defmodule Capstan.ValueFree do
  @moduledoc """
  Rule-1 assertion helper (design § Invariant conformance) — the day-one enforcement
  layer (F11).

  A sentinel planted in an input — a row VALUE and the connection PASSWORD (Task 16); a
  DDL literal and a `ROWS_QUERY` statement (Task 17) — must appear in **no** log line and
  **no** telemetry metadata payload across every error/halt path reachable with the
  surface under test. Task 16 lands the two vectors that need only `Capstan.Error` and
  `Capstan.Telemetry`; the remaining two, which need a live pipeline, land in Task 17.

  The helper is deliberately **adversarially non-vacuous**: un-redacting any one vector
  (leaking its sentinel through `Capstan.Error.from/1`) makes that vector's assertion go
  RED, through the log channel or the telemetry channel or both. A helper that reported
  green over a real leak is the exact failure this guards.
  """

  import ExUnit.Assertions
  import ExUnit.CaptureLog
  require Logger

  alias Capstan.Error
  alias Capstan.Telemetry

  # Distinctive, unlikely-to-collide sentinels: a match anywhere is a genuine leak, never
  # an incidental substring of some structural atom.
  @row_value_sentinel "capstan_rule1_row_value_sentinel_9f3a1c7e"
  @password_sentinel "capstan_rule1_password_sentinel_4b8d2e60"

  # Every capstan telemetry event (design § Events / telemetry) — the helper attaches to
  # ALL of them so a sentinel riding any payload is caught, not only the ones a given
  # path happens to emit.
  @capstan_events [
    [:capstan, :connection, :established],
    [:capstan, :connection, :halt],
    [:capstan, :transaction, :committed],
    [:capstan, :transaction, :filtered],
    [:capstan, :transaction, :skipped],
    [:capstan, :schema_change, :received],
    [:capstan, :gap, :detected]
  ]

  @doc "The row-value sentinel the row vector plants."
  @spec row_value_sentinel() :: String.t()
  def row_value_sentinel, do: @row_value_sentinel

  @doc "The password sentinel the password vector plants."
  @spec password_sentinel() :: String.t()
  def password_sentinel, do: @password_sentinel

  @doc """
  Row-VALUE vector: a raw driver error whose message embeds a row value is normalised by
  `Capstan.Error.from/1` and surfaced through the log AND a `connection.halt` telemetry
  payload; the row value must appear in neither.

  Un-redacting `Capstan.Error.from/1` (retaining the raw exception message on `:reason`
  or `:shape`) makes this go RED.
  """
  @spec assert_row_value_free() :: :ok
  def assert_row_value_free do
    raw = %RuntimeError{message: "duplicate entry '#{@row_value_sentinel}' for key 'PRIMARY'"}
    refute_leaks(@row_value_sentinel, fn -> drive_error_paths(raw) end)
  end

  @doc """
  PASSWORD vector: a connect failure whose raw reason carries the connection password is
  normalised by `Capstan.Error.from/1` and surfaced through the log AND a
  `connection.halt` telemetry payload; the password must appear in neither.

  Un-redacting `Capstan.Error.from/1` (retaining the tuple payload) makes this go RED.
  """
  @spec assert_password_free() :: :ok
  def assert_password_free do
    raw = {:connect_failed, [host: ~c"db.internal", port: 3306, password: @password_sentinel]}
    refute_leaks(@password_sentinel, fn -> drive_error_paths(raw) end)
  end

  @doc """
  Run `fun` while capturing all log output and all `[:capstan | _]` telemetry metadata,
  then assert `sentinel` appears in neither. Metadata is deep-scanned (keys AND values,
  via `inspect/1`), so a sentinel nested anywhere in a payload is caught.
  """
  @spec refute_leaks(String.t(), (-> any())) :: :ok
  def refute_leaks(sentinel, fun) when is_binary(sentinel) and is_function(fun, 0) do
    {:ok, agent} = Agent.start_link(fn -> [] end)
    handler_id = {__MODULE__, make_ref()}
    :ok = :telemetry.attach_many(handler_id, @capstan_events, &__MODULE__.__collect__/4, agent)

    log =
      try do
        capture_log(fun)
      after
        :telemetry.detach(handler_id)
      end

    metas = Agent.get(agent, & &1)
    Agent.stop(agent)

    refute String.contains?(log, sentinel),
           "Rule 1 violation: sentinel #{inspect(sentinel)} leaked into a LOG line:\n#{log}"

    for meta <- metas do
      refute String.contains?(inspect(meta), sentinel),
             "Rule 1 violation: sentinel #{inspect(sentinel)} leaked into telemetry metadata " <>
               inspect(meta)
    end

    :ok
  end

  @doc false
  # A module-function handler (not a local capture) keeps :telemetry from logging a
  # per-attach performance warning; the accumulator agent rides in `config`.
  @spec __collect__([atom(), ...], map(), map(), Agent.agent()) :: :ok
  def __collect__(_event, _measurements, metadata, agent) do
    Agent.update(agent, &[metadata | &1])
  end

  # The error/halt paths reachable with only Error + Telemetry: normalise the raw reason,
  # then surface the normalised (value-free) error through BOTH channels a real halt uses
  # — the crash/error LOG and the connection.halt telemetry payload. If the normaliser
  # scrubs correctly, neither channel carries the sentinel.
  defp drive_error_paths(raw) do
    error = Error.from(raw)
    Logger.error(Exception.message(error))
    Logger.error(inspect(error))
    Telemetry.event([:capstan, :connection, :halt], %{}, %{reason: error.reason})
  end
end

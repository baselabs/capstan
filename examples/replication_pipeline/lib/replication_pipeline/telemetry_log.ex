defmodule ReplicationPipeline.TelemetryLog do
  @moduledoc """
  The minimal operability floor: capstan's halts and stream stalls are
  TELEMETRY events, not logs — a consumer that attaches nothing sees a
  silently-stopped pipeline. This handler makes the fail-closed posture
  LOUD: a halt or stall is logged (value-free metadata only — reason atoms,
  never row data, per capstan Rule 1).
  """

  require Logger

  def attach do
    events = [
      [:capstan, :connection, :halt],
      [:capstan, :connection, :stream_timeout],
      [:capstan, :connection, :established]
    ]

    :ok = :telemetry.attach_many({__MODULE__, make_ref()}, events, &__MODULE__.handle/4, nil)
  end

  def handle([:capstan, :connection, :halt], _m, %{reason: reason}, _config) do
    Logger.error("capstan pipeline HALTED (fail-closed): #{inspect(reason)}")
  end

  def handle([:capstan, :connection, :stream_timeout], _m, _meta, _config) do
    Logger.error("capstan stream stalled (missed heartbeats) — the pipeline will reconnect")
  end

  def handle([:capstan, :connection, :established], _m, _meta, _config) do
    Logger.info("capstan stream established")
  end
end

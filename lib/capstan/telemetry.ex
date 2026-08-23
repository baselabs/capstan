defmodule Capstan.Telemetry do
  @moduledoc """
  Value-free telemetry emission. Provides the metadata **allowlist** for "no row values
  and no passwords in telemetry" (Rule 1): `event/3` routes
  through `validate!/1`, which **raises** on any key outside the allowlist, so a stray row
  value or password cannot ride a payload emitted through this module.

  Only structural metadata — GTIDs, schema / table
  names, DDL kinds, atom reasons, server identity, TLS posture, and server-reported
  missing GTIDs — is permitted.

  **Scope (C1):** this is the pipeline's sole telemetry emit path. Every emitter in
  `Capstan.Connection` and `Capstan.AssemblerServer` routes through `event/3` (Rule-1
  completion), so the allowlist gates them at runtime — a future emitter attaching a
  row value or password raises rather than shipping it. The complementary test-time
  guarantee is the `Capstan.ValueFree` helper, which scans the log and telemetry channels
  (and, for the live vectors, the delivered sink outputs) for a planted sentinel across
  every delivered and error/halt path.

  Mirrors `replicant/lib/replicant/telemetry.ex`.
  """

  # Exactly the value-free metadata keys of the design's Events / telemetry table. The
  # allowlist guards METADATA, where a stray value would otherwise ride; the measurement
  # channel guards VALUES (counts and monotonic durations only — numbers carry no
  # identity, so a row value or password can never travel as a measurement either).
  @allowed_meta_keys ~w(server_version server_uuid tls reason gtid schema table kind missing_gtids)a

  @doc "The permitted telemetry metadata keys."
  @spec allowed_meta_keys() :: [atom()]
  def allowed_meta_keys, do: @allowed_meta_keys

  @doc """
  Emit a telemetry event with allowlist-validated metadata and numeric-only measurements.

  Raises `ArgumentError` if `meta` carries any key outside `allowed_meta_keys/0`, or if
  any measurement value is not a non-negative number — the fail-closed points that keep
  a value off a payload on both channels.
  """
  @spec event([atom(), ...], map(), map()) :: :ok
  def event(name, measurements, meta)
      when is_list(name) and is_map(measurements) and is_map(meta) do
    :telemetry.execute(name, validate_measurements!(measurements), validate!(meta))
  end

  @doc false
  @spec validate_measurements!(map()) :: map()
  def validate_measurements!(measurements) when is_map(measurements) do
    if Enum.all?(measurements, fn {_k, v} -> is_number(v) and v >= 0 end) do
      measurements
    else
      raise ArgumentError,
            "telemetry measurements must be numeric (non-negative counts and durations), " <>
              "got #{inspect(measurements)} (no row values or passwords in telemetry)"
    end
  end

  @doc false
  @spec validate!(map()) :: map()
  def validate!(meta) when is_map(meta) do
    case Map.keys(meta) -- @allowed_meta_keys do
      [] ->
        meta

      bad ->
        raise ArgumentError,
              "telemetry metadata keys #{inspect(bad)} are not in the value-free allowlist " <>
                "#{inspect(@allowed_meta_keys)} (no row values or passwords in telemetry)"
    end
  end
end

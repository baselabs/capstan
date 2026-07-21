defmodule Capstan.Telemetry do
  @moduledoc """
  Value-free telemetry emission. Owns the metadata **allowlist** — the single
  enforcement point for "no row values and no passwords in telemetry" (Rule 1, design
  § Events / telemetry).

  Column values and the connection password are never permitted; only the structural
  metadata the design's event table lists — GTIDs, schema / table names, DDL kinds, atom
  reasons, server identity, TLS posture, and server-reported missing GTIDs — may ride a
  payload. An off-allowlist key **raises** rather than shipping a value downstream, so a
  stray row value or password can never leave through a telemetry payload even if a
  future emitter tries to attach one.

  Mirrors `replicant/lib/replicant/telemetry.ex`.
  """

  # Exactly the value-free metadata keys of the design's Events / telemetry table.
  # MEASUREMENTS (change_count, lag_ms) are numeric and carry no identity, so they are not
  # gated here; the allowlist guards METADATA, where a stray value would otherwise ride.
  @allowed_meta_keys ~w(server_version server_uuid tls reason gtid schema table kind missing_gtids)a

  @doc "The permitted telemetry metadata keys."
  @spec allowed_meta_keys() :: [atom()]
  def allowed_meta_keys, do: @allowed_meta_keys

  @doc """
  Emit a telemetry event with allowlist-validated metadata.

  Raises `ArgumentError` if `meta` carries any key outside `allowed_meta_keys/0` — the
  fail-closed point that keeps a value off a payload.
  """
  @spec event([atom(), ...], map(), map()) :: :ok
  def event(name, measurements, meta)
      when is_list(name) and is_map(measurements) and is_map(meta) do
    :telemetry.execute(name, measurements, validate!(meta))
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

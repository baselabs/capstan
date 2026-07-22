defmodule Capstan.Error do
  @moduledoc """
  A typed, **value-free** error — the boundary normaliser that scrubs a raw driver /
  socket error, exception, or reason tuple to a stable atom `reason` (Rule 1, design
  § Rule 1 / F11).

  No row value, column value, or connection password ever reaches a field here. A raw
  reason that could embed a value — a `%SomeError{message: "... 'secret' ..."}`, a
  connection keyword carrying `password:`, a nested tuple — is reduced to its structural
  shape: the exception's MODULE name (never its message) on `:shape`, or a stable atom on
  `:reason`. `message/1` and the derived `Inspect` render ONLY the structural fields, so
  even a field a careless caller loaded with a value is never surfaced by the default
  string / inspect forms.

  Mirrors `replicant/lib/replicant/error.ex` — refuse to leak DB payloads at the boundary.
  """

  @typedoc "A stable, value-free error reason atom."
  @type reason ::
          :config_invalid
          | :server_id_required
          | :tls_verification_unspecified
          | :invalid_sink
          | :sink_missing_handle_transaction
          | :sink_missing_checkpoint
          | :sink_missing_handle_schema_change
          | :checkpoint_store_required
          | :sink_owned_mode_unsupported
          | :start_position_current_unsupported
          | :start_position_override_unsupported
          # C2 initial-snapshot fail-closed halts (one distinct reason per silent-loss
          # condition; each a bare atom — `from/1`'s `| atom()` branch carries them value-free).
          | :sink_missing_handle_snapshot
          | :snapshot_table_not_captured
          | :snapshot_table_no_primary_key
          | :snapshot_pk_unsupported_type
          | :snapshot_source_mismatch
          | :snapshot_lock_unavailable
          | :snapshot_schema_drifted
          | :snapshot_chunk_read_failed
          | :snapshot_query_connect_failed
          | :snapshot_bootstrap_gtid_read_failed
          | :snapshot_state_read_failed
          | :snapshot_state_write_failed
          | :snapshot_coordinator_down
          | :unknown
          | atom()

  @type t :: %__MODULE__{
          reason: reason(),
          shape: String.t() | nil
        }

  defexception [:reason, :shape]

  @doc """
  Normalise any raw reason / exception / tuple into a value-free `Capstan.Error`.

    * a `Capstan.Error` passes through unchanged;
    * a value-free atom stays as the `:reason` (capstan's reasons are atoms by
      construction);
    * an exception keeps ONLY `inspect(module)` on `:shape` — its message, which may
      embed a row value or a password, is **discarded**;
    * a `{tag, payload}` tuple keeps only the atom `tag` as the reason and **discards**
      the payload (which may carry a value or the connection keyword);
    * anything else — a bare string, a map, a list — is `:unknown` with no retained
      payload (a value must never survive as a captured term).
  """
  @spec from(term()) :: t()
  def from(%__MODULE__{} = error), do: error
  def from(reason) when is_atom(reason), do: %__MODULE__{reason: reason}

  def from(%{__exception__: true, __struct__: module}),
    do: %__MODULE__{reason: :unknown, shape: inspect(module)}

  def from({tag, _payload}) when is_atom(tag), do: %__MODULE__{reason: tag}
  def from(_other), do: %__MODULE__{reason: :unknown}

  @impl true
  @spec message(t()) :: String.t()
  def message(%__MODULE__{reason: reason, shape: shape}) do
    ["capstan error", "reason=#{reason}", shape && "shape=#{shape}"]
    |> Enum.reject(&is_nil/1)
    |> Enum.join(" ")
  end

  defimpl Inspect do
    import Inspect.Algebra

    def inspect(err, opts) do
      # Render ONLY structural fields; a careless caller cannot surface a value through
      # the default inspect because no value-bearing field exists to render.
      safe = Map.take(err, [:reason, :shape])
      concat(["#Capstan.Error<", to_doc(safe, opts), ">"])
    end
  end
end

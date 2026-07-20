defmodule Capstan.Position do
  @moduledoc """
  A replication position: the processed GTID set plus diagnostic binlog coordinates.

  `gtid_set` is AUTHORITATIVE and the ONLY persisted field (design Q1/Q12). `file`
  and `pos` are DIAGNOSTIC ONLY — **never an ordering key**. A scalar ordinal built
  from `file`/`pos` moves backward after failover or `RESET BINARY LOGS AND GTIDS`
  while the GTID set moves forward, so no `ordinal` field is exported (Q12).

  `to_persisted/1` and `from_persisted/1` round-trip only `gtid_set`: persisting a
  `Position` and restoring it always yields `file: nil, pos: nil`, even when the
  original carried diagnostic coordinates. There is exactly one durable value.
  """

  @type t :: %__MODULE__{
          gtid_set: String.t(),
          file: String.t() | nil,
          pos: non_neg_integer() | nil
        }

  defstruct [:gtid_set, :file, :pos]

  @doc """
  Returns the persisted representation of `position` — the `gtid_set` string alone.

  `file`/`pos` are diagnostic and are never persisted.
  """
  @spec to_persisted(t()) :: String.t()
  def to_persisted(%__MODULE__{gtid_set: gtid_set}), do: gtid_set

  @doc """
  Builds a `Position` from a persisted `gtid_set` string.

  `file` and `pos` are always `nil` — they are diagnostic-only and never survive
  persistence.
  """
  @spec from_persisted(String.t()) :: t()
  def from_persisted(gtid_set) when is_binary(gtid_set) do
    %__MODULE__{gtid_set: gtid_set, file: nil, pos: nil}
  end
end

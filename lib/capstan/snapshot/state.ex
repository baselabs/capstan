defmodule Capstan.Snapshot.State do
  @moduledoc """
  The durable snapshot-progress state — one value per pipeline identity, persisted through
  a `Capstan.SnapshotStore` (mirrors `Capstan.CheckpointStore`'s one-durable-value model).

  `status` is the phase (`:initializing` before the `P0` seed lands, `:snapshotting` during
  the backfill, `:complete` once every table is done — a `:complete` store never
  re-snapshots). `p0` is the pipeline-global floor GTID position the stream was seeded from
  (stored so the bootstrap seed is re-runnable and immune to `@@gtid_executed` drift across a
  bootstrap crash). `tables` maps each `{schema, table}` to its per-table progress:
  `fingerprint` (structural drift guard), `pk_columns`/`pk_types` (the introspected primary
  key), `pk_cursor` (`:start` before the first chunk, else the canonical PK of the last
  backfilled row), and `done?`.

  ## Rule 1 (Ch5)

  `pk_cursor` and `fingerprint` are USER DATA (a row value / a column-derived hash). The
  `@derive {Inspect, only: [:status, :p0]}` elides the entire `tables` map, so an incidental
  `inspect/1` (a crash dump, a logger) can never surface a cursor value. The `SnapshotStore`
  behaviour separately BINDS its implementers to never log/telemeter `write/2` args.
  """

  @type pk_cursor :: term() | :start

  @type table_progress :: %{
          fingerprint: String.t(),
          pk_columns: [String.t()],
          pk_types: [atom()],
          pk_cursor: pk_cursor(),
          done?: boolean()
        }

  @type t :: %__MODULE__{
          status: :initializing | :snapshotting | :complete,
          p0: String.t() | nil,
          tables: %{optional({String.t(), String.t()}) => table_progress()}
        }

  # Rule 1 (Ch5): `tables` holds row-derived cursors + fingerprints (user data). Rendering
  # only `status`/`p0` (a phase atom + a GTID-set string, both value-free) elides them, so no
  # incidental inspect of the state can leak a cursor.
  @derive {Inspect, only: [:status, :p0]}
  defstruct status: :initializing, p0: nil, tables: %{}
end

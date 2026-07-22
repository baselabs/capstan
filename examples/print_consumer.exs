# A minimal end-to-end capstan consumer.
#
# It starts a pipeline against a MySQL source and PRINTS every committed row change of the
# `capstan_example.demo` table as it streams. Run it, then INSERT/UPDATE/DELETE rows in that table
# and watch them appear. Full walkthrough: examples/README.md.
#
#   docker compose up -d --wait                 # bring up the dev substrate first
#   mix run examples/print_consumer.exs
#
# Defaults target the dev substrate; an adopter points the `connection:` block at their own replica
# (host/port/user/password/TLS) via the SOURCE_* env vars. Nothing here is capstan-test-only.

# --- the sink: three callbacks are the whole consumer contract (usage-rules.md) -------------------
defmodule PrintSink do
  @behaviour Capstan.Sink

  @impl Capstan.Sink
  def handle_transaction(%Capstan.Transaction{gtid: gtid, changes: changes} = txn) do
    # `changes` is a single-pass Enumerable — enumerate exactly ONCE (never length/1). Here we just
    # print; a real sink applies each change as an idempotent UPSERT/DELETE keyed on the PK.
    Enum.each(changes, fn %Capstan.Change{
                            op: op,
                            schema: s,
                            table: t,
                            record: rec,
                            old_record: old
                          } ->
      case op do
        :update -> IO.puts("  update #{s}.#{t}: #{inspect(old)} -> #{inspect(rec)}")
        :delete -> IO.puts("  delete #{s}.#{t}: #{inspect(old)}")
        _insert -> IO.puts("  insert #{s}.#{t}: #{inspect(rec)}")
      end
    end)

    IO.puts("[txn #{gtid}] committed\n")

    # {:ok, position} advances the lib-owned checkpoint to this transaction's position. {:error, _}
    # would HALT fail-closed without advancing (re-delivered on restart — at-least-once).
    {:ok, txn.position}
  end

  @impl Capstan.Sink
  def handle_schema_change(%Capstan.SchemaChange{schema: s, table: t, kind: kind}, _position) do
    # DDL arrives as structured schema/table/kind only — the raw statement text is redacted (Rule 1).
    IO.puts("[ddl] #{kind} #{s}.#{t}\n")
    :ok
  end
end

# --- a minimal checkpoint store, SEEDABLE with a start position -----------------------------------
# Process-lifetime only (not durable across a restart). Production implements these same two
# callbacks over storage it already trusts (usage-rules.md § Checkpoint store). Seeding it with the
# source's current `@@global.gtid_executed` starts the stream "from now" instead of replaying all
# retained history (usage-rules.md § First start).
defmodule ExampleStore do
  @behaviour Capstan.CheckpointStore

  def start_link(opts) do
    Agent.start_link(fn -> Keyword.get(opts, :initial) end)
  end

  @impl Capstan.CheckpointStore
  def read(store), do: {:ok, Agent.get(store, & &1)}

  @impl Capstan.CheckpointStore
  def write(store, gtid_set) do
    Agent.update(store, fn _ -> gtid_set end)
    :ok
  end
end

# --- start the pipeline ---------------------------------------------------------------------------
port =
  String.to_integer(System.get_env("SOURCE_PORT") || System.get_env("MYSQL_PORT_80") || "11619")

# Set START_GTID to the source's `SELECT @@global.gtid_executed` to stream only NEW changes (the
# README shows how). Unset ⇒ nil ⇒ capstan replays all retained history (and halts :data_gap if the
# server has already purged its earliest logs).
start_gtid = System.get_env("START_GTID")
if start_gtid in [nil, ""], do: IO.puts("(no START_GTID — replaying all retained history)\n")

{:ok, _sup} =
  Capstan.start_link(
    connection: [
      host: System.get_env("SOURCE_HOST") || "127.0.0.1",
      port: port,
      username: System.get_env("SOURCE_USER") || "capstan_sha2",
      password: System.get_env("SOURCE_PASSWORD") || "capstan_sha2_pw",
      # The dev substrate is plaintext. A real source uses TLS: `ssl: true` + `cacertfile:` (and
      # `server_name_indication: :disable` for MySQL's SAN-less auto-cert) — see usage-rules.md.
      ssl: false
    ],
    server_id: String.to_integer(System.get_env("CAPSTAN_SERVER_ID") || "9001"),
    sink: PrintSink,
    checkpoint_store: [module: ExampleStore, options: [initial: start_gtid]],
    tables: [{"capstan_example", "demo"}]
  )

IO.puts(
  "capstan example consumer streaming `capstan_example.demo` from :#{port} — Ctrl-C twice to stop.\n"
)

Process.sleep(:infinity)

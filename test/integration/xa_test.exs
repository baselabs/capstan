defmodule Capstan.Integration.XaTest do
  @moduledoc """
  Live-substrate XA marquees (C5, ADR-0006) — the acceptance gate.

  Each drives a REAL `xa: :track` pipeline against mysql-cdc-probe with the two-phase
  grammar captured live: prepare terminates its own GTID'd transaction (type 38), the
  resolution arrives as a separate GTID'd `XA COMMIT`/`XA ROLLBACK` query.

    * **commit delivers exactly once** — the prepared rows are delivered only at the
      resolution, one transaction, once (the held-out watermark).
    * **rollback delivers zero rows** — the rows NEVER reach the sink; the resolution
      is a row-less watermark advance.
    * **dangling pre-start resolution** — a prepare that predates the pipeline (dump
      skips its rows) resolves as a row-less advance, not a desync halt (the
      connect-time XA RECOVER pre-seed).
    * **the default :refuse posture halts on XA** — the C1 behavior, byte-for-byte.

  `:integration`-tagged; never restarts the shared container.
  """
  use ExUnit.Case, async: false

  alias Capstan.MysqlCase
  alias Capstan.MysqlCase.{DurableStore, Sink}
  alias Capstan.Protocol.{Command, Handshake}

  @moduletag :integration

  setup_all do
    MysqlCase.ensure_sha2_user!(MysqlCase.query_connection())
    :ok
  end

  setup do
    Sink.configure(%{pid: self()})
    on_exit(fn -> Sink.clear() end)

    table = "xa_mrq_#{:erlang.unique_integer([:positive]) |> rem(100_000)}"

    qconn = MysqlCase.socket!(MysqlCase.query_connection())

    MysqlCase.run_all!(qconn, [
      "DROP TABLE IF EXISTS #{table}",
      "CREATE TABLE #{table} (id INT PRIMARY KEY, v VARCHAR(32)) ENGINE=InnoDB"
    ])

    on_exit(fn -> MysqlCase.close!(qconn) end)
    {:ok, table: table, qconn: qconn}
  end

  test "a prepared-then-committed XA delivers its rows EXACTLY ONCE, at the resolution", ctx do
    sup = start_tracking_pipeline!(ctx)
    # Streaming barrier: a plain committed row delivered proves the pipeline is LIVE
    # before the XA begins (otherwise the async establish races the prepare).
    MysqlCase.run!(ctx.qconn, "INSERT INTO #{ctx.table} (id, v) VALUES (0, 'warm-up')")
    assert {:txn, _g0, _c0, _p0} = collect_one()

    ref = Process.monitor(MysqlCase.assembler_pid(sup))
    xid = "'mrz-#{System.unique_integer([:positive])}'"

    MysqlCase.run_all!(ctx.qconn, [
      "XA START #{xid}",
      "INSERT INTO #{ctx.table} (id, v) VALUES (1, 'committed-row')",
      "XA END #{xid}",
      "XA PREPARE #{xid}"
    ])

    # The prepared rows are NOT delivered while the XA dangles (the held-out window).
    refute_receive {:txn, _gtid, _changes, _pos}, 500

    MysqlCase.run!(ctx.qconn, "XA COMMIT #{xid}")

    assert {:txn, _gtid, changes, _pos} = collect_one()
    table = ctx.table
    assert [%{op: :insert, table: ^table, record: %{"v" => "committed-row"}}] = changes

    refute_receive {:txn, _g, _c, _p}, 500
    refute_receive {:DOWN, ^ref, :process, _pid, _reason}, 100
  end

  test "a prepared-then-rolled-back XA delivers ZERO rows (row-less advance)", ctx do
    sup = start_tracking_pipeline!(ctx)
    # Streaming barrier (a delivered plain row) before the XA begins.
    MysqlCase.run!(ctx.qconn, "INSERT INTO #{ctx.table} (id, v) VALUES (20, 'warm-up')")
    assert {:txn, _g0, _c0, _p0} = collect_one()

    ref = Process.monitor(MysqlCase.assembler_pid(sup))
    xid = "'mrz-#{System.unique_integer([:positive])}'"

    MysqlCase.run_all!(ctx.qconn, [
      "XA START #{xid}",
      "INSERT INTO #{ctx.table} (id, v) VALUES (2, 'rolled-back-row')",
      "XA END #{xid}",
      "XA PREPARE #{xid}"
    ])

    base = checkpoint_count(ctx)
    MysqlCase.run!(ctx.qconn, "XA ROLLBACK #{xid}")

    # A row-less resolution checkpoints WITHOUT calling the sink (Q14): the proof
    # surface is the durable checkpoint advancing past BOTH gtids (G_p ∪ G_c in ONE
    # write — count +2 in a single poll, never a delivery) and no halt.
    assert :ok = wait_checkpoint(ctx, base + 2)
    refute_receive {:txn, _g, _c, _p}, 500
    refute_receive {:DOWN, ^ref, :process, _pid, _reason}, 100
  end

  test "a pre-start dangling prepare resolves row-lessly, never halts (XA RECOVER pre-seed)",
       ctx do
    xid = "'mrz-#{System.unique_integer([:positive])}'"

    MysqlCase.run_all!(ctx.qconn, [
      "XA START #{xid}",
      "INSERT INTO #{ctx.table} (id, v) VALUES (3, 'pre-start-row')",
      "XA END #{xid}",
      "XA PREPARE #{xid}"
    ])

    # Seed the watermark AFTER the prepare: the dump SKIPS the prepare's events, so
    # only the resolution streams in — and must not be a :xa_commit_without_prepare halt.
    # Barrier on :established: the connect-time XA RECOVER must have enumerated the
    # dangling XID BEFORE the resolution commits (resolve-first would empty it).
    h = MysqlCase.attach_established_telemetry(self())
    sup = start_tracking_pipeline!(ctx)
    assert_receive :connection_established, 20_000
    :telemetry.detach(h)

    ref = Process.monitor(MysqlCase.assembler_pid(sup))
    base = checkpoint_count(ctx)
    MysqlCase.run!(ctx.qconn, "XA COMMIT #{xid}")

    # Row-less: the checkpoint advances over G_c, no delivery, no halt.
    assert :ok = wait_checkpoint(ctx, base + 1)
    refute_receive {:txn, _g, _c, _p}, 500
    refute_receive {:DOWN, ^ref, :process, _pid, _reason}, 100
  end

  test "the default :refuse posture halts fail-closed on an XA prepare", ctx do
    table = ctx.table
    watermark = MysqlCase.read_gtid_executed!(ctx.qconn)

    store_table = DurableStore.new_table()
    DurableStore.seed(store_table, table, watermark)

    {:ok, sup} =
      Capstan.start_link(
        connection: MysqlCase.pipeline_connection(),
        server_id: MysqlCase.unique_server_id(),
        sink: Sink,
        checkpoint_store: [module: DurableStore, options: [table: store_table, key: table]]
      )

    on_exit(fn -> MysqlCase.stop_pipeline(sup) end)
    ref = Process.monitor(MysqlCase.assembler_pid(sup))

    xid = "'mrz-#{System.unique_integer([:positive])}'"

    MysqlCase.run_all!(ctx.qconn, [
      "XA START #{xid}",
      "INSERT INTO #{ctx.table} (id, v) VALUES (4, 'refused-row')",
      "XA END #{xid}",
      "XA PREPARE #{xid}"
    ])

    assert_receive {:DOWN, ^ref, :process, _pid,
                    {:shutdown, {:halt, :unsupported_transaction_shape}}},
                   20_000

    MysqlCase.run!(ctx.qconn, "XA ROLLBACK #{xid}")
  end

  test "a prepared XA SERIALIZES against the snapshot brief lock (the Q10 primitive)", ctx do
    # Live-probed fact (1205 observed): LOCK TABLES T READ WAITS behind a prepared XA's
    # row locks on T and, with a bounded lock_wait_timeout, converts to a lock fault.
    # This is what keeps the cursor-gate safe during backfill: an XA's rows cannot be
    # chunk-captured OR suppressed-by-cursor while prepared — its k stays above the
    # cursor until resolution (ADR-0006 §6.7's mechanism, one level down).
    xid = "'mrz-#{System.unique_integer([:positive])}'"

    MysqlCase.run_all!(ctx.qconn, [
      "XA START #{xid}",
      "INSERT INTO #{ctx.table} (id, v) VALUES (5, 'locked-row')",
      "XA END #{xid}",
      "XA PREPARE #{xid}"
    ])

    {:ok, %{socket: sock}} =
      Handshake.connect(
        {:gen_tcp, raw_socket(ctx)},
        MysqlCase.pipeline_connection()
      )

    :ok = Command.query(sock, "SET SESSION lock_wait_timeout = 2")

    assert {:error, {:query_error, 1205}} =
             Command.query(sock, "LOCK TABLES #{ctx.table} READ")

    MysqlCase.run!(ctx.qconn, "XA ROLLBACK #{xid}")
    :gen_tcp.close(raw_socket(ctx))
  end

  ## ---------------------------------------------------------------------------
  ## helpers
  ## ---------------------------------------------------------------------------

  defp start_tracking_pipeline!(ctx) do
    table = ctx.table
    watermark = MysqlCase.read_gtid_executed!(ctx.qconn)

    store_table = DurableStore.new_table()
    DurableStore.seed(store_table, table, watermark)
    :persistent_term.put({__MODULE__, :store}, {store_table, table})

    {:ok, sup} =
      Capstan.start_link(
        connection: MysqlCase.pipeline_connection(),
        server_id: MysqlCase.unique_server_id(),
        sink: Sink,
        checkpoint_store: [module: DurableStore, options: [table: store_table, key: table]],
        xa: :track
      )

    on_exit(fn -> MysqlCase.stop_pipeline(sup) end)
    sup
  end

  defp raw_socket(_ctx) do
    {:ok, s} =
      :gen_tcp.connect(~c"127.0.0.1", MysqlCase.shared_port(), [:binary, active: false], 5000)

    s
  end

  defp checkpoint_count(_ctx) do
    {store_table, table} = :persistent_term.get({__MODULE__, :store})

    case DurableStore.current(store_table, table) do
      nil -> 0
      set -> MysqlCase.committed_count(set)
    end
  end

  defp wait_checkpoint(ctx, min_count, attempts \\ 200) do
    if checkpoint_count(ctx) >= min_count do
      :ok
    else
      if attempts == 0, do: flunk("checkpoint never reached #{min_count}")
      Process.sleep(50)
      wait_checkpoint(ctx, min_count, attempts - 1)
    end
  end

  defp collect_one do
    receive do
      {:txn, gtid, changes, pos} -> {:txn, gtid, changes, pos}
    after
      20_000 -> flunk("timed out awaiting the XA resolution delivery")
    end
  end
end

defmodule ReplicationPipeline.CheckpointStore do
  @moduledoc """
  Durable `Capstan.CheckpointStore` over the destination database: one row per
  pipeline in `capstan_checkpoint`. The write is an idempotent upsert; a query
  fault returns a value-free error and capstan applies its bounded
  retry-then-halt policy.
  """

  @behaviour Capstan.CheckpointStore

  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, Map.new(opts))
  end

  @impl Capstan.CheckpointStore
  def read(store), do: GenServer.call(store, :read)

  @impl Capstan.CheckpointStore
  def write(store, gtid_set) when is_binary(gtid_set),
    do: GenServer.call(store, {:write, gtid_set})

  @impl GenServer
  def init(state), do: {:ok, state}

  @impl GenServer
  def handle_call(:read, _from, %{pool: pool, pipeline_id: id} = state) do
    reply =
      case MyXQL.query(pool, "SELECT gtid_set FROM capstan_checkpoint WHERE pipeline_id = ?", [id]) do
        {:ok, %MyXQL.Result{rows: [[gtid_set]]}} -> {:ok, gtid_set}
        {:ok, %MyXQL.Result{rows: []}} -> {:ok, nil}
        {:error, _} -> {:error, :checkpoint_read_query_failed}
      end

    {:reply, reply, state}
  end

  def handle_call({:write, gtid_set}, _from, %{pool: pool, pipeline_id: id} = state) do
    sql = """
    INSERT INTO capstan_checkpoint (pipeline_id, gtid_set) VALUES (?, ?) AS new
    ON DUPLICATE KEY UPDATE gtid_set = new.gtid_set
    """

    reply =
      case MyXQL.query(pool, sql, [id, gtid_set]) do
        {:ok, _} -> :ok
        {:error, _} -> {:error, :checkpoint_write_query_failed}
      end

    {:reply, reply, state}
  end
end

# capstan examples

Runnable, minimal consumers that show how to integrate capstan end-to-end. They are **not** part of
the test suite — run them by hand against the dev substrate. (Exception:
`replication_pipeline/` IS wired into CI as the public sink API's canary.)

## `replication_pipeline/` — the durable reference stack (docker)

The complete production shape as one `docker compose up`: MySQL source (ROW
binlog + GTID) → capstan as an OTP release in one container → MySQL
destination, with a **value-free receipts ledger**, an **idempotent
upsert mirror**, and a **durable GTID-set checkpoint** in the destination
database. Restart the pipeline mid-life and watch resume-without-re-delivery.
See [replication_pipeline/README.md](replication_pipeline/README.md).

## `print_consumer.exs` — stream changes to stdout

A ~90-line consumer: a `Capstan.Sink` that prints every committed row change, a seedable
`Capstan.CheckpointStore`, and the `Capstan.start_link/1` wiring. It streams the
`capstan_example.demo` table and prints each insert/update/delete.

### Run it

1. **Bring up the dev substrate** (from the repo root):

   ```sh
   docker compose up -d --wait
   ```

2. **Create the demo table** on the source:

   ```sh
   docker compose exec mysql-cdc-probe mysql -uroot -pprobe -e "
     CREATE DATABASE IF NOT EXISTS capstan_example;
     CREATE TABLE IF NOT EXISTS capstan_example.demo (id INT PRIMARY KEY, name VARCHAR(50), qty INT) ENGINE=InnoDB;"
   ```

3. **Start the consumer, seeded to stream from *now*** (so it doesn't replay retained history):

   ```sh
   START_GTID=$(docker compose exec -T mysql-cdc-probe mysql -uroot -pprobe -N -e "SELECT @@global.gtid_executed" | tr -d '\n') \
     mix run examples/print_consumer.exs
   ```

   (Omit `START_GTID` to replay all retained history instead — capstan halts `:data_gap` if the
   server has already purged its earliest logs.)

4. **In another terminal, make some changes** and watch them stream:

   ```sh
   docker compose exec mysql-cdc-probe mysql -uroot -pprobe -e "
     INSERT INTO capstan_example.demo (id, name, qty) VALUES (1, 'widget', 10);
     UPDATE capstan_example.demo SET qty = 20 WHERE id = 1;
     DELETE FROM capstan_example.demo WHERE id = 1;"
   ```

   The consumer prints:

   ```
     insert capstan_example.demo: %{"id" => 1, "name" => "widget", "qty" => 10}
   [txn <uuid>:781] committed
     update capstan_example.demo: %{... "qty" => 10} -> %{... "qty" => 20}
   [txn <uuid>:782] committed
     delete capstan_example.demo: %{... "qty" => 20}
   [txn <uuid>:783] committed
   ```

   `Ctrl-C` twice to stop.

### Point it at your own source

The `connection:` block reads `SOURCE_HOST` / `SOURCE_PORT` / `SOURCE_USER` / `SOURCE_PASSWORD`
(defaulting to the dev substrate). For a real replica you also want:

- **TLS** — `ssl: true` with `cacertfile:` (and `server_name_indication: :disable` for MySQL's
  SAN-less auto-cert). See [usage-rules.md](../usage-rules.md) § TLS.
- **A durable checkpoint store** — the example's `ExampleStore` is process-lifetime only. Implement
  `read/2` + `write/2` over storage you already trust. See usage-rules.md § Checkpoint store.
- **A real sink** — apply each change as an idempotent UPSERT/DELETE keyed on the primary key, not a
  print. See usage-rules.md § The `Capstan.Sink` behaviour.

For production shape — supervision, a durable store over your own database, telemetry handling, and a
release healthcheck that surfaces a fail-closed halt — build on the patterns in usage-rules.md.

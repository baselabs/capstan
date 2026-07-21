defmodule Capstan.Sink do
  @moduledoc """
  The consumer behaviour: a capstan pipeline delivers committed transactions and
  DDL schema changes to a sink and, in sink-owned checkpoint mode, reads the durable
  processed position back through `c:checkpoint/0`.

  ## Callbacks are all optional; required-ness is a per-mode config concern

  Every callback here is an `@optional_callbacks` entry — the behaviour itself imposes
  **no** required set. Which callbacks a given sink MUST implement depends on the
  configured checkpoint mode; that per-mode requirement is validated where the pipeline
  starts up and checks its config (not by this behaviour, and not yet wired as of this
  module — see the pipeline's start-up validation):

    * **Sink-owned checkpoint mode** (no `:checkpoint_store`): the sink persists the
      position atomically with its own data write and hands it back via
      `c:checkpoint/0`; `c:checkpoint/0` and `c:handle_transaction/1` are required.
    * **Lib-owned checkpoint mode** (`:checkpoint_store` configured): the position is
      persisted by `Capstan.CheckpointStore`, so `c:checkpoint/0` is not consulted;
      `c:handle_transaction/1` is required.
    * `c:handle_schema_change/2` is required only when DDL delivery is enabled.

  Splitting required-ness out of the behaviour keeps a sink from being forced to stub
  a callback its mode never calls.

  ## Three load-bearing contracts

  These are not style notes; each guards a silent-loss class the type signature alone
  does not catch.

    * **`changes` is `Enumerable.t()`, not a list — a sink must never call `length/1`
      on it** (nor `Enum.count/1`, nor anything else that consumes it more than once).
      C1 delivers a list today, but the type is the day-one contract because C3's
      spill/batch work makes `changes` a lazy, single-pass, disk-backed enumerable
      valid only during the delivery call. A `length/1` habit passes every capstan
      test now and then silently consumes a replicant-style single-pass reader once
      C3 lands. Enumerate exactly once, streaming.
    * **Dedup uses `Capstan.Gtid.member?/2` — hand-rolling GTID-set membership is
      unsafe.** A committed GTID may fall inside a non-contiguous set (failover, purge,
      multi-source), so an ordinal comparison silently re-applies or skips. `Capstan.Gtid`
      is public contract precisely so no sink author writes interval arithmetic on the
      effect-once path; `member?/2` is the one correct membership check.
    * **The checkpoint is a PROCESSED watermark, not a delivery log.** It records
      every committed GTID the pipeline has processed — delivered *and* filtered
      (ADR-0003). It does not mean "delivered". Recording only delivered GTIDs would
      fragment the set unboundedly and stall progress forever during a fully-filtered
      quiet period; recording processed keeps the set a compact contiguous interval and
      gives `member?/2` one unambiguous meaning ("already processed").
  """

  @doc """
  Sink-owned checkpoint read (sink-owned mode only).

  Returns the durable processed position the sink persisted atomically with its last
  data write, or `nil` when the sink has never checkpointed. The pipeline resumes from
  this position; a value-free `{:error, term()}` fails closed rather than resume from a
  guessed position.
  """
  @callback checkpoint() :: {:ok, Capstan.Position.t() | nil} | {:error, term()}

  @doc """
  Deliver one committed transaction.

  Returns `{:ok, position}` — the processed position INCLUDING this transaction — once
  the sink has durably applied it, or a value-free `{:error, term()}` to halt the
  pipeline fail-closed without advancing the checkpoint. See the moduledoc: never call
  `length/1` on `txn.changes`, and dedup only via `Capstan.Gtid.member?/2`.
  """
  @callback handle_transaction(Capstan.Transaction.t()) ::
              {:ok, Capstan.Position.t()} | {:error, term()}

  @doc """
  Deliver one DDL schema change and the processed position that includes it.

  The `Capstan.SchemaChange` carries only structured `schema`/`table`/`kind` — the raw
  DDL statement text is redacted before it reaches the sink (Rule 1).
  Returns `:ok`, or a value-free `{:error, term()}` to halt fail-closed.
  """
  @callback handle_schema_change(Capstan.SchemaChange.t(), Capstan.Position.t()) ::
              :ok | {:error, term()}

  @optional_callbacks checkpoint: 0, handle_transaction: 1, handle_schema_change: 2
end

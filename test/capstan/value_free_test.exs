defmodule Capstan.ValueFreeTest do
  @moduledoc """
  Rule-1 (value-redaction) conformance across all four vectors (design § Rule 1 / F11).

  The two error-path vectors run in the default suite; the two live vectors (`@tag :live`,
  excluded by default) drive a real lib-owned pipeline against `mysql-cdc-probe` and are run
  with `mix test --only live`. `Capstan.ValueFree` is the adversarially non-vacuous helper —
  un-redacting any one vector makes its assertion go RED (proven by inspection during
  implementation, see the module doc).
  """
  use ExUnit.Case, async: false

  alias Capstan.ValueFree

  describe "Rule 1 — a planted sentinel reaches no log or telemetry payload" do
    test "ROW VALUE vector: a column value is scrubbed at the Error boundary" do
      assert :ok = ValueFree.assert_row_value_free()
    end

    test "PASSWORD vector: the connection password is scrubbed at the Error boundary" do
      assert :ok = ValueFree.assert_password_free()
    end
  end

  describe "Rule 1 — structural identity never widens an Inspect surface" do
    test "the Connection's tracked @@server_uuid (the ADR-0013 cycle reset) never renders" do
      # The reset's previous-cycle UUID is structural identity (as Capstan.Query already
      # treats it), not a row value — but the Connection struct's Inspect allowlist stays
      # exactly its pre-reset fields, so an incidental inspect never carries it.
      state =
        struct(Capstan.Connection,
          server_uuid: "3e11fa47-71ca-11e1-9e33-c80aa9429562",
          connection: [password: "never-rendered"]
        )

      refute inspect(state) =~ "3e11fa47"
      refute inspect(state) =~ "never-rendered"
    end
  end

  describe "Rule 1 — a planted sentinel survives no live pipeline output, log, or telemetry" do
    @describetag :live

    test "DDL-LITERAL vector: a DDL DEFAULT literal reaches no SchemaChange, log, or telemetry" do
      assert :ok = ValueFree.assert_ddl_literal_free()
    end

    test "ROWS_QUERY vector: the ROWS_QUERY SQL reaches no output, log, or telemetry" do
      assert :ok = ValueFree.assert_rows_query_free()
    end
  end
end

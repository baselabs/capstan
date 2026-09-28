defmodule Capstan.Integration.DisposableGuardTest do
  @moduledoc """
  The disposable server's identity guard (`Capstan.MysqlCase.guard_disposable!/3`), against the
  real disposable MySQL. A marked server whose `@@server_uuid` is one of the shared substrate's is
  refused before any statement that changes server state: the case of a shared server that carries
  the marker schema and is reached through another port. The unset-port, shared-port and
  unmarked-server refusals raise inside `with_disposable_mysql/2` and are exercised by pointing
  `CAPSTAN_DISPOSABLE_MYSQL_PORT` at such servers (docs/testing.md).
  """
  use ExUnit.Case, async: false

  alias Capstan.MysqlCase

  @moduletag :disposable_mysql

  test "a marked server that is a shared substrate server (same @@server_uuid) is refused" do
    port = MysqlCase.disposable_port!()
    admin = MysqlCase.socket!(Keyword.delete(MysqlCase.query_connection(port), :database))

    try do
      [[uuid]] = MysqlCase.query_rows!(admin, "SELECT @@server_uuid")

      assert_raise RuntimeError, ~r/is a shared substrate server/, fn ->
        MysqlCase.guard_disposable!(admin, port, [uuid])
      end

      # The contrast that keeps the refusal honest: the same server passes when no shared server
      # has its identity.
      assert :ok = MysqlCase.guard_disposable!(admin, port, [])
    after
      MysqlCase.close!(admin)
    end
  end
end

defmodule Capstan.Integration.AuroraMysqlMisconfigTest do
  @moduledoc """
  ADR-0013's gate-refusal scenario: a RE-PARAMETERIZED Aurora MySQL cluster carrying
  exactly ONE documented misconfiguration, named by `AURORA_MYSQL_MISCONFIG`.

  A separate module from `Capstan.Integration.AuroraMysqlTest` because the two need
  differently parameterized clusters: this one runs ALONE, against a cluster whose
  cluster parameter group was changed to carry exactly the named misconfiguration
  (followed by the writer reboot the AWS console requires). Run one scenario at a time
  and accumulate the receipt lines across runs.

  Excluded by default (`:aurora_mysql`); raises naming every missing variable — an
  unconfigured marquee is a loud failure, never a silent skip. **AUTHORED 2026-09-29,
  NOT EXECUTED** (no Aurora cluster exists for the repository yet — see ADR-0013's
  Evidence section).
  """

  use ExUnit.Case, async: false

  alias Capstan.MysqlCase
  alias Capstan.MysqlCase.{SeededStore, Sink}

  @moduletag :aurora_mysql
  @moduletag timeout: 300_000

  # The distinct refusal each documented misconfiguration must produce. `binlog_format_off`
  # and `log_bin_off` are the SAME Aurora condition (the group's OFF disables log_bin
  # while binlog_format reads ROW — ADR-0013's table row 1); both keys exist because they
  # name the two ways an operator produces it (leaving the group's default vs. setting it
  # OFF). STATEMENT and MIXED are the plain MySQL refusals, re-proven on the Aurora engine.
  @misconfig_reasons %{
    "binlog_format_off" => :binlog_disabled,
    "log_bin_off" => :binlog_disabled,
    "binlog_format_statement" => :binlog_format_not_row,
    "binlog_format_mixed" => :binlog_format_not_row,
    "binlog_row_metadata_minimal" => :binlog_row_metadata_not_full,
    "gtid_mode_off" => :gtid_mode_not_on
  }

  test "each documented misconfiguration refuses with its distinct reason" do
    misconfig = aurora_setting!(:misconfig)

    expected =
      case Map.fetch(@misconfig_reasons, misconfig) do
        {:ok, reason} ->
          reason

        :error ->
          raise ArgumentError,
                "AURORA_MYSQL_MISCONFIG must be one of #{inspect(Map.keys(@misconfig_reasons))}"
      end

    conn = MysqlCase.aurora_connection!()

    socket = aurora_socket!(conn)
    on_exit(fn -> MysqlCase.close!(socket) end)

    IO.puts(
      "[aurora-receipt] misconfiguration #{misconfig} variables " <>
        inspect(
          MysqlCase.query_rows!(
            socket,
            "SELECT @@global.binlog_format, @@global.log_bin, @@global.binlog_row_metadata, " <>
              "@@global.gtid_mode"
          )
        )
    )

    halts = MysqlCase.attach_halt_telemetry(self())
    on_exit(fn -> :telemetry.detach(halts) end)

    Sink.configure(%{pid: self()})
    on_exit(&Sink.clear/0)

    {:ok, sup} =
      Capstan.start_link(
        connection: conn,
        server_id: MysqlCase.unique_server_id(),
        sink: Sink,
        checkpoint_store: [module: SeededStore, options: [gtid_set: ""]],
        max_command_retries: 0
      )

    on_exit(fn -> MysqlCase.stop_pipeline(sup) end)

    assert_receive {:connection_halt, ^expected}, 60_000
    refute_receive {:txn, _, _, _}, 300
  end

  ## ---------------------------------------------------------------------------
  ## helpers (the same host-aware connect + loud setting read as the healthy module)
  ## ---------------------------------------------------------------------------

  defp aurora_socket!(conn), do: MysqlCase.connect_at!(conn) |> elem(0)

  defp aurora_setting!(key) do
    env_name = "AURORA_MYSQL_" <> (key |> Atom.to_string() |> String.upcase())

    case Application.get_env(:capstan, :aurora_substrate, [])[key] do
      value when is_binary(value) and value != "" ->
        value

      _ ->
        raise "capstan aurora_mysql_test: #{env_name} is required for this marquee " <>
                "(see .env.example); an unset prerequisite is an unconfigured marquee"
    end
  end
end

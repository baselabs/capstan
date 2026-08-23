defmodule Capstan.FixtureCapture do
  @moduledoc """
  Golden-fixture capture tool — connects to the live `mysql-cdc-probe` substrate,
  runs a pinned DDL/DML scenario, and writes each raw binlog event's bytes to
  `test/fixtures/binlog/<scenario>/<NN>-<event_type>.bin`.

  This is a capture TOOL, not an `ExUnit` test — Tasks 8–11 decode against the
  fixtures it produces, but nothing here asserts anything. Run it manually to
  (re)generate the fixtures, e.g.:

      MIX_ENV=test mix run -e "Capstan.FixtureCapture.capture_all()"

  The generating SQL for every scenario is documented in `test/fixtures/README.md`,
  which is also how a fixture gets regenerated after a scenario changes.

  ## Capture flow (per scenario)

  1. A QUERY connection runs the scenario's setup DDL, then reads
     `p0 = SELECT @@gtid_executed` — the checkpoint "already applied" before the
     scenario's own DML/DDL runs.
  2. The scenario's DML/DDL statement(s) run on the same connection.
  3. `p1 = SELECT @@gtid_executed` is read — `p1 − p0` is exactly the scenario's
     own GTIDs.
  4. A FRESH dump connection (unique `server_id`, 4000+ range) declares
     `@master_binlog_checksum` (required or the dump is refused error 1236) and
     issues `COM_BINLOG_DUMP_GTID` resuming from `p0` — the server streams only
     the scenario's own transactions.
  5. Every event is read via `Packet.read_packet/2`, the leading `0x00` OK marker
     is stripped, and the remaining bytes (19-byte header + body + 4-byte CRC)
     are written verbatim as the fixture. The dump's artificial `ROTATE` +
     `FORMAT_DESCRIPTION` + `PREVIOUS_GTIDS` preamble is captured too.
  6. Capture stops once every GTID in `p1 − p0` has been observed AND its
     terminator seen (`XID`, `QUERY("COMMIT")`, or a self-committing DDL
     `QUERY`) — bounded by an idle read timeout and a max-event cap so a
     misbehaving scenario cannot stream forever.

  Never restarts, stops, or duplicates the running `mysql-cdc-probe` container.
  """

  alias Capstan.Gtid
  alias Capstan.Protocol.{Command, Handshake, Packet}

  @host ~c"127.0.0.1"
  @connect_timeout 20_000
  @idle_timeout 15_000
  @max_events 500

  @connect_opts [
    ssl: false,
    auth_plugins: [:mysql_native_password],
    username: "root",
    password: "probe",
    database: "probe_db"
  ]

  @fixtures_dir [__DIR__, "..", "fixtures", "binlog"] |> Path.join() |> Path.expand()

  # Binlog event type byte -> fixture-name atom (a copy of probe:161's map, extended
  # with the sub-type names Tasks 8-11 name their fixtures with — see plan Task 7).
  @event_names %{
    2 => :query,
    4 => :rotate,
    15 => :format_description,
    16 => :xid,
    19 => :table_map,
    27 => :heartbeat,
    29 => :rows_query,
    30 => :write_rows,
    31 => :update_rows,
    32 => :delete_rows,
    33 => :gtid,
    34 => :anonymous_gtid,
    35 => :previous_gtids,
    36 => :transaction_context,
    37 => :view_change,
    38 => :xa_prepare,
    39 => :partial_update_rows,
    40 => :transaction_payload,
    41 => :heartbeat_v2
  }

  @typedoc "One pinned capture scenario."
  @type scenario :: %{
          name: String.t(),
          server_id: pos_integer(),
          setup: [String.t()],
          statements: [String.t()]
        }

  @doc """
  Captures every pinned scenario (Step 2 of Task 7) against the live substrate.
  """
  @spec capture_all() :: :ok
  def capture_all do
    Enum.each(scenarios(), &capture_scenario!/1)
    :ok
  end

  @doc """
  Captures a single scenario: runs its setup + DML/DDL on a query connection,
  then dumps the resulting events from a fresh, uniquely `server_id`-tagged dump
  connection, and writes them under `test/fixtures/binlog/<scenario.name>/`.
  """
  @spec capture_scenario!(scenario()) :: :ok
  def capture_scenario!(scenario) do
    IO.puts("[capture] #{scenario.name} — running setup + scenario SQL")
    query_socket = connect!()

    {checkpoint, expected} =
      try do
        Enum.each(scenario.setup, &run_query!(query_socket, &1))
        p0 = read_gtid_executed!(query_socket)
        Enum.each(scenario.statements, &run_query!(query_socket, &1))
        p1 = read_gtid_executed!(query_socket)
        {Gtid.parse(p0), Gtid.subtract(Gtid.parse(p1), Gtid.parse(p0))}
      after
        close!(query_socket)
      end

    if Gtid.sources(expected) == [] do
      raise "capstan fixture_capture: scenario #{scenario.name} produced no new GTIDs " <>
              "(p0 == p1) — the DML/DDL did not commit anything"
    end

    IO.puts("[capture] #{scenario.name} — dumping (server_id=#{scenario.server_id})")
    events = capture_dump!(scenario.server_id, checkpoint, expected)
    write_fixtures!(scenario.name, events)
    IO.puts("[capture] #{scenario.name} — wrote #{length(events)} event(s)")
    :ok
  end

  ## ---------------------------------------------------------------------------
  ## the pinned scenarios (Task 7 Step 2)
  ## ---------------------------------------------------------------------------

  @spec scenarios() :: [scenario()]
  defp scenarios do
    [
      %{
        name: "simple_dml",
        server_id: 4001,
        setup: [
          "DROP TABLE IF EXISTS widgets",
          "CREATE TABLE widgets (id INT PRIMARY KEY, name VARCHAR(50), qty INT) ENGINE=InnoDB"
        ],
        statements: [
          "INSERT INTO widgets (id, name, qty) VALUES (1, 'widget-one', 10)",
          "UPDATE widgets SET qty = 20 WHERE id = 1",
          "DELETE FROM widgets WHERE id = 1"
        ]
      },
      %{
        name: "multi_table",
        server_id: 4002,
        setup: [
          "DROP TABLE IF EXISTS ta",
          "DROP TABLE IF EXISTS tb",
          "CREATE TABLE ta (id INT PRIMARY KEY, val INT) ENGINE=InnoDB",
          "CREATE TABLE tb (id INT PRIMARY KEY, val INT) ENGINE=InnoDB",
          "INSERT INTO ta (id, val) VALUES (1, 100), (2, 200)",
          "INSERT INTO tb (id, val) VALUES (1, 1), (2, 2)"
        ],
        statements: [
          "UPDATE ta JOIN tb ON ta.id = tb.id SET ta.val = ta.val + tb.val, tb.val = tb.val + 1"
        ]
      },
      %{
        name: "alter_ddl",
        server_id: 4003,
        setup: [
          "DROP TABLE IF EXISTS widgets_ddl",
          "CREATE TABLE widgets_ddl (id INT PRIMARY KEY, name VARCHAR(50)) ENGINE=InnoDB"
        ],
        statements: [
          "ALTER TABLE widgets_ddl ADD COLUMN extra INT DEFAULT 7"
        ]
      },
      %{
        name: "all_types",
        server_id: 4004,
        setup: [
          "DROP TABLE IF EXISTS all_types",
          """
          CREATE TABLE all_types (
            id INT PRIMARY KEY,
            tiny_s TINYINT,
            tiny_u TINYINT UNSIGNED,
            small_s SMALLINT,
            small_u SMALLINT UNSIGNED,
            medium_s MEDIUMINT,
            medium_u MEDIUMINT UNSIGNED,
            int_s INT,
            int_u INT UNSIGNED,
            big_s BIGINT,
            big_u BIGINT UNSIGNED,
            dec_col DECIMAL(10,2),
            varchar_col VARCHAR(50),
            char_col CHAR(10),
            text_col TEXT,
            blob_col BLOB,
            datetime_col DATETIME,
            timestamp_col TIMESTAMP NULL,
            date_col DATE,
            time_col TIME,
            enum_col ENUM('small', 'medium', 'large')
          ) ENGINE=InnoDB
          """
        ],
        statements: [
          """
          INSERT INTO all_types (
            id, tiny_s, tiny_u, small_s, small_u, medium_s, medium_u, int_s, int_u,
            big_s, big_u, dec_col, varchar_col, char_col, text_col, blob_col,
            datetime_col, timestamp_col, date_col, time_col, enum_col
          ) VALUES (
            1, -128, 255, -32768, 65535, -8388608, 16777215, -2147483648, 4294967295,
            -9223372036854775808, 18446744073709551615, 12345.67, 'varchar-value',
            'char-val', 'text value here', 'blob-bytes', '2024-01-15 10:30:00',
            '2024-01-15 10:30:00', '2024-01-15', '10:30:00', 'medium'
          )
          """
        ]
      },
      %{
        name: "rows_query",
        server_id: 4005,
        setup: [
          "DROP TABLE IF EXISTS rq_widgets",
          "CREATE TABLE rq_widgets (id INT PRIMARY KEY, name VARCHAR(50)) ENGINE=InnoDB",
          "SET SESSION binlog_rows_query_log_events = ON"
        ],
        statements: [
          "INSERT INTO rq_widgets (id, name) VALUES (1, 'rq-one')"
        ]
      },
      %{
        name: "myisam",
        server_id: 4006,
        setup: [
          "DROP TABLE IF EXISTS myisam_widgets",
          "CREATE TABLE myisam_widgets (id INT PRIMARY KEY, name VARCHAR(50)) ENGINE=MyISAM"
        ],
        statements: [
          "INSERT INTO myisam_widgets (id, name) VALUES (1, 'myisam-one')"
        ]
      },
      %{
        name: "json_col",
        server_id: 4007,
        setup: [
          "DROP TABLE IF EXISTS json_widgets",
          "CREATE TABLE json_widgets (id INT PRIMARY KEY, doc JSON) ENGINE=InnoDB"
        ],
        statements: [
          "INSERT INTO json_widgets (id, doc) VALUES " <>
            "(1, '{\"a\":1,\"b\":[true,false,null],\"c\":\"text\"}')"
        ]
      },
      # SET and ENUM both wire as MYSQL_TYPE_STRING (254); C1 must unpack the STRING
      # meta pair to DETECT a SET column and halt `:unsupported_column_type` (Task 11 /
      # design F3) — the `all_types` scenario has an ENUM but no SET, so this scenario
      # supplies the real captured SET bytes the SET-detection tripwire must go RED
      # against (self-signed/synthetic meta would only prove roundtrip, not conformance).
      %{
        name: "set_type",
        server_id: 4008,
        setup: [
          "DROP TABLE IF EXISTS set_probe",
          "CREATE TABLE set_probe (id INT PRIMARY KEY, flags SET('a', 'b', 'c')) ENGINE=InnoDB"
        ],
        statements: [
          "INSERT INTO set_probe (id, flags) VALUES (1, 'a,c')"
        ]
      },
      # Temporal widths (DATETIME2/TIME2/TIMESTAMP2) carry fractional-second precision
      # in the column meta, not the type byte. `all_types` uses fsp 0 (zero fractional
      # bytes), which cannot catch a decoder that ignores the meta and assumes a fixed
      # width. Non-zero fsp columns make Task 11's meta-driven temporal decode
      # non-vacuous.
      %{
        name: "frac_temporal",
        server_id: 4009,
        setup: [
          "DROP TABLE IF EXISTS frac_probe",
          """
          CREATE TABLE frac_probe (
            id INT PRIMARY KEY,
            dt DATETIME(3),
            tm TIME(6),
            ts TIMESTAMP(6) NULL
          ) ENGINE=InnoDB
          """
        ],
        statements: [
          "INSERT INTO frac_probe (id, dt, tm, ts) VALUES " <>
            "(1, '2024-01-15 10:30:00.123', '10:30:00.123456', '2024-01-15 10:30:00.654321')"
        ]
      }
    ]
  end

  @doc """
  Regenerates only the named scenarios, leaving every other fixture directory
  untouched. Use this when a scenario is added or changed and re-running the whole
  `capture_all/0` would needlessly churn the GTID bytes of unrelated fixtures (which
  breaks the tests that assert against their exact captured values).

      MIX_ENV=test mix run -e 'Capstan.FixtureCapture.capture_only!(["set_type"])'
  """
  @spec capture_only!([String.t()]) :: :ok
  def capture_only!(names) do
    wanted = MapSet.new(names)

    known = MapSet.new(scenarios(), & &1.name)
    unknown = MapSet.difference(wanted, known)

    unless MapSet.size(unknown) == 0 do
      raise "capstan fixture_capture: unknown scenario(s) #{inspect(MapSet.to_list(unknown))}"
    end

    scenarios()
    |> Enum.filter(&MapSet.member?(wanted, &1.name))
    |> Enum.each(&capture_scenario!/1)

    :ok
  end

  ## ---------------------------------------------------------------------------
  ## query connection
  ## ---------------------------------------------------------------------------

  defp run_query!(socket, sql) do
    case Command.query(socket, sql) do
      :ok -> :ok
      {:ok, _rows} -> :ok
      {:error, reason} -> raise "capstan fixture_capture: query failed #{inspect(reason)}: #{sql}"
    end
  end

  defp read_gtid_executed!(socket) do
    case Command.query(socket, "SELECT @@gtid_executed") do
      {:ok, [[value]]} when is_binary(value) ->
        value

      other ->
        raise "capstan fixture_capture: unexpected @@gtid_executed response #{inspect(other)}"
    end
  end

  ## ---------------------------------------------------------------------------
  ## dump connection + event capture
  ## ---------------------------------------------------------------------------

  defp capture_dump!(server_id, checkpoint, expected) do
    socket = connect!()

    try do
      run_query!(socket, "SET @master_binlog_checksum = @@global.binlog_checksum")
      dump = Command.com_binlog_dump_gtid(server_id, checkpoint)
      :ok = Packet.send_packet(socket, dump, 0)
      read_dump_events(socket, %{remaining: expected, current: nil, begun: false}, 1, [])
    after
      close!(socket)
    end
  end

  defp read_dump_events(_socket, state, seq, _acc) when seq > @max_events do
    raise "capstan fixture_capture: exceeded max event cap (#{@max_events}) with " <>
            "#{inspect(Gtid.sources(state.remaining))} GTID(s) still unresolved"
  end

  defp read_dump_events(socket, state, seq, acc) do
    case Packet.read_packet(socket, @idle_timeout) do
      {_pkt_seq, <<0x00, event::binary>>} ->
        {name, state} = classify(event, state)
        acc = [{seq, name, event} | acc]

        if Gtid.sources(state.remaining) == [] do
          Enum.reverse(acc)
        else
          read_dump_events(socket, state, seq + 1, acc)
        end

      {_pkt_seq, <<0xFE, _rest::binary>>} ->
        raise "capstan fixture_capture: stream EOF with " <>
                "#{inspect(Gtid.sources(state.remaining))} GTID(s) still unresolved"

      {_pkt_seq, <<0xFF, code::16-little, rest::binary>>} ->
        raise "capstan fixture_capture: dump error #{code}: #{inspect(rest)}"
    end
  end

  defp classify(event, state) do
    <<_ts::32-little, type::8, _server_id::32-little, _size::32-little, _log_pos::32-little,
      _flags::16-little, body_ck::binary>> = event

    name = Map.get(@event_names, type, :"type_#{type}")
    body_len = byte_size(body_ck) - 4
    <<body::binary-size(^body_len), _crc::32-little>> = body_ck
    {name, apply_event(name, body, state)}
  end

  defp apply_event(:gtid, body, state) do
    <<_flags::8, sid::binary-size(16), gno::64-little-signed, _rest::binary>> = body
    %{state | current: {format_uuid(sid), gno}, begun: false}
  end

  defp apply_event(:query, body, state) do
    case query_text(body) do
      "BEGIN" -> %{state | begun: true}
      "COMMIT" -> close_current(state)
      _other when state.begun == false -> close_current(state)
      _other -> state
    end
  end

  defp apply_event(:xid, _body, state), do: close_current(state)
  defp apply_event(_other, _body, state), do: state

  defp close_current(%{current: nil} = state), do: state

  defp close_current(%{current: {uuid, gno}} = state) do
    txn = Gtid.parse("#{uuid}:#{gno}")
    %{state | remaining: Gtid.subtract(state.remaining, txn), current: nil, begun: false}
  end

  defp query_text(body) do
    <<_thread_id::32-little, _exec_time::32-little, schema_len::8, _err_code::16-little,
      status_vars_len::16-little, rest::binary>> = body

    <<_status_vars::binary-size(^status_vars_len), _schema::binary-size(^schema_len), 0,
      sql::binary>> = rest

    String.trim(sql)
  end

  defp format_uuid(
         <<a::binary-size(4), b::binary-size(2), c::binary-size(2), d::binary-size(2),
           e::binary-size(6)>>
       ) do
    Enum.map_join([a, b, c, d, e], "-", &Base.encode16(&1, case: :lower))
  end

  ## ---------------------------------------------------------------------------
  ## fixture writing
  ## ---------------------------------------------------------------------------

  defp write_fixtures!(scenario_name, events) do
    dir = Path.join(@fixtures_dir, scenario_name)
    File.rm_rf!(dir)
    File.mkdir_p!(dir)

    Enum.each(events, fn {seq, name, event} ->
      dir |> Path.join("#{pad(seq)}-#{name}.bin") |> File.write!(event)
    end)
  end

  defp pad(seq), do: seq |> Integer.to_string() |> String.pad_leading(2, "0")

  ## ---------------------------------------------------------------------------
  ## connection lifecycle
  ## ---------------------------------------------------------------------------

  defp connect! do
    {:ok, raw} =
      :gen_tcp.connect(
        @host,
        Capstan.MysqlCase.shared_port(),
        [:binary, active: false],
        @connect_timeout
      )

    case Handshake.connect({:gen_tcp, raw}, @connect_opts) do
      {:ok, %{socket: socket}} -> socket
      {:error, reason} -> raise "capstan fixture_capture: handshake failed #{inspect(reason)}"
    end
  end

  defp close!({:gen_tcp, sock}), do: :gen_tcp.close(sock)
  defp close!({:ssl, sock}), do: :ssl.close(sock)
end

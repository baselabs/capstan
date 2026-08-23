defmodule Capstan.Xa.IdTest do
  @moduledoc """
  The XID identity conformance — both encodings the pipeline sees must produce the
  SAME digest, or a resolution could never match its pooled prepare.
  """
  use ExUnit.Case, async: true

  alias Capstan.Binlog.{Decoder, Event}
  alias Capstan.Xa.Id

  test "the REAL captured session's type-38 XID and its XA COMMIT text digest identically" do
    {:ok, prepare_event} = "10-xa_prepare.bin" |> fixture() |> Event.parse()
    {:ok, {:xa_prepare, xid}} = Decoder.decode(prepare_event)

    {:ok, commit_event} = "12-query.bin" |> fixture() |> Event.parse()
    {:ok, {:query, %{sql: sql}}} = Decoder.decode(commit_event)

    assert {:ok, {:commit, digest_from_event}} = Id.parse_resolution(sql)
    assert digest_from_event == Id.digest(xid)
  end

  test "parse_resolution accepts the canonical hex-literal forms and refuses everything else" do
    assert {:ok, {:commit, _}} = Id.parse_resolution("XA COMMIT X'0a',X'',1")
    assert {:ok, {:rollback, _}} = Id.parse_resolution("XA ROLLBACK X'0a',X'0b',42")
    assert {:ok, {:commit, _}} = Id.parse_resolution("XA COMMIT X'0a',X'0b',42 ONE PHASE")

    assert :error = Id.parse_resolution("XA COMMIT")
    assert :error = Id.parse_resolution("XA COMMIT 'plain-string','b',1")
    assert :error = Id.parse_resolution("XA COMMIT X'abc',X'',1")
    assert :error = Id.parse_resolution("XA RECOVER")
    assert :error = Id.parse_resolution("DROP TABLE t")
  end

  test "hex decoding is byte-exact (uppercase and lowercase literals agree)" do
    assert {:ok, {:commit, d1}} = Id.parse_resolution("XA COMMIT X'414243',X'',1")
    assert {:ok, {:commit, d2}} = Id.parse_resolution("XA COMMIT X'616263',X'',1")

    xid_upper = %{one_phase: false, format_id: 1, gtrid: "ABC", bqual: ""}
    assert d1 == Id.digest(xid_upper)
    # 'abc' (0x616263) is different bytes than 'ABC' (0x414243) — different digests.
    refute d1 == d2
  end

  defp fixture(name), do: File.read!("test/fixtures/binlog/xa/#{name}")
end

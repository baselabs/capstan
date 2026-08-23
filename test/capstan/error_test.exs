defmodule Capstan.ErrorTest do
  @moduledoc """
  The value-free boundary contract of `Capstan.Error` (Rule 1): `from/1` keeps
  only an atom `reason` and an `inspect(module)` `shape`, and the derived
  `message/1` renders exactly those — no exception message, tuple payload, or
  raw term ever survives the boundary.
  """

  use ExUnit.Case, async: true

  alias Capstan.Error

  test "an existing Capstan.Error passes through from/1 unchanged" do
    error = %Error{reason: :snapshot_chunk_read_failed, shape: nil}
    assert Error.from(error) == error

    shaped = %Error{reason: :unknown, shape: "MatchError"}
    assert Error.from(shaped) == shaped
  end

  test "message/1 renders only the structural fields, in the stable order" do
    assert Error.message(%Error{reason: :config_invalid, shape: nil}) ==
             "capstan error reason=config_invalid"

    assert Error.message(%Error{reason: :unknown, shape: "RuntimeError"}) ==
             "capstan error reason=unknown shape=RuntimeError"
  end
end

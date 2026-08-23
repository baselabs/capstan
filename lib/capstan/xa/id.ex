defmodule Capstan.Xa.Id do
  @moduledoc """
  The value-free identity of an XA transaction (ADR-0006, Rule 1).

  `gtrid`/`bqual` are application-chosen bytes — row-value class — so the pool keys on
  a **sha256 digest** of the canonical XID frame, and the raw bytes are never stored,
  logged, or emitted. Even the digest is never emitted (a digest of a low-entropy
  application XID is dictionary-reversible); it exists as the in-memory pool key ONLY.
  Telemetry and errors correlate on server-assigned GTIDs, never on anything
  XID-derived.

  Both XID encodings capstan sees produce the same digest:

    * the `XA_prepare_log_event` (type 38) body — `one_phase` byte, `format_id` /
      `gtrid_length` / `bqual_length` as little-endian u32s, then the raw bytes
      (layout read from the MySQL server source, `control_events.cpp`);
    * the resolution `QUERY` text — `XA COMMIT X'<hex gtrid>',X'<hex bqual>',<format_id>`
      (and `XA ROLLBACK`), the canonical hex-literal form the server writes (captured
      live; `test/fixtures/binlog/xa/`).
  """

  @typedoc "A parsed XID's value-free identity: the sha256 digest of the canonical frame."
  @type t :: <<_::256>>

  @type xid :: %{
          required(:one_phase) => boolean(),
          required(:format_id) => non_neg_integer(),
          required(:gtrid) => binary(),
          required(:bqual) => binary()
        }

  @typedoc "A parsed resolution statement: the verb and its XID digest."
  @type resolution :: {:commit | :rollback, t()}

  @frame_prefix "capstan.xa.v1"

  @doc """
  The digest of a canonical XID. The frame is namespaced and length-prefixed so no
  two distinct `{format_id, gtrid, bqual}` triples collide.
  """
  @spec digest(xid()) :: t()
  def digest(%{format_id: format_id, gtrid: gtrid, bqual: bqual})
      when is_integer(format_id) and is_binary(gtrid) and is_binary(bqual) do
    :crypto.hash(
      :sha256,
      @frame_prefix <>
        <<format_id::32-little>> <>
        <<byte_size(gtrid)::32-little>> <>
        gtrid <>
        <<byte_size(bqual)::32-little>> <> bqual
    )
  end

  @doc """
  Parses an `XA COMMIT`/`XA ROLLBACK` resolution `QUERY` text into its verb + digest.

  Accepts exactly the canonical hex-literal form the server writes
  (`XA COMMIT X'…',X'…',<format_id>`); a bare `XA COMMIT`-shaped text with no XID
  (or any other shape) returns `:error` — the caller treats an unparseable
  resolution as unmatched and fails closed, never guesses.
  """
  @spec parse_resolution(String.t()) :: {:ok, resolution()} | :error
  def parse_resolution(sql) when is_binary(sql) do
    with "XA " <> rest <- String.trim(sql),
         [verb, xid_text] <- String.split(rest, " ", parts: 2),
         {:ok, verb} <- verb(verb),
         {:ok, xid} <- parse_hex_xid(String.trim(xid_text)) do
      {:ok, {verb, digest(xid)}}
    else
      _ -> :error
    end
  end

  defp verb("COMMIT"), do: {:ok, :commit}
  defp verb("ROLLBACK"), do: {:ok, :rollback}
  defp verb(_), do: :error

  # X'<hex>',X'<hex>',<format_id> — the trailing ` ONE PHASE` variant rides the
  # prepare event's flag, but a client-side one-phase text still names the XID the
  # same way, so tolerate it.
  defp parse_hex_xid(text) do
    text = text |> String.trim() |> String.replace_suffix("ONE PHASE", "") |> String.trim()

    with [gtrid_hex, bqual_hex, format_str] <- String.split(text, ",", parts: 3),
         {:ok, gtrid} <- hex_literal(gtrid_hex),
         {:ok, bqual} <- hex_literal(bqual_hex),
         {format_id, ""} <- Integer.parse(String.trim(format_str)) do
      {:ok, %{one_phase: false, format_id: format_id, gtrid: gtrid, bqual: bqual}}
    else
      _ -> :error
    end
  end

  defp hex_literal("X'" <> hex_quoted) do
    case String.split(hex_quoted, "'", parts: 2) do
      [hex, ""] ->
        # Even-length hex digits only: an odd nibble count is not a byte string.
        if rem(byte_size(hex), 2) == 0 and valid_hex?(hex) do
          {:ok, hex |> String.downcase() |> hex_to_bytes()}
        else
          :error
        end

      _ ->
        :error
    end
  end

  defp hex_literal(_), do: :error

  defp valid_hex?(hex), do: match?({<<_::binary>>, ""}, valid_hex_bytes(hex))

  defp valid_hex_bytes(<<c::8, rest::binary>>)
       when c in ?0..?9 or c in ?a..?f or c in ?A..?F,
       do: valid_hex_bytes(rest)

  defp valid_hex_bytes(rest), do: {<<>>, rest}

  defp hex_to_bytes(hex) do
    for <<a::8, b::8 <- hex>>, into: <<>>, do: <<nibble(a)::4, nibble(b)::4>>
  end

  defp nibble(c) when c in ?0..?9, do: c - ?0
  defp nibble(c) when c in ?a..?f, do: c - ?a + 10
end

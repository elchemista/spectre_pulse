defmodule Spectre.Pulse.Codec.JSON do
  @moduledoc """
  Interoperable Pulse v1 JSON codec.

  Decoding uses string keys and the controlled act vocabulary; it never creates
  atoms from remote input.
  """

  @behaviour Spectre.Pulse.Codec

  alias Spectre.Pulse.Envelope
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Protocol

  @doc false
  @spec encode(Envelope.t(), keyword()) :: {:ok, binary()} | {:error, Error.t()}
  @impl Spectre.Pulse.Codec
  def encode(%Envelope{} = envelope, opts) do
    max_bytes = limit(opts)

    case Jason.encode(Envelope.to_wire(envelope)) do
      {:ok, encoded} when byte_size(encoded) <= max_bytes ->
        {:ok, encoded}

      {:ok, encoded} ->
        {:error,
         Error.not_sent(
           :codec,
           {:envelope_too_large, byte_size(encoded), max_bytes},
           message_id: envelope.id
         )}

      {:error, reason} ->
        {:error, Error.not_sent(:codec, {:json_encode_failed, reason}, message_id: envelope.id)}
    end
  end

  @doc false
  @spec decode(term(), keyword()) :: {:ok, Envelope.t()} | {:error, Error.t()}
  @impl Spectre.Pulse.Codec
  def decode(encoded, opts) when is_binary(encoded) do
    max_bytes = limit(opts)

    if byte_size(encoded) > max_bytes do
      {:error, Error.not_sent(:codec, {:envelope_too_large, byte_size(encoded), max_bytes})}
    else
      case Jason.decode(encoded) do
        {:ok, map} when is_map(map) -> Envelope.new(map, opts)
        {:ok, value} -> {:error, Error.not_sent(:codec, {:json_envelope_not_object, value})}
        {:error, reason} -> {:error, Error.not_sent(:codec, {:json_decode_failed, reason})}
      end
    end
  end

  def decode(value, _opts),
    do: {:error, Error.not_sent(:codec, {:json_codec_expected_binary, value})}

  @spec limit(keyword()) :: pos_integer()
  defp limit(opts) do
    Keyword.get(opts, :max_envelope_bytes, Protocol.default_limits().max_envelope_bytes)
  end
end

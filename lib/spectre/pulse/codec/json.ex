defmodule Spectre.Pulse.Codec.JSON do
  @moduledoc """
  Interoperable Pulse v1 JSON codec.

  Decoding uses string keys and the controlled act vocabulary; it never creates
  atoms from remote input.
  """

  @behaviour Spectre.Pulse.Codec

  alias Spectre.Pulse.Envelope
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Options
  alias Spectre.Pulse.Protocol

  @doc false
  @spec encode(Envelope.t(), keyword()) :: {:ok, binary()} | {:error, Error.t()}
  @impl Spectre.Pulse.Codec
  def encode(%Envelope{} = envelope, opts) do
    with {:ok, opts} <- Options.keyword(opts),
         {:ok, max_bytes} <- envelope_limit(opts),
         {:ok, envelope} <- Envelope.new(envelope, opts) do
      case encode_json(Envelope.to_wire(envelope)) do
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
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, Error.not_sent(:codec, reason, message_id: envelope.id)}
    end
  end

  @doc false
  @spec decode(term(), keyword()) :: {:ok, Envelope.t()} | {:error, Error.t()}
  @impl Spectre.Pulse.Codec
  def decode(encoded, opts) when is_binary(encoded) do
    with {:ok, opts} <- Options.keyword(opts),
         {:ok, max_bytes} <- envelope_limit(opts) do
      decode_envelope(encoded, max_bytes, opts)
    else
      {:error, reason} -> {:error, Error.not_sent(:codec, reason)}
    end
  end

  def decode(value, _opts),
    do: {:error, Error.not_sent(:codec, {:json_codec_expected_binary, value})}

  @spec decode_envelope(binary(), pos_integer(), keyword()) ::
          {:ok, Envelope.t()} | {:error, Error.t()}
  defp decode_envelope(encoded, max_bytes, _opts) when byte_size(encoded) > max_bytes,
    do: {:error, Error.not_sent(:codec, {:envelope_too_large, byte_size(encoded), max_bytes})}

  defp decode_envelope(encoded, _max_bytes, opts) do
    case decode_json(encoded) do
      {:ok, map} when is_map(map) -> Envelope.new(map, opts)
      {:ok, value} -> {:error, Error.not_sent(:codec, {:json_envelope_not_object, value})}
      {:error, reason} -> {:error, Error.not_sent(:codec, {:json_decode_failed, reason})}
    end
  end

  @spec envelope_limit(keyword()) :: {:ok, pos_integer()} | {:error, term()}
  defp envelope_limit(opts) do
    Options.positive_integer(
      opts,
      :max_envelope_bytes,
      Protocol.default_limits().max_envelope_bytes
    )
  end

  @spec encode_json(term()) :: {:ok, binary()} | {:error, term()}
  defp encode_json(value) do
    Jason.encode(value)
  rescue
    exception -> {:error, exception}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  @spec decode_json(binary()) :: {:ok, term()} | {:error, term()}
  defp decode_json(value) do
    Jason.decode(value)
  rescue
    exception -> {:error, exception}
  catch
    kind, reason -> {:error, {kind, reason}}
  end
end

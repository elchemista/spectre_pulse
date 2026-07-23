defmodule Spectre.Pulse.Codec.Identity do
  @moduledoc """
  Zero-copy codec for trusted in-VM delivery of an envelope struct.
  """

  @behaviour Spectre.Pulse.Codec

  alias Spectre.Pulse.Envelope
  alias Spectre.Pulse.Error

  @doc false
  @spec encode(Envelope.t(), keyword()) :: {:ok, Envelope.t()}
  @impl Spectre.Pulse.Codec
  def encode(%Envelope{} = envelope, _opts), do: {:ok, envelope}

  @doc false
  @spec decode(term(), keyword()) :: {:ok, Envelope.t()} | {:error, Error.t()}
  @impl Spectre.Pulse.Codec
  def decode(%Envelope{} = envelope, opts), do: Envelope.new(envelope, opts)

  def decode(value, _opts),
    do: {:error, Error.not_sent(:codec, {:identity_codec_expected_envelope, value})}
end

defmodule Spectre.Pulse.Codec do
  @moduledoc """
  Encoding boundary for the transport-independent envelope.

  A codec changes representation, never protocol semantics.
  """

  alias Spectre.Pulse.Envelope
  alias Spectre.Pulse.Error

  @type encoded :: iodata() | binary() | Envelope.t()

  @callback encode(Envelope.t(), keyword()) :: {:ok, encoded()} | {:error, Error.t() | term()}
  @callback decode(encoded(), keyword()) :: {:ok, Envelope.t()} | {:error, Error.t() | term()}

  @doc "Calls an encoder module with a normalized envelope."
  @spec encode(module(), Envelope.t(), keyword()) :: {:ok, encoded()} | {:error, Error.t()}
  def encode(codec, %Envelope{} = envelope, opts \\ []) when is_atom(codec) do
    with {:module, _module} <- Code.ensure_loaded(codec),
         true <- function_exported?(codec, :encode, 2),
         result <- codec.encode(envelope, opts) do
      normalize_result(result, envelope.id)
    else
      false -> {:error, Error.not_sent(:codec, {:invalid_codec, codec}, message_id: envelope.id)}
      {:error, reason} -> {:error, Error.not_sent(:codec, reason, message_id: envelope.id)}
    end
  end

  @doc "Calls a decoder module."
  @spec decode(module(), encoded(), keyword()) :: {:ok, Envelope.t()} | {:error, Error.t()}
  def decode(codec, encoded, opts \\ []) when is_atom(codec) do
    with {:module, _module} <- Code.ensure_loaded(codec),
         true <- function_exported?(codec, :decode, 2),
         result <- codec.decode(encoded, opts) do
      normalize_result(result, nil)
    else
      false -> {:error, Error.not_sent(:codec, {:invalid_codec, codec})}
      {:error, reason} -> {:error, Error.not_sent(:codec, reason)}
    end
  end

  @spec normalize_result(term(), String.t() | nil) :: {:ok, term()} | {:error, Error.t()}
  defp normalize_result({:ok, value}, _message_id), do: {:ok, value}
  defp normalize_result({:error, %Error{} = error}, _message_id), do: {:error, error}

  defp normalize_result({:error, reason}, message_id),
    do: {:error, Error.not_sent(:codec, reason, message_id: message_id)}

  defp normalize_result(other, message_id),
    do: {:error, Error.not_sent(:codec, {:invalid_codec_result, other}, message_id: message_id)}
end

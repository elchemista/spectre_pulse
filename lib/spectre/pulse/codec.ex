defmodule Spectre.Pulse.Codec do
  @moduledoc """
  Encoding boundary for the transport-independent envelope.

  A codec changes representation, never protocol semantics.
  """

  alias Spectre.Pulse.Envelope
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Options

  @type encoded :: iodata() | binary() | Envelope.t()

  @callback encode(Envelope.t(), keyword()) :: {:ok, encoded()} | {:error, Error.t() | term()}
  @callback decode(encoded(), keyword()) :: {:ok, Envelope.t()} | {:error, Error.t() | term()}

  @doc "Calls an encoder module with a normalized envelope."
  @spec encode(module(), Envelope.t(), keyword()) :: {:ok, encoded()} | {:error, Error.t()}
  def encode(codec, envelope, opts \\ [])

  def encode(codec, %Envelope{} = envelope, opts) when is_atom(codec) and not is_nil(codec) do
    with {:ok, opts} <- Options.keyword(opts),
         {:ok, envelope} <- Envelope.new(envelope, opts),
         {:module, _module} <- Code.ensure_loaded(codec),
         true <- function_exported?(codec, :encode, 2),
         result <- protected_call(codec, :encode, [envelope, opts], envelope.id) do
      normalize_encode_result(result, envelope.id, opts)
    else
      {:error, %Error{} = error} -> {:error, error}
      false -> {:error, Error.not_sent(:codec, {:invalid_codec, codec}, message_id: envelope.id)}
      {:error, reason} -> {:error, Error.not_sent(:codec, reason, message_id: envelope.id)}
    end
  end

  def encode(codec, envelope, _opts),
    do:
      {:error,
       Error.not_sent(:codec, {:invalid_codec_input, codec, envelope},
         message_id: message_id(envelope)
       )}

  @doc "Calls a decoder module."
  @spec decode(module(), encoded(), keyword()) :: {:ok, Envelope.t()} | {:error, Error.t()}
  def decode(codec, encoded, opts \\ [])

  def decode(codec, encoded, opts) when is_atom(codec) and not is_nil(codec) do
    with {:ok, opts} <- Options.keyword(opts),
         {:module, _module} <- Code.ensure_loaded(codec),
         true <- function_exported?(codec, :decode, 2),
         result <- protected_call(codec, :decode, [encoded, opts], nil) do
      normalize_decode_result(result, opts)
    else
      false -> {:error, Error.not_sent(:codec, {:invalid_codec, codec})}
      {:error, reason} -> {:error, Error.not_sent(:codec, reason)}
    end
  end

  def decode(codec, _encoded, _opts),
    do: {:error, Error.not_sent(:codec, {:invalid_codec, codec})}

  @spec normalize_encode_result(term(), String.t(), keyword()) ::
          {:ok, encoded()} | {:error, Error.t()}
  defp normalize_encode_result({:ok, value}, _message_id, _opts) when is_binary(value),
    do: {:ok, value}

  defp normalize_encode_result({:ok, %Envelope{} = value}, _message_id, opts),
    do: Envelope.new(value, opts)

  defp normalize_encode_result({:ok, value}, message_id, _opts) when is_list(value) do
    _length = IO.iodata_length(value)
    {:ok, value}
  rescue
    _exception -> invalid_encoded(value, message_id)
  end

  defp normalize_encode_result({:ok, value}, message_id, _opts),
    do: invalid_encoded(value, message_id)

  defp normalize_encode_result({:error, %Error{} = error}, message_id, _opts),
    do: {:error, put_message_context(error, message_id)}

  defp normalize_encode_result({:error, reason}, message_id, _opts),
    do: {:error, Error.not_sent(:codec, reason, message_id: message_id)}

  defp normalize_encode_result(other, message_id, _opts),
    do: {:error, Error.not_sent(:codec, {:invalid_codec_result, other}, message_id: message_id)}

  @spec normalize_decode_result(term(), keyword()) :: {:ok, Envelope.t()} | {:error, Error.t()}
  defp normalize_decode_result({:ok, %Envelope{} = envelope}, opts),
    do: Envelope.new(envelope, opts)

  defp normalize_decode_result({:ok, value}, _opts),
    do: {:error, Error.not_sent(:codec, {:decoded_value_not_envelope, value})}

  defp normalize_decode_result({:error, %Error{} = error}, _opts), do: {:error, error}

  defp normalize_decode_result({:error, reason}, _opts),
    do: {:error, Error.not_sent(:codec, reason)}

  defp normalize_decode_result(other, _opts),
    do: {:error, Error.not_sent(:codec, {:invalid_codec_result, other})}

  @spec invalid_encoded(term(), String.t()) :: {:error, Error.t()}
  defp invalid_encoded(value, message_id),
    do: {:error, Error.not_sent(:codec, {:invalid_encoded_value, value}, message_id: message_id)}

  @spec protected_call(module(), atom(), [term()], String.t() | nil) :: term()
  defp protected_call(module, function, args, message_id) do
    apply(module, function, args)
  rescue
    exception ->
      {:error,
       Error.not_sent(:codec, {:codec_exception, exception},
         message_id: message_id,
         cause: exception
       )}
  catch
    kind, reason ->
      {:error, Error.not_sent(:codec, {:codec_exit, kind, reason}, message_id: message_id)}
  end

  @spec put_message_context(Error.t(), String.t()) :: Error.t()
  defp put_message_context(%Error{message_id: nil} = error, message_id),
    do: %{error | message_id: message_id}

  defp put_message_context(%Error{} = error, _message_id), do: error

  @spec message_id(term()) :: term()
  defp message_id(%Envelope{id: id}), do: id
  defp message_id(_envelope), do: nil
end

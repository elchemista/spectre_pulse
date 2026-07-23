defmodule Spectre.Pulse.Validator do
  @moduledoc """
  Protocol validation independent of transport and encoding.
  """

  alias Spectre.Pulse.Address
  alias Spectre.Pulse.Envelope
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Payload
  alias Spectre.Pulse.Protocol

  @version Protocol.version()
  @uuid_pattern ~r/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/

  @doc "Validates every controlled field in an envelope."
  @spec validate(Envelope.t(), keyword()) :: {:ok, Envelope.t()} | {:error, Error.t()}
  def validate(envelope, opts \\ [])

  def validate(%Envelope{} = envelope, opts) do
    with :ok <- validate_version(envelope.version),
         :ok <- validate_id(:id, envelope.id),
         :ok <- validate_relation(envelope.relates_to, envelope.id),
         {:ok, from} <- Address.normalize(envelope.from, opts),
         {:ok, to} <- Address.normalize(envelope.to, opts),
         :ok <- validate_act(envelope.act),
         {:ok, payload} <- Payload.new(envelope.payload, opts),
         :ok <- validate_metadata(envelope.metadata, opts) do
      {:ok, %{envelope | from: from, to: to, payload: payload}}
    else
      {:error, %Error{} = error} ->
        {:error, put_message_id(error, envelope.id)}

      {:error, reason} ->
        {:error, Error.not_sent(:validation, reason, message_id: envelope.id)}
    end
  end

  def validate(value, _opts),
    do: {:error, Error.not_sent(:validation, {:invalid_envelope, value})}

  @doc "Returns true for RFC 9562 UUIDv7 strings accepted by Pulse v1."
  @spec valid_id?(term()) :: boolean()
  def valid_id?(id), do: is_binary(id) and Regex.match?(@uuid_pattern, id)

  @spec validate_version(term()) :: :ok | {:error, term()}
  defp validate_version(@version), do: :ok
  defp validate_version(version), do: {:error, {:unsupported_version, version}}

  @spec validate_id(atom(), term()) :: :ok | {:error, term()}
  defp validate_id(_field, id) when is_binary(id) do
    if valid_id?(id), do: :ok, else: {:error, {:invalid_message_id, id}}
  end

  defp validate_id(field, id), do: {:error, {:invalid_message_id, field, id}}

  @spec validate_relation(term(), String.t()) :: :ok | {:error, term()}
  defp validate_relation(nil, _id), do: :ok
  defp validate_relation(id, id), do: {:error, :message_cannot_relate_to_itself}
  defp validate_relation(relation, _id), do: validate_id(:relates_to, relation)

  @spec validate_act(term()) :: :ok | {:error, term()}
  defp validate_act(act) do
    if Protocol.valid_act?(act), do: :ok, else: {:error, {:unsupported_act, act}}
  end

  @spec validate_metadata(term(), keyword()) :: :ok | {:error, term()}
  defp validate_metadata(metadata, opts) when is_map(metadata) do
    max_bytes =
      Keyword.get(opts, :max_metadata_bytes, Protocol.default_limits().max_metadata_bytes)

    case Jason.encode(metadata) do
      {:ok, encoded} when byte_size(encoded) <= max_bytes ->
        :ok

      {:ok, encoded} ->
        {:error, {:metadata_too_large, byte_size(encoded), max_bytes}}

      {:error, reason} ->
        {:error, {:metadata_not_encodable, reason}}
    end
  end

  defp validate_metadata(metadata, _opts), do: {:error, {:invalid_metadata, metadata}}

  @spec put_message_id(Error.t(), String.t() | nil) :: Error.t()
  defp put_message_id(%Error{message_id: nil} = error, id), do: %{error | message_id: id}
  defp put_message_id(%Error{} = error, _id), do: error
end

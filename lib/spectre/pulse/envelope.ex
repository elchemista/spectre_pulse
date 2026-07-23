defmodule Spectre.Pulse.Envelope do
  @moduledoc """
  The immutable semantic unit transported by Pulse.

  Delivery is at-least-possibly-once: envelopes can be duplicated and arrive
  out of order. Re-delivering the same message reuses the same `id`.
  """

  alias Spectre.Pulse.Address
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Payload
  alias Spectre.Pulse.Protocol
  alias Spectre.Pulse.Validator

  @enforce_keys [:version, :id, :from, :to, :act, :payload]
  defstruct [:version, :id, :from, :to, :act, :relates_to, :payload, metadata: %{}]

  @type act :: :inform | :query | :request

  @type t :: %__MODULE__{
          version: pos_integer(),
          id: String.t(),
          from: String.t(),
          to: String.t(),
          act: act(),
          relates_to: String.t() | nil,
          payload: Payload.t(),
          metadata: map()
        }

  @doc "Builds a validated v1 envelope, generating a UUIDv7 when needed."
  @spec new(t() | map() | keyword(), keyword()) :: {:ok, t()} | {:error, Error.t()}
  def new(envelope, opts \\ [])
  def new(%__MODULE__{} = envelope, opts), do: Validator.validate(envelope, opts)
  def new(envelope, opts) when is_list(envelope), do: envelope |> Map.new() |> new(opts)

  def new(envelope, opts) when is_map(envelope) do
    with {:ok, from} <- Address.normalize(attr(envelope, :from), opts),
         {:ok, to} <- Address.normalize(attr(envelope, :to), opts),
         {:ok, act} <- Protocol.decode_act(attr(envelope, :act, :inform)),
         {:ok, payload} <- Payload.new(attr(envelope, :payload), opts) do
      value = %__MODULE__{
        version: attr(envelope, :version, Protocol.version()),
        id: attr(envelope, :id, Spectre.Identity.uuid7()),
        from: from,
        to: to,
        act: act,
        relates_to: attr(envelope, :relates_to),
        payload: payload,
        metadata: attr(envelope, :metadata, %{})
      }

      Validator.validate(value, opts)
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, Error.not_sent(:validation, reason)}
    end
  end

  def new(envelope, _opts),
    do: {:error, Error.not_sent(:validation, {:invalid_envelope, envelope})}

  @doc "Like `new/2`, but raises `ArgumentError` for invalid input."
  @spec new!(t() | map() | keyword(), keyword()) :: t()
  def new!(envelope, opts \\ []) do
    case new(envelope, opts) do
      {:ok, value} -> value
      {:error, error} -> raise ArgumentError, Exception.message(error)
    end
  end

  @doc "Returns a string-keyed representation shared by wire codecs."
  @spec to_wire(t()) :: map()
  def to_wire(%__MODULE__{} = envelope) do
    {:ok, act} = Protocol.encode_act(envelope.act)

    %{
      "version" => envelope.version,
      "id" => envelope.id,
      "from" => envelope.from,
      "to" => envelope.to,
      "act" => act,
      "relates_to" => envelope.relates_to,
      "payload" => Payload.to_wire(envelope.payload),
      "metadata" => envelope.metadata
    }
  end

  @doc "Builds a response envelope by swapping sender and recipient."
  @spec reply(t(), String.t(), term(), keyword()) :: {:ok, t()} | {:error, Error.t()}
  def reply(%__MODULE__{} = incoming, type, data, opts \\ []) do
    new(
      version: incoming.version,
      from: incoming.to,
      to: incoming.from,
      act: Keyword.get(opts, :act, :inform),
      relates_to: Keyword.get(opts, :relates_to, incoming.id),
      payload: %{type: type, data: data},
      metadata: Keyword.get(opts, :metadata, %{})
    )
  end

  @spec attr(map(), atom(), term()) :: term()
  defp attr(map, key, default \\ nil),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key), default))
end

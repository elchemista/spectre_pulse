defmodule Spectre.Pulse.Payload do
  @moduledoc """
  Namespaced application data carried by an envelope.

  Pulse validates `type` and treats `data` as opaque. Domain interpretation
  remains the receiving agent's responsibility.
  """

  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Options
  alias Spectre.Pulse.Protocol

  defstruct [:type, data: %{}]

  @type t :: %__MODULE__{type: String.t(), data: term()}

  @doc "Builds and validates a payload."
  @spec new(t() | map() | keyword(), keyword()) :: {:ok, t()} | {:error, Error.t()}
  def new(payload, opts \\ [])
  def new(%__MODULE__{} = payload, opts), do: validate(payload, opts)

  def new(payload, opts) when is_list(payload) do
    if Keyword.keyword?(payload),
      do: payload |> Map.new() |> new(opts),
      else: {:error, Error.not_sent(:validation, {:invalid_payload, payload})}
  end

  def new(payload, opts) when is_map(payload) do
    type = attr(payload, :type)
    data = attr(payload, :data, %{})
    validate(%__MODULE__{type: type, data: data}, opts)
  end

  def new(payload, _opts),
    do: {:error, Error.not_sent(:validation, {:invalid_payload, payload})}

  @doc "Like `new/2`, but raises for invalid input."
  @spec new!(t() | map() | keyword(), keyword()) :: t()
  def new!(payload, opts \\ []) do
    case new(payload, opts) do
      {:ok, value} -> value
      {:error, error} -> raise ArgumentError, Exception.message(error)
    end
  end

  @doc "Returns the string-keyed wire projection."
  @spec to_wire(t()) :: map()
  def to_wire(%__MODULE__{} = payload), do: %{"type" => payload.type, "data" => payload.data}

  @spec validate(t(), keyword()) :: {:ok, t()} | {:error, Error.t()}
  defp validate(%__MODULE__{type: type} = payload, opts) when is_binary(type) do
    with {:ok, opts} <- Options.keyword(opts),
         {:ok, max_bytes} <-
           Options.positive_integer(
             opts,
             :max_type_bytes,
             Protocol.default_limits().max_type_bytes
           ) do
      cond do
        type == "" ->
          {:error, Error.not_sent(:validation, :payload_type_required)}

        byte_size(type) > max_bytes ->
          {:error,
           Error.not_sent(:validation, {:payload_type_too_large, byte_size(type), max_bytes})}

        not String.valid?(type) or
            not Regex.match?(~r/^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)+$/, type) ->
          {:error, Error.not_sent(:validation, {:invalid_payload_type, type})}

        true ->
          {:ok, payload}
      end
    else
      {:error, reason} -> {:error, Error.not_sent(:validation, reason)}
    end
  end

  defp validate(%__MODULE__{type: type}, _opts),
    do: {:error, Error.not_sent(:validation, {:invalid_payload_type, type})}

  @spec attr(map(), atom(), term()) :: term()
  defp attr(map, key, default \\ nil),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key), default))
end

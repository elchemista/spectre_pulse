defmodule Spectre.Pulse.InboundContext do
  @moduledoc """
  Facts supplied by a transport for one inbound envelope.

  These fields are host/transport assertions. They are kept separate from the
  sender-declared `Envelope.metadata`.
  """

  defstruct [
    :authenticated_identity,
    :binding,
    :peer,
    :target,
    :target_identity,
    :resolver,
    :authorization,
    verified: %{},
    metadata: %{}
  ]

  @type t :: %__MODULE__{
          authenticated_identity: String.t() | nil,
          binding: atom() | String.t() | nil,
          peer: term(),
          target: module() | GenServer.server() | nil,
          target_identity: String.t() | nil,
          resolver: term(),
          authorization: term(),
          verified: map(),
          metadata: map()
        }

  @doc "Normalizes a context struct, map, or keyword list."
  @spec new(t() | map() | keyword()) :: t()
  def new(%__MODULE__{} = context), do: context
  def new(context) when is_list(context), do: context |> Map.new() |> new()

  def new(context) when is_map(context) do
    fields = __MODULE__.__struct__() |> Map.keys() |> List.delete(:__struct__)

    context =
      Enum.reduce(fields, %{}, fn field, acc ->
        value = Map.get(context, field, Map.get(context, Atom.to_string(field)))
        if is_nil(value), do: acc, else: Map.put(acc, field, value)
      end)

    struct(__MODULE__, context)
  end
end

defmodule Spectre.Pulse.Route do
  @moduledoc """
  A physical way to reach one logical Pulse address.

  Routes are application/infrastructure data. They are never included in the
  envelope or exposed to model reasoning by default.
  """

  alias Spectre.Pulse.Address
  alias Spectre.Pulse.Error

  @enforce_keys [:id, :address, :transport, :target]
  defstruct [:id, :address, :transport, :target, priority: 100, metadata: %{}]

  @type t :: %__MODULE__{
          id: term(),
          address: String.t(),
          transport: module(),
          target: term(),
          priority: integer(),
          metadata: map()
        }

  @doc "Builds a validated route."
  @spec new(t() | map() | keyword()) :: {:ok, t()} | {:error, Error.t()}
  def new(%__MODULE__{} = route), do: validate(route)
  def new(attrs) when is_list(attrs), do: attrs |> Map.new() |> new()

  def new(attrs) when is_map(attrs) do
    with {:ok, address} <- Address.normalize(attr(attrs, :address)) do
      route = %__MODULE__{
        id: attr(attrs, :id, Spectre.Identity.uuid7()),
        address: address,
        transport: attr(attrs, :transport),
        target: attr(attrs, :target),
        priority: attr(attrs, :priority, 100),
        metadata: attr(attrs, :metadata, %{})
      }

      validate(route)
    end
  end

  def new(value),
    do: {:error, Error.not_sent(:routing, {:invalid_route, value})}

  @doc "Like `new/1`, but raises for invalid route data."
  @spec new!(t() | map() | keyword()) :: t()
  def new!(route) do
    case new(route) do
      {:ok, value} -> value
      {:error, error} -> raise ArgumentError, Exception.message(error)
    end
  end

  @doc """
  Convenience constructor for an in-node process mailbox.

  Agent code normally does not build this Route. A local `subscribe/2`
  registration lets discovery create it automatically.
  """
  @spec local(String.t(), term(), keyword()) :: t()
  def local(address, target, opts \\ []) do
    build_convenience(address, Spectre.Pulse.Transports.Local, target, opts)
  end

  @doc "Convenience constructor for a remote HTTP endpoint."
  @spec rest(String.t(), String.t(), keyword()) :: t()
  def rest(address, url, opts \\ []) do
    build_convenience(address, Spectre.Pulse.Transports.REST, url, opts)
  end

  @doc "Convenience constructor for an application-owned WebSocket connection."
  @spec web_socket(String.t(), term(), keyword()) :: t()
  def web_socket(address, sender, opts \\ []) do
    build_convenience(address, Spectre.Pulse.Transports.WebSocket, sender, opts)
  end

  @doc "Convenience constructor for an authenticated BEAM node."
  @spec node(String.t(), node(), term(), keyword()) :: t()
  def node(address, node, endpoint, opts \\ []) do
    build_convenience(
      address,
      Spectre.Pulse.Transports.Node,
      %{node: node, endpoint: endpoint},
      opts
    )
  end

  @doc "Convenience constructor for a PubSub topic."
  @spec pub_sub(String.t(), term(), keyword()) :: t()
  def pub_sub(address, target, opts \\ []) do
    build_convenience(address, Spectre.Pulse.Transports.PubSub, target, opts)
  end

  @spec validate(t()) :: {:ok, t()} | {:error, Error.t()}
  defp validate(%__MODULE__{} = route) do
    cond do
      is_nil(route.id) ->
        {:error, Error.not_sent(:routing, :route_id_required)}

      not is_atom(route.transport) or is_nil(route.transport) ->
        {:error, Error.not_sent(:routing, {:invalid_transport, route.transport})}

      is_nil(route.target) ->
        {:error, Error.not_sent(:routing, :route_target_required)}

      not is_integer(route.priority) ->
        {:error, Error.not_sent(:routing, {:invalid_route_priority, route.priority})}

      not is_map(route.metadata) ->
        {:error, Error.not_sent(:routing, {:invalid_route_metadata, route.metadata})}

      true ->
        {:ok, route}
    end
  end

  @spec build_convenience(String.t(), module(), term(), keyword()) :: t()
  defp build_convenience(address, transport, target, opts) do
    new!(
      id: Keyword.get(opts, :id, Spectre.Identity.uuid7()),
      address: address,
      transport: transport,
      target: target,
      priority: Keyword.get(opts, :priority, 100),
      metadata: Keyword.get(opts, :metadata, %{})
    )
  end

  @spec attr(map(), atom(), term()) :: term()
  defp attr(map, key, default \\ nil),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key), default))
end

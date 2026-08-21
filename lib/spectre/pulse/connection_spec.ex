defmodule Spectre.Pulse.ConnectionSpec do
  @moduledoc """
  Host configuration for one class of Pulse connections.

  A spec exposes every discovered local Agent by default. It may instead name
  an explicit allow-list by Agent module or canonical Pulse address. One spec
  may produce several live connections and one Agent may be exposed by several
  specs.
  """

  alias Spectre.Pulse.Address
  alias Spectre.Pulse.AgentDescriptor
  alias Spectre.Pulse.Error

  @modes [:listen, :connect, :both, :stateless]

  @enforce_keys [:id, :transport]
  defstruct [
    :id,
    :transport,
    :endpoint,
    :authenticate,
    :authorize,
    mode: :connect,
    agents: :all,
    agent_addresses: [],
    profiles: ["pulse.messaging/1"],
    scopes: [],
    priority: 100,
    enabled: true,
    metadata: %{}
  ]

  @type agent_selector :: :all | [module() | String.t()]
  @type t :: %__MODULE__{
          id: atom() | String.t(),
          transport: atom(),
          endpoint: term(),
          authenticate: term(),
          authorize: term(),
          mode: :listen | :connect | :both | :stateless,
          agents: agent_selector(),
          agent_addresses: [String.t()],
          profiles: [String.t()],
          scopes: [String.t()],
          priority: integer(),
          enabled: boolean(),
          metadata: map()
        }

  @doc "Normalizes one connection configuration."
  @spec new(t() | map() | keyword()) :: {:ok, t()} | {:error, Error.t()}
  def new(%__MODULE__{} = spec), do: spec |> Map.from_struct() |> new()

  def new(attrs) when is_list(attrs) do
    if Keyword.keyword?(attrs),
      do: attrs |> Map.new() |> new(),
      else: invalid(attrs)
  end

  def new(attrs) when is_map(attrs) do
    with {:ok, id} <- identifier(attr(attrs, :id), :connection_spec_id),
         {:ok, transport} <- transport(attr(attrs, :transport)),
         {:ok, mode} <- mode(attr(attrs, :mode, :connect)),
         {:ok, agents} <- agents(attr(attrs, :agents, :all)),
         {:ok, profiles} <- names(attr(attrs, :profiles, ["pulse.messaging/1"]), :profiles),
         {:ok, scopes} <- names(attr(attrs, :scopes, []), :scopes),
         {:ok, priority} <- integer(attr(attrs, :priority, 100), :priority),
         {:ok, enabled} <- boolean(attr(attrs, :enabled, true), :enabled),
         :ok <- callback(attr(attrs, :authenticate), :authenticate),
         :ok <- callback(attr(attrs, :authorize), :authorize),
         {:ok, metadata} <- metadata(attr(attrs, :metadata, %{})) do
      {:ok,
       %__MODULE__{
         id: id,
         transport: transport,
         endpoint: attr(attrs, :endpoint),
         authenticate: attr(attrs, :authenticate),
         authorize: attr(attrs, :authorize),
         mode: mode,
         agents: agents,
         agent_addresses: [],
         profiles: profiles,
         scopes: scopes,
         priority: priority,
         enabled: enabled,
         metadata: metadata
       }}
    else
      {:error, reason} -> {:error, Error.not_sent(:validation, reason)}
    end
  end

  def new(value), do: invalid(value)

  @doc "Resolves the configured Agent selector against the local catalog."
  @spec resolve_agents(t(), [AgentDescriptor.t()]) :: {:ok, t()} | {:error, Error.t()}
  def resolve_agents(%__MODULE__{agents: :all} = spec, descriptors) when is_list(descriptors) do
    with {:ok, descriptors} <- normalize_descriptors(descriptors) do
      {:ok, %{spec | agent_addresses: Enum.map(descriptors, & &1.address)}}
    end
  end

  def resolve_agents(%__MODULE__{agents: selectors} = spec, descriptors)
      when is_list(selectors) and is_list(descriptors) do
    with {:ok, descriptors} <- normalize_descriptors(descriptors),
         {:ok, addresses} <- resolve_selectors(selectors, descriptors) do
      {:ok, %{spec | agent_addresses: addresses}}
    end
  end

  def resolve_agents(%__MODULE__{} = spec, descriptors),
    do:
      {:error,
       Error.not_sent(:validation, {:invalid_connection_agent_catalog, spec.id, descriptors})}

  @doc "Returns a public projection without endpoint credentials or callbacks."
  @spec to_public_map(t()) :: map()
  def to_public_map(%__MODULE__{} = spec) do
    %{
      id: spec.id,
      transport: spec.transport,
      mode: spec.mode,
      agent_addresses: spec.agent_addresses,
      profiles: spec.profiles,
      scopes: spec.scopes,
      priority: spec.priority,
      enabled: spec.enabled,
      metadata: spec.metadata
    }
  end

  @spec resolve_selectors([term()], [AgentDescriptor.t()]) ::
          {:ok, [String.t()]} | {:error, Error.t()}
  defp resolve_selectors(selectors, descriptors) do
    Enum.reduce_while(selectors, {:ok, []}, fn selector, {:ok, addresses} ->
      case descriptor_for(selector, descriptors) do
        {:ok, descriptor} ->
          {:cont, {:ok, [descriptor.address | addresses]}}

        :error ->
          {:halt, {:error, Error.not_sent(:validation, {:unknown_connection_agent, selector})}}
      end
    end)
    |> case do
      {:ok, addresses} -> {:ok, addresses |> Enum.reverse() |> Enum.uniq()}
      {:error, %Error{} = error} -> {:error, error}
    end
  end

  @spec descriptor_for(term(), [AgentDescriptor.t()]) :: {:ok, AgentDescriptor.t()} | :error
  defp descriptor_for(module, descriptors) when is_atom(module) and not is_nil(module) do
    case Enum.find(descriptors, &(&1.module == module)) do
      nil -> :error
      descriptor -> {:ok, descriptor}
    end
  end

  defp descriptor_for(address, descriptors) when is_binary(address) do
    with {:ok, canonical} <- Address.normalize(address),
         %AgentDescriptor{} = descriptor <- Enum.find(descriptors, &(&1.address == canonical)) do
      {:ok, descriptor}
    else
      _other -> :error
    end
  end

  defp descriptor_for(_selector, _descriptors), do: :error

  @spec normalize_descriptors([term()]) ::
          {:ok, [AgentDescriptor.t()]} | {:error, Error.t()}
  defp normalize_descriptors(descriptors) do
    Enum.reduce_while(descriptors, {:ok, []}, fn descriptor, {:ok, values} ->
      case AgentDescriptor.new(descriptor) do
        {:ok, value} -> {:cont, {:ok, [value | values]}}
        {:error, %Error{} = error} -> {:halt, {:error, error}}
      end
    end)
    |> case do
      {:ok, values} -> {:ok, Enum.reverse(values)}
      error -> error
    end
  end

  @spec identifier(term(), atom()) :: {:ok, atom() | String.t()} | {:error, term()}
  defp identifier(value, _field) when is_atom(value) and not is_nil(value), do: {:ok, value}

  defp identifier(value, _field) when is_binary(value) do
    if String.valid?(value) and String.trim(value) != "",
      do: {:ok, value},
      else: {:error, {:invalid_connection_spec_id, value}}
  end

  defp identifier(value, _field), do: {:error, {:invalid_connection_spec_id, value}}

  @spec transport(term()) :: {:ok, atom()} | {:error, term()}
  defp transport(value) when is_atom(value) and not is_nil(value), do: {:ok, value}
  defp transport(value), do: {:error, {:invalid_connection_transport, value}}

  @spec mode(term()) :: {:ok, atom()} | {:error, term()}
  defp mode(value) when value in @modes, do: {:ok, value}
  defp mode(value), do: {:error, {:invalid_connection_mode, value}}

  @spec agents(term()) :: {:ok, agent_selector()} | {:error, term()}
  defp agents(:all), do: {:ok, :all}

  defp agents(values) when is_list(values) do
    if Enum.all?(values, &valid_agent_selector?/1),
      do: {:ok, Enum.uniq(values)},
      else: {:error, {:invalid_connection_agents, values}}
  end

  defp agents(value), do: {:error, {:invalid_connection_agents, value}}

  @spec valid_agent_selector?(term()) :: boolean()
  defp valid_agent_selector?(value) when is_atom(value), do: not is_nil(value)

  defp valid_agent_selector?(value) when is_binary(value),
    do: String.valid?(value) and value != ""

  defp valid_agent_selector?(_value), do: false

  @spec names(term(), atom()) :: {:ok, [String.t()]} | {:error, term()}
  defp names(values, field) when is_list(values) do
    if Enum.all?(values, &valid_name?/1),
      do: {:ok, values |> Enum.map(&to_string/1) |> Enum.uniq()},
      else: {:error, {:invalid_connection_spec_field, field, values}}
  end

  defp names(value, field), do: {:error, {:invalid_connection_spec_field, field, value}}

  @spec valid_name?(term()) :: boolean()
  defp valid_name?(value) when is_atom(value), do: not is_nil(value)
  defp valid_name?(value) when is_binary(value), do: String.valid?(value) and value != ""
  defp valid_name?(_value), do: false

  @spec integer(term(), atom()) :: {:ok, integer()} | {:error, term()}
  defp integer(value, _field) when is_integer(value), do: {:ok, value}
  defp integer(value, field), do: {:error, {:invalid_connection_spec_field, field, value}}

  @spec boolean(term(), atom()) :: {:ok, boolean()} | {:error, term()}
  defp boolean(value, _field) when is_boolean(value), do: {:ok, value}
  defp boolean(value, field), do: {:error, {:invalid_connection_spec_field, field, value}}

  @spec callback(term(), atom()) :: :ok | {:error, term()}
  defp callback(nil, _field), do: :ok
  defp callback(value, _field) when is_function(value, 1) or is_function(value, 2), do: :ok
  defp callback(value, _field) when is_atom(value) and not is_nil(value), do: :ok

  defp callback({module, function, args}, _field)
       when is_atom(module) and not is_nil(module) and is_atom(function) and is_list(args),
       do: :ok

  defp callback(value, field),
    do: {:error, {:invalid_connection_spec_field, field, value}}

  @spec metadata(term()) :: {:ok, map()} | {:error, term()}
  defp metadata(value) when is_map(value), do: {:ok, value}
  defp metadata(value), do: {:error, {:invalid_connection_metadata, value}}

  @spec invalid(term()) :: {:error, Error.t()}
  defp invalid(value),
    do: {:error, Error.not_sent(:validation, {:invalid_connection_spec, value})}

  @spec attr(map(), atom(), term()) :: term()
  defp attr(map, key, default \\ nil),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key), default))
end

defmodule Spectre.Pulse.Connection do
  @moduledoc """
  One authenticated live link created from a ConnectionSpec.

  Connections are technical and ephemeral. They retain an authenticated
  Principal and granted access, but never the credential used during the
  handshake. A single connection may expose and discover several Agents.
  """

  alias Spectre.Pulse.AgentDescriptor
  alias Spectre.Pulse.ConnectionSpec
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Principal

  @directions [:inbound, :outbound]
  @statuses [:connecting, :connected, :draining, :disconnected]

  @enforce_keys [:id, :spec_id, :transport, :principal, :owner]
  defstruct [
    :id,
    :spec_id,
    :peer_id,
    :transport,
    :transport_pid,
    :owner,
    :principal,
    :connected_at,
    :last_seen_at,
    direction: :inbound,
    status: :connected,
    requested_profiles: [],
    granted_profiles: [],
    granted_scopes: [],
    local_agent_addresses: [],
    remote_agents: [],
    verified: %{},
    metadata: %{}
  ]

  @type t :: %__MODULE__{
          id: term(),
          spec_id: atom() | String.t(),
          peer_id: String.t() | nil,
          transport: atom(),
          transport_pid: pid() | nil,
          owner: pid(),
          principal: Principal.t(),
          connected_at: DateTime.t(),
          last_seen_at: DateTime.t(),
          direction: :inbound | :outbound,
          status: :connecting | :connected | :draining | :disconnected,
          requested_profiles: [String.t()],
          granted_profiles: [String.t()],
          granted_scopes: [String.t()],
          local_agent_addresses: [String.t()],
          remote_agents: [AgentDescriptor.t()],
          verified: map(),
          metadata: map()
        }

  @doc "Builds one live connection from a resolved ConnectionSpec."
  @spec new(ConnectionSpec.t(), map() | keyword()) :: {:ok, t()} | {:error, Error.t()}
  def new(%ConnectionSpec{} = spec, attrs) when is_list(attrs) do
    if Keyword.keyword?(attrs),
      do: new(spec, Map.new(attrs)),
      else: invalid({spec.id, attrs})
  end

  def new(%ConnectionSpec{} = spec, attrs) when is_map(attrs) do
    with true <- spec.enabled,
         {:ok, principal} <- Principal.new(attr(attrs, :principal)),
         {:ok, owner} <- owner(attr(attrs, :owner, attr(attrs, :transport_pid))),
         {:ok, transport_pid} <- optional_pid(attr(attrs, :transport_pid), :transport_pid),
         {:ok, peer_id} <- optional_string(attr(attrs, :peer_id), :peer_id),
         {:ok, direction} <- member(attr(attrs, :direction, :inbound), @directions, :direction),
         {:ok, status} <- member(attr(attrs, :status, :connected), @statuses, :status),
         {:ok, requested_profiles} <-
           names(attr(attrs, :requested_profiles, []), :requested_profiles),
         {:ok, granted_profiles} <-
           names(attr(attrs, :granted_profiles, spec.profiles), :granted_profiles),
         allowed_scopes <- intersect(spec.scopes, principal.scopes),
         {:ok, granted_scopes} <-
           names(attr(attrs, :granted_scopes, allowed_scopes), :granted_scopes),
         :ok <- ensure_subset(granted_profiles, spec.profiles, :profiles),
         :ok <- ensure_subset(granted_scopes, allowed_scopes, :scopes),
         {:ok, remote_agents} <- remote_agents(attr(attrs, :remote_agents, [])),
         {:ok, verified} <- plain_map(attr(attrs, :verified, principal.verified), :verified),
         {:ok, metadata} <- plain_map(attr(attrs, :metadata, %{}), :metadata),
         {:ok, connected_at} <-
           datetime(attr(attrs, :connected_at, DateTime.utc_now()), :connected_at),
         {:ok, last_seen_at} <-
           datetime(attr(attrs, :last_seen_at, connected_at), :last_seen_at) do
      {:ok,
       %__MODULE__{
         id: attr_lazy(attrs, :id, &Spectre.Identity.uuid7/0),
         spec_id: spec.id,
         peer_id: peer_id,
         transport: spec.transport,
         transport_pid: transport_pid,
         owner: owner,
         principal: principal,
         connected_at: connected_at,
         last_seen_at: last_seen_at,
         direction: direction,
         status: status,
         requested_profiles: requested_profiles,
         granted_profiles: granted_profiles,
         granted_scopes: granted_scopes,
         local_agent_addresses: spec.agent_addresses,
         remote_agents: remote_agents,
         verified: verified,
         metadata: metadata
       }}
    else
      false -> {:error, Error.not_sent(:authorization, {:connection_spec_disabled, spec.id})}
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, Error.not_sent(:validation, reason)}
    end
  end

  def new(%ConnectionSpec{} = spec, attrs), do: invalid({spec.id, attrs})

  @doc "Returns remote Agent addresses advertised through this connection."
  @spec remote_agent_addresses(t()) :: [String.t()]
  def remote_agent_addresses(%__MODULE__{} = connection),
    do: Enum.map(connection.remote_agents, & &1.address)

  @doc "Returns a credential-free projection suitable for Studio and logs."
  @spec to_public_map(t()) :: map()
  def to_public_map(%__MODULE__{} = connection) do
    %{
      id: connection.id,
      spec_id: connection.spec_id,
      peer_id: connection.peer_id,
      transport: connection.transport,
      principal: %{
        id: connection.principal.id,
        identity: connection.principal.identity,
        kind: connection.principal.kind,
        scopes: connection.principal.scopes
      },
      connected_at: connection.connected_at,
      last_seen_at: connection.last_seen_at,
      direction: connection.direction,
      status: connection.status,
      requested_profiles: connection.requested_profiles,
      granted_profiles: connection.granted_profiles,
      granted_scopes: connection.granted_scopes,
      local_agent_addresses: connection.local_agent_addresses,
      remote_agents: Enum.map(connection.remote_agents, &AgentDescriptor.to_wire/1),
      verified: connection.verified,
      metadata: connection.metadata
    }
  end

  @spec owner(term()) :: {:ok, pid()} | {:error, term()}
  defp owner(value) when is_pid(value) do
    if Process.alive?(value),
      do: {:ok, value},
      else: {:error, :connection_owner_not_alive}
  end

  defp owner(value), do: {:error, {:invalid_connection_owner, value}}

  @spec optional_pid(term(), atom()) :: {:ok, pid() | nil} | {:error, term()}
  defp optional_pid(nil, _field), do: {:ok, nil}
  defp optional_pid(value, _field) when is_pid(value), do: {:ok, value}

  defp optional_pid(value, field),
    do: {:error, {:invalid_connection_field, field, value}}

  @spec optional_string(term(), atom()) :: {:ok, String.t() | nil} | {:error, term()}
  defp optional_string(nil, _field), do: {:ok, nil}

  defp optional_string(value, _field) when is_binary(value) do
    if String.valid?(value) and String.trim(value) != "",
      do: {:ok, value},
      else: {:error, {:invalid_connection_peer_id, value}}
  end

  defp optional_string(value, _field), do: {:error, {:invalid_connection_peer_id, value}}

  @spec member(term(), [term()], atom()) :: {:ok, term()} | {:error, term()}
  defp member(value, allowed, field) do
    if value in allowed,
      do: {:ok, value},
      else: {:error, {:invalid_connection_field, field, value}}
  end

  @spec names(term(), atom()) :: {:ok, [String.t()]} | {:error, term()}
  defp names(values, field) when is_list(values) do
    if Enum.all?(values, &valid_name?/1),
      do: {:ok, values |> Enum.map(&to_string/1) |> Enum.uniq()},
      else: {:error, {:invalid_connection_field, field, values}}
  end

  defp names(value, field), do: {:error, {:invalid_connection_field, field, value}}

  @spec valid_name?(term()) :: boolean()
  defp valid_name?(value) when is_atom(value), do: not is_nil(value)
  defp valid_name?(value) when is_binary(value), do: String.valid?(value) and value != ""
  defp valid_name?(_value), do: false

  @spec ensure_subset([String.t()], [String.t()], atom()) :: :ok | {:error, term()}
  defp ensure_subset(values, allowed, field) do
    case values -- allowed do
      [] -> :ok
      denied -> {:error, {:connection_grant_exceeds_spec, field, denied}}
    end
  end

  @spec remote_agents(term()) :: {:ok, [AgentDescriptor.t()]} | {:error, Error.t()}
  defp remote_agents(values) when is_list(values) do
    Enum.reduce_while(values, {:ok, []}, fn value, {:ok, agents} ->
      case AgentDescriptor.new(value) do
        {:ok, descriptor} -> {:cont, {:ok, [descriptor | agents]}}
        {:error, %Error{} = error} -> {:halt, {:error, error}}
      end
    end)
    |> case do
      {:ok, agents} -> unique_remote_agents(Enum.reverse(agents))
      error -> error
    end
  end

  defp remote_agents(value),
    do: {:error, Error.not_sent(:validation, {:invalid_connection_remote_agents, value})}

  @spec unique_remote_agents([AgentDescriptor.t()]) ::
          {:ok, [AgentDescriptor.t()]} | {:error, Error.t()}
  defp unique_remote_agents(agents) do
    duplicate =
      agents
      |> Enum.group_by(& &1.address)
      |> Enum.find(fn {_address, values} -> length(values) > 1 end)

    case duplicate do
      nil ->
        {:ok, agents}

      {address, _values} ->
        {:error, Error.not_sent(:validation, {:duplicate_connection_remote_agent, address})}
    end
  end

  @spec plain_map(term(), atom()) :: {:ok, map()} | {:error, term()}
  defp plain_map(value, _field) when is_map(value), do: {:ok, value}

  defp plain_map(value, field),
    do: {:error, {:invalid_connection_field, field, value}}

  @spec datetime(term(), atom()) :: {:ok, DateTime.t()} | {:error, term()}
  defp datetime(%DateTime{} = value, _field), do: {:ok, value}

  defp datetime(value, field),
    do: {:error, {:invalid_connection_field, field, value}}

  @spec intersect([String.t()], [String.t()]) :: [String.t()]
  defp intersect(left, right), do: Enum.filter(left, &(&1 in right))

  @spec invalid(term()) :: {:error, Error.t()}
  defp invalid(value),
    do: {:error, Error.not_sent(:validation, {:invalid_connection, value})}

  @spec attr(map(), atom(), term()) :: term()
  defp attr(map, key, default \\ nil),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key), default))

  @spec attr_lazy(map(), atom(), (-> term())) :: term()
  defp attr_lazy(map, key, default) do
    case Map.fetch(map, key) do
      {:ok, value} -> value
      :error -> Map.get_lazy(map, Atom.to_string(key), default)
    end
  end
end

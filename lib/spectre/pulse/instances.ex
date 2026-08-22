defmodule Spectre.Pulse.Instances do
  @moduledoc """
  Bounded discovery of live Agent Instances exposed to an authenticated connection.

  Discovery returns logical subjects and opaque Instance refs only. It never
  publishes GenServer state, mailbox contents, Registry metadata, or private
  Agents. The same `agent.runtime.read` grant used for one runtime snapshot is
  required to discover which snapshots are available.
  """

  alias Spectre.Instance.Ref
  alias Spectre.Pulse.Address
  alias Spectre.Pulse.AgentDescriptor
  alias Spectre.Pulse.Connection
  alias Spectre.Pulse.ConnectionRegistry
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.RuntimeInfo

  @maximum_instances 256

  @doc "Lists live Instances for one Agent exposed by a Pulse connection."
  @spec list(String.t(), term(), keyword()) :: {:ok, map()} | {:error, Error.t()}
  def list(agent_address, connection_id, opts \\ []) do
    with {:ok, address} <- Address.normalize(agent_address),
         {:ok, connection} <- fetch_connection(connection_id),
         :ok <- require_scope(connection),
         :ok <- require_exposed_agent(connection, address),
         {:ok, descriptor} <- ConnectionRegistry.local_agent(address),
         {:ok, registry} <- instance_registry(opts),
         {:ok, instances} <- registry_instances(registry, descriptor) do
      {:ok,
       %{
         "schema_version" => 1,
         "capability" => "agent.instances.list",
         "scope" => RuntimeInfo.scope(),
         "agent_address" => address,
         "count" => length(instances),
         "instances" => instances
       }}
    end
  rescue
    exception ->
      {:error,
       Error.not_sent(:inbound, :instance_discovery_failed,
         cause: exception,
         details: %{operation: :instance_discovery}
       )}
  catch
    :exit, _reason ->
      {:error, Error.not_sent(:routing, :instance_registry_unavailable)}
  end

  @spec fetch_connection(term()) :: {:ok, Connection.t()} | {:error, Error.t()}
  defp fetch_connection(connection_id) do
    case ConnectionRegistry.fetch(connection_id) do
      {:ok, %Connection{} = connection} -> {:ok, connection}
      :error -> {:error, Error.not_sent(:routing, :unknown_connection)}
    end
  end

  @spec require_scope(Connection.t()) :: :ok | {:error, Error.t()}
  defp require_scope(connection) do
    if RuntimeInfo.scope() in connection.granted_scopes do
      :ok
    else
      {:error, Error.not_sent(:authorization, {:connection_scope_required, RuntimeInfo.scope()})}
    end
  end

  @spec require_exposed_agent(Connection.t(), String.t()) :: :ok | {:error, Error.t()}
  defp require_exposed_agent(connection, address) do
    if address in connection.local_agent_addresses,
      do: :ok,
      else: {:error, Error.not_sent(:authorization, :agent_not_exposed_on_connection)}
  end

  @spec instance_registry(keyword()) :: {:ok, atom()} | {:error, Error.t()}
  defp instance_registry(opts) do
    case Keyword.get(opts, :instance_registry, Spectre.Instance.Registry) do
      registry when is_atom(registry) -> {:ok, registry}
      _invalid -> {:error, Error.not_sent(:validation, :invalid_instance_registry)}
    end
  end

  @spec registry_instances(atom(), AgentDescriptor.t()) ::
          {:ok, [map()]} | {:error, Error.t()}
  defp registry_instances(registry, descriptor) do
    instances =
      registry
      |> Registry.select([{{:"$1", :"$2", :"$3"}, [], [{{:"$2", :"$3"}}]}])
      |> Enum.flat_map(&public_instance(&1, descriptor))
      |> Enum.sort_by(& &1["subject"])
      |> Enum.take(@maximum_instances)

    {:ok, instances}
  rescue
    ArgumentError -> {:error, Error.not_sent(:routing, :instance_registry_unavailable)}
  end

  @spec public_instance({term(), term()}, AgentDescriptor.t()) :: [map()]
  defp public_instance({pid, %Ref{} = ref}, descriptor) when is_pid(pid) do
    if Process.alive?(pid) and ref.agent_ref.definition == descriptor.module do
      [
        %{
          "subject" => ref.subject.id,
          "instance_ref" => ref.key,
          "pid" => inspect(pid),
          "node" => Atom.to_string(node(pid)),
          "alive" => true
        }
      ]
    else
      []
    end
  end

  defp public_instance(_entry, _descriptor), do: []
end

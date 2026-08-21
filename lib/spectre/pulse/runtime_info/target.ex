defmodule Spectre.Pulse.RuntimeInfo.Target do
  @moduledoc false

  alias Spectre.Instance.Ref
  alias Spectre.Pulse.Address
  alias Spectre.Pulse.AgentDescriptor
  alias Spectre.Pulse.Connection
  alias Spectre.Pulse.ConnectionRegistry
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.RuntimeInfo.Request

  @enforce_keys [:agent, :address, :ref, :pid]
  defstruct [:agent, :address, :ref, :pid]

  @type t :: %__MODULE__{
          agent: module() | Spectre.AgentRef.t(),
          address: String.t(),
          ref: Ref.t(),
          pid: pid()
        }

  @type target :: module() | Spectre.AgentRef.t() | String.t()

  @doc false
  @spec resolve(target(), term(), Request.t(), String.t()) ::
          {:ok, t()} | {:error, Error.t()}
  def resolve(target, subject, %Request{} = request, scope) when is_binary(scope) do
    with {:ok, agent, address} <- authorize(target, request.connection, scope),
         {:ok, ref} <- instance_ref(agent, subject),
         {:ok, pid} <- lookup(ref, request.instance_registry) do
      {:ok, %__MODULE__{agent: agent, address: address, ref: ref, pid: pid}}
    end
  end

  @spec authorize(target(), term() | nil, String.t()) ::
          {:ok, module() | Spectre.AgentRef.t(), String.t()} | {:error, Error.t()}
  defp authorize(target, nil, _scope), do: resolve_local_target(target)

  defp authorize(target, connection_id, scope) do
    with {:ok, connection} <- fetch_connection(connection_id),
         :ok <- require_scope(connection, scope),
         {:ok, address} <- target_address(target),
         :ok <- require_exposed_agent(connection, address),
         {:ok, descriptor} <- fetch_local_agent(address) do
      {:ok, descriptor.module, address}
    end
  end

  @spec fetch_connection(term()) :: {:ok, Connection.t()} | {:error, Error.t()}
  defp fetch_connection(connection_id) do
    case ConnectionRegistry.fetch(connection_id) do
      {:ok, %Connection{} = connection} ->
        {:ok, connection}

      :error ->
        {:error, Error.not_sent(:routing, :unknown_connection)}
    end
  end

  @spec require_scope(Connection.t(), String.t()) :: :ok | {:error, Error.t()}
  defp require_scope(connection, scope) do
    if scope in connection.granted_scopes do
      :ok
    else
      {:error,
       Error.not_sent(:authorization, {:connection_scope_required, scope},
         details: %{connection_id: connection.id}
       )}
    end
  end

  @spec require_exposed_agent(Connection.t(), String.t()) :: :ok | {:error, Error.t()}
  defp require_exposed_agent(connection, address) do
    if address in connection.local_agent_addresses do
      :ok
    else
      {:error,
       Error.not_sent(:authorization, :agent_not_exposed_on_connection,
         details: %{connection_id: connection.id}
       )}
    end
  end

  @spec resolve_local_target(target()) ::
          {:ok, module() | Spectre.AgentRef.t(), String.t()} | {:error, Error.t()}
  defp resolve_local_target(address) when is_binary(address) do
    with {:ok, canonical} <- Address.normalize(address),
         {:ok, descriptor} <- fetch_local_agent(canonical) do
      {:ok, descriptor.module, canonical}
    end
  end

  defp resolve_local_target(%Spectre.AgentRef{} = ref),
    do: {:ok, ref, "agent-ref:" <> ref.id}

  defp resolve_local_target(agent) when is_atom(agent) and not is_nil(agent) do
    address =
      case AgentDescriptor.for_agent(agent) do
        {:ok, descriptor} -> descriptor.address
        {:error, _error} -> "agent:" <> Atom.to_string(agent)
      end

    {:ok, agent, address}
  end

  defp resolve_local_target(_target),
    do: {:error, Error.not_sent(:validation, :invalid_runtime_info_target)}

  @spec target_address(target()) :: {:ok, String.t()} | {:error, Error.t()}
  defp target_address(address) when is_binary(address), do: Address.normalize(address)

  defp target_address(agent) when is_atom(agent) and not is_nil(agent) do
    case AgentDescriptor.for_agent(agent) do
      {:ok, descriptor} -> {:ok, descriptor.address}
      {:error, %Error{} = error} -> {:error, error}
    end
  end

  defp target_address(_target),
    do: {:error, Error.not_sent(:validation, :invalid_runtime_info_target)}

  @spec fetch_local_agent(String.t()) :: {:ok, AgentDescriptor.t()} | {:error, Error.t()}
  defp fetch_local_agent(address) do
    case ConnectionRegistry.local_agent(address) do
      {:ok, %AgentDescriptor{} = descriptor} ->
        {:ok, descriptor}

      :error ->
        {:error, Error.not_sent(:routing, :unknown_local_agent)}

      {:error, %Error{} = error} ->
        {:error, error}
    end
  end

  @spec instance_ref(module() | Spectre.AgentRef.t(), term()) ::
          {:ok, Ref.t()} | {:error, Error.t()}
  defp instance_ref(agent, subject) do
    {:ok, Ref.new(agent, subject)}
  rescue
    _exception -> {:error, Error.not_sent(:validation, :invalid_runtime_info_subject)}
  end

  @spec lookup(Ref.t(), atom()) :: {:ok, pid()} | {:error, Error.t()}
  defp lookup(ref, registry) do
    case Spectre.Instance.Registry.lookup_ref(ref, registry) do
      {:ok, pid} -> {:ok, pid}
      {:error, :instance_not_found} -> {:error, Error.not_sent(:routing, :instance_not_found)}
    end
  rescue
    ArgumentError -> {:error, Error.not_sent(:routing, :instance_registry_unavailable)}
  catch
    :exit, _reason -> {:error, Error.not_sent(:routing, :instance_registry_unavailable)}
  end
end

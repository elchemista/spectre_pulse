defmodule Spectre.Pulse.InstanceTarget do
  @moduledoc false

  alias Spectre.Instance.Ref
  alias Spectre.Pulse.Address
  alias Spectre.Pulse.AgentDescriptor
  alias Spectre.Pulse.Connection
  alias Spectre.Pulse.ConnectionRegistry
  alias Spectre.Pulse.Error

  @enforce_keys [:agent, :address, :ref, :pid]
  defstruct [:agent, :address, :ref, :pid]

  @type t :: %__MODULE__{
          agent: module() | Spectre.AgentRef.t(),
          address: String.t(),
          ref: Ref.t(),
          pid: pid()
        }

  @type target :: module() | Spectre.AgentRef.t() | String.t()
  @type error_context :: :runtime_info | :operations | :studio

  @doc false
  @spec resolve(target(), term(), term() | nil, atom(), String.t(), error_context()) ::
          {:ok, t()} | {:error, Error.t()}
  def resolve(target, subject, connection_id, registry, scope, error_context)
      when is_atom(registry) and is_binary(scope) and
             error_context in [:runtime_info, :operations, :studio] do
    with {:ok, agent, address} <- authorize(target, connection_id, scope, error_context),
         {:ok, ref} <- instance_ref(agent, subject, error_context),
         {:ok, pid} <- lookup(ref, registry) do
      {:ok, %__MODULE__{agent: agent, address: address, ref: ref, pid: pid}}
    end
  end

  @spec authorize(target(), term() | nil, String.t(), error_context()) ::
          {:ok, module() | Spectre.AgentRef.t(), String.t()} | {:error, Error.t()}
  defp authorize(target, nil, _scope, error_context),
    do: resolve_local_target(target, error_context)

  defp authorize(target, connection_id, scope, error_context) do
    with {:ok, connection} <- fetch_connection(connection_id),
         :ok <- require_scope(connection, scope),
         {:ok, address} <- target_address(target, error_context),
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

  @spec resolve_local_target(target(), error_context()) ::
          {:ok, module() | Spectre.AgentRef.t(), String.t()} | {:error, Error.t()}
  defp resolve_local_target(address, _error_context) when is_binary(address) do
    with {:ok, canonical} <- Address.normalize(address),
         {:ok, descriptor} <- fetch_local_agent(canonical) do
      {:ok, descriptor.module, canonical}
    end
  end

  defp resolve_local_target(%Spectre.AgentRef{} = ref, _error_context),
    do: {:ok, ref, "agent-ref:" <> ref.id}

  defp resolve_local_target(agent, _error_context) when is_atom(agent) and not is_nil(agent) do
    address =
      case AgentDescriptor.for_agent(agent) do
        {:ok, descriptor} -> descriptor.address
        {:error, _error} -> "agent:" <> Atom.to_string(agent)
      end

    {:ok, agent, address}
  end

  defp resolve_local_target(_target, error_context),
    do: {:error, Error.not_sent(:validation, invalid_reason(error_context, :target))}

  @spec target_address(target(), error_context()) :: {:ok, String.t()} | {:error, Error.t()}
  defp target_address(address, _error_context) when is_binary(address),
    do: Address.normalize(address)

  defp target_address(agent, _error_context) when is_atom(agent) and not is_nil(agent) do
    case AgentDescriptor.for_agent(agent) do
      {:ok, descriptor} -> {:ok, descriptor.address}
      {:error, %Error{} = error} -> {:error, error}
    end
  end

  defp target_address(_target, error_context),
    do: {:error, Error.not_sent(:validation, invalid_reason(error_context, :target))}

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

  @spec instance_ref(module() | Spectre.AgentRef.t(), term(), error_context()) ::
          {:ok, Ref.t()} | {:error, Error.t()}
  defp instance_ref(agent, subject, error_context) do
    {:ok, Ref.new(agent, subject)}
  rescue
    _exception ->
      {:error, Error.not_sent(:validation, invalid_reason(error_context, :subject))}
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

  @spec invalid_reason(error_context(), :target | :subject) :: atom()
  defp invalid_reason(:runtime_info, :target), do: :invalid_runtime_info_target
  defp invalid_reason(:runtime_info, :subject), do: :invalid_runtime_info_subject
  defp invalid_reason(:operations, :target), do: :invalid_operations_target
  defp invalid_reason(:operations, :subject), do: :invalid_operations_subject
  defp invalid_reason(:studio, :target), do: :invalid_studio_target
  defp invalid_reason(:studio, :subject), do: :invalid_studio_subject
end

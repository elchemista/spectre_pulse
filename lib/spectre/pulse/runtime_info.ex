defmodule Spectre.Pulse.RuntimeInfo do
  @moduledoc """
  Bounded, transport-safe OTP process information for a live Agent Instance.

  Pulse resolves the Instance and samples the BEAM process on demand. It does
  not retain metrics, read GenServer state, or expose mailbox contents and the
  process dictionary. Remote callers should pass a live Pulse connection so
  the standard `agent.runtime.read` scope and exposed-Agent boundary are
  enforced before the Instance is resolved.
  """

  alias Spectre.Instance.Ref
  alias Spectre.Instance.Registry, as: InstanceRegistry
  alias Spectre.Pulse.Address
  alias Spectre.Pulse.AgentDescriptor
  alias Spectre.Pulse.Connection
  alias Spectre.Pulse.ConnectionRegistry
  alias Spectre.Pulse.Error

  @scope "agent.runtime.read"
  @fields [
    :registered_name,
    :status,
    :initial_call,
    :current_function,
    :message_queue_len,
    :links,
    :monitors,
    :monitored_by,
    :trap_exit,
    :error_handler,
    :priority,
    :group_leader,
    :total_heap_size,
    :heap_size,
    :stack_size,
    :reductions,
    :garbage_collection,
    :suspending,
    :memory
  ]

  @type target :: module() | Spectre.AgentRef.t() | String.t()

  @doc "Returns the authorization scope required by remote runtime inspection."
  @spec scope() :: String.t()
  def scope, do: @scope

  @doc """
  Resolves an Agent Instance and returns a current OTP process snapshot.

  Pass `connection: connection_id` for a remotely initiated call. Pulse then
  verifies that the connection has `agent.runtime.read` and that the requested
  Agent is exposed by that connection. Trusted host code may omit it.
  """
  @spec fetch(target(), term(), keyword()) :: {:ok, map()} | {:error, Error.t()}
  def fetch(target, subject, opts \\ []) do
    with {:ok, opts} <- options(opts),
         {:ok, agent, address} <- authorize_target(target, opts),
         {:ok, ref} <- instance_ref(agent, subject),
         {:ok, pid} <- lookup(ref, opts),
         {:ok, process} <- process_info(pid, Keyword.get(opts, :fields, @fields)) do
      {:ok,
       %{
         "schema_version" => 1,
         "capability" => "agent.runtime.info",
         "scope" => @scope,
         "agent_address" => address,
         "instance_ref" => ref.key,
         "node" => Atom.to_string(node(pid)),
         "pid" => inspect(pid),
         "alive" => true,
         "sampled_at_unix_ms" => System.system_time(:millisecond),
         "process" => process
       }}
    end
  rescue
    exception ->
      {:error, Error.not_sent(:inbound, {:runtime_info_exception, exception}, cause: exception)}
  catch
    kind, reason ->
      {:error, Error.not_sent(:inbound, {:runtime_info_exit, kind, reason})}
  end

  @doc "Returns a bounded process snapshot for trusted host-side tooling."
  @spec process_info(pid(), [atom() | String.t()]) :: {:ok, map()} | {:error, Error.t()}
  def process_info(pid, fields \\ @fields)

  def process_info(pid, fields) when is_pid(pid) do
    with {:ok, fields} <- fields(fields) do
      case Process.info(pid, fields) do
        nil ->
          {:error, Error.not_sent(:routing, :agent_instance_not_alive)}

        values ->
          {:ok,
           Map.new(values, fn
             {:registered_name, []} -> {"registered_name", nil}
             {key, value} -> {Atom.to_string(key), wire_value(value)}
           end)}
      end
    end
  rescue
    ArgumentError -> {:error, Error.not_sent(:validation, :invalid_runtime_process)}
  end

  def process_info(pid, _fields),
    do: {:error, Error.not_sent(:validation, {:invalid_runtime_process, pid})}

  @spec options(term()) :: {:ok, keyword()} | {:error, Error.t()}
  defp options(opts) when is_list(opts) do
    if Keyword.keyword?(opts) and
         Keyword.keys(opts) -- [:connection, :fields, :instance_registry] == [] do
      {:ok, opts}
    else
      {:error, Error.not_sent(:validation, {:invalid_runtime_info_options, opts})}
    end
  end

  defp options(opts),
    do: {:error, Error.not_sent(:validation, {:invalid_runtime_info_options, opts})}

  @spec authorize_target(target(), keyword()) ::
          {:ok, module() | Spectre.AgentRef.t(), String.t()} | {:error, Error.t()}
  defp authorize_target(target, opts) do
    case Keyword.fetch(opts, :connection) do
      :error -> resolve_local_target(target)
      {:ok, connection_id} -> authorize_connection_target(connection_id, target)
    end
  end

  @spec authorize_connection_target(term(), target()) ::
          {:ok, module(), String.t()} | {:error, Error.t()}
  defp authorize_connection_target(connection_id, target) do
    with {:ok, %Connection{} = connection} <- fetch_connection(connection_id),
         :ok <- require_scope(connection),
         {:ok, address} <- target_address(target),
         true <- address in connection.local_agent_addresses,
         {:ok, %AgentDescriptor{module: module}} <- ConnectionRegistry.local_agent(address) do
      {:ok, module, address}
    else
      false ->
        {:error,
         Error.not_sent(:authorization, {:agent_not_exposed_on_connection, connection_id})}

      :error ->
        {:error, Error.not_sent(:routing, {:unknown_local_agent, target})}

      {:error, %Error{} = error} ->
        {:error, error}
    end
  end

  @spec fetch_connection(term()) :: {:ok, Connection.t()} | {:error, Error.t()}
  defp fetch_connection(connection_id) do
    case ConnectionRegistry.fetch(connection_id) do
      {:ok, %Connection{} = connection} -> {:ok, connection}
      :error -> {:error, Error.not_sent(:routing, {:unknown_connection, connection_id})}
    end
  end

  @spec require_scope(Connection.t()) :: :ok | {:error, Error.t()}
  defp require_scope(%Connection{} = connection) do
    if @scope in connection.granted_scopes do
      :ok
    else
      {:error,
       Error.not_sent(:authorization, {:connection_scope_required, @scope},
         details: %{connection_id: connection.id}
       )}
    end
  end

  @spec resolve_local_target(target()) ::
          {:ok, module() | Spectre.AgentRef.t(), String.t()} | {:error, Error.t()}
  defp resolve_local_target(target) when is_binary(target) do
    with {:ok, address} <- target_address(target),
         {:ok, %AgentDescriptor{module: module}} <- ConnectionRegistry.local_agent(address) do
      {:ok, module, address}
    else
      _other -> {:error, Error.not_sent(:routing, {:unknown_local_agent, target})}
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

  defp resolve_local_target(target),
    do: {:error, Error.not_sent(:validation, {:invalid_runtime_info_target, target})}

  @spec target_address(target()) :: {:ok, String.t()} | {:error, Error.t()}
  defp target_address(address) when is_binary(address), do: Address.normalize(address)

  defp target_address(agent) when is_atom(agent) and not is_nil(agent) do
    case AgentDescriptor.for_agent(agent) do
      {:ok, descriptor} -> {:ok, descriptor.address}
      {:error, %Error{} = error} -> {:error, error}
    end
  end

  defp target_address(target),
    do: {:error, Error.not_sent(:validation, {:invalid_runtime_info_target, target})}

  @spec instance_ref(module() | Spectre.AgentRef.t(), term()) ::
          {:ok, Ref.t()} | {:error, Error.t()}
  defp instance_ref(agent, subject) do
    {:ok, Ref.new(agent, subject)}
  rescue
    exception ->
      {:error,
       Error.not_sent(:validation, {:invalid_runtime_info_subject, Exception.message(exception)})}
  end

  @spec lookup(Ref.t(), keyword()) :: {:ok, pid()} | {:error, Error.t()}
  defp lookup(ref, opts) do
    registry = Keyword.get(opts, :instance_registry, InstanceRegistry)

    if is_atom(registry) do
      case InstanceRegistry.lookup_ref(ref, registry) do
        {:ok, pid} -> {:ok, pid}
        {:error, :instance_not_found} -> {:error, Error.not_sent(:routing, :instance_not_found)}
      end
    else
      {:error, Error.not_sent(:validation, {:invalid_instance_registry, registry})}
    end
  end

  @spec fields(term()) :: {:ok, [atom()]} | {:error, Error.t()}
  defp fields(fields) when is_list(fields) do
    normalized = Enum.map(fields, &normalize_field/1)

    cond do
      normalized == [] ->
        {:error, Error.not_sent(:validation, :runtime_info_fields_required)}

      Enum.any?(normalized, &is_nil/1) ->
        {:error, Error.not_sent(:authorization, :runtime_info_field_not_allowed)}

      true ->
        {:ok, Enum.uniq(normalized)}
    end
  end

  defp fields(_fields),
    do: {:error, Error.not_sent(:validation, :invalid_runtime_info_fields)}

  defp normalize_field(field) when field in @fields, do: field

  defp normalize_field(field) when is_binary(field) do
    Enum.find(@fields, &(Atom.to_string(&1) == field))
  end

  defp normalize_field(_field), do: nil

  @spec wire_value(term()) :: term()
  defp wire_value(value) when is_pid(value) or is_port(value) or is_reference(value),
    do: inspect(value)

  defp wire_value(nil), do: nil
  defp wire_value(value) when is_atom(value), do: Atom.to_string(value)
  defp wire_value(value) when is_binary(value) or is_number(value) or is_boolean(value), do: value

  defp wire_value(value) when is_tuple(value),
    do: value |> Tuple.to_list() |> Enum.map(&wire_value/1)

  defp wire_value(value) when is_list(value), do: Enum.map(value, &wire_value/1)

  defp wire_value(value) when is_map(value) do
    Map.new(value, fn {key, item} -> {wire_key(key), wire_value(item)} end)
  end

  defp wire_value(value), do: inspect(value, limit: 20, printable_limit: 2_000)

  defp wire_key(key) when is_atom(key), do: Atom.to_string(key)
  defp wire_key(key) when is_binary(key), do: key
  defp wire_key(key), do: inspect(key, limit: 10, printable_limit: 200)
end

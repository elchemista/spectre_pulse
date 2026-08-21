defmodule Spectre.Pulse.ConnectionRegistry do
  @moduledoc false

  use GenServer

  alias Spectre.Pulse.AgentDescriptor
  alias Spectre.Pulse.Connection
  alias Spectre.Pulse.ConnectionSpec
  alias Spectre.Pulse.Error

  @typep state :: %{
           configuration_owner: pid() | nil,
           configuration_monitor: reference() | nil,
           specs: %{optional(term()) => ConnectionSpec.t()},
           local_agents: %{optional(String.t()) => AgentDescriptor.t()},
           connections: %{optional(term()) => Connection.t()},
           monitors: %{optional(reference()) => term()},
           connection_monitors: %{optional(term()) => reference()}
         }

  @doc false
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc false
  @spec configure(pid(), [ConnectionSpec.t()], [AgentDescriptor.t()]) ::
          :ok | {:error, Error.t()}
  def configure(owner, specs, local_agents),
    do: call({:configure, owner, specs, local_agents})

  @doc false
  @spec clear_configuration(pid()) :: :ok
  def clear_configuration(owner), do: call({:clear_configuration, owner})

  @doc "Returns configured connection specs."
  @spec specs() :: [ConnectionSpec.t()]
  def specs, do: call(:specs)

  @doc "Returns one configured connection spec."
  @spec fetch_spec(term()) :: {:ok, ConnectionSpec.t()} | :error
  def fetch_spec(id), do: call({:fetch_spec, id})

  @doc "Returns the discovered local Agent catalog."
  @spec local_agents() :: [AgentDescriptor.t()]
  def local_agents, do: call(:local_agents)

  @doc "Returns local Agents exposed by one connection spec."
  @spec exposed_agents(term()) :: {:ok, [AgentDescriptor.t()]} | :error
  def exposed_agents(spec_id), do: call({:exposed_agents, spec_id})

  @doc "Registers an authenticated live connection."
  @spec open(term(), map() | keyword()) :: {:ok, Connection.t()} | {:error, Error.t()}
  def open(spec_id, attrs), do: call({:open, spec_id, attrs})

  @doc "Returns every live connection sorted by id."
  @spec connections() :: [Connection.t()]
  def connections, do: call(:connections)

  @doc "Returns one live connection."
  @spec fetch(term()) :: {:ok, Connection.t()} | :error
  def fetch(id), do: call({:fetch, id})

  @doc "Updates the last observation time for one live connection."
  @spec touch(term(), DateTime.t()) :: {:ok, Connection.t()} | :error | {:error, Error.t()}
  def touch(id, observed_at \\ DateTime.utc_now()), do: call({:touch, id, observed_at})

  @doc "Removes one live connection without terminating its transport owner."
  @spec close(term()) :: :ok
  def close(id), do: call({:close, id})

  @doc "Returns remote Agent descriptors grouped across live connections."
  @spec remote_agents() :: [AgentDescriptor.t()]
  def remote_agents, do: call(:remote_agents)

  @doc false
  @impl GenServer
  def init(_opts) do
    {:ok,
     %{
       configuration_owner: nil,
       configuration_monitor: nil,
       specs: %{},
       local_agents: %{},
       connections: %{},
       monitors: %{},
       connection_monitors: %{}
     }}
  end

  @doc false
  @impl GenServer
  def handle_call({:configure, owner, specs, local_agents}, _from, state) do
    case normalize_configuration(owner, specs, local_agents, state) do
      {:ok, configuration} ->
        state = state |> reset_configuration() |> replace_configuration_monitor(owner)
        {:reply, :ok, Map.merge(state, configuration)}

      {:error, %Error{} = error} ->
        {:reply, {:error, error}, state}
    end
  end

  def handle_call({:clear_configuration, owner}, _from, %{configuration_owner: owner} = state) do
    state = reset_configuration(state)
    {:reply, :ok, state}
  end

  def handle_call({:clear_configuration, _owner}, _from, state), do: {:reply, :ok, state}

  def handle_call(:specs, _from, state),
    do: {:reply, state.specs |> Map.values() |> sort_by_id(), state}

  def handle_call({:fetch_spec, id}, _from, state),
    do: {:reply, Map.fetch(state.specs, id), state}

  def handle_call(:local_agents, _from, state),
    do: {:reply, state.local_agents |> Map.values() |> Enum.sort_by(& &1.address), state}

  def handle_call({:exposed_agents, spec_id}, _from, state) do
    result =
      with {:ok, spec} <- Map.fetch(state.specs, spec_id) do
        {:ok, Enum.map(spec.agent_addresses, &Map.fetch!(state.local_agents, &1))}
      end

    {:reply, result, state}
  end

  def handle_call({:open, spec_id, attrs}, _from, state) do
    with {:ok, spec} <- fetch_connection_spec(state, spec_id),
         {:ok, connection} <- Connection.new(spec, attrs),
         :ok <- ensure_connection_id_available(state, connection.id) do
      monitor = Process.monitor(connection.owner)

      next = %{
        state
        | connections: Map.put(state.connections, connection.id, connection),
          monitors: Map.put(state.monitors, monitor, connection.id),
          connection_monitors: Map.put(state.connection_monitors, connection.id, monitor)
      }

      {:reply, {:ok, connection}, next}
    else
      :error ->
        {:reply, {:error, Error.not_sent(:routing, {:unknown_connection_spec, spec_id})}, state}

      {:error, %Error{} = error} ->
        {:reply, {:error, error}, state}
    end
  end

  def handle_call(:connections, _from, state),
    do: {:reply, state.connections |> Map.values() |> sort_by_id(), state}

  def handle_call({:fetch, id}, _from, state),
    do: {:reply, Map.fetch(state.connections, id), state}

  def handle_call({:touch, id, observed_at}, _from, state) do
    case {Map.fetch(state.connections, id), observed_at} do
      {{:ok, connection}, %DateTime{}} ->
        connection = %{connection | last_seen_at: observed_at}
        {:reply, {:ok, connection}, put_in(state, [:connections, id], connection)}

      {:error, _value} ->
        {:reply, :error, state}

      {{:ok, _connection}, value} ->
        {:reply, {:error, Error.not_sent(:validation, {:invalid_connection_last_seen_at, value})},
         state}
    end
  end

  def handle_call({:close, id}, _from, state),
    do: {:reply, :ok, remove_connection(state, id)}

  def handle_call(:remote_agents, _from, state) do
    agents =
      state.connections
      |> Map.values()
      |> Enum.flat_map(& &1.remote_agents)
      |> Enum.reduce(%{}, &Map.put_new(&2, &1.address, &1))
      |> Map.values()
      |> Enum.sort_by(& &1.address)

    {:reply, agents, state}
  end

  @doc false
  @impl GenServer
  def handle_info({:DOWN, monitor, :process, _pid, _reason}, state) do
    if monitor == state.configuration_monitor do
      {:noreply, reset_configuration(state, false)}
    else
      case Map.fetch(state.monitors, monitor) do
        {:ok, connection_id} -> {:noreply, remove_connection(state, connection_id, false)}
        :error -> {:noreply, state}
      end
    end
  end

  @spec normalize_configuration(pid(), [term()], [term()], state()) ::
          {:ok, map()} | {:error, Error.t()}
  defp normalize_configuration(owner, specs, local_agents, state)
       when is_pid(owner) and is_list(specs) and is_list(local_agents) do
    cond do
      not Process.alive?(owner) ->
        {:error, Error.not_sent(:validation, :connection_configuration_owner_not_alive)}

      state.configuration_owner not in [nil, owner] and
          Process.alive?(state.configuration_owner) ->
        {:error,
         Error.not_sent(
           :validation,
           {:connection_registry_already_configured, state.configuration_owner}
         )}

      true ->
        with {:ok, agents} <- normalize_agents(local_agents),
             {:ok, specs} <- normalize_specs(specs, agents) do
          {:ok, %{configuration_owner: owner, specs: specs, local_agents: agents}}
        end
    end
  end

  defp normalize_configuration(owner, specs, local_agents, _state),
    do:
      {:error,
       Error.not_sent(
         :validation,
         {:invalid_connection_registry_configuration, owner, specs, local_agents}
       )}

  @spec normalize_agents([term()]) ::
          {:ok, %{optional(String.t()) => AgentDescriptor.t()}} | {:error, Error.t()}
  defp normalize_agents(agents) do
    Enum.reduce_while(agents, {:ok, %{}}, fn agent, {:ok, values} ->
      with {:ok, descriptor} <- AgentDescriptor.new(agent),
           false <- Map.has_key?(values, descriptor.address) do
        {:cont, {:ok, Map.put(values, descriptor.address, descriptor)}}
      else
        true ->
          {:halt,
           {:error, Error.not_sent(:validation, {:duplicate_local_agent, agent_address(agent)})}}

        {:error, %Error{} = error} ->
          {:halt, {:error, error}}
      end
    end)
  end

  @spec normalize_specs([term()], map()) ::
          {:ok, %{optional(term()) => ConnectionSpec.t()}} | {:error, Error.t()}
  defp normalize_specs(specs, agents) do
    descriptors = Map.values(agents)

    Enum.reduce_while(specs, {:ok, %{}}, fn spec, {:ok, values} ->
      with {:ok, spec} <- ConnectionSpec.new(spec),
           {:ok, spec} <- ConnectionSpec.resolve_agents(spec, descriptors),
           false <- Map.has_key?(values, spec.id) do
        {:cont, {:ok, Map.put(values, spec.id, spec)}}
      else
        true ->
          {:halt,
           {:error, Error.not_sent(:validation, {:duplicate_connection_spec, spec_id(spec)})}}

        {:error, %Error{} = error} ->
          {:halt, {:error, error}}
      end
    end)
  end

  @spec fetch_connection_spec(state(), term()) :: {:ok, ConnectionSpec.t()} | :error
  defp fetch_connection_spec(state, id), do: Map.fetch(state.specs, id)

  @spec ensure_connection_id_available(state(), term()) :: :ok | {:error, Error.t()}
  defp ensure_connection_id_available(state, id) do
    if Map.has_key?(state.connections, id),
      do: {:error, Error.not_sent(:validation, {:connection_id_already_registered, id})},
      else: :ok
  end

  @spec replace_configuration_monitor(state(), pid()) :: state()
  defp replace_configuration_monitor(state, owner) do
    if is_reference(state.configuration_monitor) do
      Process.demonitor(state.configuration_monitor, [:flush])
    end

    %{state | configuration_monitor: Process.monitor(owner)}
  end

  @spec reset_configuration(state(), boolean()) :: state()
  defp reset_configuration(state, demonitor? \\ true) do
    state = Enum.reduce(Map.keys(state.connections), state, &remove_connection(&2, &1))

    if demonitor? and is_reference(state.configuration_monitor) do
      Process.demonitor(state.configuration_monitor, [:flush])
    end

    %{
      state
      | configuration_owner: nil,
        configuration_monitor: nil,
        specs: %{},
        local_agents: %{}
    }
  end

  @spec remove_connection(state(), term(), boolean()) :: state()
  defp remove_connection(state, id, demonitor? \\ true) do
    case Map.pop(state.connection_monitors, id) do
      {nil, _monitors} ->
        %{state | connections: Map.delete(state.connections, id)}

      {monitor, connection_monitors} ->
        if demonitor?, do: Process.demonitor(monitor, [:flush])

        %{
          state
          | connections: Map.delete(state.connections, id),
            connection_monitors: connection_monitors,
            monitors: Map.delete(state.monitors, monitor)
        }
    end
  end

  @spec sort_by_id([map()]) :: [map()]
  defp sort_by_id(values), do: Enum.sort_by(values, &inspect(&1.id))

  @spec agent_address(term()) :: term()
  defp agent_address(%AgentDescriptor{address: address}), do: address

  defp agent_address(value) when is_map(value),
    do: Map.get(value, :address, Map.get(value, "address"))

  defp agent_address(value), do: value

  @spec spec_id(term()) :: term()
  defp spec_id(%ConnectionSpec{id: id}), do: id
  defp spec_id(value) when is_list(value), do: Keyword.get(value, :id, value)
  defp spec_id(value) when is_map(value), do: Map.get(value, :id, Map.get(value, "id"))
  defp spec_id(value), do: value

  @spec call(term()) :: term()
  defp call(message) do
    GenServer.call(__MODULE__, message)
  catch
    :exit, reason ->
      {:error, Error.not_sent(:routing, {:connection_registry_unavailable, reason})}
  end
end

defmodule Spectre.Pulse.Fabric do
  @moduledoc """
  Runtime route table and transport-driver registry.

  Applications register transport implementations and connect physical routes.
  Agents never query this process directly and never select a binding: the
  discovery layer combines this route table with local subscriptions and
  application directories.

  The Fabric is technical, ephemeral infrastructure. It is not an Agent
  registry, a message broker, or authoritative application state.
  """

  use GenServer

  alias Spectre.Pulse.Address
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Route

  @type transport_name :: atom()
  @type transport_entry :: %{
          module: module(),
          priority: integer(),
          metadata: map()
        }
  @type registration_option ::
          {:priority, integer()}
          | {:metadata, map()}
          | {:replace, boolean()}
  @type connection_option ::
          {:id, term()}
          | {:priority, integer()}
          | {:metadata, map()}
          | {:owner, pid()}
  @typep route_entries :: %{optional(term()) => Route.t()}
  @typep state :: %{
           transports: %{optional(transport_name()) => transport_entry()},
           routes: %{optional(String.t()) => route_entries()},
           route_monitors: %{optional(term()) => reference()},
           monitor_routes: %{optional(reference()) => term()}
         }

  @built_in_transports %{
    local: Spectre.Pulse.Transports.Local,
    pub_sub: Spectre.Pulse.Transports.PubSub,
    websocket: Spectre.Pulse.Transports.WebSocket,
    rest: Spectre.Pulse.Transports.REST,
    beam_node: Spectre.Pulse.Transports.Node
  }

  @registration_options [:metadata, :priority, :replace]
  @connection_options [:id, :metadata, :owner, :priority]

  @doc false
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Registers a transport driver under an application-local name.

  Registering the same name and module is idempotent. Replacing a name with a
  different module requires `replace: true`. The remaining options set the
  driver's default `priority` and technical `metadata`.
  """
  @spec register_transport(transport_name(), module(), [registration_option()]) ::
          :ok | {:error, Error.t()}
  def register_transport(name, module, opts \\ []) do
    call({:register_transport, name, module, opts})
  end

  @doc "Returns the registered transport drivers."
  @spec transports() ::
          %{optional(transport_name()) => transport_entry()} | {:error, Error.t()}
  def transports, do: call(:transports)

  @doc """
  Connects a physical target to a logical Agent address.

  `transport_name` identifies a registered driver. The resulting Route belongs
  to Pulse infrastructure and is discovered automatically during delivery.
  `owner` may identify the process that owns a connection; Pulse then removes
  its route automatically when that process exits.
  """
  @spec connect(String.t(), transport_name(), term(), [connection_option()]) ::
          {:ok, Route.t()} | {:error, Error.t()}
  def connect(address, transport_name, target, opts \\ []) do
    call({:connect, address, transport_name, target, opts})
  end

  @doc "Removes one ephemeral connected route."
  @spec disconnect(term()) :: :ok | {:error, Error.t()}
  def disconnect(route_id), do: call({:disconnect, route_id})

  @doc "Returns currently connected routes for one canonical address."
  @spec routes(String.t()) :: [Route.t()] | {:error, Error.t()}
  def routes(address) do
    case Address.normalize(address) do
      {:ok, canonical} -> call({:routes, canonical})
      {:error, _error} -> []
    end
  end

  @doc false
  @spec init(keyword()) :: {:ok, state()}
  @impl GenServer
  def init(_opts) do
    transports =
      Map.new(@built_in_transports, fn {name, module} ->
        {name, %{module: module, priority: default_priority(name), metadata: %{}}}
      end)

    {:ok,
     %{
       transports: transports,
       routes: %{},
       route_monitors: %{},
       monitor_routes: %{}
     }}
  end

  @doc false
  @spec handle_call(term(), GenServer.from(), state()) :: {:reply, term(), state()}
  @impl GenServer
  def handle_call({:register_transport, name, module, opts}, _from, state) do
    with :ok <- validate_transport_name(name),
         :ok <- validate_transport(module),
         :ok <- validate_registration_options(opts),
         :ok <- ensure_replace_allowed(state.transports, name, module, opts) do
      entry = transport_entry(Map.get(state.transports, name), module, opts)

      {:reply, :ok, put_in(state, [:transports, name], entry)}
    else
      {:error, %Error{} = error} -> {:reply, {:error, error}, state}
    end
  end

  def handle_call(:transports, _from, state), do: {:reply, state.transports, state}

  def handle_call({:connect, address, name, target, opts}, _from, state) do
    with {:ok, canonical} <- Address.normalize(address),
         {:ok, entry} <- fetch_transport(state.transports, name),
         :ok <- validate_connection_options(opts),
         {:ok, route} <- build_route(canonical, name, target, entry, opts),
         {:ok, owner} <- connection_owner(target, opts) do
      state = remove_route(state, route.id)

      routes =
        state.routes
        |> Map.get(canonical, %{})
        |> Map.put(route.id, route)

      state =
        state
        |> put_in([:routes, canonical], routes)
        |> monitor_route(route.id, owner)

      {:reply, {:ok, route}, state}
    else
      {:error, %Error{} = error} -> {:reply, {:error, error}, state}
    end
  end

  def handle_call({:disconnect, route_id}, _from, state) do
    {:reply, :ok, remove_route(state, route_id)}
  end

  def handle_call({:routes, address}, _from, state) do
    routes =
      state.routes
      |> Map.get(address, %{})
      |> Map.values()
      |> Enum.sort_by(& &1.priority)

    {:reply, routes, state}
  end

  @doc false
  @spec handle_info(term(), state()) :: {:noreply, state()}
  @impl GenServer
  def handle_info({:DOWN, monitor, :process, _owner, _reason}, state) do
    case Map.fetch(state.monitor_routes, monitor) do
      {:ok, route_id} -> {:noreply, remove_route(state, route_id, false)}
      :error -> {:noreply, state}
    end
  end

  @spec build_route(String.t(), transport_name(), term(), transport_entry(), keyword()) ::
          {:ok, Route.t()} | {:error, Error.t()}
  defp build_route(address, name, target, entry, opts) do
    metadata =
      entry.metadata
      |> Map.merge(Keyword.get(opts, :metadata, %{}))
      |> Map.put_new(:transport_name, name)
      |> Map.put_new(:discovered_by, :fabric)

    Route.new(
      id: Keyword.get(opts, :id, Spectre.Identity.uuid7()),
      address: address,
      transport: entry.module,
      target: target,
      priority: Keyword.get(opts, :priority, entry.priority),
      metadata: metadata
    )
  end

  @spec fetch_transport(
          %{optional(transport_name()) => transport_entry()},
          transport_name()
        ) :: {:ok, transport_entry()} | {:error, Error.t()}
  defp fetch_transport(transports, name) do
    case Map.fetch(transports, name) do
      {:ok, entry} -> {:ok, entry}
      :error -> {:error, Error.not_sent(:routing, {:unknown_transport, name})}
    end
  end

  @spec validate_transport_name(term()) :: :ok | {:error, Error.t()}
  defp validate_transport_name(name) when is_atom(name) and not is_nil(name), do: :ok

  defp validate_transport_name(name),
    do: {:error, Error.not_sent(:validation, {:invalid_transport_name, name})}

  @spec validate_transport(term()) :: :ok | {:error, Error.t()}
  defp validate_transport(module) when is_atom(module) and not is_nil(module) do
    if Code.ensure_loaded?(module) and function_exported?(module, :deliver, 3) do
      :ok
    else
      {:error, Error.not_sent(:validation, {:invalid_transport, module})}
    end
  end

  defp validate_transport(module),
    do: {:error, Error.not_sent(:validation, {:invalid_transport, module})}

  @spec validate_registration_options(term()) :: :ok | {:error, Error.t()}
  defp validate_registration_options(opts) do
    with :ok <- validate_options(opts, @registration_options) do
      validate_replace_option(opts)
    end
  end

  @spec validate_connection_options(term()) :: :ok | {:error, Error.t()}
  defp validate_connection_options(opts), do: validate_options(opts, @connection_options)

  @spec validate_options(term(), [atom()]) :: :ok | {:error, Error.t()}
  defp validate_options(opts, allowed) when is_list(opts) do
    if Keyword.keyword?(opts) do
      validate_known_options(opts, allowed)
    else
      {:error, Error.not_sent(:validation, {:invalid_transport_options, opts})}
    end
  end

  defp validate_options(opts, _allowed),
    do: {:error, Error.not_sent(:validation, {:invalid_transport_options, opts})}

  @spec validate_known_options(keyword(), [atom()]) :: :ok | {:error, Error.t()}
  defp validate_known_options(opts, allowed) do
    unknown = Keyword.keys(opts) -- allowed

    cond do
      unknown != [] ->
        {:error, Error.not_sent(:validation, {:unknown_transport_options, unknown})}

      not is_map(Keyword.get(opts, :metadata, %{})) ->
        {:error, Error.not_sent(:validation, {:invalid_transport_metadata, opts[:metadata]})}

      not is_integer(Keyword.get(opts, :priority, 100)) ->
        {:error, Error.not_sent(:validation, {:invalid_transport_priority, opts[:priority]})}

      true ->
        :ok
    end
  end

  @spec validate_replace_option(keyword()) :: :ok | {:error, Error.t()}
  defp validate_replace_option(opts) do
    case Keyword.fetch(opts, :replace) do
      :error -> :ok
      {:ok, value} when is_boolean(value) -> :ok
      {:ok, value} -> {:error, Error.not_sent(:validation, {:invalid_replace_option, value})}
    end
  end

  @spec transport_entry(transport_entry() | nil, module(), keyword()) :: transport_entry()
  defp transport_entry(existing, module, opts) do
    %{
      module: module,
      priority: Keyword.get(opts, :priority, existing_value(existing, :priority, 100)),
      metadata: Keyword.get(opts, :metadata, existing_value(existing, :metadata, %{}))
    }
  end

  @spec existing_value(map() | nil, atom(), term()) :: term()
  defp existing_value(nil, _key, default), do: default
  defp existing_value(existing, key, default), do: Map.get(existing, key, default)

  @spec ensure_replace_allowed(
          %{optional(transport_name()) => transport_entry()},
          transport_name(),
          module(),
          keyword()
        ) :: :ok | {:error, Error.t()}
  defp ensure_replace_allowed(transports, name, module, opts) do
    case Map.get(transports, name) do
      nil ->
        :ok

      %{module: ^module} ->
        :ok

      %{module: existing} ->
        if Keyword.get(opts, :replace, false) do
          :ok
        else
          {:error,
           Error.not_sent(:validation, {:transport_name_already_registered, name, existing})}
        end
    end
  end

  @spec connection_owner(term(), keyword()) :: {:ok, pid() | nil} | {:error, Error.t()}
  defp connection_owner(target, opts) do
    owner =
      if Keyword.has_key?(opts, :owner),
        do: Keyword.get(opts, :owner),
        else: infer_connection_owner(target)

    validate_connection_owner(owner)
  end

  @spec infer_connection_owner(term()) :: pid() | nil
  defp infer_connection_owner(target) when is_pid(target), do: target
  defp infer_connection_owner(%{connection: connection}) when is_pid(connection), do: connection
  defp infer_connection_owner({_module, connection}) when is_pid(connection), do: connection
  defp infer_connection_owner(_target), do: nil

  @spec validate_connection_owner(term()) :: {:ok, pid() | nil} | {:error, Error.t()}
  defp validate_connection_owner(nil), do: {:ok, nil}

  defp validate_connection_owner(owner) when is_pid(owner) do
    if Process.alive?(owner) do
      {:ok, owner}
    else
      {:error, Error.not_sent(:routing, :connection_owner_not_alive)}
    end
  end

  defp validate_connection_owner(owner),
    do: {:error, Error.not_sent(:validation, {:invalid_connection_owner, owner})}

  @spec monitor_route(state(), term(), pid() | nil) :: state()
  defp monitor_route(state, _route_id, nil), do: state

  defp monitor_route(state, route_id, owner) do
    monitor = Process.monitor(owner)

    %{
      state
      | route_monitors: Map.put(state.route_monitors, route_id, monitor),
        monitor_routes: Map.put(state.monitor_routes, monitor, route_id)
    }
  end

  @spec remove_route(state(), term(), boolean()) :: state()
  defp remove_route(state, route_id, demonitor? \\ true) do
    {monitor, route_monitors} = Map.pop(state.route_monitors, route_id)

    if demonitor? and is_reference(monitor) do
      Process.demonitor(monitor, [:flush])
    end

    routes =
      Enum.reduce(state.routes, %{}, fn {address, entries}, acc ->
        case Map.delete(entries, route_id) do
          entries when map_size(entries) == 0 -> acc
          entries -> Map.put(acc, address, entries)
        end
      end)

    %{
      state
      | routes: routes,
        route_monitors: route_monitors,
        monitor_routes: Map.delete(state.monitor_routes, monitor)
    }
  end

  @spec call(term()) :: term()
  defp call(message) do
    GenServer.call(__MODULE__, message)
  catch
    :exit, reason ->
      {:error, Error.not_sent(:routing, {:fabric_unavailable, reason})}
  end

  @spec default_priority(:local | :websocket | :beam_node | :pub_sub | :rest) ::
          non_neg_integer()
  defp default_priority(:local), do: 0
  defp default_priority(:websocket), do: 20
  defp default_priority(:beam_node), do: 30
  defp default_priority(:pub_sub), do: 40
  defp default_priority(:rest), do: 50
end

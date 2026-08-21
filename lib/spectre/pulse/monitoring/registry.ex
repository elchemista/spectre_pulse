defmodule Spectre.Pulse.Monitoring.Registry do
  @moduledoc false

  use GenServer

  alias Spectre.Pulse.Monitoring.Subscription

  @typep state :: %{
           subscriptions: %{optional(String.t()) => Subscription.t()},
           sessions: %{optional(pid()) => MapSet.t(String.t())},
           monitors: %{optional(reference()) => pid()},
           session_monitors: %{optional(pid()) => reference()}
         }

  @doc false
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc false
  @spec register(pid(), Subscription.t()) :: :ok
  def register(session, %Subscription{} = subscription),
    do: GenServer.call(__MODULE__, {:register, session, subscription})

  @doc false
  @spec unregister(pid(), String.t()) :: :ok
  def unregister(session, subscription_id),
    do: GenServer.call(__MODULE__, {:unregister, session, subscription_id})

  @doc false
  @spec unregister_session(pid()) :: :ok
  def unregister_session(session), do: GenServer.call(__MODULE__, {:unregister_session, session})

  @doc false
  @spec list(term() | :all, Subscription.kind() | :all) :: [map()]
  def list(connection_id \\ :all, kind \\ :all),
    do: GenServer.call(__MODULE__, {:list, connection_id, kind})

  @doc false
  @impl GenServer
  def init(_opts) do
    {:ok, %{subscriptions: %{}, sessions: %{}, monitors: %{}, session_monitors: %{}}}
  end

  @doc false
  @impl GenServer
  def handle_call({:register, session, subscription}, _from, state) do
    state = ensure_session_monitor(state, session)
    ids = state.sessions |> Map.fetch!(session) |> MapSet.put(subscription.id)

    {:reply, :ok,
     %{
       state
       | subscriptions: Map.put(state.subscriptions, subscription.id, subscription),
         sessions: Map.put(state.sessions, session, ids)
     }}
  end

  def handle_call({:unregister, session, subscription_id}, _from, state) do
    {:reply, :ok, remove_subscription(state, session, subscription_id)}
  end

  def handle_call({:unregister_session, session}, _from, state) do
    {:reply, :ok, remove_session(state, session)}
  end

  def handle_call({:list, connection_id, kind}, _from, state) do
    subscriptions =
      state.subscriptions
      |> Map.values()
      |> filter_connection(connection_id)
      |> filter_kind(kind)
      |> Enum.sort_by(& &1.created_at_unix_ms)
      |> Enum.map(&Subscription.to_public_map/1)

    {:reply, subscriptions, state}
  end

  @doc false
  @impl GenServer
  def handle_info({:DOWN, monitor, :process, session, _reason}, state) do
    case Map.fetch(state.monitors, monitor) do
      {:ok, ^session} -> {:noreply, remove_session(state, session, false)}
      _unknown -> {:noreply, state}
    end
  end

  @spec ensure_session_monitor(state(), pid()) :: state()
  defp ensure_session_monitor(state, session) do
    if Map.has_key?(state.sessions, session) do
      state
    else
      monitor = Process.monitor(session)

      %{
        state
        | sessions: Map.put(state.sessions, session, MapSet.new()),
          monitors: Map.put(state.monitors, monitor, session),
          session_monitors: Map.put(state.session_monitors, session, monitor)
      }
    end
  end

  @spec remove_subscription(state(), pid(), String.t()) :: state()
  defp remove_subscription(state, session, subscription_id) do
    ids = state.sessions |> Map.get(session, MapSet.new()) |> MapSet.delete(subscription_id)

    %{
      state
      | subscriptions: Map.delete(state.subscriptions, subscription_id),
        sessions: Map.put(state.sessions, session, ids)
    }
  end

  @spec remove_session(state(), pid(), boolean()) :: state()
  defp remove_session(state, session, demonitor? \\ true) do
    ids = Map.get(state.sessions, session, MapSet.new())
    subscriptions = Enum.reduce(ids, state.subscriptions, &Map.delete(&2, &1))
    {monitor, session_monitors} = Map.pop(state.session_monitors, session)

    if demonitor? and is_reference(monitor), do: Process.demonitor(monitor, [:flush])

    %{
      state
      | subscriptions: subscriptions,
        sessions: Map.delete(state.sessions, session),
        monitors:
          if(is_reference(monitor), do: Map.delete(state.monitors, monitor), else: state.monitors),
        session_monitors: session_monitors
    }
  end

  @spec filter_connection([Subscription.t()], term() | :all) :: [Subscription.t()]
  defp filter_connection(subscriptions, :all), do: subscriptions

  defp filter_connection(subscriptions, connection_id),
    do: Enum.filter(subscriptions, &(&1.connection_id == connection_id))

  @spec filter_kind([Subscription.t()], Subscription.kind() | :all) :: [Subscription.t()]
  defp filter_kind(subscriptions, :all), do: subscriptions

  defp filter_kind(subscriptions, kind) when kind in [:runtime, :operations],
    do: Enum.filter(subscriptions, &(&1.kind == kind))
end

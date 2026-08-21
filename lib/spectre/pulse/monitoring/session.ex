defmodule Spectre.Pulse.Monitoring.Session do
  @moduledoc false

  use GenServer

  alias Spectre.Pulse.Connection
  alias Spectre.Pulse.ConnectionRegistry
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Monitoring
  alias Spectre.Pulse.Monitoring.Entry
  alias Spectre.Pulse.Monitoring.Registry, as: MonitoringRegistry
  alias Spectre.Pulse.Monitoring.Subscription
  alias Spectre.Pulse.Operations
  alias Spectre.Pulse.RuntimeInfo

  @maximum_subscriptions 16
  @maximum_sink_queue 100

  @enforce_keys [:connection_id, :sink, :sink_monitor]
  defstruct [:connection_id, :sink, :sink_monitor, trusted_options: [], subscriptions: %{}]

  @type t :: %__MODULE__{
          connection_id: term(),
          sink: pid(),
          sink_monitor: reference(),
          trusted_options: keyword(),
          subscriptions: %{optional(String.t()) => Entry.t()}
        }

  @doc false
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @doc false
  @spec enable(pid(), map()) :: {:ok, map()} | {:error, Error.t()}
  def enable(session, attrs), do: GenServer.call(session, {:enable, :runtime, attrs})

  @doc false
  @spec enable_operations(pid(), map()) :: {:ok, map()} | {:error, Error.t()}
  def enable_operations(session, attrs),
    do: GenServer.call(session, {:enable, :operations, attrs})

  @doc false
  @spec disable(pid(), term()) :: {:ok, map()} | {:error, Error.t()}
  def disable(session, subscription_id),
    do: GenServer.call(session, {:disable, :runtime, subscription_id})

  @doc false
  @spec disable_operations(pid(), term()) :: {:ok, map()} | {:error, Error.t()}
  def disable_operations(session, subscription_id),
    do: GenServer.call(session, {:disable, :operations, subscription_id})

  @doc false
  @impl GenServer
  def init(opts) do
    with {:ok, connection_id} <- Keyword.fetch(opts, :connection),
         {:ok, sink} <- Keyword.fetch(opts, :sink),
         true <- is_pid(sink),
         {:ok, _connection} <- ConnectionRegistry.fetch(connection_id) do
      trusted_options =
        opts
        |> Keyword.get(:runtime, [])
        |> Keyword.put(:connection, connection_id)

      {:ok,
       %__MODULE__{
         connection_id: connection_id,
         sink: sink,
         sink_monitor: Process.monitor(sink),
         trusted_options: trusted_options
       }}
    else
      _invalid -> {:stop, :invalid_runtime_monitor_session}
    end
  end

  @doc false
  @impl GenServer
  def handle_call({:enable, kind, attrs}, _from, state) when kind in [:runtime, :operations] do
    with :ok <- ensure_capacity(state, kind),
         :ok <- authorize_stream(state.connection_id, kind),
         {:ok, subscription} <-
           Subscription.new(kind, state.connection_id, attrs, state.trusted_options),
         {:ok, snapshot} <- sample(subscription) do
      entry = schedule(subscription)
      :ok = MonitoringRegistry.register(self(), subscription)

      response = event(event_type(subscription, "enabled"), subscription, 0, snapshot, 0)
      {:reply, {:ok, response}, put_in(state.subscriptions[subscription.id], entry)}
    else
      {:error, %Error{} = error} -> {:reply, {:error, error}, state}
    end
  end

  def handle_call({:disable, kind, subscription_id}, _from, state)
      when kind in [:runtime, :operations] do
    case pop_subscription(state, subscription_id, kind) do
      {:ok, entry, next} ->
        cancel_timers(entry)
        :ok = MonitoringRegistry.unregister(self(), entry.subscription.id)
        response = event(event_type(entry.subscription, "disabled"), entry.subscription)
        {:reply, {:ok, response}, next}

      :error ->
        error = Error.not_sent(:routing, unknown_subscription_reason(kind))
        {:reply, {:error, error}, state}
    end
  end

  @doc false
  @impl GenServer
  def handle_info({:sample, subscription_id}, state) do
    case Map.fetch(state.subscriptions, subscription_id) do
      {:ok, entry} -> {:noreply, sample_and_reschedule(state, entry)}
      :error -> {:noreply, state}
    end
  end

  def handle_info({:expire, subscription_id}, state) do
    case pop_subscription(state, subscription_id) do
      {:ok, entry, next} ->
        cancel_timers(entry)
        :ok = MonitoringRegistry.unregister(self(), entry.subscription.id)

        notify(
          state.sink,
          event(event_type(entry.subscription, "expired"), entry.subscription)
        )

        {:noreply, next}

      :error ->
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, monitor, :process, sink, _reason}, state)
      when monitor == state.sink_monitor and sink == state.sink,
      do: {:stop, :normal, state}

  def handle_info(_message, state), do: {:noreply, state}

  @doc false
  @impl GenServer
  def terminate(_reason, state) do
    Enum.each(state.subscriptions, fn {_id, entry} -> cancel_timers(entry) end)

    if Process.whereis(MonitoringRegistry) do
      MonitoringRegistry.unregister_session(self())
    end

    :ok
  end

  @spec ensure_capacity(t(), Subscription.kind()) :: :ok | {:error, Error.t()}
  defp ensure_capacity(state, kind) do
    if map_size(state.subscriptions) < @maximum_subscriptions,
      do: :ok,
      else: {:error, Error.not_sent(:authorization, limit_reason(kind))}
  end

  @spec authorize_stream(term(), Subscription.kind()) :: :ok | {:error, Error.t()}
  defp authorize_stream(connection_id, kind) do
    scope = stream_scope(kind)

    case ConnectionRegistry.fetch(connection_id) do
      {:ok, %Connection{granted_scopes: scopes}} ->
        if scope in scopes,
          do: :ok,
          else: {:error, Error.not_sent(:authorization, {:connection_scope_required, scope})}

      :error ->
        {:error, Error.not_sent(:routing, :unknown_connection)}
    end
  end

  @spec sample(Subscription.t()) :: {:ok, map()} | {:error, Error.t()}
  defp sample(%Subscription{kind: :runtime} = subscription) do
    RuntimeInfo.fetch(
      subscription.agent_address,
      subscription.subject,
      subscription.sample_options
    )
  end

  defp sample(%Subscription{kind: :operations} = subscription) do
    Operations.list(
      subscription.agent_address,
      subscription.subject,
      subscription.sample_options
    )
  end

  @spec schedule(Subscription.t()) :: Entry.t()
  defp schedule(subscription) do
    %Entry{
      subscription: subscription,
      sample_timer:
        Process.send_after(self(), {:sample, subscription.id}, subscription.interval_ms),
      expiry_timer: expiry_timer(subscription)
    }
  end

  @spec expiry_timer(Subscription.t()) :: reference() | nil
  defp expiry_timer(%Subscription{duration_ms: nil}), do: nil

  defp expiry_timer(subscription),
    do: Process.send_after(self(), {:expire, subscription.id}, subscription.duration_ms)

  @spec sample_and_reschedule(t(), Entry.t()) :: t()
  defp sample_and_reschedule(state, entry) do
    if sink_saturated?(state.sink) do
      reschedule(state, %{entry | dropped_updates: entry.dropped_updates + 1})
    else
      next_sequence = entry.sequence + 1
      notify_sample(state.sink, entry, next_sequence)
      reschedule(state, %{entry | sequence: next_sequence, dropped_updates: 0})
    end
  end

  @spec notify_sample(pid(), Entry.t(), pos_integer()) :: :ok
  defp notify_sample(sink, entry, sequence) do
    message =
      case sample(entry.subscription) do
        {:ok, snapshot} ->
          event(
            event_type(entry.subscription, "update"),
            entry.subscription,
            sequence,
            snapshot,
            entry.dropped_updates
          )

        {:error, %Error{} = error} ->
          error_event(entry.subscription, sequence, error)
      end

    notify(sink, message)
  end

  @spec reschedule(t(), Entry.t()) :: t()
  defp reschedule(state, entry) do
    timer =
      Process.send_after(
        self(),
        {:sample, entry.subscription.id},
        entry.subscription.interval_ms
      )

    put_in(state.subscriptions[entry.subscription.id], %{entry | sample_timer: timer})
  end

  @spec sink_saturated?(pid()) :: boolean()
  defp sink_saturated?(sink) do
    case Process.info(sink, :message_queue_len) do
      {:message_queue_len, length} -> length >= @maximum_sink_queue
      nil -> true
    end
  end

  @spec pop_subscription(t(), term(), Subscription.kind()) :: {:ok, Entry.t(), t()} | :error
  defp pop_subscription(state, subscription_id, kind) when is_binary(subscription_id) do
    case Map.pop(state.subscriptions, subscription_id) do
      {nil, _subscriptions} ->
        :error

      {%Entry{subscription: %Subscription{kind: ^kind}} = entry, subscriptions} ->
        {:ok, entry, %{state | subscriptions: subscriptions}}

      {%Entry{}, _subscriptions} ->
        :error
    end
  end

  defp pop_subscription(_state, _subscription_id, _kind), do: :error

  @spec pop_subscription(t(), term()) :: {:ok, Entry.t(), t()} | :error
  defp pop_subscription(state, subscription_id) when is_binary(subscription_id) do
    case Map.pop(state.subscriptions, subscription_id) do
      {nil, _subscriptions} -> :error
      {%Entry{} = entry, subscriptions} -> {:ok, entry, %{state | subscriptions: subscriptions}}
    end
  end

  defp pop_subscription(_state, _subscription_id), do: :error

  @spec cancel_timers(Entry.t()) :: :ok
  defp cancel_timers(entry) do
    Process.cancel_timer(entry.sample_timer)
    if is_reference(entry.expiry_timer), do: Process.cancel_timer(entry.expiry_timer)
    :ok
  end

  @spec notify(pid(), map()) :: :ok
  defp notify(sink, event) do
    send(sink, {:spectre_pulse_monitoring, event})
    :ok
  end

  @spec event(String.t(), Subscription.t()) :: map()
  defp event(type, subscription) do
    subscription
    |> Subscription.to_public_map()
    |> Map.put("type", type)
  end

  @spec event(String.t(), Subscription.t(), non_neg_integer(), map(), non_neg_integer()) :: map()
  defp event(type, subscription, sequence, snapshot, dropped_updates) do
    type
    |> event(subscription)
    |> Map.merge(%{
      "sequence" => sequence,
      "snapshot" => snapshot,
      "dropped_updates" => dropped_updates
    })
  end

  @spec error_event(Subscription.t(), non_neg_integer(), Error.t()) :: map()
  defp error_event(subscription, sequence, error) do
    event_type(subscription, "error")
    |> event(subscription)
    |> Map.merge(%{
      "sequence" => sequence,
      "error" => %{
        "kind" => Atom.to_string(error.kind),
        "code" => reason_code(error.reason)
      }
    })
  end

  @spec reason_code(term()) :: String.t()
  defp reason_code(reason) when is_atom(reason), do: Atom.to_string(reason)
  defp reason_code(reason) when is_tuple(reason), do: reason |> elem(0) |> reason_code()
  defp reason_code(_reason), do: "monitor_sample_failed"

  @spec stream_scope(Subscription.kind()) :: String.t()
  defp stream_scope(:runtime), do: Monitoring.scope()
  defp stream_scope(:operations), do: Monitoring.operations_scope()

  @spec event_type(Subscription.t(), String.t()) :: String.t()
  defp event_type(%Subscription{kind: kind}, suffix),
    do: "agent.#{kind}.monitor.#{suffix}"

  @spec unknown_subscription_reason(Subscription.kind()) :: atom()
  defp unknown_subscription_reason(:runtime), do: :unknown_runtime_monitor_subscription
  defp unknown_subscription_reason(:operations), do: :unknown_operations_monitor_subscription

  @spec limit_reason(Subscription.kind()) :: atom()
  defp limit_reason(:runtime), do: :runtime_monitor_limit_reached
  defp limit_reason(:operations), do: :operations_monitor_limit_reached
end

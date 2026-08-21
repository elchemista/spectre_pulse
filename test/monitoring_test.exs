defmodule Spectre.Pulse.MonitoringTest.Agent do
  use Spectre.Agent
  use Spectre.Pulse

  pulsing do
    identity("spectre://monitoring/agent")
  end
end

defmodule Spectre.Pulse.MonitoringTest do
  use ExUnit.Case, async: false

  alias Spectre.Instance.Ref
  alias Spectre.Pulse.AgentDescriptor
  alias Spectre.Pulse.ConnectionRegistry
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Monitoring
  alias Spectre.Pulse.RuntimeInfo

  alias __MODULE__.Agent

  setup do
    ConnectionRegistry.clear_configuration(self())
    registry = unique_name("instance_registry")
    start_supervised!({Registry, keys: :unique, name: registry})
    subject = "studio-monitor-#{System.unique_integer([:positive])}"
    ref = Ref.new(Agent, subject)
    {:ok, _owner} = Registry.register(registry, ref.key, ref)
    descriptor = AgentDescriptor.for_agent(Agent) |> elem(1)

    on_exit(fn ->
      ConnectionRegistry.clear_configuration(self())
    end)

    %{descriptor: descriptor, registry: registry, subject: subject}
  end

  test "enables, streams and explicitly disables a runtime subscription", context do
    connection = open_connection(context.descriptor, [RuntimeInfo.scope(), Monitoring.scope()])
    session = start_session(connection.id, context.registry)

    assert {:ok, enabled} =
             Monitoring.enable(session, %{
               "request_id" => "liveview-1",
               "agent_address" => context.descriptor.address,
               "subject" => context.subject,
               "interval_ms" => 250,
               "fields" => ["memory", "status"]
             })

    assert enabled["type"] == "agent.runtime.monitor.enabled"
    assert enabled["request_id"] == "liveview-1"
    assert enabled["sequence"] == 0
    assert enabled["snapshot"]["process"] |> Map.keys() |> Enum.sort() == ["memory", "status"]
    subscription_id = enabled["subscription_id"]

    assert [%{"subscription_id" => ^subscription_id}] =
             Spectre.Pulse.monitoring_subscriptions(connection.id)

    assert_receive {:spectre_pulse_monitoring,
                    %{
                      "type" => "agent.runtime.monitor.update",
                      "subscription_id" => ^subscription_id,
                      "sequence" => 1,
                      "dropped_updates" => 0
                    }},
                   500

    assert {:ok, disabled} = Monitoring.disable(session, subscription_id)
    assert disabled["type"] == "agent.runtime.monitor.disabled"
    assert Spectre.Pulse.monitoring_subscriptions(connection.id) == []
    refute_receive {:spectre_pulse_monitoring, %{"subscription_id" => ^subscription_id}}, 350
  end

  test "automatically expires a duration-bounded subscription", context do
    connection = open_connection(context.descriptor, [RuntimeInfo.scope(), Monitoring.scope()])
    session = start_session(connection.id, context.registry)

    assert {:ok, enabled} =
             Monitoring.enable(session, %{
               "agent_address" => context.descriptor.address,
               "subject" => context.subject,
               "interval_ms" => 250,
               "duration_ms" => 300
             })

    subscription_id = enabled["subscription_id"]

    assert_receive {:spectre_pulse_monitoring,
                    %{
                      "type" => "agent.runtime.monitor.expired",
                      "subscription_id" => ^subscription_id
                    }},
                   600

    assert Spectre.Pulse.monitoring_subscriptions(connection.id) == []

    assert {:error, %Error{reason: :unknown_runtime_monitor_subscription}} =
             Monitoring.disable(session, subscription_id)
  end

  test "requires both streaming and snapshot permissions", context do
    read_only = open_connection(context.descriptor, [RuntimeInfo.scope()])
    read_session = start_session(read_only.id, context.registry)

    assert {:error,
            %Error{
              kind: :authorization,
              reason: {:connection_scope_required, "agent.runtime.stream"}
            }} = Monitoring.enable(read_session, request(context))

    :ok = ConnectionRegistry.close(read_only.id)
    stream_only = open_connection(context.descriptor, [Monitoring.scope()])
    stream_session = start_session(stream_only.id, context.registry)

    assert {:error,
            %Error{
              kind: :authorization,
              reason: {:connection_scope_required, "agent.runtime.read"}
            }} = Monitoring.enable(stream_session, request(context))
  end

  test "enforces exposure, timing limits and safe request fields", context do
    connection =
      open_connection(context.descriptor, [RuntimeInfo.scope(), Monitoring.scope()], [])

    session = start_session(connection.id, context.registry)

    assert {:error, %Error{reason: :agent_not_exposed_on_connection}} =
             Monitoring.enable(session, request(context))

    :ok = ConnectionRegistry.close(connection.id)

    connection = open_connection(context.descriptor, [RuntimeInfo.scope(), Monitoring.scope()])
    session = start_session(connection.id, context.registry)

    assert {:error, %Error{reason: :invalid_runtime_monitor_interval}} =
             Monitoring.enable(session, Map.put(request(context), "interval_ms", 249))

    assert {:error, %Error{reason: :invalid_runtime_monitor_duration}} =
             Monitoring.enable(session, Map.put(request(context), "duration_ms", 3_600_001))

    assert {:error, %Error{reason: :runtime_info_field_not_allowed}} =
             Monitoring.enable(session, Map.put(request(context), "fields", ["messages"]))

    assert {:error, %Error{reason: :runtime_monitor_subject_required}} =
             Monitoring.enable(session, Map.delete(request(context), "subject"))
  end

  test "session shutdown removes every catalog entry", context do
    connection = open_connection(context.descriptor, [RuntimeInfo.scope(), Monitoring.scope()])
    session = start_session(connection.id, context.registry)

    assert {:ok, enabled} = Monitoring.enable(session, request(context))
    assert [_subscription] = Spectre.Pulse.monitoring_subscriptions(connection.id)

    GenServer.stop(session)

    assert eventually(fn -> Spectre.Pulse.monitoring_subscriptions(connection.id) == [] end)
    refute Process.alive?(session)
    assert enabled["subject_id"] == Spectre.Subject.new(context.subject).id
  end

  test "enforces the per-connection subscription ceiling", context do
    connection = open_connection(context.descriptor, [RuntimeInfo.scope(), Monitoring.scope()])
    session = start_session(connection.id, context.registry)
    request = Map.put(request(context), "interval_ms", 60_000)

    Enum.each(1..16, fn _index ->
      assert {:ok, %{"type" => "agent.runtime.monitor.enabled"}} =
               Monitoring.enable(session, request)
    end)

    assert length(Spectre.Pulse.monitoring_subscriptions(connection.id)) == 16

    assert {:error, %Error{kind: :authorization, reason: :runtime_monitor_limit_reached}} =
             Monitoring.enable(session, request)
  end

  test "coalesces samples while the transport sink is under backpressure", context do
    connection = open_connection(context.descriptor, [RuntimeInfo.scope(), Monitoring.scope()])
    session = start_session(connection.id, context.registry)
    assert {:ok, enabled} = Monitoring.enable(session, request(context))
    subscription_id = enabled["subscription_id"]

    Enum.each(1..101, fn _index -> send(self(), :transport_backpressure) end)
    refute_receive {:spectre_pulse_monitoring, _event}, 300
    flush_backpressure(101)

    assert_receive {:spectre_pulse_monitoring,
                    %{
                      "subscription_id" => ^subscription_id,
                      "sequence" => 1,
                      "dropped_updates" => dropped_updates
                    }},
                   500

    assert dropped_updates >= 1
  end

  test "reports a safe stream error when the Agent Instance disappears", context do
    ref = Ref.new(Agent, context.subject)
    Registry.unregister(context.registry, ref.key)
    test_pid = self()

    instance =
      spawn(fn ->
        {:ok, _owner} = Registry.register(context.registry, ref.key, ref)
        send(test_pid, :monitoring_instance_ready)
        receive do: (:stop -> :ok)
      end)

    assert_receive :monitoring_instance_ready
    connection = open_connection(context.descriptor, [RuntimeInfo.scope(), Monitoring.scope()])
    session = start_session(connection.id, context.registry)
    assert {:ok, enabled} = Monitoring.enable(session, request(context))
    subscription_id = enabled["subscription_id"]
    monitor = Process.monitor(instance)
    send(instance, :stop)
    assert_receive {:DOWN, ^monitor, :process, ^instance, :normal}

    assert_receive {:spectre_pulse_monitoring,
                    %{
                      "type" => "agent.runtime.monitor.error",
                      "subscription_id" => ^subscription_id,
                      "sequence" => 1,
                      "error" => %{"kind" => "routing", "code" => "instance_not_found"}
                    }},
                   500
  end

  defp request(context) do
    %{
      "agent_address" => context.descriptor.address,
      "subject" => context.subject,
      "interval_ms" => 250
    }
  end

  defp open_connection(descriptor, grants, agents \\ :default) do
    agents = if agents == :default, do: [descriptor.module], else: agents

    assert :ok =
             ConnectionRegistry.configure(
               self(),
               [[id: :monitoring, transport: :websocket, agents: agents, scopes: grants]],
               [descriptor]
             )

    assert {:ok, connection} =
             ConnectionRegistry.open(:monitoring,
               owner: self(),
               principal: %{id: "studio", scopes: grants}
             )

    connection
  end

  defp start_session(connection_id, registry) do
    assert {:ok, session} =
             Monitoring.start_link(
               connection: connection_id,
               sink: self(),
               runtime: [instance_registry: registry]
             )

    on_exit(fn -> stop_session(session) end)

    session
  end

  defp flush_backpressure(0), do: :ok

  defp flush_backpressure(remaining) do
    receive do
      :transport_backpressure -> flush_backpressure(remaining - 1)
    end
  end

  defp stop_session(session) do
    if Process.alive?(session), do: GenServer.stop(session)
  catch
    :exit, _reason -> :ok
  end

  defp eventually(callback, attempts \\ 50)
  defp eventually(_callback, 0), do: false

  defp eventually(callback, attempts) do
    if callback.() do
      true
    else
      Process.sleep(10)
      eventually(callback, attempts - 1)
    end
  end

  defp unique_name(prefix),
    do: String.to_atom("#{__MODULE__}.#{prefix}.#{System.unique_integer([:positive])}")
end

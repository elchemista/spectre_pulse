defmodule Spectre.Pulse.OperationsMonitoringTest.Work do
  @moduledoc false

  use Spectre.Work,
    id: :pulse_monitoring_work,
    version: 1,
    input: :map,
    state: :map,
    waits: [:human]

  @impl Spectre.Operation.Controller
  def init(input, _context), do: {:ok, input}

  @impl Spectre.Operation.Controller
  def next(_state, _context), do: wait(:human)

  @impl Spectre.Operation.Controller
  def apply_result(state, _request, _result, _context), do: {:ok, state}

  @impl Spectre.Operation.Controller
  def complete(_state, _context), do: :continue
end

defmodule Spectre.Pulse.OperationsMonitoringTest.Vigil do
  @moduledoc false

  use Spectre.Vigil,
    id: :pulse_monitoring_vigil,
    version: 1,
    input: :map,
    state: :map,
    waits: [:timer]

  @impl Spectre.Operation.Controller
  def init(input, _context), do: {:ok, input}

  @impl Spectre.Operation.Controller
  def next(_state, context), do: wait_for(60_000, context)

  @impl Spectre.Operation.Controller
  def apply_result(state, _request, _result, _context), do: {:ok, state}
end

defmodule Spectre.Pulse.OperationsMonitoringTest.Agent do
  @moduledoc false

  use Spectre.Agent
  use Spectre.Pulse

  pulsing do
    identity("spectre://operations-monitoring/agent")
  end
end

defmodule Spectre.Pulse.OperationsMonitoringTest.Endpoint do
  @moduledoc false
end

defmodule Spectre.Pulse.OperationsMonitoringTest do
  use ExUnit.Case, async: false

  alias Spectre.Operation.View
  alias Spectre.Pulse.AgentDescriptor
  alias Spectre.Pulse.ConnectionRegistry
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Monitoring
  alias Spectre.Pulse.Monitoring.Subscription
  alias Spectre.Pulse.Operations
  alias Spectre.Pulse.Operations.Request
  alias Spectre.Pulse.Operations.View, as: OperationsView
  alias Spectre.Pulse.Phoenix.Socket
  alias Spectre.Pulse.WireValue

  alias __MODULE__.Agent
  alias __MODULE__.Endpoint
  alias __MODULE__.Vigil
  alias __MODULE__.Work

  setup do
    ConnectionRegistry.clear_configuration(self())
    subject = "operations-monitoring-#{System.unique_integer([:positive])}"
    {:ok, instance} = Spectre.summon(agent: Agent, subject: subject, idle: false)
    {:ok, work_ref, %View{kind: :work}} = Spectre.start_work(instance, Work, %{job: "review"})

    {:ok, vigil_ref, %View{kind: :vigil}} =
      Spectre.register_vigil(instance, Vigil, %{watch: "cache"})

    descriptor = AgentDescriptor.for_agent(Agent) |> elem(1)

    on_exit(fn ->
      ConnectionRegistry.clear_configuration(self())
      stop_process(instance)
    end)

    %{
      descriptor: descriptor,
      instance: instance,
      subject: subject,
      vigil_ref: vigil_ref,
      work_ref: work_ref
    }
  end

  test "lists bounded privacy-safe Work and Vigil views", context do
    assert {:ok, snapshot} = Spectre.Pulse.operations(Agent, context.subject)

    assert snapshot["capability"] == "agent.operations.list"
    assert snapshot["scope"] == "agent.operations.read"
    assert snapshot["agent_address"] == context.descriptor.address
    assert snapshot["counts"] == %{"total" => 2, "work" => 1, "vigil" => 1}
    assert snapshot["truncated"] == false

    operations = snapshot["operations"]
    assert Enum.map(operations, & &1["kind"]) |> Enum.sort() == ["vigil", "work"]
    assert Enum.all?(operations, &is_binary(&1["status"]))
    assert Enum.all?(operations, &is_map(&1["budget"]))
    refute Enum.any?(operations, &Map.has_key?(&1, "state"))
    refute Enum.any?(operations, &Map.has_key?(&1, "base_input"))

    assert {:ok, work_only} =
             Spectre.Pulse.operations(Agent, context.subject,
               kinds: [:work],
               max_collection_entries: 1,
               max_depth: 2
             )

    assert work_only["counts"] == %{"total" => 1, "work" => 1, "vigil" => 0}
    assert [%{"kind" => "work"}] = work_only["operations"]

    assert work_only["limits"] == %{
             "max_binary_bytes" => 16_384,
             "max_collection_entries" => 1,
             "max_depth" => 2
           }
  end

  test "bounds published binaries and keeps operation views JSON-safe" do
    assert {:ok, request} = Request.new(max_binary_bytes: 4)

    view = %View{
      id: "view",
      kind: :work,
      progress: "ééé",
      metadata: %{<<255>> => <<255, 0, 1, 2, 3>>}
    }

    assert {wire, truncated_fields} = OperationsView.to_wire(view, request)
    assert wire["progress"] == "éé"
    assert Enum.sort(truncated_fields) == ["metadata", "progress"]
    assert {:ok, _json} = Jason.encode(wire)
  end

  test "encodes uncommon published values within every wire limit" do
    limits = %{max_binary_bytes: 3, max_collection_entries: 1, max_depth: 1}

    assert {"é", true} = WireValue.encode("éé", limits)
    assert {pid, false} = WireValue.encode(self(), limits)
    assert pid == inspect(self())
    assert {reference, false} = WireValue.encode(make_ref(), limits)
    assert String.starts_with?(reference, "#Reference<")
    assert {["one"], true} = WireValue.encode({:one, :two}, limits)

    assert {%{"nested" => "[truncated:max_depth]"}, true} =
             WireValue.encode(%{nested: %{value: 1}}, %{limits | max_binary_bytes: 10})

    assert {%{"{1, 2}" => "value"}, false} =
             WireValue.encode(%{{1, 2} => :value}, %{limits | max_depth: 2})

    assert {encoded_function, false} = WireValue.encode(fn -> :ok end, limits)
    assert String.starts_with?(encoded_function, "#Function<")
  end

  test "excludes terminal loops unless Studio requests them", context do
    assert {:ok, %View{status: :terminal}} =
             Spectre.stop_loop(context.instance, context.work_ref, :studio_test)

    assert {:ok, %{"counts" => %{"total" => 1}, "operations" => [active]}} =
             Spectre.Pulse.operations(Agent, context.subject)

    assert active["kind"] == "vigil"

    assert {:ok, %{"counts" => %{"total" => 2}, "operations" => operations}} =
             Spectre.Pulse.operations(Agent, context.subject, include_terminal: true)

    assert Enum.any?(operations, &(&1["status"] == "terminal"))
  end

  test "enforces operation scopes, exposure and closed request options", context do
    connection = open_connection(context.descriptor, [Operations.scope()], [])

    assert {:error, %Error{reason: :agent_not_exposed_on_connection}} =
             Spectre.Pulse.operations(context.descriptor.address, context.subject,
               connection: connection.id
             )

    assert {:error, %Error{reason: :invalid_operations_options}} =
             Spectre.Pulse.operations(Agent, context.subject, internal_state: true)

    assert {:error, %Error{reason: :invalid_operations_kinds}} =
             Spectre.Pulse.operations(Agent, context.subject, kinds: ["controller"])

    assert {:error, %Error{reason: {:invalid_operations_limit, :max_depth}}} =
             Spectre.Pulse.operations(Agent, context.subject, max_depth: 9)

    assert {:error, %Error{reason: {:invalid_operations_limit, :max_binary_bytes}}} =
             Spectre.Pulse.operations(Agent, context.subject, max_binary_bytes: 65_537)

    assert {:error, %Error{reason: :invalid_operations_options}} =
             Request.new(:not_a_keyword)

    assert {:error, %Error{reason: :invalid_operations_kinds}} = Request.new(kinds: [])

    assert {:error, %Error{reason: :invalid_operations_include_terminal}} =
             Request.new(include_terminal: "yes")

    assert {:error, %Error{reason: :invalid_operations_instance_registry}} =
             Request.new(instance_registry: "private")

    assert {:error, %Error{reason: :invalid_operations_target}} =
             Spectre.Pulse.operations({:invalid, :target}, context.subject)

    assert {:error, %Error{reason: :invalid_operations_subject}} =
             Spectre.Pulse.operations(Agent, fn -> :invalid end)

    assert {:error, %Error{reason: :instance_not_found}} =
             Spectre.Pulse.operations(Agent, {:missing, context.subject})

    assert {:error, %Error{reason: :unknown_connection}} =
             Spectre.Pulse.operations(context.descriptor.address, context.subject,
               connection: :missing
             )
  end

  test "streams and disables Work and Vigil monitoring through Phoenix", context do
    scopes = [Operations.scope(), Monitoring.operations_scope()]
    configure_phoenix_connection(context.descriptor, scopes)

    transport_info = %{
      endpoint: Endpoint,
      transport: :websocket,
      params: %{"token" => "operations-token"},
      connect_info: %{peer_data: %{address: {127, 0, 0, 1}}}
    }

    assert {:ok, pending} = Socket.connect(transport_info, connection: :operations_studio)
    assert {:ok, state} = Socket.init(pending)
    assert_receive {:spectre_pulse_manifest, _connection_id}

    enable =
      Jason.encode!(%{
        "pulse" => "connection",
        "version" => 1,
        "type" => "agent.operations.monitor.enable",
        "request_id" => "work-vigil-liveview",
        "agent_address" => context.descriptor.address,
        "subject" => context.subject,
        "interval_ms" => 250,
        "duration_ms" => 1_000,
        "kinds" => ["work", "vigil"]
      })

    assert {:reply, :ok, {:text, enabled_frame}, state} =
             Socket.handle_in({enable, opcode: :text}, state)

    assert {:ok,
            %{
              "type" => "agent.operations.monitor.enabled",
              "monitor" => "operations",
              "request_id" => "work-vigil-liveview",
              "subscription_id" => subscription_id,
              "sequence" => 0,
              "snapshot" => %{"counts" => %{"total" => 2}}
            }} = Jason.decode(enabled_frame)

    assert [%{"subscription_id" => ^subscription_id}] =
             Spectre.Pulse.operation_monitoring_subscriptions(state.connection.id)

    assert_receive {:spectre_pulse_monitoring, update}, 500

    assert {:push, {:text, update_frame}, state} =
             Socket.handle_info({:spectre_pulse_monitoring, update}, state)

    assert {:ok,
            %{
              "type" => "agent.operations.monitor.update",
              "subscription_id" => ^subscription_id,
              "sequence" => 1,
              "snapshot" => %{"operations" => operations}
            }} = Jason.decode(update_frame)

    assert length(operations) == 2

    disable =
      Jason.encode!(%{
        "pulse" => "connection",
        "version" => 1,
        "type" => "agent.operations.monitor.disable",
        "subscription_id" => subscription_id
      })

    assert {:reply, :ok, {:text, disabled_frame}, state} =
             Socket.handle_in({disable, opcode: :text}, state)

    assert {:ok,
            %{
              "type" => "agent.operations.monitor.disabled",
              "subscription_id" => ^subscription_id
            }} = Jason.decode(disabled_frame)

    assert Spectre.Pulse.operation_monitoring_subscriptions(state.connection.id) == []
    refute_receive {:spectre_pulse_monitoring, %{"subscription_id" => ^subscription_id}}, 350
    assert :ok = Socket.terminate(:closed, state)
  end

  test "requires both operation read and stream scopes", context do
    read_only = open_connection(context.descriptor, [Operations.scope()])
    session = start_session(read_only.id)

    assert {:error,
            %Error{
              reason: {:connection_scope_required, "agent.operations.stream"}
            }} = Monitoring.enable_operations(session, monitor_request(context))

    GenServer.stop(session)
    :ok = ConnectionRegistry.close(read_only.id)

    stream_only = open_connection(context.descriptor, [Monitoring.operations_scope()])
    session = start_session(stream_only.id)

    assert {:error,
            %Error{
              reason: {:connection_scope_required, "agent.operations.read"}
            }} = Monitoring.enable_operations(session, monitor_request(context))
  end

  test "rejects malformed operations subscriptions without creating state", context do
    scopes = [Operations.scope(), Monitoring.operations_scope()]
    connection = open_connection(context.descriptor, scopes)
    session = start_session(connection.id)
    request = monitor_request(context)

    assert {:error, %Error{reason: :operations_monitor_agent_address_required}} =
             Monitoring.enable_operations(session, Map.delete(request, "agent_address"))

    assert {:error, %Error{reason: :operations_monitor_subject_required}} =
             Monitoring.enable_operations(session, Map.delete(request, "subject"))

    assert {:error, %Error{reason: :invalid_operations_monitor_request_id}} =
             Monitoring.enable_operations(session, Map.put(request, "request_id", ""))

    assert {:error, %Error{reason: :invalid_operations_monitor_interval}} =
             Monitoring.enable_operations(session, Map.put(request, "interval_ms", 249))

    assert {:error, %Error{reason: :invalid_operations_monitor_duration}} =
             Monitoring.enable_operations(session, Map.put(request, "duration_ms", 3_600_001))

    assert {:error, %Error{reason: :invalid_operations_monitor_request}} =
             Monitoring.enable_operations(session, [:not, :a, :map])

    assert {:error, %Error{reason: :invalid_monitor_kind}} =
             Subscription.new(:unknown, connection.id, request, connection: connection.id)

    assert Spectre.Pulse.operation_monitoring_subscriptions(connection.id) == []
  end

  defp monitor_request(context) do
    %{
      "agent_address" => context.descriptor.address,
      "subject" => context.subject,
      "interval_ms" => 250
    }
  end

  defp open_connection(descriptor, scopes, agents \\ :default) do
    agents = if agents == :default, do: [descriptor.module], else: agents

    assert :ok =
             ConnectionRegistry.configure(
               self(),
               [[id: :operations, transport: :websocket, agents: agents, scopes: scopes]],
               [descriptor]
             )

    assert {:ok, connection} =
             ConnectionRegistry.open(:operations,
               owner: self(),
               principal: %{id: "studio", scopes: scopes}
             )

    connection
  end

  defp configure_phoenix_connection(descriptor, scopes) do
    authenticator = fn credential, _context ->
      if get_in(credential, [:params, "token"]) == "operations-token",
        do: {:ok, %{id: "studio", kind: :studio, scopes: scopes}},
        else: {:error, :invalid_token}
    end

    authorizer = fn _principal, _request -> {:ok, granted_scopes: scopes} end

    assert :ok =
             ConnectionRegistry.configure(
               self(),
               [
                 [
                   id: :operations_studio,
                   transport: :websocket,
                   agents: [descriptor.module],
                   authenticate: authenticator,
                   authorize: authorizer,
                   scopes: scopes
                 ]
               ],
               [descriptor]
             )
  end

  defp start_session(connection_id) do
    assert {:ok, session} =
             Monitoring.start_link(connection: connection_id, sink: self())

    on_exit(fn ->
      stop_process(session)
    end)

    session
  end

  defp stop_process(pid) do
    if Process.alive?(pid), do: GenServer.stop(pid, :normal)
  catch
    :exit, _reason -> :ok
  end
end

defmodule Spectre.Pulse.RuntimeInfoTest.Agent do
  use Spectre.Agent
  use Spectre.Pulse

  pulsing do
    identity("spectre://runtime-info/agent")
  end
end

defmodule Spectre.Pulse.RuntimeInfoTest do
  use ExUnit.Case, async: false

  alias Spectre.Instance.Ref
  alias Spectre.Pulse
  alias Spectre.Pulse.AgentDescriptor
  alias Spectre.Pulse.ConnectionRegistry
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.RuntimeInfo
  alias Spectre.Pulse.RuntimeInfo.Request
  alias Spectre.Pulse.RuntimeInfo.Snapshot

  alias __MODULE__.Agent

  setup do
    ConnectionRegistry.clear_configuration(self())

    registry = unique_name("instance_registry")
    start_supervised!({Registry, keys: :unique, name: registry})

    subject = {:studio_subject, System.unique_integer([:positive])}
    ref = Ref.new(Agent, subject)
    {:ok, _owner} = Registry.register(registry, ref.key, ref)

    on_exit(fn -> ConnectionRegistry.clear_configuration(self()) end)

    %{registry: registry, subject: subject, ref: ref}
  end

  test "returns a bounded wire-safe OTP snapshot", context do
    assert {:ok, snapshot} =
             Pulse.runtime_info(Agent, context.subject, instance_registry: context.registry)

    assert snapshot["capability"] == "agent.runtime.info"
    assert snapshot["scope"] == "agent.runtime.read"
    assert snapshot["agent_address"] == "spectre://runtime-info/agent"
    assert snapshot["instance_ref"] == context.ref.key
    assert snapshot["pid"] == inspect(self())
    assert snapshot["alive"]
    assert is_integer(snapshot["sampled_at_unix_ms"])

    assert snapshot["limits"] == %{
             "max_collection_entries" => 128,
             "max_depth" => 6
           }

    assert is_list(snapshot["truncated_fields"])

    process = snapshot["process"]
    assert is_integer(process["memory"])
    assert is_integer(process["message_queue_len"])
    assert is_integer(process["reductions"])
    assert is_binary(process["status"])
    refute Map.has_key?(process, "messages")
    refute Map.has_key?(process, "dictionary")
  end

  test "allows an explicit subset of safe fields", context do
    assert {:ok, %{"process" => process}} =
             Pulse.runtime_info(Agent, context.subject,
               instance_registry: context.registry,
               fields: ["memory", :status]
             )

    assert Map.keys(process) |> Enum.sort() == ["memory", "status"]

    assert {:error, %Error{kind: :authorization, reason: :runtime_info_field_not_allowed}} =
             RuntimeInfo.process_info(self(), [:messages])
  end

  test "enforces hard serialization limits and reports truncation", context do
    Enum.each(1..3, fn _index -> spawn_link(fn -> Process.sleep(:infinity) end) end)

    assert {:ok, snapshot} =
             Pulse.runtime_info(Agent, context.subject,
               instance_registry: context.registry,
               fields: [:links],
               max_collection_entries: 1,
               max_depth: 2
             )

    assert length(snapshot["process"]["links"]) == 1
    assert snapshot["truncated_fields"] == ["links"]
    assert snapshot["limits"] == %{"max_collection_entries" => 1, "max_depth" => 2}

    assert {:error,
            %Error{
              kind: :validation,
              reason: {:invalid_runtime_info_limit, :max_collection_entries}
            }} =
             Pulse.runtime_info(Agent, context.subject,
               instance_registry: context.registry,
               max_collection_entries: 257
             )
  end

  test "rejects unknown options and registries without leaking their values", context do
    assert {:error, %Error{reason: :invalid_runtime_info_options}} =
             Pulse.runtime_info(Agent, context.subject, unknown: "private")

    assert {:error, %Error{reason: :invalid_runtime_info_instance_registry}} =
             Pulse.runtime_info(Agent, context.subject, instance_registry: "private")

    assert {:error, %Error{reason: :invalid_runtime_info_options}} = Request.new(:invalid)

    assert {:error, %Error{reason: :runtime_info_fields_required}} =
             RuntimeInfo.process_info(self(), [])

    assert {:error, %Error{reason: :invalid_runtime_info_fields}} =
             RuntimeInfo.process_info(self(), :invalid)

    assert {:error, %Error{reason: :invalid_runtime_process}} =
             RuntimeInfo.process_info(:not_a_pid)

    assert Snapshot.default_fields() |> Enum.member?(:memory)
  end

  test "resolves local addresses and stable Agent references", context do
    descriptor = AgentDescriptor.for_agent(Agent) |> elem(1)
    assert :ok = ConnectionRegistry.configure(self(), [], [descriptor])

    assert {:ok, %{"agent_address" => address}} =
             Pulse.runtime_info(descriptor.address, context.subject,
               instance_registry: context.registry
             )

    assert address == descriptor.address

    agent_ref = Spectre.AgentRef.new(Agent, id: "runtime-info-ref")
    ref_subject = {:agent_ref_subject, context.subject}
    ref = Ref.new(agent_ref, ref_subject)
    assert {:ok, _owner} = Registry.register(context.registry, ref.key, ref)

    assert {:ok, %{"agent_address" => "agent-ref:runtime-info-ref"}} =
             Pulse.runtime_info(agent_ref, ref_subject, instance_registry: context.registry)
  end

  test "returns stable routing and authorization failures", context do
    descriptor = AgentDescriptor.for_agent(Agent) |> elem(1)

    assert :ok =
             ConnectionRegistry.configure(
               self(),
               [
                 [
                   id: :private,
                   transport: :websocket,
                   agents: [],
                   scopes: [RuntimeInfo.scope()]
                 ]
               ],
               [descriptor]
             )

    assert {:ok, connection} =
             ConnectionRegistry.open(:private,
               owner: self(),
               principal: %{id: "operator", scopes: [RuntimeInfo.scope()]}
             )

    assert {:error, %Error{kind: :authorization, reason: :agent_not_exposed_on_connection}} =
             Pulse.runtime_info(descriptor.address, context.subject,
               connection: connection.id,
               instance_registry: context.registry
             )

    assert {:error, %Error{kind: :routing, reason: :unknown_connection}} =
             Pulse.runtime_info(descriptor.address, context.subject,
               connection: :missing,
               instance_registry: context.registry
             )

    assert {:error, %Error{kind: :validation, reason: :invalid_runtime_info_target}} =
             Pulse.runtime_info({:invalid, :target}, context.subject,
               instance_registry: context.registry
             )

    assert {:error, %Error{kind: :routing, reason: :instance_registry_unavailable}} =
             Pulse.runtime_info(Agent, context.subject,
               instance_registry: unique_name("missing_registry")
             )
  end

  test "requires the scope and exposed Agent for connection calls", context do
    descriptor = AgentDescriptor.for_agent(Agent) |> elem(1)

    assert :ok =
             ConnectionRegistry.configure(
               self(),
               [[id: :studio, transport: :websocket, scopes: [RuntimeInfo.scope()]]],
               [descriptor]
             )

    assert {:ok, denied} =
             ConnectionRegistry.open(:studio,
               id: :denied,
               owner: self(),
               principal: %{id: "viewer", scopes: []}
             )

    assert denied.granted_scopes == []

    assert {:error,
            %Error{
              kind: :authorization,
              reason: {:connection_scope_required, "agent.runtime.read"}
            }} =
             Pulse.runtime_info(descriptor.address, context.subject,
               connection: denied.id,
               instance_registry: context.registry
             )

    assert :ok = ConnectionRegistry.close(denied.id)

    assert {:ok, allowed} =
             ConnectionRegistry.open(:studio,
               id: :allowed,
               owner: self(),
               principal: %{
                 id: "operator",
                 scopes: [RuntimeInfo.scope()]
               }
             )

    assert {:ok, %{"pid" => pid}} =
             Pulse.runtime_info(descriptor.address, context.subject,
               connection: allowed.id,
               instance_registry: context.registry
             )

    assert pid == inspect(self())
  end

  test "reports missing and dead Instances without reading state", context do
    assert {:error, %Error{kind: :routing, reason: :instance_not_found}} =
             Pulse.runtime_info(Agent, {:missing, context.subject},
               instance_registry: context.registry
             )

    pid = spawn(fn -> receive do: (:stop -> :ok) end)
    monitor = Process.monitor(pid)
    send(pid, :stop)
    assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}

    assert {:error, %Error{kind: :routing, reason: :agent_instance_not_alive}} =
             RuntimeInfo.process_info(pid)
  end

  defp unique_name(prefix),
    do: String.to_atom("#{__MODULE__}.#{prefix}.#{System.unique_integer([:positive])}")
end

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

    pid = spawn(fn -> :ok end)
    monitor = Process.monitor(pid)
    assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}

    assert {:error, %Error{kind: :routing, reason: :agent_instance_not_alive}} =
             RuntimeInfo.process_info(pid)
  end

  defp unique_name(prefix),
    do: String.to_atom("#{__MODULE__}.#{prefix}.#{System.unique_integer([:positive])}")
end

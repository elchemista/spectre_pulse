defmodule Spectre.Pulse.InstancesTest.Agent do
  use Spectre.Agent
  use Spectre.Pulse

  pulsing do
    identity("spectre://instances/agent")
  end
end

defmodule Spectre.Pulse.InstancesTest do
  use ExUnit.Case, async: false

  alias Spectre.Instance.Ref
  alias Spectre.Pulse.AgentDescriptor
  alias Spectre.Pulse.ConnectionRegistry
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Instances
  alias Spectre.Pulse.RuntimeInfo

  alias __MODULE__.Agent

  setup do
    ConnectionRegistry.clear_configuration(self())

    registry = unique_name("registry")
    start_supervised!({Registry, keys: :unique, name: registry})

    descriptor = AgentDescriptor.for_agent(Agent) |> elem(1)

    :ok =
      ConnectionRegistry.configure(
        self(),
        [[id: :studio, transport: :websocket, scopes: [RuntimeInfo.scope()]]],
        [descriptor]
      )

    on_exit(fn -> ConnectionRegistry.clear_configuration(self()) end)

    %{descriptor: descriptor, registry: registry}
  end

  test "lists only live Instances of the exposed Agent", context do
    register(context.registry, Agent, "conversation-42")
    register(context.registry, Agent, "conversation-7")

    assert {:ok, connection} =
             ConnectionRegistry.open(:studio,
               owner: self(),
               principal: %{id: "operator", scopes: [RuntimeInfo.scope()]}
             )

    assert {:ok, result} =
             Instances.list(context.descriptor.address, connection.id,
               instance_registry: context.registry
             )

    assert result["scope"] == "agent.runtime.read"
    assert result["count"] == 2
    assert Enum.map(result["instances"], & &1["subject"]) == ["conversation-42", "conversation-7"]
    assert Enum.all?(result["instances"], & &1["alive"])
    refute inspect(result) =~ "metadata"
  end

  test "enforces both the scope and exposed-Agent boundary", context do
    assert {:ok, denied} =
             ConnectionRegistry.open(:studio,
               id: :denied,
               owner: self(),
               principal: %{id: "viewer", scopes: []}
             )

    assert {:error, %Error{reason: {:connection_scope_required, "agent.runtime.read"}}} =
             Instances.list(context.descriptor.address, denied.id,
               instance_registry: context.registry
             )

    assert :ok = ConnectionRegistry.close(denied.id)

    :ok =
      ConnectionRegistry.configure(
        self(),
        [[id: :private, transport: :websocket, agents: [], scopes: [RuntimeInfo.scope()]]],
        [context.descriptor]
      )

    assert {:ok, private} =
             ConnectionRegistry.open(:private,
               owner: self(),
               principal: %{id: "operator", scopes: [RuntimeInfo.scope()]}
             )

    assert {:error, %Error{reason: :agent_not_exposed_on_connection}} =
             Instances.list(context.descriptor.address, private.id,
               instance_registry: context.registry
             )
  end

  defp register(registry, agent, subject) do
    ref = Ref.new(agent, subject)
    {:ok, _owner} = Registry.register(registry, ref.key, ref)
  end

  defp unique_name(prefix),
    do: String.to_atom("#{__MODULE__}.#{prefix}.#{System.unique_integer([:positive])}")
end

defmodule Spectre.Pulse.ServerToolTest do
  use ExUnit.Case, async: true
  alias Spectre.Pulse.{Config, Envelope, ServerTool}

  defmodule Stack do
    use Spectre.Stack
    install(Spectre.Pulse, bind: [send: :send_agent_message])
  end

  defmodule Server do
    use Spectre.Server
    def handle_flow(:unmatched, _input, ctx), do: respond(ctx, "accepted")
    def handle_flow(route, input, ctx), do: super(route, input, ctx)
  end

  defmodule Agent do
    use Spectre.Agent, stack: Stack, server: Server
  end

  test "protocol helpers are portable and sending requires a host binding" do
    {:ok, installation} = Spectre.Stack.installation(Agent, :pulse)
    assert installation.adapters.tools.pulse.operations.send.path == :governed
    assert installation.adapters.tools.pulse.operations.send.operation == :send_agent_message

    assert {:ok, %{}} =
             ServerTool.prepare(:protocol, [], %{agent: Agent, config: installation.config})

    assert {:ok, {:ok, %{name: "spectre-pulse"} = protocol}} =
             ServerTool.project(:protocol, ServerTool.invoke(:protocol, %{}, %{}))

    assert :ok = Spectre.GAM.Canonical.Value.validate(protocol)
    refute Map.has_key?(Spectre.Pulse.tools(%{}).pulse.operations, :send)
  end

  test "send preparation pins sender identity and rejects transport overrides" do
    {:ok, config} = Config.fetch(Agent)
    attrs = %{to: "spectre://test/receiver", payload: %{type: "support.message", data: "Hi"}}
    assert {:ok, args} = ServerTool.prepare(:send, [attrs], %{agent: Agent})
    assert args["from"] == config.identity
    refute Map.has_key?(args, "id")

    assert {:error, :unsupported_pulse_fields} =
             ServerTool.prepare(:send, [Map.put(attrs, :network, __MODULE__)], %{agent: Agent})
  end

  test "authenticated inbound queues on a real AgentServer without synthesizing a legacy Turn" do
    {:ok, server} = Spectre.Server.start_link(agent: Agent, consumer: self())
    {:ok, config} = Config.fetch(Agent)

    envelope =
      Envelope.new!(
        from: "spectre://test/sender",
        to: config.identity,
        payload: %{type: "support.message", data: "Hello"}
      )

    context = %{target: Agent, authenticated_identity: envelope.from, binding: :test}
    assert {:ok, result} = Spectre.Pulse.receive(envelope, context, agent_server: server)
    assert result.turn == nil
    assert is_binary(result.interaction)
    id = result.interaction
    assert_receive {Spectre.Server, ^server, {:response, ^id, "accepted", []}}, 2000

    assert {:error, %{kind: :authentication}} =
             Spectre.Pulse.receive(
               envelope,
               %{context | authenticated_identity: "spectre://test/forged"},
               agent_server: server
             )

    GenServer.stop(server)
  end
end

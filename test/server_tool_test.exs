defmodule Spectre.Pulse.ServerToolTest do
  use ExUnit.Case, async: true
  alias Spectre.GAM.Canonical.Value
  alias Spectre.Pulse.{Config, Envelope, Error, Receipt, ServerTool}
  alias Spectre.Stack.Effects

  test "managed message registration remains opt-in" do
    assert %{} = Spectre.Pulse.effects(%{})
    effects = Spectre.Pulse.effects(%{options: [bind: [send: :send_agent_message]]})
    assert %{send_agent_message: %{adapter: ServerTool, operation: :send}} = effects
    assert Effects.validate!(effects) == effects
  end

  defmodule DeliveryNetwork do
    def deliver(envelope, _opts) do
      send(self(), {:pulse_delivery_attempt, envelope})

      case Process.get({__MODULE__, :result}) do
        nil ->
          {:ok, Receipt.accepted(envelope.id, via: :fixture, route_id: "fixture")}

        {:raise, message} ->
          raise message

        {:throw, reason} ->
          throw(reason)

        result ->
          result
      end
    end
  end

  defmodule DeliveryAgent do
    def __spectre_pulse__ do
      Config.new!(
        identity: "spectre://test/sender",
        network: DeliveryNetwork,
        contacts: [%{key: :receiver, identity: "spectre://test/receiver"}]
      )
    end
  end

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

    assert :ok = Value.validate(protocol)
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
    assert_receive {Spectre.Server, ^server, {:response, ^id, "accepted", metadata}}, 2000
    assert is_binary(metadata[:message_id])

    assert {:error, %{kind: :authentication}} =
             Spectre.Pulse.receive(
               envelope,
               %{context | authenticated_identity: "spectre://test/forged"},
               agent_server: server
             )

    GenServer.stop(server)
  end

  test "admitted send invokes the real network dispatcher and pins identity and stable command id" do
    attrs = %{to: "spectre://test/receiver", payload: %{type: "support.message", data: "Hello"}}
    {:ok, prepared} = ServerTool.prepare(:send, [attrs], %{agent: DeliveryAgent})
    id = Spectre.Identity.uuid7()
    forged = Map.merge(prepared, %{from: "spectre://evil/forged", id: "forged"})

    assert {:ok, %{message_id: ^id, status: :accepted}} =
             ServerTool.execute(forged, %{agent: DeliveryAgent, command_id: id})

    assert_received {:pulse_delivery_attempt,
                     %{id: ^id, from: "spectre://test/sender", to: "spectre://test/receiver"}}

    assert {:error, %{outcome: :not_sent}} =
             ServerTool.execute(prepared, %{agent: DeliveryAgent, command_id: "invalid-id"})

    assert {:error, %{outcome: :not_sent}} =
             ServerTool.execute(prepared, %{agent: DeliveryAgent, command_id: 123})

    refute_received {:pulse_delivery_attempt, _}
  end

  test "send uncertainty and generic errors remain bounded portable data" do
    attrs = %{to: "spectre://test/receiver", payload: %{type: "support.message", data: "Hi"}}
    {:ok, prepared} = ServerTool.prepare(:send, [attrs], %{agent: DeliveryAgent})

    Process.put(
      {DeliveryNetwork, :result},
      {:error,
       Error.outcome_unknown(:transport, %{
         pid: self(),
         huge: String.duplicate("private", 5000)
       })}
    )

    assert {:error, %{outcome: :outcome_unknown} = error} =
             ServerTool.execute(prepared, %{
               agent: DeliveryAgent,
               command_id: Spectre.Identity.uuid7()
             })

    assert_received {:pulse_delivery_attempt, _}
    assert :ok = Value.validate(error, max_bytes: 4096)
    refute inspect(error) =~ "private"

    assert {:ok, {:error, %{outcome: :outcome_unknown} = projected}} =
             ServerTool.project(:send, {:error, self()})

    assert :ok = Value.validate(projected, max_bytes: 4096)
  end

  test "binding typos, duplicates, nil and non-atom operations fail during installation" do
    for bindings <- [
          [sned: :send],
          [send: nil],
          [send: "send"],
          [send: :one, send: :two],
          :invalid
        ] do
      assert_raise ArgumentError, ~r/pulse bind:/, fn ->
        Spectre.Pulse.tools(%{options: [bind: bindings]})
      end
    end
  end

  test "governed sends distinguish delivery success, definite rejection and uncertainty" do
    args = %{to: "spectre://test/receiver", payload: %{type: "support.message", data: "Hi"}}
    context = %{agent: DeliveryAgent, command_id: Spectre.Identity.uuid7()}

    assert {:ok, %{status: :accepted}} = ServerTool.execute_effect(:send, args, context)

    Process.put(
      {DeliveryNetwork, :result},
      {:error, Error.not_sent(:routing, :offline)}
    )

    assert {:error, :definitive_no_effect, %{outcome: :not_sent}} =
             ServerTool.execute_effect(:send, args, context)

    Process.put(
      {DeliveryNetwork, :result},
      {:error, Error.outcome_unknown(:transport, :disconnected)}
    )

    assert {:error, :ambiguous, %{outcome: :outcome_unknown}} =
             ServerTool.execute_effect(:send, args, context)

    assert {:error, :definitive_no_effect, :invalid_pulse_effect} =
             ServerTool.execute_effect(:unknown, args, context)
  end

  test "a host transport crash or throw never escapes the governed effect boundary" do
    args = %{to: "spectre://test/receiver", payload: %{type: "support.message", data: "Hi"}}
    context = %{agent: DeliveryAgent, command_id: Spectre.Identity.uuid7()}

    for failure <- [{:raise, "private transport details"}, {:throw, {:private, self()}}] do
      Process.put({DeliveryNetwork, :result}, failure)

      assert {:error, :ambiguous, error} = ServerTool.execute_effect(:send, args, context)
      assert :ok = Value.validate(error)
      refute inspect(error) =~ "private"
    end
  end

  test "invalid tool requests and receipts cannot become successful portable results" do
    assert {:error, :invalid_pulse_tool_request} = ServerTool.prepare(:send, [], %{})
    assert {:error, :invalid_pulse_tool_result} = ServerTool.project(:send, :unexpected)

    assert {:error, %{outcome: :not_sent}} =
             ServerTool.prepare(:send, [%{to: "invalid", payload: %{type: "support.message"}}], %{
               agent: DeliveryAgent
             })

    id = Spectre.Identity.uuid7()
    receipt = Receipt.accepted(id, via: :fixture, route_id: self())
    Process.put({DeliveryNetwork, :result}, {:ok, receipt})

    assert {:error, :ambiguous, %{message: ":invalid_pulse_receipt"}} =
             ServerTool.execute_effect(
               :send,
               %{to: "spectre://test/receiver", payload: %{type: "support.message"}},
               %{agent: DeliveryAgent, command_id: id}
             )
  end

  test "contact discovery projects only the public contact fields" do
    assert {:ok, %{}} = ServerTool.prepare(:contacts, [], %{})

    assert {:ok, [contact]} =
             ServerTool.invoke(:contacts, %{}, %{
               service_context: %{agent: DeliveryAgent}
             })

    assert contact == %{
             key: :receiver,
             identity: "spectre://test/receiver",
             display_name: nil,
             capabilities: []
           }

    assert {:ok, {:ok, [^contact]}} = ServerTool.project(:contacts, {:ok, [contact]})
  end
end

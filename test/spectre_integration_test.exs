defmodule Spectre.Pulse.IntegrationTest.ResultHook do
  def notify(result) do
    if process = Process.whereis(:spectre_pulse_integration_test) do
      send(process, {:inbound_result, result})
    end

    :ok
  end
end

defmodule Spectre.Pulse.IntegrationTest.Receiver do
  use Spectre.Agent
  use Spectre.Pulse

  pulsing do
    identity("spectre://acme/tao")
    state_scope(:agent)
    advertise(capabilities: [:research])
    pulse_inbound(on_result: {Spectre.Pulse.IntegrationTest.ResultHook, :notify, []})
  end

  flow :remote_work do
    on :perform_research, pulse: "research.perform" do
      run(:perform_research)
    end
  end

  def perform_research(input, _ctx) do
    "accepted:" <> input.meta.pulse.type
  end
end

defmodule Spectre.Pulse.IntegrationTest.Sender do
  use Spectre.Agent
  use Spectre.Pulse

  pulsing do
    identity("spectre://acme/anna")
    state_scope(:agent)

    contact(:tao, "spectre://acme/tao",
      display_name: "Tao",
      capabilities: [:research]
    )

    advertise(capabilities: [:planning])
  end

  flow :delegation do
    on :delegate, regex: ~r/^delegate:/ do
      pulse(:tao,
        act: :request,
        type: "research.perform",
        build: :build_request,
        expect: "research.completed"
      )
    end
  end

  def build_request(input, _ctx) do
    %{"topic" => String.replace_prefix(input.text, "delegate:", "")}
  end
end

defmodule Spectre.Pulse.SpectreIntegrationTest do
  use ExUnit.Case, async: false

  alias Spectre.Pulse.Codec.JSON
  alias Spectre.Pulse.Envelope
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Inbound.Result, as: InboundResult
  alias Spectre.Pulse.IntegrationTest.Receiver
  alias Spectre.Pulse.IntegrationTest.Sender
  alias Spectre.Pulse.Receipt
  alias Spectre.Pulse.State, as: PulseState
  alias Spectre.Pulse.Transports.PubSub
  alias Spectre.Pulse.Transports.REST
  alias Spectre.Pulse.Transports.WebSocket

  setup do
    Process.register(self(), :spectre_pulse_integration_test)

    start_supervised!({Spectre.Pulse, id: :integration_pulse_runtime})

    on_exit(fn ->
      if Process.whereis(:spectre_pulse_integration_test),
        do: Process.unregister(:spectre_pulse_integration_test)
    end)

    :ok
  end

  test "inbound envelope becomes a normal Spectre turn with deterministic Pulse metadata" do
    envelope =
      Envelope.new!(
        from: "spectre://acme/anna",
        to: "spectre://acme/tao",
        act: :request,
        payload: %{type: "research.perform", data: %{"topic" => "nautica"}}
      )

    assert {:ok, %InboundResult{} = inbound} =
             Spectre.Pulse.receive(envelope, %{
               authenticated_identity: envelope.from,
               binding: :test,
               target: Receiver
             })

    assert inbound.input.meta.pulse.message_id == envelope.id
    assert inbound.input.meta.pulse.authenticated
    assert inbound.input.meta.pulse.type == "research.perform"
    assert inbound.turn.opts[:turn_id] == envelope.id
    assert {:reply, result} = inbound.turn.decision
    assert result.reply_text == "accepted:research.perform"
  end

  test "declared sender cannot differ from transport-authenticated identity" do
    envelope =
      Envelope.new!(
        from: "spectre://acme/mallory",
        to: "spectre://acme/tao",
        payload: %{type: "research.perform", data: %{}}
      )

    assert {:error, %Error{kind: :authentication, reason: :sender_identity_mismatch}} =
             Spectre.Pulse.receive(envelope, %{
               authenticated_identity: "spectre://acme/anna",
               binding: :test,
               target: Receiver
             })
  end

  test "pulse DSL stages a generic effect, tracks expectation, then executes explicitly" do
    assert {:ok, turn} = Spectre.turn(Sender, "delegate:boats")
    assert {:needs, effect, staged_result} = turn.decision

    assert effect.kind == :pulse
    assert effect.name == :send
    assert effect.payload.to == "spectre://acme/tao"
    assert effect.payload.act == :request
    assert effect.payload.type == "research.perform"
    assert effect.payload.data == %{"topic" => "boats"}

    expectation = PulseState.expectations(staged_result.state)[effect.id]
    assert expectation.status == :open
    assert expectation.waiting_for == {:type, "research.completed"}

    assert {:ok, executed_turn} = Spectre.Pulse.execute_turn(turn)
    assert {:completed, completed, executed_result} = executed_turn.decision
    assert completed.kind == :pulse
    assert completed.status == :completed
    assert %Receipt{message_id: message_id, via: :local, route_id: route_id} = completed.result
    assert message_id == effect.id
    assert route_id == "local:spectre://acme/tao"
    assert executed_result.state.pending_effects == []

    assert_receive {:inbound_result, %InboundResult{} = inbound}
    assert inbound.envelope.id == effect.id
    assert inbound.envelope.payload.data == %{"topic" => "boats"}
    assert {:reply, receiver_result} = inbound.turn.decision
    assert receiver_result.reply_text == "accepted:research.perform"

    completion =
      Envelope.new!(
        from: "spectre://acme/tao",
        to: "spectre://acme/anna",
        relates_to: effect.id,
        payload: %{type: "research.completed", data: %{summary: "done"}}
      )

    assert {:ok, correlated_state, resolved} =
             Spectre.Pulse.correlate(executed_result.state, completion)

    assert resolved.status == :resolved
    assert PulseState.expectations(correlated_state)[effect.id].resolved_by == completion.id
  end

  test "REST server binding returns 202 technical receipt and uses the same inbound bridge" do
    envelope =
      Envelope.new!(
        from: "spectre://acme/anna",
        to: "spectre://acme/tao",
        act: :request,
        payload: %{type: "research.perform", data: %{topic: "nautica"}}
      )

    {:ok, body} = JSON.encode(envelope, [])

    response =
      REST.handle_request(body, %{"authorization" => "Bearer test"}, {127, 0, 0, 1},
        authenticator: fn headers, _peer ->
          assert headers["authorization"] == "Bearer test"
          {:ok, "spectre://acme/anna", %{token: :verified}}
        end
      )

    assert response.status == 202
    assert {:ok, receipt} = response.body |> Jason.decode!() |> Receipt.new()
    assert receipt.message_id == envelope.id
    assert receipt.status == :accepted
  end

  test "WebSocket frames resolve their recipient from the local subscriptions" do
    envelope =
      Envelope.new!(
        from: "spectre://acme/anna",
        to: "spectre://acme/tao",
        act: :request,
        payload: %{type: "research.perform", data: %{topic: "nautica"}}
      )

    {:ok, frame} = JSON.encode(envelope, [])

    assert {:ok, %InboundResult{} = inbound} =
             WebSocket.handle_frame(frame, %{
               authenticated_identity: envelope.from,
               peer: self(),
               verified: %{connection: :authenticated}
             })

    assert inbound.target == Receiver
    assert inbound.context.binding == :websocket
    assert inbound.input.meta.pulse.verified.connection == :authenticated
  end

  test "PubSub messages resolve their recipient from the local subscriptions" do
    envelope =
      Envelope.new!(
        from: "spectre://acme/anna",
        to: "spectre://acme/tao",
        payload: %{type: "research.perform", data: %{topic: "nautica"}}
      )

    assert {:ok, %Receipt{message_id: message_id, via: :pub_sub}} =
             PubSub.handle_message(
               {:spectre_pulse, envelope},
               %{
                 authenticated_identity: envelope.from,
                 peer: "research-topic",
                 verified: %{broker: :authenticated}
               }
             )

    assert message_id == envelope.id
  end

  test "REST server fails closed without a connection authenticator" do
    envelope =
      Envelope.new!(
        from: "spectre://acme/anna",
        to: "spectre://acme/tao",
        payload: %{type: "research.perform", data: %{}}
      )

    {:ok, body} = JSON.encode(envelope, [])
    response = REST.handle_request(body, %{}, :peer, target: Receiver)

    assert response.status == 401
    assert Jason.decode!(response.body)["error"] == "authentication"
  end
end

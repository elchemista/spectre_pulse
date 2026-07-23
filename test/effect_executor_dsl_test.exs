defmodule Spectre.Pulse.EffectExecutorDSLTest.AcceptTransport do
  @behaviour Spectre.Pulse.Transport

  alias Spectre.Pulse.Receipt

  @impl true
  def deliver(route, envelope, _opts) do
    send(route.target, {:effect_delivered, envelope})
    {:ok, Receipt.accepted(envelope.id, via: :effect_test, route_id: route.id)}
  end
end

defmodule Spectre.Pulse.EffectExecutorDSLTest.Builders do
  def build(_input, _context, suffix), do: {:ok, %{source: "mfa" <> suffix}}
end

defmodule Spectre.Pulse.EffectExecutorDSLTest.Agent do
  use Spectre.Agent
  use Spectre.Pulse

  pulsing do
    identity("spectre://effects/agent")
    contact(:receiver, "spectre://effects/receiver")
  end

  def build_one(input), do: %{source: input.text}
  def build_two(input, context), do: {:ok, %{source: input.text, agent: context.agent}}
end

defmodule Spectre.Pulse.EffectExecutorDSLTest do
  use ExUnit.Case, async: true

  alias Spectre.Pulse.Contact
  alias Spectre.Pulse.ContactBook
  alias Spectre.Pulse.DSL
  alias Spectre.Pulse.EffectBuilder
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Executor
  alias Spectre.Pulse.Expectation
  alias Spectre.Pulse.Route
  alias Spectre.Pulse.State, as: PulseState

  alias __MODULE__.AcceptTransport
  alias __MODULE__.Agent
  alias __MODULE__.Builders

  setup do
    input = %Spectre.Input{text: "work", meta: %{}, raw: "work"}

    context = %Spectre.Context{
      agent: Agent,
      input: input,
      state: %Spectre.State{},
      opts: []
    }

    %{input: input, context: context}
  end

  test "effect builder stages default data and trusted origin", %{input: input, context: context} do
    assert {:ok, result} =
             EffectBuilder.stage(Agent, input, context,
               to: :receiver,
               type: "effects.perform",
               data: %{source: :default},
               act: "request",
               id: Spectre.Identity.uuid7()
             )

    effect = Spectre.State.pending_effect(result.state)
    assert effect.kind == :pulse
    assert effect.name == :send
    assert effect.owner == Agent
    assert effect.scope == :agent
    assert effect.payload.to == "spectre://effects/receiver"
    assert effect.payload.data == %{source: :default}
    assert effect.payload.act == :request
    assert [%{type: :pulse_effect_staged}] = result.events

    routed_context = %{
      context
      | route: %Spectre.Route{owner: Agent, scope: :agent, label: :routed}
    }

    assert {:ok, routed} =
             EffectBuilder.stage(Agent, input, routed_context,
               to: :receiver,
               type: "effects.routed"
             )

    routed_effect = Spectre.State.pending_effect(routed.state)
    assert routed_effect.owner == Agent
    assert routed_effect.scope == :agent
  end

  test "effect builder supports named, anonymous and MFA data builders", %{
    input: input,
    context: context
  } do
    builders = [
      {:build_one, %{source: "work"}},
      {:build_two, %{source: "work", agent: Agent}},
      {fn received, ctx -> %{source: received.text, agent: ctx.agent} end,
       %{source: "work", agent: Agent}},
      {{Builders, :build, ["-builder"]}, %{source: "mfa-builder"}}
    ]

    for {builder, expected} <- builders do
      assert {:ok, result} =
               EffectBuilder.stage(Agent, input, context,
                 to: :receiver,
                 type: "effects.perform",
                 build: builder
               )

      assert Spectre.State.pending_effect(result.state).payload.data == expected
    end
  end

  test "effect builder normalizes builder failures", %{input: input, context: context} do
    builders = [
      {:missing_builder, {:undefined_pulse_builder, Agent, :missing_builder}},
      {:invalid, {:undefined_pulse_builder, Agent, :invalid}},
      {123, {:invalid_pulse_builder, 123}},
      {fn _input, _context -> {:error, :builder_failed} end, :builder_failed},
      {fn _input, _context -> raise "builder crash" end,
       {:pulse_builder_exception, RuntimeError}},
      {fn _input, _context -> throw(:builder_throw) end,
       {:pulse_builder_exit, :throw, :builder_throw}}
    ]

    for {builder, expected} <- builders do
      assert {:error, actual} =
               EffectBuilder.stage(Agent, input, context,
                 to: :receiver,
                 type: "effects.perform",
                 build: builder
               )

      case {expected, actual} do
        {{:pulse_builder_exception, RuntimeError}, {:pulse_builder_exception, %RuntimeError{}}} ->
          :ok

        _other ->
          assert actual == expected
      end
    end
  end

  test "effect builder tracks reply and typed expectations", %{input: input, context: context} do
    tracking = [
      {:reply, :reply},
      {true, :reply},
      {{:type, "effects.completed"}, {:type, "effects.completed"}},
      {"effects.completed", {:type, "effects.completed"}}
    ]

    for {expect, waiting_for} <- tracking do
      assert {:ok, result} =
               EffectBuilder.stage(Agent, input, context,
                 to: :receiver,
                 type: "effects.perform",
                 expect: expect,
                 expectation_metadata: %{source: :test}
               )

      effect = Spectre.State.pending_effect(result.state)

      assert %Expectation{waiting_for: ^waiting_for, metadata: %{source: :test}} =
               PulseState.expectations(result.state)[effect.id]

      assert Enum.any?(result.events, &(&1.type == :pulse_expectation_opened))
    end

    for expect <- [false, nil] do
      assert {:ok, result} =
               EffectBuilder.stage(Agent, input, context,
                 to: :receiver,
                 type: "effects.perform",
                 expect: expect
               )

      assert PulseState.expectations(result.state) == %{}
    end

    assert {:ok, tracked} =
             EffectBuilder.stage(Agent, input, context,
               to: :receiver,
               type: "effects.perform",
               track: true
             )

    tracked_effect = Spectre.State.pending_effect(tracked.state)
    assert PulseState.expectations(tracked.state)[tracked_effect.id].waiting_for == :reply
  end

  test "effect builder resolves sender and incoming relation from inbound metadata", %{
    input: input,
    context: context
  } do
    incoming_id = Spectre.Identity.uuid7()

    inbound = %{
      input
      | meta: %{
          pulse: %{
            from: "spectre://effects/sender",
            message_id: incoming_id
          }
        }
    }

    assert {:ok, result} =
             EffectBuilder.stage(Agent, inbound, %{context | input: inbound},
               to: :sender,
               relates_to: :incoming,
               type: "effects.reply"
             )

    effect = Spectre.State.pending_effect(result.state)
    assert effect.payload.to == "spectre://effects/sender"
    assert effect.payload.relates_to == incoming_id

    assert {:error, :pulse_sender_not_available} =
             EffectBuilder.stage(Agent, input, context,
               to: :sender,
               type: "effects.reply"
             )

    assert {:error, :incoming_pulse_message_not_available} =
             EffectBuilder.stage(Agent, input, context,
               to: :receiver,
               relates_to: :incoming,
               type: "effects.reply"
             )
  end

  test "effect builder rejects missing and invalid semantic options", %{
    input: input,
    context: context
  } do
    cases = [
      {[type: "effects.perform"], :pulse_recipient_required},
      {[to: :receiver], :pulse_type_required},
      {[to: :receiver, type: :invalid], {:invalid_pulse_type, :invalid}},
      {[to: :receiver, type: "effects.perform", act: :invalid], {:unsupported_act, :invalid}},
      {[to: :receiver, type: "effects.perform", expect: :invalid],
       {:invalid_pulse_expectation, :invalid}},
      {[to: :unknown, type: "effects.perform"], {:unknown_contact, :unknown}}
    ]

    for {opts, reason} <- cases do
      assert {:error, actual} = EffectBuilder.stage(Agent, input, context, opts)

      case actual do
        %Error{reason: actual_reason} -> assert actual_reason == reason
        _other -> assert actual == reason
      end
    end

    assert {:ok, first} =
             EffectBuilder.stage(Agent, input, context,
               to: :receiver,
               type: "effects.perform"
             )

    pending = Spectre.State.pending_effect(first.state)

    assert {:error, {:pending_effect_not_resolved, id, :pending}} =
             EffectBuilder.stage(Agent, input, %{context | state: first.state},
               to: :receiver,
               type: "effects.perform"
             )

    assert id == pending.id
  end

  test "executor completes successful delivery and preserves result context", %{
    input: input,
    context: context
  } do
    assert {:ok, staged} =
             EffectBuilder.stage(Agent, input, context,
               to: :receiver,
               type: "effects.perform",
               data: %{value: 1}
             )

    route =
      Route.new!(
        id: "effect-route",
        address: "spectre://effects/receiver",
        transport: AcceptTransport,
        target: self()
      )

    staged = %{staged | events: [%{type: :before_execution}], metadata: %{before: true}}

    assert {:ok, executed} = Executor.execute(Agent, staged, routes: [route])

    assert [%Spectre.Effect{status: :completed, result: %Spectre.Pulse.Receipt{}}] =
             executed.effects

    assert Enum.any?(executed.events, &(&1.type == :before_execution))
    assert Enum.any?(executed.events, &(&1.type == :pulse_delivery_accepted))
    assert executed.metadata.before
    assert executed.metadata.pulse_execution_transition

    assert_receive {:effect_delivered, envelope}
    assert envelope.id == hd(executed.effects).id
    assert envelope.payload.type == "effects.perform"

    assert {:ok, missing} = Executor.execute_pending(%Spectre.State{}, Agent, input: input)
    assert [%{type: :pulse_effect_missing}] = missing.events
  end

  test "executor updates turns and records unambiguous delivery failure", %{
    input: input,
    context: context
  } do
    assert {:ok, staged} =
             EffectBuilder.stage(Agent, input, context,
               to: :receiver,
               type: "effects.perform"
             )

    turn = Spectre.Turn.from_result(Agent, input, [], staged)

    route =
      Route.new!(
        address: "spectre://effects/receiver",
        transport: AcceptTransport,
        target: self()
      )

    assert {:ok, executed_turn} = Executor.execute_turn(turn, routes: [route])
    assert {:completed, %Spectre.Effect{status: :completed}, _result} = executed_turn.decision
    assert_receive {:effect_delivered, _envelope}

    assert {:ok, failed} = Executor.execute_pending(staged.state, Agent)
    assert [%Spectre.Effect{status: :failed, error: %Error{}}] = failed.effects
    assert [%{type: :pulse_delivery_failed, error: %Error{}}] = failed.events
  end

  test "executor rejects policy, status, kind and origin violations", %{
    input: input,
    context: context
  } do
    assert {:ok, staged} =
             EffectBuilder.stage(Agent, input, context,
               to: :receiver,
               type: "effects.perform"
             )

    effect = Spectre.State.pending_effect(staged.state)

    invalid_effects = [
      {%{effect | status: :waiting_policy}, {:effect_not_approved, effect.id}},
      {%{effect | status: :completed}, {:effect_not_executable, effect.id, :completed}},
      {%{effect | kind: :action}, {:unsupported_effect_kind, :action}}
    ]

    for {invalid, reason} <- invalid_effects do
      assert {:error, %Error{reason: ^reason}} =
               Executor.deliver(Agent, invalid, %Spectre.State{})
    end

    assert {:error, {:effect_scope_missing, effect_id}} =
             Executor.execute_pending(
               %{staged.state | pending_effects: [%{effect | scope: nil}]},
               Agent
             )

    assert effect_id == effect.id

    assert {:error, {:effect_owner_mismatch, String, Agent}} =
             Executor.execute_pending(
               %{staged.state | pending_effects: [%{effect | owner: String}]},
               Agent
             )

    assert {:error, {:effect_scope_unresolvable, {:skill, :missing}, _reason}} =
             Executor.execute_pending(
               %{staged.state | pending_effects: [%{effect | scope: {:skill, :missing}}]},
               Agent
             )
  end

  test "DSL rewrites pulse calls, route evidence and leaves unrelated AST untouched" do
    assert {:run, _, [:__spectre_pulse_stage__, [spectre_pulse: [to: :receiver]]]} =
             DSL.rewrite(quote(do: pulse(:receiver)))

    rewritten =
      DSL.rewrite(
        quote do
          on :message do
            pulse(:receiver, type: "effects.perform")
          end
        end
      )

    assert Macro.to_string(rewritten) =~ "__spectre_pulse_stage__"

    opts = DSL.rewrite_route_opts(pulse: "effects.perform", regex: ~r/effects/)
    assert {:pulse_type, "effects.perform"} in opts[:checks]
    assert Regex.source(opts[:regex]) == "effects"
    assert opts[:cache] == false

    assert DSL.rewrite_route_opts(label: :plain) == [label: :plain]
    assert DSL.rewrite_route_opts(:unchanged) == :unchanged
    assert DSL.rewrite(quote(do: untouched())) == quote(do: untouched())
  end

  test "Pulse DSL refuses invalid installation order and non-keyword use options" do
    suffix = System.unique_integer([:positive])

    assert_raise ArgumentError, ~r/use Spectre.Agent must appear before use Spectre.Pulse/, fn ->
      Code.compile_string("""
      defmodule Spectre.Pulse.InvalidOrder#{suffix} do
        use Spectre.Pulse
      end
      """)
    end

    assert_raise ArgumentError, ~r/use Spectre.Pulse expects a keyword list/, fn ->
      Code.compile_string("""
      defmodule Spectre.Pulse.InvalidOptions#{suffix} do
        use Spectre.Agent
        use Spectre.Pulse, :invalid
      end
      """)
    end
  end

  test "contacts and contact books reject malformed data and keep indexes coherent" do
    address = "spectre://contacts/one"

    invalid_contacts = [
      {%{key: nil, identity: address}, {:invalid_contact_key, nil}},
      {%{key: "", identity: address}, {:invalid_contact_key, ""}},
      {%{key: 1, identity: address}, {:invalid_contact_key, 1}},
      {%{key: :one, identity: address, capabilities: :invalid},
       {:invalid_capabilities, :invalid}},
      {%{key: :one, identity: address, metadata: []}, {:invalid_contact_metadata, []}},
      {%{key: :one, identity: address, routes: :invalid}, {:invalid_contact_routes, :invalid}}
    ]

    for {attrs, reason} <- invalid_contacts do
      assert {:error, %Error{reason: ^reason}} = Contact.new(attrs)
    end

    mismatched_route = Route.local("spectre://contacts/other", self())

    assert {:error, %Error{reason: :contact_route_identity_mismatch}} =
             Contact.new(key: :one, identity: address, routes: [mismatched_route])

    assert {:error, %Error{reason: {:invalid_contact, :invalid}}} = Contact.new(:invalid)
    assert_raise ArgumentError, fn -> Contact.new!(nil, address) end

    original = Contact.new!(:one, address, capabilities: [:a, :a])
    assert original.capabilities == [:a]
    book = ContactBook.new!([original])
    replacement = Contact.new!(:one, "spectre://contacts/replacement")
    assert {:ok, replaced} = ContactBook.put(book, replacement)
    assert :error = ContactBook.fetch(replaced, address)
    assert {:ok, ^replacement} = ContactBook.fetch(replaced, :one)
    assert {:ok, ^replacement} = ContactBook.fetch(replaced, replacement.identity)
    assert :error = ContactBook.fetch(replaced, %{invalid: true})

    assert {:ok, "spectre://contacts/direct"} =
             ContactBook.resolve(replaced, "spectre://contacts/direct")

    assert ContactBook.routes(replaced, :missing) == []
    assert ContactBook.delete(replaced, :missing) == replaced
    assert [^replacement] = ContactBook.find(replaced, identity: replacement.identity)
    assert {:ok, ^replaced} = ContactBook.merge([replaced])
  end
end

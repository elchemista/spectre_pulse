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
    network(Spectre.Pulse.Network.Routed)
    contact(:receiver, "spectre://effects/receiver")
  end

  flow :default_pulse do
    on :default_pulse, regex: ~r/^default pulse$/ do
      pulse(:receiver)
    end
  end

  interrupt :pulse_block, pulse: "effects.interrupt.block" do
    run(:build_one)
  end

  interrupt(:pulse_compact,
    pulse: "effects.interrupt.compact",
    do: run(:build_one)
  )

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
  alias Spectre.Pulse.Fabric
  alias Spectre.Pulse.Handler
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

  test "effect builder scopes independent Instance lifecycle by Run", %{
    input: input,
    context: context
  } do
    first_context = %{
      context
      | opts: [instance_run_lifecycle?: true, run_id: "pulse-run-first"]
    }

    assert {:ok, first} =
             EffectBuilder.stage(Agent, input, first_context,
               to: :receiver,
               type: "effects.first",
               policy: :confirm_send
             )

    first_effect = Spectre.State.pending_effect(first.state, "pulse-run-first")
    first_awaitable = Spectre.State.open_policy_awaitable(first.state, "pulse-run-first")

    assert first_effect.run_id == "pulse-run-first"
    assert first_awaitable.run_id == "pulse-run-first"
    assert first.effects == [first_effect]

    second_context = %{
      context
      | state: first.state,
        opts: [instance_run_lifecycle?: true, run_id: "pulse-run-second"]
    }

    assert {:ok, second} =
             EffectBuilder.stage(Agent, input, second_context,
               to: :receiver,
               type: "effects.second",
               policy: :confirm_send
             )

    second_effect = Spectre.State.pending_effect(second.state, "pulse-run-second")
    second_awaitable = Spectre.State.open_policy_awaitable(second.state, "pulse-run-second")

    assert second_effect.run_id == "pulse-run-second"
    assert second_awaitable.run_id == "pulse-run-second"
    assert second.effects == [second_effect]

    assert Enum.map(second.state.pending_effects, & &1.run_id) == [
             "pulse-run-first",
             "pulse-run-second"
           ]

    assert second.state.awaitables
           |> Enum.filter(&(&1.status == :open))
           |> Enum.map(& &1.run_id) == ["pulse-run-first", "pulse-run-second"]

    assert {:error, {:pending_effect_not_resolved, id, :waiting_policy}} =
             EffectBuilder.stage(Agent, input, %{first_context | state: second.state},
               to: :receiver,
               type: "effects.duplicate"
             )

    assert id == first_effect.id
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

  test "effect builder rejects malformed boundaries before staging", %{
    input: input,
    context: context
  } do
    assert {:error, {:invalid_options, :invalid}} =
             EffectBuilder.stage(Agent, input, context, :invalid)

    assert {:error, {:invalid_options, [:invalid]}} =
             EffectBuilder.stage(Agent, input, context, [:invalid])

    other_context = %{context | agent: String}

    assert {:error, {:pulse_agent_context_mismatch, Agent, String}} =
             EffectBuilder.stage(Agent, input, other_context,
               to: :receiver,
               type: "effects.perform"
             )

    assert {:error, %Error{reason: {:invalid_pulse_stage, Agent, ^input, _, _}}} =
             EffectBuilder.stage(Agent, input, %{context | state: nil},
               to: :receiver,
               type: "effects.perform"
             )

    invalid_cases = [
      {[to: :receiver, type: "effects.perform", id: "invalid"], {:invalid_message_id, "invalid"}},
      {[to: :receiver, type: "INVALID"], {:invalid_payload_type, "INVALID"}},
      {[to: :receiver, type: "effects.perform", metadata: %{callback: fn -> :ok end}],
       {:metadata_not_encodable, Protocol.UndefinedError}},
      {[to: :receiver, type: "effects.perform", expect: true, due_at: :tomorrow],
       {:invalid_expectation_due_at, :tomorrow}},
      {[
         to: :receiver,
         type: "effects.perform",
         expect: true,
         expectation_metadata: :invalid
       ], {:invalid_expectation_metadata, :invalid}},
      {[to: :receiver, type: "effects.perform", build: {Builders, :missing, []}],
       {:undefined_pulse_builder, Builders, :missing, 2}}
    ]

    for {opts, expected} <- invalid_cases do
      assert {:error, actual} = EffectBuilder.stage(Agent, input, context, opts)

      case {expected, actual} do
        {{:metadata_not_encodable, Protocol.UndefinedError},
         %Error{reason: {:metadata_not_encodable, %Protocol.UndefinedError{}}}} ->
          :ok

        {reason, %Error{reason: reason}} ->
          :ok

        {reason, reason} ->
          :ok

        other ->
          flunk("unexpected boundary result: #{inspect(other)}")
      end
    end

    id = Spectre.Identity.uuid7()

    assert {:error, %Error{reason: :message_cannot_relate_to_itself}} =
             EffectBuilder.stage(Agent, input, context,
               to: :receiver,
               type: "effects.perform",
               id: id,
               relates_to: id
             )

    assert {:error, :pulse_stage_options_required} =
             Handler.stage(input, context)
  end

  test "executor delegates successful delivery to the canonical Spectre lifecycle", %{
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

    assert [%{type: :effect_completed, kind: :pulse, name: :send}] = executed.events
    assert executed.metadata.before
    assert executed.metadata.execution_transition

    assert_receive {:effect_delivered, envelope}
    assert envelope.id == hd(executed.effects).id
    assert envelope.payload.type == "effects.perform"

    assert {:ok, missing} = Executor.execute_pending(%Spectre.State{}, Agent, input: input)
    assert [%{type: :effect_missing}] = missing.events

    empty_result = %Spectre.Result{state: %Spectre.State{}, input: input}
    assert {:ok, %Spectre.Result{}} = Executor.execute(Agent, empty_result)
    assert {:ok, %Spectre.Result{}} = Spectre.Pulse.execute(Agent, empty_result)

    empty_turn = Spectre.Turn.from_result(Agent, input, [], empty_result)
    assert {:ok, %Spectre.Turn{}} = Executor.execute_turn(empty_turn)

    ownerless_state = %{
      staged.state
      | pending_effects: [
          %{Spectre.State.pending_effect(staged.state) | owner: nil}
        ]
    }

    assert {:ok, ownerless} =
             Executor.execute_pending(ownerless_state, Agent, routes: [route])

    assert [%Spectre.Effect{status: :completed}] = ownerless.effects
    assert_receive {:effect_delivered, _ownerless_envelope}
  end

  test "executor uses only compiled Stack transports and validates restored effects", %{
    input: input,
    context: context
  } do
    assert {:ok, staged} =
             EffectBuilder.stage(Agent, input, context,
               to: :receiver,
               type: "effects.perform"
             )

    route =
      Route.new!(
        id: "trusted-effect-route",
        address: "spectre://effects/receiver",
        transport: AcceptTransport,
        target: self()
      )

    injected = String.to_atom("injected_#{System.unique_integer([:positive])}")

    assert {:ok, _executed} =
             Executor.execute(Agent, staged,
               routes: [route],
               transports: [%{id: injected, module: AcceptTransport}]
             )

    refute Map.has_key?(Fabric.transports(), injected)
    assert_receive {:effect_delivered, _envelope}

    effect = Spectre.State.pending_effect(staged.state)

    assert {:error, %Error{reason: {:invalid_pulse_effect_payload, :invalid}}} =
             Executor.deliver(Agent, %{effect | payload: :invalid}, %Spectre.State{})

    assert {:error, %Error{reason: {:pulse_effect_field_missing, :to}}} =
             Executor.deliver(
               Agent,
               %{effect | payload: Map.delete(effect.payload, :to)},
               %Spectre.State{}
             )

    assert {:error, %Error{reason: {:invalid_options, :invalid}}} =
             Executor.deliver(Agent, effect, %Spectre.State{}, :invalid)

    assert {:error, {:invalid_pulse_assigns, :invalid}} =
             Executor.execute_pending(staged.state, Agent, assigns: :invalid)
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

    assert {:awaiting, ref} = turn.observable
    assert %Spectre.Invocation{ref: ^ref, operation: {:pulse, :send}} = turn.boundary

    route =
      Route.new!(
        address: "spectre://effects/receiver",
        transport: AcceptTransport,
        target: self()
      )

    assert {:ok, executed_turn} = Executor.execute_turn(turn, routes: [route])
    assert {:completed, %Spectre.Effect{status: :completed}, _result} = executed_turn.decision
    assert {:reply, output, completed_ref} = executed_turn.observable
    assert output == executed_turn.result.reply_text
    assert executed_turn.ref == completed_ref

    assert %Spectre.Run.Boundary{
             kind: :reply,
             ref: ^completed_ref,
             output: ^output
           } = executed_turn.boundary

    refute executed_turn.boundary == turn.boundary
    refute completed_ref == turn.ref
    assert_receive {:effect_delivered, _envelope}

    assert {:ok, failed} = Executor.execute_pending(staged.state, Agent)
    assert [%Spectre.Effect{status: :failed, error: %Error{}}] = failed.effects

    assert [%{type: :effect_failed, kind: :pulse, name: :send, error: %Error{}}] =
             failed.events
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

    assert {:error, {:effect_owner_mismatch, ^effect_id, String, Agent}} =
             Executor.execute_pending(
               %{staged.state | pending_effects: [%{effect | owner: String}]},
               Agent
             )

    assert {:error, {:effect_scope_unresolvable, ^effect_id, {:skill, :missing}, _reason}} =
             Executor.execute_pending(
               %{staged.state | pending_effects: [%{effect | scope: {:skill, :missing}}]},
               Agent
             )
  end

  test "DSL rewrites pulse calls, route evidence and leaves unrelated AST untouched" do
    assert {:run, _,
            [
              :stage,
              [
                handler_owner: Spectre.Pulse.Handler,
                spectre_pulse: [to: :receiver]
              ]
            ]} =
             DSL.rewrite(quote(do: pulse(:receiver)))

    rewritten =
      DSL.rewrite(
        quote do
          on :message do
            pulse(:receiver, type: "effects.perform")
          end
        end
      )

    assert Macro.to_string(rewritten) =~ "Spectre.Pulse.Handler"

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

    assert_raise ArgumentError, ~r/use Spectre.Pulse expects a keyword list/, fn ->
      Code.compile_string("""
      defmodule Spectre.Pulse.InvalidKeywordOptions#{suffix} do
        use Spectre.Agent
        use Spectre.Pulse, [:invalid]
      end
      """)
    end
  end

  test "contacts and contact books reject malformed data and keep indexes coherent" do
    address = "spectre://contacts/one"
    assert {:ok, %Contact{key: :default, identity: ^address}} = Contact.new(:default, address)

    assert {:error, %Error{reason: :route_target_required}} =
             Contact.new(:invalid_route, address,
               routes: [
                 %{
                   id: "invalid-contact-route",
                   address: address,
                   transport: AcceptTransport,
                   target: nil
                 }
               ]
             )

    invalid_contacts = [
      {%{key: nil, identity: address}, {:invalid_contact_key, nil}},
      {%{key: "", identity: address}, {:invalid_contact_key, ""}},
      {%{key: <<255>>, identity: address}, {:invalid_contact_key, <<255>>}},
      {%{key: 1, identity: address}, {:invalid_contact_key, 1}},
      {%{key: :one, identity: address, display_name: <<255>>}, {:invalid_display_name, <<255>>}},
      {%{key: :one, identity: address, capabilities: :invalid},
       {:invalid_capabilities, :invalid}},
      {%{key: :one, identity: address, capabilities: [<<255>>]},
       {:invalid_capabilities, [<<255>>]}},
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

    assert {:ok, %ContactBook{}} = ContactBook.new()
    assert %ContactBook{} = ContactBook.new!()

    assert_raise ArgumentError, fn ->
      ContactBook.new!([%{key: nil, identity: address}])
    end

    named = Contact.new!("named", "spectre://contacts/named")
    assert {:ok, named_book} = ContactBook.new([named])
    assert {:ok, ^named} = ContactBook.fetch(named_book, "named")

    renamed = %{named | display_name: "Named"}
    assert {:ok, renamed_book} = ContactBook.put(named_book, renamed)
    assert {:ok, ^renamed} = ContactBook.fetch(renamed_book, "named")
  end
end

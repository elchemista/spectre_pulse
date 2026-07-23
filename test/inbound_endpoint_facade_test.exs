defmodule Spectre.Pulse.InboundEndpointFacadeTest.EndpointCallbacks do
  def handle_two(_envelope, _context), do: :ok
  def handle_extra(_envelope, _context, reply), do: reply

  def notify(result, pid, reply) do
    send(pid, {:endpoint_result, result})
    reply
  end
end

defmodule Spectre.Pulse.InboundEndpointFacadeTest.ThreeArityEndpoint do
  def handle_pulse(_envelope, _context, opts), do: Keyword.get(opts, :reply, :ok)
end

defmodule Spectre.Pulse.InboundEndpointFacadeTest.TwoArityEndpoint do
  def handle_pulse(_envelope, _context), do: :ok
end

defmodule Spectre.Pulse.InboundEndpointFacadeTest.Resolver do
  def resolve_target(_address, context), do: context.metadata[:resolver_reply]
end

defmodule Spectre.Pulse.InboundEndpointFacadeTest.Authorizer do
  def authorize(_envelope, context, _target), do: context.metadata[:authorization_reply]
end

defmodule Spectre.Pulse.InboundEndpointFacadeTest.Callbacks do
  def map_input(_envelope, _context, base, suffix) do
    %{base | text: base.text <> suffix}
  end

  def state_scope(_target, _envelope, context, suffix) do
    {:ok, {:custom_scope, context.binding, suffix}}
  end
end

defmodule Spectre.Pulse.InboundEndpointFacadeTest.BaseAgent do
  use Spectre.Agent
  use Spectre.Pulse

  pulsing do
    identity("spectre://inbound/base")
    advertise(capabilities: [:inbound])
  end

  flow :inbound do
    on :perform, pulse: "inbound.perform" do
      run(:perform)
    end
  end

  def perform(input, _context), do: "received:" <> input.text
end

defmodule Spectre.Pulse.InboundEndpointFacadeTest.PlainAgent do
  use Spectre.Agent

  flow :plain do
    on :perform, regex: ~r/./ do
      run(:perform)
    end
  end

  def perform(input, _context), do: "plain:" <> input.text
end

defmodule Spectre.Pulse.InboundEndpointFacadeTest.ExternalDirectory do
  alias Spectre.Pulse.Contact
  alias Spectre.Pulse.Route

  def resolve(:external, _opts), do: {:ok, "spectre://facade/external"}
  def resolve(_reference, _opts), do: :error

  def routes("spectre://facade/external", opts) do
    [Route.web_socket("spectre://facade/external", Keyword.get(opts, :target, self()))]
  end

  def routes(_address, _opts), do: []

  def contacts(_opts) do
    [
      Contact.new!(:external, "spectre://facade/external", capabilities: [:external])
    ]
  end
end

defmodule Spectre.Pulse.InboundEndpointFacadeTest.ConflictDirectory do
  alias Spectre.Pulse.Contact

  def contacts(_opts) do
    [Contact.new!(:other_name, "spectre://facade/static")]
  end
end

defmodule Spectre.Pulse.InboundEndpointFacadeTest.FacadeAgent do
  use Spectre.Agent
  use Spectre.Pulse

  pulsing do
    identity("spectre://facade/agent")
    directory(Spectre.Pulse.InboundEndpointFacadeTest.ExternalDirectory)

    contact(:static, "spectre://facade/static", capabilities: [:static])
  end
end

defmodule Spectre.Pulse.InboundEndpointFacadeTest.ConflictAgent do
  use Spectre.Agent
  use Spectre.Pulse

  pulsing do
    identity("spectre://facade/conflict")
    directory(Spectre.Pulse.InboundEndpointFacadeTest.ConflictDirectory)
    contact(:static, "spectre://facade/static")
  end
end

defmodule Spectre.Pulse.InboundEndpointFacadeTest do
  use ExUnit.Case, async: false

  alias Spectre.Pulse.Config
  alias Spectre.Pulse.Contact
  alias Spectre.Pulse.ContactBook
  alias Spectre.Pulse.Endpoint
  alias Spectre.Pulse.Envelope
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Expectation
  alias Spectre.Pulse.Fabric
  alias Spectre.Pulse.Inbound
  alias Spectre.Pulse.Inbound.Result, as: InboundResult
  alias Spectre.Pulse.InboundContext
  alias Spectre.Pulse.Reachability
  alias Spectre.Pulse.Receipt
  alias Spectre.Pulse.State, as: PulseState

  alias __MODULE__.Authorizer
  alias __MODULE__.BaseAgent
  alias __MODULE__.Callbacks
  alias __MODULE__.ConflictAgent
  alias __MODULE__.EndpointCallbacks
  alias __MODULE__.FacadeAgent
  alias __MODULE__.PlainAgent
  alias __MODULE__.Resolver
  alias __MODULE__.ThreeArityEndpoint
  alias __MODULE__.TwoArityEndpoint

  setup do
    envelope =
      Envelope.new!(
        from: "spectre://inbound/sender",
        to: "spectre://inbound/base",
        act: :request,
        payload: %{type: "inbound.perform", data: %{"text" => "work"}}
      )

    context = %{
      authenticated_identity: envelope.from,
      binding: :test,
      target: BaseAgent,
      verified: %{connection: :verified}
    }

    %{envelope: envelope, context: context}
  end

  test "endpoint dispatcher accepts every endpoint shape", %{envelope: envelope} do
    context = %{authenticated_identity: envelope.from}

    three = fn _received, _context, _opts -> {:ok, :accepted} end
    assert {:ok, %Receipt{message_id: message_id}} = Endpoint.accept(three, envelope, context)
    assert message_id == envelope.id

    assert {:ok, %Receipt{}} =
             Endpoint.accept({EndpointCallbacks, :handle_two}, envelope, context)

    assert {:ok, %Receipt{}} =
             Endpoint.accept(
               {EndpointCallbacks, :handle_extra, [:ok]},
               envelope,
               context
             )

    assert {:ok, %Receipt{}} =
             Endpoint.accept(ThreeArityEndpoint, envelope, context, reply: :ok)

    assert {:ok, %Receipt{}} =
             Endpoint.accept(TwoArityEndpoint, envelope, context)

    receipt = Receipt.accepted(envelope.id, via: :custom)

    assert {:ok, ^receipt} =
             Endpoint.accept(fn _received, _context -> {:ok, receipt} end, envelope, context)

    assert {:error, %Error{reason: {:invalid_endpoint, %{invalid: true}}}} =
             Endpoint.accept(%{invalid: true}, envelope, context)

    assert {:error, %Error{reason: {:invalid_endpoint, String}}} =
             Endpoint.accept(String, envelope, context)
  end

  test "endpoint dispatcher normalizes callback failures and invalid replies", %{
    envelope: envelope
  } do
    existing = Error.not_sent(:inbound, :existing)

    assert {:error, ^existing} =
             Endpoint.accept(fn _envelope, _context -> {:error, existing} end, envelope, %{})

    assert {:error, %Error{outcome: :outcome_unknown, reason: :failure}} =
             Endpoint.accept(fn _envelope, _context -> {:error, :failure} end, envelope, %{})

    assert {:error, %Error{reason: {:invalid_endpoint_result, :invalid}}} =
             Endpoint.accept(fn _envelope, _context -> :invalid end, envelope, %{})

    assert {:error, %Error{reason: {:endpoint_exception, %RuntimeError{}}}} =
             Endpoint.accept(fn _envelope, _context -> raise "endpoint crash" end, envelope, %{})

    assert {:error, %Error{reason: {:endpoint_exit, :throw, :endpoint_throw}}} =
             Endpoint.accept(fn _envelope, _context -> throw(:endpoint_throw) end, envelope, %{})
  end

  test "endpoint result hooks support functions, MFA and every failure contract", %{
    envelope: envelope
  } do
    receipt = Receipt.accepted(envelope.id)

    result = %InboundResult{
      envelope: envelope,
      context: InboundContext.new(%{}),
      canonical_sender: envelope.from,
      target: BaseAgent,
      input: %Spectre.Input{},
      turn: %Spectre.Turn{},
      receipt: receipt
    }

    assert {:ok, ^receipt} =
             Endpoint.accept(fn _envelope, _context -> result end, envelope, %{},
               on_result: fn received ->
                 send(self(), {:function_result, received})
                 {:ok, :notified}
               end
             )

    assert_receive {:function_result, ^result}

    assert {:ok, ^receipt} =
             Endpoint.accept(fn _envelope, _context -> {:ok, result} end, envelope, %{},
               on_result: {EndpointCallbacks, :notify, [self(), :ok]}
             )

    assert_receive {:endpoint_result, ^result}

    failures = [
      {fn _result -> {:error, :hook_failed} end, :hook_failed},
      {fn _result -> :invalid end, {:invalid_on_result_reply, :invalid}},
      {fn _result -> raise "hook crash" end, {:on_result_exception, RuntimeError}},
      {fn _result -> throw(:hook_throw) end, {:on_result_exit, :throw, :hook_throw}}
    ]

    for {hook, expected} <- failures do
      assert {:error, %Error{reason: {:on_result_failed, actual}}} =
               Endpoint.accept(fn _envelope, _context -> result end, envelope, %{},
                 on_result: hook
               )

      case {expected, actual} do
        {{:on_result_exception, RuntimeError}, {:on_result_exception, %RuntimeError{}}} -> :ok
        _other -> assert actual == expected
      end
    end

    assert {:error,
            %Error{
              reason: {:on_result_failed, {:invalid_on_result, :invalid}}
            }} =
             Endpoint.accept(fn _envelope, _context -> result end, envelope, %{},
               on_result: :invalid
             )
  end

  test "inbound maps authenticated and explicitly unauthenticated envelopes to inputs", %{
    envelope: envelope
  } do
    assert {:error, %Error{reason: :authenticated_identity_required}} =
             Inbound.to_input(envelope, %{})

    assert {:ok, input} =
             Inbound.to_input(envelope, %{}, allow_unauthenticated: true)

    refute input.meta.pulse.authenticated
    assert input.text == "work"

    binary = %{envelope | payload: %{envelope.payload | data: "plain text"}}

    assert {:ok, %{text: "plain text"}} =
             Inbound.to_input(binary, %{}, allow_unauthenticated: true)

    map = %{envelope | payload: %{envelope.payload | data: %{value: 1}}}
    assert {:ok, map_input} = Inbound.to_input(map, %{}, allow_unauthenticated: true)
    assert Jason.decode!(map_input.text) == %{"value" => 1}

    scalar = %{envelope | payload: %{envelope.payload | data: [:opaque]}}
    assert {:ok, scalar_input} = Inbound.to_input(scalar, %{}, allow_unauthenticated: true)
    assert scalar_input.text == "[\"opaque\"]"

    unencodable = %{envelope | payload: %{envelope.payload | data: fn -> :ok end}}
    assert {:ok, inspected} = Inbound.to_input(unencodable, %{}, allow_unauthenticated: true)
    assert inspected.text =~ "#Function"
  end

  test "input mapper accepts function and MFA forms and rejects malformed replies", %{
    envelope: envelope
  } do
    context = %{authenticated_identity: envelope.from}

    assert {:ok, %Spectre.Input{text: "mapped"}} =
             Inbound.to_input(envelope, context,
               input_mapper: fn _envelope, _context, base -> %{base | text: "mapped"} end
             )

    assert {:ok, %Spectre.Input{text: "work-mfa"}} =
             Inbound.to_input(envelope, context, input_mapper: {Callbacks, :map_input, ["-mfa"]})

    existing = Error.not_sent(:inbound, :existing)

    replies = [
      {{:ok, %Spectre.Input{text: "ok"}}, {:ok, "ok"}},
      {{:error, existing}, {:error, :existing}},
      {{:error, :mapping_failed}, {:error, :mapping_failed}},
      {:invalid, {:error, {:input_mapper_must_return_input, :invalid}}}
    ]

    for {reply, expected} <- replies do
      result =
        Inbound.to_input(envelope, context,
          input_mapper: fn _envelope, _context, _base -> reply end
        )

      case {result, expected} do
        {{:ok, %Spectre.Input{text: text}}, {:ok, text}} -> :ok
        {{:error, %Error{reason: reason}}, {:error, reason}} -> :ok
        _other -> flunk("unexpected mapper result: #{inspect(result)}")
      end
    end

    assert {:error, %Error{reason: {:invalid_input_mapper, :invalid}}} =
             Inbound.to_input(envelope, context, input_mapper: :invalid)

    assert {:error, %Error{reason: {:input_mapper, :exception, %RuntimeError{}}}} =
             Inbound.to_input(envelope, context,
               input_mapper: fn _envelope, _context, _base -> raise "mapper crash" end
             )

    assert {:error, %Error{reason: {:input_mapper, :throw, :mapper_throw}}} =
             Inbound.to_input(envelope, context,
               input_mapper: fn _envelope, _context, _base -> throw(:mapper_throw) end
             )
  end

  test "target resolvers normalize functions, modules, bare values and failures", %{
    envelope: envelope
  } do
    base = %{authenticated_identity: envelope.from, binding: :resolver}

    assert {:ok, %InboundResult{target: BaseAgent}} =
             Inbound.receive(envelope, base,
               target_resolver: fn _address, _context -> {:ok, BaseAgent} end
             )

    assert {:ok, %InboundResult{target: BaseAgent}} =
             Inbound.receive(envelope, base,
               target_resolver: fn _address, _context -> BaseAgent end
             )

    module_context =
      Map.put(base, :metadata, %{resolver_reply: {:ok, BaseAgent}})

    assert {:ok, %InboundResult{target: BaseAgent}} =
             Inbound.receive(envelope, module_context, target_resolver: Resolver)

    failures = [
      {fn _address, _context -> {:error, :resolver_failed} end, :resolver_failed},
      {fn _address, _context -> :error end, {:target_not_found, envelope.to}},
      {fn _address, _context -> nil end, {:target_not_found, envelope.to}},
      {:invalid, :target_not_found},
      {fn _address, _context -> raise "resolver crash" end,
       {:target_resolver, :exception, RuntimeError}},
      {fn _address, _context -> throw(:resolver_throw) end,
       {:target_resolver, :throw, :resolver_throw}}
    ]

    for {resolver, expected} <- failures do
      assert {:error, %Error{kind: :routing, reason: actual}} =
               Inbound.receive(envelope, base, target_resolver: resolver)

      case {expected, actual} do
        {{:target_resolver, :exception, RuntimeError},
         {:target_resolver, :exception, %RuntimeError{}}} ->
          :ok

        _other ->
          assert actual == expected
      end
    end
  end

  test "inbound enforces recipient and allowed payload types", %{
    envelope: envelope,
    context: context
  } do
    assert {:ok, %InboundResult{}} =
             Inbound.receive(envelope, context, allowed_types: ["inbound.perform"])

    assert {:error, %Error{reason: {:payload_type_not_allowed, "inbound.perform"}}} =
             Inbound.receive(envelope, context, allowed_types: ["other.perform"])

    assert {:error, %Error{reason: {:invalid_allowed_types, :invalid}}} =
             Inbound.receive(envelope, context, allowed_types: :invalid)

    wrong_recipient = %{envelope | to: "spectre://inbound/other"}

    assert {:error, %Error{reason: :recipient_identity_mismatch}} =
             Inbound.receive(wrong_recipient, context)
  end

  test "authorization supports arity-two, arity-three, modules and all failures", %{
    envelope: envelope,
    context: context
  } do
    assert {:ok, %InboundResult{}} =
             Inbound.receive(envelope, context, authorize: fn _envelope, _context -> true end)

    assert {:ok, %InboundResult{}} =
             Inbound.receive(envelope, context,
               authorize: fn _envelope, _context, _target -> :ok end
             )

    module_context =
      Map.put(context, :metadata, %{authorization_reply: true})

    assert {:ok, %InboundResult{}} =
             Inbound.receive(envelope, module_context, authorize: Authorizer)

    existing = Error.not_sent(:authorization, :existing)

    replies = [
      {false, :forbidden},
      {{:error, existing}, :existing},
      {{:error, :denied}, :denied},
      {:invalid, {:invalid_authorization_result, :invalid}}
    ]

    for {reply, reason} <- replies do
      assert {:error, %Error{kind: :authorization, reason: ^reason}} =
               Inbound.receive(envelope, context,
                 authorize: fn _envelope, _context, _target -> reply end
               )
    end

    assert {:error, %Error{reason: {:invalid_authorizer, String}}} =
             Inbound.receive(envelope, context, authorize: String)

    assert {:error, %Error{reason: {:invalid_authorizer, %{invalid: true}}}} =
             Inbound.receive(envelope, context, authorize: %{invalid: true})

    assert {:error, %Error{reason: {:authorization, :exception, %RuntimeError{}}}} =
             Inbound.receive(envelope, context,
               authorize: fn _envelope, _context, _target -> raise "authorization crash" end
             )

    assert {:error, %Error{reason: {:authorization, :throw, :authorization_throw}}} =
             Inbound.receive(envelope, context,
               authorize: fn _envelope, _context, _target -> throw(:authorization_throw) end
             )
  end

  test "explicit target configuration supports peer, function and MFA state scopes" do
    envelope =
      Envelope.new!(
        from: "spectre://inbound/sender",
        to: "spectre://inbound/plain",
        payload: %{type: "plain.perform", data: "plain"}
      )

    context = %{authenticated_identity: envelope.from, target: PlainAgent, binding: :scope}

    assert {:ok, peer} =
             Inbound.receive(envelope, context,
               target_identity: envelope.to,
               state_scope: :peer
             )

    assert peer.turn.opts[:conversation_id] ==
             {:pulse_peer, envelope.to, envelope.from}

    assert {:ok, function_scope} =
             Inbound.receive(envelope, context,
               target_identity: envelope.to,
               state_scope: fn _target, _envelope, _context -> :function_scope end
             )

    assert function_scope.turn.opts[:conversation_id] == :function_scope

    assert {:ok, mfa_scope} =
             Inbound.receive(envelope, context,
               target_identity: envelope.to,
               state_scope: {Callbacks, :state_scope, [:extra]}
             )

    assert mfa_scope.turn.opts[:conversation_id] == {:custom_scope, :scope, :extra}

    assert {:error, %Error{reason: :scope_failed}} =
             Inbound.receive(envelope, context,
               target_identity: envelope.to,
               state_scope: fn _target, _envelope, _context -> {:error, :scope_failed} end
             )

    assert {:error, %Error{reason: {:state_scope, :exception, %RuntimeError{}}}} =
             Inbound.receive(envelope, context,
               target_identity: envelope.to,
               state_scope: fn _target, _envelope, _context -> raise "scope crash" end
             )
  end

  test "explicit target configuration requires identity and accepts string keyed contexts", %{
    envelope: envelope
  } do
    assert {:error, %Error{reason: :target_identity_required}} =
             Inbound.receive(envelope, %{
               "authenticated_identity" => envelope.from,
               "target" => PlainAgent
             })

    assert {:error, %Error{reason: {:invalid_state_scope, :invalid}}} =
             Inbound.receive(
               envelope,
               %{
                 authenticated_identity: envelope.from,
                 target: PlainAgent
               },
               target_identity: envelope.to,
               state_scope: :invalid
             )
  end

  test "config validates shapes and builds its public identity" do
    book = ContactBook.new!([Contact.new!(:contact, "spectre://config/contact")])

    assert {:ok, config} =
             Config.new(
               identity: "SPECTRE://config/agent",
               contacts: book,
               state_scope: fn _target, _envelope, _context -> :scope end,
               advertise: %{
                 display_name: "Config Agent",
                 capabilities: [:testing],
                 metadata: %{region: "test"}
               }
             )

    public = Config.public_identity(config)
    assert public.address == "spectre://config/agent"
    assert public.display_name == "Config Agent"
    assert public.capabilities == [:testing]

    assert {:ok, _config} =
             Config.new(
               identity: "spectre://config/mfa",
               state_scope: {Callbacks, :state_scope, []}
             )

    invalid = [
      {:invalid, {:invalid_pulse_config, :invalid}},
      {[identity: "spectre://config/agent", contacts: :invalid],
       {:invalid_contact_book, :invalid}},
      {[identity: "spectre://config/agent", state_scope: :invalid],
       {:invalid_state_scope, :invalid}},
      {[identity: "spectre://config/agent", advertise: []], {:invalid_advertise_config, []}}
    ]

    for {attrs, reason} <- invalid do
      assert {:error, %Error{reason: ^reason}} = Config.new(attrs)
    end

    assert_raise ArgumentError, fn -> Config.new!(identity: "invalid") end
    assert {:error, %Error{reason: {:agent_not_pulse_enabled, String}}} = Config.fetch(String)
  end

  test "expectations cancel, expire and match reply or typed correlation", %{envelope: envelope} do
    opened_at = ~U[2026-01-01 00:00:00Z]
    due_at = ~U[2026-01-01 00:00:10Z]

    reply = %{envelope | relates_to: envelope.id, id: Spectre.Identity.uuid7()}

    expectation =
      Expectation.new(envelope.id, :sender, :reply, opened_at: opened_at, due_at: due_at)

    assert Expectation.matches?(expectation, reply)
    assert {:ok, resolved} = Expectation.resolve(expectation, reply)
    refute Expectation.matches?(resolved, reply)
    assert Expectation.cancel(resolved) == resolved
    assert Expectation.expire(resolved, due_at) == resolved
    assert Expectation.cancel(expectation).status == :cancelled
    assert Expectation.expire(expectation, DateTime.add(due_at, -1, :second)) == expectation
    assert Expectation.expire(expectation, due_at).status == :expired

    typed = Expectation.new(envelope.id, :sender, "other.perform")
    refute Expectation.matches?(typed, reply)
    assert {:error, :expectation_not_satisfied} = Expectation.resolve(typed, reply)
  end

  test "state tolerates malformed persisted contacts and manages reminders", %{envelope: envelope} do
    malformed = %Spectre.State{data: %{pulse: %{contacts: :invalid}}}
    assert ContactBook.contacts(PulseState.contact_book(malformed)) == []

    malformed_entry = %Spectre.State{
      data: %{pulse: %{contacts: [%{key: nil, identity: "spectre://state/invalid"}]}}
    }

    assert ContactBook.contacts(PulseState.contact_book(malformed_entry)) == []

    static = ContactBook.new!([Contact.new!(:same, "spectre://state/static")])
    dynamic = Contact.new!(:same, "spectre://state/dynamic")
    {:ok, state} = PulseState.remember_contact(%Spectre.State{}, dynamic)

    assert {:ok, ^dynamic} = state |> PulseState.contact_book(static) |> ContactBook.fetch(:same)

    conflict = Contact.new!(:other, "spectre://state/static")
    {:ok, conflict_state} = PulseState.remember_contact(%Spectre.State{}, conflict)
    assert PulseState.contact_book(conflict_state, static) == static

    expectation = Expectation.new(envelope.id, :sender)

    state =
      state
      |> PulseState.put_expectation(expectation)
      |> PulseState.forget_expectation(envelope.id)

    assert PulseState.expectations(state) == %{}
    assert :unmatched = PulseState.correlate(state, %{envelope | relates_to: nil})
  end

  test "Pulse facade resolves and merges module, tuple and Context contact sources" do
    static = Contact.new!(:static, "spectre://facade/static", capabilities: [:static])
    external = Contact.new!(:external, "spectre://facade/external", capabilities: [:external])

    assert contacts = Spectre.Pulse.contacts(FacadeAgent)
    assert static in contacts
    assert external in contacts
    assert Spectre.Pulse.contacts(:not_a_pulse_agent) == []
    assert Spectre.Pulse.contacts(%{}) == []

    dynamic = Contact.new!(:dynamic, "spectre://facade/dynamic", capabilities: [:dynamic])
    {:ok, state} = Spectre.Pulse.remember_contact(%Spectre.State{}, dynamic)

    assert dynamic in Spectre.Pulse.contacts({FacadeAgent, state})

    context = %Spectre.Context{agent: FacadeAgent, state: state}
    assert dynamic in Spectre.Pulse.contacts(context)

    assert {:ok, "spectre://facade/static"} = Spectre.Pulse.resolve(FacadeAgent, :static)
    assert {:ok, "spectre://facade/dynamic"} = Spectre.Pulse.resolve(context, :dynamic)

    assert {:error, %Error{reason: {:invalid_agent_context, %{}}}} =
             Spectre.Pulse.resolve(%{}, :x)

    assert [^external] = Spectre.Pulse.find_contacts(FacadeAgent, capability: :external)

    forgotten = Spectre.Pulse.forget_contact(state, :dynamic)
    refute dynamic in Spectre.Pulse.contacts({FacadeAgent, forgotten})

    assert Spectre.Pulse.contacts(ConflictAgent) == [
             Contact.new!(:static, "spectre://facade/static")
           ]
  end

  test "Pulse facade delegates envelope, registration, subscription and reachability" do
    assert {:ok, %Envelope{}} =
             Spectre.Pulse.envelope(
               from: "spectre://facade/sender",
               to: "spectre://facade/receiver",
               payload: %{type: "facade.perform", data: %{}}
             )

    transport_name = :"facade_transport_#{System.unique_integer([:positive])}"

    assert :ok =
             Spectre.Pulse.register_transport(
               transport_name,
               Spectre.Pulse.Transports.WebSocket,
               priority: 12
             )

    assert Fabric.transports()[transport_name].priority == 12

    assert {:ok, pid} = Spectre.Pulse.subscribe(FacadeAgent)
    assert Process.alive?(pid)

    assert {:ok, route} =
             Spectre.Pulse.connect(
               "spectre://facade/static",
               :websocket,
               self(),
               id: "facade-reachability"
             )

    on_exit(fn -> Spectre.Pulse.disconnect(route.id) end)

    assert {:ok, %Reachability{status: :reachable, via: :websocket}} =
             Spectre.Pulse.reachability(FacadeAgent, :static)

    assert :ok = Spectre.Pulse.disconnect(route.id)
    assert :ok = Spectre.Pulse.disconnect(route.id)
  end
end

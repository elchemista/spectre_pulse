defmodule Spectre.Pulse.ConnectionRegistryTest.AgentOne do
  use Spectre.Agent
  use Spectre.Pulse

  pulsing do
    identity("spectre://connections/agent-one")
    advertise(capabilities: ["semantic-cache"])
  end

  flow :studio do
    on :ping, pulse: "studio.ping" do
      run(:ping)
    end
  end

  flow :studio_cache do
    on :cache_alpha, regex: ~r/^cache alpha$/ do
      run(:ping)
    end

    on :cache_beta, regex: ~r/^cache beta$/ do
      run(:ping)
    end
  end

  @doc false
  @spec ping(Spectre.Input.t(), Spectre.Context.t()) :: String.t()
  def ping(_input, _context), do: "pong"
end

defmodule Spectre.Pulse.ConnectionRegistryTest.AgentTwo do
  use Spectre.Agent
  use Spectre.Pulse

  pulsing do
    identity("spectre://connections/agent-two")
    advertise(capabilities: ["ledger"])
  end
end

defmodule Spectre.Pulse.ConnectionRegistryTest do
  use ExUnit.Case, async: false

  alias Spectre.Pulse.AgentDescriptor
  alias Spectre.Pulse.Codec.JSON
  alias Spectre.Pulse.Connection
  alias Spectre.Pulse.ConnectionRegistry
  alias Spectre.Pulse.ConnectionSpec
  alias Spectre.Pulse.Envelope
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Handshake
  alias Spectre.Pulse.Local
  alias Spectre.Pulse.Monitoring
  alias Spectre.Pulse.Phoenix.Frame
  alias Spectre.Pulse.Phoenix.Socket
  alias Spectre.Pulse.Runtime
  alias Spectre.Pulse.RuntimeInfo
  alias Spectre.Pulse.Studio
  alias Spectre.Router.SemanticCache

  alias __MODULE__.AgentOne
  alias __MODULE__.AgentTwo

  setup do
    stop_runtime()

    on_exit(fn ->
      stop_runtime()
      ConnectionRegistry.clear_configuration(self())
      terminate_subscription("spectre://connections/agent-one")
      terminate_subscription("spectre://connections/agent-two")
    end)

    :ok
  end

  test "connection specs expose all Agents by default or an explicit subset" do
    descriptors = descriptors()

    assert :ok =
             ConnectionRegistry.configure(
               self(),
               [
                 [id: :studio, transport: :websocket, mode: :listen],
                 [id: :private, transport: :websocket, agents: [AgentOne]]
               ],
               descriptors
             )

    assert {:ok, all_agents} = ConnectionRegistry.exposed_agents(:studio)

    assert Enum.map(all_agents, & &1.address) == [
             "spectre://connections/agent-one",
             "spectre://connections/agent-two"
           ]

    assert {:ok, [private_agent]} = ConnectionRegistry.exposed_agents(:private)
    assert private_agent.address == "spectre://connections/agent-one"

    assert [%ConnectionSpec{id: :private}, %ConnectionSpec{id: :studio}] =
             ConnectionRegistry.specs()
  end

  test "the Runtime owns and resolves overlapping connection configurations" do
    assert {:ok, runtime} =
             Runtime.start_link(
               connections: [
                 [id: :studio, transport: :websocket, mode: :listen],
                 [
                   id: :ledger_link,
                   transport: :websocket,
                   mode: :connect,
                   agents: ["spectre://connections/agent-two"]
                 ]
               ]
             )

    assert %{connection_specs: specs, local_agents: local_agents} = :sys.get_state(runtime)
    assert Enum.map(specs, & &1.id) == [:ledger_link, :studio]

    assert Enum.any?(local_agents, &(&1.address == "spectre://connections/agent-one"))
    assert Enum.any?(local_agents, &(&1.address == "spectre://connections/agent-two"))

    assert {:ok, [ledger_agent]} = ConnectionRegistry.exposed_agents(:ledger_link)
    assert ledger_agent.address == "spectre://connections/agent-two"

    assert Enum.any?(
             ConnectionRegistry.exposed_agents(:studio) |> elem(1),
             &(&1.address == ledger_agent.address)
           )

    GenServer.stop(runtime)
    assert ConnectionRegistry.specs() == []
  end

  test "live connections retain grants and remote discovery without credentials" do
    configure_secure_connection()
    owner = spawn(fn -> Process.sleep(:infinity) end)

    assert {:ok, connection} =
             Spectre.Pulse.open_connection(:studio,
               id: "studio-session",
               owner: owner,
               transport_pid: owner,
               peer_id: "studio.local",
               principal: %{
                 id: "operator-1",
                 kind: :studio,
                 scopes: ["studio.observe", "studio.control"],
                 verified: %{issuer: "test"}
               },
               granted_scopes: ["studio.observe"],
               remote_agents: [
                 %{
                   address: "spectre://remote/cache",
                   display_name: "Remote Cache",
                   capabilities: ["semantic-cache"]
                 }
               ],
               metadata: %{session: "visible"}
             )

    assert connection.granted_scopes == ["studio.observe"]
    assert [%{address: "spectre://remote/cache"}] = Spectre.Pulse.remote_agents()
    assert {:ok, ^connection} = Spectre.Pulse.connection("studio-session")

    public = Connection.to_public_map(connection)
    refute Map.has_key?(public, :transport_pid)
    refute Map.has_key?(public, :owner)
    refute Map.has_key?(public.principal, :verified)

    Process.exit(owner, :kill)
    assert eventually(fn -> Spectre.Pulse.connections() == [] end)
  end

  test "a transport cannot grant scopes outside both spec and principal" do
    configure_secure_connection()

    assert {:error,
            %Error{
              kind: :validation,
              reason: {:connection_grant_exceeds_spec, :scopes, ["studio.control"]}
            }} =
             Spectre.Pulse.open_connection(:studio,
               owner: self(),
               principal: %{id: "observer", scopes: ["studio.observe"]},
               granted_scopes: ["studio.control"]
             )
  end

  test "the central handshake authenticates and authorizes without retaining credentials" do
    configure_authenticated_connection()
    credential = %{params: %{"token" => "private-token"}}

    assert {:ok, ticket} =
             Handshake.prepare(:studio, credential,
               context: %{binding: :websocket},
               requested_scopes: ["studio.observe"]
             )

    refute inspect(ticket) =~ "private-token"

    assert {:ok, connection} = Handshake.open(ticket, self())
    assert connection.principal.id == "operator-1"
    assert connection.principal.identity == "spectre://studio/operator-1"
    assert connection.granted_scopes == ["studio.observe"]

    assert {:error, %Error{kind: :authentication, reason: :invalid_token}} =
             Handshake.prepare(:studio, %{params: %{"token" => "wrong"}})
  end

  test "the dependency-free Phoenix bridge opens a connection and pushes its manifest" do
    configure_authenticated_connection()
    assert {:ok, _subscription} = Spectre.Pulse.subscribe(AgentOne)

    transport_info = %{
      endpoint: TestEndpoint,
      transport: :websocket,
      params: %{"token" => "private-token"},
      connect_info: %{peer_data: %{address: {127, 0, 0, 1}}}
    }

    assert {:ok, pending} = Socket.connect(transport_info, connection: :studio)
    assert {:ok, state} = Socket.init(pending)
    assert_receive {:spectre_pulse_manifest, connection_id}

    assert {:push, {:text, manifest}, state} =
             Socket.handle_info({:spectre_pulse_manifest, connection_id}, state)

    assert {:ok,
            %{
              "type" => "manifest",
              "connection" => %{"id" => ^connection_id},
              "agents" => agents
            }} = Jason.decode(manifest)

    assert Enum.any?(agents, &(&1["address"] == "spectre://connections/agent-one"))

    envelope =
      Envelope.new!(
        from: "spectre://studio/operator-1",
        to: "spectre://connections/agent-one",
        act: :request,
        payload: %{type: "studio.ping", data: %{}}
      )

    assert {:ok, encoded_envelope} = JSON.encode(envelope, [])

    assert {:reply, :ok, {:text, receipt}, state} =
             Socket.handle_in({encoded_envelope, opcode: :text}, state)

    assert {:ok, %{"type" => "receipt", "receipt" => %{"status" => "accepted"}}} =
             Jason.decode(receipt)

    denied_envelope = %{envelope | to: "spectre://connections/agent-two"}
    assert {:ok, denied_frame} = JSON.encode(denied_envelope, [])

    assert {:reply, :error, {:text, denied_error}, state} =
             Socket.handle_in({denied_frame, opcode: :text}, state)

    assert {:ok, %{"type" => "error"}} = Jason.decode(denied_error)

    assert {:reply, :error, {:text, invalid_error}, ^state} =
             Socket.handle_in({:not_binary, []}, state)

    assert {:ok, %{"error" => %{"code" => "binary_frame_expected"}}} =
             Jason.decode(invalid_error)

    assert {:stop, {:invalid_pulse_phoenix_frame, :invalid}, ^state} =
             Socket.handle_in(:invalid, state)

    assert {:push, {:text, "outbound"}, state} =
             Socket.handle_info({:spectre_pulse_frame, "outbound"}, state)

    assert {:ok, ^state} = Socket.handle_info(:ignored, state)

    assert {:reply, :ok, {:pong, "ping"}, state} =
             Socket.handle_control({"ping", [opcode: :ping]}, state)

    assert {:ok, state} = Socket.handle_control({"pong", [opcode: :pong]}, state)

    encoding_error = Frame.manifest(%{state.connection | metadata: %{pid: self()}})

    assert {:ok, %{"error" => %{"code" => "response_encoding_failed"}}} =
             Jason.decode(encoding_error)

    fallback_error =
      Frame.error(%Error{kind: "private", outcome: "private", reason: %{secret: true}})

    assert {:ok,
            %{
              "error" => %{
                "kind" => "request",
                "outcome" => "not_sent",
                "code" => "request_failed"
              }
            }} = Jason.decode(fallback_error)

    assert :ok = Socket.terminate(:closed, state)
    assert :ok = Socket.terminate(:closed, :invalid_state)
    assert :error = Spectre.Pulse.connection(connection_id)

    assert {:error, :pulse_connection_spec_required} = Socket.connect(transport_info, [])
    assert {:error, :invalid_pulse_phoenix_connect} = Socket.connect(:invalid, [])
    assert {:stop, {:invalid_pulse_phoenix_state, :invalid}} = Socket.init(:invalid)
  end

  test "the Phoenix bridge sends heartbeats before the transport idle timeout" do
    configure_authenticated_connection()

    transport_info = %{
      endpoint: TestEndpoint,
      transport: :websocket,
      params: %{"token" => "private-token"},
      connect_info: %{peer_data: %{address: {127, 0, 0, 1}}}
    }

    assert {:ok, pending} =
             Socket.connect(transport_info,
               connection: :studio,
               heartbeat_interval_ms: 10
             )

    assert {:ok, state} = Socket.init(pending)
    assert_receive {:spectre_pulse_manifest, connection_id}
    assert_receive {:spectre_pulse_heartbeat, ^connection_id}, 100

    assert {:push, {:ping, payload}, heartbeat_state} =
             Socket.handle_info({:spectre_pulse_heartbeat, connection_id}, state)

    assert is_binary(payload)
    assert byte_size(payload) <= 125
    assert heartbeat_state.heartbeat_ref != state.heartbeat_ref

    assert {:ok, pong_state} =
             Socket.handle_control({payload, [opcode: :pong]}, heartbeat_state)

    assert pong_state.connection.last_seen_at >= heartbeat_state.connection.last_seen_at
    assert :ok = Socket.terminate(:closed, pong_state)
  end

  test "the Phoenix bridge serves scoped Studio inspection envelopes" do
    configure_studio_connection()

    transport_info = %{
      endpoint: TestEndpoint,
      transport: :websocket,
      params: %{"token" => "private-token"},
      connect_info: %{peer_data: %{address: {127, 0, 0, 1}}}
    }

    assert {:ok, pending} = Socket.connect(transport_info, connection: :studio_bridge)
    assert {:ok, state} = Socket.init(pending)
    assert_receive {:spectre_pulse_manifest, _connection_id}

    request =
      Envelope.new!(
        from: "spectre://studio/operator-1",
        to: "spectre://connections/agent-one",
        act: :query,
        payload: %{type: "studio.skills.list", data: %{}}
      )

    assert {:ok, request_frame} = JSON.encode(request, [])

    assert {:reply, :ok, {:text, receipt_frame}, next_state} =
             Socket.handle_in({request_frame, opcode: :text}, state)

    assert %{"type" => "receipt", "receipt" => %{"message_id" => message_id}} =
             Jason.decode!(receipt_frame)

    assert message_id == request.id
    assert_receive {:spectre_pulse_frame, response_frame}

    assert {:ok, response} = JSON.decode(response_frame, [])
    assert response.relates_to == request.id
    assert response.payload.type == "studio.skills.list.result"
    assert response.payload.data == %{"count" => 0, "skills" => []}

    denied =
      Envelope.new!(
        from: "spectre://studio/operator-1",
        to: "spectre://connections/agent-one",
        act: :query,
        payload: %{type: "studio.semantic_cache.examples", data: %{}}
      )

    assert {:ok, denied_frame} = JSON.encode(denied, [])

    assert {:reply, :ok, {:text, _receipt}, final_state} =
             Socket.handle_in({denied_frame, opcode: :text}, next_state)

    assert_receive {:spectre_pulse_frame, error_frame}
    assert {:ok, error_response} = JSON.decode(error_frame, [])
    assert error_response.payload.type == "studio.semantic_cache.examples.error"
    assert error_response.payload.data["kind"] == "authorization"
    assert error_response.payload.data["code"] == "connection_scope_required"

    reserved =
      Envelope.new!(
        from: "spectre://studio/operator-1",
        to: "spectre://connections/agent-one",
        act: :request,
        payload: %{type: "studio.skill.mount", data: %{}}
      )

    assert {:ok, reserved_frame} = JSON.encode(reserved, [])

    assert {:reply, :ok, {:text, _receipt}, terminal_state} =
             Socket.handle_in({reserved_frame, opcode: :text}, final_state)

    assert_receive {:spectre_pulse_frame, reserved_error_frame}
    assert {:ok, reserved_error} = JSON.decode(reserved_error_frame, [])
    assert reserved_error.payload.type == "studio.skill.mount.error"
    assert reserved_error.payload.data["code"] == "skill_mount_requires_governed_morph"

    assert :ok = Socket.terminate(:closed, terminal_state)
  end

  test "the Phoenix bridge edits mutable cache rows under its dedicated scope" do
    configure_studio_connection([
      "agent.semantic_cache.read",
      "agent.semantic_cache.write"
    ])

    :ok = SemanticCache.clear(AgentOne)
    on_exit(fn -> SemanticCache.clear(AgentOne) end)

    assert {:ok, row} =
             SemanticCache.put(
               "draft cache category",
               %{label: :cache_alpha, verified?: false},
               spectre_agent: AgentOne
             )

    transport_info = %{
      endpoint: TestEndpoint,
      transport: :websocket,
      params: %{"token" => "private-token"},
      connect_info: %{peer_data: %{address: {127, 0, 0, 1}}}
    }

    assert {:ok, pending} = Socket.connect(transport_info, connection: :studio_bridge)
    assert {:ok, state} = Socket.init(pending)
    assert_receive {:spectre_pulse_manifest, _connection_id}

    {state, examples_response} =
      studio_request(state, :query, "studio.semantic_cache.examples", %{})

    assert examples_response.payload.type == "studio.semantic_cache.examples.result"
    assert "cache_alpha" in examples_response.payload.data["labels"]
    assert "cache_beta" in examples_response.payload.data["labels"]

    {state, update_response} =
      studio_request(state, :request, "studio.semantic_cache.update", %{
        "example_id" => row.id,
        "label" => "cache_beta",
        "text" => "edited cache phrase"
      })

    assert update_response.payload.type == "studio.semantic_cache.update.result"
    assert update_response.payload.data["example"]["label"] == "cache_beta"
    assert update_response.payload.data["example"]["text"] == "edited cache phrase"

    assert {:ok, updated} = SemanticCache.get_example(AgentOne, row.id)
    assert updated.label == :cache_beta
    assert updated.text == "edited cache phrase"
    refute updated.verified?

    {_state, rejected_response} =
      studio_request(state, :request, "studio.semantic_cache.update", %{
        "example_id" => row.id,
        "label" => "not-a-real-agent-label"
      })

    assert rejected_response.payload.type == "studio.semantic_cache.update.error"
    assert rejected_response.payload.data["code"] == "unknown_label"

    assert :ok = Socket.terminate(:closed, state)
  end

  test "monitor enable failures retain their safe request correlation" do
    configure_monitoring_connection()

    transport_info = %{
      endpoint: TestEndpoint,
      transport: :websocket,
      params: %{"token" => "private-token"},
      connect_info: %{peer_data: %{address: {127, 0, 0, 1}}}
    }

    assert {:ok, pending} = Socket.connect(transport_info, connection: :studio_monitoring)
    assert {:ok, state} = Socket.init(pending)
    assert_receive {:spectre_pulse_manifest, _connection_id}

    enable =
      Jason.encode!(%{
        "pulse" => "connection",
        "version" => 1,
        "type" => "agent.runtime.monitor.enable",
        "request_id" => "missing-instance-panel",
        "agent_address" => "spectre://connections/agent-one",
        "subject" => "missing-#{System.unique_integer([:positive])}",
        "interval_ms" => 1_000
      })

    assert {:reply, :error, {:text, error_frame}, state} =
             Socket.handle_in({enable, opcode: :text}, state)

    assert {:ok,
            %{
              "type" => "error",
              "error" => %{
                "kind" => "routing",
                "code" => "instance_not_found",
                "monitor" => "runtime",
                "request_id" => "missing-instance-panel"
              }
            }} = Jason.decode(error_frame)

    assert :ok = Socket.terminate(:closed, state)
  end

  test "Studio discovers active Instance subjects through an authorized control frame" do
    configure_monitoring_connection()
    subject = "discovered-#{System.unique_integer([:positive])}"
    assert {:ok, instance} = Spectre.summon(agent: AgentOne, subject: subject)

    on_exit(fn ->
      if Process.alive?(instance), do: Process.exit(instance, :kill)
    end)

    transport_info = %{
      endpoint: TestEndpoint,
      transport: :websocket,
      params: %{"token" => "private-token"},
      connect_info: %{peer_data: %{address: {127, 0, 0, 1}}}
    }

    assert {:ok, pending} = Socket.connect(transport_info, connection: :studio_monitoring)
    assert {:ok, state} = Socket.init(pending)
    assert_receive {:spectre_pulse_manifest, _connection_id}

    request =
      Jason.encode!(%{
        "pulse" => "connection",
        "version" => 1,
        "type" => "agent.instances.list",
        "request_id" => "instances-panel",
        "agent_address" => "spectre://connections/agent-one"
      })

    assert {:reply, :ok, {:text, response}, state} =
             Socket.handle_in({request, opcode: :text}, state)

    assert {:ok,
            %{
              "type" => "agent.instances.list.result",
              "request_id" => "instances-panel",
              "agent_address" => "spectre://connections/agent-one",
              "instances" => instances
            }} = Jason.decode(response)

    assert Enum.any?(instances, &(&1["subject"] == subject and &1["alive"] == true))
    assert :ok = Socket.terminate(:closed, state)
  end

  test "the Phoenix boundary keeps credentials out of authorization and public errors" do
    test_pid = self()

    authenticator = fn credential, _context ->
      if get_in(credential, [:params, "token"]) == "private-token",
        do: {:ok, %{id: "operator", scopes: ["studio.observe"]}},
        else: {:error, :invalid_token}
    end

    authorizer = fn _principal, request ->
      send(test_pid, {:authorization_request, request})
      {:ok, granted_scopes: ["studio.observe"]}
    end

    assert :ok =
             ConnectionRegistry.configure(
               self(),
               [
                 [
                   id: :studio,
                   transport: :websocket,
                   authenticate: authenticator,
                   authorize: authorizer,
                   scopes: ["studio.observe"]
                 ]
               ],
               descriptors()
             )

    transport_info = %{
      endpoint: TestEndpoint,
      transport: :websocket,
      params: %{"token" => "private-token"},
      connect_info: %{peer_data: %{address: {127, 0, 0, 1}}}
    }

    assert {:ok, _pending} = Socket.connect(transport_info, connection: :studio)
    assert_receive {:authorization_request, request}
    refute inspect(request) =~ "private-token"

    error =
      Error.not_sent(:validation, {:invalid_payload, %{token: "never-expose-this"}})

    encoded = Frame.error(error)
    refute encoded =~ "never-expose-this"

    assert {:ok,
            %{
              "type" => "error",
              "error" => %{"kind" => "validation", "code" => "invalid_payload"}
            }} = Jason.decode(encoded)

    assert Frame.metadata([:not_a_keyword]) == %{}

    assert {:error, :invalid_pulse_phoenix_options} =
             Socket.connect(transport_info,
               connection: :studio,
               inbound: :invalid
             )

    assert {:error, :invalid_pulse_phoenix_options} =
             Socket.connect(transport_info,
               connection: :studio,
               heartbeat_interval_ms: 0
             )

    assert {:error, :invalid_pulse_phoenix_options} =
             Socket.connect(transport_info,
               connection: :studio,
               heartbeat_interval_ms: 55_001
             )

    oversized_request_id = String.duplicate("x", 129)
    encoded = Frame.error(error, monitor: :runtime, request_id: oversized_request_id)

    assert {:ok, %{"error" => safe_error}} = Jason.decode(encoded)
    assert safe_error["monitor"] == "runtime"
    refute Map.has_key?(safe_error, "request_id")
  end

  test "Studio enables and disables live monitoring through Phoenix at runtime" do
    configure_monitoring_connection()
    subject = "phoenix-live-#{System.unique_integer([:positive])}"
    assert {:ok, instance} = Spectre.summon(agent: AgentOne, subject: subject)

    on_exit(fn ->
      if Process.alive?(instance), do: Process.exit(instance, :kill)
    end)

    transport_info = %{
      endpoint: TestEndpoint,
      transport: :websocket,
      params: %{"token" => "private-token"},
      connect_info: %{peer_data: %{address: {127, 0, 0, 1}}}
    }

    assert {:ok, pending} = Socket.connect(transport_info, connection: :studio_monitoring)
    assert {:ok, state} = Socket.init(pending)
    assert_receive {:spectre_pulse_manifest, _connection_id}

    enable =
      Jason.encode!(%{
        "pulse" => "connection",
        "version" => 1,
        "type" => "agent.runtime.monitor.enable",
        "request_id" => "liveview-runtime",
        "agent_address" => "spectre://connections/agent-one",
        "subject" => subject,
        "interval_ms" => 250,
        "duration_ms" => 1_000,
        "fields" => ["memory", "message_queue_len"]
      })

    assert {:reply, :ok, {:text, enabled_frame}, state} =
             Socket.handle_in({enable, opcode: :text}, state)

    assert {:ok,
            %{
              "type" => "agent.runtime.monitor.enabled",
              "request_id" => "liveview-runtime",
              "subscription_id" => subscription_id,
              "sequence" => 0
            }} = Jason.decode(enabled_frame)

    assert_receive {:spectre_pulse_monitoring, update}, 500

    assert {:push, {:text, update_frame}, state} =
             Socket.handle_info({:spectre_pulse_monitoring, update}, state)

    assert {:ok,
            %{
              "type" => "agent.runtime.monitor.update",
              "subscription_id" => ^subscription_id,
              "sequence" => 1,
              "snapshot" => %{"process" => process}
            }} = Jason.decode(update_frame)

    assert Map.keys(process) |> Enum.sort() == ["memory", "message_queue_len"]

    disable =
      Jason.encode!(%{
        "pulse" => "connection",
        "version" => 1,
        "type" => "agent.runtime.monitor.disable",
        "subscription_id" => subscription_id
      })

    assert {:reply, :ok, {:text, disabled_frame}, state} =
             Socket.handle_in({disable, opcode: :text}, state)

    assert {:ok,
            %{
              "type" => "agent.runtime.monitor.disabled",
              "subscription_id" => ^subscription_id
            }} = Jason.decode(disabled_frame)

    refute_receive {:spectre_pulse_monitoring, %{"subscription_id" => ^subscription_id}}, 350

    invalid_version =
      enable
      |> Jason.decode!()
      |> Map.put("version", 2)
      |> Jason.encode!()

    assert {:reply, :error, {:text, invalid_frame}, state} =
             Socket.handle_in({invalid_version, opcode: :text}, state)

    assert {:ok, %{"error" => %{"code" => "unsupported_connection_protocol_version"}}} =
             Jason.decode(invalid_frame)

    assert :ok = Socket.terminate(:closed, state)
  end

  test "invalid selectors and duplicate spec identifiers fail atomically" do
    assert {:error, %Error{reason: {:unknown_connection_agent, AgentTwo}}} =
             ConnectionRegistry.configure(
               self(),
               [[id: :private, transport: :websocket, agents: [AgentTwo]]],
               [descriptor(AgentOne)]
             )

    assert ConnectionRegistry.specs() == []

    assert {:error, %Error{reason: {:duplicate_connection_spec, :duplicate}}} =
             ConnectionRegistry.configure(
               self(),
               [
                 [id: :duplicate, transport: :websocket],
                 [id: :duplicate, transport: :rest]
               ],
               descriptors()
             )

    assert ConnectionRegistry.specs() == []
  end

  defp configure_secure_connection do
    assert :ok =
             ConnectionRegistry.configure(
               self(),
               [
                 [
                   id: :studio,
                   transport: :websocket,
                   mode: :listen,
                   profiles: ["pulse.messaging/1", "spectre.studio/1"],
                   scopes: ["studio.observe", "studio.control"]
                 ]
               ],
               descriptors()
             )
  end

  defp configure_authenticated_connection do
    authenticator = fn credential, _context ->
      if get_in(credential, [:params, "token"]) == "private-token" do
        {:ok,
         %{
           id: "operator-1",
           identity: "spectre://studio/operator-1",
           kind: :studio,
           scopes: ["studio.observe", "studio.control"]
         }}
      else
        {:error, :invalid_token}
      end
    end

    authorizer = fn _principal, request ->
      requested = request.requested_scopes
      grants = if "studio.control" in requested, do: [], else: ["studio.observe"]
      {:ok, granted_scopes: grants}
    end

    assert :ok =
             ConnectionRegistry.configure(
               self(),
               [
                 [
                   id: :studio,
                   transport: :websocket,
                   mode: :listen,
                   agents: [AgentOne],
                   authenticate: authenticator,
                   authorize: authorizer,
                   scopes: ["studio.observe"]
                 ]
               ],
               descriptors()
             )
  end

  defp configure_monitoring_connection do
    scopes = [
      "studio.observe",
      RuntimeInfo.scope(),
      Monitoring.scope()
    ]

    authenticator = fn credential, _context ->
      if get_in(credential, [:params, "token"]) == "private-token",
        do: {:ok, %{id: "operator-1", kind: :studio, scopes: scopes}},
        else: {:error, :invalid_token}
    end

    authorizer = fn _principal, _request -> {:ok, granted_scopes: scopes} end

    assert :ok =
             ConnectionRegistry.configure(
               self(),
               [
                 [
                   id: :studio_monitoring,
                   transport: :websocket,
                   mode: :listen,
                   agents: [AgentOne],
                   authenticate: authenticator,
                   authorize: authorizer,
                   scopes: scopes
                 ]
               ],
               descriptors()
             )
  end

  defp configure_studio_connection(scopes \\ ["spectre.skill.read"]) do
    authenticator = fn credential, _context ->
      if get_in(credential, [:params, "token"]) == "private-token" do
        {:ok,
         %{
           id: "operator-1",
           identity: "spectre://studio/operator-1",
           kind: :studio,
           scopes: scopes
         }}
      else
        {:error, :invalid_token}
      end
    end

    authorizer = fn _principal, _request -> {:ok, granted_scopes: scopes} end

    assert "spectre.skill.read" in Studio.scopes()

    assert :ok =
             ConnectionRegistry.configure(
               self(),
               [
                 [
                   id: :studio_bridge,
                   transport: :websocket,
                   mode: :listen,
                   agents: [AgentOne],
                   authenticate: authenticator,
                   authorize: authorizer,
                   scopes: scopes
                 ]
               ],
               descriptors()
             )
  end

  defp studio_request(state, act, type, data) do
    request =
      Envelope.new!(
        from: "spectre://studio/operator-1",
        to: "spectre://connections/agent-one",
        act: act,
        payload: %{type: type, data: data}
      )

    assert {:ok, frame} = JSON.encode(request, [])

    assert {:reply, :ok, {:text, receipt_frame}, next_state} =
             Socket.handle_in({frame, opcode: :text}, state)

    assert %{"type" => "receipt", "receipt" => %{"message_id" => message_id}} =
             Jason.decode!(receipt_frame)

    assert message_id == request.id
    assert_receive {:spectre_pulse_frame, response_frame}
    assert {:ok, response} = JSON.decode(response_frame, [])
    assert response.relates_to == request.id
    {next_state, response}
  end

  defp descriptors, do: [descriptor(AgentOne), descriptor(AgentTwo)]

  defp descriptor(agent) do
    assert {:ok, %AgentDescriptor{} = descriptor} = AgentDescriptor.for_agent(agent)
    descriptor
  end

  defp eventually(callback, attempts \\ 50)
  defp eventually(_callback, 0), do: false

  defp eventually(callback, attempts) do
    if callback.() do
      true
    else
      Process.sleep(10)
      eventually(callback, attempts - 1)
    end
  end

  defp terminate_subscription(identity) do
    case Local.lookup(identity) do
      {:ok, pid, _metadata} ->
        DynamicSupervisor.terminate_child(Spectre.Pulse.Local.Supervisor, pid)

      :error ->
        :ok
    end
  end

  defp stop_runtime do
    case Process.whereis(Runtime) do
      pid when is_pid(pid) -> GenServer.stop(pid)
      nil -> :ok
    end
  catch
    :exit, _reason -> :ok
  end
end

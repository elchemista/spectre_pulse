defmodule Spectre.Pulse.ConnectionRegistryTest.AgentOne do
  use Spectre.Agent
  use Spectre.Pulse

  pulsing do
    identity("spectre://connections/agent-one")
    advertise(capabilities: ["semantic-cache"])
  end
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
  alias Spectre.Pulse.Connection
  alias Spectre.Pulse.ConnectionRegistry
  alias Spectre.Pulse.ConnectionSpec
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Handshake
  alias Spectre.Pulse.Local
  alias Spectre.Pulse.Phoenix.Socket
  alias Spectre.Pulse.Runtime

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
    assert :ok = Socket.terminate(:closed, state)
    assert :error = Spectre.Pulse.connection(connection_id)
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
                   authenticate: authenticator,
                   authorize: authorizer,
                   scopes: ["studio.observe"]
                 ]
               ],
               descriptors()
             )
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

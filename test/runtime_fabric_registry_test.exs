defmodule Spectre.Pulse.RuntimeFabricRegistryTest.TransportA do
  @behaviour Spectre.Pulse.Transport

  alias Spectre.Pulse.Receipt

  @impl true
  def deliver(route, envelope, _opts) do
    {:ok, Receipt.accepted(envelope.id, via: :a, route_id: route.id)}
  end
end

defmodule Spectre.Pulse.RuntimeFabricRegistryTest.TransportB do
  @behaviour Spectre.Pulse.Transport

  alias Spectre.Pulse.Receipt

  @impl true
  def deliver(route, envelope, _opts) do
    {:ok, Receipt.accepted(envelope.id, via: :b, route_id: route.id)}
  end
end

defmodule Spectre.Pulse.RuntimeFabricRegistryTest.AgentA do
  use Spectre.Agent
  use Spectre.Pulse

  pulsing do
    identity("spectre://registry/agent-a")
    pulse_inbound(allowed_types: ["registry.perform"])
  end
end

defmodule Spectre.Pulse.RuntimeFabricRegistryTest.AgentB do
  use Spectre.Agent
  use Spectre.Pulse

  pulsing do
    identity("spectre://registry/agent-b")
  end
end

defmodule Spectre.Pulse.RuntimeFabricRegistryTest do
  use ExUnit.Case, async: false

  alias Spectre.Pulse.Config
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Fabric
  alias Spectre.Pulse.Local
  alias Spectre.Pulse.Runtime

  alias __MODULE__.AgentA
  alias __MODULE__.AgentB
  alias __MODULE__.TransportA
  alias __MODULE__.TransportB

  test "Fabric validates every transport registration contract" do
    name = unique_atom("transport")

    assert :ok =
             Fabric.register_transport(name, TransportA,
               priority: 11,
               metadata: %{protocol: :test}
             )

    assert %{module: TransportA, priority: 11, metadata: %{protocol: :test}} =
             Fabric.transports()[name]

    assert :ok = Fabric.register_transport(name, TransportA)
    assert Fabric.transports()[name].priority == 11

    assert {:error,
            %Error{
              reason: {:transport_name_already_registered, ^name, TransportA}
            }} = Fabric.register_transport(name, TransportB)

    assert :ok = Fabric.register_transport(name, TransportB, replace: true)
    assert Fabric.transports()[name].module == TransportB

    invalid = [
      {nil, TransportA, [], {:invalid_transport_name, nil}},
      {"transport", TransportA, [], {:invalid_transport_name, "transport"}},
      {unique_atom("missing"), String, [], {:invalid_transport, String}},
      {unique_atom("non_atom"), "module", [], {:invalid_transport, "module"}},
      {unique_atom("non_list"), TransportA, :invalid, {:invalid_transport_options, :invalid}},
      {unique_atom("non_keyword"), TransportA, [:invalid],
       {:invalid_transport_options, [:invalid]}},
      {unique_atom("unknown"), TransportA, [unknown: true],
       {:unknown_transport_options, [:unknown]}},
      {unique_atom("metadata"), TransportA, [metadata: []], {:invalid_transport_metadata, []}},
      {unique_atom("priority"), TransportA, [priority: :first],
       {:invalid_transport_priority, :first}},
      {unique_atom("replace"), TransportA, [replace: :yes], {:invalid_replace_option, :yes}}
    ]

    for {transport_name, module, opts, reason} <- invalid do
      assert {:error, %Error{kind: :validation, reason: ^reason}} =
               Fabric.register_transport(transport_name, module, opts)
    end
  end

  test "Fabric validates connection options, ownership and route replacement" do
    address = "spectre://registry/routes"

    invalid = [
      {"invalid", :websocket, self(), [], {:invalid_address, :address_must_be_logical}},
      {address, :missing_transport, self(), [], {:unknown_transport, :missing_transport}},
      {address, :websocket, self(), :invalid, {:invalid_transport_options, :invalid}},
      {address, :websocket, self(), [:invalid], {:invalid_transport_options, [:invalid]}},
      {address, :websocket, self(), [unknown: true], {:unknown_transport_options, [:unknown]}},
      {address, :websocket, self(), [metadata: []], {:invalid_transport_metadata, []}},
      {address, :websocket, self(), [priority: :first], {:invalid_transport_priority, :first}},
      {address, :websocket, nil, [], :route_target_required},
      {address, :websocket, self(), [owner: :invalid], {:invalid_connection_owner, :invalid}}
    ]

    for {route_address, transport, target, opts, reason} <- invalid do
      assert {:error, %Error{reason: ^reason}} =
               Fabric.connect(route_address, transport, target, opts)
    end

    dead = spawn(fn -> :ok end)
    monitor = Process.monitor(dead)
    assert_receive {:DOWN, ^monitor, :process, ^dead, :normal}

    assert {:error, %Error{reason: :connection_owner_not_alive}} =
             Fabric.connect(address, :websocket, self(), owner: dead)

    route_id = unique_atom("replace_route")

    assert {:ok, first} =
             Fabric.connect(address, :websocket, fn _frame -> :ok end,
               id: route_id,
               owner: nil,
               priority: 30,
               metadata: %{version: 1}
             )

    assert first.metadata.transport_name == :websocket
    assert first.metadata.discovered_by == :fabric

    replacement_address = "spectre://registry/replacement"

    assert {:ok, replacement} =
             Fabric.connect(replacement_address, :rest, "http://example.test",
               id: route_id,
               priority: 5
             )

    assert Fabric.routes(address) == []
    assert [^replacement] = Fabric.routes(replacement_address)
    assert Fabric.routes("invalid") == []

    assert :ok = Fabric.disconnect(route_id)
    assert :ok = Fabric.disconnect(route_id)
  end

  test "Fabric infers owners from map and tuple connections and removes both on DOWN" do
    address = "spectre://registry/owners"
    map_owner = spawn(fn -> Process.sleep(:infinity) end)
    tuple_owner = spawn(fn -> Process.sleep(:infinity) end)

    assert {:ok, map_route} =
             Fabric.connect(
               address,
               :websocket,
               %{module: __MODULE__, connection: map_owner},
               id: unique_atom("map_owner")
             )

    assert {:ok, tuple_route} =
             Fabric.connect(
               address,
               :websocket,
               {__MODULE__, tuple_owner},
               id: unique_atom("tuple_owner")
             )

    assert Enum.sort_by(Fabric.routes(address), & &1.id) ==
             Enum.sort_by([map_route, tuple_route], & &1.id)

    map_monitor = Process.monitor(map_owner)
    tuple_monitor = Process.monitor(tuple_owner)
    Process.exit(map_owner, :kill)
    Process.exit(tuple_owner, :kill)
    assert_receive {:DOWN, ^map_monitor, :process, ^map_owner, :killed}
    assert_receive {:DOWN, ^tuple_monitor, :process, ^tuple_owner, :killed}
    assert eventually(fn -> Fabric.routes(address) == [] end)

    send(Fabric, {:DOWN, make_ref(), :process, self(), :normal})
    assert Fabric.routes(address) == []
  end

  test "Local registry is idempotent and rejects identity aliasing" do
    assert {:ok, pid_a} = Local.subscribe(AgentA)
    assert {:ok, ^pid_a} = Local.subscribe(AgentA)
    assert {:ok, pid_b} = Local.subscribe(AgentB)

    on_exit(fn ->
      terminate_subscription("spectre://registry/agent-a")
      terminate_subscription("spectre://registry/agent-b")
    end)

    assert {:ok, ^pid_a, metadata} = Local.lookup("SPECTRE://registry/agent-a")
    assert metadata.agent == AgentA
    assert {:ok, AgentA} = Local.resolve_target("spectre://registry/agent-a")
    assert :error = Local.resolve_target("spectre://registry/missing")
    assert :error = Local.lookup("invalid")
    assert [route] = Local.routes("spectre://registry/agent-a")
    assert route.id == "local:spectre://registry/agent-a"
    assert route.priority == 0
    assert Local.routes("invalid") == []

    assert {:error,
            {:pulse_identity_already_subscribed, "spectre://registry/agent-a", AgentA, AgentB}} =
             Local.subscribe(AgentB, identity: "spectre://registry/agent-a")

    assert {:error, %Error{reason: {:agent_not_pulse_enabled, String}}} =
             Local.subscribe(String)

    assert {:error, %Error{reason: {:invalid_address, :address_must_be_logical}}} =
             Local.subscribe(AgentA, identity: "invalid")

    assert {:ok, identity, AgentA, endpoint_opts} =
             Local.subscription(AgentA,
               inbound: [allow_unauthenticated: true],
               on_result: :hook
             )

    assert identity == "spectre://registry/agent-a"
    assert endpoint_opts[:allowed_types] == ["registry.perform"]
    assert endpoint_opts[:allow_unauthenticated]
    assert endpoint_opts[:on_result] == :hook
    assert Process.alive?(pid_b)
  end

  test "Runtime rejects malformed options and transport registrations" do
    invalid = [
      {[unknown: true], {:unknown_runtime_options, [:unknown]}},
      {[:invalid], {:invalid_runtime_options, [:invalid]}},
      {[transports: :invalid], {:invalid_transport_registrations, :invalid}},
      {[transports: [:invalid]], {:invalid_transport_registration, :invalid}},
      {[transports: [{:driver, TransportA, [:invalid]}]],
       {:invalid_transport_registration_options, :driver, [:invalid]}},
      {[transports: [{:driver, String}]], {:invalid_transport, String}}
    ]

    for {opts, reason} <- invalid do
      assert {:stop, %Error{reason: ^reason}} = Runtime.init(opts)
    end

    assert {:stop, %Error{reason: {:invalid_runtime_options, :invalid}}} =
             Runtime.init(:invalid)
  end

  test "Runtime detects duplicate logical identities before starting subscriptions" do
    Process.flag(:trap_exit, true)
    identity = "spectre://registry/duplicate"
    pulse_config = Config.new!(identity: identity)
    suffix = System.unique_integer([:positive])
    first = Module.concat(__MODULE__, :"Duplicate#{suffix}A")
    second = Module.concat(__MODULE__, :"Duplicate#{suffix}B")

    for module <- [first, second] do
      Module.create(
        module,
        quote do
          def __spectre_pulse__ do
            unquote(Macro.escape(pulse_config))
          end
        end,
        Macro.Env.location(__ENV__)
      )
    end

    on_exit(fn ->
      for module <- [first, second] do
        :code.purge(module)
        :code.delete(module)
      end
    end)

    assert {:error,
            %Error{
              reason: {:duplicate_pulse_identity, ^identity, agents}
            }} = Runtime.start_link([])

    assert Enum.sort(agents) == Enum.sort([first, second])
  end

  defp unique_atom(prefix) do
    String.to_atom("#{prefix}_#{System.unique_integer([:positive])}")
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
end

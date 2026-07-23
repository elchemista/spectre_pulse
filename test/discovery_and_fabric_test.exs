defmodule Spectre.Pulse.DiscoveryAndFabricTest.CustomTransport do
  @behaviour Spectre.Pulse.Transport

  alias Spectre.Pulse.Receipt

  @impl true
  def deliver(route, envelope, _opts) do
    send(route.target, {:custom_transport_delivery, route.id, envelope})
    {:ok, Receipt.accepted(envelope.id, via: :custom, route_id: route.id)}
  end
end

defmodule Spectre.Pulse.DiscoveryAndFabricTest.Directory do
  alias Spectre.Pulse.Route

  def resolve(:navigator, _opts), do: {:ok, "spectre://directory/navigator"}
  def resolve(_reference, _opts), do: :error

  def routes("spectre://directory/navigator", opts) do
    [
      Route.web_socket("spectre://directory/navigator", Keyword.fetch!(opts, :connection),
        id: "directory:websocket",
        priority: 25
      )
    ]
  end

  def routes(_address, _opts), do: []
end

defmodule Spectre.Pulse.DiscoveryAndFabricTest.AutoIdentityAgent do
  use Spectre.Agent
  use Spectre.Pulse

  pulsing do
    advertise(capabilities: [:automatic_identity])
  end
end

defmodule Spectre.Pulse.DiscoveryAndFabricTest do
  use ExUnit.Case, async: false

  alias Spectre.Pulse.Address
  alias Spectre.Pulse.Codec.JSON
  alias Spectre.Pulse.ContactBook
  alias Spectre.Pulse.Discovery
  alias Spectre.Pulse.Envelope
  alias Spectre.Pulse.Fabric
  alias Spectre.Pulse.Local
  alias Spectre.Pulse.Receipt

  alias __MODULE__.AutoIdentityAgent
  alias __MODULE__.CustomTransport
  alias __MODULE__.Directory

  test "Directory behaves like DNS while physical routes stay outside the Agent" do
    book = ContactBook.new!()
    directory = {Directory, connection: self()}

    assert {:ok, resolution} =
             Discovery.resolve_identity(book, :navigator, directory: directory)

    assert resolution.address == "spectre://directory/navigator"

    assert {:ok, [route]} =
             Discovery.routes(resolution.address, directory: directory)

    assert route.id == "directory:websocket"
    assert route.target == self()
  end

  test "Fabric discovers connected bindings and Pulse chooses the preferred one" do
    address = "spectre://fabric/preferred"

    assert {:ok, websocket} =
             Spectre.Pulse.connect(address, :websocket, self(), id: "fabric:websocket")

    assert {:ok, rest} =
             Spectre.Pulse.connect(
               address,
               :rest,
               "https://pulse.invalid/spectre-pulse/v1/messages",
               id: "fabric:rest"
             )

    on_exit(fn ->
      Spectre.Pulse.disconnect(websocket.id)
      Spectre.Pulse.disconnect(rest.id)
    end)

    assert {:ok, routes} = Discovery.routes(address)
    assert Enum.map(routes, & &1.id) == [websocket.id, rest.id]

    envelope = envelope(address)

    assert {:ok, %Receipt{via: :websocket, route_id: route_id}} =
             Spectre.Pulse.deliver(envelope, [])

    assert route_id == websocket.id
    assert_receive {:spectre_pulse_frame, frame}
    assert {:ok, ^envelope} = JSON.decode(frame, [])
  end

  test "the routed network falls back from a disconnected WebSocket to discovered REST" do
    address = "spectre://fabric/failover"
    envelope = envelope(address)
    {:ok, listener} = :gen_tcp.listen(0, [:binary, packet: :raw, active: false, reuseaddr: true])
    {:ok, {_address, port}} = :inet.sockname(listener)
    test = self()

    server =
      Task.async(fn ->
        {:ok, socket} = :gen_tcp.accept(listener, 5_000)
        request_body = read_http_body(socket, "")
        send(test, {:automatic_rest_body, request_body})

        body =
          envelope.id
          |> Receipt.accepted(via: :rest)
          |> Receipt.to_wire()
          |> Jason.encode!()

        response = [
          "HTTP/1.1 202 Accepted\r\n",
          "content-type: application/json\r\n",
          "content-length: ",
          Integer.to_string(byte_size(body)),
          "\r\nconnection: close\r\n\r\n",
          body
        ]

        :ok = :gen_tcp.send(socket, response)
        :gen_tcp.close(socket)
        :gen_tcp.close(listener)
      end)

    disconnected = fn _frame -> {:error, {:not_sent, :connection_closed}} end

    assert {:ok, websocket} =
             Spectre.Pulse.connect(address, :websocket, disconnected,
               id: "fabric:closed-websocket"
             )

    assert {:ok, rest} =
             Spectre.Pulse.connect(
               address,
               :rest,
               "http://127.0.0.1:#{port}/spectre-pulse/v1/messages",
               id: "fabric:rest-failover"
             )

    on_exit(fn ->
      Spectre.Pulse.disconnect(websocket.id)
      Spectre.Pulse.disconnect(rest.id)
    end)

    assert {:ok, %Receipt{via: :rest, route_id: route_id}} =
             Spectre.Pulse.deliver(envelope, [])

    assert route_id == rest.id
    assert_receive {:automatic_rest_body, request_body}
    assert {:ok, ^envelope} = JSON.decode(request_body, [])
    Task.await(server, 5_000)
  end

  test "applications can register a custom driver without exposing it to Agents" do
    address = "spectre://fabric/custom"

    runtime =
      start_supervised!(
        {Spectre.Pulse,
         id: :custom_pulse_runtime,
         transports: [
           {:discovery_test, CustomTransport, priority: 15}
         ]}
      )

    assert is_pid(runtime)
    assert Fabric.transports().discovery_test.priority == 15

    assert {:ok, route} =
             Spectre.Pulse.connect(address, :discovery_test, self(), id: "fabric:custom")

    on_exit(fn -> Spectre.Pulse.disconnect(route.id) end)

    envelope = envelope(address)

    assert {:ok, %Receipt{via: :custom, route_id: "fabric:custom"}} =
             Spectre.Pulse.deliver(envelope, [])

    assert_receive {:custom_transport_delivery, "fabric:custom", ^envelope}
  end

  test "use Spectre.Pulse assigns a stable 128-bit address and the runtime subscribes it" do
    expected = Address.for_agent(AutoIdentityAgent)

    assert {:ok, config} = Spectre.Pulse.config(AutoIdentityAgent)
    assert config.identity == expected
    assert expected =~ ~r/^spectre:\/\/pulse\/[0-9a-f]{32}$/
    assert Address.for_agent(AutoIdentityAgent) == expected

    runtime = start_supervised!({Spectre.Pulse, id: :automatic_agent_runtime})
    assert is_pid(runtime)

    assert {:ok, mailbox, %{agent: AutoIdentityAgent, endpoint: AutoIdentityAgent}} =
             Local.lookup(expected)

    assert Process.alive?(mailbox)
  end

  test "a connection owned by a process disappears when that process exits" do
    address = "spectre://fabric/temporary"
    connection = spawn(fn -> Process.sleep(:infinity) end)

    assert {:ok, route} =
             Spectre.Pulse.connect(address, :websocket, connection, id: "fabric:temporary")

    assert [^route] = Fabric.routes(address)

    monitor = Process.monitor(connection)
    Process.exit(connection, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^connection, :killed}

    assert eventually(fn -> Fabric.routes(address) == [] end)
  end

  defp envelope(address) do
    Envelope.new!(
      from: "spectre://fabric/sender",
      to: address,
      act: :request,
      payload: %{type: "routing.test", data: %{"value" => 1}}
    )
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

  defp read_http_body(socket, buffer) do
    case :binary.match(buffer, "\r\n\r\n") do
      {header_end, 4} ->
        header_size = header_end + 4
        headers = binary_part(buffer, 0, header_end)
        body = binary_part(buffer, header_size, byte_size(buffer) - header_size)
        content_length = content_length(headers)

        if byte_size(body) >= content_length do
          binary_part(body, 0, content_length)
        else
          {:ok, more} = :gen_tcp.recv(socket, 0, 5_000)
          read_http_body(socket, buffer <> more)
        end

      :nomatch ->
        {:ok, more} = :gen_tcp.recv(socket, 0, 5_000)
        read_http_body(socket, buffer <> more)
    end
  end

  defp content_length(headers) do
    headers
    |> String.split("\r\n")
    |> Enum.find_value(0, &content_length_header/1)
  end

  defp content_length_header(line) do
    case String.split(line, ":", parts: 2) do
      [name, value] -> parse_content_length(String.downcase(name), value)
      _other -> nil
    end
  end

  defp parse_content_length("content-length", value),
    do: value |> String.trim() |> String.to_integer()

  defp parse_content_length(_name, _value), do: nil
end

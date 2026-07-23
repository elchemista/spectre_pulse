defmodule Spectre.Pulse.NetworkAndTransportTest.NotSentTransport do
  @behaviour Spectre.Pulse.Transport

  alias Spectre.Pulse.Error

  def deliver(route, envelope, _opts) do
    send(route.target, {:attempt, route.id})

    {:error,
     Error.not_sent(:transport, :connection_refused,
       message_id: envelope.id,
       route_id: route.id
     )}
  end
end

defmodule Spectre.Pulse.NetworkAndTransportTest.AcceptingTransport do
  @behaviour Spectre.Pulse.Transport

  alias Spectre.Pulse.Receipt

  def deliver(route, envelope, _opts) do
    send(route.target, {:attempt, route.id})
    {:ok, Receipt.accepted(envelope.id, route_id: route.id, via: :test)}
  end
end

defmodule Spectre.Pulse.NetworkAndTransportTest.UnknownTransport do
  @behaviour Spectre.Pulse.Transport

  alias Spectre.Pulse.Error

  def deliver(route, envelope, _opts) do
    send(route.target, {:attempt, route.id})

    {:error,
     Error.outcome_unknown(:transport, :timeout,
       message_id: envelope.id,
       route_id: route.id
     )}
  end
end

defmodule Spectre.Pulse.NetworkAndTransportTest do
  use ExUnit.Case, async: true

  alias Spectre.Pulse.Codec.JSON
  alias Spectre.Pulse.Envelope
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Network
  alias Spectre.Pulse.Receipt
  alias Spectre.Pulse.Route
  alias Spectre.Pulse.Transport
  alias Spectre.Pulse.Transports.Local
  alias Spectre.Pulse.Transports.PubSub

  alias __MODULE__.AcceptingTransport
  alias __MODULE__.NotSentTransport
  alias __MODULE__.UnknownTransport

  setup do
    envelope =
      Envelope.new!(
        from: "spectre://acme/anna",
        to: "spectre://acme/tao",
        act: :request,
        payload: %{type: "research.perform", data: %{"topic" => "boats"}}
      )

    %{envelope: envelope}
  end

  test "routed network fails over only after a known not-sent outcome", %{envelope: envelope} do
    routes = [
      Route.new!(
        id: "first",
        address: envelope.to,
        transport: NotSentTransport,
        target: self(),
        priority: 1
      ),
      Route.new!(
        id: "second",
        address: envelope.to,
        transport: AcceptingTransport,
        target: self(),
        priority: 2
      )
    ]

    assert {:ok, %Receipt{route_id: "second"}} =
             Network.deliver(nil, envelope, routes: routes)

    assert_receive {:attempt, "first"}
    assert_receive {:attempt, "second"}
  end

  test "routed network stops on ambiguous delivery", %{envelope: envelope} do
    routes = [
      Route.new!(
        id: "ambiguous",
        address: envelope.to,
        transport: UnknownTransport,
        target: self(),
        priority: 1
      ),
      Route.new!(
        id: "must-not-run",
        address: envelope.to,
        transport: AcceptingTransport,
        target: self(),
        priority: 2
      )
    ]

    assert {:error, %Error{outcome: :outcome_unknown, reason: :timeout}} =
             Network.deliver(nil, envelope, routes: routes)

    assert_receive {:attempt, "ambiguous"}
    refute_receive {:attempt, "must-not-run"}
  end

  test "local transport puts the envelope in a process mailbox with send/2", %{
    envelope: envelope
  } do
    test = self()

    endpoint = fn received, context ->
      send(test, {:local_received, received, context})
      :ok
    end

    route = Route.local(envelope.to, self())

    assert {:ok, %Receipt{message_id: id, via: :local}} =
             Transport.dispatch(route, envelope)

    assert id == envelope.id
    assert_receive {:spectre_pulse, sender, ^envelope} = message
    assert sender == self()

    assert {:ok, %Receipt{message_id: ^id, via: :local}} =
             Local.handle_message(message, endpoint)

    assert_receive {:local_received, ^envelope, %{binding: :local, peer: ^sender}}
  end

  test "local transport reports not-sent when the mailbox is unavailable", %{
    envelope: envelope
  } do
    route = Route.local(envelope.to, :missing_pulse_mailbox)

    assert {:error,
            %Error{
              outcome: :not_sent,
              reason: :local_endpoint_not_found
            }} = Transport.dispatch(route, envelope)
  end

  test "websocket transport emits the same JSON envelope as one frame", %{envelope: envelope} do
    route = Route.web_socket(envelope.to, self())

    assert {:ok, %Receipt{via: :websocket}} =
             Transport.dispatch(route, envelope)

    assert_receive {:spectre_pulse_frame, frame}
    assert {:ok, ^envelope} = JSON.decode(frame, [])
  end

  test "BEAM node binding preserves the envelope through the same endpoint contract", %{
    envelope: envelope
  } do
    test = self()

    endpoint = fn received, context ->
      send(test, {:node_received, received, context.peer})
      :ok
    end

    route = Route.node(envelope.to, node(), endpoint)

    assert {:ok, %Receipt{via: :beam_node}} =
             Transport.dispatch(route, envelope)

    assert_receive {:node_received, ^envelope, peer}
    assert peer == node()
  end

  test "REST transport posts the JSON envelope and accepts only a matching receipt", %{
    envelope: envelope
  } do
    {:ok, listener} =
      :gen_tcp.listen(0, [:binary, packet: :raw, active: false, reuseaddr: true])

    {:ok, {_address, port}} = :inet.sockname(listener)
    test = self()

    server =
      Task.async(fn ->
        {:ok, socket} = :gen_tcp.accept(listener, 5_000)
        request_body = read_http_body(socket, "")
        send(test, {:rest_body, request_body})

        receipt =
          envelope.id
          |> Receipt.accepted(via: :rest)
          |> Receipt.to_wire()
          |> Jason.encode!()

        response = [
          "HTTP/1.1 202 Accepted\r\n",
          "content-type: application/json\r\n",
          "content-length: ",
          Integer.to_string(byte_size(receipt)),
          "\r\nconnection: close\r\n\r\n",
          receipt
        ]

        :ok = :gen_tcp.send(socket, response)
        :gen_tcp.close(socket)
        :gen_tcp.close(listener)
      end)

    route = Route.rest(envelope.to, "http://127.0.0.1:#{port}/spectre-pulse/v1/messages")

    assert {:ok, %Receipt{message_id: id, via: :rest, route_id: route_id}} =
             Transport.dispatch(route, envelope)

    assert id == envelope.id
    assert route_id == route.id
    assert_receive {:rest_body, request_body}
    assert {:ok, ^envelope} = JSON.decode(request_body, [])
    Task.await(server, 5_000)
  end

  test "generic PubSub accepts broker publication without claiming subscriber processing", %{
    envelope: envelope
  } do
    test = self()

    publisher = fn message ->
      send(test, {:published, message})
      :ok
    end

    route = Route.pub_sub(envelope.to, publisher)

    assert {:ok, %Receipt{via: :pub_sub}} =
             PubSub.deliver(route, envelope, [])

    assert_receive {:published, {:spectre_pulse, ^envelope}}
    assert {:ok, reachability} = PubSub.probe(route, [])
    assert reachability.status == :unknown
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

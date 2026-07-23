defmodule Spectre.Pulse.TransportAdaptersErrorsTest.BroadcastAdapter do
  def broadcast(server, topic, event) do
    send(server, {:broadcast, topic, event})
    :ok
  end
end

defmodule Spectre.Pulse.TransportAdaptersErrorsTest.PublishAdapter do
  def publish(server, topic, event) do
    send(server, {:publish, topic, event})
    {:ok, :published}
  end
end

defmodule Spectre.Pulse.TransportAdaptersErrorsTest.WebSocketSender do
  def send_frame({pid, reply}, frame) do
    send(pid, {:module_frame, frame})
    reply
  end
end

defmodule Spectre.Pulse.TransportAdaptersErrorsTest.TargetAgent do
  use Spectre.Agent
  use Spectre.Pulse

  pulsing do
    identity("spectre://adapters/receiver")
  end

  flow :inbound do
    on :perform, pulse: "adapters.perform" do
      run(:perform)
    end
  end

  def perform(_input, _context), do: :accepted
end

defmodule Spectre.Pulse.TransportAdaptersErrorsTest do
  use ExUnit.Case, async: true

  alias Spectre.Pulse.Codec.JSON
  alias Spectre.Pulse.Envelope
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Protocol
  alias Spectre.Pulse.Reachability
  alias Spectre.Pulse.Receipt
  alias Spectre.Pulse.Route
  alias Spectre.Pulse.Transports.Local
  alias Spectre.Pulse.Transports.Node
  alias Spectre.Pulse.Transports.PubSub
  alias Spectre.Pulse.Transports.REST
  alias Spectre.Pulse.Transports.WebSocket

  alias __MODULE__.BroadcastAdapter
  alias __MODULE__.PublishAdapter
  alias __MODULE__.TargetAgent
  alias __MODULE__.WebSocketSender

  setup do
    envelope =
      Envelope.new!(
        from: "spectre://adapters/sender",
        to: "spectre://adapters/receiver",
        act: :request,
        payload: %{type: "adapters.perform", data: %{"value" => 1}}
      )

    %{envelope: envelope}
  end

  test "REST delivery accepts empty and valid receipt responses", %{envelope: envelope} do
    route = Route.rest(envelope.to, "http://adapter.test/messages", id: "rest-success")

    assert {:ok, %Receipt{message_id: message_id, via: :rest, route_id: "rest-success"}} =
             REST.deliver(route, envelope, req_options: [adapter: adapter(202, ""), retry: false])

    assert message_id == envelope.id

    remote_receipt =
      envelope.id
      |> Receipt.accepted(via: :remote, route_id: "remote-route", metadata: %{remote: true})
      |> Receipt.to_wire()

    assert {:ok, receipt} =
             REST.deliver(route, envelope,
               req_options: [adapter: adapter(200, remote_receipt), retry: false]
             )

    assert receipt.via == :rest
    assert receipt.route_id == route.id
    assert receipt.metadata.remote
    assert receipt.metadata.remote_via == "remote"
    assert receipt.metadata.remote_route_id == "remote-route"

    encoded_receipt = Jason.encode!(remote_receipt)

    assert {:ok, %Receipt{message_id: ^message_id}} =
             REST.deliver(route, envelope,
               req_options: [adapter: adapter(202, encoded_receipt), retry: false]
             )
  end

  test "REST delivery classifies HTTP and receipt failures without ambiguity loss", %{
    envelope: envelope
  } do
    route = Route.rest(envelope.to, "http://adapter.test/messages", id: "rest-errors")

    assert {:error, %Error{outcome: :not_sent, reason: {:http_rejected, 409}}} =
             REST.deliver(route, envelope, req_options: [adapter: adapter(409, ""), retry: false])

    assert {:error, %Error{outcome: :outcome_unknown, reason: {:http_failure, 503}}} =
             REST.deliver(route, envelope, req_options: [adapter: adapter(503, ""), retry: false])

    assert {:error, %Error{reason: {:invalid_receipt_json, _reason}}} =
             REST.deliver(route, envelope,
               req_options: [adapter: adapter(202, "{"), retry: false]
             )

    assert {:error, %Error{reason: {:invalid_receipt_body, :invalid}}} =
             REST.deliver(route, envelope,
               req_options: [adapter: adapter(202, :invalid), retry: false]
             )

    mismatched =
      Spectre.Identity.uuid7()
      |> Receipt.accepted()
      |> Receipt.to_wire()

    assert {:error, %Error{reason: :receipt_message_mismatch}} =
             REST.deliver(route, envelope,
               req_options: [adapter: adapter(202, mismatched), retry: false]
             )

    invalid_receipt = %{
      "message_id" => "invalid",
      "status" => "accepted",
      "accepted_at" => DateTime.to_iso8601(DateTime.utc_now())
    }

    assert {:error,
            %Error{
              outcome: :outcome_unknown,
              reason: {:invalid_receipt_message_id, "invalid"},
              route_id: "rest-errors"
            }} =
             REST.deliver(route, envelope,
               req_options: [adapter: adapter(202, invalid_receipt), retry: false]
             )
  end

  test "REST delivery distinguishes definitely-unsent network failures", %{envelope: envelope} do
    route = Route.rest(envelope.to, "http://adapter.test/messages", id: "rest-network")

    assert {:error, %Error{outcome: :not_sent, reason: :econnrefused}} =
             REST.deliver(route, envelope,
               req_options: [adapter: error_adapter(:econnrefused), retry: false]
             )

    assert {:error, %Error{outcome: :outcome_unknown, reason: :timeout}} =
             REST.deliver(route, envelope,
               req_options: [adapter: error_adapter(:timeout), retry: false]
             )

    invalid_route = %{route | target: :invalid}

    assert {:error, %Error{outcome: :not_sent, reason: {:invalid_rest_url, :invalid}}} =
             REST.deliver(invalid_route, envelope, [])

    invalid_envelope = %{envelope | metadata: %{callback: fn -> :ok end}}

    assert {:error, %Error{kind: :codec, reason: {:json_encode_failed, _reason}}} =
             REST.deliver(route, invalid_envelope, [])

    raising_route = %{route | metadata: %{req_options: :invalid}}

    assert {:error, %Error{reason: {:rest_exception, %FunctionClauseError{}}}} =
             REST.deliver(raising_route, envelope, [])
  end

  test "REST probe observes reachable, rejected-server and connection failure states", %{
    envelope: envelope
  } do
    {reachable_url, reachable_server} = http_server(204)
    route = Route.rest(envelope.to, reachable_url, id: "rest-probe-reachable")

    assert {:ok, %Reachability{status: :reachable, metadata: %{status: 204}}} =
             REST.probe(route, valid_for_ms: 9_000)

    Task.await(reachable_server, 5_000)

    {failure_url, failure_server} = http_server(600)
    failure_route = %{route | id: "rest-probe-failure", target: failure_url}

    assert {:ok,
            %Reachability{
              status: :unreachable,
              reason: {:http_status, 600}
            }} = REST.probe(failure_route, [])

    Task.await(failure_server, 5_000)

    refused_url = closed_local_url()
    refused_route = %{route | id: "rest-probe-refused", target: refused_url}

    assert {:ok, %Reachability{status: :unreachable, reason: reason}} =
             REST.probe(refused_route, timeout: 200, req_options: [retry: false])

    assert reason in [:econnrefused, :closed]

    assert {:ok, %Reachability{status: :unknown, reason: :invalid_rest_url}} =
             REST.probe(%{route | target: :invalid}, [])
  end

  test "REST inbound authentication supports every callback contract", %{envelope: envelope} do
    {:ok, body} = JSON.encode(envelope, [])
    endpoint = fn _received, _context -> :ok end

    response =
      REST.handle_request(body, [{"Authorization", "Bearer token"}], :peer,
        target: endpoint,
        authenticator: fn headers, peer, opts ->
          assert headers["authorization"] == "Bearer token"
          assert peer == :peer
          assert opts[:marker] == :present
          {:ok, envelope.from}
        end,
        marker: :present
      )

    assert response.status == 202

    unauthenticated =
      REST.handle_request(body, %{}, :peer,
        target: endpoint,
        allow_unauthenticated: true
      )

    assert unauthenticated.status == 202

    existing = Error.not_sent(:authentication, :revoked)

    failures = [
      {fn _headers, _peer -> {:error, existing} end, "revoked"},
      {fn _headers, _peer -> {:error, :invalid_token} end, "invalid_token"},
      {fn _headers, _peer -> :invalid end, "invalid_auth_result"},
      {fn _headers, _peer -> raise "auth crash" end, "authenticator_exception"}
    ]

    for {authenticator, expected_reason} <- failures do
      response =
        REST.handle_request(body, %{}, :peer,
          target: endpoint,
          authenticator: authenticator
        )

      assert response.status == 401
      assert Jason.decode!(response.body)["reason"] == expected_reason
    end

    assert REST.handle_request(body, %{}, :peer,
             target: endpoint,
             authenticator: :invalid
           ).status == 401
  end

  test "REST inbound maps codec, validation, authorization, routing and endpoint errors", %{
    envelope: envelope
  } do
    auth = fn _headers, _peer -> {:ok, envelope.from, %{authenticated: true}} end
    {:ok, body} = JSON.encode(envelope, [])

    assert REST.handle_request("{", %{}, :peer, authenticator: auth).status == 400

    invalid_version =
      body
      |> Jason.decode!()
      |> Map.put("version", 2)
      |> Jason.encode!()

    assert REST.handle_request(invalid_version, %{}, :peer, authenticator: auth).status == 422

    assert REST.handle_request(body, %{}, :peer,
             target: TargetAgent,
             authenticator: auth,
             authorize: fn _envelope, _context, _target -> false end
           ).status == 403

    assert REST.handle_request(body, %{}, :peer,
             target: :invalid_endpoint_module,
             authenticator: auth
           ).status == 404

    assert REST.handle_request(body, %{}, :peer,
             target: fn _envelope, _context -> {:error, :endpoint_failure} end,
             authenticator: auth
           ).status == 503
  end

  test "PubSub delivery supports function and adapter targets", %{envelope: envelope} do
    unary = fn message ->
      send(self(), {:unary_publish, message})
      {:ok, :published}
    end

    route = Route.pub_sub(envelope.to, unary, id: "pubsub-unary")
    assert {:ok, %Receipt{via: :pub_sub}} = PubSub.deliver(route, envelope, [])
    assert_receive {:unary_publish, {:spectre_pulse, ^envelope}}

    binary = fn topic, message ->
      send(self(), {:binary_publish, topic, message})
      :ok
    end

    assert {:ok, %Receipt{}} =
             PubSub.deliver(%{route | target: binary}, envelope, [])

    assert_receive {:binary_publish, topic, {:spectre_pulse, ^envelope}}
    assert topic == envelope.to

    broadcast_target = %{
      adapter: BroadcastAdapter,
      server: self(),
      topic: "events",
      event: {:custom, envelope.id}
    }

    assert {:ok, %Receipt{}} =
             PubSub.deliver(%{route | target: broadcast_target}, envelope, [])

    assert_receive {:broadcast, "events", {:custom, message_id}}
    assert message_id == envelope.id

    publish_target = %{adapter: PublishAdapter, server: self(), topic: "events"}

    assert {:ok, %Receipt{}} =
             PubSub.deliver(%{route | target: publish_target}, envelope, [])

    assert_receive {:publish, "events", {:spectre_pulse, ^envelope}}
  end

  test "PubSub delivery and inbound reject every malformed contract", %{envelope: envelope} do
    route = Route.pub_sub(envelope.to, fn _message -> {:error, :broker_down} end)

    assert {:error, %Error{reason: :broker_down}} = PubSub.deliver(route, envelope, [])

    assert {:error, %Error{reason: {:invalid_pub_sub_result, :invalid}}} =
             PubSub.deliver(%{route | target: fn _message -> :invalid end}, envelope, [])

    assert {:error, %Error{reason: {:pub_sub_exception, %RuntimeError{}}}} =
             PubSub.deliver(
               %{route | target: fn _message -> raise "broker crash" end},
               envelope,
               []
             )

    assert {:error, %Error{reason: {:invalid_pub_sub_adapter, Protocol}}} =
             PubSub.deliver(
               %{route | target: %{adapter: Protocol, server: self(), topic: "events"}},
               envelope,
               []
             )

    assert {:error, %Error{reason: {:invalid_pub_sub_target, :invalid}}} =
             PubSub.deliver(%{route | target: :invalid}, envelope, [])

    assert {:error, %Error{reason: :invalid_pub_sub_message}} =
             PubSub.handle_message(:invalid, %{})

    assert {:error, %Error{reason: :invalid_pub_sub_message}} =
             PubSub.handle_message(:invalid, nil, %{}, [])

    endpoint = fn received, context ->
      send(self(), {:pubsub_endpoint, received, context.binding})
      :ok
    end

    assert {:ok, %Receipt{via: :pub_sub}} =
             PubSub.handle_message(
               {:spectre_pulse, envelope},
               endpoint,
               %{authenticated_identity: envelope.from},
               []
             )

    assert_receive {:pubsub_endpoint, ^envelope, :pub_sub}
  end

  test "WebSocket delivery supports all sender forms and normalizes failures", %{
    envelope: envelope
  } do
    route = Route.web_socket(envelope.to, fn _frame -> {:ok, :sent} end, id: "websocket")
    assert {:ok, %Receipt{via: :websocket}} = WebSocket.deliver(route, envelope, [])

    existing = Error.not_sent(:transport, :closed)

    assert {:error, %Error{reason: :closed, route_id: "websocket"}} =
             WebSocket.deliver(
               %{route | target: fn _frame -> {:error, existing} end},
               envelope,
               []
             )

    assert {:error, %Error{outcome: :not_sent, reason: :closed}} =
             WebSocket.deliver(
               %{route | target: fn _frame -> {:error, {:not_sent, :closed}} end},
               envelope,
               []
             )

    assert {:error, %Error{outcome: :outcome_unknown, reason: :timeout}} =
             WebSocket.deliver(
               %{route | target: fn _frame -> {:error, :timeout} end},
               envelope,
               []
             )

    assert {:error, %Error{reason: {:invalid_websocket_send_result, :invalid}}} =
             WebSocket.deliver(%{route | target: fn _frame -> :invalid end}, envelope, [])

    module_target = {WebSocketSender, {self(), :ok}}
    assert {:ok, %Receipt{}} = WebSocket.deliver(%{route | target: module_target}, envelope, [])
    assert_receive {:module_frame, frame}
    assert {:ok, ^envelope} = JSON.decode(frame, [])

    map_target = %{module: WebSocketSender, connection: {self(), {:ok, :sent}}}
    assert {:ok, %Receipt{}} = WebSocket.deliver(%{route | target: map_target}, envelope, [])
    assert_receive {:module_frame, _frame}

    assert {:error, %Error{outcome: :not_sent, reason: {:invalid_websocket_sender, Protocol}}} =
             WebSocket.deliver(%{route | target: {Protocol, :connection}}, envelope, [])

    assert {:error, %Error{outcome: :not_sent, reason: {:invalid_websocket_target, :invalid}}} =
             WebSocket.deliver(%{route | target: :invalid}, envelope, [])
  end

  test "WebSocket probes every target form and rejects malformed frames", %{envelope: envelope} do
    route = Route.web_socket(envelope.to, self(), id: "websocket-probe")
    assert {:ok, %Reachability{status: :reachable}} = WebSocket.probe(route, [])

    dead = spawn(fn -> :ok end)
    monitor = Process.monitor(dead)
    assert_receive {:DOWN, ^monitor, :process, ^dead, :normal}

    assert {:ok, %Reachability{status: :unreachable}} =
             WebSocket.probe(%{route | target: dead}, [])

    for target <- [
          fn _frame -> :ok end,
          {WebSocketSender, {self(), :ok}},
          %{module: WebSocketSender, connection: {self(), :ok}}
        ] do
      assert {:ok, %Reachability{status: :reachable}} =
               WebSocket.probe(%{route | target: target}, [])
    end

    assert {:ok, %Reachability{status: :unknown}} =
             WebSocket.probe(%{route | target: :invalid}, [])

    assert {:error, %Error{kind: :codec}} = WebSocket.handle_frame("{", %{})
  end

  test "Node transport validates targets, reachability and remote failures", %{envelope: envelope} do
    endpoint = fn _received, _context -> :ok end
    route = Route.node(envelope.to, node(), endpoint, id: "node-local")

    assert {:ok, %Receipt{via: :beam_node, route_id: "node-local"}} =
             Node.deliver(route, envelope, [])

    existing = Error.not_sent(:inbound, :rejected)
    rejecting = Route.node(envelope.to, node(), fn _envelope, _context -> {:error, existing} end)

    assert {:error, %Error{reason: :rejected, route_id: route_id}} =
             Node.deliver(rejecting, envelope, [])

    assert route_id == rejecting.id

    sleeping =
      Route.node(envelope.to, node(), fn _envelope, _context ->
        Process.sleep(100)
        :ok
      end)

    assert {:error, %Error{outcome: :outcome_unknown, reason: :node_timeout}} =
             Node.deliver(sleeping, envelope, timeout: 1)

    unavailable =
      Route.node(envelope.to, :"pulse_missing@127.0.0.1", endpoint, id: "node-missing")

    assert {:error, %Error{outcome: :not_sent, reason: :node_not_connected}} =
             Node.deliver(unavailable, envelope, timeout: 10)

    invalid = %{route | target: :invalid}

    assert {:error, %Error{reason: {:invalid_node_target, :invalid}}} =
             Node.deliver(invalid, envelope, [])

    assert {:ok, %Reachability{status: :reachable}} = Node.probe(route, [])

    assert {:ok, %Reachability{status: :unreachable, reason: :node_not_connected}} =
             Node.probe(unavailable, [])

    assert {:ok, %Reachability{status: :unknown, reason: :invalid_node_target}} =
             Node.probe(invalid, [])
  end

  test "Node accept_remote carries peer facts and optional result callback", %{envelope: envelope} do
    parent = self()

    endpoint = fn received, context ->
      send(parent, {:node_endpoint, received, context})
      :ok
    end

    assert {:ok, %Receipt{via: :beam_node}} =
             Node.accept_remote(endpoint, envelope, :trusted@node,
               target_identity: envelope.to,
               inbound_opts: [marker: :preserved],
               on_result: fn _result -> :ok end
             )

    assert_receive {:node_endpoint, ^envelope, context}
    assert context.binding == :beam_node
    assert context.peer == :trusted@node
    assert context.verified == %{beam_node: :trusted@node}
  end

  test "Local transport resolves names, probes mailboxes and rejects malformed messages", %{
    envelope: envelope
  } do
    registered = :"pulse_local_#{System.unique_integer([:positive])}"
    Process.register(self(), registered)
    on_exit(fn -> if Process.whereis(registered), do: Process.unregister(registered) end)

    route = Route.local(envelope.to, registered, id: "local-registered")

    assert {:ok, %Receipt{via: :local}} = Local.deliver(route, envelope, [])
    assert_receive {:spectre_pulse, sender, ^envelope}
    assert sender == self()
    assert {:ok, %Reachability{status: :reachable}} = Local.probe(route, [])

    missing = %{route | target: :"missing_#{System.unique_integer([:positive])}"}

    assert {:ok, %Reachability{status: :unreachable, reason: :local_endpoint_not_found}} =
             Local.probe(missing, [])

    invalid = %{route | target: {:invalid, :name, :tuple}}

    assert {:error, %Error{reason: {:invalid_local_endpoint, _target}}} =
             Local.deliver(invalid, envelope, [])

    assert {:ok, %Reachability{status: :unreachable, reason: {:invalid_local_endpoint, _target}}} =
             Local.probe(invalid, [])

    assert {:error, %Error{reason: :invalid_local_mailbox_message}} =
             Local.handle_message(:invalid, nil)

    endpoint = fn received, context ->
      send(self(), {:local_endpoint, received, context})
      :ok
    end

    message = {:spectre_pulse, self(), envelope}

    assert {:ok, %Receipt{via: :local}} =
             Local.handle_message(message, endpoint,
               context: %{verified: %{supplied: true}},
               authenticated_identity: envelope.from,
               target_identity: envelope.to,
               authorize: fn _envelope, _context, _target -> true end
             )

    assert_receive {:local_endpoint, ^envelope, context}
    assert context.binding == :local
    assert context.target == nil
    assert context.target_identity == envelope.to
    assert context.verified.supplied
    assert context.verified.trust_boundary == :beam_vm
  end

  defp adapter(status, body) do
    fn request -> {request, %Req.Response{status: status, body: body}} end
  end

  defp error_adapter(reason) do
    fn request -> {request, %Req.TransportError{reason: reason}} end
  end

  defp http_server(status) do
    {:ok, listener} =
      :gen_tcp.listen(0, [:binary, packet: :raw, active: false, reuseaddr: true])

    {:ok, {_address, port}} = :inet.sockname(listener)

    server =
      Task.async(fn ->
        {:ok, socket} = :gen_tcp.accept(listener, 5_000)
        {:ok, _request} = :gen_tcp.recv(socket, 0, 5_000)

        response = "HTTP/1.1 #{status} Test\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
        :ok = :gen_tcp.send(socket, response)
        :gen_tcp.close(socket)
        :gen_tcp.close(listener)
      end)

    {"http://127.0.0.1:#{port}/pulse", server}
  end

  defp closed_local_url do
    {:ok, listener} =
      :gen_tcp.listen(0, [:binary, packet: :raw, active: false, reuseaddr: true])

    {:ok, {_address, port}} = :inet.sockname(listener)
    :gen_tcp.close(listener)
    "http://127.0.0.1:#{port}/pulse"
  end
end

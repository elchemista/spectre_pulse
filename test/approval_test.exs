defmodule Spectre.Pulse.ApprovalTest do
  use ExUnit.Case, async: false
  alias Spectre.Server
  alias Spectre.Pulse.{Approval, Envelope, Receipt, Route}
  alias Spectre.Pulse.Codec.JSON
  alias Spectre.Pulse.Transports.Local

  defmodule Network do
    @behaviour Spectre.Pulse.Network
    def deliver(envelope, _) do
      # Verify the interoperable wire projection, then use the real local binding.
      with {:ok, encoded} <- JSON.encode(envelope, []),
           {:ok, envelope} <- JSON.decode(encoded, []) do
        Local.deliver(Route.local(envelope.to, Spectre.Pulse.ApprovalTest), envelope, [])
      end
    end
  end

  defmodule RESTNetwork do
    @behaviour Spectre.Pulse.Network
    def deliver(envelope, opts) do
      route = Route.rest(envelope.to, Map.fetch!(opts[:http_routes], envelope.to))

      token =
        if envelope.from == "spectre://client/assistant",
          do: "customer-test-token",
          else: "owner-test-token"

      Spectre.Pulse.Transports.REST.deliver(route, envelope,
        headers: [{"authorization", "Bearer " <> token}]
      )
    end
  end

  defmodule Effect do
    @behaviour Spectre.Server.GAM.EffectAdapter
    def execute_effect(:open, args, _) do
      send(Spectre.Pulse.ApprovalTest, {:browser_opened, args})
      {:ok, %{opened: true}}
    end
  end

  defmodule Package do
    use Spectre.Stack.Installable, id: :pulse_approval_test, version: "0.1.0", spectre: "~> 0.4.0"

    def effects(_),
      do: %{
        browser_open: %{
          contract: 1,
          adapter: Effect,
          operation: :open,
          side_effect: :non_idempotent,
          material: :arguments,
          partition: [],
          schema_hash: nil,
          config: %{}
        }
      }
  end

  defmodule Stack do
    use Spectre.Stack
    install(Package)
  end

  defmodule App do
    use Spectre.Server

    def handle_flow({:web, :open}, _, ctx),
      do: propose(ctx, :browser_open, %{"url" => "https://example.test"}, on_result: :opened)

    def handle_result(:opened, result, ctx) do
      send(Spectre.Pulse.ApprovalTest, {:completed, result})
      no_response(ctx)
    end

    def handle_result(tag, result, ctx), do: super(tag, result, ctx)
  end

  defmodule Agent do
    use Spectre.Agent, stack: Stack, server: App

    flow :web do
      on(:open, regex: ~r/.*/)
    end
  end

  defmodule LocalClient do
    use Spectre.Agent, stack: Stack, server: App
    use Spectre.Pulse

    pulsing do
      identity("spectre://client/assistant")
    end

    flow :web do
      on(:open, regex: ~r/.*/)
    end
  end

  defmodule LocalOwner do
    use Spectre.Agent, stack: Stack, server: App
    use Spectre.Pulse

    pulsing do
      identity("spectre://owner/assistant")
    end

    flow :web do
      on(:open, regex: ~r/.*/)
    end
  end

  setup context do
    Process.register(self(), __MODULE__)
    {public, private} = :crypto.generate_key(:eddsa, :ed25519)
    principal = %{kind: :human, attributes: %{"subject" => "owner"}}
    http = if context[:rest], do: http_listeners()

    network_opts =
      cond do
        http -> [network: RESTNetwork, http_routes: http.routes]
        context[:routed] -> []
        true -> [network: Network]
      end

    transport = {Approval, network_opts}

    client =
      start_supervised!(
        {Server,
         [
           id: :client,
           agent: if(context[:routed], do: LocalClient, else: Agent),
           consumer: self(),
           gam: [
             operations: %{
               browser_open: [confirm: true, approver: {:external, "spectre://owner/assistant"}]
             },
             approvals: [
               address: "spectre://client/assistant",
               transport: transport,
               approvers: %{
                 "spectre://owner/assistant" => [
                   principal: principal,
                   public_key: public,
                   mandate_ref: "owner:browser-consent"
                 ]
               }
             ]
           ]
         ]}
      )

    owner =
      start_supervised!(
        {Server,
         [
           id: :owner,
           agent: if(context[:routed], do: LocalOwner, else: Agent),
           consumer: self(),
           gam: [
             approvals: [
               address: "spectre://owner/assistant",
               transport: transport,
               signer: [
                 principal: principal,
                 private_key: private,
                 mandate_ref: "owner:browser-consent"
               ]
             ]
           ]
         ]}
      )

    if context[:routed] do
      {:ok, client_endpoint} = Spectre.Pulse.Local.subscribe(LocalClient)
      {:ok, owner_endpoint} = Spectre.Pulse.Local.subscribe(LocalOwner)
      # Reuse automatic subscriptions when the host binds its live Servers.
      assert {:ok, ^client_endpoint} =
               Spectre.Pulse.Local.subscribe(LocalClient, inbound: [agent_server: client])

      assert {:ok, ^owner_endpoint} =
               Spectre.Pulse.Local.subscribe(LocalOwner, inbound: [agent_server: owner])

      on_exit(fn ->
        for endpoint <- [client_endpoint, owner_endpoint],
            do: DynamicSupervisor.terminate_child(Spectre.Pulse.Local.Supervisor, endpoint)
      end)
    end

    if http do
      start_http_endpoint(http.client, client, "spectre://client/assistant")
      start_http_endpoint(http.owner, owner, "spectre://owner/assistant")
    end

    %{client: client, owner: owner}
  end

  defp receive_local(message, server, identity) do
    Local.handle_message(message, nil,
      agent_server: server,
      target_identity: identity,
      target_resolver: fn _, _ -> {:ok, server} end,
      context: %{target: server, authenticated_identity: elem(message, 2).from}
    )
  end

  for decision <- [:confirm, :reject] do
    @decision decision
    test "local Pulse carries human #{@decision} between two AgentServers", %{
      client: client,
      owner: owner
    } do
      assert {:ok, _} = Server.input(client, "browse")

      assert_receive {:spectre_pulse, _,
                      %Envelope{payload: %{type: "approval.request"}} = request} = incoming,
                     5_000

      assert request.version == 1
      assert {:ok, %Receipt{status: :accepted}} = receive_local(incoming, owner, request.to)
      assert_receive {Server, ^owner, {:approval_request, material}}
      refute_receive {:browser_opened, _}
      refute_receive {Server, ^client, {:pending, _, _}}
      assert :ok = Server.approval_presented(owner, request.id)
      assert_receive {:spectre_pulse, _, %{payload: %{type: "approval.presented"}}} = shown
      assert {:ok, _} = receive_local(shown, client, request.from)
      assert_receive {Server, ^client, {:pending, _, _}}, 5_000
      assert :ok = Server.approval_decide(owner, request.id, @decision)

      assert_receive {:spectre_pulse, _, %{payload: %{type: "approval.decision"}} = response} =
                       decided

      assert response.relates_to == request.id

      assert response.payload.data["request_digest"] ==
               Spectre.GAM.Canonical.Value.digest!(material)

      assert {:ok, _} = receive_local(decided, client, request.from)

      if @decision == :confirm do
        assert_receive {:browser_opened, %{"url" => "https://example.test"}}, 5_000
        assert_receive {:completed, {:governed, _}}, 5_000
      else
        assert_receive {:completed, {:consent, :rejected}}, 5_000
        refute_receive {:browser_opened, _}
      end

      assert {:error, _} = receive_local(decided, client, request.from)
      refute_receive {:browser_opened, _}
    end
  end

  @tag routed: true
  test "default discovery and existing Local subscriptions deliver directly to both Servers", %{
    client: client,
    owner: owner
  } do
    assert {:ok, _} = Server.input(client, "browse")
    assert_receive {Server, ^owner, {:approval_request, request}}, 5_000
    refute_receive {:browser_opened, _}
    :ok = Server.approval_presented(owner, request["id"])
    assert_receive {Server, ^client, {:pending, _, _}}, 5_000
    :ok = Server.approval_decide(owner, request["id"], :confirm)
    assert_receive {:browser_opened, %{"url" => "https://example.test"}}, 5_000
    assert_receive {:completed, {:governed, _}}, 5_000
    refute_receive {:browser_opened, _}
  end

  for choice <- [:confirm, :reject] do
    @choice choice
    @tag rest: true
    test "authenticated HTTP carries #{@choice} between independent AgentServers", %{
      client: client,
      owner: owner
    } do
      assert {:ok, _} = Server.input(client, "browse")
      assert_receive {Server, ^owner, {:approval_request, request}}, 5_000
      refute_receive {:browser_opened, _}
      :ok = Server.approval_presented(owner, request["id"])
      assert_receive {Server, ^client, {:pending, _, _}}, 5_000
      :ok = Server.approval_decide(owner, request["id"], @choice)

      if @choice == :confirm do
        assert_receive {:browser_opened, %{"url" => "https://example.test"}}, 5_000
        assert_receive {:completed, {:governed, _}}, 5_000
      else
        assert_receive {:completed, {:consent, :rejected}}, 5_000
        refute_receive {:browser_opened, _}
      end
    end
  end

  defp http_listeners do
    options = [:binary, packet: :http_bin, active: false, ip: {127, 0, 0, 1}, reuseaddr: true]
    {:ok, client} = :gen_tcp.listen(0, options)
    {:ok, owner} = :gen_tcp.listen(0, options)
    {:ok, {_, client_port}} = :inet.sockname(client)
    {:ok, {_, owner_port}} = :inet.sockname(owner)

    on_exit(fn ->
      :gen_tcp.close(client)
      :gen_tcp.close(owner)
    end)

    %{
      client: client,
      owner: owner,
      routes: %{
        "spectre://client/assistant" => "http://127.0.0.1:#{client_port}/approval",
        "spectre://owner/assistant" => "http://127.0.0.1:#{owner_port}/approval"
      }
    }
  end

  defp start_http_endpoint(listener, server, identity) do
    start_supervised!(%{
      id: {:approval_http, identity},
      start: {Task, :start_link, [fn -> http_loop(listener, server, identity) end]}
    })
  end

  defp http_loop(listener, server, identity) do
    case :gen_tcp.accept(listener) do
      {:ok, socket} ->
        {:ok, {:http_request, :POST, _, _}} = :gen_tcp.recv(socket, 0, 5_000)
        headers = http_headers(socket, %{})
        :ok = :inet.setopts(socket, packet: :raw)
        {:ok, body} = :gen_tcp.recv(socket, String.to_integer(headers["content-length"]), 5_000)

        response =
          Spectre.Pulse.Transports.REST.handle_request(body, headers, :loopback,
            agent_server: server,
            target_identity: identity,
            target_resolver: fn _, _ -> {:ok, server} end,
            authenticator: &http_identity/2
          )

        :ok =
          :gen_tcp.send(socket, [
            "HTTP/1.1 ",
            to_string(response.status),
            " Result\r\nContent-Type: application/json\r\nContent-Length: ",
            to_string(byte_size(response.body)),
            "\r\nConnection: close\r\n\r\n",
            response.body
          ])

        :gen_tcp.close(socket)
        http_loop(listener, server, identity)

      {:error, :closed} ->
        :ok
    end
  end

  defp http_headers(socket, headers) do
    case :gen_tcp.recv(socket, 0, 5_000) do
      {:ok, {:http_header, _, key, _, value}} ->
        http_headers(socket, Map.put(headers, String.downcase(to_string(key)), value))

      {:ok, :http_eoh} ->
        headers
    end
  end

  defp http_identity(headers, _peer) do
    case headers["authorization"] do
      "Bearer customer-test-token" -> {:ok, "spectre://client/assistant"}
      "Bearer owner-test-token" -> {:ok, "spectre://owner/assistant"}
      _ -> {:error, :unauthenticated}
    end
  end

  test "approval protocol rejects unauthenticated contexts even if ordinary messages allow them",
       %{owner: owner} do
    envelope =
      Envelope.new!(
        from: "spectre://client/assistant",
        to: "spectre://owner/assistant",
        act: :request,
        payload: %{type: "approval.request", data: %{}}
      )

    assert {:error, %{kind: :authentication}} =
             Spectre.Pulse.receive(
               envelope,
               %{target: owner, target_identity: envelope.to},
               agent_server: owner,
               allow_unauthenticated: true
             )

    assert {:error, %{kind: :authentication}} =
             Spectre.Pulse.receive(
               envelope,
               %{
                 target: owner,
                 target_identity: envelope.to,
                 authenticated_identity: "spectre://forged/agent"
               },
               agent_server: owner
             )

    assert {:error, _} =
             Approval.validate_ingress(
               %{envelope | payload: %{envelope.payload | data: %{"from" => "smuggled"}}},
               true
             )

    assert :ok = Approval.validate_ingress(envelope, true)
  end
end

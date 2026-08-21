# Connections and transports

Pulse separates logical Agent identity from physical delivery. Agents address
each other with canonical Pulse addresses; the host application owns sockets,
URLs, brokers, Erlang nodes, credentials, and transport processes.

```text
Agent contact
  → canonical address
  → Discovery gathers current routes
  → Network orders routes
  → one Transport delivers the unchanged envelope
```

## Connection definitions

A `ConnectionSpec` describes one class of authenticated physical connections:

```elixir
{Spectre.Pulse,
 connections: [
   [
     id: :studio,
     transport: :websocket,
     mode: :listen,
     agents: :all,
     profiles: ["pulse.messaging/1", "spectre.studio/1"],
     scopes: ["agent.runtime.read"],
     authenticate: &MyApp.PulseAccess.authenticate/2,
     authorize: &MyApp.PulseAccess.authorize/2
   ],
   [
     id: :private_mesh,
     transport: :grpc,
     mode: :both,
     agents: [MyApp.Researcher, "spectre://acme/coordinator"],
     scopes: []
   ]
 ]}
```

Important fields:

| Field | Meaning |
| --- | --- |
| `id` | Stable name used by the host adapter |
| `transport` | Binding name, such as `:websocket` or `:grpc` |
| `mode` | `:listen`, `:connect`, `:both`, or `:stateless` |
| `agents` | `:all` or an allow-list of modules/canonical addresses |
| `profiles` | Protocol profiles this connection class permits |
| `scopes` | Maximum permission scopes this class may grant |
| `authenticate` | Credential-to-principal callback |
| `authorize` | Principal-to-grants callback |
| `priority` | Technical route priority |
| `metadata` | Public technical metadata, never credentials |

Pulse enforces `agent.runtime.read`, `agent.runtime.stream`,
`agent.operations.read`, and `agent.operations.stream` for its built-in Studio
capabilities. Applications may define additional scope names, but their own
authorization callbacks must enforce the meaning of those names.

One definition may create several live connections. One live connection can
expose several Agents. Definitions may overlap, so the same Agent can be
available to Studio and an Agent mesh with different grants.

## Agent discovery and exposure

Pulse discovers every compiled module using `Spectre.Pulse`. Do not duplicate
that catalog in the host application.

By default a connection exposes every discovered Agent:

```elixir
[id: :studio, transport: :websocket, agents: :all]
```

Use an allow-list to define a smaller boundary:

```elixir
[
  id: :ledger_observer,
  transport: :websocket,
  agents: [MyApp.LedgerAgent, "spectre://acme/auditor"]
]
```

The allow-list is resolved when the Pulse runtime starts. Unknown Agent
selectors fail configuration instead of silently exposing an empty or broader
set.

Inspect the configured catalog through:

```elixir
Spectre.Pulse.connection_specs()
Spectre.Pulse.local_agents()
Spectre.Pulse.exposed_agents(:studio)
```

## Live connections

An adapter authenticates a peer and registers the resulting connection once:

```elixir
{:ok, connection} =
  Spectre.Pulse.open_connection(:studio,
    owner: socket_pid,
    transport_pid: socket_pid,
    peer_id: "studio.example",
    principal: %{
      id: "operator-42",
      identity: "spectre://studio/operator-42",
      kind: :studio,
      scopes: ["agent.runtime.read"]
    },
    granted_scopes: ["agent.runtime.read"],
    remote_agents: [],
    verified: %{tls: true}
  )
```

The live connection retains only the principal, grants, exposed addresses,
verified facts, and technical metadata. It never retains the handshake
credential.

The owner PID defines lifecycle. Pulse removes the connection when that process
exits. Adapters may also close it explicitly:

```elixir
Spectre.Pulse.connections()
Spectre.Pulse.connection(connection.id)
Spectre.Pulse.touch_connection(connection.id)
Spectre.Pulse.close_connection(connection.id)
```

The [Phoenix adapter](PHOENIX.md) performs this handshake and lifecycle
automatically.

## Route discovery

When an Agent sends to a contact, Pulse gathers routes from:

1. local Agent subscriptions;
2. routes registered in the live Fabric;
3. explicit compatibility routes;
4. application directories.

Routes are de-duplicated and sorted by technical priority. Built-in defaults
are:

| Binding | Default priority |
| --- | ---: |
| Local | 0 |
| WebSocket | 20 |
| BEAM node | 30 |
| PubSub | 40 |
| REST | 50 |

Lower values are preferred. A connection can override its route priority
without exposing that decision to Agent code.

`Network.Routed` moves to another route only after `outcome: :not_sent`. It
stops after `:outcome_unknown`, because the first transport may already have
accepted the message.

## Connect built-in transports

Local delivery needs no explicit route registration. Starting the Pulse runtime
creates local subscriptions for discovered Agents.

The host registers remote routes as infrastructure becomes available. Every
example below binds the same logical Agent address:

```elixir
address = "spectre://acme/researcher"

{:ok, websocket_route} =
  Spectre.Pulse.connect(address, :websocket, socket_pid)

{:ok, rest_route} =
  Spectre.Pulse.connect(
    address,
    :rest,
    "https://agents.example/spectre-pulse/v1/messages",
    metadata: %{
      headers: [{"authorization", "Bearer " <> access_token}],
      req_options: [retry: false]
    }
  )

{:ok, pub_sub_route} =
  Spectre.Pulse.connect(
    address,
    :pub_sub,
    %{
      adapter: Phoenix.PubSub,
      server: MyApp.PubSub,
      topic: "pulse:researcher"
    }
  )

{:ok, node_route} =
  Spectre.Pulse.connect(
    address,
    :beam_node,
    %{node: :"researcher@agents.internal", endpoint: MyApp.Researcher}
  )
```

Connection options are `id`, `priority`, `metadata`, and `owner`. An `owner`
PID lets Fabric monitor a resource whose target does not itself contain a PID:

```elixir
{:ok, route} =
  Spectre.Pulse.connect(
    address,
    :rest,
    endpoint_url,
    id: "researcher:rest:primary",
    priority: 45,
    owner: connection_owner
  )
```

Removal is idempotent:

```elixir
:ok = Spectre.Pulse.disconnect(route.id)
:ok = Spectre.Pulse.disconnect(route.id)
```

## Transport responsibilities

A transport receives an already validated envelope and a resolved physical
route. It is responsible only for technical handoff and an honest delivery
outcome.

| Binding | Infrastructure target | Inbound entry point |
| --- | --- | --- |
| Local | Subscribed process mailbox | Automatic |
| WebSocket | PID, send function, or adapter/connection | `WebSocket.handle_frame/3` |
| REST | HTTP URL | `REST.handle_request/4` |
| PubSub | Publisher or adapter/server/topic map | `PubSub.handle_message/2` |
| BEAM node | Node and optional endpoint | Automatic through `:erpc` |

Changing the binding must not change envelope ids, correlation, acts, payload
types, or Agent flow behavior.

## Implement a custom transport

Implement `Spectre.Pulse.Transport`:

```elixir
defmodule MyApp.GRPCPulse do
  @behaviour Spectre.Pulse.Transport

  alias Spectre.Pulse.Envelope
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Receipt
  alias Spectre.Pulse.Route
  alias Spectre.Pulse.Transport

  @impl Transport
  def deliver(%Route{} = route, %Envelope{} = envelope, _opts) do
    case MyApp.GRPC.deliver(route.target, Envelope.to_wire(envelope)) do
      {:ok, remote_ack} ->
        {:ok,
         Receipt.accepted(envelope.id,
           via: :grpc,
           route_id: route.id,
           metadata: %{remote_ack: remote_ack}
         )}

      {:error, :not_connected} ->
        {:error,
         Error.not_sent(:transport, :not_connected,
           message_id: envelope.id,
           route_id: route.id
         )}

      {:error, reason} ->
        {:error,
         Error.outcome_unknown(:transport, reason,
           message_id: envelope.id,
           route_id: route.id
         )}
    end
  end
end
```

Register the driver once in the Pulse child:

```elixir
{Spectre.Pulse,
 transports: [
   {:grpc, MyApp.GRPCPulse, priority: 35}
 ]}
```

Connect each physical channel when it becomes available:

```elixir
{:ok, _route} =
  Spectre.Pulse.connect(
    "spectre://acme/researcher",
    :grpc,
    channel,
    owner: channel_process
  )
```

`probe/2` is optional. Without it Pulse reports reachability as `:unknown`
instead of pretending the remote endpoint is online.

The transport must return `not_sent` only when it knows that the envelope did
not cross its handoff boundary. Timeouts and ambiguous failures are
`outcome_unknown`.

## Framework-neutral REST inbound

The host reads the body, headers, and peer, then adapts Pulse's response to Plug,
Phoenix, or another HTTP server:

```elixir
response =
  Spectre.Pulse.Transports.REST.handle_request(
    body,
    headers,
    remote_ip,
    authenticator: fn headers, peer ->
      MyApp.PulseAuth.authenticate(headers, peer)
      # => {:ok, "spectre://acme/coordinator", %{mtls: true}}
    end,
    authorize: &MyApp.PulsePolicy.authorize/3
  )
```

Status `202` means technical acceptance. The Agent's semantic response is a
later correlated envelope, never the synchronous HTTP response.

REST fails closed without an authenticator. A deliberately unauthenticated
private endpoint must set `allow_unauthenticated: true`; recipient validation
and application authorization still run.

## WebSocket and PubSub inbound

Custom consumers use the same inbound bridge and supply authenticated facts:

```elixir
{:ok, inbound} =
  Spectre.Pulse.Transports.WebSocket.handle_frame(frame, %{
    authenticated_identity: peer_identity,
    peer: socket_pid,
    verified: %{tls: true, certificate: certificate_fingerprint}
  })

{:ok, receipt} =
  Spectre.Pulse.Transports.PubSub.handle_message(
    {:spectre_pulse, envelope},
    %{
      authenticated_identity: broker_identity,
      peer: "pulse:researcher",
      verified: %{broker: :authenticated}
    }
  )
```

The surrounding adapter proves the peer. `Envelope.to` selects the locally
subscribed Agent. For the complete trust boundary read
[delivery and security](DELIVERY_AND_SECURITY.md).

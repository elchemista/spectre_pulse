# Phoenix WebSocket integration

Pulse integrates with the Phoenix Endpoint already owned by the host
application. It does not start a second HTTP server and does not add Phoenix as
a package dependency.

Use this adapter when Spectre Studio, a browser, or another remote Agent needs a
persistent bidirectional connection.

## How the integration is divided

```text
Phoenix / Bandit
  owns HTTP, TLS, origin policy and WebSocket upgrade
        │
        ▼
Spectre.Pulse.Phoenix
  authenticates one physical connection
  registers its principal, grants and exposed Agents
        │
        ├── Pulse envelopes → ordinary Agent inbound flow
        └── Studio calls    → connection-scoped control operations
```

Phoenix remains responsible for network serving. Pulse remains responsible for
protocol validation, authenticated connection state, Agent exposure, messages,
and monitoring subscription lifecycle.

## 1. Define the socket transport

Create one small module in the Phoenix application:

```elixir
defmodule MyAppWeb.PulseSocket do
  use Spectre.Pulse.Phoenix, connection: :studio
end
```

The `:studio` value identifies a Pulse connection definition. The generated
module implements `Phoenix.Socket.Transport`; it is not a Phoenix Channel and
does not require topics or a Channel process per Agent.

## 2. Mount it on the Endpoint

```elixir
defmodule MyAppWeb.Endpoint do
  use Phoenix.Endpoint, otp_app: :my_app

  socket "/pulse", MyAppWeb.PulseSocket,
    websocket: [
      connect_info: [:peer_data, :uri],
      check_origin: true
    ],
    longpoll: false

  # The rest of the existing Endpoint configuration follows.
end
```

Phoenix exposes the transport at:

```text
ws://host/pulse/websocket
wss://host/pulse/websocket
```

The `/websocket` suffix is added by Phoenix. `check_origin` and the allowed
origin configuration belong to Phoenix and should remain enabled for browser
clients in production.

## 3. Configure the connection definition

Start Pulse before the Endpoint in the supervision tree:

```elixir
def start(_type, _args) do
  children = [
    {Phoenix.PubSub, name: MyApp.PubSub},
    {Spectre.Pulse,
     connections: [
       [
         id: :studio,
         transport: :websocket,
         mode: :listen,
         agents: :all,
         authenticate: &MyApp.PulseAccess.authenticate/2,
         authorize: &MyApp.PulseAccess.authorize/2,
         scopes: [
           "agent.runtime.read",
           "agent.runtime.stream"
         ],
         profiles: ["pulse.messaging/1", "spectre.studio/1"]
       ]
     ]},
    MyAppWeb.Endpoint
  ]

  Supervisor.start_link(children,
    strategy: :one_for_one,
    name: MyApp.Supervisor
  )
end
```

`agents: :all` exposes every Pulse-enabled Agent discovered by the runtime.
Replace it with an explicit allow-list when a connection must see only part of
the application:

```elixir
agents: [MyApp.Researcher, "spectre://acme/coordinator"]
```

The same Agent may appear in several connection definitions. One WebSocket can
carry several Agents and does not create an independent listener for each one.

## 4. Authenticate and authorize

The authenticator receives Phoenix's complete transport information as an
opaque credential value plus a reduced technical context. Validate the
credential and return a Pulse principal:

```elixir
defmodule MyApp.PulseAccess do
  alias Spectre.Pulse.Principal

  @allowed_scopes [
    "agent.runtime.read",
    "agent.runtime.stream"
  ]

  @spec authenticate(map(), map()) ::
          {:ok, map()} | {:error, :invalid_token}
  def authenticate(transport_info, _context) do
    token = get_in(transport_info, [:params, "token"])

    case MyApp.Accounts.verify_short_lived_socket_token(token) do
      {:ok, operator} ->
        {:ok,
         %{
           id: operator.id,
           identity: "spectre://studio/#{operator.id}",
           kind: :studio,
           scopes: @allowed_scopes,
           verified: %{issuer: "my_app"}
         }}

      :error ->
        {:error, :invalid_token}
    end
  end

  @spec authorize(Principal.t(), map()) :: {:ok, keyword()}
  def authorize(principal, request) do
    permitted =
      request.connection_spec.scopes
      |> Enum.filter(&(&1 in principal.scopes))
      |> MyApp.PulsePolicy.scopes_for(principal)

    {:ok, granted_scopes: permitted}
  end
end
```

The example uses a query parameter because it is easy to show, but production
browser clients should use a short-lived, purpose-bound socket token or a signed
session made available through Phoenix `connect_info`. Never place a long-lived
API secret in a WebSocket URL.

Authentication proves who owns the physical connection. Authorization decides
what that principal may do. A scope must exist in all three places before it is
granted:

1. the connection definition allows it;
2. the authenticated principal owns it;
3. the authorizer returns it, or accepts the default intersection.

Pulse discards the credential after the handshake. Live connection state,
manifests, errors, and catalogs contain the principal and grants but never the
token.

## 5. Receive the manifest

The first server frame is a connection manifest:

```json
{
  "pulse": "connection",
  "version": 1,
  "type": "manifest",
  "connection": {
    "id": "019...",
    "spec_id": "studio",
    "transport": "websocket",
    "granted_scopes": [
      "agent.runtime.read",
      "agent.runtime.stream"
    ]
  },
  "spec": {
    "id": "studio",
    "agent_addresses": ["spectre://acme/researcher"]
  },
  "agents": [
    {
      "address": "spectre://acme/researcher",
      "capabilities": ["research"]
    }
  ]
}
```

Studio should build its navigation and available actions from the manifest and
granted scopes. Capability advertisements describe an Agent; they do not grant
permission.

## 6. Send Agent messages

After the manifest the client can send the normal JSON Pulse envelope:

```json
{
  "version": 1,
  "id": "019f...",
  "from": "spectre://studio/operator-42",
  "to": "spectre://acme/researcher",
  "act": "request",
  "relates_to": null,
  "payload": {
    "type": "research.perform",
    "data": {"topic": "nautical market"}
  },
  "metadata": {}
}
```

Pulse binds `from` to the authenticated connection identity, validates the
envelope, and permits only recipients exposed by this connection. A successful
technical handoff produces a receipt frame:

```json
{
  "pulse": "connection",
  "version": 1,
  "type": "receipt",
  "receipt": {
    "message_id": "019f...",
    "status": "accepted"
  }
}
```

The receipt is not the Agent's semantic answer. That answer is a later Pulse
envelope correlated with `relates_to`.

## Studio control calls

Connection control calls use the same socket but are not Agent envelopes. The
current control surface includes runtime monitoring:

- `agent.runtime.monitor.enable`;
- `agent.runtime.monitor.disable`.

See [Studio monitoring](STUDIO_MONITORING.md) for their schemas and lifecycle.

## WSS and TLS

Pulse requires no SSL-specific option. If the Phoenix Endpoint serves HTTPS,
the same mount serves WSS:

```elixir
config :my_app, MyAppWeb.Endpoint,
  https: [
    ip: {0, 0, 0, 0},
    port: 443,
    cipher_suite: :strong,
    keyfile: System.fetch_env!("PULSE_TLS_KEYFILE"),
    certfile: System.fetch_env!("PULSE_TLS_CERTFILE")
  ]
```

TLS may also terminate at a reverse proxy. In that case preserve Phoenix's
origin checks and forward the client/transport facts needed by the host's
authentication policy. Pulse records only facts the adapter places in the
verified connection context; sender-declared envelope metadata never becomes a
verified TLS fact.

## Connection lifecycle

The Phoenix socket process owns the Pulse connection:

- opening the socket authenticates and registers one live connection;
- inbound and outbound activity updates its observation time;
- socket termination removes the connection;
- every runtime-monitor subscription owned by the socket is removed;
- no credential is retained after the handshake.

The host can inspect live state through:

```elixir
Spectre.Pulse.connection_specs()
Spectre.Pulse.connections()
Spectre.Pulse.local_agents()
Spectre.Pulse.exposed_agents(:studio)
Spectre.Pulse.monitoring_subscriptions(connection_id)
```

## Multiple connection classes

Use distinct connection definitions when audiences need different Agent sets or
permissions:

```elixir
connections: [
  [
    id: :studio_observer,
    transport: :websocket,
    agents: :all,
    scopes: ["agent.runtime.read", "agent.runtime.stream"]
  ],
  [
    id: :agent_mesh,
    transport: :websocket,
    agents: [MyApp.Researcher],
    scopes: []
  ]
]
```

Create one Phoenix transport module and mount for each class. Authentication and
authorization callbacks can still share application code.

## Failure behavior

- Missing authenticator: `connection_authenticator_required`.
- Invalid credentials: the WebSocket upgrade is rejected.
- Scope outside the spec/principal intersection: the connection is not opened.
- Recipient outside the Agent allow-list: the frame is rejected before Agent
  execution.
- Unsupported connection-control version: a safe protocol error is returned.
- Socket death: connection and monitoring state are cleaned automatically.

For delivery outcomes and retry rules see [delivery and security](DELIVERY_AND_SECURITY.md).

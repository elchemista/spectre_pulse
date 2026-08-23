# Spectre Pulse

Spectre Pulse connects autonomous [Spectre](https://github.com/elchemista/spectre)
Agents through one transport-independent protocol. The same Agent can communicate
locally, through Phoenix WebSocket, REST, PubSub, distributed Erlang, or a custom
adapter without changing its message semantics.

Pulse also provides the connection and control boundary used by Spectre Studio:
one authenticated Phoenix WebSocket can expose several Agents, advertise what is
available, carry Agent-to-Agent messages, and stream temporary OTP and Work/Vigil
monitoring when Studio asks for it.

```text
Spectre Studio / remote Agent
              │
              │ authenticated WebSocket (WSS)
              ▼
      Pulse connection manifest
        ├── exposed Agents
        ├── granted scopes
        ├── Agent messages
        ├── OTP runtime subscriptions
        ├── Work and Vigil subscriptions
        └── scoped Studio inspection
              ├── semantic-cache review / promotion
              ├── persisted turn journal
              ├── Skill inventory
              └── governed Morph proposals
              │
              ▼
    Spectre Agent Instances, operational loops, and OTP processes
```

## What Pulse owns

Pulse owns the common technical boundary:

- canonical Agent addresses and immutable versioned envelopes;
- authenticated connection manifests and per-connection Agent exposure;
- discovery and delivery through replaceable transports;
- safe inbound validation and technical receipts;
- scoped, temporary OTP and Work/Vigil monitoring for Studio;
- bounded semantic-cache, turn-journal, Skill and Morph Studio operations.

Pulse does not own Agent reasoning, memory, ledgers, semantic cache, tasks,
workflows, journals, or application authorization policy. Those remain in
Spectre and its extensions. Pulse exposes only bounded scoped projections and
transports their results.

## Installation

Pulse targets Spectre `0.3.3` and Elixir 1.19 or later:

```elixir
def deps do
  [
    {:spectre, "~> 0.3.3"},
    {:spectre_pulse, github: "elchemista/spectre_pulse", branch: "main"}
  ]
end
```

## Phoenix WebSocket

Phoenix is the shortest path to connect Spectre Studio or remote Agents. Pulse
uses the Phoenix application's existing Endpoint and WebSocket server; it does
not add Phoenix as a dependency.

Define a transport module:

```elixir
defmodule MyAppWeb.PulseSocket do
  use Spectre.Pulse.Phoenix, connection: :studio
end
```

Mount it on the Endpoint:

```elixir
socket "/pulse", MyAppWeb.PulseSocket,
  websocket: [connect_info: [:peer_data, :uri]],
  longpoll: false
```

Start Pulse with one authenticated connection definition:

```elixir
children = [
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
         "agent.runtime.stream",
         "agent.operations.read",
         "agent.operations.stream"
       ] ++ Spectre.Pulse.Studio.scopes()
     ]
   ]},
  MyAppWeb.Endpoint
]
```

Phoenix serves the connection at `/pulse/websocket`. Under an HTTPS Endpoint it
is automatically available through `wss://`; certificates and TLS termination
remain normal Phoenix/Bandit infrastructure.

The generated transport sends a bounded 25-second WebSocket heartbeat, keeping
the socket below Phoenix's default inbound-idle timeout. The interval is
configurable on `use Spectre.Pulse.Phoenix` for stricter proxy infrastructure.

After authentication Pulse immediately sends a credential-free manifest with
the connection, granted scopes, and exposed Agents. The same socket then accepts
ordinary Pulse envelopes and Studio control calls. See the complete
[Phoenix integration guide](docs/PHOENIX.md), including authentication, WSS,
multiple Agent sets, and lifecycle behavior.

Semantic cache, Journal/turns, Skills and Morph require explicit grants from
both the principal and `authorize/2`. See
[Studio inspection and governance](docs/STUDIO_INSPECTION.md) for the operation
contract and its security boundaries.

## Define an Agent

An Agent declares a logical identity and the Pulse payload types accepted by its
normal Spectre flows:

```elixir
defmodule MyApp.Researcher do
  use Spectre.Agent
  use Spectre.Pulse

  pulsing do
    identity("spectre://acme/researcher")
    advertise(capabilities: [:research])
  end

  flow :remote_requests do
    on :research, pulse: "research.perform" do
      run(:research)
    end
  end

  def research(input, _context) do
    "accepted: #{input.text}"
  end
end
```

The sender knows only a logical contact. It does not know whether the recipient
is local or reached through WebSocket, REST, gRPC, or another transport:

```elixir
pulsing do
  identity("spectre://acme/coordinator")
  contact(:researcher, "spectre://acme/researcher")
end

flow :delegation do
  on :delegate, regex: ~r/^research:/ do
    pulse(:researcher,
      act: :request,
      type: "research.perform",
      build: :research_request
    )
  end
end
```

`pulse/2` stages a normal Spectre Effect. The explicit side-effect boundary sends
it:

```elixir
{:ok, turn} = Spectre.turn(MyApp.Coordinator, "research:nautical market")
{:ok, result} = Spectre.execute(MyApp.Coordinator, turn.result)
```

For a complete local example run:

```console
mix run examples/local_agents.exs
```

The [getting started guide](docs/GETTING_STARTED.md) explains Stack installation,
contacts, inbound flows, Subject-scoped Instances, correlation, and execution.

## Studio monitoring is runtime-controlled

Realtime monitoring is not enabled in application configuration. Configuration
only defines which scopes a connection may grant. A Studio LiveView opens a
temporary subscription when the operator starts watching an Agent:

```json
{
  "pulse": "connection",
  "version": 1,
  "type": "agent.runtime.monitor.enable",
  "request_id": "liveview-panel-42",
  "agent_address": "spectre://acme/researcher",
  "subject": "account-123",
  "interval_ms": 1000,
  "duration_ms": 60000,
  "fields": ["memory", "message_queue_len", "reductions", "status"]
}
```

Pulse returns snapshot sequence `0`, pushes subsequent updates on the same
WebSocket, and stops when Studio sends `agent.runtime.monitor.disable`, the
duration expires, or the socket disconnects. Remote monitoring requires both
`agent.runtime.read` and `agent.runtime.stream`.

Work and Vigil monitoring uses the same runtime-controlled lifecycle with the
separate `agent.operations.read` and `agent.operations.stream` scopes:

```json
{
  "pulse": "connection",
  "version": 1,
  "type": "agent.operations.monitor.enable",
  "agent_address": "spectre://acme/researcher",
  "subject": "account-123",
  "interval_ms": 1000,
  "kinds": ["work", "vigil"]
}
```

See [OTP runtime monitoring](docs/STUDIO_MONITORING.md) and
[Work and Vigil monitoring](docs/STUDIO_OPERATIONS.md) for frames, permissions,
limits, events, and LiveView lifecycle recommendations.

## Connections and transports

One connection can expose all discovered Agents or an explicit subset. Several
connections can expose overlapping Agent sets with different transports,
principals, and scopes:

```elixir
connections: [
  [id: :studio, transport: :websocket, agents: :all],
  [id: :private, transport: :grpc, agents: [MyApp.Researcher], scopes: []]
]
```

Pulse provides Local, WebSocket, REST, PubSub, and BEAM-node bindings. A custom
transport implements one behavior and receives the same validated envelope;
discovery, correlation, permissions, receipts, and Agent logic remain common.

See [connections and transports](docs/CONNECTIONS_AND_TRANSPORTS.md) for
connection specs, discovery, route priorities, built-in adapters, REST inbound,
and custom transport implementation.

## Protocol and delivery guarantees

Pulse v1 uses one sender, one recipient, a UUIDv7 message id, one communicative
act (`inform`, `query`, or `request`), a namespaced payload type, and optional
causal correlation through `relates_to`.

A receipt means that the selected transport accepted the envelope. It does not
mean the remote Agent completed the semantic work. Delivery can be duplicated or
arrive out of order. Pulse retries another route only after a failure known to be
`not_sent`; it stops on `outcome_unknown` to avoid silently duplicating work.

Read [delivery and security](docs/DELIVERY_AND_SECURITY.md) before exposing an
Agent across a network. The JSON envelope schema is
[`priv/schema/pulse-envelope-v1.schema.json`](https://github.com/elchemista/spectre_pulse/blob/main/priv/schema/pulse-envelope-v1.schema.json).

## Documentation

| Guide | Use it for |
| --- | --- |
| [Getting started](docs/GETTING_STARTED.md) | Agent authoring, Stack setup, local messaging, Instances, contacts |
| [Phoenix integration](docs/PHOENIX.md) | Endpoint setup, authentication, WSS, manifests, socket lifecycle |
| [OTP runtime monitoring](docs/STUDIO_MONITORING.md) | Process snapshots, enable/disable calls, safe fields, limits |
| [Work and Vigil monitoring](docs/STUDIO_OPERATIONS.md) | Active loops, publication-safe views, realtime Studio subscriptions |
| [Connections and transports](docs/CONNECTIONS_AND_TRANSPORTS.md) | Multi-Agent connections, discovery, REST, PubSub, BEAM, custom adapters |
| [Delivery and security](docs/DELIVERY_AND_SECURITY.md) | Trust boundaries, receipts, retries, correlation, common failures |
| [Architecture](docs/ARCHITECTURE.md) | Ownership boundaries and internal design |
| [Public API](docs/PUBLIC_API.md) | Normative supported modules and callables |

The transport-independent protocol description is also available at runtime:

```elixir
Spectre.Pulse.protocol()
```

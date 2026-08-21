# Spectre Pulse architecture

Pulse keeps the network technical and the Agents autonomous:

> Pulse does not coordinate Agents; it enables Agents to coordinate.

This document describes ownership and internal layers. Use the
[getting started guide](GETTING_STARTED.md) for Agent code, the
[Phoenix guide](PHOENIX.md) for WebSocket setup, and
[connections and transports](CONNECTIONS_AND_TRANSPORTS.md) for adapter code.

## Ownership boundaries

| Concern | Owner |
| --- | --- |
| Planning and decisions | Spectre Agent |
| Authoritative state, memory, and journal | Spectre and host adapters |
| Ledger, Lab, semantic cache, and skills | Their Spectre extensions |
| Expectations and semantic retries | Individual Agent state and host policy |
| Logical identity and envelope grammar | Pulse protocol |
| Correlation and technical validation | Pulse protocol |
| Connection principal, grants, and exposed Agents | Pulse connection boundary |
| Physical route discovery and ordering | Pulse Discovery, Fabric, and Network |
| HTTP, WebSocket, PubSub, BEAM, custom delivery | Transport binding |
| Temporary OTP and Work/Vigil subscriptions | Pulse Monitoring |
| Authentication credential validation | Host transport callback |

Pulse has no Room, shared Task, Coordinator, Workflow, Store, Journal, semantic
queue, or process per message.

## Dependency direction

```text
spectre_pulse ──► spectre
spectre       ──X spectre_pulse
```

Spectre core does not know about Pulse addresses, REST, WebSocket, or connection
manifests. Pulse maps its protocol into ordinary Spectre Input, Turn, Effect,
Instance, and State boundaries.

## Layers

```text
┌──────────────────────────────────────────────────────────────┐
│ Agent semantics                                              │
│ Spectre State · memory · journal · policy · expectations     │
└───────────────────────────┬──────────────────────────────────┘
                            │ %Spectre.Effect{kind: :pulse}
┌───────────────────────────▼──────────────────────────────────┐
│ Pulse protocol                                               │
│ Address · Envelope · Payload · Validator · correlation       │
└───────────────────────────┬──────────────────────────────────┘
                            │ logical destination
┌───────────────────────────▼──────────────────────────────────┐
│ Discovery and delivery                                      │
│ Local · Fabric · Directory · Route · Network · Reachability  │
└───────────────────────────┬──────────────────────────────────┘
                            │ one selected physical route
┌───────────────────────────▼──────────────────────────────────┐
│ Replaceable transport                                       │
│ WebSocket · REST · PubSub · BEAM node · custom               │
└───────────────────────────┬──────────────────────────────────┘
                            │ authenticated inbound facts
┌───────────────────────────▼──────────────────────────────────┐
│ Inbound bridge                                               │
│ validate · authorize · Spectre.Input · Spectre.turn/3        │
└──────────────────────────────────────────────────────────────┘
```

The Phoenix adapter adds a connection-control lane beside Agent envelopes:

```text
Phoenix socket
  ├── Agent envelope → WebSocket binding → Pulse inbound bridge
  └── control frame  → Monitoring session
                         ├── OTP runtime snapshots
                         └── Spectre Work/Vigil views
```

The two lanes share the authenticated connection and exposed-Agent boundary but
do not confuse connection commands with Agent payloads.

## Outbound lifecycle

`pulse/2` is declarative. It stages data:

```elixir
%Spectre.Effect{
  kind: :pulse,
  name: :send,
  id: message_id,
  idempotency_key: "pulse:" <> message_id,
  payload: %{
    to: "spectre://acme/researcher",
    act: :request,
    type: "research.perform",
    data: data,
    relates_to: nil,
    metadata: %{}
  }
}
```

No transport call happens during routing:

```text
Spectre turn
  → stage Pulse Effect
  → host persists staged state
  → Spectre execution boundary invokes Pulse delivery
  → transport returns Receipt or typed Error
  → host persists terminal state
```

Pulse does not add a parallel persistence lifecycle. The Effect result describes
technical delivery, not semantic completion by the remote Agent.

## Inbound lifecycle

Every inbound binding eventually calls `Spectre.Pulse.receive/3` with an
`InboundContext`. The binding proves connection identity; envelope metadata does
not contribute trust.

The bridge:

1. decodes and validates the envelope;
2. binds `Envelope.from` to the authenticated identity;
3. resolves `Envelope.to` within the connection boundary;
4. applies application authorization;
5. maps payload and trusted facts to `Spectre.Input`;
6. invokes the ordinary Spectre flow;
7. returns a technical receipt.

The `pulse_type` Input projection lets the Pulse DSL compile
`pulse: "research.perform"` into a normal deterministic Spectre rule. Pulse is
not a global `Spectre.Turn.Handler`.

For Subject-scoped work, the trusted host supplies a Subject and Instance
supervisor. The core Instance owns its Runs, state, mailbox fairness, and
inference lifecycle.

## Discovery and routing components

| Component | Responsibility |
| --- | --- |
| `Runtime` | Discovers Pulse-enabled modules and starts local subscriptions |
| `ContactBook` | Resolves Agent-owned logical contact names |
| `Directory` | Resolves application service-like identities and optional routes |
| `Local` | Maps local Agent addresses to mailbox endpoints |
| `Fabric` | Registers transport drivers and live remote routes |
| `Discovery` | Merges current route sources |
| `Network` | Orders routes and applies safe failover |
| `Transport` | Performs one physical handoff |
| `Reachability` | Reports one timestamped technical observation |

An Agent contains only a logical contact or address:

```text
Agent pulse(:researcher)
  → ContactBook/Directory resolves canonical address
  → Discovery gathers Local/Fabric/Directory routes
  → Network selects one transport
  → transport delivers the unchanged Envelope
```

Fabric removes PID-owned routes when their owner exits. Non-process resources
are disconnected explicitly by the host.

## Connection model

A `ConnectionSpec` is host configuration for one connection class. It defines:

- transport and mode;
- Agent allow-list;
- maximum profiles and scopes;
- authentication and authorization callbacks;
- public technical metadata.

A live `Connection` is ephemeral. It contains the authenticated principal,
actual grants, exposed Agent addresses, remote advertisements, verified facts,
and owner process. It never contains the credential used to authenticate.

One spec can produce many live connections. One connection can expose many
Agents. Agent sets may overlap across specs.

## Monitoring model

Monitoring is dynamic connection state, not static Agent configuration:

1. an authenticated adapter starts one monitoring session for its connection;
2. Studio enables a subscription for an exposed Agent and Subject;
3. Pulse verifies the matching runtime or operations stream scope and performs
   the corresponding scoped read;
4. the session serially samples and pushes bounded events;
5. disable, expiry, or adapter death removes the subscription;
6. a central safe catalog records who is monitoring what.

Monitoring never reads raw Agent state. OTP monitoring samples a fixed safe
`Process.info/2` vocabulary. Operations monitoring reads only Spectre's
privacy-safe `Operation.View` projections for Work and Vigil. Domain
observability such as Ledger tokens remains owned by the corresponding Spectre
extension.

## Failure model

The route stack uses three terminal decisions:

```text
accepted        → stop and return Receipt
not_sent        → safely try the next route
outcome_unknown → stop; host or Agent decides whether to reconcile/retry
```

This prevents an ambiguous timeout on one transport from automatically
duplicating a non-idempotent operation on another.

See [delivery and security](DELIVERY_AND_SECURITY.md) for the external contract
and [public API](PUBLIC_API.md) for the supported surface.

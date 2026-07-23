# Spectre Pulse

Spectre Pulse is a transport-independent protocol for communication between
[Spectre](https://github.com/elchemista/spectre) agents.

Pulse does not coordinate agents. It gives autonomous agents a common,
versioned envelope and a replaceable delivery boundary so they can coordinate
themselves:

```text
Agent A state
    │ stages %Spectre.Effect{kind: :pulse}
    ▼
immutable Pulse Envelope
    │
    ├── local mailbox / PubSub
    ├── WebSocket / REST
    ├── distributed Erlang
    └── any custom Transport
    │
    ▼
Pulse inbound validation
    │ preserves message id and authenticated identity
    ▼
Spectre.turn/3 for Agent B
```

The envelope keeps the same meaning when the transport changes. Pulse owns no
room, shared task, workflow, session, store, journal, retry process, or global
presence truth.

## Protocol v1

Every transport carries the same semantic value:

```elixir
%Spectre.Pulse.Envelope{
  version: 1,
  id: "019f...",
  from: "spectre://acme/anna",
  to: "spectre://acme/tao",
  act: :request,
  relates_to: nil,
  payload: %Spectre.Pulse.Payload{
    type: "research.perform",
    data: %{"topic" => "Italian nautical market"}
  },
  metadata: %{}
}
```

Pulse v1 has only three communicative acts:

- `:inform` shares a fact, result, or update;
- `:query` asks a question;
- `:request` asks the recipient to do something.

A response is a new envelope whose `relates_to` points at an earlier message.
Messages may be duplicated or arrive out of order. Pulse does not promise
exactly-once delivery and never interprets application payload data.

The JSON representation is specified by
[`priv/schema/pulse-envelope-v1.schema.json`](priv/schema/pulse-envelope-v1.schema.json).

## Installation

Until the first Hex release, install directly from GitHub:

```elixir
def deps do
  [
    {:spectre,
     github: "elchemista/spectre",
     ref: "38aae368aca51225e0d2e8d68b8ce10465f55ca5"},
    {:spectre_pulse, github: "elchemista/spectre_pulse"}
  ]
end
```

For sibling-repository development:

```elixir
def deps do
  [
    {:spectre, path: "../spectre"},
    {:spectre_pulse, path: "../spectre_pulse"}
  ]
end
```

Pulse requires Elixir 1.19 or later.

## Define a Pulse-enabled Agent

`use Spectre.Pulse` must follow `use Spectre.Agent`:

```elixir
defmodule MyApp.Anna do
  use Spectre.Agent
  use Spectre.Pulse

  pulsing do
    identity "spectre://acme/anna"
    state_scope :agent

    contact :tao, "spectre://acme/tao",
      display_name: "Tao",
      capabilities: [:research]

    advertise capabilities: [:planning, :coordination]
  end

  flow :delegation do
    on :delegate, regex: ~r/^research:/ do
      pulse :tao,
        act: :request,
        type: "research.perform",
        build: :build_research_request,
        expect: "research.completed"
    end
  end

  def build_research_request(input, ctx) do
    %{
      "topic" => String.replace_prefix(input.text, "research:", ""),
      "project_id" => ctx.state.data[:project_id]
    }
  end
end
```

No PID, URL, socket, transport, or `Route` belongs in the Agent. Anna knows
only Tao's logical address and declared capabilities.

`identity/1` is optional. When omitted, Pulse assigns the module a stable
128-bit logical address:

```elixir
Spectre.Pulse.Address.for_agent(MyApp.Anna)
# => "spectre://pulse/4f...32-lowercase-hex-digits..."
```

The same module receives the same address across process and node restarts.
Different modules receive different addresses. An explicit `identity/1`
remains useful for a public, human-readable address or when identity must
survive a module rename. The generated identifier is an address, not a secret
or authentication credential.

## Start Pulse and connect infrastructure

Add Pulse to the host application's supervision tree. There is no parallel
list of Agents: the runtime discovers every module using `Spectre.Pulse` and
subscribes its compiled identity automatically.

```elixir
def start(_type, _args) do
  children = [
    {Spectre.Pulse,
     transports: [
       {:grpc, MyApp.GRPCPulse, priority: 35}
     ]}
  ]

  Supervisor.start_link(children, strategy: :one_for_one)
end
```

`transports` contains only application-defined drivers. Each entry is
`{name, module}` or `{name, module, options}`; Pulse validates `deliver/3` and
registers it in the shared Fabric. Driver options are `priority`, `metadata`,
and the explicit `replace` flag. Local, WebSocket, BEAM node, PubSub, and REST
are already registered, so `{Spectre.Pulse, []}` is sufficient when no custom
binding is needed.

Pulse then discovers delivery paths in the same way a network stack resolves
a logical destination:

1. the ContactBook or a `Directory` resolves `:tao` to
   `spectre://acme/tao`;
2. a local subscription, the runtime `Fabric`, and application directories
   contribute current physical routes;
3. `Network.Routed` orders them and selects the best route;
4. a known `:not_sent` result safely falls through to the next route.

Connections belong to the host application. Register them when they become
available:

```elixir
{:ok, websocket_route} =
  Spectre.Pulse.connect(
    "spectre://acme/tao",
    :websocket,
    connection_pid
  )

{:ok, rest_route} =
  Spectre.Pulse.connect(
    "spectre://acme/tao",
    :rest,
    "https://agents.example/spectre-pulse/v1/messages"
  )
```

The local mailbox is preferred when Tao is on this node, then an active
WebSocket, then other configured bindings such as REST. A connection whose
target/owner PID exits is removed automatically. For other connection types,
the host removes the route explicitly. Connection options are `id`, `priority`,
`metadata`, and `owner`:

```elixir
:ok = Spectre.Pulse.disconnect(rest_route.id)
```

The custom transport declared in the supervision tree joins the exact same
discovery path:

```elixir
{:ok, _route} =
  Spectre.Pulse.connect(
    "spectre://acme/tao",
    :grpc,
    channel,
    owner: channel_process
  )
```

The Agent still calls only `pulse(:tao, ...)`; switching between Local,
WebSocket, REST, or gRPC requires no Agent change.

`pulse/2` does not send while Spectre is routing. It stages a normal
`%Spectre.Effect{kind: :pulse, name: :send}`:

```elixir
{:ok, turn} = Spectre.turn(MyApp.Anna, "research:nautical market")
{:needs, %Spectre.Effect{kind: :pulse} = effect, _result} = turn.decision

# This is the explicit side-effect boundary.
{:ok, executed_turn} = Spectre.Pulse.execute_turn(turn)
{:completed, completed_effect, result} = executed_turn.decision
```

The host must persist the staged state before delivery and persist the returned
terminal state afterwards, using the same durability strategy it uses for
other Spectre effects. Pulse itself has no store.

`expect:` adds only a pure `%Spectre.Pulse.Expectation{}` to the sender's
`Spectre.State`. It does not create a remote task or timer:

```elixir
case Spectre.Pulse.correlate(result.state, incoming_envelope) do
  {:ok, new_state, resolved_expectation} ->
    persist(new_state)

  :unmatched ->
    :ok
end
```

## Receive an envelope

An inbound binding authenticates its connection and supplies the resulting
facts. `Envelope.to` is resolved through the local subscriptions, so the
binding does not have to select an Agent:

```elixir
{:ok, inbound} =
  Spectre.Pulse.receive(envelope, %{
    authenticated_identity: "spectre://acme/tao",
    binding: :websocket,
    peer: peer,
    verified: %{tls: true}
  })

%Spectre.Turn{} = inbound.turn
```

Pulse:

1. validates version, UUIDv7, addresses, act, payload type, and limits;
2. compares `Envelope.from` with the transport-authenticated identity;
3. checks the recipient and application authorization callback;
4. creates a `Spectre.Input` with trusted facts under `input.meta.pulse`;
5. calls `Spectre.turn/3` with `turn_id: envelope.id`;
6. returns the normal Spectre turn and a technical receipt.

Route an inbound payload deterministically with `pulse:`:

```elixir
flow :remote_requests do
  on :research_request, pulse: "research.perform" do
    run :handle_research_request
  end
end
```

This compiles to an ordinary Spectre metadata check. There is no parallel Pulse
router and no global `Spectre.Turn.Handler`.

## Transports

Routes pair a logical address with one physical binding, but they are
infrastructure values discovered by Pulse rather than Agent configuration:

| Binding | Infrastructure target | Inbound entry point |
| --- | --- | --- |
| Local | subscribed process mailbox | automatic |
| PubSub | publisher function or adapter/server/topic map | `PubSub.handle_message/2` |
| WebSocket | connection PID, sender function, or module/connection | `WebSocket.handle_frame/3` |
| REST | HTTP URL | `REST.handle_request/4` |
| BEAM node | node and optional endpoint | automatic through `:erpc` |

The built-in Fabric priorities are Local `0`, WebSocket `20`, BEAM node `30`,
PubSub `40`, and REST `50`. The host can override a priority when connecting a
route. Agents never inspect this ordering.

The default stateless network sorts routes by priority. It tries another route
only when the prior adapter returns `outcome: :not_sent`. It stops on
`:outcome_unknown`, because immediate failover could duplicate an operation.

### Framework-neutral REST endpoint

The host reads the request body and headers, then adapts the returned response
to Plug/Phoenix or another HTTP server:

```elixir
response =
  Spectre.Pulse.Transports.REST.handle_request(
    body,
    headers,
    remote_ip,
    authenticator: fn headers, peer ->
      MyApp.PulseAuth.authenticate(headers, peer)
      # => {:ok, "spectre://acme/anna", %{mTLS: true}}
    end,
    authorize: &MyApp.PulsePolicy.authorize/3
  )

# response.status == 202 after technical acceptance
```

The agent's semantic response is always a later correlated envelope. It is
never the synchronous HTTP response.

## Contacts, directory, and reachability

Static contacts, contacts stored in `Spectre.State`, and an application-owned
directory can be composed without a global semantic Agent registry:

```elixir
Spectre.Pulse.contacts({MyApp.Anna, state})
Spectre.Pulse.resolve({MyApp.Anna, state}, :tao)
Spectre.Pulse.find_contacts({MyApp.Anna, state}, capability: :research)
Spectre.Pulse.remember_contact(state, contact)
Spectre.Pulse.forget_contact(state, :tao)
Spectre.Pulse.reachability({MyApp.Anna, state}, :tao)
```

Reachability is `:reachable`, `:unreachable`, or `:unknown` and always carries
an observation time. It does not mean that an agent is available or has
accepted work.

## Security boundary

- The transport authenticates the connection.
- Pulse validates the envelope and binds its declared sender to that identity.
- Spectre or the host authorizes the requested capability and data access.

Sender-declared metadata is exposed separately as
`input.meta.pulse.declared_metadata`; it is never merged into the transport's
`verified` facts. Remote controlled values are decoded through fixed
vocabularies, so JSON decoding never creates atoms.

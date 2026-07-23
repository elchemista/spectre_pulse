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

The important boundary is simple:

- an Agent names only a logical recipient such as `:tao` or
  `spectre://acme/tao`;
- the Pulse runtime discovers Pulse-enabled Agents automatically;
- the host application registers physical connections and optional transport
  drivers;
- Pulse resolves and orders routes, exposes probes, and performs safe failover;
- the same immutable envelope reaches the same Spectre flow regardless of the
  selected transport.

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

## Quick start: two Agents, no routes in Agent code

Define the receiving Agent. It declares its logical identity and the Pulse
payload type that should enter its normal Spectre flow:

```elixir
defmodule MyApp.Tao do
  use Spectre.Agent
  use Spectre.Pulse

  pulsing do
    identity("spectre://acme/tao")
    advertise(capabilities: [:research])
  end

  flow :remote_requests do
    on :research, pulse: "research.perform" do
      run(:research)
    end
  end

  def research(input, _context) do
    "accepted #{input.meta.pulse.type}: #{input.text}"
  end
end
```

The sending Agent knows Tao only as a contact. It does not know whether Tao is
local, behind a WebSocket, reachable through REST, or connected through a
custom transport:

```elixir
defmodule MyApp.Anna do
  use Spectre.Agent
  use Spectre.Pulse

  pulsing do
    identity("spectre://acme/anna")

    contact(:tao, "spectre://acme/tao",
      display_name: "Tao",
      capabilities: [:research]
    )
  end

  flow :delegation do
    on :delegate, regex: ~r/^research:/ do
      pulse(:tao,
        act: :request,
        type: "research.perform",
        build: :research_request,
        expect: "research.completed"
      )
    end
  end

  def research_request(input, _context) do
    %{"topic" => String.replace_prefix(input.text, "research:", "")}
  end
end
```

Start one Pulse runtime in the host application's supervision tree. Do not add
an `agents: [...]` option: Pulse finds both compiled modules and subscribes
their identities itself.

```elixir
def start(_type, _args) do
  children = [
    {Spectre.Pulse, transports: []}
  ]

  Supervisor.start_link(children,
    strategy: :one_for_one,
    name: MyApp.Supervisor
  )
end
```

Run Anna and explicitly execute the staged side effect:

```elixir
{:ok, turn} = Spectre.turn(MyApp.Anna, "research:nautical market")
{:needs, effect, staged_result} = turn.decision

effect.payload.to
# => "spectre://acme/tao"

# Persist staged_result.state here when the application requires durability.
{:ok, executed_turn} = Spectre.Pulse.execute_turn(turn)
{:completed, completed_effect, final_result} = executed_turn.decision

completed_effect.result.via
# => :local

completed_effect.result.message_id == effect.id
# => true
```

Pulse discovers Tao's local subscription, creates the Local route internally,
puts the envelope in Tao's mailbox, and invokes Tao's normal Spectre flow.
Moving Tao behind another transport does not change either Agent.

The repository includes the same scenario as an executable example:

```console
mix run examples/local_agents.exs
```

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

### Connect built-in transports

Local delivery needs no explicit connection. Starting the runtime subscribes
each Pulse-enabled Agent in the unique local Registry and discovery creates the
route when it is needed.

The host registers remote connections as they appear. These examples all bind
the same logical Agent address:

```elixir
address = "spectre://acme/tao"

# A live connection process. Pulse removes the route when the process exits.
{:ok, websocket_route} =
  Spectre.Pulse.connect(address, :websocket, socket_pid)

# An HTTP endpoint. Headers and Req options remain infrastructure metadata.
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

# Any adapter exposing broadcast/3 or publish/3.
{:ok, pub_sub_route} =
  Spectre.Pulse.connect(
    address,
    :pub_sub,
    %{
      adapter: Phoenix.PubSub,
      server: MyApp.PubSub,
      topic: "pulse:tao"
    }
  )

# A distributed Erlang peer authenticated by the host's node configuration.
{:ok, node_route} =
  Spectre.Pulse.connect(
    address,
    :beam_node,
    %{node: :"tao@agents.internal", endpoint: MyApp.Tao}
  )
```

An explicit route `priority:` overrides the driver's default. An `owner:` PID
lets Fabric monitor a connection whose target does not itself contain that
PID. Routes may be removed idempotently:

```elixir
{:ok, route} =
  Spectre.Pulse.connect(
    address,
    :rest,
    endpoint_url,
    id: "tao:rest:primary",
    priority: 45,
    owner: connection_owner
  )

:ok = Spectre.Pulse.disconnect(route.id)
:ok = Spectre.Pulse.disconnect(route.id)
```

### Implement a custom transport

A transport is a small infrastructure adapter implementing
`Spectre.Pulse.Transport`. `deliver/3` receives the already-resolved physical
route and the common envelope:

```elixir
defmodule MyApp.PulseTransport do
  @behaviour Spectre.Pulse.Transport

  alias Spectre.Pulse.Envelope
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Receipt
  alias Spectre.Pulse.Route
  alias Spectre.Pulse.Transport

  @doc false
  @spec deliver(Route.t(), Envelope.t(), keyword()) ::
          {:ok, Receipt.t()} | {:error, Error.t()}
  @impl Transport
  def deliver(%Route{} = route, %Envelope{} = envelope, _opts) do
    case route.target.(envelope) do
      :ok ->
        {:ok,
         Receipt.accepted(envelope.id,
           via: :my_bus,
           route_id: route.id
         )}

      {:error, {:not_sent, reason}} ->
        {:error,
         Error.not_sent(:transport, reason,
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

Register the driver when Pulse starts, then connect an application-owned
publisher. Neither name appears in Agent code:

```elixir
children = [
  {Spectre.Pulse,
   transports: [
     {:my_bus, MyApp.PulseTransport, priority: 25}
   ]}
]

publisher = fn envelope ->
  MyApp.Bus.publish(bus_connection, {:spectre_pulse, envelope})
end

{:ok, _route} =
  Spectre.Pulse.connect(
    "spectre://acme/tao",
    :my_bus,
    publisher,
    # bus_connection is the owning process PID.
    owner: bus_connection
  )
```

`probe/2` is optional. If it is not implemented, reachability is reported as
`:unknown` instead of pretending the destination is online. A custom adapter
must return `:not_sent` only when it knows the envelope did not cross its
handoff boundary; every ambiguous failure must be `:outcome_unknown`.

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

REST fails closed when no authenticator is configured. For a deliberately
unauthenticated private endpoint, the host must opt in explicitly with
`allow_unauthenticated: true`; authorization and recipient validation still
run.

WebSocket and PubSub consumers use the same inbound bridge. The surrounding
adapter supplies connection-authenticated facts, while `Envelope.to` selects
the locally subscribed Agent:

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
      peer: "pulse:tao",
      verified: %{broker: :authenticated}
    }
  )
```

The Local and BEAM-node bindings build equivalent trusted inbound contexts
inside their respective VM/node trust boundaries.

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

An application directory may resolve service-like names without leaking
physical routes into the Agent:

```elixir
defmodule MyApp.PulseDirectory do
  @behaviour Spectre.Pulse.Directory

  alias Spectre.Pulse.Directory

  @doc false
  @spec resolve(term(), keyword()) ::
          {:ok, String.t()} | :error
  @impl Directory
  def resolve(:researcher, _opts),
    do: {:ok, "spectre://services/researcher"}

  def resolve(_reference, _opts), do: :error
end
```

Configure only the directory contract in an Agent:

```elixir
pulsing do
  directory(MyApp.PulseDirectory)
end
```

The runtime's Local Registry, connected Fabric routes, and optional
`routes/2` callback on the directory are merged at delivery time. Resolution
is therefore dynamic while the envelope remains addressed to the canonical
logical identity.

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

## Receipts, errors, and delivery guarantees

A `%Spectre.Pulse.Receipt{status: :accepted}` is a technical acknowledgement:
the selected binding accepted the envelope. It does not mean that the
recipient understood, authorized, accepted, or completed the semantic work.
Semantic completion is represented by a later envelope correlated through
`relates_to`.

Pulse separates an error's category from its delivery outcome:

| Outcome | Meaning | Automatic next route? |
| --- | --- | --- |
| `:not_sent` | The adapter knows the envelope did not cross its handoff boundary | Yes |
| `:outcome_unknown` | Delivery may already have happened | No |

For example, connection refusal before an HTTP request is written is
`:not_sent`; a timeout after writing the request is
`:outcome_unknown`. Stopping on ambiguity prevents transparent failover from
silently duplicating a non-idempotent request.

```elixir
case Spectre.Pulse.execute_turn(turn) do
  {:ok, executed_turn} ->
    persist_terminal_state(executed_turn)

  {:error, %Spectre.Pulse.Error{outcome: :not_sent} = error} ->
    schedule_retry(error.message_id)

  {:error, %Spectre.Pulse.Error{outcome: :outcome_unknown} = error} ->
    reconcile_before_retry(error.message_id)
end
```

Delivery is at-least-possibly-once. Keep handlers idempotent by message ID when
their domain operation requires it. Pulse preserves the UUIDv7 message ID
across transports and uses it as the Spectre turn ID on inbound delivery.

## Common failures

- `{:unknown_contact, reference}` means neither the Agent's contacts nor its
  directory resolved the logical name.
- `{:no_route, address}` means identity resolution succeeded, but there is
  currently no local subscription, connected Fabric route, or directory
  route.
- `{:duplicate_pulse_identity, identity, agents}` stops runtime startup so two
  modules cannot silently own the same logical address.
- `:rest_authenticator_required` is the safe REST default; configure an
  authenticator or explicitly opt into an unauthenticated endpoint.
- `:sender_identity_mismatch` means `Envelope.from` disagrees with the
  transport-authenticated peer.
- `:recipient_identity_mismatch` means the selected endpoint does not own
  `Envelope.to`.

Use `Spectre.Pulse.reachability/3` to inspect current technical reachability,
but do not treat it as a promise that an Agent is available or will accept the
request.


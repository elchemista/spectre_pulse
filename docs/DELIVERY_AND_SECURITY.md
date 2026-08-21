# Delivery semantics and security

Pulse separates message meaning, technical delivery, authenticated connection
facts, and Agent policy. Read this guide before exposing an Agent over a network.

## Envelope invariants

Every transport carries the same Pulse v1 envelope:

```elixir
%Spectre.Pulse.Envelope{
  version: 1,
  id: "019f...",
  from: "spectre://acme/coordinator",
  to: "spectre://acme/researcher",
  act: :request,
  relates_to: nil,
  payload: %Spectre.Pulse.Payload{
    type: "research.perform",
    data: %{"topic" => "Italian nautical market"}
  },
  metadata: %{}
}
```

Version 1 has:

- one canonical sender and recipient;
- one UUIDv7 message id;
- one controlled communicative act;
- one namespaced payload type;
- opaque application data;
- optional causal correlation through `relates_to`;
- sender-declared metadata.

The three acts are:

| Act | Meaning |
| --- | --- |
| `inform` | Share a fact, result, or update |
| `query` | Ask a question |
| `request` | Ask the recipient to perform work |

Pulse validates structure but never interprets application payload data.
The normative JSON shape is
[`priv/schema/pulse-envelope-v1.schema.json`](https://github.com/elchemista/spectre_pulse/blob/main/priv/schema/pulse-envelope-v1.schema.json).

## Responses and correlation

A semantic response is always a new envelope:

```text
m1 Coordinator → Researcher  request research.perform
m2 Researcher  → Coordinator inform  research.accepted   relates_to m1
m3 Researcher  → Coordinator query   research.details    relates_to m1
m4 Coordinator → Researcher  inform  research.details    relates_to m3
m5 Researcher  → Coordinator inform  research.completed  relates_to m1
```

`relates_to` builds a causal graph. It is not a response channel, session id,
or instruction to resume a particular Spectre Run. Pulse delivers each inbound
message as an ordinary Spectre turn. Resolving it to an owned Run belongs to the
host and Instance layer.

Retries reuse the same message id. A new semantic statement uses a new id.

## Inbound trust boundary

An inbound transport must supply authenticated facts independently of the
envelope:

```elixir
{:ok, inbound} =
  Spectre.Pulse.receive(envelope, %{
    authenticated_identity: "spectre://acme/coordinator",
    binding: :websocket,
    peer: peer,
    verified: %{tls: true}
  })
```

Pulse then:

1. validates version, UUIDv7, addresses, act, payload type, and size limits;
2. compares `Envelope.from` with the transport-authenticated identity;
3. resolves and validates `Envelope.to`;
4. applies the connection and application authorization boundaries;
5. creates a provider-neutral `Spectre.Input`;
6. calls the Agent's normal `Spectre.turn/3` path;
7. returns the normal turn and a technical receipt.

Trusted data is projected under `input.meta.pulse`:

```elixir
%{
  message_id: envelope.id,
  from: authenticated_sender,
  to: envelope.to,
  act: envelope.act,
  relates_to: envelope.relates_to,
  type: envelope.payload.type,
  authenticated: true,
  binding: inbound_context.binding,
  verified: inbound_context.verified,
  declared_metadata: envelope.metadata
}
```

Sender-declared metadata is kept in `declared_metadata`. It is never merged into
`verified`. A sender cannot claim mTLS, an issuer, a principal role, or another
transport fact through envelope JSON.

Remote controlled vocabularies are fixed. Decoding acts, process fields, and
connection commands never calls `String.to_atom/1` on user input.

## Known, authenticated, and authorized are different

Keep these concepts separate:

| State | Meaning |
| --- | --- |
| Known | An address appears in an Agent ContactBook or directory |
| Authenticated | A transport proved the connection principal |
| Authorized | Host or Spectre policy permits this operation and data |

Capability advertisements are discovery claims, not grants. Display-name
similarity never merges identities. A contact is not automatically trusted.

The safe network default requires authentication. Explicit anonymous modes
remain marked `authenticated: false` and still pass recipient and authorization
checks.

## Connection scopes and Agent exposure

Authorization is bounded by the live connection:

- the `ConnectionSpec` defines the maximum scopes;
- the authenticated `Principal` owns a set of scopes;
- the authorizer chooses grants from their intersection;
- the connection exposes all or an explicit subset of local Agents;
- inbound messages and Studio calls cannot escape that Agent subset.

Credentials are used only during handshake preparation and are not stored in
the connection registry, public manifest, or safe error frames.

For the concrete WebSocket boundary see [Phoenix integration](PHOENIX.md).

## Technical receipts

A receipt confirms only that a binding accepted the envelope:

```elixir
%Spectre.Pulse.Receipt{
  message_id: "019f...",
  status: :accepted,
  via: :websocket
}
```

It does not mean that the recipient:

- understood the payload;
- accepted the requested business work;
- completed the operation;
- persisted a semantic result.

Represent those facts with later correlated envelopes.

## Delivery outcomes

Pulse distinguishes two failure outcomes:

| Outcome | Meaning | Try another route automatically? |
| --- | --- | --- |
| `not_sent` | The adapter knows the envelope did not cross its handoff boundary | Yes |
| `outcome_unknown` | Delivery may already have happened | No |

Examples:

- A connection refusal before writing a REST request is `not_sent`.
- A missing WebSocket process before sending is `not_sent`.
- A timeout after writing a request is `outcome_unknown`.
- A connection loss after handing a frame to the socket is generally
  `outcome_unknown`.

Stopping on ambiguity prevents transparent failover from duplicating a
non-idempotent operation.

## At-least-possibly-once delivery

Pulse does not claim exactly-once delivery or global ordering. Messages may be
duplicated and may arrive out of order. Handlers whose domain operations require
deduplication should use the stable message id as their idempotency key.

The staged Effect already uses a stable id:

```elixir
%Spectre.Effect{
  kind: :pulse,
  name: :send,
  id: message_id,
  idempotency_key: "pulse:" <> message_id
}
```

The application decides whether and when to retry after ambiguous delivery:

```elixir
case Spectre.execute(MyApp.Coordinator, turn.result) do
  {:ok, %{effects: [%Spectre.Effect{status: :completed}]} = result} ->
    persist_terminal_state(result)

  {:ok,
   %{effects: [%Spectre.Effect{
     status: :failed,
     error: %Spectre.Pulse.Error{outcome: :not_sent} = error
   }]}} ->
    schedule_retry(error.message_id)

  {:ok,
   %{effects: [%Spectre.Effect{
     status: :failed,
     error: %Spectre.Pulse.Error{outcome: :outcome_unknown} = error
   }]}} ->
    reconcile_before_retry(error.message_id)
end
```

## Runtime information safety

Remote OTP snapshots require the `agent.runtime.read` scope and an Agent exposed
by the connection. Realtime streams additionally require
`agent.runtime.stream`.

Runtime inspection returns bounded, wire-safe `Process.info/2` fields. It never
returns mailbox contents, process dictionaries, or raw GenServer state. See
[Studio monitoring](STUDIO_MONITORING.md) for the complete boundary.

## Reachability is an observation

Reachability is one of:

- `reachable`;
- `unreachable`;
- `unknown`.

It includes an observation time. It is not a promise that an Agent is idle,
available, willing to accept work, or semantically healthy. A transport without
a reliable probe reports `unknown`.

## Reserved control envelopes

The minimal protocol control payload types are ordinary Agent envelopes:

- `pulse.identity.describe`;
- `pulse.reachability.ping`;
- `pulse.reachability.pong`.

They create no global presence truth or heartbeat process. A pong proves only
that an endpoint answered one observation.

Connection-control frames such as `agent.runtime.monitor.enable` are a separate
technical surface tied to an authenticated connection; they are not Agent
payload envelopes.

## Common failures

| Error | Meaning |
| --- | --- |
| `unknown_contact` | ContactBook and directory could not resolve the logical name |
| `no_route` | The address resolved, but no current route exists |
| `duplicate_pulse_identity` | Two discovered modules declared the same address |
| `connection_authenticator_required` | A network connection has no authenticator |
| `sender_identity_mismatch` | `Envelope.from` disagrees with the authenticated peer |
| `recipient_identity_mismatch` | The selected endpoint does not own `Envelope.to` |
| `connection_recipient_not_exposed` | The connection does not expose the target Agent |
| `connection_scope_required` | The live connection lacks the required grant |
| `rest_authenticator_required` | REST is using its fail-closed default |
| `instance_not_found` | The selected Subject-scoped Agent Instance is unavailable |

Safe public errors expose a stable kind and code. They do not serialize
credentials, callback exceptions, arbitrary payloads, or internal state.

## Production checklist

- Serve browser WebSockets through WSS and retain Phoenix origin checks.
- Use short-lived, purpose-bound credentials for socket authentication.
- Keep connection Agent allow-lists and scopes as narrow as practical.
- Treat capability advertisements as descriptive only.
- Preserve `not_sent` versus `outcome_unknown` in every custom transport.
- Make domain handlers idempotent where duplicate delivery matters.
- Never promote sender metadata into verified transport facts.
- Disable temporary monitoring when the UI no longer needs it.
- Treat reachability as a timestamped technical observation.

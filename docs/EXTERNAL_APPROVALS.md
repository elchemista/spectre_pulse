# External human approvals over Pulse

A customer's AgentServer can ask an authorized human to approve an operation
through that person's AgentServer. Pulse carries the messages; Managed GAM owns
the frozen request, trust configuration, human signatures, Evidence and admission.
The receiving AI has no authority to approve for the human.

Start with the [core two-Server setup](https://github.com/elchemista/spectre/blob/0.4.0/docs/EXTERNAL_APPROVALS.md), including
independent identities/storage, operation permissions, the approver's Ed25519
public key and the human chat host's private signing key. That example also runs
without Pulse. The keys and approval mandate remain trusted host configuration,
never model tool arguments. Browser governance must cover every navigation
operation used by the application; see that guide's Lens bindings.

## Select the transport

On both Servers, set this entry in the existing `gam: [approvals: ...]` options:

```elixir
transport: {Spectre.Pulse.Approval, []}
```

Give each Agent a matching Pulse identity using `use Spectre.Pulse` and the
ordinary Agent configuration DSL:

```elixir
pulsing do
  identity("spectre://owner/assistant")
end
```

For local delivery, subscribe the two existing AgentServers:

```elixir
{:ok, _} = Spectre.Pulse.Local.subscribe(MyApp.CustomerAgent,
  inbound: [agent_server: customer])
{:ok, _} = Spectre.Pulse.Local.subscribe(MyApp.PersonalAgent,
  inbound: [agent_server: personal])
```

For a remote transport, the authenticated host resolves the destination Server
and calls `Spectre.Pulse.receive(envelope, trusted_context, agent_server: server)`.
`trusted_context.authenticated_identity` must come from verified peer credentials,
not `envelope.from`. Apply the normal inbound authorizer and allowed-types policy.
`allow_unauthenticated: true` cannot enable approval messages. Local delivery uses
the existing trusted-BEAM boundary; see [delivery and security](DELIVERY_AND_SECURITY.md).

## Present the request in the human's chat

The configured consumer receives
`{Spectre.Server, personal_server, {:approval_request, request}}`. Render
`request["rendered_payload"]` literally with requester and expiry visible.
After rendering, the authenticated consumer records presentation and submits only
the human's explicit choice:

```elixir
reference = %{id: request["id"], from: request["from"]}
:ok = Spectre.Server.approval_presented(personal_server, reference)
:ok = Spectre.Server.approval_decide(personal_server, reference, :confirm)
# Use :reject for refusal.
```

These calls persist the exact signed response and queue delivery. They are
consumer-only APIs, not model signing tools. A separate `:approval_delivery`
consumer event reports `:accepted`, `:not_sent` or `:unknown`; those statuses
only describe transport delivery. Retry the same choice to resend the stored
signed reply; a completed choice cannot be changed.

## Envelope and recovery contracts

Envelope stays at version 1. `Spectre.Pulse.Approval.types/0` exposes four types:

| Type | Meaning |
| --- | --- |
| `approval.request` | Frozen request, Presentation, original operation references and deadline |
| `approval.presented` | Signed rendering acknowledgement; it is not consent |
| `approval.decision` | Signed human `confirm` or `reject`, bound to the complete request |
| `approval.cancelled` | Requester notification that the pending wait closed |

Responses use `relates_to`. Managed GAM verifies the configured human's signature,
request identity/revision/digests, expiry and one-use proof before admission under
the customer's original authority. A known peer or an `accepted` Pulse receipt
cannot supply consent or widen execution permissions.

Delivery may duplicate or reorder messages. Deduplicate UI rows by the receiving
address plus `{request["from"], request["id"]}`; use the scoped reference above
for history and decisions. `approval/2` and paginated `approval_inbox/2` read the
selected coordination Store on demand. An active Ledger resource is reused;
Pulse adds no approval database or journal. Database retention, payload protection
and outage buffering belong to the application and core storage policy.

A restored customer resumes consent suspended; its `resume_pending/2` callback
must issue a fresh Presentation/request. Old decisions cannot authorize that new
wait. Cancellation is best effort and deadlines remain binding. See the
[core approval and storage guide](https://github.com/elchemista/spectre/blob/0.4.0/docs/EXTERNAL_APPROVALS.md) for recovery,
outages and UI events, and [payload storage](https://github.com/elchemista/spectre/blob/0.4.0/docs/PAYLOAD_STORAGE.md) for
application-owned encryption or external object storage.

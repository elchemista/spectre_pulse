# Pulse with Spectre 0.4 AgentServer

[README](../README.md) · [Migration](MIGRATING_TO_0_4.md) · [Public API](PUBLIC_API.md)

## Helper contract

| Input | Path | Callback result |
| --- | --- | --- |
| `{:protocol}` | Cognitive | `{:ok, protocol_map}` |
| `{:contacts}` | Cognitive | `{:ok, [%{key: ..., identity: ..., display_name: ..., capabilities: ...}]}` |
| `{:send, attrs}` | Governed when bound | GAM result rather than a raw synchronous receipt |

There is no default operation; descriptor budgets are 1 MiB. Contacts omit live
route handles and private metadata. Sending accepts only `:to`, `:act`, `:payload`,
`:relates_to` and `:metadata`. Sender identity and command id are trusted inputs,
not model options. Bindings reject unknown keys, duplicates, nil and non-atoms.

```elixir
install Spectre.Pulse, bind: [send: :notify_agent]
```

The Agent's Pulse configuration supplies identity, contacts, discovery directory
and network. The retained `pulsing` declarations can configure those values:

```elixir
defmodule MyApp.SupportAgent do
  use Spectre.Agent, stack: MyApp.AI, server: MyApp.SupportServer
  use Spectre.Pulse

  pulsing do
    identity "spectre://support/assistant"
    network MyApp.DeliveryNetwork
  end
  # Keep the Agent's flow declarations here as well.
end
```

`use Spectre.Pulse` imports the optional Agent configuration DSL; it is not an
import of the Server's generated helper. `MyApp.DeliveryNetwork` is a host
implementation of `Spectre.Pulse.Network`, not a built-in provider. The Stack
selects available transport/directory modules; Pulse's existing application
transport infrastructure remains host-supervised, not implicitly per-Server.

## The governed send boundary

Before starting a Server that selects the bound Stack, configure the standard GAM
bridge with the host's scope and `:notify_agent` Candidate factory:

```elixir
bridge = [scope: gam_scope,
  bindings: %{notify_agent: %{candidate: {MyApp.GAMHost, :notification}}}]
{:ok, server} = Spectre.Server.start_link(
  agent: MyApp.SupportAgent, gam: {Spectre.Server.GAM, bridge}, consumer: self())
```

The domain, lawful surface, mandate/evidence and admitted executor are application
requirements; this snippet does not create them. After admission the host executor
may call `Spectre.Pulse.ServerTool.execute(prepared_wire_attrs, trusted_context)`.
The trusted context includes `:agent` and a stable UUIDv7 `:command_id`. Pulse pins
both string/atom forms of sender and id before using the real network dispatcher.
It validates the id before making a transport attempt.

A successful native execution result contains only `message_id`, `status`, `via`,
`route_id` and ISO `accepted_at`. Error maps retain `outcome: :not_sent` or
`:outcome_unknown` and bounded portable details. Generic/nonportable failures are
conservatively unknown. Map unknown delivery to a GAM `:ambiguous` outcome rather
than definitive failure. A stable message id supports correlation, but Pulse's
protocol does not guarantee exactly-once delivery.

In `handle_result/3`, a successful admitted send is represented by
`{:governed, %{outcome: %{status: :succeeded}}}`; ambiguous effects retain
`:ambiguous`. Technical acceptance is not a remote business-task result. A remote
answer is a separate envelope correlated using `relates_to`.

## Authenticated ingress into the existing Server

```elixir
trusted_context = %{
  target: MyApp.SupportAgent,
  authenticated_identity: authenticated_peer.address,
  binding: :https
}
{:ok, accepted} = Spectre.Pulse.receive(envelope, trusted_context, agent_server: server)
interaction = accepted.interaction
# accepted.turn is nil; responses arrive through the Server consumer.
```

`authenticated_peer` comes from the transport's verified authentication boundary;
it is not inferred from `envelope.from`. Normal sender/recipient checks, payload
validation, authorization, mapping and conversation scope run before enqueueing.
The host resolves the correct Server and consumes its eventual responses. The
receipt means input acceptance, not completion of the Agent's work.

Without `agent_server:`, the retained bridge follows its existing Turn/Instance
path. Existing Phoenix/Studio runtime and operations inspectors are not an
automatically installed AgentServer registry or new authorization system. See
[transport and legacy runtime material](TRANSPORTS_AND_LEGACY_RUNTIME.md),
[Phoenix](PHOENIX.md), and [delivery/security](DELIVERY_AND_SECURITY.md).

## Who owns the loop

`Spectre.Stack` installs immutable capabilities and tool descriptors. The Agent
selects that Stack and declares routing; `Spectre.Server` owns the GenServer,
conversation, pending continuations and asynchronous results. Use
`use Spectre.Server, tools: MyApp.AI` to generate private helpers. A helper returns
an updated context containing a command; return that context from the callback.
It does not synchronously return the package's result. Tuple input chooses the
operation, and `on_result:` identifies the continuation in `handle_result/3`.

Preparation validates ordinary input before dispatch. The Server records command
identity/outbox state, invokes a worker, projects a bounded portable result and
acquires that result before the callback. Durable recovery needs a configured
checkpoint Store. Keep live clients and process handles in supervised resources
or process-local private context, not durable assigns. Native OTP callbacks remain
available; they use the same command-return boundary.

## Where GAM intervenes

The [Governed Act Model](https://spectre.guide/en/governed-act-model) separates
authority (Mandate), supporting facts (Evidence), the admitted execution record
(Act), and unresolved obligations (Duty). Installing a package connects a
capability; it does not grant authority to act on behalf of the end user.

A cognitive descriptor uses the tool worker path, without treating every model
call or classifier as a consent request. A governed descriptor becomes `propose`
before dispatch and uses the standard `Spectre.Server.GAM` bridge. Host-declared
surfaces still determine which effects, protected disclosures or resource uses
need governance; read-oriented operations are not categorically exempt from those
application boundaries. Ordinary configured cognition is not automatically a new
end-user authorization ceremony.

The host supplies a real GAM scope, Candidate factories, mandates/evidence,
Presentations or interpretation when needed, and admitted executors. `bind:` names
that host operation only. Native package calls outside the Server do not acquire
these guarantees automatically. Do not call an effect executor directly from a
flow callback and describe that call as governed.

## Recovery and errors

These packages' cognitive helper operations declare safe replay; the runtime can
retry interrupted cognitive work according to the command lifecycle. A result is
still scoped to its original command. Application input rejections arrive as
`{:error, {:tool_rejected, tool, operation, reason}}`; library domain errors retain
the package's tagged result. Invalid installation/configuration is a developer
error, not a successful acquisition. The general `{:tool_unknown, details}` result
belongs to nonrepeatable tool operations; it is not an instruction to retry effects.

Governed effects use GAM recovery and their original command identities. An
ambiguous outcome remains unresolved: it must not be reported as proof of failure
or sent again blindly. A changed checkpoint fingerprint needs an explicit host
migration. Contract drift is handled by the core recovery rules rather than
invoking new adapter code with old arguments. See the
[core lifecycle](https://github.com/elchemista/spectre/blob/feature/agent-server-gam/docs/STACK_TOOLS.md)
and [AgentServer guide](https://github.com/elchemista/spectre/blob/feature/agent-server-gam/docs/AGENT_SERVER.md).

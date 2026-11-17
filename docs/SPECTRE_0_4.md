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

Before starting a Server that selects the bound Stack, configure its allowed
operation and confirmation policy:

```elixir
{:ok, server} = Spectre.Server.start_link(
  agent: MyApp.SupportAgent,
  identity: %{subject: authenticated_user.id},
  consumer: self(),
  gam: [operations: %{notify_agent: [confirm: true]}])
```

AgentServer creates its GAM boundary and frozen presentation automatically.
After admission, the registered Pulse adapter dispatches the committed request.
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

## Mounted Skill runtimes

Use `pulse` in Skill Server callbacks with the same installed Stack. Each mount
keeps its own state and checkpoint while sharing the root GAM Domain; mounting
does not grant authority. A bound send remains governed and an uncertain transport outcome must be reconciled before another send. See the
[core Skill ownership guide](https://github.com/elchemista/spectre/blob/0.4.0/docs/SKILLS.md).

## Command lifecycle

Return the context containing the helper command and receive its tagged result
in `handle_result/3`. Invalid input is reported as
`{:error, {:tool_rejected, tool, operation, reason}}`. Recovery and acquired results
remain scoped to the original command; native package calls outside the Server
retain their own host boundary. See the
[Server lifecycle](https://github.com/elchemista/spectre/blob/0.4.0/docs/STACK_TOOLS.md) and
[Managed GAM guide](https://github.com/elchemista/spectre/blob/0.4.0/docs/MANAGED_GAM.md).

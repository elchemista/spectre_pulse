# Spectre Pulse

Pulse is a transport-independent protocol for communicating agents: canonical
addresses, versioned envelopes, discovery, transports, authenticated ingress and
technical delivery receipts. Native protocol and transport APIs remain useful
without an AgentServer. Phoenix WebSocket, REST, PubSub and distributed Erlang
adapters share these message semantics.

With Spectre 0.4, a Stack supplies the `pulse` helper, an Agent supplies routes,
and the AgentServer owns the conversation. Protocol/contact inspection is a
cognitive command; an explicitly bound send becomes a GAM proposal. Existing
Instance and Studio integrations remain available as separate runtime surfaces.

## Installation on the 0.4 development branch

This branch targets the **unreleased Spectre 0.4 architecture**. Use matching Git
branches; the examples do not assume that Spectre 0.4 is available from Hex.
For reproducible deployments, replace branch selectors with reviewed commit refs.

```elixir
def deps do
  [
    {:spectre, github: "elchemista/spectre",
     branch: "feature/agent-server-gam", override: true},
    {:spectre_pulse, github: "elchemista/spectre_pulse",
     branch: "feature/stack-tools-0.4"}
  ]
end
```

The manifest requirement `{:spectre, "~> 0.4.0"}` describes compatibility, not
publication availability. Local contributors can use `SPECTRE_PATH=../spectre` for this repository's dependency override.

## Discover the protocol in AgentServer

```elixir
defmodule MyApp.AI do
  use Spectre.Stack
  install Spectre.Pulse
end

defmodule MyApp.SupportServer do
  use Spectre.Server, tools: MyApp.AI

  def handle_flow({:support, :protocol}, _input, ctx),
    do: pulse(ctx, {:protocol}, on_result: :protocol)

  def handle_result(:protocol, {:ok, protocol}, ctx),
    do: respond(ctx, "#{protocol.name} version #{protocol.version}")

  def handle_result(tag, result, ctx), do: super(tag, result, ctx)
end

defmodule MyApp.SupportAgent do
  use Spectre.Agent, stack: MyApp.AI, server: MyApp.SupportServer
  flow :support do
    on :protocol, regex: ~r/^protocol$/i
  end
end
```

Start it with `Spectre.Server.start_link(agent: MyApp.SupportAgent, consumer:
self())` and submit `Spectre.Server.input(server, "protocol")`. The helper returns
through `handle_result/3`; it is generated from the Stack, not imported from Pulse.

## Send an agent message

Add `bind: [send: :notify_agent]` to the installation and configure a GAM host
binding for `:notify_agent`. Then stage the message from a Server callback:

```elixir
pulse(ctx,
  {:send, %{to: "spectre://research/reviewer", act: :inform,
            payload: %{type: "research.completed", data: %{report_id: "r-42"}}}},
  on_result: :notified)
```

Payload types use protocol names such as `research.completed`, not MIME types
such as `text/plain`. Pulse pins the sender from the Agent configuration and the
message id from the admitted command. The host owns network/discovery configuration,
recipient authorization, consent when required, and reconciliation of uncertain sends.
A technical `:accepted` receipt does not prove that the remote task completed.

For authenticated inbound messages, the transport host selects the existing
Server and calls `Spectre.Pulse.receive(envelope, trusted_context,
agent_server: server)`. Acceptance returns `result.interaction`; replies arrive
asynchronously through the Server consumer. Do not derive authenticated identity
from an unverified `envelope.from`. The [integration guide](docs/SPECTRE_0_4.md)
spells out this boundary and the retained legacy paths.

## Documentation

- [AgentServer integration, helpers and authority](docs/SPECTRE_0_4.md)
- [Migration from the 0.3 integration](docs/MIGRATING_TO_0_4.md)
- [Standalone and retained runtime reference](docs/TRANSPORTS_AND_LEGACY_RUNTIME.md)
- [Public API](docs/PUBLIC_API.md) and [changelog](CHANGELOG.md)
- [Governed Act Model](https://spectre.guide/en/governed-act-model) and [core tool lifecycle](https://github.com/elchemista/spectre/blob/feature/agent-server-gam/docs/STACK_TOOLS.md)

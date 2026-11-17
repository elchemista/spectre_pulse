# Native Pulse and retained runtime integration

For the retained core runtime APIs, see the [compatibility index](https://github.com/elchemista/spectre/blob/0.4.0/docs/COMPATIBILITY.md).

Pulse's Envelope v1, addresses, discovery and transports can be used without an
AgentServer. The host owns authentication, routing, supervision and application
authorization. Native technical delivery receipts do not grant GAM authority.

Choose the guide for the runtime you use:

| Integration | Guide |
| --- | --- |
| AgentServer helpers and authenticated ingress | [AgentServer 0.4](SPECTRE_0_4.md) |
| External human consent | [External approvals](EXTERNAL_APPROVALS.md) |
| Native envelopes, local Agents and the retained action DSL | [Getting started](GETTING_STARTED.md) |
| Phoenix Endpoint and authenticated WebSockets | [Phoenix](PHOENIX.md) |
| Local, Node, REST, PubSub and custom adapters | [Connections and transports](CONNECTIONS_AND_TRANSPORTS.md) |
| Delivery uncertainty, peer trust and correlation | [Delivery and security](DELIVERY_AND_SECURITY.md) |
| Instance OTP process inspection | [Runtime monitoring](STUDIO_MONITORING.md) |
| Retained Work/Vigil views and subscriptions | [Operational monitoring](STUDIO_OPERATIONS.md) |
| Retained semantic cache, journals, Skills and Morph | [Studio inspection](STUDIO_INSPECTION.md) |

The Instance and Studio examples use their retained runtime contracts. They do
not automatically register or inspect AgentServers. For an AgentServer, resolve
the actual Server in the authenticated host and pass `agent_server: server` to
inbound delivery. Responses arrive asynchronously through its consumer.

Native message delivery still uses the same versioned protocol:

```elixir
Spectre.Pulse.protocol()
```

Run `mix run examples/local_agents.exs` for the retained local messaging example.
Its Turn/Effect lifecycle is distinct from the new Server helper lifecycle; the
[migration guide](MIGRATING_TO_0_4.md) lists the changes.

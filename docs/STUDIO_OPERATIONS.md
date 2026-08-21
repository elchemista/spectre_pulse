# Spectre Studio Work and Vigil monitoring

Pulse exposes the committed, read-only Work and Vigil views already provided by
Spectre 0.3.2. Studio can read one snapshot or open a temporary near-realtime
subscription while an operator watches an Agent Instance.

This capability is separate from OTP runtime monitoring:

| Capability | Answers |
| --- | --- |
| `agent.runtime.*` | Is the Instance process healthy, busy, or accumulating messages? |
| `agent.operations.*` | Which Work and Vigil loops are active, waiting, blocked, paused, or terminal? |

Pulse does not become the owner of the loops. Spectre remains responsible for
their canonical state, scheduling, persistence, publication policy, and control
plane.

## Security boundary

Remote access uses two independent scopes:

| Scope | Meaning |
| --- | --- |
| `agent.operations.read` | Read bounded Work and Vigil views |
| `agent.operations.stream` | Maintain a repeating subscription |

The authenticated connection must also expose the selected Agent. Pulse obtains
the connection id from trusted adapter state; a client cannot select or forge a
different connection in its frame.

Pulse calls `Spectre.loops/2` and serializes only `Spectre.Operation.View`.
It never reads the Instance's GenServer state or canonical loop structs. Spectre
applies each Definition's publication policy before Pulse receives the view, so
progress, results, artifacts, and blockers may contain `"redacted"`.

## Read one snapshot

Trusted host tooling can call:

```elixir
{:ok, snapshot} =
  Spectre.Pulse.operations(MyApp.Researcher, subject,
    kinds: [:work, :vigil],
    include_terminal: false
  )
```

An adapter acting for a remote principal must include its trusted connection id:

```elixir
{:ok, snapshot} =
  Spectre.Pulse.operations(
    "spectre://acme/researcher",
    subject,
    connection: connection_id
  )
```

By default Pulse returns nonterminal Work and Vigil loops. Supported filters are:

| Option | Default | Meaning |
| --- | --- | --- |
| `kinds` | `[:work, :vigil]` | Include Work, Vigil, or both |
| `include_terminal` | `false` | Include completed, failed, or stopped loops |
| `max_binary_bytes` | `16384` | Bound each published binary; hard maximum 65536 |
| `max_collection_entries` | `128` | Bound loops and nested collections; hard maximum 256 |
| `max_depth` | `6` | Bound nested serialization; hard maximum 8 |

Example response:

```json
{
  "schema_version": 1,
  "capability": "agent.operations.list",
  "scope": "agent.operations.read",
  "agent_address": "spectre://acme/researcher",
  "instance_ref": "...",
  "sampled_at_unix_ms": 1787352200000,
  "counts": {"total": 2, "work": 1, "vigil": 1},
  "truncated": false,
  "truncated_fields": {},
  "filters": {
    "kinds": ["work", "vigil"],
    "include_terminal": false
  },
  "limits": {
    "max_binary_bytes": 16384,
    "max_collection_entries": 128,
    "max_depth": 6
  },
  "operations": [
    {
      "id": "019...",
      "kind": "work",
      "definition": "research_pages",
      "status": "waiting",
      "phase": "reading",
      "operation": "fetch_page",
      "progress": {"completed": 4, "total": 12},
      "blocker": null,
      "retries": 1,
      "next_trigger": null,
      "budget": {
        "consumed": {"steps": 5},
        "remaining": {"steps": 495}
      }
    }
  ]
}
```

The complete view also includes safe control state, attempts, observations,
checkpoint summary, pending-command summary, wait reference, trigger generation,
reconciliation state, and publication-safe metadata.

## Enable near-realtime monitoring

Monitoring is enabled by a Studio call, not application configuration. Send on
the authenticated Phoenix WebSocket:

```json
{
  "pulse": "connection",
  "version": 1,
  "type": "agent.operations.monitor.enable",
  "request_id": "operations-panel-42",
  "agent_address": "spectre://acme/researcher",
  "subject": "account-123",
  "interval_ms": 1000,
  "duration_ms": 60000,
  "kinds": ["work", "vigil"],
  "include_terminal": false
}
```

Pulse authorizes the request and captures the first snapshot before registering
the subscription. The reply contains sequence `0`:

```json
{
  "pulse": "connection",
  "version": 1,
  "type": "agent.operations.monitor.enabled",
  "monitor": "operations",
  "request_id": "operations-panel-42",
  "subscription_id": "019...",
  "sequence": 0,
  "dropped_updates": 0,
  "snapshot": {
    "capability": "agent.operations.list",
    "counts": {"total": 2, "work": 1, "vigil": 1},
    "operations": []
  }
}
```

Periodic frames use `agent.operations.monitor.update` and sequence numbers
starting at `1`. Each update is a complete bounded snapshot, not a delta or a
durable business event. Studio should replace the displayed snapshot for that
subscription and treat sequence gaps as observation gaps.

`dropped_updates` reports samples coalesced while the socket sink was under
backpressure. Sampling never overlaps for one subscription.

## Disable, expiry, and errors

When the panel closes, send:

```json
{
  "pulse": "connection",
  "version": 1,
  "type": "agent.operations.monitor.disable",
  "subscription_id": "019..."
}
```

Pulse replies with `agent.operations.monitor.disabled`. It sends
`agent.operations.monitor.expired` when `duration_ms` elapses and
`agent.operations.monitor.error` when a later sample fails safely.

Subscription ids belong to their monitoring kind and socket session. A runtime
disable call cannot remove an operations subscription, and an unknown id returns
`unknown_operations_monitor_subscription`.

All subscriptions are removed when the owning socket or adapter exits. They are
not resumed after reconnection; Studio must enable them again.

## Adapter-neutral API

A custom transport uses the same authorization and lifecycle:

```elixir
{:ok, session} =
  Spectre.Pulse.Monitoring.start_link(
    connection: trusted_connection_id,
    sink: self()
  )

{:ok, enabled} =
  Spectre.Pulse.Monitoring.enable_operations(session, %{
    "agent_address" => "spectre://acme/researcher",
    "subject" => "account-123",
    "interval_ms" => 1000
  })

receive do
  {:spectre_pulse_monitoring, update} ->
    MyTransport.push(update)
end

{:ok, _disabled} =
  Spectre.Pulse.Monitoring.disable_operations(
    session,
    enabled["subscription_id"]
  )
```

The adapter implements authentication and delivery. Pulse retains Agent
exposure checks, scopes, validation, bounded serialization, timing,
backpressure, expiry, and cleanup.

Trusted host tooling can inspect the active safe catalog:

```elixir
Spectre.Pulse.operation_monitoring_subscriptions()
Spectre.Pulse.operation_monitoring_subscriptions(connection_id)
```

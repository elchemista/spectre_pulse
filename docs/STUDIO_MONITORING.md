# Spectre Studio and runtime monitoring

Pulse provides a temporary, connection-scoped stream of safe OTP process
snapshots. Spectre Studio can enable it when an operator opens a LiveView panel
and disable it when the panel closes.

Monitoring is a runtime call, not an application setting.

```text
Studio opens Agent panel
  → monitor.enable
  ← enabled + snapshot sequence 0
  ← update sequence 1
  ← update sequence 2
  ...
Studio closes panel
  → monitor.disable
  ← disabled
```

If Studio disconnects without sending `disable`, Pulse removes every
subscription owned by that WebSocket automatically.

## What this capability exposes

Runtime monitoring samples selected `Process.info/2` fields from the live OTP
process behind one Spectre Agent Instance. It can answer questions such as:

- Is the Instance process alive and runnable?
- How much BEAM memory does it currently report?
- Is its mailbox length increasing?
- How many reductions has it consumed?
- What are its current status, heap, stack, links, and monitors?

It does not expose:

- mailbox message contents;
- the process dictionary;
- raw GenServer state;
- Agent memory records or prompt content;
- Ledger entries, model calls, or token totals;
- Lab playback or diff operations.

Those are different domain capabilities. For example, token totals and model
usage belong to Spectre Ledger. Pulse may transport separately authorized
Ledger or Studio calls, but it does not reinterpret them as OTP runtime data.

## Permissions

A remote monitoring connection needs both scopes:

| Scope | Meaning |
| --- | --- |
| `agent.runtime.read` | Read one bounded runtime snapshot |
| `agent.runtime.stream` | Maintain a repeating subscription |

The connection must also expose the selected Agent. Pulse obtains the
connection id from trusted adapter state, never from a client-controlled frame.

```elixir
[
  id: :studio,
  transport: :websocket,
  agents: [MyApp.Researcher],
  scopes: ["agent.runtime.read", "agent.runtime.stream"]
]
```

The authenticated principal and authorizer must grant the same scopes. An Agent
address that is valid but not exposed by this connection is rejected.

## Read one snapshot

Trusted host code can read one current snapshot directly:

```elixir
{:ok, snapshot} =
  Spectre.Pulse.runtime_info(MyApp.Researcher, subject,
    fields: [:memory, :message_queue_len, :reductions, :status]
  )
```

An adapter serving a remote caller must pass its trusted connection id:

```elixir
{:ok, snapshot} =
  Spectre.Pulse.runtime_info(
    "spectre://acme/researcher",
    subject,
    connection: connection_id,
    fields: ["memory", "message_queue_len", "status"]
  )
```

Example response:

```json
{
  "schema_version": 1,
  "capability": "agent.runtime.info",
  "scope": "agent.runtime.read",
  "agent_address": "spectre://acme/researcher",
  "instance_ref": "...",
  "node": "agent@host",
  "pid": "#PID<0.412.0>",
  "alive": true,
  "sampled_at_unix_ms": 1787352200000,
  "process": {
    "memory": 42184,
    "message_queue_len": 0,
    "reductions": 18291,
    "status": "waiting"
  },
  "truncated_fields": [],
  "limits": {
    "max_collection_entries": 128,
    "max_depth": 6
  }
}
```

## Enable a realtime subscription

Studio sends this connection-control frame on the authenticated WebSocket:

```json
{
  "pulse": "connection",
  "version": 1,
  "type": "agent.runtime.monitor.enable",
  "request_id": "liveview-panel-42",
  "agent_address": "spectre://acme/researcher",
  "subject": "account-123",
  "interval_ms": 1000,
  "duration_ms": 60000,
  "fields": ["memory", "message_queue_len", "reductions", "status"],
  "max_collection_entries": 128,
  "max_depth": 6
}
```

Fields:

| Field | Required | Meaning |
| --- | --- | --- |
| `agent_address` | yes | Canonical Agent address exposed by this connection |
| `subject` | yes | Portable identity selecting the Spectre Instance |
| `request_id` | no | Studio correlation id, at most 128 bytes |
| `interval_ms` | no | Sampling interval; default 1000 ms |
| `duration_ms` | no | Automatic expiry; omission means until disable/disconnect |
| `fields` | no | Safe process fields; omission selects all safe fields |
| `max_collection_entries` | no | Lower response collection bound |
| `max_depth` | no | Lower response nesting bound |

Pulse validates permissions and captures the first sample before creating the
subscription. A successful reply therefore proves that the Agent Instance was
resolvable at enable time:

```json
{
  "pulse": "connection",
  "version": 1,
  "type": "agent.runtime.monitor.enabled",
  "request_id": "liveview-panel-42",
  "subscription_id": "019...",
  "connection_id": "019...",
  "agent_address": "spectre://acme/researcher",
  "subject_id": "account-123",
  "interval_ms": 1000,
  "duration_ms": 60000,
  "created_at_unix_ms": 1787352200000,
  "sequence": 0,
  "dropped_updates": 0,
  "snapshot": {}
}
```

Store the server-generated `subscription_id`. Do not construct subscription ids
in Studio.

## Receive updates

Pulse pushes updates on the same connection:

```json
{
  "pulse": "connection",
  "version": 1,
  "type": "agent.runtime.monitor.update",
  "subscription_id": "019...",
  "sequence": 1,
  "dropped_updates": 0,
  "snapshot": {
    "sampled_at_unix_ms": 1787352201000,
    "process": {
      "memory": 42720,
      "message_queue_len": 2,
      "reductions": 18902,
      "status": "running"
    }
  }
}
```

Sequence `0` belongs to `enabled`; periodic updates begin at `1`. Studio should
use `subscription_id` as the stream key and treat sequence gaps as observation
gaps, not as missing business events.

`dropped_updates` reports samples coalesced while the transport sink was under
backpressure. Pulse does not allow monitoring to grow an unbounded socket
mailbox.

## Disable the subscription

When the operator stops watching the Agent, Studio sends:

```json
{
  "pulse": "connection",
  "version": 1,
  "type": "agent.runtime.monitor.disable",
  "subscription_id": "019..."
}
```

Pulse confirms removal:

```json
{
  "pulse": "connection",
  "version": 1,
  "type": "agent.runtime.monitor.disabled",
  "subscription_id": "019...",
  "agent_address": "spectre://acme/researcher",
  "subject_id": "account-123"
}
```

Disable is scoped to the session that owns the subscription. An unknown or
foreign id returns `unknown_runtime_monitor_subscription`.

## Expiry and sampling errors

When `duration_ms` elapses Pulse removes the subscription and pushes:

```json
{
  "pulse": "connection",
  "version": 1,
  "type": "agent.runtime.monitor.expired",
  "subscription_id": "019..."
}
```

If the Instance disappears after the stream started, Pulse pushes a safe error
event without exposing process state or exception details:

```json
{
  "pulse": "connection",
  "version": 1,
  "type": "agent.runtime.monitor.error",
  "subscription_id": "019...",
  "sequence": 4,
  "error": {
    "kind": "routing",
    "code": "instance_not_found"
  }
}
```

Studio may keep the panel open, show the error, explicitly disable, or wait for
the Instance to return. Pulse continues the bounded sampling schedule until the
subscription otherwise ends.

## Safe process fields

The allowed vocabulary is fixed and decoding never creates atoms from remote
input:

```text
registered_name       status              initial_call
current_function      message_queue_len   links
monitors              monitored_by        trap_exit
error_handler         priority            group_leader
total_heap_size       heap_size           stack_size
reductions            garbage_collection suspending
memory
```

Mailbox contents (`messages`), process dictionaries, and raw state are not in
the vocabulary and cannot be requested.

## Internal safety limits

The following are Pulse guardrails, not user configuration:

| Limit | Value |
| --- | --- |
| Minimum interval | 250 ms |
| Default interval | 1000 ms |
| Maximum interval | 60,000 ms |
| Minimum explicit duration | 250 ms |
| Maximum explicit duration | 3,600,000 ms |
| Active subscriptions per connection | 16 |
| Maximum sink queue before coalescing | 100 messages |
| Default / hard collection entries | 128 / 256 |
| Default / hard serialization depth | 6 / 8 |

Sampling for one subscription never overlaps: Pulse schedules the next tick
only after the current sample has completed.

## LiveView lifecycle recommendation

For each monitoring panel:

1. Wait for the Pulse connection manifest.
2. Check both runtime scopes and confirm the Agent is exposed.
3. Send `enable` only after the user requests live monitoring.
4. Store `request_id → subscription_id` in the Studio connection state.
5. Route `update`, `error`, and `expired` by `subscription_id`.
6. Send `disable` when the panel or monitoring mode closes.
7. Treat socket loss as terminal for all subscription ids from that connection.
8. Re-enable explicitly after reconnect; old ids are not resumable.

This makes the UI lifecycle explicit while retaining server-side cleanup if the
browser or LiveView process disappears unexpectedly.

## Adapter-neutral API

Phoenix only translates WebSocket frames. A custom transport can reuse the same
session and event semantics:

```elixir
{:ok, session} =
  Spectre.Pulse.Monitoring.start_link(
    connection: trusted_connection_id,
    sink: self()
  )

{:ok, enabled} =
  Spectre.Pulse.Monitoring.enable(session, %{
    "agent_address" => "spectre://acme/researcher",
    "subject" => "account-123",
    "interval_ms" => 1000
  })

receive do
  {:spectre_pulse_monitoring, update} ->
    MyTransport.push(update)
end

{:ok, disabled} =
  Spectre.Pulse.Monitoring.disable(session, enabled["subscription_id"])
```

The adapter supplies authentication and a trusted connection id. Pulse owns the
remaining authorization, Agent exposure, validation, timing, catalog, and
cleanup logic.

Trusted host tooling can inspect the active safe catalog:

```elixir
Spectre.Pulse.monitoring_subscriptions()
Spectre.Pulse.monitoring_subscriptions(connection_id)
```

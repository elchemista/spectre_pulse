# Studio inspection and governance

Pulse exposes a small, scoped Studio bridge over the authenticated Phoenix
connection. Bridge envelopes terminate at the connection boundary: they do not
enter an Agent turn, prompt, or application flow.

## Operations

| Payload | Required scope | Result |
| --- | --- | --- |
| `studio.semantic_cache.examples` | `agent.semantic_cache.read` | Up to 100 bounded cache examples |
| `studio.semantic_cache.verify` | `agent.semantic_cache.promote` | The verified/promoted example |
| `studio.journal.turns` | `ledger.read` | Up to 100 persisted compact chat turns |
| `studio.skills.list` | `spectre.skill.read` | The Agent Definition's Skill mounts |
| `studio.morph.propose_skill` | `spectre.morph.propose` | A governed candidate evaluated against the Agent |

Successful replies use the request payload type plus `.result`. Safe failures
use `.error`; both correlate through `relates_to`. The WebSocket receipt only
confirms technical acceptance and is not the operation result.

`studio.skill.mount` is reserved but rejected by the standard bridge. Runtime
Skill mutation needs an application-owned Authority Envelope, runtime registry,
and revision fence. Studio authors new reply-only Skills through Morph instead;
approval and activation remain separate host governance commits.

## Enable the panels

The connection specification and the authenticated principal must both allow
the scopes, and `authorize/2` must return them as grants. Pulse never grants a
Studio capability merely because the package implements it.

```elixir
studio_scopes = [
  "agent.runtime.read",
  "agent.runtime.stream",
  "agent.operations.read",
  "agent.operations.stream"
] ++ Spectre.Pulse.Studio.scopes()

children = [
  {Spectre.Pulse,
   connections: [
     [
       id: :studio,
       transport: :websocket,
       mode: :listen,
       agents: :all,
       authenticate: &MyApp.PulseAccess.authenticate/2,
       authorize: &MyApp.PulseAccess.authorize/2,
       scopes: studio_scopes
     ]
   ]}
]
```

The authenticated principal's `scopes` must include the same permitted subset.
The manifest sent to Studio then reports the actual grants. A locked Studio tab
means its required scope was not granted; an Agent with an empty capability list
means the host descriptor did not advertise any capabilities.

## Boundaries

- Semantic cache access uses the Agent's configured cache adapter and never
  copies the cache into Pulse.
- Journal turns are the compact `state.data[:chat_history]` persisted through
  the Agent's state adapter. They are not the connection event log, a raw state
  dump, or a full audit ledger.
- The Skill inventory is the immutable Agent Definition projection. No module
  atom is created from client input.
- Morph resolves a live Subject-scoped Instance and stops after evaluation. It
  does not approve or activate the candidate.
- OTP monitoring remains a separate temporary stream and publishes only the
  bounded `Process.info/2` allowlist. It never exposes mailbox contents, process
  dictionaries, or GenServer state.

The bridge bounds row counts and text sizes and projects publication-safe fields
only. Exceptions are converted to a generic correlated error instead of
terminating the socket or disclosing host internals.

# Migrate Pulse integration from 0.3 to 0.4

This guide covers integration with Spectre `~> 0.4.0`. Install the compatible
package versions shown in the [README](../README.md), then follow the ownership
and callback changes below. Satellite versions remain independent of core.

## Move ownership, preserve capabilities

| Earlier integration | AgentServer 0.4 |
| --- | --- |
| Default inbound resolves legacy Turn/Instance | Pass `agent_server: existing_server` after authenticated host routing |
| Immediate Turn result returned by ingress | Result carries interaction id, `turn: nil`; consumer receives asynchronous replies |
| Legacy `pulse/2` action DSL | Server gets Stack-generated `pulse/3`; bind send to a host operation |
| Studio/Instance inspectors assumed to cover every runtime | Keep those adapters explicitly scoped; new Server lookup/consumer wiring belongs to the host |

Keep routes in the Agent and move execution callbacks to
`use Spectre.Server, tools: MyApp.AI`. Return the context produced by `pulse`
and handle its tagged result in `handle_result/3`. The
[core migration guide](https://github.com/elchemista/spectre/blob/0.4.0/docs/MIGRATING_TO_0_4.md) covers shared Server, GAM and
checkpoint changes; old Instance snapshots are not AgentServer snapshots.

## What remains available

Envelope, discovery and transport contracts remain applicable. Instance, Work/Vigil and Studio workflow examples in the retained guide use their retained runtime APIs; they are not an automatic AgentServer registry or AgentServer monitoring integration. Use the explicit authenticated AgentServer bridge for new servers.

See the [retained reference](TRANSPORTS_AND_LEGACY_RUNTIME.md) for these APIs and
[0.4 helper/lifecycle contracts](SPECTRE_0_4.md) for the new integration. Migration
is an ownership/API change, not a promise that every legacy convenience now
implicitly routes through AgentServer or GAM.

## Skill Servers

Mounted Skills share their root's GAM Domain and retain separate checkpoints.
Follow the [core Skill guide](https://github.com/elchemista/spectre/blob/0.4.0/docs/SKILLS.md); this package requires no separate
Skill host or second GAM configuration.

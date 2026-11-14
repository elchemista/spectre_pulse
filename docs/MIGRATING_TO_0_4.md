# Migrate Pulse integration from 0.3 to 0.4

This guide targets the current unreleased development branch, not a published
Hex release. Start with the matching Git dependencies in the [README](../README.md).
The package's compatibility requirement is Spectre `~> 0.4.0`; use the core
`feature/agent-server-gam` and satellite `feature/stack-tools-0.4` branches together.

## Move ownership, preserve capabilities

| Earlier integration | AgentServer 0.4 |
| --- | --- |
| Default inbound resolves legacy Turn/Instance | Pass `agent_server: existing_server` after authenticated host routing |
| Immediate Turn result returned by ingress | Result carries interaction id, `turn: nil`; consumer receives asynchronous replies |
| Legacy `pulse/2` action DSL | Server gets Stack-generated `pulse/3`; bind send to a host operation |
| Studio/Instance inspectors assumed to cover every runtime | Keep those adapters explicitly scoped; new Server lookup/consumer wiring belongs to the host |

1. Keep the Stack installation and supported provider/configuration choices.
   Change any dependency pins to the matching branches or reviewed commits.
2. Keep routes/classification declarations in the Agent, set `server: MyApp.Server`,
   and move execution callbacks to `use Spectre.Server, tools: MyApp.AI`.
3. Return the context containing generated helper commands. Handle actual tagged
   results in `handle_result/3`; there is no synchronous helper result and no import
   of invented <code>Spectre.Pulse.pulse/3</code> functions.
4. For consequential work, configure AgentServer allowed operations and consent
   continuations where required. Installing a package does not authorize end-user
   effects. Follow the [GAM manifest](https://spectre.guide/en/governed-act-model).
5. Choose and supervise the actual runtime resources and checkpoint Store. Keep
   handles outside durable assigns; use portable result/state representations.
6. Exercise recovery with your application's real scope, storage and provider
   contracts. A changed checkpoint fingerprint requires an explicit core migration;
   old Instance snapshots are not automatically AgentServer snapshots.

## What remains available

Envelope, discovery and transport contracts remain applicable. Instance, Work/Vigil and Studio workflow examples in the retained guide use their retained runtime APIs; they are not an automatic AgentServer registry or AgentServer monitoring integration. Use the explicit authenticated AgentServer bridge for new servers.

See the [retained reference](TRANSPORTS_AND_LEGACY_RUNTIME.md) for these APIs and
[0.4 helper/lifecycle contracts](SPECTRE_0_4.md) for the new integration. Migration
is an ownership/API change, not a promise that every legacy convenience now
implicitly routes through AgentServer or GAM.

## Skill Servers and the shared GAM

A mounted Skill may have a separate Server, state and checkpoint. It automatically
reuses the root AgentServer's GAM Domain, authority ledger and execution boundary;
no Skill Host, second Domain or manual ingress is part of this migration. Keep
root operation permissions and mount `governed:` references aligned. Proposal
provenance remains recorded, confirmations stay bound to the exact Skill and
presentation, and timer closure cannot erase a Duty. Parallel workers remain
subject to the same kernel admission laws.

See [the package integration guide](SPECTRE_0_4.md#mounted-skill-runtimes) and the
[core Skill ownership and consent contract](https://github.com/elchemista/spectre/blob/feature/agent-server-gam/docs/SKILLS.md).

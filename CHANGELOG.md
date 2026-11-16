# Changelog

## Unreleased — Spectre 0.4

### Added

- Stack-generated protocol/contact inspection and explicitly bound governed sends
  through AgentServer; authenticated inbound delivery to an existing Server.
- `Spectre.Pulse.Approval` carries external human requests, signed presentation
  acknowledgements and decisions, and cancellations over Envelope v1.
- Local subscriptions can bind an existing endpoint to its live AgentServer.
- Confirm/reject integration tests across independent Domains using local delivery
  and authenticated loopback REST.

### Changed

- Pin sender and command identity in governed sends, bound portable errors and
  preserve uncertain delivery outcomes for GAM reconciliation.
- Target Spectre `~> 0.4.0`; CI and contributors can select matching local sources.
- Publish protocol, migration, approval and transport guides with the package.

### Compatibility

- Envelope v1, discovery and transport semantics remain unchanged. A technical
  acceptance receipt never means human consent or remote business completion.
- Retain Instance/Work/Vigil/Studio APIs, connection manifests, scoped Phoenix
  monitoring and Studio inspection. These do not implicitly register AgentServers.
- Hosts continue to own transport authentication, scopes, key provisioning,
  storage policy and Phoenix Endpoint lifecycle.

## 0.3.0 — 2026-08-13

- Align Pulse and the complete ecosystem conformance Stack with Spectre 0.3.0.
- Resolve the core dependency from Hex with `~> 0.3.0`, while the unpublished
  satellite libraries continue to be distributed from GitHub.
- Keep every unpublished satellite dependency explicit on GitHub with no path
  fallback and retain GitHub-only satellite package distribution.
- Migrate the full-ecosystem prompt fixture to Spectre 0.3's closed,
  data-only template renderer.
- Stabilize multi-Run Effect ownership coverage by waiting for the Spectre
  Instance scheduler to release the completed Run before resuming the next.
- Harden every public Pulse boundary so malformed options, restored values,
  callback replies, identities, routes, and transport inputs return typed
  errors instead of leaking exceptions or accepting ambiguous state.
- Lock REST delivery to validated HTTP(S) POST requests with bounded response
  streaming, protected Req options, strict headers, disabled redirects/retries,
  and explicit inbound authentication semantics.
- Make Fabric transport batches atomic, reject duplicate registrations, and
  ensure Effect execution uses only transports compiled into the Agent Stack.
- Validate runtime, directory, network, local subscription, DSL, and Stack
  configuration before side effects while preserving message and route context
  across all failure paths.

## 0.2.0 — 2026-08-01

- Raise Pulse, Stack, and ecosystem dependency contracts to Spectre 0.2.0.
- Verify all seven packages together through the full Agent ecosystem test.
- Preserve protocol v1 and the permanent 0.1.6 envelope compatibility fixture.
- Keep transport and connections host-owned while Spectre owns policy, Effect,
  Run, Work, Vigil, and canonical persistence lifecycle.

## 0.1.6 — 2026-07-31

- Establish a recoverable consolidation baseline with an explicit normative
  public API manifest and uniform release documentation.
- Add no runtime functionality and make no intentional breaking API change.

## 0.1.5

- Bind every outbound Pulse Effect and policy Awaitable to the owning
  `Spectre.Instance` Run.
- Scope pending-Effect checks and selection by `run_id`, allowing several
  outbound Pulse Runs to wait independently in one subject Instance.
- Align all package, Stack, and ecosystem contracts with Spectre 0.1.5.
- Add end-to-end coverage for independent Pulse Invocations and the complete
  seven-package 0.1.5 Stack.

## 0.1.4

- Align the package and Stack manifest with the Spectre 0.1.4 Instance
  contract.
- Allow a trusted inbound context to resolve the core Instance for an explicit
  `AgentRef + Subject`.
- Keep Pulse transport-only: the resolved core Instance remains the sole owner
  of state, scheduling, and concurrent Runs.
- Preserve legacy module and process targets when no Subject is supplied.

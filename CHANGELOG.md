# Changelog

## Unreleased

- Make every Spectre ecosystem dependency an explicit GitHub dependency with
  no Hex or path fallback, and remove Hex package metadata and package build CI.
- Stabilize multi-Run Effect ownership coverage by waiting for the Spectre
  Instance scheduler to release the completed Run before resuming the next.

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

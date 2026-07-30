# Changelog

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

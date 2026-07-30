# Changelog

## 0.1.4

- Align the package and Stack manifest with the Spectre 0.1.4 Instance
  contract.
- Allow a trusted inbound context to resolve the core Instance for an explicit
  `AgentRef + Subject`.
- Keep Pulse transport-only: the resolved core Instance remains the sole owner
  of state, scheduling, and concurrent Runs.
- Preserve legacy module and process targets when no Subject is supplied.

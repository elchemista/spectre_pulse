# Spectre Pulse public API — 0.1.6 baseline

This file is the normative public API manifest for the recoverable `0.1.6`
baseline. Compatibility guarantees apply only to the modules and callables
listed below. Any module, function, macro, or callback not listed here is an
implementation detail even when it is exported or visible in generated docs.

Default arguments are expanded into every callable arity. For the listed
modules, documented types, opaque types, and documented struct fields are also
public. Modules with no callable row expose only their documented module,
type, and struct contract.

## Manifest

- `Spectre.Pulse`
  - functions: `config/1`, `connect/3`, `connect/4`, `contacts/1`, `correlate/2`, `deliver/2`, `disconnect/1`, `envelope/1`, `execute/2`, `execute/3`, `execute_turn/1`, `execute_turn/2`, `find_contacts/2`, `forget_contact/2`, `protocol/0`, `reachability/2`, `reachability/3`, `receive/2`, `receive/3`, `register_transport/2`, `register_transport/3`, `remember_contact/2`, `resolve/2`, `start_link/1`, `subscribe/1`, `subscribe/2`
  - macros: `__using__/0`, `__using__/1`
- `Spectre.Pulse.Address`
  - functions: `agent_id/1`, `equal?/2`, `for_agent/1`, `new/1`, `new/2`, `new!/1`, `new!/2`, `normalize/1`, `normalize/2`, `normalize!/1`, `normalize!/2`
- `Spectre.Pulse.Codec`
  - functions: `decode/2`, `decode/3`, `encode/2`, `encode/3`
  - callbacks: `decode/2`, `encode/2`
- `Spectre.Pulse.Codec.Identity`
- `Spectre.Pulse.Codec.JSON`
- `Spectre.Pulse.Config`
  - functions: `fetch/1`, `new/1`, `new!/1`, `public_identity/1`
- `Spectre.Pulse.Contact`
  - functions: `new/1`, `new/2`, `new/3`, `new!/2`, `new!/3`
- `Spectre.Pulse.ContactBook`
  - functions: `contacts/1`, `delete/2`, `fetch/2`, `find/2`, `merge/1`, `new/0`, `new/1`, `new!/0`, `new!/1`, `put/2`, `resolve/2`, `routes/2`
- `Spectre.Pulse.Control`
  - functions: `describe/2`, `describe/3`, `ping/2`, `ping/3`, `pong/1`, `pong/2`
- `Spectre.Pulse.DSL`
  - macros: `advertise/1`, `contact/2`, `contact/3`, `directory/1`, `flow/2`, `identity/1`, `network/1`, `pulse/1`, `pulse/2`, `pulse_inbound/1`, `pulsing/1`, `state_scope/1`
- `Spectre.Pulse.Directory`
  - functions: `contacts/2`, `resolve/2`, `resolve/3`, `routes/2`, `routes/3`
  - callbacks: `contacts/1`, `resolve/2`, `routes/2`
- `Spectre.Pulse.Directory.Resolution`
- `Spectre.Pulse.Discovery`
  - functions: `directories/0`, `directories/1`, `resolve_identity/2`, `resolve_identity/3`, `routes/1`, `routes/2`
- `Spectre.Pulse.Endpoint`
  - functions: `accept/3`, `accept/4`
  - callbacks: `handle_pulse/3`
- `Spectre.Pulse.Envelope`
  - functions: `new/1`, `new/2`, `new!/1`, `new!/2`, `reply/3`, `reply/4`, `to_wire/1`
- `Spectre.Pulse.Error`
  - functions: `normalize/1`, `normalize/2`, `normalize/3`, `not_sent/2`, `not_sent/3`, `not_sent?/1`, `outcome_unknown/2`, `outcome_unknown/3`, `outcome_unknown?/1`
- `Spectre.Pulse.Executor`
  - functions: `deliver/3`, `deliver/4`, `execute/2`, `execute/3`, `execute_pending/2`, `execute_pending/3`, `execute_turn/1`, `execute_turn/2`
- `Spectre.Pulse.Expectation`
  - functions: `cancel/1`, `expire/2`, `matches?/2`, `new/2`, `new/3`, `new/4`, `resolve/2`
- `Spectre.Pulse.Fabric`
  - functions: `child_spec/1`, `connect/3`, `connect/4`, `disconnect/1`, `register_transport/2`, `register_transport/3`, `routes/1`, `transports/0`
- `Spectre.Pulse.Identity`
  - functions: `new/1`, `new/2`, `new!/1`, `new!/2`, `to_public_map/1`
- `Spectre.Pulse.Inbound`
  - functions: `receive/2`, `receive/3`, `to_input/2`, `to_input/3`
- `Spectre.Pulse.Inbound.Result`
- `Spectre.Pulse.InboundContext`
  - functions: `new/1`
- `Spectre.Pulse.Local`
  - functions: `lookup/1`, `resolve_target/1`, `resolve_target/2`, `routes/1`, `subscribe/1`, `subscribe/2`, `via/1`
- `Spectre.Pulse.Local.Endpoint`
- `Spectre.Pulse.Network`
  - functions: `deliver/2`, `deliver/3`, `probe/2`, `probe/3`
  - callbacks: `deliver/2`, `probe/2`
- `Spectre.Pulse.Network.Routed`
- `Spectre.Pulse.Payload`
  - functions: `new/1`, `new/2`, `new!/1`, `new!/2`, `to_wire/1`
- `Spectre.Pulse.Protocol`
  - functions: `acts/0`, `control_types/0`, `decode_act/1`, `default_limits/0`, `describe/0`, `encode_act/1`, `limits/0`, `limits/1`, `valid_act?/1`, `version/0`
- `Spectre.Pulse.Reachability`
  - functions: `expired?/1`, `expired?/2`, `new/1`, `new/2`, `unknown/0`, `unknown/1`, `unknown/2`
- `Spectre.Pulse.Receipt`
  - functions: `accepted/1`, `accepted/2`, `new/1`, `to_wire/1`
- `Spectre.Pulse.Route`
  - functions: `local/2`, `local/3`, `new/1`, `new!/1`, `node/3`, `node/4`, `pub_sub/2`, `pub_sub/3`, `rest/2`, `rest/3`, `web_socket/2`, `web_socket/3`
- `Spectre.Pulse.Runtime`
  - functions: `child_spec/1`
- `Spectre.Pulse.Stack`
- `Spectre.Pulse.State`
  - functions: `contact_book/1`, `contact_book/2`, `correlate/2`, `expectations/1`, `forget_contact/2`, `forget_expectation/2`, `put_expectation/2`, `remember_contact/2`
- `Spectre.Pulse.Transport`
  - functions: `dispatch/2`, `dispatch/3`, `probe/1`, `probe/2`
  - callbacks: `deliver/3`, `probe/2`
- `Spectre.Pulse.Transports.Local`
  - functions: `handle_message/2`, `handle_message/3`
- `Spectre.Pulse.Transports.Node`
- `Spectre.Pulse.Transports.PubSub`
  - functions: `handle_message/2`, `handle_message/3`, `handle_message/4`
- `Spectre.Pulse.Transports.REST`
  - functions: `handle_request/3`, `handle_request/4`
- `Spectre.Pulse.Transports.REST.Response`
- `Spectre.Pulse.Transports.WebSocket`
  - functions: `handle_frame/2`, `handle_frame/3`
- `Spectre.Pulse.Validator`
  - functions: `valid_id?/1`, `validate/1`, `validate/2`

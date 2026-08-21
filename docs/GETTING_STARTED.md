# Getting started with Spectre Pulse

This guide covers Agent authoring and local Agent-to-Agent communication. For a
network connection start with the [Phoenix guide](PHOENIX.md) or the
[connections and transports guide](CONNECTIONS_AND_TRANSPORTS.md).

## Choose how Pulse is installed in an Agent

Pulse implements `Spectre.Stack.Installable`. A Stack is the preferred place to
select the logical Pulse adapters shared by several Agents:

```elixir
defmodule MyApp.AI do
  use Spectre.Stack

  install Spectre.Pulse do
    transport(:local, Spectre.Pulse.Transports.Local)
    directory(MyApp.AgentDirectory)
  end
end

defmodule MyApp.Agent do
  use Spectre.Agent, stack: MyApp.AI
end
```

Selecting the Stack binds Pulse configuration, the `pulse(...)` Flow handler,
and the `:pulse` effect executor. Add `use Spectre.Pulse` after
`use Spectre.Agent` when the Agent also needs the `pulsing`, `identity`,
`contact`, `advertise`, or inbound authoring DSL:

```elixir
defmodule MyApp.Agent do
  use Spectre.Agent, stack: MyApp.AI
  use Spectre.Pulse
end
```

The Stack contains no PID, URL, socket, credential, or physical route. The host
application still starts one Pulse runtime so it can discover and subscribe
Pulse-enabled Agents.

## Build two local Agents

The receiver declares a logical identity and maps one Pulse payload type to an
ordinary Spectre flow:

```elixir
defmodule MyApp.Researcher do
  use Spectre.Agent
  use Spectre.Pulse

  pulsing do
    identity("spectre://acme/researcher")
    advertise(capabilities: [:research])
  end

  flow :remote_requests do
    on :research, pulse: "research.perform" do
      run(:research)
    end
  end

  def research(input, _context) do
    %{
      "status" => "accepted",
      "topic" => input.text,
      "message_id" => input.meta.pulse.message_id
    }
  end
end
```

`pulse: "research.perform"` compiles to a normal deterministic Spectre metadata
check. Pulse does not add a parallel semantic router.

The sender stores only the recipient's logical address:

```elixir
defmodule MyApp.Coordinator do
  use Spectre.Agent
  use Spectre.Pulse

  pulsing do
    identity("spectre://acme/coordinator")

    contact(:researcher, "spectre://acme/researcher",
      display_name: "Researcher",
      capabilities: [:research]
    )
  end

  flow :delegation do
    on :delegate, regex: ~r/^research:/ do
      pulse(:researcher,
        act: :request,
        type: "research.perform",
        build: :research_request,
        expect: "research.completed"
      )
    end
  end

  def research_request(input, _context) do
    %{"topic" => String.replace_prefix(input.text, "research:", "")}
  end
end
```

No transport appears in either Agent. Moving `MyApp.Researcher` from the local
node to WebSocket, REST, or a custom transport does not change the Agent code or
payload meaning.

## Start the runtime

Start one Pulse runtime in the host supervision tree:

```elixir
def start(_type, _args) do
  children = [
    {Spectre.Pulse, []}
  ]

  Supervisor.start_link(children,
    strategy: :one_for_one,
    name: MyApp.Supervisor
  )
end
```

Do not maintain an application-wide Agent list. Pulse discovers modules using
`Spectre.Pulse`, validates that their explicit identities are unique, and creates
their local subscriptions. Agent allow-lists belong to individual connection
definitions because different connections may expose different subsets.

## Send the staged Effect

`pulse/2` does not perform I/O while Spectre is routing. It stages a normal
`%Spectre.Effect{kind: :pulse, name: :send}`:

```elixir
{:ok, turn} = Spectre.turn(MyApp.Coordinator, "research:nautical market")
{:needs, effect, staged_result} = turn.decision

effect.payload.to
# => "spectre://acme/researcher"
```

Use the ordinary Spectre execution boundary to deliver it:

```elixir
{:ok, final_result} = Spectre.execute(MyApp.Coordinator, staged_result)
[%Spectre.Effect{status: :completed} = completed] = final_result.effects

completed.result.via
# => :local
```

`Spectre.execute/3` owns the normal policy and persistence lifecycle. Pulse does
not add another store. `Spectre.Pulse.execute/3` and `execute_turn/2` remain thin
compatibility aliases.

The repository includes this flow as an executable example:

```console
mix run examples/local_agents.exs
```

## Identity

`identity/1` is optional. Without it Pulse derives a stable 128-bit logical
address from the Agent module:

```elixir
Spectre.Pulse.Address.for_agent(MyApp.Coordinator)
# => "spectre://pulse/4f...32-lowercase-hex-digits..."
```

Use an explicit address for a public human-readable identity or when the
identity must survive a module rename. An address is not an authentication
credential.

## Contacts and directories

Contacts are Agent-owned logical values. Physical routes remain infrastructure:

```elixir
Spectre.Pulse.contacts({MyApp.Coordinator, state})
Spectre.Pulse.resolve({MyApp.Coordinator, state}, :researcher)
Spectre.Pulse.find_contacts({MyApp.Coordinator, state}, capability: :research)
Spectre.Pulse.remember_contact(state, contact)
Spectre.Pulse.forget_contact(state, :researcher)
```

An application directory can resolve service-like names:

```elixir
defmodule MyApp.AgentDirectory do
  @behaviour Spectre.Pulse.Directory

  @impl true
  def resolve(:researcher, _opts),
    do: {:ok, "spectre://services/researcher"}

  def resolve(_reference, _opts), do: :error
end
```

Configure the contract, not its routes, in the Agent:

```elixir
pulsing do
  directory(MyApp.AgentDirectory)
end
```

At delivery time Pulse merges local subscriptions, connected routes, and routes
provided by the directory.

## Route inbound work to a Subject-scoped Instance

An authenticated inbound binding can target the Spectre Instance selected by an
explicit `AgentRef + Subject`. The trusted host supplies the Subject; Pulse never
infers it from a sender, address, or conversation:

```elixir
{:ok, inbound} =
  Spectre.Pulse.receive(envelope, %{
    authenticated_identity: envelope.from,
    target: MyApp.Researcher,
    subject: Spectre.Subject.new(customer_id),
    instance_supervisor: MyApp.SpectreSupervisor
  })

%Spectre.Instance.Ref{} = Spectre.Instance.ref(inbound.target)
```

The Instance owns state, mailbox fairness, Runs, and inference lifecycle. Pulse
only authenticates, validates, resolves, and maps the inbound envelope. If
`:subject` is omitted, module and process target behavior remains available.

Outbound Pulse Effects staged inside an Instance inherit the current `run_id`.
Independent Subject Runs can therefore wait on independent Pulse Invocations or
policy gates.

## Correlation and expectations

A response is a new envelope whose `relates_to` references an earlier message.
It never mutates or synchronously completes the original envelope:

```elixir
case Spectre.Pulse.correlate(state, incoming_envelope) do
  {:ok, new_state, resolved_expectation} ->
    persist(new_state, resolved_expectation)

  :unmatched ->
    :ok
end
```

`expect:` stores a pure `%Spectre.Pulse.Expectation{}` in the sender's
`Spectre.State`. It does not create a remote task, timer, room, or session.

## Next steps

- Connect Studio or another system through [Phoenix WebSocket](PHOENIX.md).
- Control Agent runtime observation through [Studio monitoring](STUDIO_MONITORING.md).
- Add remote routes through [connections and transports](CONNECTIONS_AND_TRANSPORTS.md).
- Review [delivery and security](DELIVERY_AND_SECURITY.md) before production exposure.

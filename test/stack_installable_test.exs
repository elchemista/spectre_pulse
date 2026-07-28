defmodule Spectre.Pulse.StackInstallableTest.Directory do
  @moduledoc false

  def resolve(:receiver, _opts), do: {:ok, "spectre://stack/receiver"}

  def routes(address, opts) do
    case Keyword.get(opts, :test_pid) do
      nil ->
        []

      pid ->
        [
          %{
            id: "stack-test-route",
            address: address,
            transport: Spectre.Pulse.StackInstallableTest.Transport,
            target: pid
          }
        ]
    end
  end
end

defmodule Spectre.Pulse.StackInstallableTest.Transport do
  @moduledoc false

  @behaviour Spectre.Pulse.Transport

  @impl true
  def deliver(route, envelope, _opts) do
    send(route.target, {:stack_pulse_delivered, envelope})

    {:ok,
     Spectre.Pulse.Receipt.accepted(envelope.id,
       via: :stack_test,
       route_id: route.id
     )}
  end
end

defmodule Spectre.Pulse.StackInstallableTest.Stack do
  @moduledoc false

  use Spectre.Stack, id: :pulse_contract_test

  install Spectre.Pulse do
    transport(:local, Spectre.Pulse.Transports.Local)
    transport(:rest, Spectre.Pulse.Transports.REST)
    transport(:stack_test, Spectre.Pulse.StackInstallableTest.Transport)
    directory(Spectre.Pulse.StackInstallableTest.Directory)
  end
end

defmodule Spectre.Pulse.StackInstallableTest.Agent do
  @moduledoc false

  use Spectre.Agent, stack: Spectre.Pulse.StackInstallableTest.Stack

  flow :outbound do
    on :send, regex: ~r/^send$/ do
      pulse(:receiver, type: "stack.message", data: %{source: :stack})
    end
  end
end

defmodule Spectre.Pulse.StackInstallableTest do
  use ExUnit.Case, async: true

  alias Spectre.Pulse.Address
  alias Spectre.Pulse.Stack, as: StackAdapter
  alias Spectre.Pulse.StackInstallableTest.Agent
  alias Spectre.Pulse.StackInstallableTest.Directory
  alias Spectre.Pulse.StackInstallableTest.Stack
  alias Spectre.Stack.Contract.V1
  alias Spectre.Stack.Definition
  alias Spectre.Stack.Ref
  alias Spectre.Stack.Runtime

  test "publishes the common Stack contract" do
    assert {:ok, package} = V1.verify_installable(Spectre.Pulse)
    assert package.id == :pulse
    assert package.version == "0.1.2"
    assert package.contract == 1
    assert package.spectre == "~> 0.1.2"
    assert package.dsl == StackAdapter
    assert package.operations == []
    assert package.actions == []
    assert package.resources == []
    assert package.agent_extensions == [Spectre.Pulse.Extension]
    assert {:ok, []} = Runtime.child_specs(Stack)
  end

  test "compiles package-local transport and directory declarations" do
    assert {:ok, installation} = Definition.installation(Stack, :pulse)

    assert installation.config == %{
             transports: [
               %{id: :local, module: Spectre.Pulse.Transports.Local},
               %{id: :rest, module: Spectre.Pulse.Transports.REST},
               %{
                 id: :stack_test,
                 module: Spectre.Pulse.StackInstallableTest.Transport
               }
             ],
             directory: Directory
           }

    assert {:ok, %Ref{package: :pulse}} =
             Definition.resolve(Stack, :contract, {:pulse, 1})

    assert {:ok, %Ref{package: :pulse}} = Definition.resolve(Stack, :service, :pulse)
  end

  test "selecting the Stack activates Pulse without a second use" do
    assert Spectre.Definition.fetch!(Agent).stack == Stack
    assert {:ok, config} = Spectre.Pulse.config(Agent)
    assert config.identity == Address.for_agent(Agent)
    assert config.directory == Directory

    assert [
             %Spectre.Effect.Executor.Mount{
               kind: :pulse,
               module: Spectre.Pulse.EffectExecutor
             }
           ] = Spectre.EffectConfig.executors(Agent)
  end

  test "Stack-only Pulse handlers execute through the core effect lifecycle" do
    assert {:ok, turn} = Spectre.turn(Agent, "send")
    assert {:needs, %Spectre.Effect{kind: :pulse} = effect, staged} = turn.decision
    assert effect.payload.type == "stack.message"

    assert {:ok, completed} = Spectre.execute(Agent, staged, test_pid: self())
    assert [%Spectre.Effect{kind: :pulse, status: :completed} = terminal] = completed.effects
    assert %Spectre.Pulse.Receipt{via: :stack_test} = terminal.result
    assert completed.state.pending_effects == []
    effect_id = effect.id
    assert_receive {:stack_pulse_delivered, %{id: ^effect_id}}
  end

  test "rejects duplicate package-local declarations" do
    assert_raise ArgumentError, ~r/duplicate_pulse_transport/, fn ->
      Code.compile_string("""
      defmodule Spectre.Pulse.StackInstallableTest.DuplicateStack do
        use Spectre.Stack

        install Spectre.Pulse do
          transport :local, Spectre.Pulse.Transports.Local
          transport :local, Spectre.Pulse.Transports.REST
        end
      end
      """)
    end
  end
end

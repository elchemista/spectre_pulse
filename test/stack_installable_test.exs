defmodule Spectre.Pulse.StackInstallableTest.Directory do
  @moduledoc false
end

defmodule Spectre.Pulse.StackInstallableTest.Stack do
  @moduledoc false

  use Spectre.Stack, id: :pulse_contract_test

  install Spectre.Pulse do
    transport(:local, Spectre.Pulse.Transports.Local)
    transport(:rest, Spectre.Pulse.Transports.REST)
    directory(Spectre.Pulse.StackInstallableTest.Directory)
  end
end

defmodule Spectre.Pulse.StackInstallableTest.Agent do
  @moduledoc false

  use Spectre.Agent, stack: Spectre.Pulse.StackInstallableTest.Stack
  use Spectre.Pulse
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
    assert {:ok, []} = Runtime.child_specs(Stack)
  end

  test "compiles package-local transport and directory declarations" do
    assert {:ok, installation} = Definition.installation(Stack, :pulse)

    assert installation.config == %{
             transports: [
               %{id: :local, module: Spectre.Pulse.Transports.Local},
               %{id: :rest, module: Spectre.Pulse.Transports.REST}
             ],
             directory: Directory
           }

    assert {:ok, %Ref{package: :pulse}} =
             Definition.resolve(Stack, :contract, {:pulse, 1})

    assert {:ok, %Ref{package: :pulse}} = Definition.resolve(Stack, :service, :pulse)
  end

  test "binds the Stack logically without changing the existing Agent integration" do
    assert Spectre.Definition.fetch!(Agent).stack == Stack
    assert {:ok, config} = Spectre.Pulse.config(Agent)
    assert config.identity == Address.for_agent(Agent)
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

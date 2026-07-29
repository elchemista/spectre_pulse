defmodule Spectre.Pulse.EcosystemStackIntegrationTest.Adapters do
  @moduledoc false

  defmodule Inference do
    @moduledoc false
  end

  defmodule FastClassifier do
    @moduledoc false
  end

  defmodule MemoryStore do
    @moduledoc false
  end

  defmodule ContinuityStore do
    @moduledoc false
  end

  defmodule Clock do
    @moduledoc false
  end

  defmodule Browser do
    @moduledoc false
  end

  defmodule WebPolicy do
    @moduledoc false
  end

  defmodule Telegram do
    @moduledoc false
  end

  defmodule WhatsApp do
    @moduledoc false
  end

  defmodule Directory do
    @moduledoc false
  end
end

defmodule Spectre.Pulse.EcosystemStackIntegrationTest.Stack do
  @moduledoc false

  alias Spectre.Pulse.EcosystemStackIntegrationTest.Adapters

  use Spectre.Stack, id: :ecosystem_contract

  install Spectre.Prism do
    provider(:openrouter, Adapters.Inference)
    model(:fast, id: "small-model")
  end

  install Spectre.Kinetic do
    classifier(Adapters.FastClassifier)
  end

  install Spectre.Mnemonic do
    store(Adapters.MemoryStore)
    isolate_by([:agent, :subject, :conversation, :flow, :task])
  end

  install Spectre.Directive do
    store(Adapters.ContinuityStore)
    clock(Adapters.Clock)
    resident_runs(16)
  end

  install Spectre.Lens do
    backend(Adapters.Browser)
    policy(Adapters.WebPolicy)
  end

  install Spectre.Beam do
    channel(:telegram, Adapters.Telegram)
    channel(:whatsapp, Adapters.WhatsApp)
  end

  install Spectre.Pulse do
    transport(:local, Spectre.Pulse.Transports.Local)
    directory(Adapters.Directory)
  end
end

defmodule Spectre.Pulse.EcosystemStackIntegrationTest.Agent do
  @moduledoc false

  use Spectre.Agent, stack: Spectre.Pulse.EcosystemStackIntegrationTest.Stack
end

defmodule Spectre.Pulse.EcosystemStackIntegrationTest do
  use ExUnit.Case, async: true

  alias Spectre.Pulse.EcosystemStackIntegrationTest.Adapters
  alias Spectre.Pulse.EcosystemStackIntegrationTest.Agent
  alias Spectre.Pulse.EcosystemStackIntegrationTest.Stack
  alias Spectre.Stack.Definition
  alias Spectre.Stack.Runtime

  @package_ids [:prism, :kinetic, :mnemonic, :directive, :lens, :beam, :pulse]

  test "compiles every ecosystem package through one generic Stack protocol" do
    definition = Definition.fetch!(Stack)

    assert Enum.map(definition.installations, & &1.package.id) == @package_ids
    assert Enum.all?(definition.installations, &is_binary(&1.digest))
    assert Enum.all?(definition.installations, &(&1.package.contract == 1))
  end

  test "keeps repeated DSL words package-local and preserves every configuration" do
    assert {:ok, mnemonic} = Definition.installation(Stack, :mnemonic)
    assert {:ok, directive} = Definition.installation(Stack, :directive)
    assert {:ok, pulse} = Definition.installation(Stack, :pulse)

    assert mnemonic.config.store == Adapters.MemoryStore
    assert directive.config.store == Adapters.ContinuityStore
    assert pulse.config.directory == Adapters.Directory
  end

  test "binds every package adapter and exposes caller-owned runtime resources" do
    agent_definition = Spectre.Definition.fetch!(Agent)
    assert agent_definition.stack == Stack

    assert Enum.map(agent_definition.extensions, & &1.id) ==
             [:prism, :kinetic, :mnemonic, :directive, :lens, :beam, :pulse]

    assert {:ok, [lens_runtime]} = Runtime.child_specs(Stack)
    assert {SpectreLens.Runtime, :start_link, [[backend: Adapters.Browser]]} = lens_runtime.start

    for {service, package} <- [
          kinetic: :kinetic,
          memory: :mnemonic,
          continuity: :directive,
          lens: :lens,
          pulse: :pulse
        ] do
      assert {:ok, reference} = Definition.resolve(Stack, :service, service)
      assert reference.package == package
    end
  end
end

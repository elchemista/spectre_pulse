defmodule Spectre.Pulse.FullAgentEcosystemTest.Model do
  @moduledoc false

  def complete(prompt, opts) do
    if pid = Keyword.get(opts, :test_pid) do
      send(pid, {:prism_completed, prompt, Keyword.fetch!(opts, :model)})
    end

    {:ok, ~s(Release plan ready.\n<al>RECORD AUDIT SUBJECT="release" DETAIL="ready"</al>)}
  end
end

defmodule Spectre.Pulse.FullAgentEcosystemTest.Actions do
  @moduledoc false

  use SpectreKinetic

  @al ~s(RECORD AUDIT SUBJECT="release" DETAIL="ready")
  @doc "Records a release audit decision."
  @spec record(String.t(), String.t()) :: {:ok, String.t()}
  def record(subject, detail) do
    if pid = Process.whereis(Spectre.Pulse.FullAgentEcosystemTest.Probe) do
      send(pid, {:kinetic_executed, subject, detail})
    end

    {:ok, "audit recorded for #{subject}:#{detail}"}
  end
end

defmodule Spectre.Pulse.FullAgentEcosystemTest.LensProtocol do
  @moduledoc false

  @behaviour SpectreLens.Protocol

  alias SpectreLens.Browser.Instance
  alias SpectreLens.PageMap
  alias SpectreLens.Region
  alias SpectreLens.Runtime
  alias SpectreLens.Tab

  @impl true
  def new_tab(%Instance{} = instance, opts) do
    id = "release-tab-#{System.unique_integer([:positive, :monotonic])}"
    send(instance.owner, {:lens_tab_opened, instance.id, id})

    {:ok,
     %Tab{
       id: id,
       target_id: id,
       handle: %{owner: instance.owner},
       protocol: __MODULE__,
       instance_id: instance.id,
       url_policy: SpectreLens.URLPolicy.take_options(opts)
     }}
  end

  @impl true
  def close_tab(%Tab{} = tab) do
    send(tab.handle.owner, {:lens_tab_closed, tab.instance_id, tab.id})
    if is_pid(tab.runtime), do: Runtime.release_tab(tab.runtime, tab)
    :ok
  end

  @impl true
  def command(_tab, method, params, _opts), do: {:ok, %{method: method, params: params}}

  @impl true
  def navigate(%Tab{} = tab, url, _opts) do
    send(tab.handle.owner, {:lens_navigated, tab.id, url})
    :ok
  end

  @impl true
  def evaluate(_tab, expression, _opts), do: {:ok, %{expression: expression}}

  @impl true
  def url(_tab), do: {:ok, "https://release.test/status"}

  @impl true
  def title(_tab), do: {:ok, "Release status"}

  @impl true
  def html(_tab, _opts), do: {:ok, "<main><h1>Release ready</h1></main>"}

  @impl true
  def markdown(_tab, _opts), do: {:ok, "# Release ready"}

  @impl true
  def semantic_tree(_tab, _opts),
    do: {:ok, %{"nodes" => [%{"role" => "main", "name" => "Release ready"}]}}

  @impl true
  def interactive_elements(_tab, _opts), do: {:ok, []}

  @impl true
  def structured_data(_tab, _opts), do: {:ok, %{"status" => "ready"}}

  @impl true
  def page_map(_tab, _opts) do
    {:ok,
     %PageMap{
       description: "The release status page reports ready.",
       regions: [
         %Region{
           id: "release",
           kind: :main,
           purpose: :content_section,
           text: "Release ready",
           selector: "main"
         }
       ]
     }}
  end

  @impl true
  def focus(tab, _ref, opts), do: page_map(tab, opts)

  @impl true
  def links(_tab, _opts),
    do: {:ok, [%{"href" => "https://release.test/notes", "text" => "Release notes"}]}

  @impl true
  def forms(_tab, _opts), do: {:ok, []}

  @impl true
  def screenshot(_tab, _opts), do: {:ok, "release-png"}

  @impl true
  def pdf(_tab, _opts), do: {:ok, "release-pdf"}

  @impl true
  def click(_tab, _ref, _opts), do: :ok

  @impl true
  def fill(_tab, _ref, _value, _opts), do: :ok

  @impl true
  def submit(_tab, _ref, _fields, _opts), do: :ok

  @impl true
  def wait_for_selector(_tab, _selector, _opts), do: :ok

  @impl true
  def wait_for_navigation(_tab, callback, _opts) do
    callback.()
    :ok
  end

  @impl true
  def scroll(_tab, _opts), do: :ok
end

defmodule Spectre.Pulse.FullAgentEcosystemTest.LensBackend do
  @moduledoc false

  @behaviour SpectreLens.Browser

  alias Spectre.Pulse.FullAgentEcosystemTest.LensProtocol
  alias SpectreLens.Browser.Instance

  @impl true
  def default_protocol, do: LensProtocol

  @impl true
  def start_instance(index, opts) do
    owner = Keyword.get(opts, :test_pid, self())
    send(owner, {:lens_instance_started, index})

    {:ok,
     %Instance{
       id: index,
       backend: __MODULE__,
       protocol: Keyword.fetch!(opts, :protocol),
       owner: owner,
       metadata: %{engine: :ecosystem_test}
     }}
  end

  @impl true
  def stop_instance(%Instance{id: id, owner: owner}) do
    send(owner, {:lens_instance_stopped, id})
    :ok
  end

  @impl true
  def max_tabs(_instance, opts), do: Keyword.get(opts, :max_tabs_per_instance, 2)
end

defmodule Spectre.Pulse.FullAgentEcosystemTest.BeamAdapter do
  @moduledoc false

  @behaviour Spectre.Beam.Channel

  alias Spectre.Beam.Receipt

  @impl true
  def capabilities(_opts), do: MapSet.new([:text])

  @impl true
  def decode(_event, _opts), do: {:error, :inbound_not_used}

  @impl true
  def deliver(outbound, opts) do
    send(Keyword.fetch!(opts, :test_pid), {:beam_delivered, outbound})

    {:ok,
     Receipt.accepted(outbound,
       provider_message_id: "beam-#{outbound.idempotency_key}"
     )}
  end
end

defmodule Spectre.Pulse.FullAgentEcosystemTest.PulseDirectory do
  @moduledoc false

  @behaviour Spectre.Pulse.Directory

  alias Spectre.Pulse.FullAgentEcosystemTest.PulseTransport

  @impl true
  def resolve(:operations, _opts), do: {:ok, "spectre://ecosystem/operations"}

  @impl true
  def routes(address, opts) do
    [
      %{
        id: "full-agent-route",
        address: address,
        transport: PulseTransport,
        target: Keyword.fetch!(opts, :test_pid)
      }
    ]
  end
end

defmodule Spectre.Pulse.FullAgentEcosystemTest.PulseTransport do
  @moduledoc false

  @behaviour Spectre.Pulse.Transport

  alias Spectre.Pulse.Receipt

  @impl true
  def deliver(route, envelope, _opts) do
    send(route.target, {:pulse_delivered, envelope})

    {:ok,
     Receipt.accepted(envelope.id,
       via: :full_agent_test,
       route_id: route.id
     )}
  end
end

defmodule Spectre.Pulse.FullAgentEcosystemTest.Stack do
  @moduledoc false

  alias Spectre.Pulse.FullAgentEcosystemTest.Actions
  alias Spectre.Pulse.FullAgentEcosystemTest.BeamAdapter
  alias Spectre.Pulse.FullAgentEcosystemTest.LensBackend
  alias Spectre.Pulse.FullAgentEcosystemTest.Model
  alias Spectre.Pulse.FullAgentEcosystemTest.PulseDirectory
  alias Spectre.Pulse.FullAgentEcosystemTest.PulseTransport

  use Spectre.Stack, id: :full_agent_ecosystem

  install Spectre.Prism do
    provider(:local, Model)
    model(:fast, id: "ecosystem-model")
    default(:fast)
    selector(Spectre.Prism.Selector.Rules)
  end

  install(Spectre.Kinetic,
    mode: :closed_moves,
    actions: Actions,
    modes: [record: :write]
  )

  install Spectre.Mnemonic, namespace: :full_agent_ecosystem do
    isolate_by([:agent, :conversation])
  end

  install(Spectre.Directive)

  install Spectre.Lens do
    backend(LensBackend,
      instances: 1,
      max_tabs_per_instance: 2,
      network_policy: :any
    )
  end

  install Spectre.Beam do
    channel(:notifications,
      adapter: BeamAdapter,
      type: :test,
      capabilities: [:text]
    )
  end

  install Spectre.Pulse do
    transport(:full_agent_test, PulseTransport)
    directory(PulseDirectory)
  end
end

defmodule Spectre.Pulse.FullAgentEcosystemTest.Agent do
  @moduledoc false

  use Spectre.Agent,
    stack: Spectre.Pulse.FullAgentEcosystemTest.Stack,
    prompt_root: "test/fixtures/prompts"

  router(via: [:regex], semantic_cache?: false, classification_log?: false)

  flow :release_plan do
    on :plan_release, regex: ~r/^plan release$/i do
      ask(:orchestrate)
    end
  end

  flow :release_inspection do
    on :inspect_release, regex: ~r/^inspect release$/i do
      action({:lens, :look},
        args: %{
          url: "https://release.test/status",
          opts: [include: [:markdown, :links]]
        },
        mode: :read
      )
    end
  end

  flow :release_channel_notification do
    on :notify_channel, regex: ~r/^notify channel$/i do
      beam("release-room",
        via: :notifications,
        text: "release ready"
      )
    end
  end

  flow :release_agent_notification do
    on :notify_agent, regex: ~r/^notify agent$/i do
      pulse(:operations,
        type: "release.ready",
        data: %{status: "ready"}
      )
    end
  end
end

defmodule Spectre.Pulse.FullAgentEcosystemTest do
  use ExUnit.Case, async: false

  alias Spectre.Beam.Store, as: BeamStore
  alias Spectre.Invocation
  alias Spectre.Mnemonic.Memory
  alias Spectre.Pulse.FullAgentEcosystemTest.Agent
  alias Spectre.Pulse.FullAgentEcosystemTest.Probe
  alias Spectre.Pulse.FullAgentEcosystemTest.Stack
  alias Spectre.Result
  alias Spectre.Run
  alias Spectre.Run.Boundary
  alias Spectre.Runtime
  alias Spectre.Stack.Definition
  alias SpectreDirective.Outcome
  alias SpectreDirective.Request

  setup do
    mnemonic_namespace = "spectre_pulse_full_agent_test"

    mnemonic_root =
      Path.join(
        System.tmp_dir!(),
        "spectre-pulse-full-agent-#{System.unique_integer([:positive])}"
      )

    Application.put_env(:spectre_mnemonic, :namespace, mnemonic_namespace)
    Application.put_env(:spectre_mnemonic, :data_root, mnemonic_root)

    for application <- [
          :spectre_beam,
          :spectre_directive,
          :spectre_kinetic,
          :spectre_lens,
          :spectre_mnemonic,
          :spectre_prism
        ] do
      assert {:ok, _started} = Application.ensure_all_started(application)
    end

    Process.register(self(), Probe)
    :ok = BeamStore.reset()

    on_exit(fn ->
      if Enum.any?(Application.started_applications(), &(elem(&1, 0) == :spectre_mnemonic)) do
        Application.stop(:spectre_mnemonic)
      end

      Application.delete_env(:spectre_mnemonic, :namespace)
      Application.delete_env(:spectre_mnemonic, :data_root)
      File.rm_rf!(mnemonic_root)
    end)

    {:ok, mnemonic_namespace: mnemonic_namespace}
  end

  test "one Stack-bound agent executes the complete 0.1.4 ecosystem", context do
    assert Enum.map(Definition.fetch!(Stack).installations, fn installation ->
             {installation.package.id, installation.package.version}
           end) == [
             prism: "0.1.4",
             kinetic: "0.1.4",
             mnemonic: "0.1.4",
             directive: "0.1.4",
             lens: "0.1.4",
             beam: "0.1.4",
             pulse: "0.1.4"
           ]

    assert {:ok, stack_runtime} =
             Spectre.Stack.Runtime.start_link(
               Stack,
               packages: [lens: [test_pid: self()]]
             )

    assert_receive {:lens_instance_started, 1}

    namespace = context.mnemonic_namespace
    conversation_id = "release-0.1.4-#{System.unique_integer([:positive])}"
    memory_marker = "Only notify after the release page reports ready."

    runtime_opts = [
      namespace: namespace,
      conversation_id: conversation_id,
      test_pid: self()
    ]

    assert {:ok, directive} =
             Spectre.Directive.new(
               mission: "Validate and announce release 0.1.4",
               success: "Every ecosystem boundary completes",
               mode: :autonomous,
               execution: :manual
             )

    assert {:request, %Request{kind: :reason} = plan_request, directive} =
             Spectre.Directive.next(directive)

    assert {:request, %Request{kind: :reason} = mission_step, directive} =
             Spectre.Directive.respond(
               directive,
               plan_request.id,
               {:propose_plan, [%{id: "release", title: "Run the release agent"}]}
             )

    assert {:ok, _packet} =
             SpectreMnemonic.remember(memory_marker,
               namespace: namespace,
               stream: :full_agent,
               scope:
                 {:spectre,
                  [
                    agent: Agent,
                    conversation: conversation_id
                  ]},
               persist?: false
             )

    assert {:continue, run} = Runtime.start(Agent, "plan release", runtime_opts)
    assert {:ok, checkpoint} = Run.checkpoint(run)
    assert {:ok, restored} = Run.restore(checkpoint)
    assert restored.id == run.id
    assert {:ok, %{options: []}} = Spectre.Directive.config(Agent)

    assert {:await, %Invocation{operation: {:action, :record}} = invocation, awaiting} =
             Runtime.advance(restored, runtime_opts)

    assert_receive {:prism_completed, prompt, "ecosystem-model"}
    assert prompt =~ memory_marker
    refute_received {:kinetic_executed, _, _}

    assert {:ok, awaiting_checkpoint} = Run.checkpoint(awaiting)
    assert {:ok, recovered} = Run.restore(awaiting_checkpoint)

    assert {:boundary, %Boundary{kind: :reply, output: "audit recorded for release:ready"},
            replied} =
             Runtime.resume(recovered, {:execute, invocation.id}, runtime_opts)

    assert_receive {:kinetic_executed, "release", "ready"}
    assert Result.action_outcome(replied.result) == {:ok, "audit recorded for release:ready"}

    assert {:complete, planned, completed_run} = Runtime.advance(replied, runtime_opts)
    assert completed_run.status == :complete
    assert Result.action_outcome(planned) == {:ok, "audit recorded for release:ready"}

    assert {:ok, recalled} =
             Memory.recall("plan release",
               agent: Agent,
               namespace: namespace,
               conversation: conversation_id
             )

    assert Enum.any?(recalled.moments, &String.contains?(&1.text, "plan release"))

    assert {:ok, lens_turn} = Spectre.turn(Agent, "inspect release", runtime_opts)
    assert {:needs, %Spectre.Effect{kind: :action}, lens_staged} = lens_turn.decision

    assert {:ok, lens_completed} =
             Spectre.execute(
               Agent,
               lens_staged,
               Keyword.merge(runtime_opts,
                 stack_runtime: stack_runtime,
                 lens_opts: [network_policy: :any]
               )
             )

    assert {:ok, %SpectreLens.View{} = view} = Result.action_outcome(lens_completed)
    assert view.url == "https://release.test/status"
    assert view.title == "Release status"
    assert view.markdown == "# Release ready"
    assert Enum.any?(view.links, &(&1["text"] == "Release notes"))

    assert_receive {:lens_tab_opened, 1, lens_tab_id}
    assert_receive {:lens_navigated, ^lens_tab_id, "https://release.test/status"}
    assert_receive {:lens_tab_closed, 1, ^lens_tab_id}

    assert {:ok, beam_turn} = Spectre.turn(Agent, "notify channel", runtime_opts)
    assert {:needs, %Spectre.Effect{kind: :action}, beam_staged} = beam_turn.decision

    assert {:ok, beam_completed} =
             Spectre.execute(
               Agent,
               beam_staged,
               Keyword.put(runtime_opts, :adapter_opts, test_pid: self())
             )

    assert {:ok, %Spectre.Beam.Receipt{status: :accepted}} =
             Result.action_outcome(beam_completed)

    assert_receive {:beam_delivered, beam_outbound}
    assert beam_outbound.endpoint == :notifications
    assert beam_outbound.to == "release-room"
    assert beam_outbound.content.text == "release ready"

    assert {:ok, pulse_turn} = Spectre.turn(Agent, "notify agent", runtime_opts)
    assert {:needs, %Spectre.Effect{kind: :pulse}, pulse_staged} = pulse_turn.decision

    assert {:ok, pulse_completed} = Spectre.execute(Agent, pulse_staged, runtime_opts)

    assert [
             %Spectre.Effect{
               kind: :pulse,
               status: :completed,
               result: %Spectre.Pulse.Receipt{
                 status: :accepted,
                 via: :full_agent_test
               }
             }
           ] = pulse_completed.effects

    assert_receive {:pulse_delivered, envelope}
    assert envelope.to == "spectre://ecosystem/operations"
    assert envelope.payload.type == "release.ready"
    assert envelope.payload.data == %{status: "ready"}

    assert {:request, %Request{kind: :reason} = review, directive} =
             Spectre.Directive.respond(
               directive,
               mission_step.id,
               {:complete_step, %{packages: 7}}
             )

    assert {:done, %Outcome{status: :completed, result: outcome}, finished} =
             Spectre.Directive.respond(
               directive,
               review.id,
               {:complete_mission, %{release: "0.1.4", packages: 7}}
             )

    assert outcome == %{release: "0.1.4", packages: 7}
    assert finished.status == :completed

    Supervisor.stop(stack_runtime)
  end
end

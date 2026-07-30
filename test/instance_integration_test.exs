defmodule Spectre.Pulse.InstanceIntegrationTest.Agent do
  @moduledoc false

  use Spectre.Agent
  use Spectre.Pulse

  pulsing do
    identity("spectre://instance/receiver")
    state_scope(:peer)
  end

  flow :inbound do
    on :perform, pulse: "instance.perform" do
      run(:perform)
    end
  end

  def perform(input, _context), do: "received:" <> input.meta.pulse.from
end

defmodule Spectre.Pulse.InstanceIntegrationTest.OutboundDirectory do
  @moduledoc false

  def resolve(:receiver, _opts), do: {:ok, "spectre://instance/outbound-receiver"}

  def routes(address, opts) do
    [
      %{
        id: "instance-outbound-route",
        address: address,
        transport: Spectre.Pulse.InstanceIntegrationTest.OutboundTransport,
        target: Keyword.fetch!(opts, :test_pid)
      }
    ]
  end
end

defmodule Spectre.Pulse.InstanceIntegrationTest.OutboundTransport do
  @moduledoc false

  @behaviour Spectre.Pulse.Transport

  alias Spectre.Pulse.Receipt

  @impl true
  def deliver(route, envelope, _opts) do
    send(route.target, {:instance_pulse_delivered, envelope})
    {:ok, Receipt.accepted(envelope.id, via: :instance_test, route_id: route.id)}
  end
end

defmodule Spectre.Pulse.InstanceIntegrationTest.OutboundStack do
  @moduledoc false

  use Spectre.Stack, id: :pulse_instance_outbound

  install Spectre.Pulse do
    transport(:instance_test, Spectre.Pulse.InstanceIntegrationTest.OutboundTransport)
    directory(Spectre.Pulse.InstanceIntegrationTest.OutboundDirectory)
  end
end

defmodule Spectre.Pulse.InstanceIntegrationTest.OutboundAgent do
  @moduledoc false

  use Spectre.Agent, stack: Spectre.Pulse.InstanceIntegrationTest.OutboundStack

  flow :outbound do
    on :send, regex: ~r/^send$/ do
      pulse(:receiver, type: "instance.outbound", data: %{source: :instance})
    end
  end
end

defmodule Spectre.Pulse.InstanceIntegrationTest do
  use ExUnit.Case, async: false

  alias Spectre.AgentRef
  alias Spectre.Instance
  alias Spectre.Pulse.Envelope
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Inbound.Result
  alias Spectre.Run.Ref
  alias Spectre.Subject
  alias Spectre.Turn

  alias __MODULE__.Agent
  alias __MODULE__.OutboundAgent

  test "explicit Subjects resolve core Instances and retain independent concurrent Runs" do
    supervisor =
      start_supervised!(
        {DynamicSupervisor,
         strategy: :one_for_one,
         name: :"pulse_instance_supervisor_#{System.unique_integer([:positive])}"}
      )

    subject = Subject.new({:pulse_account, System.unique_integer([:positive])})

    envelopes =
      for sender <- ["spectre://instance/alpha", "spectre://instance/beta"] do
        Envelope.new!(
          from: sender,
          to: "spectre://instance/receiver",
          payload: %{type: "instance.perform", data: %{"text" => sender}}
        )
      end

    results =
      envelopes
      |> Task.async_stream(
        fn envelope ->
          Spectre.Pulse.receive(envelope, %{
            authenticated_identity: envelope.from,
            target: AgentRef.new(Agent),
            subject: subject,
            instance_supervisor: supervisor
          })
        end,
        max_concurrency: 2,
        timeout: 5_000
      )
      |> Enum.map(fn {:ok, {:ok, %Result{} = result}} -> result end)

    assert [instance] = results |> Enum.map(& &1.target) |> Enum.uniq()
    assert Enum.all?(results, &(&1.turn.agent == instance))
    expected_target = AgentRef.key(AgentRef.new(Agent))
    assert Enum.all?(results, &(&1.receipt.metadata.target == expected_target))
    assert %Instance.Ref{subject: ^subject} = Instance.ref(instance)

    assert_eventually(fn ->
      info = Instance.info(instance)

      map_size(info.runs) == 2 and
        map_size(info.conversations) == 2 and
        Enum.all?(info.runs, fn {_id, run} -> run.status == :complete end)
    end)

    other_subject = Subject.new({:pulse_account, System.unique_integer([:positive])})
    envelope = hd(envelopes)

    assert {:ok, %Result{target: other_instance}} =
             Spectre.Pulse.receive(envelope, %{
               authenticated_identity: envelope.from,
               target: Agent,
               subject: other_subject,
               instance_supervisor: supervisor
             })

    assert other_instance != instance
    assert %Instance.Ref{subject: ^other_subject} = Instance.ref(other_instance)

    assert {:error, %Error{reason: :logical_instance_target_required}} =
             Spectre.Pulse.receive(envelope, %{
               authenticated_identity: envelope.from,
               target: instance,
               target_identity: envelope.to,
               state_scope: :peer,
               subject: subject
             })

    assert Process.alive?(instance)
  end

  test "outbound Pulse Effects retain independent ownership across Instance Runs" do
    supervisor =
      start_supervised!({DynamicSupervisor, strategy: :one_for_one})

    subject = Subject.new({:pulse_outbound, System.unique_integer([:positive])})

    assert {:ok, instance} =
             Spectre.instance(supervisor, OutboundAgent, subject, idle: false)

    assert {:ok, %Turn{observable: {:awaiting, %Ref{} = first_ref}} = first} =
             Spectre.turn(instance, "send",
               conversation_id: "pulse-outbound-first",
               test_pid: self()
             )

    assert {:ok, %Turn{observable: {:awaiting, %Ref{} = second_ref}} = second} =
             Spectre.turn(instance, "send",
               conversation_id: "pulse-outbound-second",
               test_pid: self()
             )

    refute first_ref.run_id == second_ref.run_id

    assert [first_effect] = first.result.effects
    assert [second_effect] = second.result.effects
    assert first_effect.run_id == first_ref.run_id
    assert second_effect.run_id == second_ref.run_id

    assert Spectre.state(instance).pending_effects
           |> Enum.map(& &1.run_id)
           |> Enum.sort() == Enum.sort([first_ref.run_id, second_ref.run_id])

    assert {:ok, %Turn{ref: first_completed_ref}} =
             Spectre.resume(
               instance,
               first_ref,
               {:execute, first_ref},
               test_pid: self()
             )

    assert first_completed_ref.run_id == first_ref.run_id
    first_effect_id = first_effect.id
    assert_receive {:instance_pulse_delivered, %{id: ^first_effect_id}}

    assert Enum.map(Spectre.state(instance).pending_effects, & &1.run_id) == [
             second_ref.run_id
           ]

    assert {:ok, %Turn{ref: second_completed_ref}} =
             Spectre.resume(
               instance,
               second_ref,
               {:execute, second_ref},
               test_pid: self()
             )

    assert second_completed_ref.run_id == second_ref.run_id
    second_effect_id = second_effect.id
    assert_receive {:instance_pulse_delivered, %{id: ^second_effect_id}}
    assert Spectre.state(instance).pending_effects == []

    assert_eventually(fn ->
      Enum.all?([first_ref.run_id, second_ref.run_id], fn run_id ->
        match?({:ok, %{status: :complete}}, Instance.run(instance, run_id))
      end)
    end)
  end

  test "an AgentRef requires a Subject and Instance identity options are closed" do
    envelope =
      Envelope.new!(
        from: "spectre://instance/ref-sender",
        to: "spectre://instance/receiver",
        payload: %{type: "instance.perform", data: %{}}
      )

    assert {:error, %Error{reason: :subject_required, outcome: :not_sent}} =
             Spectre.Pulse.receive(envelope, %{
               authenticated_identity: envelope.from,
               target: AgentRef.new(Agent)
             })

    supervisor =
      start_supervised!(
        {DynamicSupervisor,
         strategy: :one_for_one,
         name: :"pulse_closed_opts_supervisor_#{System.unique_integer([:positive])}"}
      )

    assert {:error,
            %Error{
              reason: {:invalid_instance_target, message},
              outcome: :not_sent
            }} =
             Spectre.Pulse.receive(envelope, %{
               authenticated_identity: envelope.from,
               target: Agent,
               subject: Subject.new("closed-instance-options"),
               instance_supervisor: supervisor,
               instance_opts: [state: %{data: %{forged: true}}]
             })

    assert message =~ "cannot set identity or state"
  end

  test "a Subject-scoped PID target must be a registered core Instance" do
    session =
      start_supervised!(
        {Spectre.Session,
         agent: Agent,
         id: {:pulse_legacy_session, System.unique_integer([:positive])},
         idle: false}
      )

    envelope =
      Envelope.new!(
        from: "spectre://instance/session-sender",
        to: "spectre://instance/receiver",
        payload: %{type: "instance.perform", data: %{}}
      )

    assert {:error, %Error{reason: :logical_instance_target_required}} =
             Spectre.Pulse.receive(envelope, %{
               authenticated_identity: envelope.from,
               target: session,
               target_identity: envelope.to,
               state_scope: :peer,
               subject: Subject.new("must-not-route-to-session")
             })

    assert Process.alive?(session)
  end

  test "omitting Subject preserves the transport-only module target" do
    envelope =
      Envelope.new!(
        from: "spectre://instance/legacy-sender",
        to: "spectre://instance/receiver",
        payload: %{type: "instance.perform", data: %{}}
      )

    assert {:ok, %Result{target: Agent, turn: turn}} =
             Spectre.Pulse.receive(envelope, %{
               authenticated_identity: envelope.from,
               target: Agent
             })

    assert turn.agent == Agent
  end

  defp assert_eventually(fun, attempts \\ 100)

  defp assert_eventually(fun, attempts) when attempts > 0 do
    if fun.() do
      :ok
    else
      Process.sleep(10)
      assert_eventually(fun, attempts - 1)
    end
  end

  defp assert_eventually(_fun, 0), do: flunk("condition did not become true")
end

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

defmodule Spectre.Pulse.InstanceIntegrationTest do
  use ExUnit.Case, async: false

  alias Spectre.AgentRef
  alias Spectre.Instance
  alias Spectre.Pulse.Envelope
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Inbound.Result
  alias Spectre.Subject

  alias __MODULE__.Agent

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

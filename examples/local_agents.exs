defmodule PulseExample.Tao do
  use Spectre.Agent
  use Spectre.Pulse

  pulsing do
    advertise(capabilities: [:research])
    pulse_inbound(on_result: {PulseExample.ShowInbound, :call, []})
  end

  flow :remote_requests do
    on :research, pulse: "research.perform" do
      run(:research)
    end
  end

  def research(input, _ctx) do
    "Tao accepted #{input.meta.pulse.type}: #{input.text}"
  end
end

defmodule PulseExample.ShowInbound do
  def call(inbound) do
    case inbound.turn.decision do
      {:reply, result} -> IO.puts("Tao replied locally: #{result.reply_text}")
      decision -> IO.puts("Tao decision: #{elem(decision, 0)}")
    end

    :ok
  end
end

defmodule PulseExample.Anna do
  use Spectre.Agent
  use Spectre.Pulse

  pulsing do
    contact(:tao, Spectre.Pulse.Address.for_agent(PulseExample.Tao), capabilities: [:research])
  end

  flow :delegation do
    on :delegate, regex: ~r/^research:/ do
      pulse(:tao,
        act: :request,
        type: "research.perform",
        build: :request_data,
        expect: "research.completed"
      )
    end
  end

  def request_data(input, _ctx) do
    %{"topic" => String.replace_prefix(input.text, "research:", "")}
  end
end

{:ok, _pulse} = Spectre.Pulse.start_link(transports: [])

{:ok, staged} = Spectre.turn(PulseExample.Anna, "research:nautical market")
{:needs, effect, _result} = staged.decision
IO.puts("Anna staged Pulse message #{effect.id} for #{effect.payload.to}")

{:ok, completed} = Spectre.Pulse.execute_turn(staged)
{:completed, delivered, _result} = completed.decision
IO.puts("Anna delivery status: #{delivered.status} via #{delivered.result.via}")

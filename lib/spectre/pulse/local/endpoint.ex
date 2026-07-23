defmodule Spectre.Pulse.Local.Endpoint do
  @moduledoc """
  Mailbox process for one locally subscribed Spectre Agent.

  Messages are serialized by the process mailbox and passed to the common
  inbound bridge. The process owns no semantic state; Spectre and the host
  remain responsible for state persistence.
  """

  use GenServer

  alias Spectre.Pulse.Local
  alias Spectre.Pulse.Transports.Local, as: LocalTransport

  @typep state :: %{
           agent: module(),
           identity: String.t(),
           endpoint: term(),
           endpoint_opts: keyword(),
           owner: pid() | nil
         }

  @doc false
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    agent = Keyword.fetch!(opts, :agent)

    %{
      id: {__MODULE__, agent},
      start: {__MODULE__, :start_link, [opts]},
      restart: :permanent,
      type: :worker
    }
  end

  @doc false
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) when is_list(opts) do
    agent = Keyword.fetch!(opts, :agent)

    with {:ok, identity, endpoint, endpoint_opts} <- Local.subscription(agent, opts) do
      state = %{
        agent: agent,
        identity: identity,
        endpoint: endpoint,
        endpoint_opts: endpoint_opts,
        owner: Keyword.get(opts, :owner)
      }

      GenServer.start_link(__MODULE__, state, name: Local.via(identity))
    end
  end

  @doc false
  @spec init(state()) :: {:ok, state()}
  @impl GenServer
  def init(state) do
    Registry.update_value(Spectre.Pulse.Local.Registry, state.identity, fn _value ->
      %{agent: state.agent, endpoint: state.endpoint, owner: state.owner}
    end)

    {:ok, state}
  end

  @doc false
  @spec handle_info(term(), state()) :: {:noreply, state()}
  @impl GenServer
  def handle_info({:spectre_pulse, _sender, _envelope} = message, state) do
    _result = LocalTransport.handle_message(message, state.endpoint, state.endpoint_opts)
    {:noreply, state}
  end

  def handle_info(_message, state), do: {:noreply, state}
end

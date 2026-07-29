defmodule Spectre.Pulse.Executor do
  @moduledoc """
  Thin execution aliases for `%Spectre.Effect{kind: :pulse}`.

  Stack-installed Agents register Pulse's effect executor, so new code uses
  the canonical `Spectre.execute/3` boundary. `execute/3` and `execute_turn/2`
  delegate to that durable boundary; `execute_pending/3` delegates to the pure
  `Spectre.Execution` workflow and leaves persistence to its caller. Pulse
  owns no parallel lifecycle.
  """

  alias Spectre.Pulse.Config
  alias Spectre.Pulse.Discovery
  alias Spectre.Pulse.Envelope
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Network
  alias Spectre.Pulse.State, as: PulseState

  @doc "Executes the pending Pulse effect in a Spectre result."
  @spec execute(module(), Spectre.Result.t(), keyword()) ::
          {:ok, Spectre.Result.t()} | {:error, term()}
  def execute(agent, %Spectre.Result{} = result, opts \\ []),
    do: Spectre.execute(agent, result, opts)

  @doc """
  Executes the pending Pulse effect selected by a local Turn.

  This compatibility helper returns a fresh Turn projection. A caller that
  owns a resumable Run must execute its Invocation through
  `Spectre.Runtime.resume/3` instead.
  """
  @spec execute_turn(Spectre.Turn.t(), keyword()) ::
          {:ok, Spectre.Turn.t()} | {:error, term()}
  def execute_turn(%Spectre.Turn{} = turn, opts \\ []) do
    with {:ok, result} <- execute(turn.agent, turn.result, opts) do
      {:ok, Spectre.Turn.from_result(turn.agent, turn.input, turn.opts, result)}
    end
  end

  @doc "Executes one pending Pulse effect against immutable Spectre state."
  @spec execute_pending(Spectre.State.t(), module(), keyword()) ::
          {:ok, Spectre.Result.t()} | {:error, term()}
  def execute_pending(%Spectre.State{} = state, agent, opts \\ []) when is_atom(agent) do
    context = %Spectre.Context{
      agent: agent,
      input: Keyword.get(opts, :input),
      state: state,
      opts: opts,
      assigns: Keyword.get(opts, :assigns, %{}),
      route: Keyword.get(opts, :route)
    }

    Spectre.Execution.execute_pending(state, context, opts)
  end

  @doc "Delivers an executable Pulse effect without changing Spectre state."
  @spec deliver(module(), Spectre.Effect.t(), Spectre.State.t(), keyword()) ::
          {:ok, Spectre.Pulse.Receipt.t()} | {:error, Error.t()}
  def deliver(agent, %Spectre.Effect{} = effect, %Spectre.State{} = state, opts \\ []) do
    with :ok <- validate_effect(effect),
         {:ok, config} <- Config.fetch(agent),
         {:ok, envelope} <- envelope(config, effect),
         {:ok, resolution} <- resolve_destination(config, state, effect.payload.to, opts),
         {:ok, routes} <-
           Discovery.routes(
             resolution.address,
             opts
             |> Keyword.put(:directory, config.directory)
             |> Keyword.put(
               :routes,
               resolution.routes ++ Keyword.get(opts, :routes, [])
             )
           ) do
      network_opts =
        opts
        |> Keyword.put(:routes, routes)
        |> Keyword.put(:contact, resolution.contact)

      Network.deliver(config.network, envelope, network_opts)
    end
  end

  @spec envelope(Config.t(), Spectre.Effect.t()) ::
          {:ok, Envelope.t()} | {:error, Error.t()}
  defp envelope(config, effect) do
    Envelope.new(
      version: 1,
      id: effect.id,
      from: config.identity,
      to: effect.payload.to,
      act: effect.payload.act,
      relates_to: effect.payload.relates_to,
      payload: %{type: effect.payload.type, data: effect.payload.data},
      metadata: effect.payload.metadata
    )
  end

  @spec resolve_destination(Config.t(), Spectre.State.t(), term(), keyword()) ::
          {:ok, Spectre.Pulse.Directory.Resolution.t()} | {:error, Error.t()}
  defp resolve_destination(config, state, reference, opts) do
    book = PulseState.contact_book(state, config.contacts)
    Discovery.resolve_identity(book, reference, Keyword.put(opts, :directory, config.directory))
  end

  @spec validate_effect(Spectre.Effect.t()) :: :ok | {:error, Error.t()}
  defp validate_effect(%Spectre.Effect{kind: :pulse, name: :send, status: status})
       when status in [:pending, :approved],
       do: :ok

  defp validate_effect(%Spectre.Effect{kind: :pulse, status: :waiting_policy} = effect),
    do: {:error, Error.not_sent(:authorization, {:effect_not_approved, effect.id})}

  defp validate_effect(%Spectre.Effect{kind: :pulse} = effect),
    do: {:error, Error.not_sent(:validation, {:effect_not_executable, effect.id, effect.status})}

  defp validate_effect(%Spectre.Effect{} = effect),
    do: {:error, Error.not_sent(:validation, {:unsupported_effect_kind, effect.kind})}
end

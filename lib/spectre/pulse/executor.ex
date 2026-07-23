defmodule Spectre.Pulse.Executor do
  @moduledoc """
  Explicit execution boundary for `%Spectre.Effect{kind: :pulse}`.

  Current Spectre executes `:action` effects through its own executor. Pulse
  consumes the same generic lifecycle data but deliberately provides this
  separate boundary. The returned state is immutable; the host persists it
  through its configured Spectre state adapter before/after execution.
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
  def execute(agent, %Spectre.Result{state: %Spectre.State{} = state} = result, opts \\ []) do
    with {:ok, executed} <-
           execute_pending(
             state,
             agent,
             Keyword.merge(opts, input: result.input, route: result.route)
           ) do
      {:ok,
       %{
         result
         | state: executed.state,
           effects: executed.effects,
           events: result.events ++ executed.events,
           metadata: Map.merge(result.metadata, executed.metadata)
       }}
    end
  end

  @doc "Executes the pending Pulse effect selected by a turn."
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
    case Spectre.State.pending_effect(state) do
      nil ->
        {:ok,
         %Spectre.Result{
           state: state,
           input: Keyword.get(opts, :input),
           route: Keyword.get(opts, :route),
           events: [%{type: :pulse_effect_missing}]
         }}

      %Spectre.Effect{} = effect ->
        execute_effect(state, agent, effect, opts)
    end
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

  @spec execute_effect(Spectre.State.t(), module(), Spectre.Effect.t(), keyword()) ::
          {:ok, Spectre.Result.t()} | {:error, term()}
  defp execute_effect(state, agent, effect, opts) do
    with :ok <- validate_effect_origin(effect, agent) do
      case deliver(agent, effect, state, opts) do
        {:ok, receipt} ->
          finish(state, effect, {:complete_effect, effect.id, receipt}, receipt, nil, opts)

        {:error, %Error{} = error} ->
          finish(state, effect, {:fail_effect, effect.id, error}, nil, error, opts)
      end
    end
  end

  @spec finish(
          Spectre.State.t(),
          Spectre.Effect.t(),
          term(),
          Spectre.Pulse.Receipt.t() | nil,
          Error.t() | nil,
          keyword()
        ) :: {:ok, Spectre.Result.t()} | {:error, term()}
  defp finish(state, effect, command, receipt, error, opts) do
    with {:ok, transition} <- Spectre.Lifecycle.apply(state, command) do
      terminal = transition.effect
      completed? = terminal.status == :completed

      event = %{
        type: if(completed?, do: :pulse_delivery_accepted, else: :pulse_delivery_failed),
        kind: :pulse,
        name: :send,
        effect_id: effect.id,
        message_id: effect.id,
        to: effect.payload.to,
        receipt: receipt,
        error: error
      }

      {:ok,
       %Spectre.Result{
         state: transition.to,
         input: Keyword.get(opts, :input),
         route: Keyword.get(opts, :route),
         effects: [terminal],
         events: [event],
         metadata: %{pulse_execution_transition: transition}
       }}
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

  @spec validate_effect_origin(Spectre.Effect.t(), module()) :: :ok | {:error, term()}
  defp validate_effect_origin(%Spectre.Effect{scope: nil} = effect, _agent),
    do: {:error, {:effect_scope_missing, effect.id}}

  defp validate_effect_origin(%Spectre.Effect{owner: nil}, _agent), do: :ok

  defp validate_effect_origin(%Spectre.Effect{} = effect, agent) do
    case Spectre.Definition.for_scope(agent, effect.scope) do
      {:ok, definition} when definition.owner == effect.owner -> :ok
      {:ok, definition} -> {:error, {:effect_owner_mismatch, effect.owner, definition.owner}}
      {:error, reason} -> {:error, {:effect_scope_unresolvable, effect.scope, reason}}
    end
  end
end

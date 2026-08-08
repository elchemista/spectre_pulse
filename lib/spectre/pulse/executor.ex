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
  alias Spectre.Pulse.Options
  alias Spectre.Pulse.State, as: PulseState

  @doc "Executes the pending Pulse effect in a Spectre result."
  @spec execute(module(), Spectre.Result.t(), keyword()) ::
          {:ok, Spectre.Result.t()} | {:error, term()}
  def execute(agent, result, opts \\ [])

  def execute(agent, %Spectre.Result{} = result, opts) do
    with {:ok, opts} <- Options.keyword(opts) do
      Spectre.execute(agent, result, opts)
    end
  end

  def execute(_agent, result, _opts), do: {:error, {:invalid_pulse_result, result}}

  @doc """
  Executes the pending Pulse effect selected by a local Turn.

  This compatibility helper returns a fresh Turn projection. A caller that
  owns a resumable Run must execute its Invocation through
  `Spectre.Runtime.resume/3` instead.
  """
  @spec execute_turn(Spectre.Turn.t(), keyword()) ::
          {:ok, Spectre.Turn.t()} | {:error, term()}
  def execute_turn(turn, opts \\ [])

  def execute_turn(%Spectre.Turn{} = turn, opts) do
    with {:ok, opts} <- Options.keyword(opts),
         {:ok, result} <- execute(turn.agent, turn.result, opts) do
      {:ok, Spectre.Turn.from_result(turn.agent, turn.input, turn.opts, result)}
    end
  end

  def execute_turn(turn, _opts), do: {:error, {:invalid_pulse_turn, turn}}

  @doc "Executes one pending Pulse effect against immutable Spectre state."
  @spec execute_pending(Spectre.State.t(), module(), keyword()) ::
          {:ok, Spectre.Result.t()} | {:error, term()}
  def execute_pending(state, agent, opts \\ [])

  def execute_pending(%Spectre.State{} = state, agent, opts)
      when is_atom(agent) and not is_nil(agent) do
    with {:ok, opts} <- Options.keyword(opts),
         :ok <- validate_assigns(Keyword.get(opts, :assigns, %{})) do
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
  end

  def execute_pending(state, agent, _opts),
    do: {:error, {:invalid_pulse_execution, state, agent}}

  @doc "Delivers an executable Pulse effect without changing Spectre state."
  @spec deliver(module(), Spectre.Effect.t(), Spectre.State.t(), keyword()) ::
          {:ok, Spectre.Pulse.Receipt.t()} | {:error, Error.t()}
  def deliver(agent, effect, state, opts \\ [])

  def deliver(agent, %Spectre.Effect{} = effect, %Spectre.State{} = state, opts)
      when is_atom(agent) and not is_nil(agent) do
    with {:ok, opts} <- Options.keyword(opts),
         :ok <- validate_effect(effect),
         {:ok, config} <- Config.fetch(agent),
         {:ok, envelope} <- envelope(config, effect, opts),
         {:ok, target} <- effect_payload(effect, :to),
         {:ok, resolution} <- resolve_destination(config, state, target, opts),
         {:ok, explicit_routes} <- route_options(opts),
         {:ok, routes} <-
           Discovery.routes(
             resolution.address,
             opts
             |> Keyword.put(:directory, config.directory)
             |> Keyword.put(
               :routes,
               resolution.routes ++ explicit_routes
             )
           ) do
      network_opts =
        opts
        |> Keyword.put(:routes, routes)
        |> Keyword.put(:contact, resolution.contact)

      Network.deliver(config.network, envelope, network_opts)
    else
      {:error, %Error{} = error} -> {:error, put_message_context(error, effect.id)}
      {:error, reason} -> {:error, Error.not_sent(:validation, reason, message_id: effect.id)}
    end
  end

  def deliver(_agent, effect, _state, _opts) do
    {:error,
     Error.not_sent(:validation, {:invalid_pulse_effect, effect}, message_id: effect_id(effect))}
  end

  @spec envelope(Config.t(), Spectre.Effect.t(), keyword()) ::
          {:ok, Envelope.t()} | {:error, Error.t()}
  defp envelope(config, effect, opts) do
    with {:ok, to} <- effect_payload(effect, :to),
         {:ok, act} <- effect_payload(effect, :act),
         {:ok, type} <- effect_payload(effect, :type),
         {:ok, data} <- effect_payload(effect, :data),
         {:ok, relates_to} <- effect_payload(effect, :relates_to),
         {:ok, metadata} <- effect_payload(effect, :metadata) do
      Envelope.new(
        [
          version: 1,
          id: effect.id,
          from: config.identity,
          to: to,
          act: act,
          relates_to: relates_to,
          payload: %{type: type, data: data},
          metadata: metadata
        ],
        opts
      )
    end
  end

  @spec resolve_destination(Config.t(), Spectre.State.t(), term(), keyword()) ::
          {:ok, Spectre.Pulse.Directory.Resolution.t()} | {:error, Error.t()}
  defp resolve_destination(config, state, reference, opts) do
    book = PulseState.contact_book(state, config.contacts)
    Discovery.resolve_identity(book, reference, Keyword.put(opts, :directory, config.directory))
  end

  @spec validate_effect(Spectre.Effect.t()) :: :ok | {:error, Error.t()}
  defp validate_effect(%Spectre.Effect{kind: :pulse, name: :send, status: status} = effect)
       when status in [:pending, :approved],
       do: validate_effect_payload_shape(effect)

  defp validate_effect(%Spectre.Effect{kind: :pulse, status: :waiting_policy} = effect),
    do: {:error, Error.not_sent(:authorization, {:effect_not_approved, effect.id})}

  defp validate_effect(%Spectre.Effect{kind: :pulse} = effect),
    do: {:error, Error.not_sent(:validation, {:effect_not_executable, effect.id, effect.status})}

  defp validate_effect(%Spectre.Effect{} = effect),
    do: {:error, Error.not_sent(:validation, {:unsupported_effect_kind, effect.kind})}

  @spec validate_effect_payload_shape(Spectre.Effect.t()) :: :ok | {:error, Error.t()}
  defp validate_effect_payload_shape(%Spectre.Effect{payload: payload}) when is_map(payload),
    do: :ok

  defp validate_effect_payload_shape(%Spectre.Effect{id: id, payload: payload}),
    do:
      {:error,
       Error.not_sent(:validation, {:invalid_pulse_effect_payload, payload}, message_id: id)}

  @spec effect_payload(Spectre.Effect.t(), atom()) :: {:ok, term()} | {:error, Error.t()}
  defp effect_payload(%Spectre.Effect{payload: payload, id: id}, key) when is_map(payload) do
    case Map.fetch(payload, key) do
      {:ok, value} ->
        {:ok, value}

      :error ->
        case Map.fetch(payload, Atom.to_string(key)) do
          {:ok, value} ->
            {:ok, value}

          :error ->
            {:error,
             Error.not_sent(:validation, {:pulse_effect_field_missing, key}, message_id: id)}
        end
    end
  end

  @spec route_options(keyword()) :: {:ok, [term()]} | {:error, term()}
  defp route_options(opts) do
    case Keyword.get(opts, :routes, []) do
      routes when is_list(routes) -> {:ok, routes}
      routes -> {:error, {:invalid_routes, routes}}
    end
  end

  @spec validate_assigns(term()) :: :ok | {:error, term()}
  defp validate_assigns(assigns) when is_map(assigns), do: :ok
  defp validate_assigns(assigns), do: {:error, {:invalid_pulse_assigns, assigns}}

  @spec put_message_context(Error.t(), term()) :: Error.t()
  defp put_message_context(%Error{message_id: nil} = error, message_id),
    do: %{error | message_id: message_id}

  defp put_message_context(%Error{} = error, _message_id), do: error

  @spec effect_id(term()) :: term()
  defp effect_id(%Spectre.Effect{id: id}), do: id
  defp effect_id(_effect), do: nil
end

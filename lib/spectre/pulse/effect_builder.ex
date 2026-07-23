defmodule Spectre.Pulse.EffectBuilder do
  @moduledoc false

  alias Spectre.Pulse.Config
  alias Spectre.Pulse.Discovery
  alias Spectre.Pulse.Expectation
  alias Spectre.Pulse.Protocol
  alias Spectre.Pulse.State, as: PulseState

  @doc false
  @spec stage(module(), Spectre.Input.t(), Spectre.Context.t(), keyword()) ::
          {:ok, Spectre.Result.t()} | {:error, term()}
  def stage(agent, %Spectre.Input{} = input, %Spectre.Context{} = ctx, opts) do
    with {:ok, config} <- Config.fetch(agent),
         :ok <- ensure_no_pending_effect(ctx.state),
         {:ok, to_ref} <- contextual_recipient(Keyword.fetch(opts, :to), input),
         {:ok, resolution} <- resolve_destination(config, ctx.state, to_ref, ctx.opts),
         {:ok, data} <- build_data(agent, input, ctx, opts),
         :ok <- validate_tracking(opts),
         {:ok, act} <- Protocol.decode_act(Keyword.get(opts, :act, :inform)),
         {:ok, relates_to} <- contextual_relation(Keyword.get(opts, :relates_to), input),
         {:ok, type} <- fetch_type(opts),
         effect <-
           build_effect(resolution.address, to_ref, act, type, data, relates_to, ctx, opts),
         {:ok, transition} <-
           Spectre.Lifecycle.apply(
             ctx.state,
             {:stage_effect, effect, Keyword.get(opts, :policy)}
           ) do
      state = maybe_track(transition.to, effect, to_ref, opts)
      staged = Spectre.State.pending_effect(state)

      events =
        [
          %{
            type: :pulse_effect_staged,
            kind: :pulse,
            name: :send,
            effect_id: staged.id,
            to: resolution.address
          }
        ] ++ expectation_event(state, effect.id)

      {:ok,
       %Spectre.Result{
         input: input,
         route: ctx.route,
         state: state,
         effects: [staged],
         events: events
       }}
    end
  end

  @spec resolve_destination(Config.t(), Spectre.State.t(), term(), keyword()) ::
          {:ok, Spectre.Pulse.Directory.Resolution.t()} | {:error, Spectre.Pulse.Error.t()}
  defp resolve_destination(config, state, reference, opts) do
    book = PulseState.contact_book(state, config.contacts)
    Discovery.resolve_identity(book, reference, Keyword.put(opts, :directory, config.directory))
  end

  @spec build_data(module(), Spectre.Input.t(), Spectre.Context.t(), keyword()) ::
          {:ok, term()} | {:error, term()}
  defp build_data(agent, input, ctx, opts) do
    case Keyword.fetch(opts, :build) do
      {:ok, builder} -> call_builder(agent, builder, input, ctx)
      :error -> {:ok, Keyword.get(opts, :data, %{})}
    end
  end

  @spec call_builder(module(), term(), Spectre.Input.t(), Spectre.Context.t()) ::
          {:ok, term()} | {:error, term()}
  defp call_builder(agent, function, input, ctx) when is_atom(function) do
    cond do
      function_exported?(agent, function, 2) ->
        protected(fn -> apply(agent, function, [input, ctx]) end)

      function_exported?(agent, function, 1) ->
        protected(fn -> apply(agent, function, [input]) end)

      true ->
        {:error, {:undefined_pulse_builder, agent, function}}
    end
  end

  defp call_builder(_agent, function, input, ctx) when is_function(function, 2),
    do: protected(fn -> function.(input, ctx) end)

  defp call_builder(_agent, {module, function, args}, input, ctx)
       when is_atom(module) and is_atom(function) and is_list(args),
       do: protected(fn -> apply(module, function, [input, ctx | args]) end)

  defp call_builder(_agent, builder, _input, _ctx),
    do: {:error, {:invalid_pulse_builder, builder}}

  @spec protected((-> term())) :: {:ok, term()} | {:error, term()}
  defp protected(callback) do
    case callback.() do
      {:ok, data} -> {:ok, data}
      {:error, reason} -> {:error, reason}
      data -> {:ok, data}
    end
  rescue
    exception -> {:error, {:pulse_builder_exception, exception}}
  catch
    kind, reason -> {:error, {:pulse_builder_exit, kind, reason}}
  end

  @spec build_effect(
          String.t(),
          term(),
          Spectre.Pulse.Protocol.act(),
          String.t(),
          term(),
          String.t() | nil,
          Spectre.Context.t(),
          keyword()
        ) :: Spectre.Effect.t()
  defp build_effect(to, contact, act, type, data, relates_to, ctx, opts) do
    id = Keyword.get(opts, :id, Spectre.Identity.uuid7())

    owner =
      case ctx.route do
        %Spectre.Route{owner: owner} when not is_nil(owner) -> owner
        _ -> ctx.agent
      end

    scope =
      case ctx.route do
        %Spectre.Route{scope: scope} when not is_nil(scope) -> scope
        _ -> :agent
      end

    %Spectre.Effect{
      id: id,
      idempotency_key: "pulse:" <> id,
      kind: :pulse,
      name: :send,
      owner: owner,
      scope: scope,
      status: :pending,
      policy: Keyword.get(opts, :policy),
      payload: %{
        to: to,
        contact: contact,
        act: act,
        type: type,
        data: data,
        relates_to: relates_to,
        metadata: Keyword.get(opts, :metadata, %{})
      },
      metadata: %{
        staged_at: DateTime.utc_now(),
        expect: Keyword.get(opts, :expect, Keyword.get(opts, :track, false))
      }
    }
  end

  @spec maybe_track(Spectre.State.t(), Spectre.Effect.t(), term(), keyword()) ::
          Spectre.State.t()
  defp maybe_track(state, effect, contact, opts) do
    case Keyword.get(opts, :expect, Keyword.get(opts, :track, false)) do
      false ->
        state

      nil ->
        state

      :reply ->
        put_expectation(state, effect, contact, :reply, opts)

      true ->
        put_expectation(state, effect, contact, :reply, opts)

      {:type, type} ->
        put_expectation(state, effect, contact, {:type, type}, opts)

      type when is_binary(type) ->
        put_expectation(state, effect, contact, {:type, type}, opts)
    end
  end

  @spec validate_tracking(keyword()) :: :ok | {:error, term()}
  defp validate_tracking(opts) do
    case Keyword.get(opts, :expect, Keyword.get(opts, :track, false)) do
      value when value in [false, nil, :reply, true] -> :ok
      {:type, type} when is_binary(type) -> :ok
      type when is_binary(type) -> :ok
      invalid -> {:error, {:invalid_pulse_expectation, invalid}}
    end
  end

  @spec put_expectation(
          Spectre.State.t(),
          Spectre.Effect.t(),
          term(),
          Expectation.waiting_for(),
          keyword()
        ) :: Spectre.State.t()
  defp put_expectation(state, effect, contact, waiting_for, opts) do
    expectation =
      Expectation.new(effect.id, contact, waiting_for,
        due_at: Keyword.get(opts, :due_at),
        metadata: Keyword.get(opts, :expectation_metadata, %{})
      )

    PulseState.put_expectation(state, expectation)
  end

  @spec expectation_event(Spectre.State.t(), String.t()) :: [map()]
  defp expectation_event(state, message_id) do
    if Map.has_key?(PulseState.expectations(state), message_id) do
      [%{type: :pulse_expectation_opened, message_id: message_id}]
    else
      []
    end
  end

  @spec contextual_recipient({:ok, term()} | :error, Spectre.Input.t()) ::
          {:ok, term()} | {:error, atom()}
  defp contextual_recipient({:ok, :sender}, input) do
    case get_in(input.meta, [:pulse, :from]) do
      sender when is_binary(sender) -> {:ok, sender}
      _ -> {:error, :pulse_sender_not_available}
    end
  end

  defp contextual_recipient({:ok, reference}, _input), do: {:ok, reference}
  defp contextual_recipient(:error, _input), do: {:error, :pulse_recipient_required}

  @spec contextual_relation(term(), Spectre.Input.t()) ::
          {:ok, term()} | {:error, :incoming_pulse_message_not_available}
  defp contextual_relation(:incoming, input) do
    case get_in(input.meta, [:pulse, :message_id]) do
      id when is_binary(id) -> {:ok, id}
      _ -> {:error, :incoming_pulse_message_not_available}
    end
  end

  defp contextual_relation(relation, _input), do: {:ok, relation}

  @spec fetch_type(keyword()) :: {:ok, String.t()} | {:error, term()}
  defp fetch_type(opts) do
    case Keyword.fetch(opts, :type) do
      {:ok, type} when is_binary(type) -> {:ok, type}
      {:ok, type} -> {:error, {:invalid_pulse_type, type}}
      :error -> {:error, :pulse_type_required}
    end
  end

  @spec ensure_no_pending_effect(Spectre.State.t()) :: :ok | {:error, term()}
  defp ensure_no_pending_effect(state) do
    case Spectre.State.pending_effect(state) do
      nil -> :ok
      effect -> {:error, {:pending_effect_not_resolved, effect.id, effect.status}}
    end
  end
end

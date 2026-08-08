defmodule Spectre.Pulse.EffectBuilder do
  @moduledoc false

  alias Spectre.Pulse.Config
  alias Spectre.Pulse.Discovery
  alias Spectre.Pulse.Envelope
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Expectation
  alias Spectre.Pulse.Options
  alias Spectre.Pulse.Protocol
  alias Spectre.Pulse.State, as: PulseState

  @doc false
  @spec stage(module(), Spectre.Input.t(), Spectre.Context.t(), keyword()) ::
          {:ok, Spectre.Result.t()} | {:error, term()}
  def stage(agent, input, ctx, opts)

  def stage(
        agent,
        %Spectre.Input{} = input,
        %Spectre.Context{agent: agent, state: %Spectre.State{}, opts: context_opts} = ctx,
        opts
      )
      when is_atom(agent) and not is_nil(agent) do
    run_id = Spectre.Context.lifecycle_run_id(ctx)

    with {:ok, opts} <- Options.keyword(opts),
         {:ok, context_opts} <- Options.keyword(context_opts),
         {:ok, config} <- Config.fetch(agent),
         :ok <- ensure_no_pending_effect(ctx.state, run_id),
         {:ok, to_ref} <- contextual_recipient(Keyword.fetch(opts, :to), input),
         {:ok, resolution} <- resolve_destination(config, ctx.state, to_ref, context_opts),
         {:ok, data} <- build_data(agent, input, ctx, opts),
         {:ok, act} <- Protocol.decode_act(Keyword.get(opts, :act, :inform)),
         {:ok, relates_to} <- contextual_relation(Keyword.get(opts, :relates_to), input),
         {:ok, type} <- fetch_type(opts),
         {:ok, effect} <-
           build_effect(
             %{
               from: config.identity,
               to: resolution.address,
               contact: to_ref,
               act: act,
               type: type,
               data: data,
               relates_to: relates_to
             },
             ctx,
             opts
           ),
         effect <- Spectre.Effect.bind_run(effect, run_id),
         {:ok, expectation} <- build_expectation(effect, to_ref, opts),
         {:ok, transition} <-
           Spectre.Lifecycle.apply(
             ctx.state,
             {:stage_effect, effect, Keyword.get(opts, :policy)}
           ) do
      state = put_expectation(transition.to, expectation)
      staged = Spectre.State.pending_effect(state, run_id)

      events =
        [
          %{
            type: :pulse_effect_staged,
            kind: :pulse,
            name: :send,
            effect_id: staged.id,
            to: resolution.address
          }
        ] ++ expectation_event(expectation)

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

  def stage(agent, %Spectre.Input{}, %Spectre.Context{agent: context_agent}, _opts)
      when is_atom(agent) and not is_nil(agent) and agent != context_agent do
    {:error, {:pulse_agent_context_mismatch, agent, context_agent}}
  end

  def stage(agent, input, context, opts) do
    {:error,
     Error.not_sent(
       :validation,
       {:invalid_pulse_stage, agent, input, context, opts}
     )}
  end

  @doc false
  @spec stage_from_context(module(), Spectre.Input.t(), Spectre.Context.t()) ::
          {:ok, Spectre.Result.t()} | {:error, term()}
  def stage_from_context(agent, %Spectre.Input{} = input, %Spectre.Context{} = context) do
    with {:ok, context_opts} <- Options.keyword(context.opts),
         {:ok, opts} <- fetch_stage_options(context_opts) do
      stage(agent, input, context, opts)
    end
  end

  @spec fetch_stage_options(keyword()) :: {:ok, keyword()} | {:error, term()}
  defp fetch_stage_options(context_opts) do
    case Keyword.fetch(context_opts, :spectre_pulse) do
      {:ok, opts} -> Options.keyword(opts)
      :error -> {:error, :pulse_stage_options_required}
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
       when is_atom(module) and not is_nil(module) and is_atom(function) and
              not is_nil(function) and is_list(args) do
    arity = length(args) + 2

    if Code.ensure_loaded?(module) and function_exported?(module, function, arity) do
      protected(fn -> apply(module, function, [input, ctx | args]) end)
    else
      {:error, {:undefined_pulse_builder, module, function, arity}}
    end
  end

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

  @spec build_effect(map(), Spectre.Context.t(), keyword()) ::
          {:ok, Spectre.Effect.t()} | {:error, Error.t()}
  defp build_effect(message, ctx, opts) do
    attrs = [
      version: Protocol.version(),
      id: Keyword.get_lazy(opts, :id, &Spectre.Identity.uuid7/0),
      from: message.from,
      to: message.to,
      act: message.act,
      relates_to: message.relates_to,
      payload: %{type: message.type, data: message.data},
      metadata: Keyword.get(opts, :metadata, %{})
    ]

    with {:ok, envelope} <- Envelope.new(attrs, opts) do
      {:ok,
       %Spectre.Effect{
         id: envelope.id,
         idempotency_key: "pulse:" <> envelope.id,
         kind: :pulse,
         name: :send,
         owner: effect_owner(ctx),
         scope: effect_scope(ctx),
         status: :pending,
         policy: Keyword.get(opts, :policy),
         payload: %{
           to: envelope.to,
           contact: message.contact,
           act: envelope.act,
           type: envelope.payload.type,
           data: envelope.payload.data,
           relates_to: envelope.relates_to,
           metadata: envelope.metadata
         },
         metadata: %{
           staged_at: DateTime.utc_now(),
           expect: tracking_option(opts)
         }
       }}
    end
  end

  @spec effect_owner(Spectre.Context.t()) :: module()
  defp effect_owner(%Spectre.Context{route: %Spectre.Route{owner: owner}})
       when is_atom(owner) and not is_nil(owner),
       do: owner

  defp effect_owner(%Spectre.Context{agent: agent}), do: agent

  @spec effect_scope(Spectre.Context.t()) :: Spectre.Definition.scope()
  defp effect_scope(%Spectre.Context{route: %Spectre.Route{scope: scope}}) when not is_nil(scope),
    do: scope

  defp effect_scope(%Spectre.Context{}), do: :agent

  @spec build_expectation(Spectre.Effect.t(), term(), keyword()) ::
          {:ok, Expectation.t() | nil} | {:error, term()}
  defp build_expectation(effect, contact, opts) do
    with {:ok, waiting_for} <- normalize_tracking(tracking_option(opts)),
         :ok <- validate_expectation_options(opts) do
      new_expectation(effect, contact, waiting_for, opts)
    end
  end

  @spec normalize_tracking(term()) ::
          {:ok, Expectation.waiting_for() | nil} | {:error, term()}
  defp normalize_tracking(value) when value in [false, nil], do: {:ok, nil}
  defp normalize_tracking(value) when value in [:reply, true], do: {:ok, :reply}
  defp normalize_tracking({:type, type}) when is_binary(type), do: {:ok, {:type, type}}
  defp normalize_tracking(type) when is_binary(type), do: {:ok, {:type, type}}
  defp normalize_tracking(value), do: {:error, {:invalid_pulse_expectation, value}}

  @spec validate_expectation_options(keyword()) :: :ok | {:error, term()}
  defp validate_expectation_options(opts) do
    due_at = Keyword.get(opts, :due_at)
    metadata = Keyword.get(opts, :expectation_metadata, %{})

    cond do
      not is_nil(due_at) and not is_struct(due_at, DateTime) ->
        {:error, {:invalid_expectation_due_at, due_at}}

      not is_map(metadata) ->
        {:error, {:invalid_expectation_metadata, metadata}}

      true ->
        :ok
    end
  end

  @spec new_expectation(Spectre.Effect.t(), term(), Expectation.waiting_for() | nil, keyword()) ::
          {:ok, Expectation.t() | nil} | {:error, term()}
  defp new_expectation(_effect, _contact, nil, _opts), do: {:ok, nil}

  defp new_expectation(effect, contact, waiting_for, opts) do
    {:ok,
     Expectation.new(effect.id, contact, waiting_for,
       due_at: Keyword.get(opts, :due_at),
       metadata: Keyword.get(opts, :expectation_metadata, %{})
     )}
  rescue
    exception -> {:error, {:invalid_pulse_expectation, exception}}
  catch
    kind, reason -> {:error, {:invalid_pulse_expectation, kind, reason}}
  end

  @spec put_expectation(Spectre.State.t(), Expectation.t() | nil) :: Spectre.State.t()
  defp put_expectation(state, nil), do: state
  defp put_expectation(state, expectation), do: PulseState.put_expectation(state, expectation)

  @spec expectation_event(Expectation.t() | nil) :: [map()]
  defp expectation_event(nil), do: []

  defp expectation_event(expectation) do
    [%{type: :pulse_expectation_opened, message_id: expectation.message_id}]
  end

  @spec tracking_option(keyword()) :: term()
  defp tracking_option(opts),
    do: Keyword.get(opts, :expect, Keyword.get(opts, :track, false))

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

  @spec ensure_no_pending_effect(Spectre.State.t(), String.t() | nil) ::
          :ok | {:error, term()}
  defp ensure_no_pending_effect(state, run_id) do
    case Spectre.State.pending_effect(state, run_id) do
      nil -> :ok
      effect -> {:error, {:pending_effect_not_resolved, effect.id, effect.status}}
    end
  end
end

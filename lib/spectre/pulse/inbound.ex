defmodule Spectre.Pulse.Inbound do
  @moduledoc """
  Secure inbound bridge from a validated Pulse envelope to `Spectre.turn/3`.

  The bridge owns no session or state. It selects the host's Spectre state
  scope and returns the ordinary `%Spectre.Turn{}` to the host.
  """

  alias Spectre.Pulse.Address
  alias Spectre.Pulse.Config
  alias Spectre.Pulse.Envelope
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Inbound.Result
  alias Spectre.Pulse.InboundContext
  alias Spectre.Pulse.Receipt

  @typep callback_label :: :authorization | :input_mapper | :state_scope | :target_resolver

  @doc "Authenticates, authorizes, maps, and delivers one inbound envelope."
  @spec receive(Envelope.t() | map(), InboundContext.t() | map() | keyword(), keyword()) ::
          {:ok, Result.t()} | {:error, Error.t()}
  def receive(envelope, context, opts \\ []) do
    context = InboundContext.new(context)

    with {:ok, envelope} <- Envelope.new(envelope, opts),
         {:ok, canonical_sender, authenticated?} <-
           authenticate_sender(envelope, context, opts),
         {:ok, target} <- resolve_target(envelope.to, context, opts),
         {:ok, config} <- target_config(target, context, opts),
         :ok <- ensure_recipient(envelope.to, config.identity),
         merged_opts <- Keyword.merge(config.inbound, opts),
         :ok <- allow_payload_type(envelope, merged_opts),
         :ok <- authorize(envelope, context, target, merged_opts),
         {:ok, input} <-
           build_input(envelope, context, canonical_sender, authenticated?, merged_opts),
         {:ok, conversation_id} <-
           state_scope(config.state_scope, target, envelope, context),
         turn_opts <- turn_options(envelope, conversation_id, merged_opts),
         {:ok, turn} <- run_turn(target, input, turn_opts, envelope),
         receipt <-
           Receipt.accepted(envelope.id,
             via: context.binding,
             metadata: %{target: inspect_target(target)}
           ) do
      {:ok,
       %Result{
         envelope: envelope,
         context: context,
         canonical_sender: canonical_sender,
         target: target,
         input: input,
         turn: turn,
         receipt: receipt
       }}
    end
  end

  @doc "Maps an envelope to a Spectre input without invoking an Agent."
  @spec to_input(Envelope.t(), InboundContext.t() | map(), keyword()) ::
          {:ok, Spectre.Input.t()} | {:error, Error.t()}
  def to_input(%Envelope{} = envelope, context, opts \\ []) do
    context = InboundContext.new(context)

    with {:ok, sender, authenticated?} <- authenticate_sender(envelope, context, opts) do
      build_input(envelope, context, sender, authenticated?, opts)
    end
  end

  @spec authenticate_sender(Envelope.t(), InboundContext.t(), keyword()) ::
          {:ok, String.t(), boolean()} | {:error, Error.t()}
  defp authenticate_sender(
         %Envelope{} = envelope,
         %InboundContext{authenticated_identity: nil},
         opts
       ) do
    if Keyword.get(opts, :allow_unauthenticated, false) do
      {:ok, envelope.from, false}
    else
      {:error,
       Error.not_sent(:authentication, :authenticated_identity_required, message_id: envelope.id)}
    end
  end

  defp authenticate_sender(
         %Envelope{} = envelope,
         %InboundContext{authenticated_identity: identity},
         _opts
       ) do
    with {:ok, canonical} <- Address.normalize(identity),
         :ok <- ensure_sender_identity(canonical, envelope) do
      {:ok, canonical, true}
    end
  end

  @spec ensure_sender_identity(String.t(), Envelope.t()) :: :ok | {:error, Error.t()}
  defp ensure_sender_identity(identity, %Envelope{from: identity}), do: :ok

  defp ensure_sender_identity(identity, %Envelope{} = envelope) do
    {:error,
     Error.not_sent(:authentication, :sender_identity_mismatch,
       message_id: envelope.id,
       details: %{declared: envelope.from, authenticated: identity}
     )}
  end

  @spec resolve_target(String.t(), InboundContext.t(), keyword()) ::
          {:ok, term()} | {:error, Error.t()}
  defp resolve_target(_address, %InboundContext{target: target}, _opts) when not is_nil(target),
    do: {:ok, target}

  defp resolve_target(address, context, opts) do
    resolver =
      Keyword.get(opts, :target_resolver) ||
        context.resolver ||
        Spectre.Pulse.Local

    resolver
    |> call_target_resolver(address, context)
    |> normalize_target_resolution(address)
  end

  @spec call_target_resolver(term(), String.t(), InboundContext.t()) :: term()
  defp call_target_resolver(resolver, address, context) when is_function(resolver, 2),
    do: safe_callback(resolver, [address, context], :target_resolver)

  defp call_target_resolver(resolver, address, context) when is_atom(resolver) do
    if Code.ensure_loaded?(resolver) and function_exported?(resolver, :resolve_target, 2) do
      safe_apply(resolver, :resolve_target, [address, context], :target_resolver)
    else
      {:error, :target_not_found}
    end
  end

  defp call_target_resolver(_resolver, _address, _context), do: {:error, :target_not_found}

  @spec normalize_target_resolution(term(), String.t()) :: {:ok, term()} | {:error, Error.t()}
  defp normalize_target_resolution({:ok, target}, _address) when not is_nil(target),
    do: {:ok, target}

  defp normalize_target_resolution({:error, reason}, _address),
    do: {:error, Error.not_sent(:routing, reason)}

  defp normalize_target_resolution(result, address) when result in [:error, nil],
    do: {:error, Error.not_sent(:routing, {:target_not_found, address})}

  defp normalize_target_resolution(target, _address), do: {:ok, target}

  @spec target_config(term(), InboundContext.t(), keyword()) ::
          {:ok, Config.t()} | {:error, Error.t()}
  defp target_config(target, context, opts) when is_atom(target) do
    case Config.fetch(target) do
      {:ok, config} ->
        {:ok, config}

      {:error, _error} ->
        explicit_target_config(context, opts)
    end
  end

  defp target_config(_target, context, opts), do: explicit_target_config(context, opts)

  @spec explicit_target_config(InboundContext.t(), keyword()) ::
          {:ok, Config.t()} | {:error, Error.t()}
  defp explicit_target_config(context, opts) do
    identity = Keyword.get(opts, :target_identity, context.target_identity)

    if identity do
      Config.new(
        identity: identity,
        state_scope: Keyword.get(opts, :state_scope, :agent),
        inbound: Keyword.get(opts, :inbound, [])
      )
    else
      {:error, Error.not_sent(:routing, :target_identity_required)}
    end
  end

  @spec ensure_recipient(String.t(), String.t()) :: :ok | {:error, Error.t()}
  defp ensure_recipient(recipient, target_identity) do
    if Address.equal?(recipient, target_identity) do
      :ok
    else
      {:error,
       Error.not_sent(:routing, :recipient_identity_mismatch,
         details: %{recipient: recipient, target_identity: target_identity}
       )}
    end
  end

  @spec allow_payload_type(Envelope.t(), keyword()) :: :ok | {:error, Error.t()}
  defp allow_payload_type(envelope, opts) do
    case Keyword.get(opts, :allowed_types, :all) do
      :all ->
        :ok

      allowed when is_list(allowed) ->
        if envelope.payload.type in allowed do
          :ok
        else
          {:error,
           Error.not_sent(:authorization, {:payload_type_not_allowed, envelope.payload.type},
             message_id: envelope.id
           )}
        end

      invalid ->
        {:error, Error.not_sent(:authorization, {:invalid_allowed_types, invalid})}
    end
  end

  @spec authorize(Envelope.t(), InboundContext.t(), term(), keyword()) ::
          :ok | {:error, Error.t()}
  defp authorize(envelope, context, target, opts) do
    authorization = Keyword.get(opts, :authorize, context.authorization)

    authorization
    |> call_authorizer(envelope, context, target)
    |> normalize_authorization(envelope)
  end

  @spec call_authorizer(term(), Envelope.t(), InboundContext.t(), term()) :: term()
  defp call_authorizer(nil, _envelope, _context, _target), do: :ok

  defp call_authorizer(authorization, envelope, context, target)
       when is_function(authorization, 3),
       do: safe_callback(authorization, [envelope, context, target], :authorization)

  defp call_authorizer(authorization, envelope, context, _target)
       when is_function(authorization, 2),
       do: safe_callback(authorization, [envelope, context], :authorization)

  defp call_authorizer(authorization, envelope, context, target)
       when is_atom(authorization) do
    if function_exported?(authorization, :authorize, 3) do
      safe_apply(
        authorization,
        :authorize,
        [envelope, context, target],
        :authorization
      )
    else
      {:error, {:invalid_authorizer, authorization}}
    end
  end

  defp call_authorizer(authorization, _envelope, _context, _target),
    do: {:error, {:invalid_authorizer, authorization}}

  @spec normalize_authorization(term(), Envelope.t()) :: :ok | {:error, Error.t()}
  defp normalize_authorization(result, _envelope) when result in [:ok, true], do: :ok

  defp normalize_authorization(false, envelope),
    do: {:error, Error.not_sent(:authorization, :forbidden, message_id: envelope.id)}

  defp normalize_authorization({:error, %Error{} = error}, _envelope), do: {:error, error}

  defp normalize_authorization({:error, reason}, envelope),
    do: {:error, Error.not_sent(:authorization, reason, message_id: envelope.id)}

  defp normalize_authorization(result, envelope) do
    {:error,
     Error.not_sent(:authorization, {:invalid_authorization_result, result},
       message_id: envelope.id
     )}
  end

  @spec build_input(Envelope.t(), InboundContext.t(), String.t(), boolean(), keyword()) ::
          {:ok, Spectre.Input.t()} | {:error, Error.t()}
  defp build_input(envelope, context, sender, authenticated?, opts) do
    pulse_meta = %{
      message_id: envelope.id,
      from: sender,
      to: envelope.to,
      act: envelope.act,
      relates_to: envelope.relates_to,
      type: envelope.payload.type,
      authenticated: authenticated?,
      binding: context.binding,
      peer: context.peer,
      verified: context.verified,
      declared_metadata: envelope.metadata
    }

    base = %Spectre.Input{
      text: default_input_text(envelope.payload.data),
      raw: envelope,
      meta: %{
        pulse: pulse_meta,
        pulse_type: envelope.payload.type,
        pulse_act: envelope.act,
        pulse_from: sender,
        pulse_message_id: envelope.id
      }
    }

    case Keyword.get(opts, :input_mapper) do
      nil ->
        {:ok, base}

      mapper when is_function(mapper, 3) ->
        mapper
        |> safe_callback([envelope, context, base], :input_mapper)
        |> normalize_input()

      {module, function, extra} when is_atom(module) and is_atom(function) and is_list(extra) ->
        module
        |> safe_apply(function, [envelope, context, base | extra], :input_mapper)
        |> normalize_input()

      invalid ->
        {:error, Error.not_sent(:inbound, {:invalid_input_mapper, invalid})}
    end
  end

  @spec default_input_text(term()) :: String.t()
  defp default_input_text(data) when is_binary(data), do: data

  defp default_input_text(data) when is_map(data) do
    case Map.get(data, :text, Map.get(data, "text")) do
      text when is_binary(text) -> text
      _ -> encode_input_data(data)
    end
  end

  defp default_input_text(data), do: encode_input_data(data)

  @spec encode_input_data(term()) :: String.t()
  defp encode_input_data(data) do
    case Jason.encode(data) do
      {:ok, text} -> text
      {:error, _reason} -> inspect(data, limit: 20, printable_limit: 2_000)
    end
  end

  @spec normalize_input(term()) :: {:ok, Spectre.Input.t()} | {:error, Error.t()}
  defp normalize_input(%Spectre.Input{} = input), do: {:ok, input}
  defp normalize_input({:ok, %Spectre.Input{} = input}), do: {:ok, input}
  defp normalize_input({:error, %Error{} = error}), do: {:error, error}
  defp normalize_input({:error, reason}), do: {:error, Error.not_sent(:inbound, reason)}

  defp normalize_input(value),
    do: {:error, Error.not_sent(:inbound, {:input_mapper_must_return_input, value})}

  @spec state_scope(Config.state_scope(), term(), Envelope.t(), InboundContext.t()) ::
          {:ok, term()} | {:error, Error.t()}
  defp state_scope(:agent, target, envelope, _context),
    do: {:ok, {:pulse_agent, target_identity(target, envelope.to)}}

  defp state_scope(:peer, target, envelope, _context),
    do: {:ok, {:pulse_peer, target_identity(target, envelope.to), envelope.from}}

  defp state_scope(scope, target, envelope, context) when is_function(scope, 3) do
    scope
    |> safe_callback([target, envelope, Map.from_struct(context)], :state_scope)
    |> normalize_state_scope()
  end

  defp state_scope({module, function, args}, target, envelope, context) do
    module
    |> safe_apply(
      function,
      [target, envelope, Map.from_struct(context) | args],
      :state_scope
    )
    |> normalize_state_scope()
  end

  @spec target_identity(term(), String.t()) :: String.t()
  defp target_identity(_target, fallback), do: fallback

  @spec normalize_state_scope(term()) :: {:ok, term()} | {:error, Error.t()}
  defp normalize_state_scope({:ok, value}), do: {:ok, value}
  defp normalize_state_scope({:error, %Error{} = error}), do: {:error, error}

  defp normalize_state_scope({:error, reason}),
    do: {:error, Error.not_sent(:inbound, reason)}

  defp normalize_state_scope(value), do: {:ok, value}

  @spec turn_options(Envelope.t(), term(), keyword()) :: keyword()
  defp turn_options(envelope, conversation_id, opts) do
    trace_id = Map.get(envelope.metadata, "trace_id", Map.get(envelope.metadata, :trace_id))

    opts
    |> Keyword.get(:turn_opts, [])
    |> Keyword.merge(Keyword.take(opts, [:state, :assigns, :memory, :timeout]))
    |> Keyword.put(:conversation_id, conversation_id)
    |> Keyword.put(:turn_id, envelope.id)
    |> Keyword.put(:trace_id, trace_id || envelope.id)
  end

  @spec run_turn(term(), Spectre.Input.t(), keyword(), Envelope.t()) ::
          {:ok, Spectre.Turn.t()} | {:error, Error.t()}
  defp run_turn(target, input, turn_opts, envelope) do
    case Spectre.turn(target, input, turn_opts) do
      {:ok, %Spectre.Turn{} = turn} ->
        {:ok, turn}

      {:error, reason} ->
        {:error,
         Error.outcome_unknown(:inbound, {:spectre_turn_failed, reason}, message_id: envelope.id)}
    end
  rescue
    exception ->
      {:error,
       Error.outcome_unknown(:inbound, {:spectre_turn_exception, exception},
         message_id: envelope.id,
         cause: exception
       )}
  catch
    kind, reason ->
      {:error,
       Error.outcome_unknown(:inbound, {:spectre_turn_exit, kind, reason},
         message_id: envelope.id
       )}
  end

  @spec inspect_target(term()) :: String.t()
  defp inspect_target(target) when is_atom(target), do: Atom.to_string(target)
  defp inspect_target(target) when is_pid(target), do: inspect(target)
  defp inspect_target(_target), do: "host_target"

  @spec safe_callback(function(), [term()], callback_label()) :: term()
  defp safe_callback(function, args, label) do
    apply(function, args)
  rescue
    exception -> {:error, {label, :exception, exception}}
  catch
    kind, reason -> {:error, {label, kind, reason}}
  end

  @spec safe_apply(module(), atom(), [term()], callback_label()) :: term()
  defp safe_apply(module, function, args, label) do
    apply(module, function, args)
  rescue
    exception -> {:error, {label, :exception, exception}}
  catch
    kind, reason -> {:error, {label, kind, reason}}
  end
end

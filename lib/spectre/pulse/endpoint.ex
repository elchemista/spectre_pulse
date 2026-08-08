defmodule Spectre.Pulse.Endpoint do
  @moduledoc """
  Application-facing endpoint dispatcher used by local-style transports.

  An endpoint may be a Pulse-enabled Agent, a function, an MFA, or a module
  exporting `handle_pulse/2` or `handle_pulse/3`.
  """

  alias Spectre.Pulse.Config
  alias Spectre.Pulse.Envelope
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Inbound
  alias Spectre.Pulse.Inbound.Result
  alias Spectre.Pulse.InboundContext
  alias Spectre.Pulse.Options
  alias Spectre.Pulse.Receipt

  @callback handle_pulse(Envelope.t(), InboundContext.t(), keyword()) ::
              {:ok, Result.t() | Receipt.t()} | {:error, Error.t() | term()}

  @typep endpoint_result ::
           :ok
           | Result.t()
           | {:ok, :accepted | Result.t() | Receipt.t()}
           | {:error, Error.t() | term()}
           | term()

  @doc "Accepts an envelope through a configured endpoint target."
  @spec accept(term(), Envelope.t(), InboundContext.t() | map(), keyword()) ::
          {:ok, Receipt.t()} | {:error, Error.t()}
  def accept(target, envelope, context, opts \\ [])

  def accept(target, %Envelope{} = envelope, context, opts) do
    with {:ok, opts} <- Options.keyword(opts),
         {:ok, envelope} <- Envelope.new(envelope, opts),
         {:ok, context} <- InboundContext.normalize(context) do
      target
      |> invoke(envelope, context, opts)
      |> normalize(envelope, opts)
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, Error.not_sent(:validation, reason, message_id: envelope.id)}
    end
  end

  def accept(_target, envelope, _context, _opts),
    do: {:error, Error.not_sent(:validation, {:invalid_envelope, envelope})}

  @spec invoke(term(), Envelope.t(), InboundContext.t(), keyword()) :: endpoint_result()
  defp invoke(nil, envelope, context, opts), do: Inbound.receive(envelope, context, opts)

  defp invoke(target, envelope, context, _opts) when is_function(target, 2),
    do: protected(fn -> target.(envelope, context) end, envelope)

  defp invoke(target, envelope, context, opts) when is_function(target, 3),
    do: protected(fn -> target.(envelope, context, opts) end, envelope)

  defp invoke({module, function}, envelope, context, _opts)
       when is_atom(module) and is_atom(function),
       do: protected(fn -> apply(module, function, [envelope, context]) end, envelope)

  defp invoke({module, function, args}, envelope, context, _opts)
       when is_atom(module) and is_atom(function) and is_list(args),
       do: protected(fn -> apply(module, function, [envelope, context | args]) end, envelope)

  defp invoke(target, envelope, context, opts) when is_atom(target),
    do: invoke_module(target, envelope, context, opts)

  defp invoke(target, envelope, _context, _opts),
    do: {:error, invalid_endpoint(target, envelope)}

  @spec invoke_module(module(), Envelope.t(), InboundContext.t(), keyword()) :: endpoint_result()
  defp invoke_module(target, envelope, context, opts) do
    cond do
      pulse_agent?(target) ->
        Inbound.receive(envelope, %{context | target: target}, opts)

      function_exported?(target, :handle_pulse, 3) ->
        protected(fn -> target.handle_pulse(envelope, context, opts) end, envelope)

      function_exported?(target, :handle_pulse, 2) ->
        protected(fn -> target.handle_pulse(envelope, context) end, envelope)

      true ->
        {:error, invalid_endpoint(target, envelope)}
    end
  end

  @spec invalid_endpoint(term(), Envelope.t()) :: Error.t()
  defp invalid_endpoint(target, envelope),
    do: Error.not_sent(:routing, {:invalid_endpoint, target}, message_id: envelope.id)

  @spec pulse_agent?(module()) :: boolean()
  defp pulse_agent?(target) when is_atom(target) do
    Code.ensure_loaded?(target) and match?({:ok, %Config{}}, Config.fetch(target))
  end

  @spec normalize(endpoint_result(), Envelope.t(), keyword()) ::
          {:ok, Receipt.t()} | {:error, Error.t()}
  defp normalize({:ok, %Result{} = result}, envelope, opts) do
    with {:ok, receipt} <- validate_receipt(result.receipt, envelope, opts),
         result = %{result | receipt: receipt},
         :ok <- notify_result(result, opts) do
      {:ok, receipt}
    else
      {:error, %Error{} = error} ->
        {:error, error}

      {:error, reason} ->
        {:error,
         Error.outcome_unknown(:inbound, {:on_result_failed, reason}, message_id: envelope.id)}
    end
  end

  defp normalize(%Result{} = result, envelope, opts),
    do: normalize({:ok, result}, envelope, opts)

  defp normalize({:ok, %Receipt{} = receipt}, envelope, opts),
    do: validate_receipt(receipt, envelope, opts)

  defp normalize(:ok, envelope, opts),
    do:
      {:ok,
       Receipt.accepted(envelope.id,
         via: Keyword.get(opts, :via),
         route_id: Keyword.get(opts, :route_id)
       )}

  defp normalize({:ok, :accepted}, envelope, opts), do: normalize(:ok, envelope, opts)
  defp normalize({:error, %Error{} = error}, _envelope, _opts), do: {:error, error}

  defp normalize({:error, reason}, envelope, _opts),
    do: {:error, Error.outcome_unknown(:inbound, reason, message_id: envelope.id)}

  defp normalize(other, envelope, _opts),
    do:
      {:error,
       Error.outcome_unknown(:inbound, {:invalid_endpoint_result, other}, message_id: envelope.id)}

  @spec notify_result(Result.t(), keyword()) :: :ok | {:error, term()}
  defp notify_result(result, opts) do
    case Keyword.get(opts, :on_result) do
      nil ->
        :ok

      callback when is_function(callback, 1) ->
        callback_result(fn -> callback.(result) end)

      {module, function, args}
      when is_atom(module) and is_atom(function) and is_list(args) ->
        callback_result(fn -> apply(module, function, [result | args]) end)

      invalid ->
        {:error, {:invalid_on_result, invalid}}
    end
  end

  @spec callback_result((-> term())) :: :ok | {:error, term()}
  defp callback_result(callback) do
    case callback.() do
      :ok -> :ok
      {:ok, _value} -> :ok
      {:error, reason} -> {:error, reason}
      other -> {:error, {:invalid_on_result_reply, other}}
    end
  rescue
    exception -> {:error, {:on_result_exception, exception}}
  catch
    kind, reason -> {:error, {:on_result_exit, kind, reason}}
  end

  @spec protected((-> term()), Envelope.t()) :: endpoint_result()
  defp protected(callback, envelope) do
    callback.()
  rescue
    exception ->
      {:error,
       Error.outcome_unknown(:inbound, {:endpoint_exception, exception},
         message_id: envelope.id,
         cause: exception
       )}
  catch
    kind, reason ->
      {:error,
       Error.outcome_unknown(:inbound, {:endpoint_exit, kind, reason}, message_id: envelope.id)}
  end

  @spec validate_receipt(term(), Envelope.t(), keyword()) ::
          {:ok, Receipt.t()} | {:error, Error.t()}
  defp validate_receipt(%Receipt{} = receipt, envelope, opts) do
    case Receipt.new(receipt) do
      {:ok, %Receipt{message_id: message_id} = receipt} when message_id == envelope.id ->
        {:ok,
         %{
           receipt
           | via: receipt.via || Keyword.get(opts, :via),
             route_id: receipt.route_id || Keyword.get(opts, :route_id)
         }}

      {:ok, %Receipt{} = receipt} ->
        {:error,
         Error.outcome_unknown(:inbound, :receipt_message_mismatch,
           message_id: envelope.id,
           details: %{receipt_message_id: receipt.message_id}
         )}

      {:error, %Error{} = error} ->
        {:error, %{error | outcome: :outcome_unknown, message_id: envelope.id}}
    end
  end

  defp validate_receipt(receipt, envelope, _opts),
    do:
      {:error,
       Error.outcome_unknown(:inbound, {:invalid_endpoint_receipt, receipt},
         message_id: envelope.id
       )}
end

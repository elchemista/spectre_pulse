defmodule Spectre.Pulse.Transport do
  @moduledoc """
  Contract implemented by all delivery bindings.
  """

  alias Spectre.Pulse.Envelope
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Options
  alias Spectre.Pulse.Reachability
  alias Spectre.Pulse.Receipt
  alias Spectre.Pulse.Route

  @callback deliver(Route.t(), Envelope.t(), keyword()) ::
              {:ok, Receipt.t()} | {:error, Error.t() | term()}

  @callback probe(Route.t(), keyword()) ::
              {:ok, Reachability.t()} | {:error, Error.t() | term()}

  @optional_callbacks probe: 2

  @doc "Dispatches one envelope through the route's transport module."
  @spec dispatch(Route.t(), Envelope.t(), keyword()) ::
          {:ok, Receipt.t()} | {:error, Error.t()}
  def dispatch(route, envelope, opts \\ [])

  def dispatch(%Route{} = route, %Envelope{} = envelope, opts) do
    with {:ok, opts} <- Options.keyword(opts),
         {:ok, route} <- Route.new(route),
         {:ok, envelope} <- Envelope.new(envelope, opts) do
      transport = route.transport

      if Code.ensure_loaded?(transport) and function_exported?(transport, :deliver, 3) do
        transport
        |> protected_call(:deliver, [route, envelope, opts], envelope, route, :delivery)
        |> normalize_delivery(envelope, route)
      else
        {:error,
         Error.not_sent(
           :transport,
           {:invalid_transport, transport},
           message_id: envelope.id,
           route_id: route.id
         )}
      end
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, Error.not_sent(:validation, reason, message_id: envelope.id)}
    end
  end

  def dispatch(route, %Envelope{} = envelope, _opts),
    do: {:error, Error.not_sent(:validation, {:invalid_route, route}, message_id: envelope.id)}

  def dispatch(%Route{} = route, envelope, _opts),
    do: {:error, Error.not_sent(:validation, {:invalid_envelope, envelope}, route_id: route.id)}

  def dispatch(route, envelope, _opts),
    do: {:error, Error.not_sent(:validation, {:invalid_transport_dispatch, route, envelope})}

  @doc "Probes a route when its transport supports a reliable probe."
  @spec probe(Route.t(), keyword()) :: {:ok, Reachability.t()} | {:error, Error.t()}
  def probe(route, opts \\ [])

  def probe(%Route{} = route, opts) do
    with {:ok, opts} <- Options.keyword(opts),
         {:ok, route} <- Route.new(route) do
      transport = route.transport

      if Code.ensure_loaded?(transport) and function_exported?(transport, :probe, 2) do
        transport
        |> protected_call(:probe, [route, opts], nil, route, :probe)
        |> normalize_probe(route)
      else
        {:ok,
         Reachability.unknown(:probe_not_supported,
           via: transport,
           level: :route_known,
           metadata: %{route_id: route.id}
         )}
      end
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, Error.not_sent(:validation, reason, route_id: route.id)}
    end
  end

  def probe(route, _opts),
    do: {:error, Error.not_sent(:validation, {:invalid_route, route})}

  @spec normalize_delivery(term(), Envelope.t(), Route.t()) ::
          {:ok, Receipt.t()} | {:error, Error.t()}
  defp normalize_delivery({:ok, %Receipt{} = receipt}, envelope, route) do
    case Receipt.new(receipt) do
      {:ok, %Receipt{message_id: message_id} = receipt} when message_id == envelope.id ->
        {:ok, %{receipt | route_id: receipt.route_id || route.id}}

      {:ok, %Receipt{} = receipt} ->
        {:error,
         Error.outcome_unknown(:transport, :receipt_message_mismatch,
           message_id: envelope.id,
           route_id: route.id,
           details: %{receipt_message_id: receipt.message_id}
         )}

      {:error, %Error{} = error} ->
        {:error,
         %{
           error
           | outcome: :outcome_unknown,
             message_id: envelope.id,
             route_id: route.id
         }}
    end
  end

  defp normalize_delivery({:error, %Error{} = error}, envelope, route),
    do: {:error, add_context(error, envelope, route)}

  defp normalize_delivery({:error, reason}, envelope, route) do
    {:error,
     Error.outcome_unknown(:transport, reason,
       message_id: envelope.id,
       route_id: route.id
     )}
  end

  defp normalize_delivery(other, envelope, route) do
    {:error,
     Error.outcome_unknown(:transport, {:invalid_transport_result, other},
       message_id: envelope.id,
       route_id: route.id
     )}
  end

  @spec normalize_probe(term(), Route.t()) ::
          {:ok, Reachability.t()} | {:error, Error.t()}
  defp normalize_probe({:ok, %Reachability{} = reachability}, route) do
    if valid_reachability?(reachability) do
      {:ok, reachability}
    else
      {:error,
       Error.not_sent(:transport, {:invalid_reachability, reachability}, route_id: route.id)}
    end
  end

  defp normalize_probe({:error, %Error{} = error}, route),
    do: {:error, add_context(error, nil, route)}

  defp normalize_probe({:error, reason}, route),
    do: {:error, Error.not_sent(:transport, reason, route_id: route.id)}

  defp normalize_probe(other, route),
    do: {:error, Error.not_sent(:transport, {:invalid_probe_result, other}, route_id: route.id)}

  @spec protected_call(
          module(),
          atom(),
          [term()],
          Envelope.t() | nil,
          Route.t(),
          :delivery | :probe
        ) :: term()
  defp protected_call(module, function, args, envelope, route, operation) do
    apply(module, function, args)
  rescue
    exception ->
      {:error,
       callback_error(operation, {:transport_exception, exception}, envelope, route,
         message_id: envelope && envelope.id,
         route_id: route.id,
         cause: exception
       )}
  catch
    kind, reason ->
      {:error,
       callback_error(operation, {:transport_exit, kind, reason}, envelope, route,
         message_id: envelope && envelope.id,
         route_id: route.id
       )}
  end

  @spec callback_error(:delivery | :probe, term(), Envelope.t() | nil, Route.t(), keyword()) ::
          Error.t()
  defp callback_error(:delivery, reason, _envelope, _route, opts),
    do: Error.outcome_unknown(:transport, reason, opts)

  defp callback_error(:probe, reason, _envelope, _route, opts),
    do: Error.not_sent(:transport, reason, opts)

  @spec add_context(Error.t(), Envelope.t() | nil, Route.t()) :: Error.t()
  defp add_context(error, envelope, route) do
    %{
      error
      | message_id: error.message_id || (envelope && envelope.id),
        route_id: error.route_id || route.id
    }
  end

  @spec valid_reachability?(Reachability.t()) :: boolean()
  defp valid_reachability?(reachability) do
    reachability.status in [:reachable, :unreachable, :unknown] and
      reachability.level in [:route_known, :pulse_endpoint] and
      is_struct(reachability.observed_at, DateTime) and
      is_integer(reachability.valid_for_ms) and reachability.valid_for_ms >= 0 and
      is_map(reachability.metadata)
  end
end

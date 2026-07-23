defmodule Spectre.Pulse.Transport do
  @moduledoc """
  Contract implemented by all delivery bindings.
  """

  alias Spectre.Pulse.Envelope
  alias Spectre.Pulse.Error
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
  def dispatch(%Route{} = route, %Envelope{} = envelope, opts \\ []) do
    transport = route.transport

    if Code.ensure_loaded?(transport) and function_exported?(transport, :deliver, 3) do
      transport
      |> protected_call(:deliver, [route, envelope, opts], envelope, route)
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
  end

  @doc "Probes a route when its transport supports a reliable probe."
  @spec probe(Route.t(), keyword()) :: {:ok, Reachability.t()} | {:error, Error.t()}
  def probe(%Route{} = route, opts \\ []) do
    transport = route.transport

    if Code.ensure_loaded?(transport) and function_exported?(transport, :probe, 2) do
      transport
      |> protected_call(:probe, [route, opts], nil, route)
      |> normalize_probe(route)
    else
      {:ok,
       Reachability.unknown(:probe_not_supported,
         via: transport,
         level: :route_known,
         metadata: %{route_id: route.id}
       )}
    end
  end

  @spec normalize_delivery(term(), Envelope.t(), Route.t()) ::
          {:ok, Receipt.t()} | {:error, Error.t()}
  defp normalize_delivery({:ok, %Receipt{} = receipt}, envelope, _route) do
    if receipt.message_id == envelope.id do
      {:ok, receipt}
    else
      {:error,
       Error.outcome_unknown(:transport, :receipt_message_mismatch,
         message_id: envelope.id,
         details: %{receipt_message_id: receipt.message_id}
       )}
    end
  end

  defp normalize_delivery({:error, %Error{} = error}, _envelope, _route), do: {:error, error}

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
  defp normalize_probe({:ok, %Reachability{} = reachability}, _route),
    do: {:ok, reachability}

  defp normalize_probe({:error, %Error{} = error}, _route), do: {:error, error}

  defp normalize_probe({:error, reason}, route),
    do: {:error, Error.not_sent(:transport, reason, route_id: route.id)}

  defp normalize_probe(other, route),
    do: {:error, Error.not_sent(:transport, {:invalid_probe_result, other}, route_id: route.id)}

  @spec protected_call(module(), atom(), [term()], Envelope.t() | nil, Route.t()) :: term()
  defp protected_call(module, function, args, envelope, route) do
    apply(module, function, args)
  rescue
    exception ->
      {:error,
       Error.outcome_unknown(:transport, {:transport_exception, exception},
         message_id: envelope && envelope.id,
         route_id: route.id,
         cause: exception
       )}
  catch
    kind, reason ->
      {:error,
       Error.outcome_unknown(:transport, {:transport_exit, kind, reason},
         message_id: envelope && envelope.id,
         route_id: route.id
       )}
  end
end

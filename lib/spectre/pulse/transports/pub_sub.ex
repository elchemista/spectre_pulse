defmodule Spectre.Pulse.Transports.PubSub do
  @moduledoc """
  Generic PubSub binding without a mandatory broker dependency.

  The route target may be a publisher function or a map containing
  `:adapter`, `:server`, and `:topic`. The common
  `adapter.broadcast(server, topic, message)` contract is supported.
  """

  @behaviour Spectre.Pulse.Transport

  alias Spectre.Pulse.Endpoint
  alias Spectre.Pulse.Envelope
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.InboundContext
  alias Spectre.Pulse.Reachability
  alias Spectre.Pulse.Receipt
  alias Spectre.Pulse.Route

  @typep message :: {:spectre_pulse, Envelope.t()}

  @doc false
  @spec deliver(Route.t(), Envelope.t(), keyword()) ::
          {:ok, Receipt.t()} | {:error, Error.t()}
  @impl Spectre.Pulse.Transport
  def deliver(%Route{} = route, %Envelope{} = envelope, _opts) do
    message = {:spectre_pulse, envelope}

    case publish(route.target, message) do
      :ok ->
        {:ok, Receipt.accepted(envelope.id, via: :pub_sub, route_id: route.id)}

      {:ok, _value} ->
        {:ok, Receipt.accepted(envelope.id, via: :pub_sub, route_id: route.id)}

      {:error, reason} ->
        {:error, Error.not_sent(:transport, reason, message_id: envelope.id, route_id: route.id)}

      other ->
        {:error,
         Error.not_sent(:transport, {:invalid_pub_sub_result, other},
           message_id: envelope.id,
           route_id: route.id
         )}
    end
  rescue
    exception ->
      {:error,
       Error.not_sent(:transport, {:pub_sub_exception, exception},
         message_id: envelope.id,
         route_id: route.id,
         cause: exception
       )}
  end

  @doc false
  @spec probe(Route.t(), keyword()) :: {:ok, Reachability.t()}
  @impl Spectre.Pulse.Transport
  def probe(%Route{} = route, _opts) do
    {:ok,
     Reachability.unknown(:subscriber_processing_not_observable,
       level: :route_known,
       via: :pub_sub,
       metadata: %{route_id: route.id}
     )}
  end

  @doc """
  Handles a standard PubSub message and resolves its recipient subscription.

  The broker consumer supplies authenticated connection facts, not an Agent
  module. `Envelope.to` selects the locally subscribed endpoint.
  """
  @spec handle_message(
          {:spectre_pulse, Envelope.t()},
          InboundContext.t() | map() | keyword(),
          keyword()
        ) :: {:ok, Receipt.t()} | {:error, Error.t()}
  def handle_message(message, context, opts \\ [])

  def handle_message({:spectre_pulse, %Envelope{} = envelope}, context, opts)
      when is_list(opts) do
    handle_message({:spectre_pulse, envelope}, nil, context, opts)
  end

  def handle_message(_message, _context, _opts) do
    {:error, Error.not_sent(:inbound, :invalid_pub_sub_message)}
  end

  @doc "Handles a standard PubSub message at an explicitly configured endpoint."
  @spec handle_message(
          {:spectre_pulse, Envelope.t()},
          term(),
          InboundContext.t() | map() | keyword(),
          keyword()
        ) :: {:ok, Receipt.t()} | {:error, Error.t()}
  def handle_message({:spectre_pulse, %Envelope{} = envelope}, endpoint, context, opts)
      when is_list(opts) do
    context =
      context
      |> Map.new()
      |> Map.put(:binding, :pub_sub)

    Endpoint.accept(endpoint, envelope, context, Keyword.put(opts, :via, :pub_sub))
  end

  def handle_message(_message, _endpoint, _context, _opts) do
    {:error, Error.not_sent(:inbound, :invalid_pub_sub_message)}
  end

  @spec publish(term(), message()) :: :ok | {:ok, term()} | {:error, term()} | term()
  defp publish(function, message) when is_function(function, 1), do: function.(message)

  defp publish(function, message) when is_function(function, 2),
    do: function.(topic(message), message)

  defp publish(%{adapter: adapter, server: server, topic: topic} = target, message) do
    event = Map.get(target, :event, message)

    cond do
      Code.ensure_loaded?(adapter) and function_exported?(adapter, :broadcast, 3) ->
        adapter.broadcast(server, topic, event)

      Code.ensure_loaded?(adapter) and function_exported?(adapter, :publish, 3) ->
        adapter.publish(server, topic, event)

      true ->
        {:error, {:invalid_pub_sub_adapter, adapter}}
    end
  end

  defp publish(target, _message), do: {:error, {:invalid_pub_sub_target, target}}

  @spec topic(message()) :: String.t()
  defp topic({:spectre_pulse, envelope}), do: envelope.to
end

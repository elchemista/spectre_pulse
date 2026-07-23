defmodule Spectre.Pulse.Transports.WebSocket do
  @moduledoc """
  WebSocket frame binding for application-owned connections.

  Pulse does not own connection processes. A route supplies a sender function,
  pid, `{module, connection}`, or `%{module: module, connection: connection}`.
  Incoming frames are handled with `handle_frame/3`.
  """

  @behaviour Spectre.Pulse.Transport

  alias Spectre.Pulse.Codec.JSON
  alias Spectre.Pulse.Envelope
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Inbound
  alias Spectre.Pulse.Reachability
  alias Spectre.Pulse.Receipt
  alias Spectre.Pulse.Route

  @doc false
  @spec deliver(Route.t(), Envelope.t(), keyword()) ::
          {:ok, Receipt.t()} | {:error, Error.t()}
  @impl Spectre.Pulse.Transport
  def deliver(%Route{} = route, %Envelope{} = envelope, opts) do
    with {:ok, frame} <- JSON.encode(envelope, opts),
         result <- send_frame(route.target, frame),
         :ok <- normalize_send(result) do
      {:ok,
       Receipt.accepted(envelope.id,
         via: :websocket,
         route_id: route.id,
         metadata: %{frame_bytes: byte_size(frame)}
       )}
    else
      {:error, %Error{} = error} ->
        {:error, %{error | route_id: error.route_id || route.id}}

      {:error, {:not_sent, reason}} ->
        {:error, Error.not_sent(:transport, reason, message_id: envelope.id, route_id: route.id)}

      {:error, reason} ->
        {:error,
         Error.outcome_unknown(:transport, reason,
           message_id: envelope.id,
           route_id: route.id
         )}
    end
  end

  @doc false
  @spec probe(Route.t(), keyword()) :: {:ok, Reachability.t()}
  @impl Spectre.Pulse.Transport
  def probe(%Route{} = route, _opts) do
    status =
      case route.target do
        pid when is_pid(pid) -> if Process.alive?(pid), do: :reachable, else: :unreachable
        function when is_function(function, 1) -> :reachable
        {_module, _connection} -> :reachable
        %{module: _module, connection: _connection} -> :reachable
        _ -> :unknown
      end

    {:ok,
     Reachability.new(status,
       level: :pulse_endpoint,
       via: :websocket,
       valid_for_ms: 0,
       metadata: %{route_id: route.id}
     )}
  end

  @doc "Decodes one text/binary frame and invokes the ordinary inbound bridge."
  @spec handle_frame(binary(), Spectre.Pulse.InboundContext.t() | map(), keyword()) ::
          {:ok, Spectre.Pulse.Inbound.Result.t()} | {:error, Error.t()}
  def handle_frame(frame, context, opts \\ []) when is_binary(frame) do
    with {:ok, envelope} <- JSON.decode(frame, opts) do
      context =
        context
        |> Map.new()
        |> Map.put(:binding, :websocket)

      Inbound.receive(envelope, context, opts)
    end
  end

  @spec send_frame(term(), binary()) :: :ok | {:ok, term()} | {:error, term()} | term()
  defp send_frame(pid, frame) when is_pid(pid) do
    if Process.alive?(pid) do
      send(pid, {:spectre_pulse_frame, frame})
      :ok
    else
      {:error, {:not_sent, :connection_not_alive}}
    end
  end

  defp send_frame(function, frame) when is_function(function, 1), do: function.(frame)

  defp send_frame({module, connection}, frame) when is_atom(module) do
    if Code.ensure_loaded?(module) and function_exported?(module, :send_frame, 2),
      do: module.send_frame(connection, frame),
      else: {:error, {:not_sent, {:invalid_websocket_sender, module}}}
  end

  defp send_frame(%{module: module, connection: connection}, frame),
    do: send_frame({module, connection}, frame)

  defp send_frame(target, _frame),
    do: {:error, {:not_sent, {:invalid_websocket_target, target}}}

  @spec normalize_send(term()) :: :ok | {:error, term()}
  defp normalize_send(:ok), do: :ok
  defp normalize_send({:ok, _value}), do: :ok
  defp normalize_send({:error, reason}), do: {:error, reason}
  defp normalize_send(other), do: {:error, {:invalid_websocket_send_result, other}}
end

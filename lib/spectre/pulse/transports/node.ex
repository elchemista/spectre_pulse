defmodule Spectre.Pulse.Transports.Node do
  @moduledoc """
  Direct BEAM node-to-node binding using `:erpc`.

  The distributed Erlang connection authenticates the peer node according to
  the host's cookie/TLS configuration. Applications remain responsible for
  deciding which agent identity that trusted node may assert.
  """

  @behaviour Spectre.Pulse.Transport

  alias Spectre.Pulse.Endpoint
  alias Spectre.Pulse.Envelope
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Reachability
  alias Spectre.Pulse.Receipt
  alias Spectre.Pulse.Route

  @doc false
  @spec deliver(Route.t(), Envelope.t(), keyword()) ::
          {:ok, Receipt.t()} | {:error, Error.t()}
  @impl Spectre.Pulse.Transport
  def deliver(%Route{target: %{node: remote_node} = target} = route, envelope, opts)
      when is_atom(remote_node) do
    endpoint = Map.get(target, :endpoint)
    timeout = Keyword.get(opts, :timeout, metadata(route, :timeout, 5_000))

    remote_opts = [
      target_identity: route.address,
      inbound_opts: metadata(route, :inbound_opts, []),
      on_result: metadata(route, :on_result)
    ]

    try do
      case :erpc.call(
             remote_node,
             __MODULE__,
             :accept_remote,
             [endpoint, envelope, node(), remote_opts],
             timeout
           ) do
        {:ok, receipt} -> {:ok, %{receipt | route_id: route.id, via: :beam_node}}
        {:error, %Error{} = error} -> {:error, %{error | route_id: error.route_id || route.id}}
        {:error, reason} -> remote_error(reason, envelope, route)
        other -> remote_error({:invalid_node_result, other}, envelope, route)
      end
    catch
      :error, {:erpc, :noconnection} ->
        {:error,
         Error.not_sent(:transport, :node_not_connected,
           message_id: envelope.id,
           route_id: route.id
         )}

      :error, {:erpc, :timeout} ->
        {:error,
         Error.outcome_unknown(:transport, :node_timeout,
           message_id: envelope.id,
           route_id: route.id
         )}

      kind, reason ->
        {:error,
         Error.outcome_unknown(:transport, {:node_call_failed, kind, reason},
           message_id: envelope.id,
           route_id: route.id
         )}
    end
  end

  def deliver(%Route{} = route, %Envelope{} = envelope, _opts) do
    {:error,
     Error.not_sent(:routing, {:invalid_node_target, route.target},
       message_id: envelope.id,
       route_id: route.id
     )}
  end

  @doc false
  @spec probe(Route.t(), keyword()) :: {:ok, Reachability.t()}
  @impl Spectre.Pulse.Transport
  def probe(%Route{target: %{node: remote_node}} = route, _opts) do
    status =
      if remote_node == node() or remote_node in Node.list(), do: :reachable, else: :unreachable

    {:ok,
     Reachability.new(status,
       level: :route_known,
       via: :beam_node,
       valid_for_ms: 1_000,
       reason: if(status == :unreachable, do: :node_not_connected),
       metadata: %{route_id: route.id, node: remote_node}
     )}
  end

  def probe(%Route{} = route, _opts) do
    {:ok,
     Reachability.unknown(:invalid_node_target,
       via: :beam_node,
       metadata: %{route_id: route.id}
     )}
  end

  @doc false
  @spec accept_remote(term(), Envelope.t(), node(), keyword()) ::
          {:ok, Receipt.t()} | {:error, Error.t()}
  def accept_remote(endpoint, %Envelope{} = envelope, peer_node, opts) do
    inbound_opts = Keyword.get(opts, :inbound_opts, [])

    context = %{
      authenticated_identity: envelope.from,
      binding: :beam_node,
      peer: peer_node,
      target: if(is_atom(endpoint), do: endpoint),
      target_identity: Keyword.get(opts, :target_identity),
      verified: %{beam_node: peer_node}
    }

    endpoint_opts =
      inbound_opts
      |> maybe_put(:on_result, Keyword.get(opts, :on_result))
      |> Keyword.put(:via, :beam_node)

    Endpoint.accept(endpoint, envelope, context, endpoint_opts)
  end

  @spec remote_error(term(), Envelope.t(), Route.t()) :: {:error, Error.t()}
  defp remote_error(reason, envelope, route) do
    {:error,
     Error.outcome_unknown(:transport, reason,
       message_id: envelope.id,
       route_id: route.id
     )}
  end

  @spec metadata(Route.t(), atom(), term()) :: term()
  defp metadata(route, key, default \\ nil),
    do: Map.get(route.metadata, key, Map.get(route.metadata, Atom.to_string(key), default))

  @spec maybe_put(keyword(), atom(), term()) :: keyword()
  defp maybe_put(opts, _key, nil), do: opts
  defp maybe_put(opts, key, value), do: Keyword.put(opts, key, value)
end

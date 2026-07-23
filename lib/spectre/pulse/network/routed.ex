defmodule Spectre.Pulse.Network.Routed do
  @moduledoc """
  Stateless priority-route delivery with ambiguity-safe failover.
  """

  @behaviour Spectre.Pulse.Network

  alias Spectre.Pulse.Envelope
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Reachability
  alias Spectre.Pulse.Route
  alias Spectre.Pulse.Transport

  @doc false
  @spec deliver(Envelope.t(), keyword()) :: {:ok, Spectre.Pulse.Receipt.t()} | {:error, Error.t()}
  @impl Spectre.Pulse.Network
  def deliver(%Envelope{} = envelope, opts) do
    routes =
      opts
      |> Keyword.get(:routes, [])
      |> Enum.filter(&match?(%Route{address: address} when address == envelope.to, &1))
      |> Enum.sort_by(& &1.priority)

    case routes do
      [] -> {:error, Error.not_sent(:routing, {:no_route, envelope.to}, message_id: envelope.id)}
      routes -> try_routes(routes, envelope, opts, [])
    end
  end

  @doc false
  @spec probe(String.t(), keyword()) :: {:ok, Reachability.t()} | {:error, Error.t()}
  @impl Spectre.Pulse.Network
  def probe(_address, opts) do
    routes =
      opts
      |> Keyword.get(:routes, [])
      |> Enum.filter(&match?(%Route{}, &1))
      |> Enum.sort_by(& &1.priority)

    case routes do
      [] ->
        {:ok, Reachability.unknown(:no_route, level: :route_known)}

      [route | _rest] ->
        Transport.probe(route, opts)
    end
  end

  @spec try_routes([Route.t()], Envelope.t(), keyword(), [Error.t()]) ::
          {:ok, Spectre.Pulse.Receipt.t()} | {:error, Error.t()}
  defp try_routes([], envelope, _opts, errors) do
    {:error,
     Error.not_sent(:routing, {:all_routes_not_sent, Enum.reverse(errors)},
       message_id: envelope.id
     )}
  end

  defp try_routes([route | rest], envelope, opts, errors) do
    case Transport.dispatch(route, envelope, opts) do
      {:ok, receipt} ->
        {:ok, receipt}

      {:error, %Error{outcome: :not_sent} = error} ->
        try_routes(rest, envelope, opts, [error | errors])

      {:error, %Error{} = error} ->
        {:error, error}
    end
  end
end

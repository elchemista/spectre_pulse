defmodule Spectre.Pulse.Network.Routed do
  @moduledoc """
  Stateless priority-route delivery with ambiguity-safe failover.
  """

  @behaviour Spectre.Pulse.Network

  alias Spectre.Pulse.Address
  alias Spectre.Pulse.Envelope
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Options
  alias Spectre.Pulse.Reachability
  alias Spectre.Pulse.Route
  alias Spectre.Pulse.Transport

  @doc false
  @spec deliver(Envelope.t(), keyword()) :: {:ok, Spectre.Pulse.Receipt.t()} | {:error, Error.t()}
  @impl Spectre.Pulse.Network
  def deliver(%Envelope{} = envelope, opts) do
    with {:ok, opts} <- Options.keyword(opts),
         {:ok, routes} <- candidate_routes(Keyword.get(opts, :routes, []), envelope.to) do
      case routes do
        [] ->
          {:error, Error.not_sent(:routing, {:no_route, envelope.to}, message_id: envelope.id)}

        routes ->
          try_routes(routes, envelope, opts, [])
      end
    else
      {:error, %Error{} = error} ->
        {:error, error}

      {:error, reason} ->
        {:error, Error.not_sent(:routing, reason, message_id: envelope.id)}
    end
  end

  @doc false
  @spec probe(String.t(), keyword()) :: {:ok, Reachability.t()} | {:error, Error.t()}
  @impl Spectre.Pulse.Network
  def probe(address, opts) do
    with {:ok, opts} <- Options.keyword(opts),
         {:ok, address} <- Address.normalize(address),
         {:ok, routes} <- candidate_routes(Keyword.get(opts, :routes, []), address) do
      case routes do
        [] ->
          {:ok, Reachability.unknown(:no_route, level: :route_known)}

        [route | _rest] ->
          Transport.probe(route, opts)
      end
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, Error.not_sent(:routing, reason)}
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

  @spec candidate_routes(term(), String.t()) :: {:ok, [Route.t()]} | {:error, term()}
  defp candidate_routes(routes, address) when is_list(routes) do
    routes
    |> Enum.reduce_while({:ok, []}, fn route, {:ok, normalized} ->
      case Route.new(route) do
        {:ok, route} -> {:cont, {:ok, [route | normalized]}}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
    |> case do
      {:ok, normalized} ->
        candidates =
          normalized
          |> Enum.reverse()
          |> Enum.filter(&(&1.address == address))
          |> Enum.sort_by(& &1.priority)

        {:ok, candidates}

      {:error, error} ->
        {:error, error}
    end
  end

  defp candidate_routes(routes, _address), do: {:error, {:invalid_routes, routes}}
end

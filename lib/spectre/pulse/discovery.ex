defmodule Spectre.Pulse.Discovery do
  @moduledoc """
  Automatic logical-name and physical-route discovery.

  Discovery plays the role of a small routing stack:

  * a ContactBook maps an Agent-local name to a canonical Pulse address;
  * directories act like DNS/service discovery;
  * local subscriptions and the Fabric act like the local route table;
  * transport routes remain invisible to Agent reasoning.
  """

  alias Spectre.Pulse.Address
  alias Spectre.Pulse.ContactBook
  alias Spectre.Pulse.Directory
  alias Spectre.Pulse.Directory.Resolution
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Fabric
  alias Spectre.Pulse.Local
  alias Spectre.Pulse.Route

  @doc "Resolves a local contact reference without selecting a transport."
  @spec resolve_identity(ContactBook.t(), term(), keyword()) ::
          {:ok, Resolution.t()} | {:error, Error.t()}
  def resolve_identity(%ContactBook{} = book, reference, opts \\ []) do
    case Directory.resolve(book, reference, opts) do
      {:ok, resolution} ->
        {:ok, resolution}

      {:error, local_error} ->
        resolve_from_directories(directories(opts), reference, opts, local_error)
    end
  end

  @doc """
  Discovers all current physical routes for an address.

  Local subscriptions are preferred, followed by connected Fabric routes and
  routes returned by configured directories. Explicit routes are supported as
  infrastructure input for compatibility, but are never required in an Agent.
  """
  @spec routes(String.t(), keyword()) :: {:ok, [Route.t()]} | {:error, Error.t()}
  def routes(address, opts \\ []) do
    with {:ok, canonical} <- Address.normalize(address),
         {:ok, explicit} <- normalize_routes(Keyword.get(opts, :routes, [])) do
      base = Local.routes(canonical) ++ fabric_routes(canonical) ++ explicit
      {route_chunks, errors} = discover_directory_routes(directories(opts), canonical, opts)
      routes = Enum.concat([base | Enum.reverse(route_chunks)])

      finalize_routes(routes, errors, canonical)
    end
  end

  @doc "Returns all configured Directory providers in deterministic order."
  @spec directories(keyword()) :: [term()]
  def directories(opts \\ []) do
    per_agent = Keyword.get(opts, :directory)
    per_call = List.wrap(Keyword.get(opts, :directories, []))
    application = List.wrap(Application.get_env(:spectre_pulse, :directories, []))

    [per_agent | per_call ++ application]
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
  end

  @spec resolve_from_directories([term()], term(), keyword(), Error.t()) ::
          {:ok, Resolution.t()} | {:error, Error.t()}
  defp resolve_from_directories([], _reference, _opts, error), do: {:error, error}

  defp resolve_from_directories([directory | rest], reference, opts, _last_error) do
    case Directory.resolve(directory, reference, opts) do
      {:ok, resolution} -> {:ok, resolution}
      {:error, error} -> resolve_from_directories(rest, reference, opts, error)
    end
  end

  @spec discover_directory_routes([term()], String.t(), keyword()) ::
          {[[Route.t()]], [Error.t()]}
  defp discover_directory_routes(directories, address, opts) do
    Enum.reduce(directories, {[], []}, fn directory, {route_chunks, errors} ->
      case Directory.routes(directory, address, opts) do
        {:ok, routes} -> {[routes | route_chunks], errors}
        {:error, error} -> {route_chunks, [error | errors]}
      end
    end)
  end

  @spec finalize_routes([Route.t()], [Error.t()], String.t()) ::
          {:ok, [Route.t()]} | {:error, Error.t()}
  defp finalize_routes(routes, errors, address) do
    routes
    |> normalize_routes()
    |> finalize_normalized_routes(errors, address)
  end

  @spec finalize_normalized_routes(
          {:ok, [Route.t()]} | {:error, Error.t()},
          [Error.t()],
          String.t()
        ) :: {:ok, [Route.t()]} | {:error, Error.t()}
  defp finalize_normalized_routes({:ok, []}, [_ | _] = errors, address) do
    {:error, Error.not_sent(:routing, {:discovery_failed, address, Enum.reverse(errors)})}
  end

  defp finalize_normalized_routes({:ok, routes}, _errors, _address) do
    {:ok,
     routes
     |> Enum.sort_by(& &1.priority)
     |> Enum.uniq_by(&{&1.transport, &1.target})}
  end

  defp finalize_normalized_routes({:error, %Error{} = error}, _errors, _address),
    do: {:error, error}

  @spec fabric_routes(String.t()) :: [Route.t()]
  defp fabric_routes(address) do
    case Fabric.routes(address) do
      routes when is_list(routes) -> routes
      {:error, _error} -> []
    end
  end

  @spec normalize_routes(term()) :: {:ok, [Route.t()]} | {:error, Error.t()}
  defp normalize_routes(routes) when is_list(routes) do
    Enum.reduce_while(routes, {:ok, []}, fn route, {:ok, acc} ->
      case Route.new(route) do
        {:ok, normalized} -> {:cont, {:ok, [normalized | acc]}}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
    |> case do
      {:ok, normalized} -> {:ok, Enum.reverse(normalized)}
      error -> error
    end
  end

  defp normalize_routes(value),
    do: {:error, Error.not_sent(:routing, {:invalid_routes, value})}
end

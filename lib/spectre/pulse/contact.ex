defmodule Spectre.Pulse.Contact do
  @moduledoc """
  One agent-owned address-book entry.

  `key` is local and never travels on the wire. A contact deliberately has no
  ambiguous `trust` flag: authentication and authorization are separate facts.
  """

  alias Spectre.Pulse.Address
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Route

  @enforce_keys [:key, :identity]
  defstruct [:key, :identity, :display_name, capabilities: [], routes: [], metadata: %{}]

  @type key :: atom() | String.t()

  @type t :: %__MODULE__{
          key: key(),
          identity: String.t(),
          display_name: String.t() | nil,
          capabilities: [atom() | String.t()],
          routes: [Route.t()],
          metadata: map()
        }

  @doc "Builds a contact."
  @spec new(key(), String.t(), keyword()) :: {:ok, t()} | {:error, Error.t()}
  def new(key, identity, opts \\ []) do
    new(Keyword.merge(opts, key: key, identity: identity))
  end

  @doc "Builds a contact from a map."
  @spec new(map() | keyword() | t()) :: {:ok, t()} | {:error, Error.t()}
  def new(%__MODULE__{} = contact), do: validate(contact)
  def new(attrs) when is_list(attrs), do: attrs |> Map.new() |> new()

  def new(attrs) when is_map(attrs) do
    if Map.has_key?(attrs, :trust) or Map.has_key?(attrs, "trust") do
      {:error, Error.not_sent(:validation, :contact_trust_is_not_a_protocol_fact)}
    else
      with {:ok, identity} <- Address.normalize(attr(attrs, :identity)),
           {:ok, routes} <- normalize_routes(attr(attrs, :routes, [])) do
        contact = %__MODULE__{
          key: attr(attrs, :key),
          identity: identity,
          display_name: attr(attrs, :display_name),
          capabilities: attr(attrs, :capabilities, []),
          routes: routes,
          metadata: attr(attrs, :metadata, %{})
        }

        validate(contact)
      end
    end
  end

  def new(value),
    do: {:error, Error.not_sent(:validation, {:invalid_contact, value})}

  @doc "Like `new/1`, but raises for invalid input."
  @spec new!(key(), String.t(), keyword()) :: t()
  def new!(key, identity, opts \\ []) do
    case new(key, identity, opts) do
      {:ok, contact} -> contact
      {:error, error} -> raise ArgumentError, Exception.message(error)
    end
  end

  @spec validate(t()) :: {:ok, t()} | {:error, Error.t()}
  defp validate(%__MODULE__{} = contact) do
    cond do
      not valid_key?(contact.key) ->
        {:error, Error.not_sent(:validation, {:invalid_contact_key, contact.key})}

      not is_list(contact.capabilities) ->
        {:error, Error.not_sent(:validation, {:invalid_capabilities, contact.capabilities})}

      not is_map(contact.metadata) ->
        {:error, Error.not_sent(:validation, {:invalid_contact_metadata, contact.metadata})}

      Enum.any?(contact.routes, &(&1.address != contact.identity)) ->
        {:error, Error.not_sent(:validation, :contact_route_identity_mismatch)}

      true ->
        {:ok,
         %{
           contact
           | capabilities: Enum.uniq(contact.capabilities),
             routes: Enum.sort_by(contact.routes, & &1.priority)
         }}
    end
  end

  @spec valid_key?(term()) :: boolean()
  defp valid_key?(key) when is_atom(key), do: not is_nil(key)
  defp valid_key?(key) when is_binary(key), do: String.trim(key) != ""
  defp valid_key?(_key), do: false

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
    do: {:error, Error.not_sent(:validation, {:invalid_contact_routes, value})}

  @spec attr(map(), atom(), term()) :: term()
  defp attr(map, key, default \\ nil),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key), default))
end

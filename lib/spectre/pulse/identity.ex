defmodule Spectre.Pulse.Identity do
  @moduledoc """
  A public, descriptive identity document for one agent.

  Capabilities are claims for discovery, not grants of authorization.
  """

  alias Spectre.Pulse.Address
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Protocol

  @enforce_keys [:address]
  defstruct [:address, :display_name, protocol_versions: [1], capabilities: [], metadata: %{}]

  @type t :: %__MODULE__{
          address: String.t(),
          display_name: String.t() | nil,
          protocol_versions: [pos_integer()],
          capabilities: [atom() | String.t()],
          metadata: map()
        }

  @doc "Builds a public identity document."
  @spec new(String.t() | map() | keyword(), keyword()) :: {:ok, t()} | {:error, Error.t()}
  def new(address_or_attrs, opts \\ [])

  def new(address, opts) when is_binary(address) do
    new(Keyword.put(opts, :address, address))
  end

  def new(attrs, opts) when is_list(attrs), do: attrs |> Map.new() |> new(opts)

  def new(attrs, _opts) when is_map(attrs) do
    with {:ok, address} <- Address.normalize(attr(attrs, :address)) do
      versions = attr(attrs, :protocol_versions, [Protocol.version()])
      capabilities = attr(attrs, :capabilities, [])
      metadata = attr(attrs, :metadata, %{})

      cond do
        not is_list(versions) or Protocol.version() not in versions ->
          {:error, Error.not_sent(:validation, {:invalid_protocol_versions, versions})}

        not is_list(capabilities) ->
          {:error, Error.not_sent(:validation, {:invalid_capabilities, capabilities})}

        not is_map(metadata) ->
          {:error, Error.not_sent(:validation, {:invalid_identity_metadata, metadata})}

        true ->
          {:ok,
           %__MODULE__{
             address: address,
             display_name: attr(attrs, :display_name),
             protocol_versions: versions,
             capabilities: Enum.uniq(capabilities),
             metadata: metadata
           }}
      end
    end
  end

  def new(value, _opts),
    do: {:error, Error.not_sent(:validation, {:invalid_identity, value})}

  @doc "Like `new/2`, but raises for invalid input."
  @spec new!(String.t() | map() | keyword(), keyword()) :: t()
  def new!(value, opts \\ []) do
    case new(value, opts) do
      {:ok, identity} -> identity
      {:error, error} -> raise ArgumentError, Exception.message(error)
    end
  end

  @doc "Returns the safe public projection used by identity describe."
  @spec to_public_map(t()) :: map()
  def to_public_map(%__MODULE__{} = identity) do
    %{
      "address" => identity.address,
      "display_name" => identity.display_name,
      "protocol_versions" => identity.protocol_versions,
      "capabilities" => Enum.map(identity.capabilities, &to_string/1),
      "metadata" => identity.metadata
    }
  end

  @spec attr(map(), atom(), term()) :: term()
  defp attr(map, key, default \\ nil),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key), default))
end

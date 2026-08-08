defmodule Spectre.Pulse.Identity do
  @moduledoc """
  A public, descriptive identity document for one agent.

  Capabilities are claims for discovery, not grants of authorization.
  """

  alias Spectre.Pulse.Address
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Options
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
    case Options.keyword(opts) do
      {:ok, opts} -> new(Keyword.put(opts, :address, address), [])
      {:error, reason} -> {:error, Error.not_sent(:validation, reason)}
    end
  end

  def new(attrs, opts) when is_list(attrs) do
    if Keyword.keyword?(attrs),
      do: attrs |> Map.new() |> new(opts),
      else: {:error, Error.not_sent(:validation, {:invalid_identity, attrs})}
  end

  def new(attrs, opts) when is_map(attrs) do
    with {:ok, opts} <- Options.keyword(opts),
         attrs <- Map.merge(attrs, Map.new(opts)),
         {:ok, address} <- Address.normalize(attr(attrs, :address)),
         {:ok, versions} <-
           validate_versions(attr(attrs, :protocol_versions, [Protocol.version()])),
         {:ok, display_name} <- validate_display_name(attr(attrs, :display_name)),
         {:ok, capabilities} <- validate_capabilities(attr(attrs, :capabilities, [])),
         {:ok, metadata} <- validate_metadata(attr(attrs, :metadata, %{})) do
      {:ok,
       %__MODULE__{
         address: address,
         display_name: display_name,
         protocol_versions: versions,
         capabilities: capabilities,
         metadata: metadata
       }}
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, Error.not_sent(:validation, reason)}
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

  @spec valid_version?(term()) :: boolean()
  defp valid_version?(version), do: is_integer(version) and version > 0

  @spec validate_versions(term()) :: {:ok, [pos_integer()]} | {:error, Error.t()}
  defp validate_versions(versions) when is_list(versions) do
    if Enum.all?(versions, &valid_version?/1) and Protocol.version() in versions,
      do: {:ok, Enum.uniq(versions)},
      else: {:error, Error.not_sent(:validation, {:invalid_protocol_versions, versions})}
  end

  defp validate_versions(versions),
    do: {:error, Error.not_sent(:validation, {:invalid_protocol_versions, versions})}

  @spec validate_display_name(term()) :: {:ok, String.t() | nil} | {:error, Error.t()}
  defp validate_display_name(nil), do: {:ok, nil}

  defp validate_display_name(display_name) when is_binary(display_name) do
    if String.valid?(display_name),
      do: {:ok, display_name},
      else: {:error, Error.not_sent(:validation, {:invalid_display_name, display_name})}
  end

  defp validate_display_name(display_name),
    do: {:error, Error.not_sent(:validation, {:invalid_display_name, display_name})}

  @spec validate_capabilities(term()) ::
          {:ok, [atom() | String.t()]} | {:error, Error.t()}
  defp validate_capabilities(capabilities) when is_list(capabilities) do
    if Enum.all?(capabilities, &valid_capability?/1),
      do: {:ok, Enum.uniq(capabilities)},
      else: {:error, Error.not_sent(:validation, {:invalid_capabilities, capabilities})}
  end

  defp validate_capabilities(capabilities),
    do: {:error, Error.not_sent(:validation, {:invalid_capabilities, capabilities})}

  @spec valid_capability?(term()) :: boolean()
  defp valid_capability?(capability) when is_atom(capability), do: not is_nil(capability)

  defp valid_capability?(capability) when is_binary(capability),
    do: capability != "" and String.valid?(capability)

  defp valid_capability?(_capability), do: false

  @spec validate_metadata(term()) :: {:ok, map()} | {:error, Error.t()}
  defp validate_metadata(metadata) when is_map(metadata) do
    if json_encodable?(metadata),
      do: {:ok, metadata},
      else: {:error, Error.not_sent(:validation, {:identity_metadata_not_encodable, metadata})}
  end

  defp validate_metadata(metadata),
    do: {:error, Error.not_sent(:validation, {:invalid_identity_metadata, metadata})}

  @spec json_encodable?(map()) :: boolean()
  defp json_encodable?(value) do
    match?({:ok, _encoded}, Jason.encode(value))
  rescue
    _exception -> false
  catch
    _kind, _reason -> false
  end
end

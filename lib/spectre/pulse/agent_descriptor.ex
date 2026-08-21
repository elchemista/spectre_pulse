defmodule Spectre.Pulse.AgentDescriptor do
  @moduledoc """
  Public description of one Agent exposed through a Pulse connection.

  The local module is retained only for host-side selection. `to_wire/1`
  deliberately removes it so runtime implementation details never cross a
  transport boundary.
  """

  alias Spectre.Pulse.Address
  alias Spectre.Pulse.Config
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Identity

  @enforce_keys [:address]
  defstruct [
    :address,
    :module,
    :display_name,
    protocol_versions: [1],
    capabilities: [],
    metadata: %{}
  ]

  @type t :: %__MODULE__{
          address: String.t(),
          module: module() | nil,
          display_name: String.t() | nil,
          protocol_versions: [pos_integer()],
          capabilities: [String.t()],
          metadata: map()
        }

  @doc "Builds a descriptor from a Pulse-enabled Agent module."
  @spec for_agent(module()) :: {:ok, t()} | {:error, Error.t()}
  def for_agent(agent) when is_atom(agent) and not is_nil(agent) do
    with {:ok, config} <- Config.fetch(agent) do
      identity = Config.public_identity(config)

      new(%{
        address: identity.address,
        module: agent,
        display_name: identity.display_name,
        protocol_versions: identity.protocol_versions,
        capabilities: identity.capabilities,
        metadata: identity.metadata
      })
    end
  end

  def for_agent(agent),
    do: {:error, Error.not_sent(:validation, {:invalid_pulse_agent, agent})}

  @doc "Normalizes a local or remote Agent descriptor."
  @spec new(t() | map() | keyword()) :: {:ok, t()} | {:error, Error.t()}
  def new(%__MODULE__{} = descriptor), do: descriptor |> Map.from_struct() |> new()

  def new(attrs) when is_list(attrs) do
    if Keyword.keyword?(attrs),
      do: attrs |> Map.new() |> new(),
      else: invalid(attrs)
  end

  def new(attrs) when is_map(attrs) do
    with {:ok, address} <- Address.normalize(attr(attrs, :address)),
         {:ok, identity} <-
           Identity.new(%{
             address: address,
             display_name: attr(attrs, :display_name),
             protocol_versions: attr(attrs, :protocol_versions, [1]),
             capabilities: attr(attrs, :capabilities, []),
             metadata: attr(attrs, :metadata, %{})
           }),
         {:ok, module} <- validate_module(attr(attrs, :module)) do
      {:ok,
       %__MODULE__{
         address: identity.address,
         module: module,
         display_name: identity.display_name,
         protocol_versions: identity.protocol_versions,
         capabilities: Enum.map(identity.capabilities, &to_string/1),
         metadata: identity.metadata
       }}
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, Error.not_sent(:validation, reason)}
    end
  end

  def new(value), do: invalid(value)

  @doc "Returns the transport-safe descriptor projection."
  @spec to_wire(t()) :: map()
  def to_wire(%__MODULE__{} = descriptor) do
    %{
      "address" => descriptor.address,
      "display_name" => descriptor.display_name,
      "protocol_versions" => descriptor.protocol_versions,
      "capabilities" => descriptor.capabilities,
      "metadata" => descriptor.metadata
    }
  end

  @spec validate_module(term()) :: {:ok, module() | nil} | {:error, term()}
  defp validate_module(nil), do: {:ok, nil}
  defp validate_module(module) when is_atom(module) and not is_nil(module), do: {:ok, module}
  defp validate_module(module), do: {:error, {:invalid_agent_descriptor_module, module}}

  @spec invalid(term()) :: {:error, Error.t()}
  defp invalid(value),
    do: {:error, Error.not_sent(:validation, {:invalid_agent_descriptor, value})}

  @spec attr(map(), atom(), term()) :: term()
  defp attr(map, key, default \\ nil),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key), default))
end

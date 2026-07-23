defmodule Spectre.Pulse.Config do
  @moduledoc """
  Immutable Pulse configuration compiled into a Spectre Agent.
  """

  alias Spectre.Pulse.Address
  alias Spectre.Pulse.ContactBook
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Identity

  @enforce_keys [:identity]
  defstruct [
    :identity,
    :directory,
    :network,
    state_scope: :agent,
    contacts: %ContactBook{},
    advertise: %{},
    inbound: []
  ]

  @type state_scope ::
          :agent
          | :peer
          | (module(), Spectre.Pulse.Envelope.t(), map() -> term())
          | {module(), atom(), list()}

  @type t :: %__MODULE__{
          identity: String.t(),
          directory: term(),
          network: term(),
          state_scope: state_scope(),
          contacts: ContactBook.t(),
          advertise: map(),
          inbound: keyword()
        }

  @doc "Builds a validated agent configuration."
  @spec new(map() | keyword()) :: {:ok, t()} | {:error, Error.t()}
  def new(attrs) when is_list(attrs), do: attrs |> Map.new() |> new()

  def new(attrs) when is_map(attrs) do
    with {:ok, identity} <- Address.normalize(attr(attrs, :identity)),
         {:ok, contacts} <- normalize_contacts(attr(attrs, :contacts, [])),
         :ok <- validate_scope(attr(attrs, :state_scope, :agent)),
         :ok <- validate_advertise(attr(attrs, :advertise, %{})) do
      {:ok,
       %__MODULE__{
         identity: identity,
         directory: attr(attrs, :directory),
         network: attr(attrs, :network),
         state_scope: attr(attrs, :state_scope, :agent),
         contacts: contacts,
         advertise: attr(attrs, :advertise, %{}),
         inbound: attr(attrs, :inbound, [])
       }}
    end
  end

  def new(value),
    do: {:error, Error.not_sent(:validation, {:invalid_pulse_config, value})}

  @doc "Like `new/1`, but raises for invalid configuration."
  @spec new!(map() | keyword()) :: t()
  def new!(attrs) do
    case new(attrs) do
      {:ok, config} -> config
      {:error, error} -> raise ArgumentError, Exception.message(error)
    end
  end

  @doc "Returns the public identity projection advertised by this agent."
  @spec public_identity(t()) :: Identity.t()
  def public_identity(%__MODULE__{} = config) do
    Identity.new!(
      address: config.identity,
      display_name: attr(config.advertise, :display_name),
      capabilities: attr(config.advertise, :capabilities, []),
      metadata: attr(config.advertise, :metadata, %{})
    )
  end

  @doc "Fetches compiled Pulse configuration from an Agent module."
  @spec fetch(module()) :: {:ok, t()} | {:error, Error.t()}
  def fetch(agent) when is_atom(agent) do
    if Code.ensure_loaded?(agent) and function_exported?(agent, :__spectre_pulse__, 0) do
      case agent.__spectre_pulse__() do
        %__MODULE__{} = config -> {:ok, config}
        other -> new(other)
      end
    else
      {:error, Error.not_sent(:routing, {:agent_not_pulse_enabled, agent})}
    end
  end

  @spec normalize_contacts(term()) :: {:ok, ContactBook.t()} | {:error, Error.t()}
  defp normalize_contacts(%ContactBook{} = contacts), do: {:ok, contacts}
  defp normalize_contacts(contacts) when is_list(contacts), do: ContactBook.new(contacts)

  defp normalize_contacts(value),
    do: {:error, Error.not_sent(:validation, {:invalid_contact_book, value})}

  @spec validate_scope(term()) :: :ok | {:error, Error.t()}
  defp validate_scope(scope) when scope in [:agent, :peer], do: :ok
  defp validate_scope(scope) when is_function(scope, 3), do: :ok

  defp validate_scope({module, function, args})
       when is_atom(module) and is_atom(function) and is_list(args),
       do: :ok

  defp validate_scope(scope),
    do: {:error, Error.not_sent(:validation, {:invalid_state_scope, scope})}

  @spec validate_advertise(term()) :: :ok | {:error, Error.t()}
  defp validate_advertise(advertise) when is_map(advertise), do: :ok

  defp validate_advertise(advertise),
    do: {:error, Error.not_sent(:validation, {:invalid_advertise_config, advertise})}

  @spec attr(map(), atom(), term()) :: term()
  defp attr(map, key, default \\ nil),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key), default))
end

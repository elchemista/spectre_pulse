defmodule Spectre.Pulse.Address do
  @moduledoc """
  A canonical logical identity such as `spectre://acme/tao`.

  An address identifies an agent. It deliberately contains no REST URL,
  socket, BEAM pid, node name, or other physical route.
  """

  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Protocol

  @agent_namespace "spectre-pulse/agent/v1:"

  defstruct [:value, :authority, :agent]

  @type t :: %__MODULE__{
          value: String.t(),
          authority: String.t(),
          agent: String.t()
        }

  @doc "Parses and canonicalizes a Pulse address."
  @spec new(t() | String.t(), keyword()) :: {:ok, t()} | {:error, Error.t()}
  def new(address, opts \\ [])
  def new(%__MODULE__{} = address, _opts), do: {:ok, address}

  def new(address, opts) when is_binary(address) do
    max_bytes =
      opts
      |> Keyword.get(:max_address_bytes, Protocol.default_limits().max_address_bytes)

    with :ok <- validate_size(address, max_bytes),
         %URI{} = uri <- URI.parse(address),
         :ok <- validate_uri(uri),
         {:ok, agent} <- normalize_agent(uri.path) do
      authority = String.downcase(uri.host)
      value = "spectre://" <> authority <> "/" <> agent
      {:ok, %__MODULE__{value: value, authority: authority, agent: agent}}
    else
      {:error, reason} -> {:error, Error.not_sent(:validation, {:invalid_address, reason})}
    end
  end

  def new(address, _opts),
    do: {:error, Error.not_sent(:validation, {:invalid_address, address})}

  @doc "Like `new/2`, but raises `ArgumentError` for invalid input."
  @spec new!(t() | String.t(), keyword()) :: t()
  def new!(address, opts \\ []) do
    case new(address, opts) do
      {:ok, parsed} -> parsed
      {:error, error} -> raise ArgumentError, Exception.message(error)
    end
  end

  @doc "Returns the canonical string representation."
  @spec normalize(t() | String.t(), keyword()) :: {:ok, String.t()} | {:error, Error.t()}
  def normalize(address, opts \\ []) do
    with {:ok, parsed} <- new(address, opts), do: {:ok, parsed.value}
  end

  @doc "Returns the canonical string or raises."
  @spec normalize!(t() | String.t(), keyword()) :: String.t()
  def normalize!(address, opts \\ []), do: address |> new!(opts) |> to_string()

  @doc """
  Derives the stable 128-bit network identifier assigned to an Agent module.

  The identifier is deterministic across process and node restarts. It is an
  address, not an authentication credential.
  """
  @spec agent_id(module()) :: <<_::256>>
  def agent_id(agent) when is_atom(agent) and not is_nil(agent) do
    :crypto.hash(:sha256, @agent_namespace <> Atom.to_string(agent))
    |> binary_part(0, 16)
    |> Base.encode16(case: :lower)
  end

  @doc """
  Returns the default canonical address assigned to a Pulse-enabled Agent.

  Applications may override it with `identity/1` when a public, human-readable
  or externally durable address is required.
  """
  @spec for_agent(module()) :: String.t()
  def for_agent(agent), do: "spectre://pulse/" <> agent_id(agent)

  @doc "Compares two addresses after canonicalization."
  @spec equal?(t() | String.t(), t() | String.t()) :: boolean()
  def equal?(left, right) do
    with {:ok, canonical_left} <- normalize(left),
         {:ok, canonical_right} <- normalize(right) do
      canonical_left == canonical_right
    else
      _ -> false
    end
  end

  defimpl String.Chars do
    @doc false
    @spec to_string(Spectre.Pulse.Address.t()) :: String.t()
    @impl String.Chars
    def to_string(address), do: address.value
  end

  @spec validate_size(String.t(), pos_integer()) :: :ok | {:error, term()}
  defp validate_size(address, max_bytes)
       when is_integer(max_bytes) and max_bytes > 0 and byte_size(address) <= max_bytes,
       do: :ok

  defp validate_size(address, max_bytes),
    do: {:error, {:address_too_large, byte_size(address), max_bytes}}

  @spec validate_uri(URI.t()) :: :ok | {:error, term()}
  defp validate_uri(%URI{
         scheme: scheme,
         host: host,
         path: path,
         port: nil,
         query: nil,
         fragment: nil,
         userinfo: nil
       })
       when is_binary(scheme) and is_binary(host) and is_binary(path) do
    cond do
      String.downcase(scheme) != "spectre" -> {:error, {:invalid_scheme, scheme}}
      host == "" -> {:error, :authority_required}
      not valid_authority?(host) -> {:error, {:invalid_authority, host}}
      true -> :ok
    end
  end

  defp validate_uri(_uri), do: {:error, :address_must_be_logical}

  @spec normalize_agent(String.t()) :: {:ok, String.t()} | {:error, term()}
  defp normalize_agent(path) do
    agent = String.trim_leading(path, "/")

    cond do
      agent == "" ->
        {:error, :agent_required}

      String.ends_with?(agent, "/") ->
        {:error, {:invalid_agent, agent}}

      String.contains?(agent, ["//", "\\"]) ->
        {:error, {:invalid_agent, agent}}

      not String.valid?(agent) ->
        {:error, {:invalid_agent, :invalid_utf8}}

      true ->
        {:ok, agent}
    end
  end

  @spec valid_authority?(String.t()) :: boolean()
  defp valid_authority?(authority) do
    Regex.match?(~r/^[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?$/, authority)
  end
end

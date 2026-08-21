defmodule Spectre.Pulse.Principal do
  @moduledoc """
  Authenticated peer identity attached to a Pulse connection.

  A Principal contains the result of authentication, never the credential
  used to authenticate. Granted scopes remain separate from advertised Agent
  capabilities.
  """

  alias Spectre.Pulse.Address
  alias Spectre.Pulse.Error

  @enforce_keys [:id]
  defstruct [:id, :identity, kind: :peer, scopes: [], verified: %{}, metadata: %{}]

  @type t :: %__MODULE__{
          id: String.t(),
          identity: String.t() | nil,
          kind: atom() | String.t(),
          scopes: [String.t()],
          verified: map(),
          metadata: map()
        }

  @doc "Normalizes an authenticated principal."
  @spec new(t() | map() | keyword()) :: {:ok, t()} | {:error, Error.t()}
  def new(%__MODULE__{} = principal), do: principal |> Map.from_struct() |> new()

  def new(attrs) when is_list(attrs) do
    if Keyword.keyword?(attrs),
      do: attrs |> Map.new() |> new(),
      else: invalid(attrs)
  end

  def new(attrs) when is_map(attrs) do
    with {:ok, id} <- nonempty_string(attr(attrs, :id), :principal_id),
         {:ok, identity} <- identity(attr(attrs, :identity)),
         {:ok, kind} <- kind(attr(attrs, :kind, :peer)),
         {:ok, scopes} <- string_list(attr(attrs, :scopes, []), :principal_scopes),
         {:ok, verified} <- plain_map(attr(attrs, :verified, %{}), :principal_verified),
         {:ok, metadata} <- plain_map(attr(attrs, :metadata, %{}), :principal_metadata) do
      {:ok,
       %__MODULE__{
         id: id,
         identity: identity,
         kind: kind,
         scopes: scopes,
         verified: verified,
         metadata: metadata
       }}
    else
      {:error, reason} -> {:error, Error.not_sent(:validation, reason)}
    end
  end

  def new(value), do: invalid(value)

  @spec nonempty_string(term(), atom()) :: {:ok, String.t()} | {:error, term()}
  defp nonempty_string(value, _field) when is_binary(value) do
    if String.valid?(value) and String.trim(value) != "",
      do: {:ok, value},
      else: {:error, {:invalid_principal_id, value}}
  end

  defp nonempty_string(value, field), do: {:error, {:invalid_principal_field, field, value}}

  @spec kind(term()) :: {:ok, atom() | String.t()} | {:error, term()}
  defp kind(value) when is_atom(value) and not is_nil(value), do: {:ok, value}

  defp kind(value) when is_binary(value) do
    if String.valid?(value) and String.trim(value) != "",
      do: {:ok, value},
      else: {:error, {:invalid_principal_kind, value}}
  end

  defp kind(value), do: {:error, {:invalid_principal_kind, value}}

  @spec identity(term()) :: {:ok, String.t() | nil} | {:error, term()}
  defp identity(nil), do: {:ok, nil}

  defp identity(value) when is_binary(value) do
    case Address.normalize(value) do
      {:ok, address} -> {:ok, address}
      {:error, _error} -> {:error, {:invalid_principal_identity, value}}
    end
  end

  defp identity(value), do: {:error, {:invalid_principal_identity, value}}

  @spec string_list(term(), atom()) :: {:ok, [String.t()]} | {:error, term()}
  defp string_list(values, _field) when is_list(values) do
    if Enum.all?(values, &valid_name?/1),
      do: {:ok, values |> Enum.map(&to_string/1) |> Enum.uniq()},
      else: {:error, {:invalid_principal_scopes, values}}
  end

  defp string_list(value, _field), do: {:error, {:invalid_principal_scopes, value}}

  @spec valid_name?(term()) :: boolean()
  defp valid_name?(value) when is_atom(value), do: not is_nil(value)
  defp valid_name?(value) when is_binary(value), do: String.valid?(value) and value != ""
  defp valid_name?(_value), do: false

  @spec plain_map(term(), atom()) :: {:ok, map()} | {:error, term()}
  defp plain_map(value, _field) when is_map(value), do: {:ok, value}
  defp plain_map(value, field), do: {:error, {:invalid_principal_field, field, value}}

  @spec invalid(term()) :: {:error, Error.t()}
  defp invalid(value), do: {:error, Error.not_sent(:validation, {:invalid_principal, value})}

  @spec attr(map(), atom(), term()) :: term()
  defp attr(map, key, default \\ nil),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key), default))
end

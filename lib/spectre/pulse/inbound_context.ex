defmodule Spectre.Pulse.InboundContext do
  @moduledoc """
  Facts supplied by a transport for one inbound envelope.

  These fields are host/transport assertions. They are kept separate from the
  sender-declared `Envelope.metadata`.
  """

  alias Spectre.Pulse.Address
  alias Spectre.Pulse.Error

  defstruct [
    :authenticated_identity,
    :binding,
    :peer,
    :target,
    :target_identity,
    :resolver,
    :authorization,
    :subject,
    :instance_supervisor,
    :instance_registry,
    :instance_opts,
    verified: %{},
    metadata: %{}
  ]

  @type t :: %__MODULE__{
          authenticated_identity: String.t() | nil,
          binding: atom() | String.t() | nil,
          peer: term(),
          target: module() | Spectre.AgentRef.t() | GenServer.server() | nil,
          target_identity: String.t() | nil,
          resolver: term(),
          authorization: term(),
          subject: Spectre.Subject.t() | term() | nil,
          instance_supervisor: GenServer.server() | nil,
          instance_registry: atom() | nil,
          instance_opts: keyword() | nil,
          verified: map(),
          metadata: map()
        }

  @doc "Normalizes a context struct, map, or keyword list."
  @spec new(t() | map() | keyword()) :: t()
  def new(context) do
    case normalize(context) do
      {:ok, normalized} -> normalized
      {:error, error} -> raise ArgumentError, Exception.message(error)
    end
  end

  @doc false
  @spec normalize(t() | map() | keyword() | term()) :: {:ok, t()} | {:error, Error.t()}
  def normalize(%__MODULE__{} = context), do: validate(context)

  def normalize(context) when is_list(context) do
    if Keyword.keyword?(context),
      do: context |> Map.new() |> normalize(),
      else: invalid(context)
  end

  def normalize(context) when is_map(context) do
    fields = __MODULE__.__struct__() |> Map.keys() |> List.delete(:__struct__)

    context =
      Enum.reduce(fields, %{}, fn field, acc ->
        case fetch_field(context, field) do
          {:ok, value} -> Map.put(acc, field, value)
          :error -> acc
        end
      end)

    context
    |> then(&struct(__MODULE__, &1))
    |> validate()
  end

  def normalize(context), do: invalid(context)

  @spec fetch_field(map(), atom()) :: {:ok, term()} | :error
  defp fetch_field(context, field) do
    case Map.fetch(context, field) do
      {:ok, value} -> {:ok, value}
      :error -> Map.fetch(context, Atom.to_string(field))
    end
  end

  @spec validate(t()) :: {:ok, t()} | {:error, Error.t()}
  defp validate(context) do
    with {:ok, authenticated_identity} <-
           normalize_address(context.authenticated_identity, :authenticated_identity),
         {:ok, target_identity} <- normalize_address(context.target_identity, :target_identity),
         :ok <- validate_binding(context.binding),
         :ok <- validate_map(context.verified, :verified),
         :ok <- validate_map(context.metadata, :metadata),
         :ok <- validate_instance_registry(context.instance_registry),
         :ok <- validate_instance_opts(context.instance_opts) do
      {:ok,
       %{
         context
         | authenticated_identity: authenticated_identity,
           target_identity: target_identity
       }}
    end
  end

  @spec normalize_address(term(), atom()) :: {:ok, String.t() | nil} | {:error, Error.t()}
  defp normalize_address(nil, _field), do: {:ok, nil}

  defp normalize_address(value, field) when is_binary(value) do
    case Address.normalize(value) do
      {:ok, address} -> {:ok, address}
      {:error, _error} -> invalid({field, value})
    end
  end

  defp normalize_address(value, field), do: invalid({field, value})

  @spec validate_binding(term()) :: :ok | {:error, Error.t()}
  defp validate_binding(binding) when is_nil(binding) or is_atom(binding) or is_binary(binding),
    do: :ok

  defp validate_binding(binding), do: invalid({:binding, binding})

  @spec validate_map(term(), atom()) :: :ok | {:error, Error.t()}
  defp validate_map(value, _field) when is_map(value), do: :ok
  defp validate_map(value, field), do: invalid({field, value})

  @spec validate_instance_registry(term()) :: :ok | {:error, Error.t()}
  defp validate_instance_registry(nil), do: :ok
  defp validate_instance_registry(registry) when is_atom(registry), do: :ok
  defp validate_instance_registry(registry), do: invalid({:instance_registry, registry})

  @spec validate_instance_opts(term()) :: :ok | {:error, Error.t()}
  defp validate_instance_opts(nil), do: :ok

  defp validate_instance_opts(opts) when is_list(opts) do
    if Keyword.keyword?(opts), do: :ok, else: invalid({:instance_opts, opts})
  end

  defp validate_instance_opts(opts), do: invalid({:instance_opts, opts})

  @spec invalid(term()) :: {:error, Error.t()}
  defp invalid(value),
    do: {:error, Error.not_sent(:validation, {:invalid_inbound_context, value})}
end

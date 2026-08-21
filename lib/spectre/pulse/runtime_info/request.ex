defmodule Spectre.Pulse.RuntimeInfo.Request do
  @moduledoc false

  alias Spectre.Pulse.Error
  alias Spectre.Pulse.RuntimeInfo.Snapshot

  @allowed_options [
    :connection,
    :fields,
    :instance_registry,
    :max_collection_entries,
    :max_depth
  ]
  @default_max_collection_entries 128
  @hard_max_collection_entries 256
  @default_max_depth 6
  @hard_max_depth 8

  @enforce_keys [:fields, :instance_registry, :max_collection_entries, :max_depth]
  defstruct [:connection, :fields, :instance_registry, :max_collection_entries, :max_depth]

  @type t :: %__MODULE__{
          connection: term() | nil,
          fields: [atom()],
          instance_registry: atom(),
          max_collection_entries: pos_integer(),
          max_depth: pos_integer()
        }

  @doc false
  @spec new(term()) :: {:ok, t()} | {:error, Error.t()}
  def new(opts) when is_list(opts) do
    with :ok <- validate_keyword(opts),
         {:ok, fields} <- Snapshot.normalize_fields(Keyword.get(opts, :fields)),
         {:ok, registry} <- registry(Keyword.get(opts, :instance_registry)),
         {:ok, max_entries} <-
           bounded_positive_integer(
             Keyword.get(opts, :max_collection_entries, @default_max_collection_entries),
             :max_collection_entries,
             @hard_max_collection_entries
           ),
         {:ok, max_depth} <-
           bounded_positive_integer(
             Keyword.get(opts, :max_depth, @default_max_depth),
             :max_depth,
             @hard_max_depth
           ) do
      {:ok,
       %__MODULE__{
         connection: Keyword.get(opts, :connection),
         fields: fields,
         instance_registry: registry,
         max_collection_entries: max_entries,
         max_depth: max_depth
       }}
    end
  end

  def new(opts), do: invalid_options(opts)

  @spec validate_keyword(keyword()) :: :ok | {:error, Error.t()}
  defp validate_keyword(opts) do
    if Keyword.keyword?(opts) and Keyword.keys(opts) -- @allowed_options == [],
      do: :ok,
      else: invalid_options(opts)
  end

  @spec registry(term()) :: {:ok, atom()} | {:error, Error.t()}
  defp registry(nil), do: {:ok, Spectre.Instance.Registry}
  defp registry(value) when is_atom(value), do: {:ok, value}

  defp registry(_value),
    do: {:error, Error.not_sent(:validation, :invalid_runtime_info_instance_registry)}

  @spec bounded_positive_integer(term(), atom(), pos_integer()) ::
          {:ok, pos_integer()} | {:error, Error.t()}
  defp bounded_positive_integer(value, _field, maximum)
       when is_integer(value) and value > 0 and value <= maximum,
       do: {:ok, value}

  defp bounded_positive_integer(_value, field, maximum) do
    {:error,
     Error.not_sent(:validation, {:invalid_runtime_info_limit, field},
       details: %{maximum: maximum}
     )}
  end

  @spec invalid_options(term()) :: {:error, Error.t()}
  defp invalid_options(_opts),
    do: {:error, Error.not_sent(:validation, :invalid_runtime_info_options)}
end

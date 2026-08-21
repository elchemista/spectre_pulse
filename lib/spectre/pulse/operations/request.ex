defmodule Spectre.Pulse.Operations.Request do
  @moduledoc false

  alias Spectre.Pulse.Error

  @allowed_options [
    :connection,
    :include_terminal,
    :instance_registry,
    :kinds,
    :max_binary_bytes,
    :max_collection_entries,
    :max_depth
  ]
  @allowed_kinds [:work, :vigil]
  @default_max_collection_entries 128
  @hard_max_collection_entries 256
  @default_max_binary_bytes 16_384
  @hard_max_binary_bytes 65_536
  @default_max_depth 6
  @hard_max_depth 8

  @enforce_keys [
    :include_terminal,
    :instance_registry,
    :kinds,
    :max_binary_bytes,
    :max_collection_entries,
    :max_depth
  ]
  defstruct [
    :connection,
    :include_terminal,
    :instance_registry,
    :kinds,
    :max_binary_bytes,
    :max_collection_entries,
    :max_depth
  ]

  @type kind :: :work | :vigil
  @type t :: %__MODULE__{
          connection: term() | nil,
          include_terminal: boolean(),
          instance_registry: atom(),
          kinds: [kind()],
          max_binary_bytes: pos_integer(),
          max_collection_entries: pos_integer(),
          max_depth: pos_integer()
        }

  @doc false
  @spec new(term()) :: {:ok, t()} | {:error, Error.t()}
  def new(opts) when is_list(opts) do
    with :ok <- validate_keyword(opts),
         {:ok, kinds} <- kinds(Keyword.get(opts, :kinds)),
         {:ok, include_terminal} <- include_terminal(Keyword.get(opts, :include_terminal, false)),
         {:ok, registry} <- registry(Keyword.get(opts, :instance_registry)),
         {:ok, max_binary_bytes} <-
           bounded_positive_integer(
             Keyword.get(opts, :max_binary_bytes, @default_max_binary_bytes),
             :max_binary_bytes,
             @hard_max_binary_bytes
           ),
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
         include_terminal: include_terminal,
         instance_registry: registry,
         kinds: kinds,
         max_binary_bytes: max_binary_bytes,
         max_collection_entries: max_entries,
         max_depth: max_depth
       }}
    end
  end

  def new(opts), do: invalid_options(opts)

  @doc false
  @spec to_options(t()) :: keyword()
  def to_options(%__MODULE__{} = request) do
    [
      connection: request.connection,
      include_terminal: request.include_terminal,
      instance_registry: request.instance_registry,
      kinds: request.kinds,
      max_binary_bytes: request.max_binary_bytes,
      max_collection_entries: request.max_collection_entries,
      max_depth: request.max_depth
    ]
  end

  @spec validate_keyword(keyword()) :: :ok | {:error, Error.t()}
  defp validate_keyword(opts) do
    if Keyword.keyword?(opts) and Keyword.keys(opts) -- @allowed_options == [],
      do: :ok,
      else: invalid_options(opts)
  end

  @spec kinds(term()) :: {:ok, [kind()]} | {:error, Error.t()}
  defp kinds(nil), do: {:ok, @allowed_kinds}

  defp kinds(values) when is_list(values) and values != [] do
    normalized = Enum.map(values, &normalize_kind/1)

    if Enum.all?(normalized, &(&1 in @allowed_kinds)),
      do: {:ok, Enum.uniq(normalized)},
      else: {:error, Error.not_sent(:validation, :invalid_operations_kinds)}
  end

  defp kinds(_values),
    do: {:error, Error.not_sent(:validation, :invalid_operations_kinds)}

  @spec normalize_kind(term()) :: kind() | nil
  defp normalize_kind(kind) when kind in @allowed_kinds, do: kind
  defp normalize_kind("work"), do: :work
  defp normalize_kind("vigil"), do: :vigil
  defp normalize_kind(_kind), do: nil

  @spec include_terminal(term()) :: {:ok, boolean()} | {:error, Error.t()}
  defp include_terminal(value) when is_boolean(value), do: {:ok, value}

  defp include_terminal(_value),
    do: {:error, Error.not_sent(:validation, :invalid_operations_include_terminal)}

  @spec registry(term()) :: {:ok, atom()} | {:error, Error.t()}
  defp registry(nil), do: {:ok, Spectre.Instance.Registry}
  defp registry(value) when is_atom(value), do: {:ok, value}

  defp registry(_value),
    do: {:error, Error.not_sent(:validation, :invalid_operations_instance_registry)}

  @spec bounded_positive_integer(term(), atom(), pos_integer()) ::
          {:ok, pos_integer()} | {:error, Error.t()}
  defp bounded_positive_integer(value, _field, maximum)
       when is_integer(value) and value > 0 and value <= maximum,
       do: {:ok, value}

  defp bounded_positive_integer(_value, field, maximum) do
    {:error,
     Error.not_sent(:validation, {:invalid_operations_limit, field}, details: %{maximum: maximum})}
  end

  @spec invalid_options(term()) :: {:error, Error.t()}
  defp invalid_options(_opts),
    do: {:error, Error.not_sent(:validation, :invalid_operations_options)}
end

defmodule Spectre.Pulse.RuntimeInfo.Snapshot do
  @moduledoc false

  alias Spectre.Pulse.Error
  alias Spectre.Pulse.RuntimeInfo.Request

  @fields [
    :registered_name,
    :status,
    :initial_call,
    :current_function,
    :message_queue_len,
    :links,
    :monitors,
    :monitored_by,
    :trap_exit,
    :error_handler,
    :priority,
    :group_leader,
    :total_heap_size,
    :heap_size,
    :stack_size,
    :reductions,
    :garbage_collection,
    :suspending,
    :memory
  ]

  @enforce_keys [
    :process,
    :sampled_at_unix_ms,
    :truncated_fields,
    :max_collection_entries,
    :max_depth
  ]
  defstruct [
    :process,
    :sampled_at_unix_ms,
    :truncated_fields,
    :max_collection_entries,
    :max_depth
  ]

  @type t :: %__MODULE__{
          process: map(),
          sampled_at_unix_ms: non_neg_integer(),
          truncated_fields: [String.t()],
          max_collection_entries: pos_integer(),
          max_depth: pos_integer()
        }

  @doc false
  @spec default_fields() :: [atom()]
  def default_fields, do: @fields

  @doc false
  @spec normalize_fields(term()) :: {:ok, [atom()]} | {:error, Error.t()}
  def normalize_fields(nil), do: {:ok, @fields}

  def normalize_fields(fields) when is_list(fields) do
    normalized = Enum.map(fields, &normalize_field/1)

    cond do
      normalized == [] ->
        {:error, Error.not_sent(:validation, :runtime_info_fields_required)}

      Enum.any?(normalized, &is_nil/1) ->
        {:error, Error.not_sent(:authorization, :runtime_info_field_not_allowed)}

      true ->
        {:ok, Enum.uniq(normalized)}
    end
  end

  def normalize_fields(_fields),
    do: {:error, Error.not_sent(:validation, :invalid_runtime_info_fields)}

  @doc false
  @spec capture(term(), Request.t()) :: {:ok, t()} | {:error, Error.t()}
  def capture(pid, %Request{} = request) when is_pid(pid) do
    case Process.info(pid, request.fields) do
      nil ->
        {:error, Error.not_sent(:routing, :agent_instance_not_alive)}

      values ->
        {process, truncated_fields} = encode_fields(values, request)

        {:ok,
         %__MODULE__{
           process: process,
           sampled_at_unix_ms: System.system_time(:millisecond),
           truncated_fields: truncated_fields,
           max_collection_entries: request.max_collection_entries,
           max_depth: request.max_depth
         }}
    end
  rescue
    ArgumentError -> {:error, Error.not_sent(:validation, :invalid_runtime_process)}
  end

  def capture(_pid, %Request{}),
    do: {:error, Error.not_sent(:validation, :invalid_runtime_process)}

  @spec normalize_field(term()) :: atom() | nil
  defp normalize_field(field) when field in @fields, do: field

  defp normalize_field(field) when is_binary(field) do
    Enum.find(@fields, &(Atom.to_string(&1) == field))
  end

  defp normalize_field(_field), do: nil

  @spec encode_fields(keyword(), Request.t()) :: {map(), [String.t()]}
  defp encode_fields(values, request) do
    Enum.reduce(values, {%{}, []}, fn {field, value}, {encoded, truncated_fields} ->
      key = Atom.to_string(field)
      value = if field == :registered_name and value == [], do: nil, else: value
      {wire_value, truncated?} = encode_value(value, request, request.max_depth)
      truncated_fields = if truncated?, do: [key | truncated_fields], else: truncated_fields

      {Map.put(encoded, key, wire_value), truncated_fields}
    end)
    |> then(fn {encoded, truncated_fields} ->
      {encoded, Enum.reverse(truncated_fields)}
    end)
  end

  @spec encode_value(term(), Request.t(), non_neg_integer()) :: {term(), boolean()}
  defp encode_value(value, _request, _depth)
       when is_binary(value) or is_number(value) or is_boolean(value) or is_nil(value),
       do: {value, false}

  defp encode_value(value, _request, _depth) when is_atom(value),
    do: {Atom.to_string(value), false}

  defp encode_value(value, _request, _depth)
       when is_pid(value) or is_port(value) or is_reference(value),
       do: {inspect(value), false}

  defp encode_value(_value, _request, 0), do: {"[truncated:max_depth]", true}

  defp encode_value(value, request, depth) when is_tuple(value) do
    value
    |> Tuple.to_list()
    |> encode_list(request, depth)
  end

  defp encode_value(value, request, depth) when is_list(value),
    do: encode_list(value, request, depth)

  defp encode_value(value, request, depth) when is_map(value) do
    entries = Enum.take(value, request.max_collection_entries + 1)
    truncated_by_size? = length(entries) > request.max_collection_entries

    {encoded, truncated_nested?} =
      entries
      |> Enum.take(request.max_collection_entries)
      |> Enum.reduce({%{}, false}, fn {key, item}, {values, truncated?} ->
        {encoded_item, item_truncated?} = encode_value(item, request, depth - 1)
        {Map.put(values, wire_key(key), encoded_item), truncated? or item_truncated?}
      end)

    {encoded, truncated_by_size? or truncated_nested?}
  end

  defp encode_value(value, _request, _depth),
    do: {inspect(value, limit: 20, printable_limit: 2_000), false}

  @spec encode_list(list(), Request.t(), pos_integer()) :: {list(), boolean()}
  defp encode_list(values, request, depth) do
    entries = Enum.take(values, request.max_collection_entries + 1)
    truncated_by_size? = length(entries) > request.max_collection_entries

    {encoded, truncated_nested?} =
      entries
      |> Enum.take(request.max_collection_entries)
      |> Enum.map_reduce(false, fn item, truncated? ->
        {encoded_item, item_truncated?} = encode_value(item, request, depth - 1)
        {encoded_item, truncated? or item_truncated?}
      end)

    {encoded, truncated_by_size? or truncated_nested?}
  end

  @spec wire_key(term()) :: String.t()
  defp wire_key(key) when is_atom(key), do: Atom.to_string(key)
  defp wire_key(key) when is_binary(key), do: key
  defp wire_key(key), do: inspect(key, limit: 10, printable_limit: 200)
end

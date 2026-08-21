defmodule Spectre.Pulse.WireValue do
  @moduledoc false

  @type limits :: %{
          required(:max_collection_entries) => pos_integer(),
          required(:max_depth) => pos_integer(),
          required(:max_binary_bytes) => pos_integer()
        }

  @doc false
  @spec encode(term(), limits()) :: {term(), boolean()}
  def encode(value, limits), do: encode_value(value, limits, limits.max_depth)

  @spec encode_value(term(), limits(), non_neg_integer()) :: {term(), boolean()}
  defp encode_value(value, _limits, _depth)
       when is_number(value) or is_boolean(value) or is_nil(value),
       do: {value, false}

  defp encode_value(value, limits, _depth) when is_binary(value),
    do: encode_binary(value, limits.max_binary_bytes)

  defp encode_value(value, _limits, _depth) when is_atom(value),
    do: {Atom.to_string(value), false}

  defp encode_value(value, _limits, _depth)
       when is_pid(value) or is_port(value) or is_reference(value),
       do: {inspect(value), false}

  defp encode_value(_value, _limits, 0), do: {"[truncated:max_depth]", true}

  defp encode_value(value, limits, depth) when is_tuple(value) do
    value
    |> Tuple.to_list()
    |> encode_list(limits, depth)
  end

  defp encode_value(value, limits, depth) when is_list(value),
    do: encode_list(value, limits, depth)

  defp encode_value(value, limits, depth) when is_map(value) do
    entries = Enum.take(value, limits.max_collection_entries + 1)
    truncated_by_size? = length(entries) > limits.max_collection_entries

    {encoded, truncated_nested?} =
      entries
      |> Enum.take(limits.max_collection_entries)
      |> Enum.reduce({%{}, false}, fn {key, item}, {values, truncated?} ->
        {encoded_item, item_truncated?} = encode_value(item, limits, depth - 1)
        {encoded_key, key_truncated?} = wire_key(key, limits)

        {Map.put(values, encoded_key, encoded_item),
         truncated? or key_truncated? or item_truncated?}
      end)

    {encoded, truncated_by_size? or truncated_nested?}
  end

  defp encode_value(value, _limits, _depth),
    do: {inspect(value, limit: 20, printable_limit: 2_000), false}

  @spec encode_list(list(), limits(), pos_integer()) :: {list(), boolean()}
  defp encode_list(values, limits, depth) do
    entries = Enum.take(values, limits.max_collection_entries + 1)
    truncated_by_size? = length(entries) > limits.max_collection_entries

    {encoded, truncated_nested?} =
      entries
      |> Enum.take(limits.max_collection_entries)
      |> Enum.map_reduce(false, fn item, truncated? ->
        {encoded_item, item_truncated?} = encode_value(item, limits, depth - 1)
        {encoded_item, truncated? or item_truncated?}
      end)

    {encoded, truncated_by_size? or truncated_nested?}
  end

  @spec encode_binary(binary(), pos_integer()) :: {String.t(), boolean()}
  defp encode_binary(value, maximum_bytes) do
    cond do
      not String.valid?(value) ->
        {inspect(value, limit: 20, printable_limit: maximum_bytes), true}

      byte_size(value) <= maximum_bytes ->
        {value, false}

      true ->
        prefix = value |> binary_part(0, maximum_bytes) |> valid_utf8_prefix()
        {prefix, true}
    end
  end

  @spec valid_utf8_prefix(binary()) :: String.t()
  defp valid_utf8_prefix(value) do
    if String.valid?(value),
      do: value,
      else: value |> binary_part(0, byte_size(value) - 1) |> valid_utf8_prefix()
  end

  @spec wire_key(term(), limits()) :: {String.t(), boolean()}
  defp wire_key(key, _limits) when is_atom(key), do: {Atom.to_string(key), false}
  defp wire_key(key, limits) when is_binary(key), do: encode_binary(key, limits.max_binary_bytes)

  defp wire_key(key, _limits),
    do: {inspect(key, limit: 10, printable_limit: 200), false}
end

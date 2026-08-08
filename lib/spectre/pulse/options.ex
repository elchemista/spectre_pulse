defmodule Spectre.Pulse.Options do
  @moduledoc false

  @spec keyword(term()) :: {:ok, keyword()} | {:error, term()}
  def keyword(options) when is_list(options) do
    if Keyword.keyword?(options),
      do: {:ok, options},
      else: {:error, {:invalid_options, options}}
  end

  def keyword(options), do: {:error, {:invalid_options, options}}

  @spec positive_integer(keyword(), atom(), pos_integer()) ::
          {:ok, pos_integer()} | {:error, term()}
  def positive_integer(options, key, default) do
    case Keyword.get(options, key, default) do
      value when is_integer(value) and value > 0 -> {:ok, value}
      value -> {:error, {:invalid_limit, key, value}}
    end
  end

  @spec non_negative_integer(keyword(), atom(), non_neg_integer()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def non_negative_integer(options, key, default) do
    case Keyword.get(options, key, default) do
      value when is_integer(value) and value >= 0 -> {:ok, value}
      value -> {:error, {:invalid_option, key, value}}
    end
  end
end

defmodule Spectre.Pulse.Reachability do
  @moduledoc """
  A temporary observation about technical reachability.

  Reachability is not availability, authorization, or acceptance of a request.
  """

  alias Spectre.Pulse.Options

  @enforce_keys [:status, :level, :observed_at]
  defstruct [:status, :level, :via, :observed_at, :valid_for_ms, :reason, metadata: %{}]

  @type status :: :reachable | :unreachable | :unknown
  @type level :: :route_known | :pulse_endpoint

  @type t :: %__MODULE__{
          status: status(),
          level: level(),
          via: atom() | String.t() | nil,
          observed_at: DateTime.t(),
          valid_for_ms: non_neg_integer(),
          reason: term(),
          metadata: map()
        }

  @doc "Builds a reachability observation."
  @spec new(status(), keyword()) :: t()
  def new(status, opts \\ [])

  def new(status, opts) when status in [:reachable, :unreachable, :unknown] do
    with {:ok, opts} <- Options.keyword(opts),
         {:ok, valid_for_ms} <- Options.non_negative_integer(opts, :valid_for_ms, 0) do
      observation = %__MODULE__{
        status: status,
        level: Keyword.get(opts, :level, :route_known),
        via: Keyword.get(opts, :via),
        observed_at: Keyword.get_lazy(opts, :observed_at, &DateTime.utc_now/0),
        valid_for_ms: valid_for_ms,
        reason: Keyword.get(opts, :reason),
        metadata: Keyword.get(opts, :metadata, %{})
      }

      validate!(observation)
    else
      {:error, reason} -> raise ArgumentError, inspect(reason)
    end
  end

  def new(status, _opts),
    do: raise(ArgumentError, "invalid reachability status: #{inspect(status)}")

  @doc "Returns an `:unknown` observation."
  @spec unknown(term(), keyword()) :: t()
  def unknown(reason \\ nil, opts \\ []) do
    case Options.keyword(opts) do
      {:ok, opts} -> new(:unknown, Keyword.put(opts, :reason, reason))
      {:error, invalid} -> raise ArgumentError, inspect(invalid)
    end
  end

  @doc "Returns whether the observation's validity window has elapsed."
  @spec expired?(t(), DateTime.t()) :: boolean()
  def expired?(observation, now \\ DateTime.utc_now())

  def expired?(%__MODULE__{valid_for_ms: 0}, _now), do: true

  def expired?(%__MODULE__{} = observation, now) do
    DateTime.diff(now, observation.observed_at, :millisecond) >= observation.valid_for_ms
  end

  @spec validate!(t()) :: t()
  defp validate!(observation) do
    cond do
      observation.level not in [:route_known, :pulse_endpoint] ->
        raise ArgumentError, "invalid reachability level: #{inspect(observation.level)}"

      not is_struct(observation.observed_at, DateTime) ->
        raise ArgumentError, "invalid reachability timestamp: #{inspect(observation.observed_at)}"

      not is_map(observation.metadata) ->
        raise ArgumentError, "invalid reachability metadata: #{inspect(observation.metadata)}"

      true ->
        observation
    end
  end
end

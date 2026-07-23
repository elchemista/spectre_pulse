defmodule Spectre.Pulse.Reachability do
  @moduledoc """
  A temporary observation about technical reachability.

  Reachability is not availability, authorization, or acceptance of a request.
  """

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
  def new(status, opts \\ []) when status in [:reachable, :unreachable, :unknown] do
    %__MODULE__{
      status: status,
      level: Keyword.get(opts, :level, :route_known),
      via: Keyword.get(opts, :via),
      observed_at: Keyword.get(opts, :observed_at, DateTime.utc_now()),
      valid_for_ms: Keyword.get(opts, :valid_for_ms, 0),
      reason: Keyword.get(opts, :reason),
      metadata: Keyword.get(opts, :metadata, %{})
    }
  end

  @doc "Returns an `:unknown` observation."
  @spec unknown(term(), keyword()) :: t()
  def unknown(reason \\ nil, opts \\ []),
    do: new(:unknown, Keyword.put(opts, :reason, reason))

  @doc "Returns whether the observation's validity window has elapsed."
  @spec expired?(t(), DateTime.t()) :: boolean()
  def expired?(observation, now \\ DateTime.utc_now())

  def expired?(%__MODULE__{valid_for_ms: 0}, _now), do: true

  def expired?(%__MODULE__{} = observation, now) do
    DateTime.diff(now, observation.observed_at, :millisecond) >= observation.valid_for_ms
  end
end

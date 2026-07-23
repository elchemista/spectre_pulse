defmodule Spectre.Pulse.Error do
  @moduledoc """
  Typed Pulse failure.

  `outcome` is intentionally separate from `kind`. A network may try another
  route only after `:not_sent`; `:outcome_unknown` means that a duplicate is
  possible and the agent or host must decide what to do.
  """

  defexception [
    :kind,
    :reason,
    :outcome,
    :message_id,
    :route_id,
    details: %{},
    cause: nil
  ]

  @type outcome :: :not_sent | :outcome_unknown

  @type t :: %__MODULE__{
          kind:
            :validation
            | :codec
            | :authentication
            | :authorization
            | :routing
            | :transport
            | :inbound
            | atom(),
          reason: term(),
          outcome: outcome(),
          message_id: String.t() | nil,
          route_id: term(),
          details: map(),
          cause: term()
        }

  @doc false
  @spec message(t()) :: String.t()
  @impl Exception
  def message(%__MODULE__{} = error) do
    "Pulse #{error.kind || :error}: #{inspect(error.reason)} (#{error.outcome || :not_sent})"
  end

  @doc "Builds an error known not to have delivered the envelope."
  @spec not_sent(atom(), term(), keyword()) :: t()
  def not_sent(kind, reason, opts \\ []) do
    build(kind, reason, :not_sent, opts)
  end

  @doc "Builds an error for which delivery may already have happened."
  @spec outcome_unknown(atom(), term(), keyword()) :: t()
  def outcome_unknown(kind, reason, opts \\ []) do
    build(kind, reason, :outcome_unknown, opts)
  end

  @doc "Normalizes arbitrary adapter errors without losing an existing outcome."
  @spec normalize(term(), outcome(), keyword()) :: t()
  def normalize(error, default_outcome \\ :outcome_unknown, opts \\ [])

  def normalize(%__MODULE__{} = error, _default_outcome, _opts), do: error

  def normalize(reason, default_outcome, opts)
      when default_outcome in [:not_sent, :outcome_unknown] do
    build(Keyword.get(opts, :kind, :transport), reason, default_outcome, opts)
  end

  @doc "Returns true only when another route is technically safe to try."
  @spec not_sent?(term()) :: boolean()
  def not_sent?(%__MODULE__{outcome: :not_sent}), do: true
  def not_sent?(_error), do: false

  @doc "Returns true when delivery may have occurred."
  @spec outcome_unknown?(term()) :: boolean()
  def outcome_unknown?(%__MODULE__{outcome: :outcome_unknown}), do: true
  def outcome_unknown?(_error), do: false

  @spec build(atom(), term(), outcome(), keyword()) :: t()
  defp build(kind, reason, outcome, opts) do
    %__MODULE__{
      kind: kind,
      reason: reason,
      outcome: outcome,
      message_id: Keyword.get(opts, :message_id),
      route_id: Keyword.get(opts, :route_id),
      details: Keyword.get(opts, :details, %{}),
      cause: Keyword.get(opts, :cause)
    }
  end
end

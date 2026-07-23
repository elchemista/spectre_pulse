defmodule Spectre.Pulse.Expectation do
  @moduledoc """
  A pure agent-owned reminder about an expected correlated message.

  Expectations start no process and schedule no timer. Reducers return new
  values for the application to persist in `Spectre.State`.
  """

  alias Spectre.Pulse.Envelope

  @enforce_keys [:message_id, :contact, :waiting_for, :status, :opened_at]
  defstruct [
    :message_id,
    :contact,
    :waiting_for,
    :status,
    :opened_at,
    :due_at,
    :resolved_by,
    metadata: %{}
  ]

  @type waiting_for :: :reply | {:type, String.t()} | String.t()
  @type status :: :open | :resolved | :cancelled | :expired

  @type t :: %__MODULE__{
          message_id: String.t(),
          contact: term(),
          waiting_for: waiting_for(),
          status: status(),
          opened_at: DateTime.t(),
          due_at: DateTime.t() | nil,
          resolved_by: String.t() | nil,
          metadata: map()
        }

  @doc "Opens an expectation for an outbound message."
  @spec new(String.t(), term(), waiting_for(), keyword()) :: t()
  def new(message_id, contact, waiting_for \\ :reply, opts \\ []) do
    %__MODULE__{
      message_id: message_id,
      contact: contact,
      waiting_for: normalize_waiting_for(waiting_for),
      status: :open,
      opened_at: Keyword.get(opts, :opened_at, DateTime.utc_now()),
      due_at: Keyword.get(opts, :due_at),
      metadata: Keyword.get(opts, :metadata, %{})
    }
  end

  @doc "Returns whether an incoming envelope satisfies this expectation."
  @spec matches?(t(), Envelope.t()) :: boolean()
  def matches?(%__MODULE__{status: :open} = expectation, %Envelope{} = envelope) do
    envelope.relates_to == expectation.message_id and type_matches?(expectation, envelope)
  end

  def matches?(%__MODULE__{}, %Envelope{}), do: false

  @doc "Marks an expectation resolved by a matching envelope."
  @spec resolve(t(), Envelope.t()) :: {:ok, t()} | {:error, term()}
  def resolve(%__MODULE__{} = expectation, %Envelope{} = envelope) do
    if matches?(expectation, envelope) do
      {:ok, %{expectation | status: :resolved, resolved_by: envelope.id}}
    else
      {:error, :expectation_not_satisfied}
    end
  end

  @doc "Cancels an open expectation without side effects."
  @spec cancel(t()) :: t()
  def cancel(%__MODULE__{status: :open} = expectation),
    do: %{expectation | status: :cancelled}

  def cancel(%__MODULE__{} = expectation), do: expectation

  @doc "Expires an open expectation whose due time elapsed."
  @spec expire(t(), DateTime.t()) :: t()
  def expire(%__MODULE__{status: :open, due_at: %DateTime{} = due_at} = expectation, now)
      when is_struct(now, DateTime) do
    if DateTime.compare(now, due_at) in [:eq, :gt],
      do: %{expectation | status: :expired},
      else: expectation
  end

  def expire(%__MODULE__{} = expectation, _now), do: expectation

  @spec normalize_waiting_for(waiting_for()) :: :reply | {:type, String.t()}
  defp normalize_waiting_for({:type, type}), do: {:type, type}
  defp normalize_waiting_for(type) when is_binary(type), do: {:type, type}
  defp normalize_waiting_for(:reply), do: :reply

  @spec type_matches?(t(), Envelope.t()) :: boolean()
  defp type_matches?(%__MODULE__{waiting_for: :reply}, _envelope), do: true

  defp type_matches?(%__MODULE__{waiting_for: {:type, type}}, envelope),
    do: envelope.payload.type == type
end

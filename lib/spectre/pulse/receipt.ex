defmodule Spectre.Pulse.Receipt do
  @moduledoc """
  Technical acknowledgement that a binding accepted an envelope.

  A receipt never means that the receiving agent accepted or completed the
  semantic request.
  """

  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Validator

  @enforce_keys [:message_id, :status, :accepted_at]
  defstruct [:message_id, :status, :via, :route_id, :accepted_at, metadata: %{}]

  @type t :: %__MODULE__{
          message_id: String.t(),
          status: :accepted,
          via: atom() | String.t() | nil,
          route_id: term(),
          accepted_at: DateTime.t(),
          metadata: map()
        }

  @doc "Creates an accepted technical receipt."
  @spec accepted(String.t(), keyword()) :: t()
  def accepted(message_id, opts \\ []) do
    %__MODULE__{
      message_id: message_id,
      status: :accepted,
      via: Keyword.get(opts, :via),
      route_id: Keyword.get(opts, :route_id),
      accepted_at: Keyword.get(opts, :accepted_at, DateTime.utc_now()),
      metadata: Keyword.get(opts, :metadata, %{})
    }
  end

  @doc "Restores and validates a receipt from a map."
  @spec new(t() | map()) :: {:ok, t()} | {:error, Error.t()}
  def new(%__MODULE__{} = receipt), do: validate(receipt)

  def new(map) when is_map(map) do
    receipt = %__MODULE__{
      message_id: attr(map, :message_id),
      status: normalize_status(attr(map, :status)),
      via: attr(map, :via),
      route_id: attr(map, :route_id),
      accepted_at: normalize_datetime(attr(map, :accepted_at)),
      metadata: attr(map, :metadata, %{})
    }

    validate(receipt)
  end

  def new(value),
    do: {:error, Error.not_sent(:validation, {:invalid_receipt, value})}

  @doc "Returns a JSON-safe wire projection."
  @spec to_wire(t()) :: map()
  def to_wire(%__MODULE__{} = receipt) do
    %{
      "message_id" => receipt.message_id,
      "status" => "accepted",
      "via" => normalize_wire_value(receipt.via),
      "route_id" => normalize_wire_value(receipt.route_id),
      "accepted_at" => DateTime.to_iso8601(receipt.accepted_at),
      "metadata" => receipt.metadata
    }
  end

  @spec validate(t()) :: {:ok, t()} | {:error, Error.t()}
  defp validate(%__MODULE__{} = receipt) do
    cond do
      not Validator.valid_id?(receipt.message_id) ->
        {:error, Error.not_sent(:validation, {:invalid_receipt_message_id, receipt.message_id})}

      receipt.status != :accepted ->
        {:error, Error.not_sent(:validation, {:invalid_receipt_status, receipt.status})}

      not match?(%DateTime{}, receipt.accepted_at) ->
        {:error, Error.not_sent(:validation, {:invalid_receipt_time, receipt.accepted_at})}

      not is_map(receipt.metadata) ->
        {:error, Error.not_sent(:validation, {:invalid_receipt_metadata, receipt.metadata})}

      true ->
        {:ok, receipt}
    end
  end

  @spec normalize_status(term()) :: term()
  defp normalize_status(:accepted), do: :accepted
  defp normalize_status("accepted"), do: :accepted
  defp normalize_status(value), do: value

  @spec normalize_datetime(term()) :: term()
  defp normalize_datetime(%DateTime{} = value), do: value

  defp normalize_datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> datetime
      _ -> value
    end
  end

  defp normalize_datetime(value), do: value

  @spec normalize_wire_value(term()) :: term()
  defp normalize_wire_value(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_wire_value(value), do: value

  @spec attr(map(), atom(), term()) :: term()
  defp attr(map, key, default \\ nil),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key), default))
end

defmodule Spectre.Pulse.Approval do
  @moduledoc """
  External human approvals over ordinary Pulse Envelope v1.

  Install this as Managed GAM's `approvals.transport`. The existing authenticated
  inbound bridge dispatches these types to Managed GAM before conversational
  routing. A transport receipt never acknowledges rendering or consent.

  The approver's chat consumer calls `Spectre.Server.approval_presented/2` after
  rendering the request, then `Spectre.Server.approval_decide/3` for a human
  confirm/reject. Model text and known peer identity cannot sign a decision.
  """
  @behaviour Spectre.Server.GAM.Managed.Approval
  alias Spectre.Pulse.{Envelope, Error}

  @types ~w(approval.request approval.presented approval.decision approval.cancelled)
  @headers ~w(type id from to relates_to)

  @doc "The application protocol's payload types; Envelope stays at version 1."
  @spec types() :: [String.t()]
  def types, do: @types

  @impl true
  def send_message(message, opts) do
    with {:ok, envelope} <-
           Envelope.new(
             id: message["id"],
             from: message["from"],
             to: message["to"],
             act: if(message["type"] == "approval.request", do: :request, else: :inform),
             relates_to: message["relates_to"],
             payload: %{type: message["type"], data: Map.drop(message, @headers)}
           ) do
      Spectre.Pulse.deliver(envelope, opts)
    end
  end

  @doc false
  @spec validate_ingress(Envelope.t(), boolean()) :: :ok | {:error, Error.t()}
  def validate_ingress(%{payload: %{type: type}} = envelope, authenticated?)
      when type in @types do
    expected_act = if type == "approval.request", do: :request, else: :inform
    data = envelope.payload.data

    if authenticated? and envelope.act == expected_act and valid_data?(data) and
         valid_relation?(type, envelope.relates_to) do
      :ok
    else
      {:error,
       Error.not_sent(:authentication, :invalid_approval_envelope, message_id: envelope.id)}
    end
  end

  def validate_ingress(_envelope, _authenticated?), do: :ok

  defp valid_data?(data) when is_map(data) and not is_struct(data),
    do: Enum.all?(Map.keys(data), &(is_binary(&1) and &1 not in @headers))

  defp valid_data?(_data), do: false

  defp valid_relation?("approval.request", relation), do: is_nil(relation)
  defp valid_relation?(_type, relation), do: is_binary(relation)

  @doc false
  @spec accept(GenServer.server(), Envelope.t()) :: {:ok, term()} | {:error, term()}
  def accept(server, envelope) do
    message =
      Map.merge(envelope.payload.data, %{
        "type" => envelope.payload.type,
        "id" => envelope.id,
        "from" => envelope.from,
        "to" => envelope.to
      })

    message =
      if envelope.relates_to,
        do: Map.put(message, "relates_to", envelope.relates_to),
        else: message

    case Spectre.Server.receive_approval(server, message) do
      :ok ->
        {:ok, {:approval_accepted, envelope.id}}

      {:ok, _interaction} ->
        {:ok, {:approval_accepted, envelope.id}}

      {:error, reason} ->
        {:error, Error.not_sent(:authorization, reason, message_id: envelope.id)}
    end
  catch
    :exit, _ ->
      {:error,
       Error.outcome_unknown(:inbound, :approval_server_unavailable, message_id: envelope.id)}
  end
end

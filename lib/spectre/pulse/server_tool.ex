defmodule Spectre.Pulse.ServerTool do
  @moduledoc """
  AgentServer tool adapter for protocol discovery and governed agent messages.
  Sending is exposed only through an explicit host operation binding.
  """
  alias Spectre.Pulse.{Config, Envelope, Error, Receipt}

  def tools(config) do
    operations =
      Map.new(
        [:protocol, :contacts],
        &{&1, %{path: :cognitive, retry: :safe, max_bytes: 1_048_576}}
      )

    operations =
      case Keyword.get(Map.get(config, :options, []), :bind, [])[:send] do
        nil ->
          operations

        operation ->
          Map.put(operations, :send, %{
            path: :governed,
            operation: operation,
            max_bytes: 1_048_576
          })
      end

    %{pulse: %{contract: 1, adapter: __MODULE__, default: nil, operations: operations}}
  end

  def prepare(operation, [], _trusted) when operation in [:protocol, :contacts], do: {:ok, %{}}

  def prepare(:send, [attrs], trusted) when is_map(attrs) do
    with true <- Map.keys(attrs) -- [:to, :act, :payload, :relates_to, :metadata] == [],
         {:ok, config} <- Config.fetch(trusted.agent),
         {:ok, envelope} <-
           Envelope.new(
             Map.merge(attrs, %{from: config.identity, id: "00000000-0000-7000-8000-000000000001"})
           ) do
      {:ok, envelope |> Envelope.to_wire() |> Map.delete("id")}
    else
      false ->
        {:error, :unsupported_pulse_fields}

      {:error, %Error{} = error} ->
        {:error, %{kind: error.kind, outcome: error.outcome, message: Exception.message(error)}}
    end
  end

  def prepare(_, _, _), do: {:error, :invalid_pulse_tool_request}

  def invoke(:protocol, _args, _context), do: {:ok, Spectre.Pulse.protocol()}

  def invoke(:contacts, _args, context) do
    {:ok,
     Enum.map(Spectre.Pulse.contacts(context.service_context.agent), fn contact ->
       %{
         key: contact.key,
         identity: contact.identity,
         display_name: contact.display_name,
         capabilities: contact.capabilities
       }
     end)}
  end

  def project(_operation, {:ok, value}), do: {:ok, {:ok, value}}

  def project(_operation, {:error, %Error{} = error}),
    do:
      {:ok,
       {:error, %{kind: error.kind, outcome: error.outcome, message: Exception.message(error)}}}

  def project(_operation, {:error, reason}), do: {:ok, {:error, reason}}
  def project(_operation, _result), do: {:error, :invalid_pulse_tool_result}

  @doc "Host executor primitive; call only after GAM admission with a stable command id."
  def execute(attrs, context) do
    with id when is_binary(id) <- Map.get(context, :command_id),
         {:ok, config} <- Config.fetch(context.agent),
         {:ok, envelope} <-
           Envelope.new(Map.merge(attrs, %{"id" => id, "from" => config.identity})),
         {:ok, %Receipt{} = receipt} <-
           Spectre.Pulse.deliver(envelope, network: config.network, directory: config.directory) do
      {:ok,
       %{
         message_id: receipt.message_id,
         status: receipt.status,
         via: receipt.via,
         route_id: receipt.route_id,
         accepted_at: DateTime.to_iso8601(receipt.accepted_at)
       }}
    else
      nil ->
        {:error, :command_id_required}

      {:error, %Error{} = error} ->
        {:error, %{kind: error.kind, outcome: error.outcome, message: Exception.message(error)}}

      {:error, _} = error ->
        error
    end
  end
end

defmodule Spectre.Pulse.ServerTool do
  @moduledoc """
  AgentServer tool adapter for protocol discovery and governed agent messages.
  Sending is exposed only through an explicit host operation binding.
  """
  alias Spectre.GAM.Canonical.Value
  alias Spectre.Pulse.{Config, Envelope, Error, Receipt}

  @behaviour Spectre.Server.GAM.EffectAdapter

  @doc "Declares the execution adapter for explicitly bound sending."
  @spec effects(map()) :: map()
  def effects(config) do
    Map.new(bindings!(config), fn {:send, operation} ->
      {operation,
       %{
         contract: 1,
         adapter: __MODULE__,
         operation: :send,
         side_effect: :non_idempotent,
         material: :arguments,
         partition: [],
         schema_hash: nil,
         config: %{}
       }}
    end)
  end

  @doc "Executes an admitted message without retry, preserving transport uncertainty."
  @spec execute_effect(:send, map(), map()) ::
          {:ok, term()} | {:error, :definitive_no_effect | :ambiguous, term()}
  def execute_effect(:send, args, context) do
    case execute(args, context) do
      {:ok, receipt} -> {:ok, receipt}
      {:error, %{outcome: :not_sent} = reason} -> {:error, :definitive_no_effect, reason}
      {:error, reason} -> {:error, :ambiguous, reason}
    end
  end

  def execute_effect(_, _, _), do: {:error, :definitive_no_effect, :invalid_pulse_effect}

  def tools(config) do
    bindings = bindings!(config)

    operations =
      Map.new(
        [:protocol, :contacts],
        &{&1, %{path: :cognitive, retry: :safe, max_bytes: 1_048_576}}
      )

    operations =
      case Keyword.get(bindings, :send) do
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
        {:error, portable_error(error)}
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

  def project(_operation, {:error, reason}), do: {:ok, {:error, portable_error(reason)}}
  def project(_operation, _result), do: {:error, :invalid_pulse_tool_result}

  @doc "Host executor primitive; call only after GAM admission with a stable command id."
  def execute(attrs, context) do
    with {:ok, envelope} <- admitted_envelope(attrs, context),
         {:ok, config} <- Config.fetch(context.agent),
         {:ok, %Receipt{} = receipt} <-
           Spectre.Pulse.deliver(envelope, network: config.network, directory: config.directory) do
      project_receipt(receipt)
    else
      {:error, reason} -> {:error, portable_error(reason)}
      other -> {:error, portable_error(other)}
    end
  rescue
    _ -> {:error, portable_error(:pulse_delivery_exception)}
  catch
    _, _ -> {:error, portable_error(:pulse_delivery_failure)}
  end

  defp admitted_envelope(attrs, %{command_id: id} = context) when is_binary(id) do
    with {:ok, config} <- Config.fetch(context.agent) do
      attrs
      |> Map.drop([:id, :from, "id", "from"])
      |> Map.merge(%{"id" => id, "from" => config.identity})
      |> Envelope.new()
    end
  end

  defp admitted_envelope(_attrs, _context),
    do: {:error, Error.not_sent(:validation, :command_id_required)}

  defp project_receipt(receipt) do
    result = %{
      message_id: receipt.message_id,
      status: receipt.status,
      via: receipt.via,
      route_id: receipt.route_id,
      accepted_at: DateTime.to_iso8601(receipt.accepted_at)
    }

    case Value.validate(result, max_bytes: 4096) do
      :ok -> {:ok, result}
      {:error, _} -> {:error, portable_error(:invalid_pulse_receipt)}
    end
  end

  defp portable_error(%Error{} = error) do
    %{
      kind: if(is_atom(error.kind), do: error.kind, else: :transport),
      outcome: if(error.outcome == :not_sent, do: :not_sent, else: :outcome_unknown),
      message: bounded_reason(error.reason)
    }
  end

  defp portable_error(reason),
    do: %{kind: :transport, outcome: :outcome_unknown, message: bounded_reason(reason)}

  defp bounded_reason(reason) do
    case Value.validate(reason, max_bytes: 2048) do
      :ok -> inspect(reason, limit: 20, printable_limit: 256)
      {:error, _} -> "pulse error details unavailable"
    end
  end

  defp bindings!(config) do
    bindings = Keyword.get(Map.get(config, :options, []), :bind, [])

    unless Keyword.keyword?(bindings) and
             length(Keyword.keys(bindings)) == length(Enum.uniq(Keyword.keys(bindings))) and
             Enum.all?(bindings, fn {key, operation} ->
               key == :send and is_atom(operation) and operation not in [nil, true, false]
             end) do
      raise ArgumentError, "pulse bind: expects unique send: host_operation entries"
    end

    bindings
  end
end

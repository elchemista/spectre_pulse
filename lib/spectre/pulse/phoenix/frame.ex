defmodule Spectre.Pulse.Phoenix.Frame do
  @moduledoc false

  alias Spectre.Pulse.AgentDescriptor
  alias Spectre.Pulse.Connection
  alias Spectre.Pulse.ConnectionRegistry
  alias Spectre.Pulse.ConnectionSpec
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Receipt

  @monitor_enable "agent.runtime.monitor.enable"
  @monitor_disable "agent.runtime.monitor.disable"
  @operations_monitor_enable "agent.operations.monitor.enable"
  @operations_monitor_disable "agent.operations.monitor.disable"
  @monitor_commands [
    @monitor_enable,
    @monitor_disable,
    @operations_monitor_enable,
    @operations_monitor_disable
  ]

  @doc false
  @spec manifest(Connection.t()) :: binary()
  def manifest(connection) do
    encode(%{
      "pulse" => "connection",
      "version" => 1,
      "type" => "manifest",
      "connection" => Connection.to_public_map(connection),
      "spec" => public_spec(connection),
      "agents" => exposed_agents(connection)
    })
  end

  @doc false
  @spec receipt(Receipt.t()) :: binary()
  def receipt(receipt) do
    encode(%{
      "pulse" => "connection",
      "version" => 1,
      "type" => "receipt",
      "receipt" => Receipt.to_wire(receipt)
    })
  end

  @doc false
  @spec error(Error.t() | term()) :: binary()
  def error(%Error{} = error) do
    encode(%{
      "pulse" => "connection",
      "version" => 1,
      "type" => "error",
      "error" => %{
        "kind" => public_atom(error.kind, "request"),
        "outcome" => public_atom(error.outcome, "not_sent"),
        "code" => reason_code(error.reason),
        "message_id" => error.message_id
      }
    })
  end

  def error(reason), do: error(Error.not_sent(:validation, reason))

  @doc false
  @spec command(binary()) ::
          :not_control
          | {:ok,
             {:monitor_enable, map()}
             | {:monitor_disable, term()}
             | {:operations_monitor_enable, map()}
             | {:operations_monitor_disable, term()}}
          | {:error, Error.t()}
  def command(frame) when is_binary(frame) do
    case Jason.decode(frame) do
      {:ok, %{"pulse" => "connection", "type" => type} = value}
      when type in @monitor_commands ->
        monitoring_command(type, value)

      _not_control ->
        :not_control
    end
  end

  @doc false
  @spec monitoring(map()) :: binary()
  def monitoring(event) when is_map(event) do
    event
    |> Map.put("pulse", "connection")
    |> Map.put("version", 1)
    |> encode()
  end

  @doc false
  @spec metadata(term()) :: map()
  def metadata(opts) when is_list(opts) and opts != [] do
    if Keyword.keyword?(opts), do: opcode_metadata(opts), else: %{}
  end

  def metadata(_opts), do: %{}

  @spec monitoring_command(String.t(), map()) ::
          {:ok,
           {:monitor_enable, map()}
           | {:monitor_disable, term()}
           | {:operations_monitor_enable, map()}
           | {:operations_monitor_disable, term()}}
          | {:error, Error.t()}
  defp monitoring_command(@monitor_enable, %{"version" => 1} = value),
    do: {:ok, {:monitor_enable, value}}

  defp monitoring_command(@monitor_disable, %{"version" => 1} = value),
    do: {:ok, {:monitor_disable, Map.get(value, "subscription_id")}}

  defp monitoring_command(@operations_monitor_enable, %{"version" => 1} = value),
    do: {:ok, {:operations_monitor_enable, value}}

  defp monitoring_command(@operations_monitor_disable, %{"version" => 1} = value),
    do: {:ok, {:operations_monitor_disable, Map.get(value, "subscription_id")}}

  defp monitoring_command(_type, _value),
    do: {:error, Error.not_sent(:validation, :unsupported_connection_protocol_version)}

  @spec opcode_metadata(keyword()) :: map()
  defp opcode_metadata(opts) do
    case Keyword.get(opts, :opcode) do
      opcode when opcode in [:text, :binary] -> %{"opcode" => Atom.to_string(opcode)}
      _other -> %{}
    end
  end

  @spec public_spec(Connection.t()) :: map()
  defp public_spec(connection) do
    case ConnectionRegistry.fetch_spec(connection.spec_id) do
      {:ok, spec} -> ConnectionSpec.to_public_map(spec)
      _unavailable -> %{id: connection.spec_id, transport: connection.transport}
    end
  end

  @spec exposed_agents(Connection.t()) :: [map()]
  defp exposed_agents(connection) do
    case ConnectionRegistry.exposed_agents(connection.spec_id) do
      {:ok, descriptors} -> Enum.map(descriptors, &AgentDescriptor.to_wire/1)
      _unavailable -> []
    end
  end

  @spec encode(map()) :: binary()
  defp encode(value) do
    case Jason.encode(value) do
      {:ok, encoded} -> encoded
      {:error, _reason} -> fallback_encoding_error()
    end
  end

  @spec fallback_encoding_error() :: binary()
  defp fallback_encoding_error do
    Jason.encode!(%{
      "pulse" => "connection",
      "version" => 1,
      "type" => "error",
      "error" => %{
        "kind" => "codec",
        "outcome" => "not_sent",
        "code" => "response_encoding_failed",
        "message_id" => nil
      }
    })
  end

  @spec reason_code(term()) :: String.t()
  defp reason_code(reason) when is_atom(reason), do: Atom.to_string(reason)

  defp reason_code(reason) when is_tuple(reason) and tuple_size(reason) > 0 do
    reason
    |> elem(0)
    |> public_atom("request_failed")
  end

  defp reason_code(_reason), do: "request_failed"

  @spec public_atom(term(), String.t()) :: String.t()
  defp public_atom(value, _fallback) when is_atom(value), do: Atom.to_string(value)
  defp public_atom(_value, fallback), do: fallback
end

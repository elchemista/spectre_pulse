defmodule Spectre.Pulse.Phoenix.Socket do
  @moduledoc false

  alias Spectre.Pulse.AgentDescriptor
  alias Spectre.Pulse.Connection
  alias Spectre.Pulse.ConnectionRegistry
  alias Spectre.Pulse.ConnectionSpec
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Handshake
  alias Spectre.Pulse.Local
  alias Spectre.Pulse.Receipt
  alias Spectre.Pulse.Transports.WebSocket

  @enforce_keys [:connection, :options]
  defstruct [:connection, :options]

  @type t :: %__MODULE__{
          connection: Connection.t(),
          options: keyword()
        }

  @doc false
  @spec connect(map(), keyword()) :: {:ok, {Handshake.t(), keyword()}} | {:error, term()}
  def connect(transport_info, opts) when is_map(transport_info) and is_list(opts) do
    with true <- Keyword.keyword?(opts),
         {:ok, connection_spec} <- Keyword.fetch(opts, :connection),
         {:ok, ticket} <-
           Handshake.prepare(connection_spec, transport_info,
             context: handshake_context(transport_info),
             direction: :inbound,
             metadata: Keyword.get(opts, :metadata, %{})
           ) do
      {:ok, {ticket, opts}}
    else
      false -> {:error, :invalid_pulse_phoenix_options}
      :error -> {:error, :pulse_connection_spec_required}
      {:error, %Error{} = error} -> {:error, error}
    end
  end

  def connect(_transport_info, _opts), do: {:error, :invalid_pulse_phoenix_connect}

  @doc false
  @spec init({Handshake.t(), keyword()}) :: {:ok, t()} | {:stop, term()}
  def init({ticket, opts}) when is_list(opts) do
    case Handshake.open(ticket, self()) do
      {:ok, connection} ->
        send(self(), {:spectre_pulse_manifest, connection.id})
        {:ok, %__MODULE__{connection: connection, options: opts}}

      {:error, %Error{} = error} ->
        {:stop, error}
    end
  end

  def init(state), do: {:stop, {:invalid_pulse_phoenix_state, state}}

  @doc false
  @spec handle_in({term(), keyword()}, t()) ::
          {:reply, :ok | :error, {:text, binary()}, t()} | {:stop, term(), t()}
  def handle_in({frame, frame_opts}, %__MODULE__{} = state) when is_binary(frame) do
    inbound_opts =
      state.options
      |> Keyword.get(:inbound, [])
      |> Keyword.put(:target_resolver, recipient_resolver(state.connection))

    context = %{
      authenticated_identity: state.connection.principal.identity,
      binding: :websocket,
      peer: state.connection.peer_id,
      verified: state.connection.verified,
      metadata: %{connection_id: state.connection.id, frame: frame_metadata(frame_opts)}
    }

    case WebSocket.handle_frame(frame, context, inbound_opts) do
      {:ok, result} ->
        {:reply, :ok, {:text, encode_receipt(result.receipt)}, touch(state)}

      {:error, %Error{} = error} ->
        {:reply, :error, {:text, encode_error(error)}, touch(state)}
    end
  end

  def handle_in({_frame, _frame_opts}, %__MODULE__{} = state),
    do: {:reply, :error, {:text, encode_error(:binary_frame_expected)}, state}

  def handle_in(frame, state), do: {:stop, {:invalid_pulse_phoenix_frame, frame}, state}

  @doc false
  @spec handle_info(term(), t()) :: {:ok, t()} | {:push, {:text, binary()}, t()}
  def handle_info({:spectre_pulse_manifest, connection_id}, %__MODULE__{} = state)
      when connection_id == state.connection.id do
    {:push, {:text, encode_manifest(state.connection)}, state}
  end

  def handle_info({:spectre_pulse_frame, frame}, %__MODULE__{} = state) when is_binary(frame),
    do: {:push, {:text, frame}, touch(state)}

  def handle_info(_message, %__MODULE__{} = state), do: {:ok, state}

  @doc false
  @spec handle_control({term(), keyword()}, t()) ::
          {:ok, t()} | {:reply, :ok, {:pong, term()}, t()}
  def handle_control({payload, opts}, %__MODULE__{} = state) do
    if Keyword.get(opts, :opcode) == :ping,
      do: {:reply, :ok, {:pong, payload}, touch(state)},
      else: {:ok, touch(state)}
  end

  @doc false
  @spec terminate(term(), t() | term()) :: :ok
  def terminate(_reason, %__MODULE__{} = state),
    do: ConnectionRegistry.close(state.connection.id)

  def terminate(_reason, _state), do: :ok

  @spec handshake_context(map()) :: map()
  defp handshake_context(transport_info) do
    %{
      binding: :websocket,
      endpoint: Map.get(transport_info, :endpoint),
      transport: Map.get(transport_info, :transport),
      params: Map.get(transport_info, :params, %{}),
      connect_info: Map.get(transport_info, :connect_info, %{})
    }
  end

  @spec recipient_resolver(Connection.t()) :: (String.t(), term() -> term())
  defp recipient_resolver(connection) do
    fn address, context ->
      if address in connection.local_agent_addresses,
        do: Local.resolve_target(address, context),
        else: {:error, {:connection_recipient_not_exposed, address}}
    end
  end

  @spec encode_manifest(Connection.t()) :: binary()
  defp encode_manifest(connection) do
    spec =
      case ConnectionRegistry.fetch_spec(connection.spec_id) do
        {:ok, spec} -> ConnectionSpec.to_public_map(spec)
        :error -> %{id: connection.spec_id, transport: connection.transport}
      end

    agents =
      case ConnectionRegistry.exposed_agents(connection.spec_id) do
        {:ok, descriptors} -> Enum.map(descriptors, &AgentDescriptor.to_wire/1)
        :error -> []
      end

    encode(%{
      "pulse" => "connection",
      "version" => 1,
      "type" => "manifest",
      "connection" => Connection.to_public_map(connection),
      "spec" => spec,
      "agents" => agents
    })
  end

  @spec encode_receipt(Receipt.t()) :: binary()
  defp encode_receipt(receipt) do
    encode(%{
      "pulse" => "connection",
      "version" => 1,
      "type" => "receipt",
      "receipt" => Receipt.to_wire(receipt)
    })
  end

  @spec encode_error(Error.t() | term()) :: binary()
  defp encode_error(%Error{} = error) do
    encode(%{
      "pulse" => "connection",
      "version" => 1,
      "type" => "error",
      "error" => %{
        "kind" => Atom.to_string(error.kind),
        "outcome" => Atom.to_string(error.outcome),
        "reason" => inspect(error.reason),
        "message_id" => error.message_id
      }
    })
  end

  defp encode_error(reason), do: encode_error(Error.not_sent(:validation, reason))

  @spec encode(map()) :: binary()
  defp encode(value) do
    case Jason.encode(value) do
      {:ok, encoded} ->
        encoded

      {:error, reason} ->
        Jason.encode!(%{"pulse" => "connection", "type" => "error", "reason" => inspect(reason)})
    end
  end

  @spec frame_metadata(term()) :: map()
  defp frame_metadata(opts) when is_list(opts), do: Map.new(opts)
  defp frame_metadata(_opts), do: %{}

  @spec touch(t()) :: t()
  defp touch(state) do
    case ConnectionRegistry.touch(state.connection.id) do
      {:ok, connection} -> %{state | connection: connection}
      _other -> state
    end
  end
end

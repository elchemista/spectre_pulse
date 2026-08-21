defmodule Spectre.Pulse.Phoenix.Socket do
  @moduledoc false

  alias Spectre.Pulse.Connection
  alias Spectre.Pulse.ConnectionRegistry
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Handshake
  alias Spectre.Pulse.Local
  alias Spectre.Pulse.Monitoring
  alias Spectre.Pulse.Phoenix.Frame
  alias Spectre.Pulse.Transports.WebSocket

  @enforce_keys [:connection, :options, :monitoring]
  defstruct [:connection, :options, :monitoring]

  @type t :: %__MODULE__{
          connection: Connection.t(),
          options: keyword(),
          monitoring: pid()
        }

  @doc false
  @spec connect(map(), keyword()) :: {:ok, {Handshake.t(), keyword()}} | {:error, term()}
  def connect(transport_info, opts) when is_map(transport_info) and is_list(opts) do
    with :ok <- validate_options(opts),
         {:ok, connection_spec} <- fetch_connection_option(opts),
         {:ok, ticket} <- prepare_handshake(connection_spec, transport_info, opts) do
      {:ok, {ticket, opts}}
    end
  end

  def connect(_transport_info, _opts), do: {:error, :invalid_pulse_phoenix_connect}

  @doc false
  @spec init({Handshake.t(), keyword()}) :: {:ok, t()} | {:stop, term()}
  def init({ticket, opts}) when is_list(opts) do
    case Handshake.open(ticket, self()) do
      {:ok, connection} ->
        case Monitoring.start_link(connection: connection.id, sink: self()) do
          {:ok, monitoring} ->
            send(self(), {:spectre_pulse_manifest, connection.id})
            {:ok, %__MODULE__{connection: connection, options: opts, monitoring: monitoring}}

          {:error, reason} ->
            ConnectionRegistry.close(connection.id)
            {:stop, reason}
        end

      {:error, %Error{} = error} ->
        {:stop, error}
    end
  end

  def init(state), do: {:stop, {:invalid_pulse_phoenix_state, state}}

  @doc false
  @spec handle_in({term(), keyword()}, t()) ::
          {:reply, :ok | :error, {:text, binary()}, t()} | {:stop, term(), t()}
  def handle_in({frame, frame_opts}, %__MODULE__{} = state) when is_binary(frame) do
    case Frame.command(frame) do
      {:ok, command} -> handle_command(command, state)
      {:error, %Error{} = error} -> error_reply(error, state)
      :not_control -> handle_envelope(frame, frame_opts, state)
    end
  end

  def handle_in({_frame, _frame_opts}, %__MODULE__{} = state),
    do: {:reply, :error, {:text, Frame.error(:binary_frame_expected)}, state}

  def handle_in(frame, state), do: {:stop, {:invalid_pulse_phoenix_frame, frame}, state}

  @doc false
  @spec handle_info(term(), t()) :: {:ok, t()} | {:push, {:text, binary()}, t()}
  def handle_info({:spectre_pulse_manifest, connection_id}, %__MODULE__{} = state)
      when connection_id == state.connection.id do
    {:push, {:text, Frame.manifest(state.connection)}, state}
  end

  def handle_info({:spectre_pulse_frame, frame}, %__MODULE__{} = state) when is_binary(frame),
    do: {:push, {:text, frame}, touch(state)}

  def handle_info({:spectre_pulse_monitoring, event}, %__MODULE__{} = state)
      when is_map(event),
      do: {:push, {:text, Frame.monitoring(event)}, touch(state)}

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
  def terminate(_reason, %__MODULE__{} = state) do
    if Process.alive?(state.monitoring), do: GenServer.stop(state.monitoring, :normal)
    ConnectionRegistry.close(state.connection.id)
  end

  def terminate(_reason, _state), do: :ok

  @spec validate_options(term()) :: :ok | {:error, atom()}
  defp validate_options(opts) do
    if Keyword.keyword?(opts),
      do: validate_option_values(opts),
      else: {:error, :invalid_pulse_phoenix_options}
  end

  @spec validate_option_values(keyword()) :: :ok | {:error, atom()}
  defp validate_option_values(opts) do
    allowed = [:connection, :inbound, :metadata]
    inbound = Keyword.get(opts, :inbound, [])
    metadata = Keyword.get(opts, :metadata, %{})

    if Keyword.keys(opts) -- allowed == [] and is_list(inbound) and Keyword.keyword?(inbound) and
         is_map(metadata),
       do: :ok,
       else: {:error, :invalid_pulse_phoenix_options}
  end

  @spec fetch_connection_option(keyword()) :: {:ok, term()} | {:error, atom()}
  defp fetch_connection_option(opts) do
    case Keyword.fetch(opts, :connection) do
      {:ok, connection} -> {:ok, connection}
      :error -> {:error, :pulse_connection_spec_required}
    end
  end

  @spec prepare_handshake(term(), map(), keyword()) ::
          {:ok, Handshake.t()} | {:error, Error.t()}
  defp prepare_handshake(connection_spec, transport_info, opts) do
    Handshake.prepare(connection_spec, transport_info,
      context: handshake_context(transport_info),
      direction: :inbound,
      metadata: Keyword.get(opts, :metadata, %{})
    )
  end

  @spec handshake_context(map()) :: map()
  defp handshake_context(transport_info) do
    connect_info = Map.get(transport_info, :connect_info, %{})

    %{
      binding: :websocket,
      endpoint: Map.get(transport_info, :endpoint),
      transport: Map.get(transport_info, :transport),
      peer_data: peer_data(connect_info)
    }
  end

  @spec peer_data(term()) :: term() | nil
  defp peer_data(connect_info) when is_map(connect_info), do: Map.get(connect_info, :peer_data)
  defp peer_data(_connect_info), do: nil

  @spec inbound_context(t(), keyword()) :: map()
  defp inbound_context(state, frame_opts) do
    %{
      authenticated_identity: state.connection.principal.identity,
      binding: :websocket,
      peer: state.connection.peer_id,
      verified: state.connection.verified,
      metadata: %{connection_id: state.connection.id, frame: Frame.metadata(frame_opts)}
    }
  end

  @spec inbound_opts(t()) :: keyword()
  defp inbound_opts(state) do
    state.options
    |> Keyword.get(:inbound, [])
    |> Keyword.put(:target_resolver, recipient_resolver(state.connection))
  end

  @spec recipient_resolver(Connection.t()) :: (String.t(), term() -> term())
  defp recipient_resolver(connection) do
    fn address, context ->
      if address in connection.local_agent_addresses,
        do: Local.resolve_target(address, context),
        else: {:error, {:connection_recipient_not_exposed, address}}
    end
  end

  @spec handle_envelope(binary(), keyword(), t()) ::
          {:reply, :ok | :error, {:text, binary()}, t()}
  defp handle_envelope(frame, frame_opts, state) do
    case WebSocket.handle_frame(frame, inbound_context(state, frame_opts), inbound_opts(state)) do
      {:ok, result} ->
        {:reply, :ok, {:text, Frame.receipt(result.receipt)}, touch(state)}

      {:error, %Error{} = error} ->
        error_reply(error, state)
    end
  end

  @spec handle_command(
          {:monitor_enable, map()}
          | {:monitor_disable, term()}
          | {:operations_monitor_enable, map()}
          | {:operations_monitor_disable, term()},
          t()
        ) ::
          {:reply, :ok | :error, {:text, binary()}, t()}
  defp handle_command({:monitor_enable, attrs}, state) do
    monitoring_reply(Monitoring.enable(state.monitoring, attrs), state)
  end

  defp handle_command({:monitor_disable, subscription_id}, state) do
    monitoring_reply(Monitoring.disable(state.monitoring, subscription_id), state)
  end

  defp handle_command({:operations_monitor_enable, attrs}, state) do
    monitoring_reply(Monitoring.enable_operations(state.monitoring, attrs), state)
  end

  defp handle_command({:operations_monitor_disable, subscription_id}, state) do
    monitoring_reply(Monitoring.disable_operations(state.monitoring, subscription_id), state)
  end

  @spec monitoring_reply({:ok, map()} | {:error, Error.t()}, t()) ::
          {:reply, :ok | :error, {:text, binary()}, t()}
  defp monitoring_reply({:ok, event}, state),
    do: {:reply, :ok, {:text, Frame.monitoring(event)}, touch(state)}

  defp monitoring_reply({:error, %Error{} = error}, state), do: error_reply(error, state)

  @spec error_reply(Error.t(), t()) :: {:reply, :error, {:text, binary()}, t()}
  defp error_reply(error, state),
    do: {:reply, :error, {:text, Frame.error(error)}, touch(state)}

  @spec touch(t()) :: t()
  defp touch(state) do
    case ConnectionRegistry.touch(state.connection.id) do
      {:ok, connection} -> %{state | connection: connection}
      _unavailable -> state
    end
  end
end

defmodule Spectre.Pulse.Phoenix do
  @moduledoc """
  Generates a Phoenix socket backed by Pulse without adding Phoenix as a
  dependency of `spectre_pulse`.

  Define a small host module:

      defmodule MyAppWeb.PulseSocket do
        use Spectre.Pulse.Phoenix, connection: :studio
      end

  Then mount it on the existing Phoenix endpoint:

      socket "/pulse", MyAppWeb.PulseSocket,
        websocket: [connect_info: [:peer_data, :uri]],
        longpoll: false

  Phoenix exposes the WebSocket at `/pulse/websocket`. Authentication and
  authorization callbacks come from the selected Pulse `ConnectionSpec`.
  """

  @doc "Generates the `Phoenix.Socket.Transport` callbacks in the host application."
  defmacro __using__(opts) do
    opts = Macro.expand(opts, __CALLER__)

    unless is_list(opts) and Keyword.keyword?(opts) and Keyword.has_key?(opts, :connection) do
      raise ArgumentError,
            "use Spectre.Pulse.Phoenix expects a keyword list containing :connection"
    end

    quote bind_quoted: [opts: opts] do
      alias Phoenix.Socket.Transport, as: PhoenixTransport
      alias Spectre.Pulse.Phoenix.Socket, as: PulseSocket

      @behaviour PhoenixTransport
      @spectre_pulse_phoenix_options opts

      @impl PhoenixTransport
      def child_spec(_opts), do: :ignore

      @impl PhoenixTransport
      def connect(transport_info),
        do: PulseSocket.connect(transport_info, @spectre_pulse_phoenix_options)

      @impl PhoenixTransport
      def init(state), do: PulseSocket.init(state)

      @impl PhoenixTransport
      def handle_in(frame, state), do: PulseSocket.handle_in(frame, state)

      @impl PhoenixTransport
      def handle_info(message, state),
        do: PulseSocket.handle_info(message, state)

      @impl PhoenixTransport
      def handle_control(frame, state),
        do: PulseSocket.handle_control(frame, state)

      @impl PhoenixTransport
      def terminate(reason, state), do: PulseSocket.terminate(reason, state)
    end
  end
end

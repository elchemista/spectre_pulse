defmodule Spectre.Pulse.Inbound.Result do
  @moduledoc """
  The local result of accepting an inbound Pulse envelope.

  AgentServer acceptance sets `interaction` and leaves `turn` nil; its eventual
  responses arrive through the Server consumer. Legacy delivery sets `turn`.
  """

  defstruct [
    :envelope,
    :context,
    :canonical_sender,
    :target,
    :input,
    :turn,
    :interaction,
    :receipt
  ]

  @type t :: %__MODULE__{
          envelope: Spectre.Pulse.Envelope.t(),
          context: Spectre.Pulse.InboundContext.t(),
          canonical_sender: String.t(),
          target: module() | GenServer.server() | {:agent_server, GenServer.server()},
          input: Spectre.Input.t(),
          turn: Spectre.Turn.t() | nil,
          interaction: String.t() | nil,
          receipt: Spectre.Pulse.Receipt.t()
        }
end

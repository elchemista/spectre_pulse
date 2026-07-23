defmodule Spectre.Pulse.Inbound.Result do
  @moduledoc """
  The local result of accepting an inbound Pulse envelope.
  """

  defstruct [
    :envelope,
    :context,
    :canonical_sender,
    :target,
    :input,
    :turn,
    :receipt
  ]

  @type t :: %__MODULE__{
          envelope: Spectre.Pulse.Envelope.t(),
          context: Spectre.Pulse.InboundContext.t(),
          canonical_sender: String.t(),
          target: module() | GenServer.server(),
          input: Spectre.Input.t(),
          turn: Spectre.Turn.t(),
          receipt: Spectre.Pulse.Receipt.t()
        }
end

defmodule Spectre.Pulse.Transports.REST.Response do
  @moduledoc """
  Framework-neutral response returned by the REST inbound adapter.

  Plug, Phoenix, Bandit, or another host maps this value onto its own response
  type at the application boundary.
  """

  defstruct [:status, headers: [{"content-type", "application/json"}], body: ""]

  @type t :: %__MODULE__{
          status: pos_integer(),
          headers: [{String.t(), String.t()}],
          body: binary()
        }
end

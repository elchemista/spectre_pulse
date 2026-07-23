defmodule Spectre.Pulse.Directory.Resolution do
  @moduledoc """
  Normalized result of resolving one Agent-local reference.

  A resolution keeps logical identity and discovered infrastructure separate:
  `address` is canonical, while `routes` are ephemeral technical paths.
  """

  alias Spectre.Pulse.Contact
  alias Spectre.Pulse.Route

  defstruct [:reference, :address, :contact, routes: [], source: nil]

  @type t :: %__MODULE__{
          reference: term(),
          address: String.t(),
          contact: Contact.t() | nil,
          routes: [Route.t()],
          source: term()
        }
end

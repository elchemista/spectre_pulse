defmodule Spectre.Pulse.Monitoring.Entry do
  @moduledoc false

  alias Spectre.Pulse.Monitoring.Subscription

  @enforce_keys [:subscription, :sample_timer]
  defstruct [:subscription, :sample_timer, :expiry_timer, sequence: 0, dropped_updates: 0]

  @type t :: %__MODULE__{
          subscription: Subscription.t(),
          sample_timer: reference(),
          expiry_timer: reference() | nil,
          sequence: non_neg_integer(),
          dropped_updates: non_neg_integer()
        }
end

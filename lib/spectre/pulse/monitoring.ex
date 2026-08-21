defmodule Spectre.Pulse.Monitoring do
  @moduledoc """
  Runtime-controlled near-realtime monitoring of Agent Instances.

  A transport adapter starts one session for an authenticated Pulse
  connection. Studio may then enable and disable individual subscriptions at
  runtime. Subscriptions are removed automatically when the adapter process
  disconnects or when their requested duration expires.
  """

  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Monitoring.Registry
  alias Spectre.Pulse.Monitoring.Session

  @scope "agent.runtime.stream"

  @doc "Returns the additional scope required to stream runtime snapshots."
  @spec scope() :: String.t()
  def scope, do: @scope

  @doc "Starts a transport-neutral monitoring session for one live connection."
  @spec start_link(keyword()) :: GenServer.on_start()
  defdelegate start_link(opts), to: Session

  @doc "Enables one temporary runtime subscription in an active session."
  @spec enable(pid(), map()) :: {:ok, map()} | {:error, Error.t()}
  defdelegate enable(session, attrs), to: Session

  @doc "Disables one runtime subscription owned by the active session."
  @spec disable(pid(), String.t()) :: {:ok, map()} | {:error, Error.t()}
  defdelegate disable(session, subscription_id), to: Session

  @doc "Returns active runtime subscriptions, optionally for one connection."
  @spec subscriptions(term() | :all) :: [map()]
  def subscriptions(connection_id \\ :all), do: Registry.list(connection_id)
end

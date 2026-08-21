defmodule Spectre.Pulse.Monitoring do
  @moduledoc """
  Runtime-controlled near-realtime monitoring of Agent Instances.

  A transport adapter starts one session for an authenticated Pulse
  connection. Studio may then enable and disable individual subscriptions at
  runtime. Runtime-process and Work/Vigil subscriptions have independent
  scopes and wire capabilities. Subscriptions are removed automatically when
  the adapter process disconnects or when their requested duration expires.
  """

  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Monitoring.Registry
  alias Spectre.Pulse.Monitoring.Session

  @runtime_scope "agent.runtime.stream"
  @operations_scope "agent.operations.stream"

  @doc "Returns the additional scope required to stream runtime snapshots."
  @spec scope() :: String.t()
  def scope, do: @runtime_scope

  @doc "Returns the additional scope required to stream Work and Vigil views."
  @spec operations_scope() :: String.t()
  def operations_scope, do: @operations_scope

  @doc "Starts a transport-neutral monitoring session for one live connection."
  @spec start_link(keyword()) :: GenServer.on_start()
  defdelegate start_link(opts), to: Session

  @doc "Enables one temporary runtime subscription in an active session."
  @spec enable(pid(), map()) :: {:ok, map()} | {:error, Error.t()}
  defdelegate enable(session, attrs), to: Session

  @doc "Enables one temporary Work and Vigil subscription in an active session."
  @spec enable_operations(pid(), map()) :: {:ok, map()} | {:error, Error.t()}
  defdelegate enable_operations(session, attrs), to: Session

  @doc "Disables one runtime subscription owned by the active session."
  @spec disable(pid(), String.t()) :: {:ok, map()} | {:error, Error.t()}
  defdelegate disable(session, subscription_id), to: Session

  @doc "Disables one Work and Vigil subscription owned by the active session."
  @spec disable_operations(pid(), String.t()) :: {:ok, map()} | {:error, Error.t()}
  defdelegate disable_operations(session, subscription_id), to: Session

  @doc "Returns active runtime subscriptions, optionally for one connection."
  @spec subscriptions(term() | :all) :: [map()]
  def subscriptions(connection_id \\ :all), do: Registry.list(connection_id, :runtime)

  @doc "Returns active Work and Vigil subscriptions, optionally for one connection."
  @spec operation_subscriptions(term() | :all) :: [map()]
  def operation_subscriptions(connection_id \\ :all),
    do: Registry.list(connection_id, :operations)
end

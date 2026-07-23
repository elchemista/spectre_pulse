defmodule Spectre.Pulse.Application do
  @moduledoc false

  use Application

  @doc false
  @spec start(Application.start_type(), term()) ::
          {:ok, pid()} | {:ok, pid(), term()} | {:error, term()}
  @impl Application
  def start(_type, _args) do
    children = [
      {Registry, keys: :unique, name: Spectre.Pulse.Local.Registry},
      {DynamicSupervisor, strategy: :one_for_one, name: Spectre.Pulse.Local.Supervisor},
      Spectre.Pulse.Fabric
    ]

    Supervisor.start_link(children,
      strategy: :one_for_one,
      name: Spectre.Pulse.Supervisor
    )
  end
end

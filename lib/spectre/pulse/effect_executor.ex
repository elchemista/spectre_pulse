defmodule Spectre.Pulse.EffectExecutor do
  @moduledoc false

  @behaviour Spectre.Effect.Executor

  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Executor
  alias Spectre.Pulse.Fabric

  @impl true
  def execute(%Spectre.Effect{kind: :pulse} = effect, context, opts) do
    with :ok <- register_transports(Keyword.get(opts, :transports, [])) do
      Executor.deliver(
        context.agent,
        effect,
        context.state,
        Keyword.delete(opts, :transports)
      )
    end
  end

  @spec register_transports(term()) :: :ok | {:error, Error.t() | term()}
  defp register_transports(transports) when is_list(transports) do
    Enum.reduce_while(transports, :ok, fn
      %{id: id, module: module}, :ok ->
        case Fabric.register_transport(id, module) do
          :ok -> {:cont, :ok}
          {:error, reason} -> {:halt, {:error, reason}}
        end

      invalid, :ok ->
        {:halt, {:error, {:invalid_pulse_transport, invalid}}}
    end)
  end

  defp register_transports(invalid),
    do: {:error, {:invalid_pulse_transports, invalid}}
end

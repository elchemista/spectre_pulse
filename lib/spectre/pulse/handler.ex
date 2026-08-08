defmodule Spectre.Pulse.Handler do
  @moduledoc false

  alias Spectre.Pulse.EffectBuilder

  @doc false
  @spec stage(Spectre.Input.t(), Spectre.Context.t()) ::
          {:ok, Spectre.Result.t()} | {:error, term()}
  def stage(%Spectre.Input{} = input, %Spectre.Context{} = context),
    do: EffectBuilder.stage_from_context(context.agent, input, context)

  def stage(input, context), do: {:error, {:invalid_pulse_handler_context, input, context}}
end

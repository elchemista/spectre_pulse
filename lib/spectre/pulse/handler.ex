defmodule Spectre.Pulse.Handler do
  @moduledoc false

  alias Spectre.Pulse.EffectBuilder

  @doc false
  @spec stage(Spectre.Input.t(), Spectre.Context.t()) ::
          {:ok, Spectre.Result.t()} | {:error, term()}
  def stage(input, context) do
    EffectBuilder.stage(
      context.agent,
      input,
      context,
      Keyword.fetch!(context.opts, :spectre_pulse)
    )
  end
end

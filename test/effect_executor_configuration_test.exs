defmodule Spectre.Pulse.EffectExecutorConfigurationTest do
  use ExUnit.Case, async: true

  alias Spectre.Pulse.{EffectExecutor, Error, Fabric}

  defmodule Transport do
    def deliver(_route, _envelope, _opts), do: raise("invalid configuration reached transport")
  end

  defmodule RestoredExtension do
    def effect_executors(:raise), do: raise("invalid extension configuration")
    def effect_executors(:exit), do: exit(:invalid_extension_configuration)
    def effect_executors(:missing), do: []

    def effect_executors(opts),
      do: [%Spectre.Effect.Executor.Mount{kind: :pulse, module: EffectExecutor, opts: opts}]
  end

  defmodule RestoredAgent do
    def __spectre_definition__ do
      %Spectre.Definition{
        owner: __MODULE__,
        extensions: [
          %Spectre.Extension.Mount{
            module: RestoredExtension,
            api_version: 1,
            compiled: Process.get({__MODULE__, :executor_options})
          }
        ]
      }
    end
  end

  test "restored executor configuration is rejected atomically before registering transports" do
    effect = %Spectre.Effect{id: Spectre.Identity.uuid7(), kind: :pulse}
    context = %Spectre.Context{agent: RestoredAgent, state: %Spectre.State{}}
    transport = %{id: :restored_validation_transport, module: Transport}

    for {options, expected_reason} <- [
          {:invalid, :invalid_options},
          {:missing, :unsupported_effect_kind},
          {[transports: :invalid], :invalid_pulse_transports},
          {[transports: [:invalid]], :invalid_pulse_transport},
          {[transports: [%{transport | id: nil}]], :invalid_pulse_transport_id},
          {[transports: [%{transport | module: "invalid"}]], :invalid_transport},
          {[transports: [%{transport | module: UnknownRestoredTransport}]], :invalid_transport},
          {[transports: [transport, transport]], :duplicate_pulse_transport},
          {:raise, :pulse_executor_config_exception},
          {:exit, :pulse_executor_config_exit}
        ] do
      Process.put({RestoredAgent, :executor_options}, options)

      assert {:error, %Error{kind: :validation, outcome: :not_sent, reason: reason}} =
               EffectExecutor.execute(effect, context, [])

      assert elem(reason, 0) == expected_reason
      refute Map.has_key?(Fabric.transports(), transport.id)
    end
  end
end

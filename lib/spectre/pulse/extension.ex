defmodule Spectre.Pulse.Extension do
  @moduledoc false

  @behaviour Spectre.Extension

  alias Spectre.Pulse.Address
  alias Spectre.Pulse.Config
  alias Spectre.Pulse.DSL

  @impl true
  def id, do: :pulse

  @impl true
  def api_version, do: 1

  @impl true
  def setup(owner, opts) do
    DSL.install!(owner, direct_options(opts))
  end

  @impl true
  def compile(owner, opts) do
    stack_config = Keyword.get(opts, :stack_config, %{})

    with :ok <- validate_stack_config(stack_config),
         {:ok, config} <- compile_config(owner, stack_config) do
      {:ok,
       %{
         config: config,
         transports: Map.get(stack_config, :transports, [])
       }}
    end
  end

  @impl true
  def agent_config(%{config: %Config{} = config}), do: [pulse: config]

  @impl true
  def effect_executors(%{transports: transports}) do
    [{:pulse, Spectre.Pulse.EffectExecutor, transports: transports}]
  end

  @impl true
  def expand_handler({:pulse, meta, [to]}, _caller, _opts) do
    expand_pulse(meta, to, [])
  end

  def expand_handler({:pulse, meta, [to, opts]}, _caller, _extension_opts)
      when is_list(opts) do
    expand_pulse(meta, to, opts)
  end

  def expand_handler(_handler, _caller, _opts), do: :ignore

  @spec compile_config(module(), map()) :: {:ok, Config.t()} | {:error, term()}
  defp compile_config(owner, stack_config) do
    contacts =
      owner
      |> Module.get_attribute(:spectre_pulse_contacts)
      |> List.wrap()
      |> Enum.reverse()

    Config.new(
      identity:
        Module.get_attribute(owner, :spectre_pulse_identity) ||
          Address.for_agent(owner),
      directory:
        Module.get_attribute(owner, :spectre_pulse_directory) ||
          Map.get(stack_config, :directory),
      network: Module.get_attribute(owner, :spectre_pulse_network),
      state_scope: Module.get_attribute(owner, :spectre_pulse_state_scope) || :agent,
      contacts: contacts,
      advertise: Module.get_attribute(owner, :spectre_pulse_advertise) || %{},
      inbound: Module.get_attribute(owner, :spectre_pulse_inbound) || []
    )
  end

  @spec expand_pulse(keyword(), Macro.t(), keyword()) :: {:ok, Macro.t()}
  defp expand_pulse(meta, to, opts) do
    pulse_opts = Keyword.put(opts, :to, to)

    {:ok,
     {:run, meta,
      [
        :stage,
        [
          handler_owner: Spectre.Pulse.Handler,
          spectre_pulse: pulse_opts
        ]
      ]}}
  end

  @spec validate_stack_config(term()) :: :ok | {:error, term()}
  defp validate_stack_config(config) when is_map(config), do: :ok
  defp validate_stack_config(config), do: {:error, {:invalid_pulse_stack_config, config}}

  @spec direct_options(keyword()) :: keyword()
  defp direct_options(opts) do
    Keyword.drop(opts, [
      :stack,
      :stack_ref,
      :stack_config,
      :stack_installation,
      :stack_package,
      :stack_package_version
    ])
  end
end

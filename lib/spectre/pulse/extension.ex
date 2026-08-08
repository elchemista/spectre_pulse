defmodule Spectre.Pulse.Extension do
  @moduledoc false

  @behaviour Spectre.Extension

  alias Spectre.Pulse.Address
  alias Spectre.Pulse.Config
  alias Spectre.Pulse.DSL
  alias Spectre.Pulse.Options

  @impl true
  def id, do: :pulse

  @impl true
  def api_version, do: 1

  @impl true
  def setup(owner, opts) do
    case Options.keyword(opts) do
      {:ok, opts} -> DSL.install!(owner, direct_options(opts))
      {:error, reason} -> raise ArgumentError, inspect(reason)
    end
  end

  @impl true
  def compile(owner, opts) do
    with {:ok, opts} <- Options.keyword(opts),
         stack_config <- Keyword.get(opts, :stack_config, %{}),
         :ok <- validate_stack_config(stack_config),
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
    if Keyword.keyword?(opts),
      do: expand_pulse(meta, to, opts),
      else: {:error, {:invalid_pulse_handler_options, opts}}
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
  defp validate_stack_config(config) when is_map(config) do
    unknown = Map.keys(config) -- [:directory, :transports]

    with [] <- unknown,
         :ok <- validate_stack_directory(Map.get(config, :directory)),
         :ok <- validate_stack_transports(Map.get(config, :transports, [])) do
      :ok
    else
      [_ | _] = keys -> {:error, {:unknown_pulse_stack_config, keys}}
      {:error, _reason} = error -> error
    end
  end

  defp validate_stack_config(config), do: {:error, {:invalid_pulse_stack_config, config}}

  @spec validate_stack_directory(term()) :: :ok | {:error, term()}
  defp validate_stack_directory(nil), do: :ok

  defp validate_stack_directory(directory)
       when is_atom(directory) and not is_nil(directory),
       do: :ok

  defp validate_stack_directory(directory),
    do: {:error, {:invalid_pulse_directory, directory}}

  @spec validate_stack_transports(term()) :: :ok | {:error, term()}
  defp validate_stack_transports(transports) when is_list(transports) do
    Enum.reduce_while(transports, {:ok, MapSet.new()}, fn
      %{id: id, module: module}, {:ok, ids}
      when is_atom(id) and not is_nil(id) and is_atom(module) and not is_nil(module) ->
        if MapSet.member?(ids, id),
          do: {:halt, {:error, {:duplicate_pulse_transport, id}}},
          else: {:cont, {:ok, MapSet.put(ids, id)}}

      transport, _acc ->
        {:halt, {:error, {:invalid_pulse_transport, transport}}}
    end)
    |> case do
      {:ok, _ids} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp validate_stack_transports(transports),
    do: {:error, {:invalid_pulse_transports, transports}}

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

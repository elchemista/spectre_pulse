defmodule Spectre.Pulse.Runtime do
  @moduledoc """
  Supervised host configuration for Pulse.

  A host application can add `{Spectre.Pulse, opts}` to its supervision tree.
  This runtime registers application transport drivers and automatically
  subscribes modules which use `Spectre.Pulse` against the shared Pulse
  Fabric. One Runtime owns the automatic subscriptions on a BEAM node.
  """

  use GenServer

  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Fabric
  alias Spectre.Pulse.Local

  @type transport_config ::
          {atom(), module()}
          | {atom(), module(), keyword()}

  @type option :: {:transports, [transport_config()]}
  @typep normalized_transport :: {atom(), module(), keyword()}
  @typep subscription :: {module(), String.t()}
  @typep state :: %{
           agents: [module()],
           transports: [normalized_transport()],
           owned_subscriptions: [String.t()]
         }

  @doc false
  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(opts) when is_list(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc false
  @spec init([option()]) :: {:ok, state()} | {:stop, Error.t()}
  @impl GenServer
  def init(opts) do
    Process.flag(:trap_exit, true)

    with :ok <- validate_options(opts),
         {:ok, transports} <- normalize_transports(Keyword.get(opts, :transports, [])),
         :ok <- register_transports(transports),
         agents <- discover_agents(),
         {:ok, owned_subscriptions} <- subscribe_agents(agents) do
      {:ok,
       %{
         agents: agents,
         transports: transports,
         owned_subscriptions: owned_subscriptions
       }}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @doc false
  @spec terminate(term(), state()) :: :ok
  @impl GenServer
  def terminate(_reason, state) do
    Enum.each(state.owned_subscriptions, &stop_owned_subscription/1)
    :ok
  end

  @spec validate_options(term()) :: :ok | {:error, Error.t()}
  defp validate_options(opts) when is_list(opts) do
    if Keyword.keyword?(opts) do
      case Keyword.keys(opts) -- [:transports] do
        [] -> :ok
        unknown -> {:error, Error.not_sent(:validation, {:unknown_runtime_options, unknown})}
      end
    else
      {:error, Error.not_sent(:validation, {:invalid_runtime_options, opts})}
    end
  end

  defp validate_options(opts),
    do: {:error, Error.not_sent(:validation, {:invalid_runtime_options, opts})}

  @spec normalize_transports(term()) ::
          {:ok, [normalized_transport()]} | {:error, Error.t()}
  defp normalize_transports(transports) when is_list(transports) do
    normalize_entries(transports, &normalize_transport/1)
  end

  defp normalize_transports(transports) do
    {:error, Error.not_sent(:validation, {:invalid_transport_registrations, transports})}
  end

  @spec normalize_transport(term()) :: {:ok, normalized_transport()} | {:error, Error.t()}
  defp normalize_transport({name, module}), do: normalize_transport({name, module, []})

  defp normalize_transport({name, module, opts})
       when is_atom(name) and not is_nil(name) and is_atom(module) and not is_nil(module) and
              is_list(opts) do
    if Keyword.keyword?(opts) do
      {:ok, {name, module, opts}}
    else
      {:error, Error.not_sent(:validation, {:invalid_transport_registration_options, name, opts})}
    end
  end

  defp normalize_transport(config) do
    {:error, Error.not_sent(:validation, {:invalid_transport_registration, config})}
  end

  @spec normalize_entries(
          [term()],
          (term() -> {:ok, term()} | {:error, Error.t()})
        ) :: {:ok, [term()]} | {:error, Error.t()}
  defp normalize_entries(entries, normalizer) do
    Enum.reduce_while(entries, {:ok, []}, fn entry, {:ok, normalized} ->
      case normalizer.(entry) do
        {:ok, value} -> {:cont, {:ok, [value | normalized]}}
        {:error, %Error{} = error} -> {:halt, {:error, error}}
      end
    end)
    |> case do
      {:ok, normalized} -> {:ok, Enum.reverse(normalized)}
      error -> error
    end
  end

  @spec register_transports([normalized_transport()]) :: :ok | {:error, Error.t()}
  defp register_transports(transports) do
    Enum.reduce_while(transports, :ok, fn {name, module, opts}, :ok ->
      case Fabric.register_transport(name, module, opts) do
        :ok -> {:cont, :ok}
        {:error, %Error{} = error} -> {:halt, {:error, error}}
      end
    end)
  end

  @spec discover_agents() :: [module()]
  defp discover_agents do
    loaded_modules =
      :code.all_loaded()
      |> Enum.map(&elem(&1, 0))

    application_modules =
      Application.loaded_applications()
      |> Enum.filter(fn {application, _description, _version} ->
        application == :spectre_pulse or
          :spectre_pulse in List.wrap(Application.spec(application, :applications))
      end)
      |> Enum.flat_map(fn {application, _description, _version} ->
        List.wrap(Application.spec(application, :modules))
      end)

    (loaded_modules ++ application_modules)
    |> Enum.uniq()
    |> Enum.filter(&pulse_agent?/1)
    |> Enum.sort()
  end

  @spec pulse_agent?(module()) :: boolean()
  defp pulse_agent?(module) when is_atom(module) do
    Code.ensure_loaded?(module) and function_exported?(module, :__spectre_pulse__, 0)
  end

  @spec subscribe_agents([module()]) :: {:ok, [String.t()]} | {:error, Error.t()}
  defp subscribe_agents(agents) do
    with {:ok, subscriptions} <- agent_subscriptions(agents),
         :ok <- ensure_unique_identities(subscriptions) do
      Enum.reduce_while(subscriptions, {:ok, []}, &reconcile_subscription/2)
    end
  end

  @spec reconcile_subscription(subscription(), {:ok, [String.t()]}) ::
          {:cont, {:ok, [String.t()]}} | {:halt, {:error, Error.t()}}
  defp reconcile_subscription({agent, identity}, {:ok, owned}) do
    identity
    |> Local.lookup()
    |> reconcile_lookup(agent, identity, owned)
  end

  @spec reconcile_lookup(
          {:ok, pid(), map()} | :error,
          module(),
          String.t(),
          [String.t()]
        ) :: {:cont, {:ok, [String.t()]}} | {:halt, {:error, Error.t()}}
  defp reconcile_lookup(
         {:ok, _pid, %{agent: agent, owner: owner}},
         agent,
         identity,
         owned
       )
       when owner == self(),
       do: {:cont, {:ok, [identity | owned]}}

  defp reconcile_lookup(
         {:ok, pid, %{agent: agent, owner: owner}},
         agent,
         identity,
         owned
       )
       when is_pid(owner),
       do: reconcile_runtime_owner(owner, pid, agent, identity, owned)

  defp reconcile_lookup({:ok, _pid, %{agent: agent}}, agent, _identity, owned),
    do: {:cont, {:ok, owned}}

  defp reconcile_lookup({:ok, _pid, metadata}, agent, identity, owned) do
    stop_subscriptions_and_error(
      owned,
      {:pulse_identity_already_subscribed, identity, Map.get(metadata, :agent), agent}
    )
  end

  defp reconcile_lookup(:error, agent, identity, owned),
    do: start_owned_subscription(agent, identity, owned)

  @spec reconcile_runtime_owner(pid(), pid(), module(), String.t(), [String.t()]) ::
          {:cont, {:ok, [String.t()]}} | {:halt, {:error, Error.t()}}
  defp reconcile_runtime_owner(owner, pid, agent, identity, owned) do
    if Process.alive?(owner) do
      stop_subscriptions_and_error(
        owned,
        {:agent_owned_by_another_runtime, agent, owner}
      )
    else
      replace_orphaned_subscription(pid, agent, identity, owned)
    end
  end

  @spec agent_subscriptions([module()]) ::
          {:ok, [subscription()]} | {:error, Error.t()}
  defp agent_subscriptions(agents) do
    Enum.reduce_while(agents, {:ok, []}, fn agent, {:ok, subscriptions} ->
      case Local.subscription(agent, []) do
        {:ok, identity, _endpoint, _endpoint_opts} ->
          {:cont, {:ok, [{agent, identity} | subscriptions]}}

        {:error, %Error{} = error} ->
          {:halt, {:error, error}}
      end
    end)
    |> case do
      {:ok, subscriptions} -> {:ok, Enum.reverse(subscriptions)}
      error -> error
    end
  end

  @spec ensure_unique_identities([subscription()]) :: :ok | {:error, Error.t()}
  defp ensure_unique_identities(subscriptions) do
    duplicate =
      subscriptions
      |> Enum.group_by(&elem(&1, 1), &elem(&1, 0))
      |> Enum.sort_by(&elem(&1, 0))
      |> Enum.find(fn {_identity, agents} -> match?([_, _ | _], agents) end)

    case duplicate do
      nil ->
        :ok

      {identity, agents} ->
        {:error,
         Error.not_sent(
           :validation,
           {:duplicate_pulse_identity, identity, Enum.sort(agents)}
         )}
    end
  end

  @spec stop_subscriptions_and_error([String.t()], term()) ::
          {:halt, {:error, Error.t()}}
  defp stop_subscriptions_and_error(owned, reason) do
    Enum.each(owned, &stop_owned_subscription/1)
    {:halt, {:error, Error.not_sent(:routing, reason)}}
  end

  @spec replace_orphaned_subscription(pid(), module(), String.t(), [String.t()]) ::
          {:cont, {:ok, [String.t()]}} | {:halt, {:error, Error.t()}}
  defp replace_orphaned_subscription(pid, agent, identity, owned) do
    case DynamicSupervisor.terminate_child(Spectre.Pulse.Local.Supervisor, pid) do
      :ok ->
        start_owned_subscription(agent, identity, owned)

      {:error, reason} ->
        stop_subscriptions_and_error(
          owned,
          {:orphaned_subscription_cleanup_failed, agent, reason}
        )
    end
  end

  @spec start_owned_subscription(module(), String.t(), [String.t()]) ::
          {:cont, {:ok, [String.t()]}} | {:halt, {:error, Error.t()}}
  defp start_owned_subscription(agent, identity, owned) do
    case Local.subscribe(agent, owner: self()) do
      {:ok, _pid} ->
        {:cont, {:ok, [identity | owned]}}

      {:error, reason} ->
        stop_subscriptions_and_error(
          owned,
          {:agent_subscription_failed, agent, reason}
        )
    end
  end

  @spec stop_owned_subscription(String.t()) :: :ok | {:error, :not_found}
  defp stop_owned_subscription(identity) do
    case Local.lookup(identity) do
      {:ok, pid, %{owner: owner}} when owner == self() ->
        DynamicSupervisor.terminate_child(Spectre.Pulse.Local.Supervisor, pid)

      _other ->
        :ok
    end
  catch
    :exit, _reason -> :ok
  end
end

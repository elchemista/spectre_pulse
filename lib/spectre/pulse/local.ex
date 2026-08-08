defmodule Spectre.Pulse.Local do
  @moduledoc """
  Local Agent subscriptions backed by BEAM process mailboxes.

  The supervised Pulse Runtime discovers modules using `Spectre.Pulse` and
  gives each logical address a local endpoint process. `subscribe/2` exposes
  the same operation to low-level hosts. Discovery can then prefer that
  endpoint automatically; neither the sending Agent nor its contact book
  contains a PID or chooses the Local transport.
  """

  alias Spectre.Pulse.Address
  alias Spectre.Pulse.Config
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Options
  alias Spectre.Pulse.Route

  @registry Spectre.Pulse.Local.Registry
  @supervisor Spectre.Pulse.Local.Supervisor

  @doc "Subscribes a Pulse-enabled Agent on its compiled logical identity."
  @spec subscribe(module(), keyword()) :: DynamicSupervisor.on_start_child()
  def subscribe(agent, opts \\ [])

  def subscribe(agent, opts) when is_atom(agent) and not is_nil(agent) do
    with {:ok, identity, _endpoint, _endpoint_opts} <- subscription(agent, opts) do
      case lookup(identity) do
        {:ok, pid, %{agent: ^agent}} ->
          {:ok, pid}

        {:ok, _pid, metadata} ->
          identity_conflict(identity, agent, metadata)

        :error ->
          start_subscription(agent, identity, opts)
      end
    end
  catch
    :exit, reason -> {:error, {:local_supervisor_unavailable, reason}}
  end

  def subscribe(agent, _opts),
    do: {:error, Error.not_sent(:validation, {:invalid_pulse_agent, agent})}

  @doc "Returns the Registry name used by one canonical local subscription."
  @spec via(String.t()) :: {:via, Registry, {module(), String.t()}}
  def via(address) do
    {:via, Registry, {@registry, Address.normalize!(address)}}
  end

  @doc "Returns subscription metadata for an address."
  @spec lookup(String.t()) :: {:ok, pid(), map()} | :error
  def lookup(address) do
    case Address.normalize(address) do
      {:ok, canonical} -> lookup_canonical(canonical)
      {:error, _error} -> :error
    end
  catch
    :exit, _reason -> :error
  end

  @doc "Resolves a subscribed address to its Spectre endpoint."
  @spec resolve_target(String.t(), term()) :: {:ok, term()} | :error
  def resolve_target(address, _context \\ nil) do
    case lookup(address) do
      {:ok, _pid, %{endpoint: endpoint}} -> {:ok, endpoint}
      _ -> :error
    end
  end

  @doc "Returns the automatically advertised Local route, when subscribed."
  @spec routes(String.t()) :: [Route.t()]
  def routes(address) do
    with {:ok, canonical} <- Address.normalize(address),
         {:ok, _pid, _metadata} <- lookup(canonical) do
      [
        Route.local(canonical, via(canonical),
          id: "local:" <> canonical,
          priority: 0,
          metadata: %{discovered_by: :local_subscription}
        )
      ]
    else
      _ -> []
    end
  end

  @doc false
  @spec subscription(module(), keyword()) ::
          {:ok, String.t(), term(), keyword()} | {:error, Error.t()}
  def subscription(agent, opts) when is_atom(agent) and not is_nil(agent) do
    with {:ok, opts} <- Options.keyword(opts),
         :ok <- validate_owner(Keyword.get(opts, :owner)),
         {:ok, inbound} <- Options.keyword(Keyword.get(opts, :inbound, [])),
         {:ok, config} <- Config.fetch(agent),
         {:ok, identity} <-
           Address.normalize(Keyword.get(opts, :identity, config.identity)) do
      endpoint = Keyword.get(opts, :endpoint, agent)

      endpoint_opts =
        config.inbound
        |> Keyword.merge(inbound)
        |> maybe_put(:on_result, Keyword.get(opts, :on_result))

      {:ok, identity, endpoint, endpoint_opts}
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, Error.not_sent(:validation, reason)}
    end
  end

  def subscription(agent, _opts),
    do: {:error, Error.not_sent(:validation, {:invalid_pulse_agent, agent})}

  @spec lookup_canonical(String.t()) :: {:ok, pid(), map()} | :error
  defp lookup_canonical(address) do
    case Registry.lookup(@registry, address) do
      [{pid, metadata}] when is_pid(pid) -> normalize_registry_entry(pid, metadata)
      _other -> :error
    end
  end

  @spec normalize_registry_entry(pid(), term()) :: {:ok, pid(), map()} | :error
  defp normalize_registry_entry(pid, metadata) when is_map(metadata),
    do: {:ok, pid, metadata}

  defp normalize_registry_entry(pid, metadata) when is_list(metadata) do
    if Keyword.keyword?(metadata), do: {:ok, pid, Map.new(metadata)}, else: :error
  end

  defp normalize_registry_entry(_pid, _metadata), do: :error

  @spec validate_owner(term()) :: :ok | {:error, Error.t()}
  defp validate_owner(owner) when is_nil(owner) or is_pid(owner), do: :ok

  defp validate_owner(owner),
    do: {:error, Error.not_sent(:validation, {:invalid_subscription_owner, owner})}

  @spec maybe_put(keyword(), atom(), term()) :: keyword()
  defp maybe_put(opts, _key, nil), do: opts
  defp maybe_put(opts, key, value), do: Keyword.put(opts, key, value)

  @spec start_subscription(module(), String.t(), keyword()) ::
          DynamicSupervisor.on_start_child() | {:error, term()}
  defp start_subscription(agent, identity, opts) do
    child_opts = Keyword.put(opts, :agent, agent)

    case DynamicSupervisor.start_child(@supervisor, {Spectre.Pulse.Local.Endpoint, child_opts}) do
      {:error, {:already_started, pid}} ->
        case lookup(identity) do
          {:ok, ^pid, %{agent: ^agent}} -> {:ok, pid}
          {:ok, _pid, metadata} -> identity_conflict(identity, agent, metadata)
          :error -> {:error, {:local_subscription_race, identity}}
        end

      other ->
        other
    end
  end

  @spec identity_conflict(String.t(), module(), map()) :: {:error, term()}
  defp identity_conflict(identity, agent, metadata) do
    {:error, {:pulse_identity_already_subscribed, identity, Map.get(metadata, :agent), agent}}
  end
end

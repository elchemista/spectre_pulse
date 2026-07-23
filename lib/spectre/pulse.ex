defmodule Spectre.Pulse do
  @moduledoc """
  Transport-independent communication protocol for Spectre agents.

  Pulse supplies identity, immutable envelopes, correlation, contact/routing
  values, technical delivery, and a bridge to the ordinary Spectre turn. It
  owns no room, task, workflow, store, journal, retry loop, or semantic process
  per message.
  """

  alias Spectre.Agent
  alias Spectre.Context
  alias Spectre.Pulse.Config
  alias Spectre.Pulse.ContactBook
  alias Spectre.Pulse.Directory
  alias Spectre.Pulse.Discovery
  alias Spectre.Pulse.DSL
  alias Spectre.Pulse.Envelope
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Executor
  alias Spectre.Pulse.Inbound
  alias Spectre.Pulse.Network
  alias Spectre.Pulse.Protocol
  alias Spectre.Pulse.Runtime
  alias Spectre.Pulse.State, as: PulseState
  alias Spectre.State

  @doc """
  Starts the host Pulse runtime.

  Add `{Spectre.Pulse, transports: [...]}` to the host application's
  supervision tree. Modules using `Spectre.Pulse` are discovered and
  subscribed automatically; `transports` contains only application-defined
  transport drivers.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts), do: Runtime.start_link(opts)

  @doc false
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    id = Keyword.get(opts, :id, __MODULE__)

    %{
      id: id,
      start: {__MODULE__, :start_link, [Keyword.delete(opts, :id)]},
      type: :worker
    }
  end

  @doc "Installs the Pulse DSL into a module which already uses `Spectre.Agent`."
  @spec __using__(Macro.t()) :: Macro.t()
  defmacro __using__(opts \\ []) do
    opts = Macro.expand(opts, __CALLER__)

    unless is_list(opts) do
      raise ArgumentError, "use Spectre.Pulse expects a keyword list"
    end

    quote bind_quoted: [opts: opts] do
      DSL.install!(__MODULE__, opts)

      require Agent

      import Agent,
        except: [flow: 2, interrupt: 2, interrupt: 3]

      import DSL

      @before_compile DSL
    end
  end

  @doc "Returns the compiled Pulse configuration for an Agent."
  @spec config(module()) :: {:ok, Config.t()} | {:error, Spectre.Pulse.Error.t()}
  def config(agent), do: Config.fetch(agent)

  @doc "Returns the transport-neutral Pulse v1 protocol description."
  @spec protocol() :: map()
  def protocol, do: Protocol.describe()

  @doc "Builds a validated envelope."
  @spec envelope(map() | keyword()) ::
          {:ok, Envelope.t()} | {:error, Spectre.Pulse.Error.t()}
  def envelope(attrs), do: Envelope.new(attrs)

  @doc "Runs the secure inbound bridge and ordinary `Spectre.turn/3`."
  @spec receive(Envelope.t() | map(), map() | keyword(), keyword()) ::
          {:ok, Spectre.Pulse.Inbound.Result.t()} | {:error, Spectre.Pulse.Error.t()}
  def receive(envelope, inbound_context, opts \\ []),
    do: Inbound.receive(envelope, inbound_context, opts)

  @doc """
  Delivers an already-built envelope through a network.

  Agent route handlers should stage an Effect with `pulse/2` instead of calling
  this function directly.
  """
  @spec deliver(Envelope.t(), keyword()) ::
          {:ok, Spectre.Pulse.Receipt.t()} | {:error, Spectre.Pulse.Error.t()}
  def deliver(%Envelope{} = envelope, opts) do
    with {:ok, routes} <- Discovery.routes(envelope.to, opts) do
      Network.deliver(
        Keyword.get(opts, :network),
        envelope,
        Keyword.put(opts, :routes, routes)
      )
    end
  end

  @doc """
  Subscribes a Pulse-enabled Agent at its logical address on this BEAM node.

  The supervised Runtime does this automatically. This low-level function is
  available to hosts which manage subscriptions themselves. The sending Agent
  remains unaware of the PID and Local transport.
  """
  @spec subscribe(module(), keyword()) :: DynamicSupervisor.on_start_child()
  defdelegate subscribe(agent, opts \\ []), to: Spectre.Pulse.Local

  @doc "Registers an application transport driver in the Pulse Fabric."
  @spec register_transport(atom(), module(), keyword()) ::
          :ok | {:error, Spectre.Pulse.Error.t()}
  defdelegate register_transport(name, module, opts \\ []), to: Spectre.Pulse.Fabric

  @doc "Connects a physical target to a logical address through a registered driver."
  @spec connect(String.t(), atom(), term(), keyword()) ::
          {:ok, Spectre.Pulse.Route.t()} | {:error, Spectre.Pulse.Error.t()}
  defdelegate connect(address, transport, target, opts \\ []), to: Spectre.Pulse.Fabric

  @doc "Removes an ephemeral connected route from the Pulse Fabric."
  @spec disconnect(term()) :: :ok | {:error, Spectre.Pulse.Error.t()}
  defdelegate disconnect(route_id), to: Spectre.Pulse.Fabric

  @doc "Executes a pending Pulse effect in an immutable Spectre result."
  @spec execute(module(), Spectre.Result.t(), keyword()) ::
          {:ok, Spectre.Result.t()} | {:error, term()}
  defdelegate execute(agent, result, opts \\ []), to: Executor

  @doc "Executes the Pulse effect selected by a Spectre turn."
  @spec execute_turn(Spectre.Turn.t(), keyword()) ::
          {:ok, Spectre.Turn.t()} | {:error, term()}
  defdelegate execute_turn(turn, opts \\ []), to: Executor

  @doc "Returns the Agent's static contacts, optionally merged with state contacts."
  @spec contacts(module() | Context.t() | {module(), State.t()}) ::
          [Spectre.Pulse.Contact.t()]
  def contacts(source) do
    with {:ok, agent, state} <- agent_and_state(source),
         {:ok, config} <- Config.fetch(agent) do
      contacts_for(config, state)
    else
      _ -> []
    end
  end

  @doc "Resolves a local contact key or canonical address."
  @spec resolve(module() | Context.t() | {module(), State.t()}, term()) ::
          {:ok, String.t()} | {:error, Error.t()}
  def resolve(source, reference) do
    with {:ok, agent, state} <- agent_and_state(source),
         {:ok, config} <- Config.fetch(agent),
         book <- contact_book(config, state),
         {:ok, resolution} <-
           Discovery.resolve_identity(book, reference, directory: config.directory) do
      {:ok, resolution.address}
    end
  end

  @doc "Finds known contacts by exact capability or identity."
  @spec find_contacts(module() | Context.t() | {module(), State.t()}, keyword()) ::
          [Spectre.Pulse.Contact.t()]
  def find_contacts(source, opts) do
    source
    |> contacts()
    |> ContactBook.new!()
    |> ContactBook.find(opts)
  end

  @doc "Returns a new Spectre state remembering a contact."
  @spec remember_contact(
          State.t(),
          Spectre.Pulse.Contact.t() | map() | keyword()
        ) :: {:ok, State.t()} | {:error, Error.t()}
  defdelegate remember_contact(state, contact), to: PulseState

  @doc "Returns a new Spectre state forgetting a contact."
  @spec forget_contact(State.t(), term()) :: State.t()
  defdelegate forget_contact(state, reference), to: PulseState

  @doc "Returns a new Spectre state with a matching expectation resolved."
  @spec correlate(State.t(), Envelope.t()) ::
          {:ok, State.t(), Spectre.Pulse.Expectation.t()} | :unmatched
  defdelegate correlate(state, envelope), to: PulseState

  @doc "Probes current technical reachability without claiming agent availability."
  @spec reachability(
          module() | Context.t() | {module(), State.t()},
          term(),
          keyword()
        ) ::
          {:ok, Spectre.Pulse.Reachability.t()} | {:error, Spectre.Pulse.Error.t()}
  def reachability(source, reference, opts \\ []) do
    with {:ok, agent, state} <- agent_and_state(source),
         {:ok, config} <- Config.fetch(agent),
         book <-
           if(state,
             do: PulseState.contact_book(state, config.contacts),
             else: config.contacts
           ),
         {:ok, resolution} <-
           Discovery.resolve_identity(
             book,
             reference,
             Keyword.put(opts, :directory, config.directory)
           ),
         {:ok, routes} <-
           Discovery.routes(
             resolution.address,
             opts
             |> Keyword.put(:directory, config.directory)
             |> Keyword.put(:routes, resolution.routes)
           ) do
      Network.probe(config.network, resolution.address, Keyword.put(opts, :routes, routes))
    end
  end

  @spec contacts_for(Config.t(), State.t() | nil) :: [Spectre.Pulse.Contact.t()]
  defp contacts_for(config, state) do
    local = contact_book(config, state)

    case merged_contact_book(config.directory, local) do
      {:ok, merged} -> ContactBook.contacts(merged)
      {:error, _error} -> ContactBook.contacts(local)
    end
  end

  @spec contact_book(Config.t(), State.t() | nil) :: ContactBook.t()
  defp contact_book(config, nil), do: config.contacts
  defp contact_book(config, state), do: PulseState.contact_book(state, config.contacts)

  @spec merged_contact_book(term(), ContactBook.t()) ::
          {:ok, ContactBook.t()} | {:error, Error.t()}
  defp merged_contact_book(directory, local) do
    with {:ok, external_contacts} <- directory_contacts(directory),
         {:ok, external} <- ContactBook.new(external_contacts) do
      ContactBook.merge([external, local])
    end
  end

  @spec directory_contacts(term()) :: {:ok, [Spectre.Pulse.Contact.t()]} | {:error, Error.t()}
  defp directory_contacts(directory) do
    [directory | Discovery.directories()]
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
    |> Enum.reduce_while({:ok, []}, &collect_directory_contacts/2)
    |> concat_contact_chunks()
  end

  @spec collect_directory_contacts(term(), {:ok, [[Spectre.Pulse.Contact.t()]]}) ::
          {:cont, {:ok, [[Spectre.Pulse.Contact.t()]]}} | {:halt, {:error, Error.t()}}
  defp collect_directory_contacts(source, {:ok, chunks}) do
    case Directory.contacts(source, []) do
      {:ok, discovered} -> {:cont, {:ok, [discovered | chunks]}}
      {:error, error} -> {:halt, {:error, error}}
    end
  end

  @spec concat_contact_chunks({:ok, [[Spectre.Pulse.Contact.t()]]} | {:error, Error.t()}) ::
          {:ok, [Spectre.Pulse.Contact.t()]} | {:error, Error.t()}
  defp concat_contact_chunks({:ok, chunks}), do: {:ok, chunks |> Enum.reverse() |> Enum.concat()}
  defp concat_contact_chunks({:error, %Error{} = error}), do: {:error, error}

  @spec agent_and_state(module() | Context.t() | {module(), State.t()}) ::
          {:ok, module(), State.t() | nil} | {:error, Error.t()}
  defp agent_and_state(%Context{agent: agent, state: state}),
    do: {:ok, agent, state}

  defp agent_and_state({agent, %State{} = state}) when is_atom(agent),
    do: {:ok, agent, state}

  defp agent_and_state(agent) when is_atom(agent), do: {:ok, agent, nil}

  defp agent_and_state(source),
    do: {:error, Error.not_sent(:validation, {:invalid_agent_context, source})}
end

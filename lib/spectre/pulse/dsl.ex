defmodule Spectre.Pulse.DSL do
  @moduledoc """
  Compile-time integration for Pulse-enabled Spectre Agents.

  `pulse/2` is rewritten to a normal Spectre `run` handler which stages a
  generic Pulse effect; delivery never occurs during routing.
  """

  alias Spectre.Pulse.Address
  alias Spectre.Pulse.Config
  alias Spectre.Pulse.Contact
  alias Spectre.Pulse.EffectBuilder

  @doc false
  @spec install!(module(), keyword()) :: :ok
  def install!(module, opts) when is_atom(module) and is_list(opts) do
    unless Module.has_attribute?(module, :spectre_config) and
             Module.has_attribute?(module, :spectre_rules) do
      raise ArgumentError, "use Spectre.Agent must appear before use Spectre.Pulse"
    end

    Module.register_attribute(module, :spectre_pulse_identity, persist: false)
    Module.register_attribute(module, :spectre_pulse_directory, persist: false)
    Module.register_attribute(module, :spectre_pulse_network, persist: false)
    Module.register_attribute(module, :spectre_pulse_state_scope, persist: false)
    Module.register_attribute(module, :spectre_pulse_contacts, accumulate: true, persist: false)
    Module.register_attribute(module, :spectre_pulse_advertise, persist: false)
    Module.register_attribute(module, :spectre_pulse_inbound, persist: false)

    Module.put_attribute(
      module,
      :spectre_pulse_state_scope,
      Keyword.get(opts, :state_scope, :agent)
    )

    Module.put_attribute(module, :spectre_pulse_advertise, %{})
    Module.put_attribute(module, :spectre_pulse_inbound, Keyword.get(opts, :inbound, []))
    :ok
  end

  @doc "Groups the declarative Pulse configuration."
  @spec pulsing(keyword()) :: Macro.t()
  defmacro pulsing(do: block), do: block

  @doc """
  Overrides the Agent's automatically assigned canonical logical identity.

  Without this declaration Pulse derives a stable 128-bit address from the
  module.
  """
  @spec identity(Macro.t()) :: Macro.t()
  defmacro identity(address) do
    quote do
      @spectre_pulse_identity unquote(address)
    end
  end

  @doc "Configures an optional application-owned directory."
  @spec directory(Macro.t()) :: Macro.t()
  defmacro directory(source) do
    quote do
      @spectre_pulse_directory unquote(source)
    end
  end

  @doc "Configures an optional application-owned network."
  @spec network(Macro.t()) :: Macro.t()
  defmacro network(source) do
    quote do
      @spectre_pulse_network unquote(source)
    end
  end

  @doc "Selects `:agent`, `:peer`, or a callback for inbound Spectre state."
  @spec state_scope(Macro.t()) :: Macro.t()
  defmacro state_scope(scope) do
    quote do
      @spectre_pulse_state_scope unquote(scope)
    end
  end

  @doc "Declares one static, agent-owned contact."
  @spec contact(Macro.t(), Macro.t(), Macro.t()) :: Macro.t()
  defmacro contact(key, address, opts \\ []) do
    quote do
      @spectre_pulse_contacts Contact.new!(unquote(key), unquote(address), unquote(opts))
    end
  end

  @doc "Declares the safe public identity projection."
  @spec advertise(Macro.t()) :: Macro.t()
  defmacro advertise(opts) do
    quote do
      @spectre_pulse_advertise Map.merge(
                                 @spectre_pulse_advertise || %{},
                                 Map.new(unquote(opts))
                               )
    end
  end

  @doc "Configures inbound validation and bridge options."
  @spec pulse_inbound(Macro.t()) :: Macro.t()
  defmacro pulse_inbound(opts) do
    quote do
      @spectre_pulse_inbound Keyword.merge(@spectre_pulse_inbound || [], unquote(opts))
    end
  end

  @doc """
  Stages an outbound Pulse effect inside a Spectre route.

      pulse :tao,
        act: :request,
        type: "research.perform",
        build: :build_request,
        expect: "research.completed"
  """
  @spec pulse(Macro.t(), Macro.t()) :: Macro.t()
  defmacro pulse(to, opts \\ []) do
    quote do
      run(:__spectre_pulse_stage__,
        spectre_pulse: Keyword.put(unquote(opts), :to, unquote(to))
      )
    end
  end

  @doc """
  Pulse-aware wrapper around `Spectre.Agent.flow/2`.

  It also compiles `pulse: "domain.type"` route evidence into a deterministic
  metadata check without adding a parallel router.
  """
  @spec flow(Macro.t(), keyword()) :: Macro.t()
  defmacro flow(name, do: block) do
    rewritten = rewrite(block)

    quote do
      Spectre.Agent.flow unquote(name) do
        unquote(rewritten)
      end
    end
  end

  @doc false
  @spec interrupt(Macro.t(), Macro.t(), keyword()) :: Macro.t()
  defmacro interrupt(label, opts, do: block) do
    rewritten = rewrite(block)
    rewritten_opts = rewrite_route_opts(opts)

    quote do
      Spectre.Agent.interrupt unquote(label), unquote(rewritten_opts) do
        unquote(rewritten)
      end
    end
  end

  @doc false
  @spec interrupt(Macro.t(), keyword()) :: Macro.t()
  defmacro interrupt(label, opts) do
    {block, opts} = Keyword.pop!(opts, :do)
    rewritten = rewrite(block)
    rewritten_opts = rewrite_route_opts(opts)

    quote do
      Spectre.Agent.interrupt unquote(label), unquote(rewritten_opts) do
        unquote(rewritten)
      end
    end
  end

  @doc false
  @spec __before_compile__(Macro.Env.t()) :: Macro.t()
  defmacro __before_compile__(env) do
    module = env.module

    identity =
      Module.get_attribute(module, :spectre_pulse_identity) ||
        Address.for_agent(module)

    contacts =
      module
      |> Module.get_attribute(:spectre_pulse_contacts)
      |> List.wrap()
      |> Enum.reverse()

    config =
      Config.new!(
        identity: identity,
        directory: Module.get_attribute(module, :spectre_pulse_directory),
        network: Module.get_attribute(module, :spectre_pulse_network),
        state_scope: Module.get_attribute(module, :spectre_pulse_state_scope) || :agent,
        contacts: contacts,
        advertise: Module.get_attribute(module, :spectre_pulse_advertise) || %{},
        inbound: Module.get_attribute(module, :spectre_pulse_inbound) || []
      )

    quote do
      @doc false
      @spec __spectre_pulse__() :: Spectre.Pulse.Config.t()
      def __spectre_pulse__, do: unquote(Macro.escape(config))

      @doc false
      @spec __spectre_pulse_stage__(Spectre.Input.t(), Spectre.Context.t()) ::
              {:ok, Spectre.Result.t()} | {:error, term()}
      def __spectre_pulse_stage__(input, ctx) do
        opts = Keyword.fetch!(ctx.opts, :spectre_pulse)
        EffectBuilder.stage(__MODULE__, input, ctx, opts)
      end

      @doc "Receives one envelope through the normal Pulse inbound bridge."
      @spec handle_pulse(
              Spectre.Pulse.Envelope.t() | map(),
              Spectre.Pulse.InboundContext.t() | map() | keyword(),
              keyword()
            ) ::
              {:ok, Spectre.Pulse.Inbound.Result.t()} | {:error, Spectre.Pulse.Error.t()}
      def handle_pulse(envelope, inbound_context, opts \\ []) do
        Spectre.Pulse.receive(
          envelope,
          Map.put(Map.new(inbound_context), :target, __MODULE__),
          opts
        )
      end

      defoverridable handle_pulse: 3
    end
  end

  @doc false
  @spec rewrite(Macro.t()) :: Macro.t()
  def rewrite(block) do
    Macro.prewalk(block, fn
      {:pulse, meta, [to]} ->
        pulse_run_ast(meta, to, [])

      {:pulse, meta, [to, opts]} when is_list(opts) ->
        pulse_run_ast(meta, to, opts)

      {:on, meta, [label, opts, [do: handler]]} when is_list(opts) ->
        {:on, meta, [label, rewrite_route_opts(opts), [do: rewrite(handler)]]}

      {:on, meta, [label, [do: handler]]} ->
        {:on, meta, [label, [do: rewrite(handler)]]}

      node ->
        node
    end)
  end

  @doc false
  @spec rewrite_route_opts(value) :: keyword() | value when value: term()
  def rewrite_route_opts(opts) when is_list(opts) do
    case Keyword.pop(opts, :pulse) do
      {nil, opts} ->
        opts

      {type, opts} ->
        opts
        |> add_pulse_check(type)
        |> add_default_pulse_evidence()
        |> Keyword.put_new(:cache, false)
    end
  end

  def rewrite_route_opts(opts), do: opts

  @spec pulse_run_ast(keyword(), Macro.t(), keyword()) :: Macro.t()
  defp pulse_run_ast(meta, to, opts) do
    pulse_opts = Keyword.put(opts, :to, to)
    {:run, meta, [:__spectre_pulse_stage__, [spectre_pulse: pulse_opts]]}
  end

  @spec add_pulse_check(keyword(), Macro.t()) :: keyword()
  defp add_pulse_check(opts, type) do
    checks = List.wrap(Keyword.get(opts, :checks, []))
    opts |> Keyword.delete(:checks) |> Keyword.put(:checks, [{:pulse_type, type} | checks])
  end

  @spec add_default_pulse_evidence(keyword()) :: keyword()
  defp add_default_pulse_evidence(opts) do
    evidence? = Enum.any?([:regex, :bag, :jaro, :embedding], &Keyword.has_key?(opts, &1))

    if evidence? do
      opts
    else
      opts
      |> Keyword.put(:regex, Macro.escape(~r/[\s\S]*/))
      |> Keyword.put(:via, [:regex])
    end
  end
end

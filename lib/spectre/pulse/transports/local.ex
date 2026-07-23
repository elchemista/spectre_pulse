defmodule Spectre.Pulse.Transports.Local do
  @moduledoc """
  In-node binding implemented with the BEAM process mailbox.

  A Local Route points to a PID or registered process name. Delivery uses
  `Kernel.send/2` and returns as soon as the envelope has been put on that
  mailbox. The subscribed process calls `handle_message/3` to run the common
  Pulse inbound bridge.

  The sender PID carried in the tuple is a routing fact inside the trusted VM,
  not cryptographic proof: any local process can forge arbitrary messages.
  Applications running untrusted BEAM code must add their own authorization
  policy at the endpoint.
  """

  @behaviour Spectre.Pulse.Transport

  alias Spectre.Pulse.Endpoint
  alias Spectre.Pulse.Envelope
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.InboundContext
  alias Spectre.Pulse.Reachability
  alias Spectre.Pulse.Receipt
  alias Spectre.Pulse.Route

  @typedoc "Message put directly into a subscribed Agent process mailbox."
  @type message :: {:spectre_pulse, pid(), Envelope.t()}

  @doc false
  @spec deliver(Route.t(), Envelope.t(), keyword()) ::
          {:ok, Receipt.t()} | {:error, Error.t()}
  @impl Spectre.Pulse.Transport
  def deliver(%Route{} = route, %Envelope{} = envelope, _opts) do
    case resolve_mailbox(route.target) do
      {:ok, mailbox} ->
        Kernel.send(mailbox, {:spectre_pulse, self(), envelope})

        {:ok,
         Receipt.accepted(envelope.id,
           via: :local,
           route_id: route.id,
           metadata: %{mailbox: inspect(route.target)}
         )}

      {:error, reason} ->
        {:error,
         Error.not_sent(:transport, reason,
           message_id: envelope.id,
           route_id: route.id
         )}
    end
  end

  @doc false
  @spec probe(Route.t(), keyword()) :: {:ok, Reachability.t()}
  @impl Spectre.Pulse.Transport
  def probe(%Route{} = route, _opts) do
    case resolve_mailbox(route.target) do
      {:ok, _mailbox} ->
        {:ok,
         Reachability.new(:reachable,
           level: :pulse_endpoint,
           via: :local,
           valid_for_ms: 0,
           metadata: %{route_id: route.id}
         )}

      {:error, reason} ->
        {:ok,
         Reachability.new(:unreachable,
           level: :pulse_endpoint,
           via: :local,
           valid_for_ms: 0,
           reason: reason,
           metadata: %{route_id: route.id}
         )}
    end
  end

  @doc """
  Handles one mailbox message at an application endpoint.

  `endpoint` may be a Pulse-enabled Spectre Agent or any endpoint supported by
  `Spectre.Pulse.Endpoint`. This function is intended for a `GenServer`
  `handle_info/2` callback or a plain receive loop.
  """
  @spec handle_message(message(), term(), keyword()) ::
          {:ok, Receipt.t()} | {:error, Error.t()}
  def handle_message(message, endpoint, opts \\ [])

  def handle_message(
        {:spectre_pulse, sender, %Envelope{} = envelope},
        endpoint,
        opts
      )
      when is_pid(sender) do
    supplied_context =
      opts
      |> Keyword.get(:context, %{})
      |> InboundContext.new()

    context = %{
      supplied_context
      | authenticated_identity:
          supplied_context.authenticated_identity ||
            Keyword.get(opts, :authenticated_identity, envelope.from),
        binding: :local,
        peer: sender,
        target: endpoint_target(endpoint),
        target_identity: supplied_context.target_identity || Keyword.get(opts, :target_identity),
        authorization: supplied_context.authorization || Keyword.get(opts, :authorize),
        verified:
          Map.merge(
            %{local_process: inspect(sender), trust_boundary: :beam_vm},
            supplied_context.verified
          )
    }

    endpoint_opts =
      opts
      |> Keyword.drop([:context, :authenticated_identity, :target_identity, :authorize])
      |> Keyword.put(:via, :local)

    Endpoint.accept(endpoint, envelope, context, endpoint_opts)
  end

  def handle_message(_message, _endpoint, _opts) do
    {:error, Error.not_sent(:inbound, :invalid_local_mailbox_message)}
  end

  @spec resolve_mailbox(term()) :: {:ok, pid()} | {:error, term()}
  defp resolve_mailbox(target) do
    case GenServer.whereis(target) do
      pid when is_pid(pid) ->
        if Process.alive?(pid),
          do: {:ok, pid},
          else: {:error, :local_endpoint_not_alive}

      nil ->
        {:error, :local_endpoint_not_found}
    end
  rescue
    _exception -> {:error, {:invalid_local_endpoint, target}}
  catch
    _kind, _reason -> {:error, {:invalid_local_endpoint, target}}
  end

  @spec endpoint_target(term()) :: module() | nil
  defp endpoint_target(endpoint) when is_atom(endpoint), do: endpoint
  defp endpoint_target(_endpoint), do: nil
end

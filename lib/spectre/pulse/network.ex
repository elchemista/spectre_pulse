defmodule Spectre.Pulse.Network do
  @moduledoc """
  Behaviour and dispatcher for technical delivery policy.

  The default routed network tries routes by priority. It fails over only after
  `:not_sent` and stops immediately on `:outcome_unknown`.
  """

  alias Spectre.Pulse.Envelope
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Network.Routed
  alias Spectre.Pulse.Reachability
  alias Spectre.Pulse.Receipt

  @callback deliver(Envelope.t(), keyword()) ::
              {:ok, Receipt.t()} | {:error, Error.t() | term()}

  @callback probe(String.t(), keyword()) ::
              {:ok, Reachability.t()} | {:error, Error.t() | term()}

  @optional_callbacks probe: 2

  @doc "Delivers through a custom network or the stateless routed default."
  @spec deliver(term(), Envelope.t(), keyword()) ::
          {:ok, Receipt.t()} | {:error, Error.t()}
  def deliver(network, envelope, opts \\ [])

  def deliver(nil, envelope, opts), do: normalize(Routed.deliver(envelope, opts), envelope)
  def deliver(Routed, envelope, opts), do: normalize(Routed.deliver(envelope, opts), envelope)

  def deliver({module, network_opts}, envelope, opts)
      when is_atom(module) and is_list(network_opts),
      do: deliver(module, envelope, Keyword.merge(network_opts, opts))

  def deliver(module, envelope, opts) when is_atom(module) do
    result =
      if Code.ensure_loaded?(module) and function_exported?(module, :deliver, 2) do
        protected_call(module, :deliver, [envelope, opts], envelope)
      else
        {:error, Error.not_sent(:routing, {:invalid_network, module}, message_id: envelope.id)}
      end

    normalize(result, envelope)
  end

  def deliver(function, envelope, opts) when is_function(function, 2) do
    result =
      try do
        function.(envelope, opts)
      rescue
        exception ->
          {:error,
           Error.outcome_unknown(:routing, {:network_exception, exception},
             message_id: envelope.id,
             cause: exception
           )}
      end

    normalize(result, envelope)
  end

  def deliver(network, envelope, _opts),
    do: {:error, Error.not_sent(:routing, {:invalid_network, network}, message_id: envelope.id)}

  @doc "Probes through a custom network or the stateless routed default."
  @spec probe(term(), String.t(), keyword()) ::
          {:ok, Reachability.t()} | {:error, Error.t()}
  def probe(network, address, opts \\ [])

  def probe(nil, address, opts), do: Routed.probe(address, opts)
  def probe(Routed, address, opts), do: Routed.probe(address, opts)

  def probe({module, network_opts}, address, opts)
      when is_atom(module) and is_list(network_opts),
      do: probe(module, address, Keyword.merge(network_opts, opts))

  def probe(module, address, opts) when is_atom(module) do
    if Code.ensure_loaded?(module) and function_exported?(module, :probe, 2) do
      case protected_call(module, :probe, [address, opts], nil) do
        {:ok, %Reachability{} = result} -> {:ok, result}
        {:error, %Error{} = error} -> {:error, error}
        {:error, reason} -> {:error, Error.not_sent(:routing, reason)}
        other -> {:error, Error.not_sent(:routing, {:invalid_network_probe_result, other})}
      end
    else
      {:ok, Reachability.unknown(:probe_not_supported)}
    end
  end

  def probe(_network, _address, _opts), do: {:ok, Reachability.unknown(:invalid_network)}

  @spec normalize(term(), Envelope.t()) :: {:ok, Receipt.t()} | {:error, Error.t()}
  defp normalize({:ok, %Receipt{} = receipt}, _envelope), do: {:ok, receipt}
  defp normalize({:error, %Error{} = error}, _envelope), do: {:error, error}

  defp normalize({:error, reason}, envelope),
    do: {:error, Error.outcome_unknown(:routing, reason, message_id: envelope.id)}

  defp normalize(other, envelope),
    do:
      {:error,
       Error.outcome_unknown(:routing, {:invalid_network_result, other}, message_id: envelope.id)}

  @spec protected_call(module(), atom(), [term()], Envelope.t() | nil) :: term()
  defp protected_call(module, function, args, envelope) do
    apply(module, function, args)
  rescue
    exception ->
      {:error,
       Error.outcome_unknown(:routing, {:network_exception, exception},
         message_id: envelope && envelope.id,
         cause: exception
       )}
  catch
    kind, reason ->
      {:error,
       Error.outcome_unknown(:routing, {:network_exit, kind, reason},
         message_id: envelope && envelope.id
       )}
  end
end

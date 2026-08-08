defmodule Spectre.Pulse.Network do
  @moduledoc """
  Behaviour and dispatcher for technical delivery policy.

  The default routed network tries routes by priority. It fails over only after
  `:not_sent` and stops immediately on `:outcome_unknown`.
  """

  alias Spectre.Pulse.Address
  alias Spectre.Pulse.Envelope
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Network.Routed
  alias Spectre.Pulse.Options
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

  def deliver(network, %Envelope{} = envelope, opts) do
    with {:ok, opts} <- Options.keyword(opts),
         {:ok, envelope} <- Envelope.new(envelope, opts) do
      do_deliver(network, envelope, opts)
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, Error.not_sent(:validation, reason, message_id: envelope.id)}
    end
  end

  def deliver(_network, envelope, _opts),
    do: {:error, Error.not_sent(:validation, {:invalid_envelope, envelope})}

  @spec do_deliver(term(), Envelope.t(), keyword()) ::
          {:ok, Receipt.t()} | {:error, Error.t()}
  defp do_deliver(nil, envelope, opts), do: normalize(Routed.deliver(envelope, opts), envelope)

  defp do_deliver(Routed, envelope, opts),
    do: normalize(Routed.deliver(envelope, opts), envelope)

  defp do_deliver({module, network_opts}, envelope, opts)
       when is_atom(module) and is_list(network_opts) do
    if Keyword.keyword?(network_opts) do
      do_deliver(module, envelope, Keyword.merge(network_opts, opts))
    else
      invalid_network({module, network_opts}, envelope)
    end
  end

  defp do_deliver(module, envelope, opts) when is_atom(module) do
    result =
      if Code.ensure_loaded?(module) and function_exported?(module, :deliver, 2) do
        protected_call(module, :deliver, [envelope, opts], envelope)
      else
        {:error, Error.not_sent(:routing, {:invalid_network, module}, message_id: envelope.id)}
      end

    normalize(result, envelope)
  end

  defp do_deliver(function, envelope, opts) when is_function(function, 2) do
    function
    |> protected_function([envelope, opts], envelope)
    |> normalize(envelope)
  end

  defp do_deliver(network, envelope, _opts), do: invalid_network(network, envelope)

  @doc "Probes through a custom network or the stateless routed default."
  @spec probe(term(), String.t(), keyword()) ::
          {:ok, Reachability.t()} | {:error, Error.t()}
  def probe(network, address, opts \\ [])

  def probe(network, address, opts) do
    with {:ok, opts} <- Options.keyword(opts),
         {:ok, address} <- Address.normalize(address) do
      do_probe(network, address, opts)
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, Error.not_sent(:validation, reason)}
    end
  end

  @spec do_probe(term(), String.t(), keyword()) ::
          {:ok, Reachability.t()} | {:error, Error.t()}
  defp do_probe(nil, address, opts), do: Routed.probe(address, opts)
  defp do_probe(Routed, address, opts), do: Routed.probe(address, opts)

  defp do_probe({module, network_opts}, address, opts)
       when is_atom(module) and is_list(network_opts) do
    if Keyword.keyword?(network_opts) do
      do_probe(module, address, Keyword.merge(network_opts, opts))
    else
      {:error, Error.not_sent(:routing, {:invalid_network, {module, network_opts}})}
    end
  end

  defp do_probe(module, address, opts) when is_atom(module) do
    if Code.ensure_loaded?(module) and function_exported?(module, :probe, 2) do
      case protected_call(module, :probe, [address, opts], nil) do
        {:ok, %Reachability{} = result} -> normalize_reachability(result)
        {:error, %Error{} = error} -> {:error, error}
        {:error, reason} -> {:error, Error.not_sent(:routing, reason)}
        other -> {:error, Error.not_sent(:routing, {:invalid_network_probe_result, other})}
      end
    else
      {:ok, Reachability.unknown(:probe_not_supported)}
    end
  end

  defp do_probe(function, address, opts) when is_function(function, 2) do
    case protected_function(function, [address, opts], nil) do
      {:ok, %Reachability{} = result} -> normalize_reachability(result)
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, Error.not_sent(:routing, reason)}
      other -> {:error, Error.not_sent(:routing, {:invalid_network_probe_result, other})}
    end
  end

  defp do_probe(network, _address, _opts),
    do: {:error, Error.not_sent(:routing, {:invalid_network, network})}

  @spec normalize(term(), Envelope.t()) :: {:ok, Receipt.t()} | {:error, Error.t()}
  defp normalize({:ok, %Receipt{} = receipt}, envelope) do
    case Receipt.new(receipt) do
      {:ok, %Receipt{message_id: message_id} = receipt} when message_id == envelope.id ->
        {:ok, receipt}

      {:ok, %Receipt{} = receipt} ->
        {:error,
         Error.outcome_unknown(:routing, :receipt_message_mismatch,
           message_id: envelope.id,
           details: %{receipt_message_id: receipt.message_id}
         )}

      {:error, %Error{} = error} ->
        {:error, %{error | outcome: :outcome_unknown, message_id: envelope.id}}
    end
  end

  defp normalize({:error, %Error{} = error}, envelope),
    do: {:error, %{error | message_id: error.message_id || envelope.id}}

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

  @spec protected_function(function(), [term()], Envelope.t() | nil) :: term()
  defp protected_function(function, args, envelope) do
    apply(function, args)
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

  @spec invalid_network(term(), Envelope.t()) :: {:error, Error.t()}
  defp invalid_network(network, envelope),
    do: {:error, Error.not_sent(:routing, {:invalid_network, network}, message_id: envelope.id)}

  @spec normalize_reachability(Reachability.t()) ::
          {:ok, Reachability.t()} | {:error, Error.t()}
  defp normalize_reachability(result) do
    if result.status in [:reachable, :unreachable, :unknown] and
         result.level in [:route_known, :pulse_endpoint] and
         is_struct(result.observed_at, DateTime) and
         is_integer(result.valid_for_ms) and result.valid_for_ms >= 0 and
         is_map(result.metadata) do
      {:ok, result}
    else
      {:error, Error.not_sent(:routing, {:invalid_reachability, result})}
    end
  end
end

defmodule Spectre.Pulse.EffectExecutor do
  @moduledoc false

  @behaviour Spectre.Effect.Executor

  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Executor
  alias Spectre.Pulse.Fabric
  alias Spectre.Pulse.Options

  @impl true
  def execute(
        %Spectre.Effect{kind: :pulse} = effect,
        %Spectre.Context{agent: agent, state: %Spectre.State{}} = context,
        opts
      )
      when is_atom(agent) and not is_nil(agent) do
    with {:ok, opts} <- Options.keyword(opts),
         {:ok, transports} <- configured_transports(agent),
         {:ok, transports} <- normalize_transports(transports),
         :ok <- register_transports(transports) do
      Executor.deliver(
        agent,
        effect,
        context.state,
        Keyword.delete(opts, :transports)
      )
    end
  end

  def execute(%Spectre.Effect{} = effect, _context, _opts),
    do:
      {:error,
       Error.not_sent(:validation, {:invalid_pulse_execution_context, effect.id},
         message_id: effect.id
       )}

  @spec configured_transports(module()) :: {:ok, term()} | {:error, Error.t()}
  defp configured_transports(agent) do
    case Spectre.EffectConfig.executor(agent, :pulse) do
      {:ok, %Spectre.Effect.Executor.Mount{opts: opts}} ->
        case Options.keyword(opts) do
          {:ok, opts} -> {:ok, Keyword.get(opts, :transports, [])}
          {:error, reason} -> {:error, Error.not_sent(:validation, reason)}
        end

      {:error, reason} ->
        {:error, Error.not_sent(:validation, reason)}
    end
  rescue
    exception ->
      {:error,
       Error.not_sent(:validation, {:pulse_executor_config_exception, exception},
         cause: exception
       )}
  catch
    kind, reason ->
      {:error, Error.not_sent(:validation, {:pulse_executor_config_exit, kind, reason})}
  end

  @spec normalize_transports(term()) :: {:ok, [{atom(), module()}]} | {:error, Error.t()}
  defp normalize_transports(transports) when is_list(transports) do
    transports
    |> Enum.reduce_while({:ok, {MapSet.new(), []}}, fn
      %{id: id, module: module}, {:ok, {ids, normalized}} ->
        with :ok <- validate_transport_id(id),
             :ok <- validate_transport_module(module),
             false <- MapSet.member?(ids, id) do
          {:cont, {:ok, {MapSet.put(ids, id), [{id, module} | normalized]}}}
        else
          true ->
            {:halt, {:error, Error.not_sent(:validation, {:duplicate_pulse_transport, id})}}

          {:error, %Error{} = error} ->
            {:halt, {:error, error}}
        end

      invalid, _acc ->
        {:halt, {:error, Error.not_sent(:validation, {:invalid_pulse_transport, invalid})}}
    end)
    |> case do
      {:ok, {_ids, normalized}} -> {:ok, Enum.reverse(normalized)}
      {:error, %Error{} = error} -> {:error, error}
    end
  end

  defp normalize_transports(invalid),
    do: {:error, Error.not_sent(:validation, {:invalid_pulse_transports, invalid})}

  @spec validate_transport_id(term()) :: :ok | {:error, Error.t()}
  defp validate_transport_id(id) when is_atom(id) and not is_nil(id), do: :ok

  defp validate_transport_id(id),
    do: {:error, Error.not_sent(:validation, {:invalid_pulse_transport_id, id})}

  @spec validate_transport_module(term()) :: :ok | {:error, Error.t()}
  defp validate_transport_module(module) when is_atom(module) and not is_nil(module) do
    if Code.ensure_loaded?(module) and function_exported?(module, :deliver, 3),
      do: :ok,
      else: {:error, Error.not_sent(:validation, {:invalid_transport, module})}
  end

  defp validate_transport_module(module),
    do: {:error, Error.not_sent(:validation, {:invalid_transport, module})}

  @spec register_transports([{atom(), module()}]) :: :ok | {:error, Error.t() | term()}
  defp register_transports(transports) do
    registrations = Enum.map(transports, fn {id, module} -> {id, module, []} end)
    Fabric.register_transports(registrations)
  end
end

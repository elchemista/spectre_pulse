defmodule Spectre.Pulse.Stack do
  @moduledoc """
  Stack adapter for the Pulse protocol boundary.

  It compiles only logical transport and directory selections. Pulse's current
  application runtime remains host-owned and globally supervised, so this
  adapter deliberately declares no per-Stack runtime resources.
  """

  alias Spectre.Stack.DSL

  @version "0.2.0"

  @doc false
  @spec manifest() :: keyword()
  def manifest do
    [
      id: :pulse,
      version: @version,
      contract: 1,
      spectre: "~> 0.2.0",
      provides: [{:contract, {:pulse, 1}}, {:service, :pulse}],
      requires: [],
      conflicts: [],
      operations: [],
      actions: [],
      resources: [],
      agent_extensions: [Spectre.Pulse.Extension],
      dsl: __MODULE__,
      metadata: %{role: :agent_protocol}
    ]
  end

  @doc false
  @spec compile(keyword(), Macro.t() | nil, Macro.Env.t()) ::
          {:ok, map()} | {:error, term()}
  def compile(opts, block, caller) do
    with :ok <- validate_options(opts),
         declarations <- DSL.compile!(block, caller, transport: 2, directory: 1),
         {:ok, transports} <- compile_transports(declarations),
         {:ok, directory} <- compile_directory(declarations) do
      {:ok, %{transports: transports, directory: directory}}
    end
  end

  @spec validate_options(keyword()) :: :ok | {:error, term()}
  defp validate_options([]), do: :ok
  defp validate_options(opts), do: {:error, {:unknown_pulse_stack_options, Keyword.keys(opts)}}

  @spec compile_transports([{atom(), [term()]}]) ::
          {:ok, [%{id: atom(), module: module()}]} | {:error, term()}
  defp compile_transports(declarations) do
    declarations
    |> Enum.flat_map(fn
      {:transport, [id, module]} -> [{id, module}]
      _declaration -> []
    end)
    |> Enum.reduce_while({:ok, {MapSet.new(), []}}, &compile_transport/2)
    |> case do
      {:ok, {_ids, transports}} -> {:ok, Enum.reverse(transports)}
      {:error, _reason} = error -> error
    end
  end

  @spec compile_transport(
          {term(), term()},
          {:ok, {MapSet.t(atom()), [map()]}}
        ) :: {:cont, {:ok, {MapSet.t(atom()), [map()]}}} | {:halt, {:error, term()}}
  defp compile_transport({id, module}, {:ok, {ids, transports}})
       when is_atom(id) and not is_nil(id) and is_atom(module) and not is_nil(module) do
    if MapSet.member?(ids, id) do
      {:halt, {:error, {:duplicate_pulse_transport, id}}}
    else
      transport = %{id: id, module: module}
      {:cont, {:ok, {MapSet.put(ids, id), [transport | transports]}}}
    end
  end

  defp compile_transport({id, module}, _state),
    do: {:halt, {:error, {:invalid_pulse_transport, id, module}}}

  @spec compile_directory([{atom(), [term()]}]) :: {:ok, module() | nil} | {:error, term()}
  defp compile_directory(declarations) do
    directories =
      Enum.flat_map(declarations, fn
        {:directory, [module]} -> [module]
        _declaration -> []
      end)

    case directories do
      [] ->
        {:ok, nil}

      [module] when is_atom(module) and not is_nil(module) ->
        {:ok, module}

      [module] ->
        {:error, {:invalid_pulse_directory, module}}

      _modules ->
        {:error, :duplicate_pulse_directory}
    end
  end
end

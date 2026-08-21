defmodule Spectre.Pulse.Operations.View do
  @moduledoc false

  alias Spectre.Operation.View, as: OperationView
  alias Spectre.Pulse.Operations.Request
  alias Spectre.Pulse.WireValue

  @fields [
    :id,
    :kind,
    :definition,
    :definition_version,
    :status,
    :control_state,
    :pause_requested,
    :terminal_category,
    :phase,
    :operation,
    :next_operation,
    :cycles,
    :attempts,
    :retries,
    :observations,
    :progress,
    :checkpoint,
    :partial_results,
    :artifacts,
    :blocker,
    :updated_at,
    :context_revision,
    :revision,
    :last_update,
    :pending_command,
    :invalidations,
    :cognitive,
    :last_crash,
    :reconciliation,
    :next_trigger,
    :wait_ref,
    :trigger_generation,
    :budget,
    :metadata
  ]

  @doc false
  @spec to_wire(OperationView.t(), Request.t()) :: {map(), [String.t()]}
  def to_wire(%OperationView{} = view, %Request{} = request) do
    limits = %{
      max_binary_bytes: request.max_binary_bytes,
      max_collection_entries: request.max_collection_entries,
      max_depth: request.max_depth
    }

    view
    |> Map.from_struct()
    |> Map.take(@fields)
    |> Enum.reduce({%{}, []}, fn {field, value}, {wire, truncated_fields} ->
      key = Atom.to_string(field)
      {encoded, truncated?} = WireValue.encode(value, limits)
      truncated_fields = if truncated?, do: [key | truncated_fields], else: truncated_fields
      {Map.put(wire, key, encoded), truncated_fields}
    end)
    |> then(fn {wire, truncated_fields} -> {wire, Enum.reverse(truncated_fields)} end)
  end
end

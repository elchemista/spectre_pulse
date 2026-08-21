defmodule Spectre.Pulse.Operations do
  @moduledoc """
  Read-only Work and Vigil observability for a live Agent Instance.

  Pulse reads only `Spectre.Operation.View` projections. It does not inspect
  canonical Instance state, own operational history, or bypass a Definition's
  publication policy. Remote callers require the `agent.operations.read`
  scope and access to the Agent on their authenticated connection.
  """

  alias Spectre.Operation.View, as: OperationView
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.InstanceTarget
  alias Spectre.Pulse.Operations.Request
  alias Spectre.Pulse.Operations.View

  @scope "agent.operations.read"

  @type target :: module() | Spectre.AgentRef.t() | String.t()

  @doc "Returns the authorization scope required to list Work and Vigil views."
  @spec scope() :: String.t()
  def scope, do: @scope

  @doc """
  Returns bounded, transport-safe Work and Vigil views for one Agent Instance.

  Terminal loops are excluded by default. Use `include_terminal: true` to
  include them, or `kinds: [:work]` / `kinds: [:vigil]` to narrow the result.
  """
  @spec list(target(), term(), keyword()) :: {:ok, map()} | {:error, Error.t()}
  def list(target, subject, opts \\ []) do
    with {:ok, request} <- Request.new(opts),
         {:ok, resolved} <-
           InstanceTarget.resolve(
             target,
             subject,
             request.connection,
             request.instance_registry,
             @scope,
             :operations
           ),
         {:ok, views} <- fetch_views(resolved.pid) do
      {:ok, snapshot(resolved, views, request)}
    end
  rescue
    exception ->
      {:error,
       Error.not_sent(:inbound, :operations_list_failed,
         cause: exception,
         details: %{operation: :operations_list}
       )}
  catch
    :exit, _reason -> {:error, Error.not_sent(:routing, :agent_instance_unavailable)}
  end

  @spec fetch_views(pid()) :: {:ok, [OperationView.t()]} | {:error, Error.t()}
  defp fetch_views(instance) do
    case Spectre.loops(instance) do
      {:ok, views} when is_list(views) -> {:ok, views}
      {:error, reason} -> {:error, Error.not_sent(:inbound, {:operations_unavailable, reason})}
    end
  end

  @spec snapshot(InstanceTarget.t(), [OperationView.t()], Request.t()) :: map()
  defp snapshot(target, views, request) do
    selected = Enum.filter(views, &selected?(&1, request))
    limited = Enum.take(selected, request.max_collection_entries)

    {operations, truncated_fields} =
      Enum.map_reduce(limited, %{}, fn view, truncated ->
        {wire, fields} = View.to_wire(view, request)
        {wire, if(fields == [], do: truncated, else: Map.put(truncated, view.id, fields))}
      end)

    %{
      "schema_version" => 1,
      "capability" => "agent.operations.list",
      "scope" => @scope,
      "agent_address" => target.address,
      "instance_ref" => target.ref.key,
      "sampled_at_unix_ms" => System.system_time(:millisecond),
      "operations" => operations,
      "counts" => counts(selected),
      "truncated" => length(selected) > length(limited),
      "truncated_fields" => truncated_fields,
      "filters" => %{
        "kinds" => Enum.map(request.kinds, &Atom.to_string/1),
        "include_terminal" => request.include_terminal
      },
      "limits" => %{
        "max_binary_bytes" => request.max_binary_bytes,
        "max_collection_entries" => request.max_collection_entries,
        "max_depth" => request.max_depth
      }
    }
  end

  @spec selected?(OperationView.t(), Request.t()) :: boolean()
  defp selected?(view, request) do
    view.kind in request.kinds and (request.include_terminal or view.status != :terminal)
  end

  @spec counts([OperationView.t()]) :: map()
  defp counts(views) do
    %{
      "total" => length(views),
      "work" => Enum.count(views, &(&1.kind == :work)),
      "vigil" => Enum.count(views, &(&1.kind == :vigil))
    }
  end
end

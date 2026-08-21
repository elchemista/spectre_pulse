defmodule Spectre.Pulse.RuntimeInfo do
  @moduledoc """
  Bounded, transport-safe OTP process information for a live Agent Instance.

  Pulse resolves the Instance and samples the BEAM process on demand. It does
  not retain metrics, read GenServer state, or expose mailbox contents and the
  process dictionary. Remote callers must pass the identifier of the live
  Pulse connection that initiated the request. Pulse then enforces the
  `agent.runtime.read` scope and that connection's exposed-Agent boundary.
  """

  alias Spectre.Pulse.Error
  alias Spectre.Pulse.InstanceTarget
  alias Spectre.Pulse.RuntimeInfo.Request
  alias Spectre.Pulse.RuntimeInfo.Snapshot

  @scope "agent.runtime.read"

  @type target :: module() | Spectre.AgentRef.t() | String.t()

  @doc "Returns the authorization scope required by remote runtime inspection."
  @spec scope() :: String.t()
  def scope, do: @scope

  @doc """
  Resolves an Agent Instance and returns a current OTP process snapshot.

  Pass `connection: connection_id` only from trusted adapter state for a
  remotely initiated call. Other options select safe fields and lower the
  hard serialization limits; mailbox contents, process dictionaries, and
  GenServer state are never available.
  """
  @spec fetch(target(), term(), keyword()) :: {:ok, map()} | {:error, Error.t()}
  def fetch(target, subject, opts \\ []) do
    with {:ok, request} <- Request.new(opts),
         {:ok, resolved} <-
           InstanceTarget.resolve(
             target,
             subject,
             request.connection,
             request.instance_registry,
             @scope,
             :runtime_info
           ),
         {:ok, snapshot} <- Snapshot.capture(resolved.pid, request) do
      {:ok, response(resolved, snapshot)}
    end
  rescue
    exception ->
      {:error,
       Error.not_sent(:inbound, :runtime_info_failed,
         cause: exception,
         details: %{operation: :runtime_info}
       )}
  end

  @doc "Returns a bounded process snapshot for trusted host-side tooling."
  @spec process_info(pid(), [atom() | String.t()]) :: {:ok, map()} | {:error, Error.t()}
  def process_info(pid, fields \\ Snapshot.default_fields()) do
    with {:ok, request} <- Request.new(fields: fields),
         {:ok, snapshot} <- Snapshot.capture(pid, request) do
      {:ok, snapshot.process}
    end
  end

  @spec response(InstanceTarget.t(), Snapshot.t()) :: map()
  defp response(target, snapshot) do
    %{
      "schema_version" => 1,
      "capability" => "agent.runtime.info",
      "scope" => @scope,
      "agent_address" => target.address,
      "instance_ref" => target.ref.key,
      "node" => Atom.to_string(node(target.pid)),
      "pid" => inspect(target.pid),
      "alive" => true,
      "sampled_at_unix_ms" => snapshot.sampled_at_unix_ms,
      "process" => snapshot.process,
      "truncated_fields" => snapshot.truncated_fields,
      "limits" => %{
        "max_collection_entries" => snapshot.max_collection_entries,
        "max_depth" => snapshot.max_depth
      }
    }
  end
end

defmodule Spectre.Pulse.Monitoring.Subscription do
  @moduledoc false

  alias Spectre.Pulse.Address
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Operations.Request, as: OperationsRequest
  alias Spectre.Pulse.RuntimeInfo.Request

  @default_interval_ms 1_000
  @minimum_interval_ms 250
  @maximum_interval_ms 60_000
  @minimum_duration_ms 250
  @maximum_duration_ms 3_600_000

  @enforce_keys [
    :id,
    :kind,
    :connection_id,
    :agent_address,
    :subject,
    :interval_ms,
    :sample_options,
    :created_at_unix_ms
  ]
  defstruct [
    :id,
    :kind,
    :connection_id,
    :agent_address,
    :subject,
    :request_id,
    :interval_ms,
    :duration_ms,
    :sample_options,
    :created_at_unix_ms
  ]

  @type kind :: :runtime | :operations
  @type t :: %__MODULE__{
          id: String.t(),
          kind: kind(),
          connection_id: term(),
          agent_address: String.t(),
          subject: Spectre.Subject.t(),
          request_id: String.t() | nil,
          interval_ms: pos_integer(),
          duration_ms: pos_integer() | nil,
          sample_options: keyword(),
          created_at_unix_ms: non_neg_integer()
        }

  @doc false
  @spec new(term(), term(), keyword()) :: {:ok, t()} | {:error, Error.t()}
  def new(connection_id, attrs, trusted_options),
    do: new(:runtime, connection_id, attrs, trusted_options)

  @doc false
  @spec new(kind(), term(), term(), keyword()) :: {:ok, t()} | {:error, Error.t()}
  def new(kind, connection_id, attrs, trusted_options)
      when kind in [:runtime, :operations] and is_map(attrs) and is_list(trusted_options) do
    with {:ok, agent_address} <- agent_address(attr(attrs, "agent_address"), kind),
         {:ok, subject} <- subject(attrs, kind),
         {:ok, request_id} <- optional_identifier(attr(attrs, "request_id"), kind),
         {:ok, interval_ms} <- interval(attr(attrs, "interval_ms", @default_interval_ms), kind),
         {:ok, duration_ms} <- duration(attr(attrs, "duration_ms"), kind),
         {:ok, sample_options} <- sample_options(kind, attrs, trusted_options) do
      {:ok,
       %__MODULE__{
         id: Spectre.Identity.uuid7(),
         kind: kind,
         connection_id: connection_id,
         agent_address: agent_address,
         subject: subject,
         request_id: request_id,
         interval_ms: interval_ms,
         duration_ms: duration_ms,
         sample_options: sample_options,
         created_at_unix_ms: System.system_time(:millisecond)
       }}
    end
  rescue
    _exception -> {:error, Error.not_sent(:validation, reason(kind, :invalid_request))}
  end

  def new(kind, _connection_id, _attrs, _trusted_options) when kind in [:runtime, :operations],
    do: {:error, Error.not_sent(:validation, reason(kind, :invalid_request))}

  def new(_kind, _connection_id, _attrs, _trusted_options),
    do: {:error, Error.not_sent(:validation, :invalid_monitor_kind)}

  @doc false
  @spec to_public_map(t()) :: map()
  def to_public_map(%__MODULE__{} = subscription) do
    %{
      "subscription_id" => subscription.id,
      "monitor" => Atom.to_string(subscription.kind),
      "request_id" => subscription.request_id,
      "connection_id" => subscription.connection_id,
      "agent_address" => subscription.agent_address,
      "subject_id" => subscription.subject.id,
      "interval_ms" => subscription.interval_ms,
      "duration_ms" => subscription.duration_ms,
      "created_at_unix_ms" => subscription.created_at_unix_ms
    }
  end

  @spec agent_address(term(), kind()) :: {:ok, String.t()} | {:error, Error.t()}
  defp agent_address(value, _kind) when is_binary(value), do: Address.normalize(value)

  defp agent_address(_value, kind),
    do: {:error, Error.not_sent(:validation, reason(kind, :agent_address_required))}

  @spec subject(map(), kind()) :: {:ok, Spectre.Subject.t()} | {:error, Error.t()}
  defp subject(attrs, kind) do
    case fetch_attr(attrs, "subject") do
      {:ok, nil} -> {:error, Error.not_sent(:validation, reason(kind, :subject_required))}
      {:ok, value} -> {:ok, Spectre.Subject.new(value)}
      :error -> {:error, Error.not_sent(:validation, reason(kind, :subject_required))}
    end
  end

  @spec optional_identifier(term(), kind()) ::
          {:ok, String.t() | nil} | {:error, Error.t()}
  defp optional_identifier(nil, _kind), do: {:ok, nil}

  defp optional_identifier(value, kind) when is_binary(value) and byte_size(value) <= 128 do
    if String.valid?(value) and String.trim(value) != "",
      do: {:ok, value},
      else: {:error, Error.not_sent(:validation, reason(kind, :invalid_request_id))}
  end

  defp optional_identifier(_value, kind),
    do: {:error, Error.not_sent(:validation, reason(kind, :invalid_request_id))}

  @spec interval(term(), kind()) :: {:ok, pos_integer()} | {:error, Error.t()}
  defp interval(value, _kind)
       when is_integer(value) and value >= @minimum_interval_ms and value <= @maximum_interval_ms,
       do: {:ok, value}

  defp interval(_value, kind) do
    {:error,
     Error.not_sent(:validation, reason(kind, :invalid_interval),
       details: %{minimum_ms: @minimum_interval_ms, maximum_ms: @maximum_interval_ms}
     )}
  end

  @spec duration(term(), kind()) :: {:ok, pos_integer() | nil} | {:error, Error.t()}
  defp duration(nil, _kind), do: {:ok, nil}

  defp duration(value, _kind)
       when is_integer(value) and value >= @minimum_duration_ms and value <= @maximum_duration_ms,
       do: {:ok, value}

  defp duration(_value, kind) do
    {:error,
     Error.not_sent(:validation, reason(kind, :invalid_duration),
       details: %{minimum_ms: @minimum_duration_ms, maximum_ms: @maximum_duration_ms}
     )}
  end

  @spec sample_options(kind(), map(), keyword()) ::
          {:ok, keyword()} | {:error, Error.t()}
  defp sample_options(:runtime, attrs, trusted_options) do
    remote_options =
      remote_options(attrs, ["fields", "max_collection_entries", "max_depth"])

    with {:ok, request} <- Request.new(Keyword.merge(remote_options, trusted_options)) do
      {:ok, runtime_request_options(request)}
    end
  end

  defp sample_options(:operations, attrs, trusted_options) do
    remote_options =
      remote_options(attrs, [
        "include_terminal",
        "kinds",
        "max_binary_bytes",
        "max_collection_entries",
        "max_depth"
      ])

    with {:ok, request} <- OperationsRequest.new(Keyword.merge(remote_options, trusted_options)) do
      {:ok, OperationsRequest.to_options(request)}
    end
  end

  @spec remote_options(map(), [String.t()]) :: keyword()
  defp remote_options(attrs, fields) do
    fields
    |> Enum.map(fn field -> {String.to_existing_atom(field), attr(attrs, field)} end)
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
  end

  @spec runtime_request_options(Request.t()) :: keyword()
  defp runtime_request_options(%Request{} = request) do
    [
      connection: request.connection,
      fields: request.fields,
      instance_registry: request.instance_registry,
      max_collection_entries: request.max_collection_entries,
      max_depth: request.max_depth
    ]
  end

  @spec reason(kind(), atom()) :: atom()
  defp reason(:runtime, :invalid_request), do: :invalid_runtime_monitor_request
  defp reason(:runtime, :agent_address_required), do: :runtime_monitor_agent_address_required
  defp reason(:runtime, :subject_required), do: :runtime_monitor_subject_required
  defp reason(:runtime, :invalid_request_id), do: :invalid_runtime_monitor_request_id
  defp reason(:runtime, :invalid_interval), do: :invalid_runtime_monitor_interval
  defp reason(:runtime, :invalid_duration), do: :invalid_runtime_monitor_duration
  defp reason(:operations, :invalid_request), do: :invalid_operations_monitor_request

  defp reason(:operations, :agent_address_required),
    do: :operations_monitor_agent_address_required

  defp reason(:operations, :subject_required), do: :operations_monitor_subject_required
  defp reason(:operations, :invalid_request_id), do: :invalid_operations_monitor_request_id
  defp reason(:operations, :invalid_interval), do: :invalid_operations_monitor_interval
  defp reason(:operations, :invalid_duration), do: :invalid_operations_monitor_duration

  @spec attr(map(), String.t(), term()) :: term()
  defp attr(attrs, key, default \\ nil),
    do: Map.get(attrs, key, Map.get(attrs, String.to_existing_atom(key), default))

  @spec fetch_attr(map(), String.t()) :: {:ok, term()} | :error
  defp fetch_attr(attrs, key) do
    case Map.fetch(attrs, key) do
      {:ok, value} -> {:ok, value}
      :error -> Map.fetch(attrs, String.to_existing_atom(key))
    end
  end
end

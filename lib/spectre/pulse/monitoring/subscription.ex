defmodule Spectre.Pulse.Monitoring.Subscription do
  @moduledoc false

  alias Spectre.Pulse.Address
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.RuntimeInfo.Request

  @default_interval_ms 1_000
  @minimum_interval_ms 250
  @maximum_interval_ms 60_000
  @minimum_duration_ms 250
  @maximum_duration_ms 3_600_000

  @enforce_keys [
    :id,
    :connection_id,
    :agent_address,
    :subject,
    :interval_ms,
    :runtime_options,
    :created_at_unix_ms
  ]
  defstruct [
    :id,
    :connection_id,
    :agent_address,
    :subject,
    :request_id,
    :interval_ms,
    :duration_ms,
    :runtime_options,
    :created_at_unix_ms
  ]

  @type t :: %__MODULE__{
          id: String.t(),
          connection_id: term(),
          agent_address: String.t(),
          subject: Spectre.Subject.t(),
          request_id: String.t() | nil,
          interval_ms: pos_integer(),
          duration_ms: pos_integer() | nil,
          runtime_options: keyword(),
          created_at_unix_ms: non_neg_integer()
        }

  @doc false
  @spec new(term(), term(), keyword()) :: {:ok, t()} | {:error, Error.t()}
  def new(connection_id, attrs, trusted_options)
      when is_map(attrs) and is_list(trusted_options) do
    with {:ok, agent_address} <- agent_address(attr(attrs, "agent_address")),
         {:ok, subject} <- subject(attrs),
         {:ok, request_id} <- optional_identifier(attr(attrs, "request_id"), :request_id),
         {:ok, interval_ms} <- interval(attr(attrs, "interval_ms", @default_interval_ms)),
         {:ok, duration_ms} <- duration(attr(attrs, "duration_ms")),
         {:ok, request} <- runtime_request(attrs, trusted_options) do
      {:ok,
       %__MODULE__{
         id: Spectre.Identity.uuid7(),
         connection_id: connection_id,
         agent_address: agent_address,
         subject: subject,
         request_id: request_id,
         interval_ms: interval_ms,
         duration_ms: duration_ms,
         runtime_options: request_options(request),
         created_at_unix_ms: System.system_time(:millisecond)
       }}
    end
  rescue
    _exception -> {:error, Error.not_sent(:validation, :invalid_runtime_monitor_request)}
  end

  def new(_connection_id, _attrs, _trusted_options),
    do: {:error, Error.not_sent(:validation, :invalid_runtime_monitor_request)}

  @doc false
  @spec to_public_map(t()) :: map()
  def to_public_map(%__MODULE__{} = subscription) do
    %{
      "subscription_id" => subscription.id,
      "request_id" => subscription.request_id,
      "connection_id" => subscription.connection_id,
      "agent_address" => subscription.agent_address,
      "subject_id" => subscription.subject.id,
      "interval_ms" => subscription.interval_ms,
      "duration_ms" => subscription.duration_ms,
      "created_at_unix_ms" => subscription.created_at_unix_ms
    }
  end

  @spec agent_address(term()) :: {:ok, String.t()} | {:error, Error.t()}
  defp agent_address(value) when is_binary(value), do: Address.normalize(value)

  defp agent_address(_value),
    do: {:error, Error.not_sent(:validation, :runtime_monitor_agent_address_required)}

  @spec subject(map()) :: {:ok, Spectre.Subject.t()} | {:error, Error.t()}
  defp subject(attrs) do
    case fetch_attr(attrs, "subject") do
      {:ok, nil} -> {:error, Error.not_sent(:validation, :runtime_monitor_subject_required)}
      {:ok, value} -> {:ok, Spectre.Subject.new(value)}
      :error -> {:error, Error.not_sent(:validation, :runtime_monitor_subject_required)}
    end
  end

  @spec optional_identifier(term(), atom()) ::
          {:ok, String.t() | nil} | {:error, Error.t()}
  defp optional_identifier(nil, _field), do: {:ok, nil}

  defp optional_identifier(value, _field) when is_binary(value) and byte_size(value) <= 128 do
    if String.valid?(value) and String.trim(value) != "",
      do: {:ok, value},
      else: {:error, Error.not_sent(:validation, :invalid_runtime_monitor_request_id)}
  end

  defp optional_identifier(_value, _field),
    do: {:error, Error.not_sent(:validation, :invalid_runtime_monitor_request_id)}

  @spec interval(term()) :: {:ok, pos_integer()} | {:error, Error.t()}
  defp interval(value)
       when is_integer(value) and value >= @minimum_interval_ms and value <= @maximum_interval_ms,
       do: {:ok, value}

  defp interval(_value) do
    {:error,
     Error.not_sent(:validation, :invalid_runtime_monitor_interval,
       details: %{minimum_ms: @minimum_interval_ms, maximum_ms: @maximum_interval_ms}
     )}
  end

  @spec duration(term()) :: {:ok, pos_integer() | nil} | {:error, Error.t()}
  defp duration(nil), do: {:ok, nil}

  defp duration(value)
       when is_integer(value) and value >= @minimum_duration_ms and value <= @maximum_duration_ms,
       do: {:ok, value}

  defp duration(_value) do
    {:error,
     Error.not_sent(:validation, :invalid_runtime_monitor_duration,
       details: %{minimum_ms: @minimum_duration_ms, maximum_ms: @maximum_duration_ms}
     )}
  end

  @spec runtime_request(map(), keyword()) :: {:ok, Request.t()} | {:error, Error.t()}
  defp runtime_request(attrs, trusted_options) do
    remote_options =
      [
        fields: attr(attrs, "fields"),
        max_collection_entries: attr(attrs, "max_collection_entries"),
        max_depth: attr(attrs, "max_depth")
      ]
      |> Enum.reject(fn {_key, value} -> is_nil(value) end)

    Request.new(Keyword.merge(remote_options, trusted_options))
  end

  @spec request_options(Request.t()) :: keyword()
  defp request_options(%Request{} = request) do
    [
      connection: request.connection,
      fields: request.fields,
      instance_registry: request.instance_registry,
      max_collection_entries: request.max_collection_entries,
      max_depth: request.max_depth
    ]
  end

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

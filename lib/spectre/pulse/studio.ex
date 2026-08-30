defmodule Spectre.Pulse.Studio do
  @moduledoc """
  Scoped server-side operations used by Spectre Studio.

  These operations travel as ordinary Pulse envelopes, but terminate at the
  authenticated connection boundary instead of entering an Agent turn. This
  keeps inspection and governance traffic out of prompts, conversation state,
  and application message handlers while preserving the exposed-Agent and
  connection-scope boundaries.

  Hosts still choose every granted scope explicitly. Advertising the
  `spectre.studio/1` profile does not grant access by itself.
  """

  alias Spectre.Morph
  alias Spectre.Morph.Change
  alias Spectre.Pulse.AgentDescriptor
  alias Spectre.Pulse.Codec.JSON
  alias Spectre.Pulse.Connection
  alias Spectre.Pulse.ConnectionRegistry
  alias Spectre.Pulse.Envelope
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.InstanceTarget
  alias Spectre.Pulse.Receipt
  alias Spectre.Router.SemanticCache
  alias Spectre.Router.SemanticCache.Learned.Rows
  alias Spectre.Runtime.Persistence

  @cache_examples "studio.semantic_cache.examples"
  @cache_update "studio.semantic_cache.update"
  @cache_verify "studio.semantic_cache.verify"
  @journal_turns "studio.journal.turns"
  @skills_list "studio.skills.list"
  @skill_mount "studio.skill.mount"
  @morph_propose "studio.morph.propose_skill"

  @operations %{
    @cache_examples => "agent.semantic_cache.read",
    @cache_update => "agent.semantic_cache.write",
    @cache_verify => "agent.semantic_cache.promote",
    @journal_turns => "ledger.read",
    @skills_list => "spectre.skill.read",
    @morph_propose => "spectre.morph.propose"
  }

  @reserved_operations [@skill_mount]

  @default_cache_examples 5_000
  @max_cache_examples 5_000
  @max_turns 100
  @max_text_graphemes 8_000

  @type handled ::
          :not_studio
          | {:ok, Receipt.t(), binary()}
          | {:error, Error.t()}

  @doc "Returns every application scope understood by the Studio bridge."
  @spec scopes() :: [String.t()]
  def scopes, do: @operations |> Map.values() |> Enum.uniq() |> Enum.sort()

  @doc false
  @spec handle_frame(binary(), Connection.t()) :: handled()
  def handle_frame(frame, %Connection{} = connection) when is_binary(frame) do
    case studio_payload_type(frame) do
      type when is_map_key(@operations, type) -> handle_operation(frame, connection, type)
      type when type in @reserved_operations -> handle_reserved(frame, type)
      _other -> :not_studio
    end
  end

  @spec handle_reserved(binary(), String.t()) :: handled()
  defp handle_reserved(frame, type) do
    with {:ok, envelope} <- JSON.decode(frame, []),
         {:ok, response} <-
           response(envelope, type, {:error, :skill_mount_requires_governed_morph}),
         {:ok, encoded} <- JSON.encode(response, []) do
      {:ok, Receipt.accepted(envelope.id, via: :websocket, route_id: :spectre_studio), encoded}
    end
  end

  @spec handle_operation(binary(), Connection.t(), String.t()) :: handled()
  defp handle_operation(frame, connection, type) do
    with {:ok, envelope} <- JSON.decode(frame, []),
         result <- safe_perform(connection, envelope, type),
         {:ok, response} <- response(envelope, type, result),
         {:ok, encoded} <- JSON.encode(response, []) do
      {:ok, Receipt.accepted(envelope.id, via: :websocket, route_id: :spectre_studio), encoded}
    end
  end

  @spec safe_perform(Connection.t(), Envelope.t(), String.t()) ::
          {:ok, map()} | {:error, term()}
  defp safe_perform(connection, envelope, type) do
    perform(connection, envelope, type)
  rescue
    _exception -> {:error, :studio_operation_failed}
  catch
    _kind, _reason -> {:error, :studio_operation_failed}
  end

  @spec perform(Connection.t(), Envelope.t(), String.t()) ::
          {:ok, map()} | {:error, term()}
  defp perform(connection, envelope, type) do
    with {:ok, agent} <- authorize(connection, envelope, Map.fetch!(@operations, type)) do
      execute(type, agent, envelope.payload.data, connection)
    end
  end

  @spec studio_payload_type(binary()) :: String.t() | nil
  defp studio_payload_type(frame) do
    case Jason.decode(frame) do
      {:ok, %{"payload" => %{"type" => "studio." <> _rest = type}}} -> type
      _other -> nil
    end
  end

  @spec authorize(Connection.t(), Envelope.t(), String.t()) ::
          {:ok, module()} | {:error, Error.t()}
  defp authorize(connection, envelope, scope) do
    with :ok <- require_principal(connection, envelope),
         :ok <- require_scope(connection, scope),
         :ok <- require_exposed(connection, envelope.to),
         {:ok, %AgentDescriptor{module: agent}} when is_atom(agent) <-
           ConnectionRegistry.local_agent(envelope.to) do
      {:ok, agent}
    else
      :error -> {:error, Error.not_sent(:routing, :unknown_local_agent)}
      {:error, %Error{} = error} -> {:error, error}
    end
  end

  @spec require_principal(Connection.t(), Envelope.t()) :: :ok | {:error, Error.t()}
  defp require_principal(connection, envelope) do
    if envelope.from == connection.principal.identity,
      do: :ok,
      else: {:error, Error.not_sent(:authorization, :connection_principal_mismatch)}
  end

  @spec require_scope(Connection.t(), String.t()) :: :ok | {:error, Error.t()}
  defp require_scope(connection, scope) do
    if scope in connection.granted_scopes,
      do: :ok,
      else: {:error, Error.not_sent(:authorization, {:connection_scope_required, scope})}
  end

  @spec require_exposed(Connection.t(), String.t()) :: :ok | {:error, Error.t()}
  defp require_exposed(connection, address) do
    if address in connection.local_agent_addresses,
      do: :ok,
      else: {:error, Error.not_sent(:authorization, :agent_not_exposed_on_connection)}
  end

  @spec execute(String.t(), module(), term(), Connection.t()) ::
          {:ok, map()} | {:error, term()}
  defp execute(@cache_examples, agent, data, _connection) when is_map(data) do
    with {:ok, source} <- cache_source(Map.get(data, "source", "online_learned")),
         {:ok, rows} <- SemanticCache.examples(agent, source: source) do
      rows = Enum.sort_by(rows, &cache_sort_key/1)
      offset = bounded_offset(Map.get(data, "offset"))
      limit = bounded_limit(Map.get(data, "limit"), @max_cache_examples)
      examples = rows |> Enum.drop(offset) |> Enum.take(limit)
      next_offset = if offset + length(examples) < length(rows), do: offset + length(examples)
      inventory = cache_inventory(rows)

      {:ok,
       %{
         "examples" => Enum.map(examples, &cache_row/1),
         "labels" => cache_labels(agent),
         "count" => length(examples),
         "total" => length(rows),
         "offset" => offset,
         "limit" => limit,
         "next_offset" => next_offset,
         "truncated?" => not is_nil(next_offset),
         "source" => wire_value(source),
         "source_counts" => inventory.source_counts,
         "searchable_count" => inventory.searchable,
         "exact_only_count" => length(rows) - inventory.searchable,
         "fully_searchable?" => inventory.searchable == length(rows)
       }}
    end
  end

  defp execute(@cache_examples, _agent, _data, _connection),
    do: {:error, :invalid_cache_examples_request}

  defp execute(@cache_update, agent, %{"example_id" => id} = data, _connection)
       when is_binary(id) and id != "" do
    with {:ok, attrs} <- cache_update_attrs(agent, data),
         {:ok, example} <- SemanticCache.update_example(agent, id, attrs) do
      {:ok, %{"example" => cache_row(example)}}
    end
  end

  defp execute(@cache_update, _agent, _data, _connection),
    do: {:error, :invalid_cache_update}

  defp execute(@cache_verify, agent, %{"example_id" => id}, _connection)
       when is_binary(id) and id != "" do
    with {:ok, example} <- SemanticCache.verify(agent, id) do
      {:ok, %{"example" => cache_row(example)}}
    end
  end

  defp execute(@cache_verify, _agent, _data, _connection),
    do: {:error, :invalid_example_id}

  defp execute(@journal_turns, agent, data, _connection) when is_map(data) do
    with {:ok, subject} <- required_text(data, "subject"),
         {:ok, state} <-
           Persistence.load_state(agent, Spectre.Input.new(""), conversation_id: subject) do
      limit = bounded_limit(Map.get(data, "limit"), @max_turns)

      history =
        state.data
        |> Map.get(:chat_history, Map.get(state.data, "chat_history", []))
        |> history()

      turns = history |> Enum.take(-limit) |> Enum.map(&turn/1)

      {:ok,
       %{
         "subject" => subject,
         "revision" => state.revision,
         "turns" => turns,
         "count" => length(turns)
       }}
    end
  end

  defp execute(@journal_turns, _agent, _data, _connection),
    do: {:error, :invalid_journal_request}

  defp execute(@skills_list, agent, _data, _connection) do
    with {:ok, definition} <- Spectre.Definition.fetch(agent) do
      {:ok,
       %{
         "skills" => Enum.map(definition.skills, &skill/1),
         "count" => length(definition.skills)
       }}
    end
  end

  defp execute(@morph_propose, agent, data, connection) when is_map(data) do
    with {:ok, subject} <- required_text(data, "subject"),
         {:ok, actor} <- required_text(data, "by"),
         {:ok, reason} <- required_text(data, "reason"),
         {:ok, mount_id} <- required_text(data, "mount_id"),
         {:ok, match} <- required_text(data, "match"),
         {:ok, reply} <- required_text(data, "reply"),
         {:ok, target} <-
           InstanceTarget.resolve(
             agent,
             subject,
             connection.id,
             Spectre.Instance.Registry,
             Map.fetch!(@operations, @morph_propose),
             :studio
           ) do
      change =
        target.pid
        |> Morph.change(by: actor, reason: reason)
        |> put_morph_operation(Map.get(data, "action"), mount_id, data, match, reply)
        |> Morph.evaluate()

      morph_result(change)
    end
  end

  defp execute(@morph_propose, _agent, _data, _connection),
    do: {:error, :invalid_morph_request}

  @spec put_morph_operation(Change.t(), term(), String.t(), map(), String.t(), String.t()) ::
          Change.t()
  defp put_morph_operation(change, "replace", mount_id, data, match, reply) do
    Morph.replace_skill(change, mount_id, morph_opts(data, match, reply))
  end

  defp put_morph_operation(change, _action, mount_id, data, match, reply) do
    Morph.mount_skill(change, mount_id, morph_opts(data, match, reply))
  end

  @spec morph_opts(map(), String.t(), String.t()) :: keyword()
  defp morph_opts(data, match, reply) do
    [match: match, reply: reply]
    |> put_optional(:scopes, string_list(Map.get(data, "scopes")))
    |> put_optional(:token_cap, positive_integer(Map.get(data, "token_cap")))
  end

  @spec morph_result(Change.t()) :: {:ok, map()} | {:error, term()}
  defp morph_result(%Change{error: nil} = change) do
    {:ok,
     %{
       "state" => Atom.to_string(change.state),
       "candidate_ref" => wire_value(change.ref),
       "mount_ids" => change.mount_ids,
       "operation_count" => length(change.operations),
       "approved" => change.state == :approved,
       "activated" => false
     }}
  end

  defp morph_result(%Change{error: error}), do: {:error, error}

  @spec response(Envelope.t(), String.t(), {:ok, map()} | {:error, term()}) ::
          {:ok, Envelope.t()} | {:error, Error.t()}
  defp response(envelope, type, {:ok, data}),
    do: Envelope.reply(envelope, type <> ".result", data)

  defp response(envelope, type, {:error, %Error{} = error}),
    do: Envelope.reply(envelope, type <> ".error", error_data(error.kind, error.reason))

  defp response(envelope, type, {:error, reason}),
    do: Envelope.reply(envelope, type <> ".error", error_data(:request, reason))

  @spec error_data(term(), term()) :: map()
  defp error_data(kind, reason) do
    %{
      "kind" => wire_value(kind),
      "code" => reason_code(reason)
    }
  end

  @spec reason_code(term()) :: String.t()
  defp reason_code(reason) when is_atom(reason), do: Atom.to_string(reason)

  defp reason_code(reason) when is_tuple(reason) and tuple_size(reason) > 0,
    do: reason |> elem(0) |> wire_value()

  defp reason_code(_reason), do: "operation_failed"

  @spec cache_row(map()) :: map()
  defp cache_row(row) do
    embedding = Map.get(row, :embedding, Map.get(row, "embedding"))
    searchable? = is_list(embedding) and embedding != []

    %{
      "id" => wire_value(Map.get(row, :id, Map.get(row, "id"))),
      "text" => bounded_text(Map.get(row, :text, Map.get(row, "text"))),
      "label" => wire_value(Map.get(row, :label, Map.get(row, "label"))),
      "source" => wire_value(Map.get(row, :source, Map.get(row, "source"))),
      "confidence" => Map.get(row, :confidence, Map.get(row, "confidence")),
      "verified?" => Map.get(row, :verified?, Map.get(row, "verified?", false)),
      "editable?" => Map.get(row, :editable?, Map.get(row, "editable?", false)),
      "searchable?" => searchable?,
      "embedding_dimensions" => if(searchable?, do: length(embedding), else: nil),
      "inserted_at" => wire_value(Map.get(row, :inserted_at, Map.get(row, "inserted_at"))),
      "updated_at" => wire_value(Map.get(row, :updated_at, Map.get(row, "updated_at")))
    }
  end

  @spec cache_source(term()) :: {:ok, atom()} | {:error, term()}
  defp cache_source("all"), do: {:ok, :all}
  defp cache_source("online_learned"), do: {:ok, :online_learned}
  defp cache_source("offline_dataset"), do: {:ok, :offline_dataset}
  defp cache_source("static_route_example"), do: {:ok, :static_route_example}

  defp cache_source(source)
       when source in [:all, :online_learned, :offline_dataset, :static_route_example],
       do: {:ok, source}

  defp cache_source(_source), do: {:error, :invalid_semantic_cache_source}

  @spec bounded_offset(term()) :: non_neg_integer()
  defp bounded_offset(value) when is_integer(value) and value >= 0, do: value
  defp bounded_offset(_value), do: 0

  @spec cache_sort_key(map()) :: tuple()
  defp cache_sort_key(row) do
    {
      cache_source_rank(Map.get(row, :source, Map.get(row, "source"))),
      wire_value(Map.get(row, :label, Map.get(row, "label"))),
      row |> Map.get(:text, Map.get(row, "text", "")) |> String.downcase(),
      wire_value(Map.get(row, :id, Map.get(row, "id")))
    }
  end

  defp cache_source_rank(:online_learned), do: 0
  defp cache_source_rank("online_learned"), do: 0
  defp cache_source_rank(:offline_dataset), do: 1
  defp cache_source_rank("offline_dataset"), do: 1
  defp cache_source_rank(:static_route_example), do: 2
  defp cache_source_rank("static_route_example"), do: 2
  defp cache_source_rank(_source), do: 3

  @spec cache_inventory([map()]) :: map()
  defp cache_inventory(rows) do
    %{
      searchable: Enum.count(rows, &cache_searchable?/1),
      source_counts:
        Map.new(Enum.frequencies_by(rows, &Map.get(&1, :source, Map.get(&1, "source"))), fn
          {source, count} -> {wire_value(source), count}
        end)
    }
  end

  defp cache_searchable?(row) do
    embedding = Map.get(row, :embedding, Map.get(row, "embedding"))
    is_list(embedding) and embedding != []
  end

  @spec cache_update_attrs(module(), map()) :: {:ok, map()} | {:error, term()}
  defp cache_update_attrs(agent, data) do
    with {:ok, text} <- optional_cache_text(data),
         {:ok, label} <- optional_cache_label(agent, data) do
      attrs = %{} |> put_update_attr(:text, text) |> put_update_attr(:label, label)

      if map_size(attrs) > 0,
        do: {:ok, attrs},
        else: {:error, :empty_cache_update}
    end
  end

  @spec optional_cache_text(map()) :: {:ok, String.t() | nil} | {:error, term()}
  defp optional_cache_text(data) do
    if Map.has_key?(data, "text") do
      validate_cache_text(Map.get(data, "text"))
    else
      {:ok, nil}
    end
  end

  defp validate_cache_text(value) when is_binary(value) do
    value = String.trim(value)

    cond do
      value == "" -> {:error, :blank_cache_text}
      String.length(value) > @max_text_graphemes -> {:error, :cache_text_too_long}
      true -> {:ok, value}
    end
  end

  defp validate_cache_text(_value), do: {:error, :invalid_cache_text}

  @spec optional_cache_label(module(), map()) :: {:ok, atom() | nil} | {:error, term()}
  defp optional_cache_label(agent, data) do
    if Map.has_key?(data, "label") do
      resolve_cache_label(agent, Map.get(data, "label"))
    else
      {:ok, nil}
    end
  end

  defp resolve_cache_label(agent, value) when is_binary(value) do
    case Rows.route_label(String.trim(value), cacheable_rules(agent)) do
      nil -> {:error, {:unknown_label, value}}
      label -> {:ok, label}
    end
  end

  defp resolve_cache_label(_agent, _value), do: {:error, :invalid_cache_label}

  @spec cache_labels(module()) :: [String.t()]
  defp cache_labels(agent) do
    agent
    |> cacheable_rules()
    |> Enum.map(&Atom.to_string(&1.label))
    |> Enum.uniq()
    |> Enum.sort()
  end

  @spec cacheable_rules(module()) :: [Spectre.Rule.t()]
  defp cacheable_rules(agent) do
    agent.__spectre_router__()
    |> Keyword.put_new(:spectre_agent, agent)
    |> Keyword.put_new(:spectre_rules, agent.__spectre_rules__())
    |> Rows.cacheable_rules()
  end

  @spec put_update_attr(map(), atom(), term()) :: map()
  defp put_update_attr(attrs, _key, nil), do: attrs
  defp put_update_attr(attrs, key, value), do: Map.put(attrs, key, value)

  @spec turn(term()) :: map()
  defp turn(entry) when is_map(entry) do
    %{
      "at" => wire_value(Map.get(entry, :at, Map.get(entry, "at"))),
      "user" => bounded_text(Map.get(entry, :user, Map.get(entry, "user"))),
      "assistant" => bounded_text(Map.get(entry, :assistant, Map.get(entry, "assistant"))),
      "route" => wire_value(Map.get(entry, :route, Map.get(entry, "route"))),
      "events" => entry |> Map.get(:events, Map.get(entry, "events", [])) |> string_list()
    }
  end

  defp turn(_entry), do: %{}

  @spec skill(Spectre.Skill.Mount.t()) :: map()
  defp skill(mount) do
    %{
      "id" => wire_value(mount.id),
      "module" => inspect(mount.module),
      "definition_id" => wire_value(mount.definition_id),
      "definition_version" => mount.definition_version,
      "origin" => "compiled"
    }
  end

  @spec required_text(map(), String.t()) :: {:ok, String.t()} | {:error, term()}
  defp required_text(data, key) do
    case Map.get(data, key) do
      value when is_binary(value) ->
        value = String.trim(value)
        if value == "", do: {:error, {:required, key}}, else: {:ok, value}

      _other ->
        {:error, {:required, key}}
    end
  end

  @spec bounded_limit(term(), pos_integer()) :: pos_integer()
  defp bounded_limit(value, maximum) when is_integer(value) and value > 0, do: min(value, maximum)
  defp bounded_limit(_value, @max_cache_examples), do: @default_cache_examples
  defp bounded_limit(_value, maximum), do: maximum

  @spec bounded_text(term()) :: String.t() | nil
  defp bounded_text(value) when is_binary(value),
    do: String.slice(value, 0, @max_text_graphemes)

  defp bounded_text(_value), do: nil

  @spec history(term()) :: list()
  defp history(value) when is_list(value), do: value
  defp history(_value), do: []

  @spec string_list(term()) :: [String.t()]
  defp string_list(values) when is_list(values),
    do: values |> Enum.map(&wire_value/1) |> Enum.filter(&is_binary/1)

  defp string_list(_values), do: []

  @spec positive_integer(term()) :: pos_integer() | nil
  defp positive_integer(value) when is_integer(value) and value > 0, do: value
  defp positive_integer(_value), do: nil

  @spec put_optional(keyword(), atom(), term()) :: keyword()
  defp put_optional(opts, _key, value) when value in [nil, []], do: opts
  defp put_optional(opts, key, value), do: Keyword.put(opts, key, value)

  @spec wire_value(term()) :: term()
  defp wire_value(%DateTime{} = value), do: DateTime.to_iso8601(value)
  defp wire_value(nil), do: nil
  defp wire_value(value) when is_boolean(value), do: value
  defp wire_value(value) when is_atom(value), do: Atom.to_string(value)
  defp wire_value(value) when is_binary(value) or is_number(value), do: value
  defp wire_value(value), do: inspect(value)
end

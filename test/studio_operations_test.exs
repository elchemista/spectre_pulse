defmodule Spectre.Pulse.StudioOperationsTest do
  use ExUnit.Case, async: false

  alias Spectre.Pulse.{AgentDescriptor, Codec.JSON, ConnectionRegistry, Envelope, Studio}
  alias Spectre.Router.SemanticCache

  defmodule JournalStore do
    def load(_input, _agent, opts) do
      history =
        case Keyword.fetch!(opts, :conversation_id) do
          "malformed" ->
            :invalid_history

          "recent" ->
            Enum.map(1..105, &%{user: "turn #{&1}", assistant: "reply"})

          _ ->
            [
              :invalid_turn,
              %{
                at: ~U[2026-11-17 10:00:00Z],
                user: String.duplicate("a", 8_001),
                assistant: 123,
                route: {:scope, :support},
                events: [:delivered, true, nil, 12]
              }
            ]
        end

      {:ok, %Spectre.State{revision: 7, data: %{chat_history: history}}}
    end
  end

  defmodule Skill do
    use Spectre.Skill, id: :studio_inspection
  end

  defmodule Agent do
    use Spectre.Agent
    use Spectre.Pulse

    state(JournalStore)
    skill(Skill, as: :inspection)

    morph(
      may_propose: [:mount_skill, :replace_skill],
      within: [scopes: [:agent], prompt_tokens: 128]
    )

    pulsing do
      identity("spectre://studio-tests/agent")
    end

    flow :cache do
      on :help, regex: ~r/^help$/ do
        run(:help)
      end
    end

    def help(_input, _context), do: "Help"
  end

  setup do
    owner = self()
    scopes = Studio.scopes()
    {:ok, descriptor} = AgentDescriptor.for_agent(Agent)

    :ok =
      ConnectionRegistry.configure(
        owner,
        [[id: :studio_operations, transport: :websocket, scopes: scopes]],
        [descriptor]
      )

    {:ok, connection} =
      ConnectionRegistry.open(:studio_operations,
        owner: owner,
        principal: %{id: "operator", identity: "spectre://studio-tests/operator", scopes: scopes}
      )

    on_exit(fn ->
      ConnectionRegistry.clear_configuration(owner)
      SemanticCache.clear(Agent)
    end)

    %{connection: connection}
  end

  test "journal inspection bounds history and safely projects persisted values", ctx do
    data = request(ctx.connection, "studio.journal.turns", %{"subject" => " archive "})
    assert data["subject"] == "archive"
    assert data["revision"] == 7
    assert [%{}, turn] = data["turns"]
    assert String.length(turn["user"]) == 8_000
    assert turn["assistant"] == nil
    assert turn["at"] == "2026-11-17T10:00:00Z"
    assert turn["route"] == "{:scope, :support}"
    assert turn["events"] == ["delivered"]

    assert %{"count" => 0, "turns" => []} =
             request(ctx.connection, "studio.journal.turns", %{"subject" => "malformed"})

    assert %{"count" => 100, "turns" => [%{"user" => "turn 6"} | _]} =
             request(ctx.connection, "studio.journal.turns", %{
               "subject" => "recent",
               "limit" => 500
             })

    assert %{"count" => 1, "turns" => [%{"user" => "turn 105"}]} =
             request(ctx.connection, "studio.journal.turns", %{
               "subject" => "recent",
               "limit" => 1
             })
  end

  test "compiled skills retain their provenance in Studio discovery", ctx do
    assert %{
             "count" => 1,
             "skills" => [
               %{
                 "id" => "inspection",
                 "module" => "Spectre.Pulse.StudioOperationsTest.Skill",
                 "origin" => "compiled",
                 "definition_version" => 1
               }
             ]
           } = request(ctx.connection, "studio.skills.list", %{})
  end

  test "promotion requires its own scope and pagination does not repeat cache rows", ctx do
    :ok = SemanticCache.clear(Agent)

    assert {:ok, first} =
             SemanticCache.put("draft alpha", %{label: :help, verified?: false},
               spectre_agent: Agent
             )

    assert {:ok, _second} =
             SemanticCache.put("draft beta", %{label: :help, verified?: false},
               spectre_agent: Agent
             )

    assert %{"count" => 1, "next_offset" => 1, "truncated?" => true} =
             request(ctx.connection, "studio.semantic_cache.examples", %{"limit" => 1})

    assert %{"count" => 1, "offset" => 1, "next_offset" => nil} =
             request(ctx.connection, "studio.semantic_cache.examples", %{
               "limit" => 1,
               "offset" => 1
             })

    assert %{"example" => %{"verified?" => true}} =
             request(ctx.connection, "studio.semantic_cache.verify", %{"example_id" => first.id})

    assert {:ok, %{verified?: true}} = SemanticCache.get_example(Agent, first.id)

    denied = %{ctx.connection | granted_scopes: ["agent.semantic_cache.read"]}

    assert %{"kind" => "authorization", "code" => "connection_scope_required"} =
             request(denied, "studio.semantic_cache.verify", %{"example_id" => first.id}, :error)
  end

  test "malformed Studio data returns correlated request errors", ctx do
    for {operation, data, code} <- [
          {"semantic_cache.examples", [], "invalid_cache_examples_request"},
          {"semantic_cache.examples", %{"source" => "unknown"}, "invalid_semantic_cache_source"},
          {"semantic_cache.update", %{}, "invalid_cache_update"},
          {"semantic_cache.update", %{"example_id" => "id"}, "empty_cache_update"},
          {"semantic_cache.update", %{"example_id" => "id", "text" => " "}, "blank_cache_text"},
          {"semantic_cache.update", %{"example_id" => "id", "text" => 1}, "invalid_cache_text"},
          {"semantic_cache.update", %{"example_id" => "id", "label" => 1}, "invalid_cache_label"},
          {"semantic_cache.verify", %{}, "invalid_example_id"},
          {"journal.turns", [], "invalid_journal_request"},
          {"journal.turns", %{"subject" => " "}, "required"},
          {"journal.turns", %{}, "required"},
          {"morph.propose_skill", [], "invalid_morph_request"}
        ] do
      assert %{"kind" => "request", "code" => ^code} =
               request(ctx.connection, "studio." <> operation, data, :error)
    end

    assert :not_studio = Studio.handle_frame("invalid json", ctx.connection)

    assert :not_studio =
             Studio.handle_frame(~s({"payload":{"type":"support.message"}}), ctx.connection)
  end

  test "a Studio proposal cannot bypass an Instance's activation requirements", ctx do
    subject = "studio-proposal-#{System.unique_integer([:positive])}"
    assert {:ok, instance} = Spectre.summon(agent: Agent, subject: subject)
    on_exit(fn -> if Process.alive?(instance), do: Process.exit(instance, :kill) end)
    previous = Spectre.state(instance)

    for action <- ["mount", "replace"] do
      assert %{"kind" => "request", "code" => "morph_requires_definition_store"} =
               request(
                 ctx.connection,
                 "studio.morph.propose_skill",
                 %{
                   "subject" => subject,
                   "by" => "operator",
                   "reason" => "Add reviewed support response",
                   "mount_id" => "support",
                   "match" => "help",
                   "reply" => "Pending review",
                   "action" => action,
                   "scopes" => ["agent"],
                   "token_cap" => 64
                 },
                 :error
               )
    end

    assert Spectre.state(instance) == previous
  end

  defp request(connection, type, data, outcome \\ :result) do
    envelope =
      Envelope.new!(
        from: connection.principal.identity,
        to: "spectre://studio-tests/agent",
        act: :request,
        payload: %{type: type, data: data}
      )

    assert {:ok, frame} = JSON.encode(envelope, [])

    assert {:ok, %{status: :accepted, message_id: id}, response_frame} =
             Studio.handle_frame(frame, connection)

    assert id == envelope.id
    assert {:ok, response} = JSON.decode(response_frame, [])
    assert response.relates_to == envelope.id
    assert response.payload.type == type <> "." <> Atom.to_string(outcome)
    response.payload.data
  end
end

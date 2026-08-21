defmodule Spectre.Pulse.ConnectionBoundariesTest.Agent do
  @moduledoc false

  use Spectre.Agent
  use Spectre.Pulse

  pulsing do
    identity("spectre://connection-boundaries/agent")
  end
end

defmodule Spectre.Pulse.ConnectionBoundariesTest.Callbacks do
  @moduledoc false

  alias Spectre.Pulse.Error

  @doc false
  @spec authenticate(term(), map()) :: {:ok, map()} | {:error, atom()}
  def authenticate(:valid, _context),
    do: {:ok, %{id: "callback", scopes: ["observe"]}}

  def authenticate(_credential, _context), do: {:error, :invalid_callback_credential}

  @doc false
  @spec authorize(Spectre.Pulse.Principal.t(), map()) :: {:ok, keyword()}
  def authorize(_principal, _request),
    do: {:ok, granted_scopes: ["observe"]}

  @doc false
  @spec authenticate_mfa(term(), map(), term()) :: {:ok, map()} | {:error, atom()}
  def authenticate_mfa(credential, _context, expected) do
    if credential == expected,
      do: {:ok, %{id: "mfa", scopes: ["observe"]}},
      else: {:error, :invalid_mfa_credential}
  end

  @doc false
  @spec raise_authentication(term(), map()) :: no_return()
  def raise_authentication(_credential, _context), do: raise("private callback failure")

  @doc false
  @spec throw_authentication(term(), map()) :: no_return()
  def throw_authentication(_credential, _context), do: throw(:private_callback_throw)

  @doc false
  @spec typed_authentication_error(term(), map()) :: {:error, Error.t()}
  def typed_authentication_error(_credential, _context),
    do: {:error, Error.not_sent(:authentication, :typed_authentication_error)}
end

defmodule Spectre.Pulse.ConnectionBoundariesTest do
  use ExUnit.Case, async: false

  alias Spectre.Pulse.AgentDescriptor
  alias Spectre.Pulse.Connection
  alias Spectre.Pulse.ConnectionRegistry
  alias Spectre.Pulse.ConnectionSpec
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Handshake
  alias Spectre.Pulse.Principal

  alias __MODULE__.Agent
  alias __MODULE__.Callbacks

  setup do
    ConnectionRegistry.clear_configuration(self())
    on_exit(fn -> ConnectionRegistry.clear_configuration(self()) end)
    :ok
  end

  test "Principal accepts boundary forms and rejects malformed identity data" do
    assert {:ok, principal} =
             Principal.new(
               id: "studio",
               identity: "spectre://studio/operator",
               kind: "studio",
               scopes: [:observe],
               verified: %{issuer: "test"},
               metadata: %{tenant: "acme"}
             )

    assert principal.scopes == ["observe"]
    assert {:ok, ^principal} = Principal.new(principal)

    invalid_values = [
      :invalid,
      [:not_a_keyword],
      %{id: 123},
      %{id: "studio", identity: "not-an-address"},
      %{id: "studio", identity: 123},
      %{id: "studio", kind: ""},
      %{id: "studio", kind: 123},
      %{id: "studio", scopes: :invalid},
      %{id: "studio", scopes: [nil]},
      %{id: "studio", verified: []},
      %{id: "studio", metadata: []}
    ]

    Enum.each(invalid_values, fn value ->
      assert {:error, %Error{kind: :validation}} = Principal.new(value)
    end)
  end

  test "ConnectionSpec validates every host-controlled boundary field" do
    assert {:ok, spec} =
             ConnectionSpec.new(
               id: :studio,
               transport: :websocket,
               authenticate: Callbacks,
               authorize: {Callbacks, :authorize, []},
               profiles: [:messaging],
               scopes: [:observe]
             )

    assert {:ok, ^spec} = ConnectionSpec.new(spec)

    invalid_values = [
      :invalid,
      [:not_a_keyword],
      %{id: "", transport: :websocket},
      %{id: :studio, transport: nil},
      %{id: :studio, transport: :websocket, mode: :invalid},
      %{id: :studio, transport: :websocket, agents: :invalid},
      %{id: :studio, transport: :websocket, agents: [123]},
      %{id: :studio, transport: :websocket, profiles: :invalid},
      %{id: :studio, transport: :websocket, priority: :invalid},
      %{id: :studio, transport: :websocket, enabled: :invalid},
      %{id: :studio, transport: :websocket, authenticate: 123},
      %{id: :studio, transport: :websocket, metadata: []}
    ]

    Enum.each(invalid_values, fn value ->
      assert {:error, %Error{kind: :validation}} = ConnectionSpec.new(value)
    end)

    assert {:error, %Error{kind: :validation}} = ConnectionSpec.resolve_agents(spec, [123])

    assert {:error, %Error{kind: :validation}} =
             ConnectionSpec.resolve_agents(%{spec | agents: :invalid}, [])
  end

  test "Connection validates lifecycle, grants, and remote descriptors" do
    descriptor = descriptor()
    {:ok, spec} = ConnectionSpec.new(id: :studio, transport: :websocket, scopes: [:observe])
    {:ok, spec} = ConnectionSpec.resolve_agents(spec, [descriptor])
    principal = %{id: "studio", scopes: ["observe"]}

    assert {:ok, connection} =
             Connection.new(spec,
               owner: self(),
               principal: principal,
               remote_agents: [%{address: "spectre://remote/agent"}]
             )

    assert Connection.remote_agent_addresses(connection) == ["spectre://remote/agent"]

    invalid_attrs = [
      [:not_a_keyword],
      :invalid,
      [owner: :invalid, principal: principal],
      [owner: self(), transport_pid: :invalid, principal: principal],
      [owner: self(), peer_id: "", principal: principal],
      [owner: self(), direction: :invalid, principal: principal],
      [owner: self(), status: :invalid, principal: principal],
      [owner: self(), requested_profiles: :invalid, principal: principal],
      [owner: self(), principal: principal, remote_agents: :invalid],
      [owner: self(), principal: principal, remote_agents: [123]],
      [owner: self(), principal: principal, verified: []],
      [owner: self(), principal: principal, metadata: []],
      [owner: self(), principal: principal, connected_at: :invalid]
    ]

    Enum.each(invalid_attrs, fn attrs ->
      assert {:error, %Error{kind: :validation}} = Connection.new(spec, attrs)
    end)

    duplicate_remote = [
      %{address: "spectre://remote/duplicate"},
      %{address: "spectre://remote/duplicate"}
    ]

    assert {:error, %Error{reason: {:duplicate_connection_remote_agent, _address}}} =
             Connection.new(spec,
               owner: self(),
               principal: principal,
               remote_agents: duplicate_remote
             )

    assert {:error, %Error{kind: :authorization}} =
             Connection.new(%{spec | enabled: false}, owner: self(), principal: principal)
  end

  test "Handshake normalizes callback forms and contains callback failures" do
    configure(Callbacks, Callbacks)
    assert {:ok, ticket} = Handshake.prepare(:studio, :valid)
    assert {:ok, connection} = Handshake.open(ticket, self())
    assert connection.granted_scopes == ["observe"]

    configure({Callbacks, :authenticate_mfa, [:expected]}, Callbacks)
    assert {:ok, _ticket} = Handshake.prepare(:studio, :expected)

    configure(fn credential -> {:ok, %{id: to_string(credential), scopes: ["observe"]}} end, nil)
    assert {:ok, _ticket} = Handshake.prepare(:studio, :one_arity)

    configure(fn _credential, _context -> false end, nil)
    assert {:error, %Error{kind: :authentication}} = Handshake.prepare(:studio, :credential)

    configure(fn _credential, _context -> :invalid end, nil)

    assert {:error, %Error{reason: {:invalid_authenticator_result, :invalid}}} =
             Handshake.prepare(:studio, :credential)

    configure(&Callbacks.typed_authentication_error/2, nil)

    assert {:error, %Error{reason: :typed_authentication_error}} =
             Handshake.prepare(:studio, :credential)

    configure(&Callbacks.raise_authentication/2, nil)
    assert {:error, %Error{kind: :authentication}} = Handshake.prepare(:studio, :credential)

    configure(&Callbacks.throw_authentication/2, nil)
    assert {:error, %Error{kind: :authentication}} = Handshake.prepare(:studio, :credential)

    assert {:error, %Error{kind: :routing}} = Handshake.prepare(:missing, :credential)
    assert {:error, %Error{kind: :validation}} = Handshake.prepare(:studio, :credential, :invalid)
    assert {:error, %Error{kind: :validation}} = Handshake.open(:invalid, self())
  end

  test "Handshake validates authorization replies and grants" do
    authenticator = fn _credential, _context ->
      {:ok, %{id: "operator", scopes: ["observe"]}}
    end

    valid_authorizers = [
      fn _principal, _request -> :ok end,
      fn _principal, _request -> true end,
      fn _principal, _request -> {:ok, %{granted_scopes: ["observe"]}} end,
      fn _principal -> {:ok, granted_scopes: ["observe"]} end
    ]

    Enum.each(valid_authorizers, fn authorizer ->
      configure(authenticator, authorizer)
      assert {:ok, _ticket} = Handshake.prepare(:studio, :credential)
    end)

    invalid_authorizers = [
      fn _principal, _request -> false end,
      fn _principal, _request -> {:error, :forbidden} end,
      fn _principal, _request -> :invalid end,
      fn _principal, _request -> {:ok, [unknown: true]} end,
      fn _principal, _request -> {:ok, [:not_a_keyword]} end
    ]

    Enum.each(invalid_authorizers, fn authorizer ->
      configure(authenticator, authorizer)
      assert {:error, %Error{}} = Handshake.prepare(:studio, :credential)
    end)

    configure(authenticator, :missing_callback_module)
    assert {:error, %Error{kind: :authorization}} = Handshake.prepare(:studio, :credential)
  end

  defp configure(authenticate, authorize) do
    assert :ok =
             ConnectionRegistry.configure(
               self(),
               [
                 [
                   id: :studio,
                   transport: :websocket,
                   authenticate: authenticate,
                   authorize: authorize,
                   scopes: ["observe"]
                 ]
               ],
               [descriptor()]
             )
  end

  defp descriptor do
    assert {:ok, %AgentDescriptor{} = descriptor} = AgentDescriptor.for_agent(Agent)
    descriptor
  end
end

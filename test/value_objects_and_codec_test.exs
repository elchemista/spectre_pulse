defmodule Spectre.Pulse.ValueObjectsAndCodecTest.ReplyCodec do
  @behaviour Spectre.Pulse.Codec

  @impl true
  def encode(_envelope, opts), do: Keyword.fetch!(opts, :reply)

  @impl true
  def decode(_encoded, opts), do: Keyword.fetch!(opts, :reply)
end

defmodule Spectre.Pulse.ValueObjectsAndCodecTest do
  use ExUnit.Case, async: true

  alias Spectre.Pulse.Address
  alias Spectre.Pulse.Codec
  alias Spectre.Pulse.Codec.Identity, as: IdentityCodec
  alias Spectre.Pulse.Codec.JSON
  alias Spectre.Pulse.Control
  alias Spectre.Pulse.Envelope
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Identity
  alias Spectre.Pulse.Payload
  alias Spectre.Pulse.Protocol
  alias Spectre.Pulse.Reachability
  alias Spectre.Pulse.Receipt
  alias Spectre.Pulse.Validator

  alias __MODULE__.ReplyCodec

  setup do
    envelope =
      Envelope.new!(
        from: "spectre://tests/sender",
        to: "spectre://tests/receiver",
        act: :request,
        payload: %{type: "tests.perform", data: %{"value" => 1}}
      )

    %{envelope: envelope}
  end

  test "generated message identifiers are unique RFC 9562 UUIDv7 values" do
    ids =
      for _index <- 1..256 do
        Envelope.new!(
          from: "spectre://tests/sender",
          to: "spectre://tests/receiver",
          payload: %{type: "tests.perform"}
        ).id
      end

    assert MapSet.size(MapSet.new(ids)) == length(ids)

    assert Enum.all?(
             ids,
             &Regex.match?(
               ~r/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
               &1
             )
           )
  end

  test "generic codec dispatcher normalizes every adapter result", %{envelope: envelope} do
    assert {:ok, :encoded} =
             Codec.encode(ReplyCodec, envelope, reply: {:ok, :encoded})

    existing = Error.outcome_unknown(:codec, :existing)

    assert {:error, ^existing} =
             Codec.encode(ReplyCodec, envelope, reply: {:error, existing})

    assert {:error, %Error{kind: :codec, reason: :failed, message_id: message_id}} =
             Codec.encode(ReplyCodec, envelope, reply: {:error, :failed})

    assert message_id == envelope.id

    assert {:error, %Error{reason: {:invalid_codec_result, :invalid}}} =
             Codec.encode(ReplyCodec, envelope, reply: :invalid)

    assert {:error, %Error{reason: {:invalid_codec, Protocol}}} =
             Codec.encode(Protocol, envelope)

    assert {:error, %Error{kind: :codec}} =
             Codec.encode(Spectre.Pulse.MissingCodec, envelope)

    assert {:ok, ^envelope} =
             Codec.decode(ReplyCodec, :wire, reply: {:ok, envelope})

    assert {:error, ^existing} =
             Codec.decode(ReplyCodec, :wire, reply: {:error, existing})

    assert {:error, %Error{kind: :codec, reason: :decode_failed}} =
             Codec.decode(ReplyCodec, :wire, reply: {:error, :decode_failed})

    assert {:error, %Error{reason: {:invalid_codec_result, :invalid}}} =
             Codec.decode(ReplyCodec, :wire, reply: :invalid)

    assert {:error, %Error{reason: {:invalid_codec, Protocol}}} =
             Codec.decode(Protocol, :wire)

    assert {:error, %Error{kind: :codec}} =
             Codec.decode(Spectre.Pulse.MissingCodec, :wire)
  end

  test "identity codec is zero-copy but rejects other values", %{envelope: envelope} do
    assert {:ok, ^envelope} = IdentityCodec.encode(envelope, [])
    assert {:ok, ^envelope} = IdentityCodec.decode(envelope, [])

    assert {:error, %Error{reason: {:identity_codec_expected_envelope, :invalid}}} =
             IdentityCodec.decode(:invalid, [])
  end

  test "JSON codec rejects malformed, oversized, non-object and non-binary input", %{
    envelope: envelope
  } do
    assert {:error, %Error{reason: {:envelope_too_large, _, 1}}} =
             JSON.decode("{}", max_envelope_bytes: 1)

    assert {:error, %Error{reason: {:json_envelope_not_object, []}}} =
             JSON.decode("[]", [])

    assert {:error, %Error{reason: {:json_decode_failed, _reason}}} =
             JSON.decode("{", [])

    assert {:error, %Error{reason: {:json_codec_expected_binary, :not_binary}}} =
             JSON.decode(:not_binary, [])

    invalid = %{envelope | metadata: %{callback: fn -> :ok end}}

    assert {:error, %Error{reason: {:json_encode_failed, _reason}}} =
             JSON.encode(invalid, [])
  end

  test "protocol exposes controlled acts, control types and overridable limits" do
    assert Protocol.version() == 1
    assert Protocol.acts() == [:inform, :query, :request]
    assert "pulse.identity.describe" in Protocol.control_types()
    assert Protocol.valid_act?(:query)
    refute Protocol.valid_act?(:invented)
    assert {:ok, "request"} = Protocol.encode_act(:request)
    assert {:error, {:unsupported_act, :invented}} = Protocol.encode_act(:invented)
    assert {:ok, :inform} = Protocol.decode_act(:inform)
    assert {:ok, :query} = Protocol.decode_act("query")
    assert {:error, {:unsupported_act, "invented"}} = Protocol.decode_act("invented")
    assert {:error, {:unsupported_act, 1}} = Protocol.decode_act(1)
    assert %{max_type_bytes: 7} = Protocol.limits(max_type_bytes: 7)
  end

  test "public identity validates every controlled field" do
    assert {:ok, identity} =
             Identity.new(
               "spectre://tests/agent",
               display_name: "Agent",
               capabilities: [:research, :research, "summary"],
               metadata: %{region: "eu"}
             )

    assert identity.capabilities == [:research, "summary"]

    assert Identity.to_public_map(identity) == %{
             "address" => "spectre://tests/agent",
             "display_name" => "Agent",
             "protocol_versions" => [1],
             "capabilities" => ["research", "summary"],
             "metadata" => %{region: "eu"}
           }

    assert {:ok, from_string_keys} =
             Identity.new(%{
               "address" => "spectre://tests/string-keys",
               "protocol_versions" => [1]
             })

    assert from_string_keys.address == "spectre://tests/string-keys"

    assert {:error, %Error{reason: {:invalid_protocol_versions, [2]}}} =
             Identity.new(address: "spectre://tests/agent", protocol_versions: [2])

    assert {:error, %Error{reason: {:invalid_protocol_versions, :invalid}}} =
             Identity.new(address: "spectre://tests/agent", protocol_versions: :invalid)

    assert {:error, %Error{reason: {:invalid_capabilities, :invalid}}} =
             Identity.new(address: "spectre://tests/agent", capabilities: :invalid)

    assert {:error, %Error{reason: {:invalid_identity_metadata, []}}} =
             Identity.new(address: "spectre://tests/agent", metadata: [])

    assert {:error, %Error{reason: {:invalid_identity, :invalid}}} =
             Identity.new(:invalid)

    assert_raise ArgumentError, fn -> Identity.new!(%{}) end
  end

  test "control messages preserve public identity, nonce and correlation" do
    identity =
      Identity.new!(
        address: "spectre://tests/sender",
        display_name: "Sender",
        capabilities: [:ping]
      )

    assert {:ok, description} =
             Control.describe(identity, "spectre://tests/receiver",
               metadata: %{trace: "description"}
             )

    assert description.act == :inform
    assert description.payload.type == "pulse.identity.describe"
    assert description.payload.data["address"] == identity.address

    assert {:ok, ping} =
             Control.ping(identity.address, "spectre://tests/receiver",
               nonce: "nonce-1",
               metadata: %{trace: "ping"}
             )

    assert ping.act == :query
    assert ping.payload.data == %{"nonce" => "nonce-1"}

    assert {:ok, pong} = Control.pong(ping)
    assert pong.act == :inform
    assert pong.relates_to == ping.id
    assert pong.payload.data == %{"nonce" => "nonce-1"}

    atom_nonce_ping = %{ping | payload: %{ping.payload | data: %{nonce: "atom-nonce"}}}
    assert {:ok, atom_nonce_pong} = Control.pong(atom_nonce_ping)
    assert atom_nonce_pong.payload.data == %{"nonce" => "atom-nonce"}

    scalar_ping = %{ping | payload: %{ping.payload | data: :invalid}}
    assert {:ok, scalar_pong} = Control.pong(scalar_ping)
    assert scalar_pong.payload.data == %{"nonce" => nil}
  end

  test "typed errors retain outcome and routing diagnostics" do
    cause = RuntimeError.exception("boom")

    not_sent =
      Error.not_sent(:transport, :closed,
        message_id: "message",
        route_id: "route",
        details: %{attempt: 1},
        cause: cause
      )

    assert Error.not_sent?(not_sent)
    refute Error.outcome_unknown?(not_sent)
    assert Exception.message(not_sent) =~ "Pulse transport"
    assert not_sent.details == %{attempt: 1}
    assert not_sent.cause == cause

    unknown = Error.outcome_unknown(:transport, :timeout)
    assert Error.outcome_unknown?(unknown)
    refute Error.not_sent?(unknown)
    refute Error.not_sent?(:closed)
    refute Error.outcome_unknown?(:timeout)
    assert Error.normalize(unknown, :not_sent) == unknown

    normalized = Error.normalize(:closed, :not_sent, kind: :routing, route_id: "route")
    assert %Error{kind: :routing, outcome: :not_sent, route_id: "route"} = normalized
  end

  test "reachability observations expire according to their validity window" do
    observed_at = ~U[2026-01-01 00:00:00Z]
    before_expiry = ~U[2026-01-01 00:00:00.999Z]
    after_expiry = ~U[2026-01-01 00:00:01Z]

    observation =
      Reachability.new(:reachable,
        level: :pulse_endpoint,
        via: :local,
        observed_at: observed_at,
        valid_for_ms: 1_000,
        metadata: %{route: "local"}
      )

    refute Reachability.expired?(observation, before_expiry)
    assert Reachability.expired?(observation, after_expiry)
    assert Reachability.expired?(Reachability.unknown(:no_route), observed_at)
  end

  test "payload validates shape, namespace and size" do
    assert {:ok, payload} = Payload.new(type: "research.perform", data: :opaque)
    assert {:ok, ^payload} = Payload.new(payload)
    assert Payload.to_wire(payload) == %{"type" => "research.perform", "data" => :opaque}

    assert {:error, %Error{reason: :payload_type_required}} =
             Payload.new(type: "")

    assert {:error, %Error{reason: {:payload_type_too_large, 16, 4}}} =
             Payload.new(%{"type" => "research.perform"}, max_type_bytes: 4)

    assert {:error, %Error{reason: {:invalid_payload_type, "Invalid"}}} =
             Payload.new(type: "Invalid")

    assert {:error, %Error{reason: {:invalid_payload_type, nil}}} =
             Payload.new(data: %{})

    assert {:error, %Error{reason: {:invalid_payload, :invalid}}} =
             Payload.new(:invalid)

    assert_raise ArgumentError, fn -> Payload.new!(%{}) end
  end

  test "receipt parsing validates ids, status, time and metadata" do
    id = Spectre.Identity.uuid7()
    accepted_at = ~U[2026-01-01 00:00:00Z]

    receipt =
      Receipt.accepted(id,
        via: :local,
        route_id: :route,
        accepted_at: accepted_at,
        metadata: %{node: "local"}
      )

    assert {:ok, ^receipt} = Receipt.new(receipt)

    assert Receipt.to_wire(receipt) == %{
             "message_id" => id,
             "status" => "accepted",
             "via" => "local",
             "route_id" => "route",
             "accepted_at" => "2026-01-01T00:00:00Z",
             "metadata" => %{node: "local"}
           }

    assert {:ok, parsed} =
             Receipt.new(%{
               "message_id" => id,
               "status" => "accepted",
               "accepted_at" => "2026-01-01T00:00:00Z"
             })

    assert parsed.accepted_at == accepted_at

    assert {:error, %Error{reason: {:invalid_receipt_message_id, "invalid"}}} =
             Receipt.new(%{message_id: "invalid", status: :accepted, accepted_at: accepted_at})

    assert {:error, %Error{reason: {:invalid_receipt_status, :rejected}}} =
             Receipt.new(%{message_id: id, status: :rejected, accepted_at: accepted_at})

    assert {:error, %Error{reason: {:invalid_receipt_time, "invalid"}}} =
             Receipt.new(%{message_id: id, status: :accepted, accepted_at: "invalid"})

    assert {:error, %Error{reason: {:invalid_receipt_time, :invalid}}} =
             Receipt.new(%{message_id: id, status: :accepted, accepted_at: :invalid})

    assert {:error, %Error{reason: {:invalid_receipt_metadata, []}}} =
             Receipt.new(%{
               message_id: id,
               status: :accepted,
               accepted_at: accepted_at,
               metadata: []
             })

    assert {:error, %Error{reason: {:invalid_receipt, :invalid}}} =
             Receipt.new(:invalid)
  end

  test "address rejects every physical or malformed component" do
    address = Address.new!("spectre://Tests/agent")
    assert Address.new(address) == {:ok, address}
    assert Address.normalize!(address) == "spectre://tests/agent"
    assert Address.equal?(address, "SPECTRE://tests/agent")
    refute Address.equal?(address, :invalid)
    assert to_string(address) == "spectre://tests/agent"
    assert byte_size(Address.agent_id(__MODULE__)) == 32
    assert Address.for_agent(__MODULE__) == "spectre://pulse/" <> Address.agent_id(__MODULE__)

    invalid_addresses = [
      {"spectre://tests/agent", [max_address_bytes: 2], {:address_too_large, 21, 2}},
      {"other://tests/agent", [], {:invalid_scheme, "other"}},
      {"spectre:///agent", [], :authority_required},
      {"spectre://bad_authority/agent", [], {:invalid_authority, "bad_authority"}},
      {"spectre://tests/", [], :agent_required},
      {"spectre://tests/agent/", [], {:invalid_agent, "agent/"}},
      {"spectre://tests/a//b", [], {:invalid_agent, "a//b"}},
      {"spectre://tests/a\\\\b", [], {:invalid_agent, "a\\\\b"}},
      {"spectre://user@tests/agent", [], :address_must_be_logical},
      {"spectre://tests/agent?route=x", [], :address_must_be_logical},
      {"spectre://tests/agent#route", [], :address_must_be_logical}
    ]

    for {value, opts, expected_reason} <- invalid_addresses do
      assert {:error, %Error{reason: {:invalid_address, ^expected_reason}}} =
               Address.new(value, opts)
    end

    assert {:error, %Error{reason: {:invalid_address, :invalid}}} = Address.new(:invalid)
    assert_raise ArgumentError, fn -> Address.new!("invalid") end
  end

  test "envelope validator rejects controlled field and metadata failures", %{envelope: envelope} do
    assert Validator.valid_id?(envelope.id)
    refute Validator.valid_id?("invalid")
    refute Validator.valid_id?(nil)
    assert {:ok, ^envelope} = Validator.validate(envelope)

    invalid_values = [
      {%{envelope | version: 2}, {:unsupported_version, 2}},
      {%{envelope | id: "invalid"}, {:invalid_message_id, "invalid"}},
      {%{envelope | id: nil}, {:invalid_message_id, :id, nil}},
      {%{envelope | relates_to: "invalid"}, {:invalid_message_id, "invalid"}},
      {%{envelope | act: :invalid}, {:unsupported_act, :invalid}},
      {%{envelope | metadata: []}, {:invalid_metadata, []}}
    ]

    for {value, reason} <- invalid_values do
      assert {:error, %Error{kind: :validation, reason: ^reason}} =
               Validator.validate(value)
    end

    assert {:error, %Error{reason: {:metadata_too_large, _, 1}}} =
             Validator.validate(%{envelope | metadata: %{trace: "large"}},
               max_metadata_bytes: 1
             )

    assert {:error, %Error{reason: {:metadata_not_encodable, _reason}}} =
             Validator.validate(%{envelope | metadata: %{callback: fn -> :ok end}})

    assert {:error, %Error{reason: {:invalid_envelope, :invalid}}} =
             Validator.validate(:invalid)

    assert {:error, %Error{reason: {:invalid_envelope, :invalid}}} =
             Envelope.new(:invalid)

    assert_raise ArgumentError, fn -> Envelope.new!(%{}) end
  end
end

defmodule Spectre.Pulse.ProtocolTest do
  use ExUnit.Case, async: true

  alias Spectre.Pulse.Address
  alias Spectre.Pulse.Codec.JSON
  alias Spectre.Pulse.Envelope
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Receipt

  test "logical addresses are canonical and reject physical routing data" do
    assert {:ok, address} = Address.new("SPECTRE://Acme/tao")
    assert address.value == "spectre://acme/tao"
    assert address.authority == "acme"
    assert address.agent == "tao"

    assert {:error, %Error{kind: :validation}} =
             Address.new("https://agents.example/tao")

    assert {:error, %Error{reason: {:invalid_address, :address_must_be_logical}}} =
             Address.new("spectre://acme:444/tao")
  end

  test "envelope v1 round-trips through JSON without dynamic atom decoding" do
    assert {:ok, envelope} =
             Envelope.new(
               from: "spectre://acme/anna",
               to: "spectre://acme/tao",
               act: :request,
               payload: %{
                 type: "research.perform",
                 data: %{"topic" => "mercato nautico italiano"}
               },
               metadata: %{"trace_id" => "trace-1"}
             )

    assert envelope.version == 1
    assert String.starts_with?(envelope.id, "0")
    assert envelope.act == :request

    assert {:ok, encoded} = JSON.encode(envelope, [])
    assert {:ok, decoded} = JSON.decode(encoded, [])
    assert decoded == envelope

    unknown_act =
      encoded
      |> Jason.decode!()
      |> Map.put("act", "invented-remotely")
      |> Jason.encode!()

    assert {:error, %Error{reason: {:unsupported_act, "invented-remotely"}}} =
             JSON.decode(unknown_act, [])
  end

  test "correlation uses a fresh message and forbids self-reference" do
    request =
      Envelope.new!(
        from: "spectre://acme/anna",
        to: "spectre://acme/tao",
        act: :query,
        payload: %{type: "research.question", data: "status?"}
      )

    assert {:ok, reply} =
             Envelope.reply(request, "research.status", %{"status" => "working"})

    assert reply.id != request.id
    assert reply.from == request.to
    assert reply.to == request.from
    assert reply.relates_to == request.id

    assert {:error, %Error{reason: :message_cannot_relate_to_itself}} =
             Envelope.new(%{reply | relates_to: reply.id})
  end

  test "wire size and metadata constraints fail before transport" do
    envelope =
      Envelope.new!(
        from: "spectre://acme/anna",
        to: "spectre://acme/tao",
        payload: %{type: "project.updated", data: String.duplicate("x", 100)}
      )

    assert {:error, %Error{outcome: :not_sent, reason: {:envelope_too_large, _, 32}}} =
             JSON.encode(envelope, max_envelope_bytes: 32)

    assert {:error, %Error{reason: {:metadata_not_encodable, _reason}}} =
             Envelope.new(%{envelope | metadata: %{callback: fn -> :ok end}})
  end

  test "receipts acknowledge technical acceptance only" do
    id = Spectre.Identity.uuid7()
    receipt = Receipt.accepted(id, via: :local, route_id: "route-1")
    wire = Receipt.to_wire(receipt)

    assert {:ok, restored} = Receipt.new(wire)
    assert restored.message_id == id
    assert restored.status == :accepted
    assert restored.via == "local"
  end

  test "protocol description makes non-guarantees explicit" do
    protocol = Spectre.Pulse.protocol()

    assert protocol.version == 1
    assert protocol.acts == ["inform", "query", "request"]
    refute protocol.delivery.exactly_once
    assert protocol.delivery.duplicates_possible
    refute protocol.delivery.ordering_guaranteed
  end
end

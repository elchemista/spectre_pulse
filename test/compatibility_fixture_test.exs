defmodule Spectre.Pulse.CompatibilityFixtureTest do
  use ExUnit.Case, async: true

  alias Spectre.Pulse.Codec.JSON
  alias Spectre.Pulse.Envelope
  alias Spectre.Pulse.Payload

  @fixture Path.expand(
             "fixtures/compatibility/0.1.6/pulse-envelope-v1.json",
             __DIR__
           )

  test "the permanent Pulse envelope v1 fixture remains wire-compatible" do
    fixture = File.read!(@fixture)

    assert {:ok,
            %Envelope{
              version: 1,
              id: "018f0f3e-7b21-7a1c-8f42-123456789abc",
              from: "spectre://compatibility/sender",
              to: "spectre://compatibility/receiver",
              act: :request,
              relates_to: "018f0f3e-7b20-7fff-b123-abcdef012345",
              payload: %Payload{
                type: "compatibility.baseline",
                data: %{
                  "release" => "0.1.6",
                  "operation" => "restore",
                  "attempt" => 1
                }
              },
              metadata: %{
                "trace_id" => "trace-pulse-0.1.6",
                "fixture" => true
              }
            } = envelope} = JSON.decode(fixture, [])

    assert {:ok, reencoded} = JSON.encode(envelope, [])
    assert Jason.decode!(reencoded) == Jason.decode!(fixture)
  end
end

defmodule Spectre.Pulse.ContactAndStateTest do
  use ExUnit.Case, async: true

  alias Spectre.Pulse.Contact
  alias Spectre.Pulse.ContactBook
  alias Spectre.Pulse.Envelope
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Expectation
  alias Spectre.Pulse.Route
  alias Spectre.Pulse.State, as: PulseState

  test "an address book keeps local names out of canonical identity" do
    route = Route.local("spectre://acme/tao", self())

    tao =
      Contact.new!(
        :tao,
        "spectre://acme/tao",
        display_name: "Tao",
        capabilities: [:research, :summarization],
        routes: [route]
      )

    assert {:ok, book} = ContactBook.new([tao])
    assert {:ok, "spectre://acme/tao"} = ContactBook.resolve(book, :tao)
    assert {:ok, "spectre://acme/tao"} = ContactBook.resolve(book, "spectre://ACME/tao")
    assert [^tao] = ContactBook.find(book, capability: :research)
    assert [^route] = ContactBook.routes(book, :tao)
  end

  test "one identity is not silently merged under two local keys" do
    assert {:ok, book} =
             ContactBook.new([
               Contact.new!(:tao, "spectre://acme/tao")
             ])

    assert {:error, %Error{reason: {:identity_already_known_as, :tao}}} =
             ContactBook.put(
               book,
               Contact.new!(:researcher, "spectre://acme/tao")
             )
  end

  test "contact trust is rejected because it conflates separate security facts" do
    assert {:error, %Error{reason: :contact_trust_is_not_a_protocol_fact}} =
             Contact.new(%{
               key: :tao,
               identity: "spectre://acme/tao",
               trust: :verified
             })
  end

  test "dynamic contacts and expectations remain pure Spectre state values" do
    state = %Spectre.State{}
    contact = Contact.new!(:tao, "spectre://acme/tao")

    assert {:ok, state} = PulseState.remember_contact(state, contact)
    assert {:ok, ^contact} = state |> PulseState.contact_book() |> ContactBook.fetch(:tao)

    message_id = Spectre.Identity.uuid7()
    expectation = Expectation.new(message_id, :tao, "research.completed")
    state = PulseState.put_expectation(state, expectation)

    response =
      Envelope.new!(
        from: "spectre://acme/tao",
        to: "spectre://acme/anna",
        relates_to: message_id,
        payload: %{type: "research.completed", data: %{result: "done"}}
      )

    assert {:ok, correlated, resolved} = PulseState.correlate(state, response)
    assert resolved.status == :resolved
    assert resolved.resolved_by == response.id
    assert correlated.data.pulse.expectations[message_id].status == :resolved

    forgotten = PulseState.forget_contact(correlated, :tao)
    assert :error = forgotten |> PulseState.contact_book() |> ContactBook.fetch(:tao)
  end

  test "a different correlated payload does not close a typed expectation" do
    message_id = Spectre.Identity.uuid7()

    state =
      %Spectre.State{}
      |> PulseState.put_expectation(
        Expectation.new(message_id, :tao, {:type, "research.completed"})
      )

    update =
      Envelope.new!(
        from: "spectre://acme/tao",
        to: "spectre://acme/anna",
        relates_to: message_id,
        payload: %{type: "research.progress", data: %{percent: 50}}
      )

    assert :unmatched = PulseState.correlate(state, update)
    assert PulseState.expectations(state)[message_id].status == :open
  end
end

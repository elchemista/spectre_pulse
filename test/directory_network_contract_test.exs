defmodule Spectre.Pulse.DirectoryNetworkContractTest.DirectoryFixture do
  @behaviour Spectre.Pulse.Directory

  @impl true
  def resolve(_reference, opts), do: reply(Keyword.fetch!(opts, :resolve_reply))

  @impl true
  def routes(_address, opts), do: reply(Keyword.get(opts, :routes_reply, []))

  @impl true
  def contacts(opts), do: reply(Keyword.get(opts, :contacts_reply, []))

  defp reply(:raise), do: raise("directory failure")
  defp reply(:throw), do: throw(:directory_failure)
  defp reply(reply), do: reply
end

defmodule Spectre.Pulse.DirectoryNetworkContractTest.NetworkFixture do
  @behaviour Spectre.Pulse.Network

  @impl true
  def deliver(_envelope, opts), do: reply(Keyword.fetch!(opts, :reply))

  @impl true
  def probe(_address, opts), do: reply(Keyword.fetch!(opts, :reply))

  defp reply(:raise), do: raise("network failure")
  defp reply(:throw), do: throw(:network_failure)
  defp reply(reply), do: reply
end

defmodule Spectre.Pulse.DirectoryNetworkContractTest.TransportFixture do
  @behaviour Spectre.Pulse.Transport

  @impl true
  def deliver(_route, _envelope, opts), do: reply(Keyword.fetch!(opts, :reply))

  @impl true
  def probe(_route, opts), do: reply(Keyword.fetch!(opts, :reply))

  defp reply(:raise), do: raise("transport failure")
  defp reply(:throw), do: throw(:transport_failure)
  defp reply(reply), do: reply
end

defmodule Spectre.Pulse.DirectoryNetworkContractTest do
  use ExUnit.Case, async: true

  alias Spectre.Pulse.Address
  alias Spectre.Pulse.Contact
  alias Spectre.Pulse.ContactBook
  alias Spectre.Pulse.Directory
  alias Spectre.Pulse.Directory.Resolution
  alias Spectre.Pulse.Envelope
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Network
  alias Spectre.Pulse.Network.Routed
  alias Spectre.Pulse.Protocol
  alias Spectre.Pulse.Reachability
  alias Spectre.Pulse.Receipt
  alias Spectre.Pulse.Route
  alias Spectre.Pulse.Transport

  alias __MODULE__.DirectoryFixture
  alias __MODULE__.NetworkFixture
  alias __MODULE__.TransportFixture

  setup do
    envelope =
      Envelope.new!(
        from: "spectre://contracts/sender",
        to: "spectre://contracts/receiver",
        payload: %{type: "contracts.perform", data: %{}}
      )

    route =
      Route.new!(
        id: "contract-route",
        address: envelope.to,
        transport: TransportFixture,
        target: self(),
        priority: 10
      )

    %{envelope: envelope, route: route}
  end

  test "directory resolves contact books, direct addresses and unknown references" do
    route = Route.local("spectre://contracts/receiver", self(), id: "contact-route")
    contact = Contact.new!(:receiver, "spectre://contracts/receiver", routes: [route])
    book = ContactBook.new!([contact])

    assert {:ok, %Resolution{contact: ^contact, routes: [^route], source: :contact_book}} =
             Directory.resolve(book, :receiver)

    assert {:ok, %Resolution{contact: ^contact, address: "spectre://contracts/receiver"}} =
             Directory.resolve(book, "spectre://contracts/receiver")

    assert {:ok, %Resolution{source: :address}} =
             Directory.resolve(nil, "SPECTRE://contracts/direct")

    assert {:error, %Error{reason: {:unknown_contact, :missing}}} =
             Directory.resolve(nil, :missing)

    assert {:error, %Error{reason: {:invalid_directory, %{invalid: true}}}} =
             Directory.resolve(%{invalid: true}, :receiver)
  end

  test "directory normalizes every supported resolution value and configured options" do
    route =
      Route.web_socket("spectre://contracts/receiver", self(),
        id: "extra",
        priority: 20
      )

    contact_route =
      Route.local("spectre://contracts/receiver", self(),
        id: "existing",
        priority: 10
      )

    contact =
      Contact.new!(:receiver, "spectre://contracts/receiver", routes: [contact_route])

    assert {:ok, %Resolution{contact: ^contact, routes: [^contact_route, ^route]}} =
             Directory.resolve(
               {DirectoryFixture,
                resolve_reply: {:ok, contact}, routes_reply: {:ok, [Map.from_struct(route)]}},
               :receiver
             )

    parsed = Address.new!("spectre://contracts/address-struct")

    assert {:ok, %Resolution{address: "spectre://contracts/address-struct"}} =
             Directory.resolve(
               {DirectoryFixture, resolve_reply: parsed},
               :receiver
             )

    resolution = %Resolution{
      reference: :receiver,
      address: "SPECTRE://contracts/resolution",
      routes: [route],
      source: nil
    }

    assert {:ok, normalized} =
             Directory.resolve({DirectoryFixture, resolve_reply: resolution}, :receiver)

    assert normalized.address == "spectre://contracts/resolution"
    assert normalized.source == DirectoryFixture

    resolver = fn reference, opts ->
      {:ok, "spectre://contracts/#{reference}-#{Keyword.fetch!(opts, :suffix)}"}
    end

    assert {:ok, %Resolution{address: "spectre://contracts/receiver-test"}} =
             Directory.resolve(resolver, :receiver, suffix: "test")
  end

  test "directory normalizes errors, malformed values, exceptions and throws" do
    existing = Error.outcome_unknown(:routing, :existing)

    replies = [
      {:error, {:unknown_contact, :receiver}},
      {:error, :existing},
      {:error, :custom_failure},
      {:error, {:invalid_directory_result, 123}},
      {:error, {:directory_exception, RuntimeError.exception("directory failure")}},
      {:error, {:directory_throw, :throw, :directory_failure}}
    ]

    sources = [
      {DirectoryFixture, resolve_reply: :error},
      {DirectoryFixture, resolve_reply: {:error, existing}},
      {DirectoryFixture, resolve_reply: {:error, :custom_failure}},
      {DirectoryFixture, resolve_reply: 123},
      {DirectoryFixture, resolve_reply: :raise},
      {DirectoryFixture, resolve_reply: :throw}
    ]

    for {source, {:error, reason}} <- Enum.zip(sources, replies) do
      assert {:error, %Error{reason: actual}} = Directory.resolve(source, :receiver)

      case reason do
        {:directory_exception, RuntimeError} ->
          assert match?({:directory_exception, %RuntimeError{}}, actual)

        _other ->
          assert actual == reason
      end
    end

    raising = fn _reference, _opts -> raise "function directory" end
    throwing = fn _reference, _opts -> throw(:function_directory) end

    assert {:error, %Error{reason: {:directory_exception, %RuntimeError{}}}} =
             Directory.resolve(raising, :receiver)

    assert {:error, %Error{reason: {:directory_throw, :throw, :function_directory}}} =
             Directory.resolve(throwing, :receiver)
  end

  test "directory route and contact lists are normalized or rejected", %{route: route} do
    book =
      ContactBook.new!([
        Contact.new!(:receiver, route.address, routes: [route])
      ])

    assert {:ok, [^route]} = Directory.routes(book, route.address)

    assert {:ok, [_normalized]} =
             Directory.routes(DirectoryFixture, route.address,
               routes_reply: [Map.from_struct(route)]
             )

    assert {:ok, []} = Directory.routes(Protocol, route.address)
    assert {:ok, []} = Directory.routes(:not_loaded_directory, route.address)
    assert {:ok, []} = Directory.routes(%{}, route.address)

    existing = Error.not_sent(:routing, :existing)

    assert {:error, ^existing} =
             Directory.routes(DirectoryFixture, route.address, routes_reply: {:error, existing})

    assert {:error, %Error{reason: :route_failure}} =
             Directory.routes(DirectoryFixture, route.address,
               routes_reply: {:error, :route_failure}
             )

    assert {:error, %Error{reason: {:invalid_routes, :invalid}}} =
             Directory.routes(DirectoryFixture, route.address, routes_reply: :invalid)

    assert {:error, %Error{reason: :route_target_required}} =
             Directory.routes(DirectoryFixture, route.address,
               routes_reply: [[address: route.address, transport: TransportFixture]]
             )

    contacts = [Contact.new!(:receiver, route.address)]

    assert {:ok, ^contacts} =
             Directory.contacts(DirectoryFixture, contacts_reply: {:ok, contacts})

    assert {:ok, ^contacts} = Directory.contacts(ContactBook.new!(contacts), [])
    assert {:ok, []} = Directory.contacts(Protocol, [])
    assert {:ok, []} = Directory.contacts(%{}, [])

    assert {:error, ^existing} =
             Directory.contacts(DirectoryFixture, contacts_reply: {:error, existing})

    assert {:error, %Error{reason: :contacts_failure}} =
             Directory.contacts(DirectoryFixture, contacts_reply: {:error, :contacts_failure})

    assert {:error, %Error{reason: {:invalid_contacts, :invalid}}} =
             Directory.contacts(DirectoryFixture, contacts_reply: :invalid)

    assert {:error, %Error{reason: {:invalid_contact, :invalid}}} =
             Directory.contacts(DirectoryFixture, contacts_reply: [:invalid])
  end

  test "route validates fields and every convenience constructor", %{envelope: envelope} do
    base = %{
      id: "route",
      address: envelope.to,
      transport: TransportFixture,
      target: self(),
      priority: 1,
      metadata: %{source: :test}
    }

    assert {:ok, route} = Route.new(base)
    assert {:ok, ^route} = Route.new(route)

    assert {:ok, string_keyed} =
             Route.new(Map.new(base, fn {key, value} -> {to_string(key), value} end))

    assert string_keyed.address == envelope.to

    invalid = [
      {%{base | id: nil}, :route_id_required},
      {%{base | transport: nil}, {:invalid_transport, nil}},
      {%{base | transport: "transport"}, {:invalid_transport, "transport"}},
      {%{base | target: nil}, :route_target_required},
      {%{base | priority: "first"}, {:invalid_route_priority, "first"}},
      {%{base | metadata: []}, {:invalid_route_metadata, []}}
    ]

    for {attrs, reason} <- invalid do
      assert {:error, %Error{reason: ^reason}} = Route.new(attrs)
    end

    assert {:error, %Error{reason: {:invalid_route, :invalid}}} = Route.new(:invalid)
    assert_raise ArgumentError, fn -> Route.new!(%{}) end

    assert %Route{transport: Spectre.Pulse.Transports.Local} = Route.local(envelope.to, self())

    assert %Route{transport: Spectre.Pulse.Transports.REST} =
             Route.rest(envelope.to, "http://example.test")

    assert %Route{transport: Spectre.Pulse.Transports.WebSocket} =
             Route.web_socket(envelope.to, self())

    assert %Route{transport: Spectre.Pulse.Transports.Node} =
             Route.node(envelope.to, node(), self())

    assert %Route{transport: Spectre.Pulse.Transports.PubSub} = Route.pub_sub(envelope.to, self())
  end

  test "transport dispatcher validates receipts, errors, exceptions and throws", %{
    envelope: envelope,
    route: route
  } do
    receipt = Receipt.accepted(envelope.id, via: :test)
    existing = Error.not_sent(:transport, :existing)

    assert {:ok, ^receipt} = Transport.dispatch(route, envelope, reply: {:ok, receipt})
    assert {:error, ^existing} = Transport.dispatch(route, envelope, reply: {:error, existing})

    mismatch = Receipt.accepted(Spectre.Identity.uuid7())

    assert {:error, %Error{reason: :receipt_message_mismatch, outcome: :outcome_unknown}} =
             Transport.dispatch(route, envelope, reply: {:ok, mismatch})

    assert {:error, %Error{reason: :adapter_failure, outcome: :outcome_unknown}} =
             Transport.dispatch(route, envelope, reply: {:error, :adapter_failure})

    assert {:error, %Error{reason: {:invalid_transport_result, :invalid}}} =
             Transport.dispatch(route, envelope, reply: :invalid)

    assert {:error,
            %Error{reason: {:transport_exception, %RuntimeError{}}, cause: %RuntimeError{}}} =
             Transport.dispatch(route, envelope, reply: :raise)

    assert {:error, %Error{reason: {:transport_exit, :throw, :transport_failure}}} =
             Transport.dispatch(route, envelope, reply: :throw)

    invalid_transport = %{route | transport: Protocol}

    assert {:error, %Error{reason: {:invalid_transport, Protocol}}} =
             Transport.dispatch(invalid_transport, envelope)
  end

  test "transport probes normalize supported and unsupported adapters", %{route: route} do
    reachable = Reachability.new(:reachable, via: :test)
    existing = Error.not_sent(:transport, :existing)

    assert {:ok, ^reachable} = Transport.probe(route, reply: {:ok, reachable})
    assert {:error, ^existing} = Transport.probe(route, reply: {:error, existing})

    assert {:error, %Error{reason: :probe_failure}} =
             Transport.probe(route, reply: {:error, :probe_failure})

    assert {:error, %Error{reason: {:invalid_probe_result, :invalid}}} =
             Transport.probe(route, reply: :invalid)

    assert {:error, %Error{reason: {:transport_exception, %RuntimeError{}}}} =
             Transport.probe(route, reply: :raise)

    assert {:error, %Error{reason: {:transport_exit, :throw, :transport_failure}}} =
             Transport.probe(route, reply: :throw)

    assert {:ok, %Reachability{status: :unknown, reason: :probe_not_supported}} =
             Transport.probe(%{route | transport: Protocol})
  end

  test "network dispatcher supports modules, configured modules and functions", %{
    envelope: envelope
  } do
    receipt = Receipt.accepted(envelope.id)
    existing = Error.not_sent(:routing, :existing)

    assert {:ok, ^receipt} =
             Network.deliver({NetworkFixture, reply: {:ok, receipt}}, envelope)

    assert {:error, ^existing} =
             Network.deliver(NetworkFixture, envelope, reply: {:error, existing})

    assert {:error, %Error{reason: :network_failure, outcome: :outcome_unknown}} =
             Network.deliver(NetworkFixture, envelope, reply: {:error, :network_failure})

    assert {:error, %Error{reason: {:invalid_network_result, :invalid}}} =
             Network.deliver(NetworkFixture, envelope, reply: :invalid)

    assert {:error, %Error{reason: {:network_exception, %RuntimeError{}}}} =
             Network.deliver(NetworkFixture, envelope, reply: :raise)

    assert {:error, %Error{reason: {:network_exit, :throw, :network_failure}}} =
             Network.deliver(NetworkFixture, envelope, reply: :throw)

    assert {:ok, ^receipt} = Network.deliver(fn _envelope, _opts -> {:ok, receipt} end, envelope)

    assert {:error, %Error{reason: {:network_exception, %RuntimeError{}}}} =
             Network.deliver(fn _envelope, _opts -> raise "function network" end, envelope)

    assert {:error, %Error{reason: {:invalid_network, Protocol}}} =
             Network.deliver(Protocol, envelope)

    assert {:error, %Error{reason: {:invalid_network, %{invalid: true}}}} =
             Network.deliver(%{invalid: true}, envelope)
  end

  test "network probes normalize all callback results" do
    address = "spectre://contracts/receiver"
    reachable = Reachability.new(:reachable)
    existing = Error.not_sent(:routing, :existing)

    assert {:ok, ^reachable} =
             Network.probe({NetworkFixture, reply: {:ok, reachable}}, address)

    assert {:error, ^existing} =
             Network.probe(NetworkFixture, address, reply: {:error, existing})

    assert {:error, %Error{reason: :probe_failure}} =
             Network.probe(NetworkFixture, address, reply: {:error, :probe_failure})

    assert {:error, %Error{reason: {:invalid_network_probe_result, :invalid}}} =
             Network.probe(NetworkFixture, address, reply: :invalid)

    assert {:error, %Error{reason: {:network_exception, %RuntimeError{}}}} =
             Network.probe(NetworkFixture, address, reply: :raise)

    assert {:error, %Error{reason: {:network_exit, :throw, :network_failure}}} =
             Network.probe(NetworkFixture, address, reply: :throw)

    assert {:ok, %Reachability{reason: :probe_not_supported}} =
             Network.probe(Protocol, address)

    assert {:ok, %Reachability{reason: :invalid_network}} =
             Network.probe(%{}, address)
  end

  test "default routed network handles no-route, exhaustion and probes", %{
    envelope: envelope,
    route: route
  } do
    assert {:error, %Error{reason: {:no_route, address}}} =
             Network.deliver(Routed, envelope, routes: [])

    assert address == envelope.to

    not_sent = Error.not_sent(:transport, :closed)

    assert {:error, %Error{reason: {:all_routes_not_sent, [_error]}}} =
             Network.deliver(nil, envelope, routes: [route], reply: {:error, not_sent})

    assert {:ok, %Reachability{reason: :no_route}} =
             Network.probe(nil, envelope.to, routes: [:invalid])

    assert {:ok, %Reachability{status: :reachable}} =
             Network.probe(Routed, envelope.to,
               routes: [route],
               reply: {:ok, Reachability.new(:reachable)}
             )
  end
end

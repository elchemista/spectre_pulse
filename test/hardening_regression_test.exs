defmodule Spectre.Pulse.HardeningRegressionTest.CodecAdapter do
  @behaviour Spectre.Pulse.Codec

  alias Spectre.Pulse.Error

  @impl true
  def encode(envelope, opts), do: reply(Keyword.fetch!(opts, :mode), envelope, opts)

  @impl true
  def decode(_encoded, opts), do: reply(Keyword.fetch!(opts, :mode), opts[:envelope], opts)

  defp reply(:binary, _envelope, _opts), do: {:ok, "encoded"}
  defp reply(:iodata, _envelope, _opts), do: {:ok, ["en", "coded"]}
  defp reply(:invalid_iodata, _envelope, _opts), do: {:ok, [:invalid]}
  defp reply(:envelope, envelope, _opts), do: {:ok, envelope}
  defp reply(:value, _envelope, _opts), do: {:ok, :value}
  defp reply(:error, _envelope, _opts), do: {:error, :codec_failed}

  defp reply(:typed_error, _envelope, _opts),
    do: {:error, Error.not_sent(:codec, :typed_failure)}

  defp reply(:contextual_error, _envelope, _opts),
    do: {:error, Error.not_sent(:codec, :contextual_failure, message_id: "existing")}

  defp reply(:invalid_result, _envelope, _opts), do: :invalid
  defp reply(:raise, _envelope, _opts), do: raise("codec crash")
  defp reply(:throw, _envelope, _opts), do: throw(:codec_throw)
end

defmodule Spectre.Pulse.HardeningRegressionTest.TransportAdapter do
  @behaviour Spectre.Pulse.Transport

  @impl true
  def deliver(_route, _envelope, opts), do: Keyword.fetch!(opts, :reply)

  @impl true
  def probe(_route, opts), do: Keyword.fetch!(opts, :reply)
end

defmodule Spectre.Pulse.HardeningRegressionTest.ReqAdapter do
  @response_key {__MODULE__, :response}

  def respond(response) do
    Process.put(@response_key, response)
    __MODULE__
  end

  def run(request), do: {request, Process.delete(@response_key)}
end

defmodule Spectre.Pulse.HardeningRegressionTest do
  use ExUnit.Case, async: false

  alias Spectre.Pulse.Codec
  alias Spectre.Pulse.Codec.JSON
  alias Spectre.Pulse.DSL
  alias Spectre.Pulse.EffectExecutor
  alias Spectre.Pulse.Endpoint
  alias Spectre.Pulse.Envelope
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Executor
  alias Spectre.Pulse.Expectation
  alias Spectre.Pulse.Extension
  alias Spectre.Pulse.Handler
  alias Spectre.Pulse.Inbound.Result, as: InboundResult
  alias Spectre.Pulse.InboundContext
  alias Spectre.Pulse.Local
  alias Spectre.Pulse.Network
  alias Spectre.Pulse.Network.Routed
  alias Spectre.Pulse.Options
  alias Spectre.Pulse.Protocol
  alias Spectre.Pulse.Reachability
  alias Spectre.Pulse.Receipt
  alias Spectre.Pulse.Route
  alias Spectre.Pulse.Stack, as: PulseStack
  alias Spectre.Pulse.Transport
  alias Spectre.Pulse.Transports.Node
  alias Spectre.Pulse.Transports.PubSub
  alias Spectre.Pulse.Transports.REST

  alias __MODULE__.CodecAdapter
  alias __MODULE__.ReqAdapter
  alias __MODULE__.TransportAdapter

  setup do
    envelope =
      Envelope.new!(
        from: "spectre://hardening/sender",
        to: "spectre://hardening/receiver",
        act: :request,
        payload: %{type: "hardening.perform", data: %{value: 1}}
      )

    route =
      Route.new!(
        id: "hardening-route",
        address: envelope.to,
        transport: TransportAdapter,
        target: self()
      )

    %{envelope: envelope, route: route}
  end

  test "codec boundaries normalize every adapter result and exception", %{envelope: envelope} do
    assert {:ok, "encoded"} = Codec.encode(CodecAdapter, envelope, mode: :binary)
    assert {:ok, ["en", "coded"]} = Codec.encode(CodecAdapter, envelope, mode: :iodata)
    assert {:ok, ^envelope} = Codec.encode(CodecAdapter, envelope, mode: :envelope)

    assert {:error, %Error{reason: {:invalid_encoded_value, [:invalid]}}} =
             Codec.encode(CodecAdapter, envelope, mode: :invalid_iodata)

    assert {:error, %Error{reason: :codec_failed}} =
             Codec.encode(CodecAdapter, envelope, mode: :error)

    assert {:error, %Error{reason: :typed_failure, message_id: message_id}} =
             Codec.encode(CodecAdapter, envelope, mode: :typed_error)

    assert message_id == envelope.id

    assert {:error, %Error{reason: :contextual_failure, message_id: "existing"}} =
             Codec.encode(CodecAdapter, envelope, mode: :contextual_error)

    assert {:error, %Error{reason: {:invalid_codec_result, :invalid}}} =
             Codec.encode(CodecAdapter, envelope, mode: :invalid_result)

    assert {:error, %Error{reason: {:codec_exception, %RuntimeError{}}}} =
             Codec.encode(CodecAdapter, envelope, mode: :raise)

    assert {:error, %Error{reason: {:codec_exit, :throw, :codec_throw}}} =
             Codec.encode(CodecAdapter, envelope, mode: :throw)

    assert {:error, %Error{reason: {:invalid_codec_input, CodecAdapter, :invalid}}} =
             Codec.encode(CodecAdapter, :invalid)

    assert {:error, %Error{reason: {:invalid_codec_input, nil, ^envelope}}} =
             Codec.encode(nil, envelope)

    for mode <- [:envelope, :value, :error, :typed_error, :invalid_result, :raise, :throw] do
      result = Codec.decode(CodecAdapter, :encoded, mode: mode, envelope: envelope)

      case mode do
        :envelope ->
          assert result == {:ok, envelope}

        :value ->
          assert match?({:error, %Error{reason: {:decoded_value_not_envelope, :value}}}, result)

        :error ->
          assert match?({:error, %Error{reason: :codec_failed}}, result)

        :typed_error ->
          assert match?({:error, %Error{reason: :typed_failure}}, result)

        :invalid_result ->
          assert match?({:error, %Error{reason: {:invalid_codec_result, :invalid}}}, result)

        :raise ->
          assert match?({:error, %Error{reason: {:codec_exception, %RuntimeError{}}}}, result)

        :throw ->
          assert match?({:error, %Error{reason: {:codec_exit, :throw, :codec_throw}}}, result)
      end
    end

    assert {:error, %Error{reason: {:invalid_codec, nil}}} = Codec.decode(nil, "encoded")

    unencodable = %{envelope | payload: %{envelope.payload | data: self()}}

    assert {:error, %Error{reason: {:json_encode_failed, _reason}}} =
             JSON.encode(unencodable, [])

    assert {:error, %Error{reason: {:invalid_options, :invalid}}} =
             JSON.encode(envelope, :invalid)

    assert {:error, %Error{reason: {:invalid_options, :invalid}}} =
             JSON.decode("{}", :invalid)
  end

  test "value constructors reject every malformed defensive option", %{envelope: envelope} do
    assert {:error, {:invalid_limit, :limit, 0}} = Options.positive_integer([], :limit, 0)

    assert {:error, {:invalid_option, :ttl, -1}} =
             Options.non_negative_integer([ttl: -1], :ttl, 0)

    assert_raise ArgumentError, fn -> Reachability.new(:invalid) end
    assert_raise ArgumentError, fn -> Reachability.new(:reachable, valid_for_ms: -1) end
    assert_raise ArgumentError, fn -> Reachability.unknown(nil, :invalid) end
    assert_raise ArgumentError, fn -> Reachability.new(:reachable, level: :invalid) end
    assert_raise ArgumentError, fn -> Reachability.new(:reachable, observed_at: :invalid) end
    assert_raise ArgumentError, fn -> Reachability.new(:reachable, metadata: :invalid) end

    id = Spectre.Identity.uuid7()
    assert_raise ArgumentError, fn -> Expectation.new("invalid", :contact) end
    assert_raise ArgumentError, fn -> Expectation.new(id, :contact, :reply, :invalid) end
    assert_raise ArgumentError, fn -> Expectation.new(id, :contact, :invalid) end
    assert_raise ArgumentError, fn -> Expectation.new(id, :contact, "INVALID") end

    assert_raise ArgumentError, fn ->
      Expectation.new(id, :contact, :reply, opened_at: :invalid)
    end

    assert_raise ArgumentError, fn -> Expectation.new(id, :contact, :reply, due_at: :invalid) end

    assert_raise ArgumentError, fn ->
      Expectation.new(id, :contact, :reply, metadata: :invalid)
    end

    typed = Expectation.new(id, :contact, "hardening.reply")
    refute Expectation.matches?(typed, %{envelope | relates_to: id, payload: :invalid})

    assert {:error, %Error{}} = InboundContext.normalize([:invalid])
    assert {:error, %Error{}} = InboundContext.normalize(:invalid)
    assert {:error, %Error{}} = InboundContext.normalize(%{authenticated_identity: "invalid"})
    assert {:error, %Error{}} = InboundContext.normalize(%{binding: 1})
    assert {:error, %Error{}} = InboundContext.normalize(%{instance_registry: "registry"})
    assert {:error, %Error{}} = InboundContext.normalize(%{instance_opts: :invalid})
    assert {:error, %Error{}} = InboundContext.normalize(%{instance_opts: [:invalid]})

    assert_raise ArgumentError, fn -> Protocol.limits(:invalid) end
    assert_raise ArgumentError, fn -> Protocol.limits([:invalid]) end
    assert_raise ArgumentError, fn -> Protocol.limits(%{unknown: 1}) end
    assert_raise ArgumentError, fn -> Protocol.limits(max_address_bytes: 0) end

    assert {:error, {:invalid_pulse_handler_context, :invalid, :invalid}} =
             Handler.stage(:invalid, :invalid)
  end

  test "network, endpoint and transport dispatchers retain typed failures", %{
    envelope: envelope,
    route: route
  } do
    assert {:error, %Error{reason: {:invalid_options, :invalid}}} =
             Transport.dispatch(route, envelope, :invalid)

    assert {:error, %Error{reason: {:invalid_transport_dispatch, :invalid, :invalid}}} =
             Transport.dispatch(:invalid, :invalid)

    bad_receipt = %Receipt{
      message_id: "invalid",
      status: :accepted,
      accepted_at: DateTime.utc_now()
    }

    assert {:error, %Error{reason: {:invalid_receipt_message_id, "invalid"}}} =
             Transport.dispatch(route, envelope, reply: {:ok, bad_receipt})

    invalid_reachability = %Reachability{
      status: :invalid,
      level: :route_known,
      observed_at: DateTime.utc_now(),
      valid_for_ms: 0
    }

    assert {:error, %Error{reason: {:invalid_reachability, ^invalid_reachability}}} =
             Transport.probe(route, reply: {:ok, invalid_reachability})

    assert {:error, %Error{reason: {:invalid_options, :invalid}}} =
             Transport.probe(route, :invalid)

    assert {:error, %Error{reason: {:invalid_network, {String, [:invalid]}}}} =
             Network.deliver({String, [:invalid]}, envelope)

    mismatched = Receipt.accepted(Spectre.Identity.uuid7())

    assert {:error, %Error{reason: :receipt_message_mismatch}} =
             Network.deliver(fn _envelope, _opts -> {:ok, mismatched} end, envelope)

    assert {:error, %Error{reason: {:invalid_receipt_message_id, "invalid"}}} =
             Network.deliver(fn _envelope, _opts -> {:ok, bad_receipt} end, envelope)

    assert {:error, %Error{reason: {:network_exit, :throw, :network_throw}}} =
             Network.deliver(fn _envelope, _opts -> throw(:network_throw) end, envelope)

    reachable = Reachability.new(:reachable)
    existing = Error.not_sent(:routing, :existing)

    probe_replies = [
      {:ok, reachable},
      {:error, existing},
      {:error, :probe_failed},
      :invalid
    ]

    for reply <- probe_replies do
      assert match?(
               {tag, _value} when tag in [:ok, :error],
               Network.probe(fn _, _ -> reply end, envelope.to)
             )
    end

    assert {:error, %Error{reason: {:network_exception, %RuntimeError{}}}} =
             Network.probe(fn _, _ -> raise "probe crash" end, envelope.to)

    assert {:error, %Error{reason: {:network_exit, :throw, :probe_throw}}} =
             Network.probe(fn _, _ -> throw(:probe_throw) end, envelope.to)

    assert {:error, %Error{reason: {:invalid_routes, :invalid}}} =
             Routed.deliver(envelope, routes: :invalid)

    assert {:error, %Error{reason: {:invalid_routes, :invalid}}} =
             Routed.probe(envelope.to, routes: :invalid)

    assert {:ok, %Reachability{reason: :no_route}} = Routed.probe(envelope.to, routes: [])

    assert {:error, %Error{reason: {:invalid_options, :invalid}}} =
             Endpoint.accept(fn _, _ -> :ok end, envelope, %{}, :invalid)

    assert {:error, %Error{reason: :receipt_message_mismatch}} =
             Endpoint.accept(fn _, _ -> {:ok, mismatched} end, envelope, %{})

    invalid_result = %InboundResult{receipt: bad_receipt}

    assert {:error, %Error{reason: {:invalid_receipt_message_id, "invalid"}}} =
             Endpoint.accept(fn _, _ -> invalid_result end, envelope, %{})

    assert {:error, %Error{reason: {:invalid_endpoint_receipt, :invalid}}} =
             Endpoint.accept(fn _, _ -> %InboundResult{receipt: :invalid} end, envelope, %{})
  end

  test "transport adapters reject malformed host and remote data", %{envelope: envelope} do
    pub_sub_route =
      Route.pub_sub(envelope.to, fn _message -> {:error, Error.not_sent(:transport, :closed)} end)

    assert {:error, %Error{reason: :closed, message_id: message_id}} =
             PubSub.deliver(pub_sub_route, envelope, [])

    assert message_id == envelope.id

    assert {:error, %Error{reason: {:pub_sub_exit, :throw, :broker_throw}}} =
             PubSub.deliver(
               %{pub_sub_route | target: fn _ -> throw(:broker_throw) end},
               envelope,
               []
             )

    assert {:error, %Error{reason: {:invalid_options, [:invalid]}}} =
             PubSub.handle_message({:spectre_pulse, envelope}, nil, %{}, [:invalid])

    assert {:error, %Error{reason: {:invalid_inbound_context, :invalid}}} =
             PubSub.handle_message({:spectre_pulse, envelope}, nil, :invalid, [])

    node_route = Route.node(envelope.to, node(), fn _, _ -> :ok end)
    assert {:ok, %Receipt{}} = Node.deliver(node_route, envelope, timeout: :infinity)

    assert {:error, %Error{reason: {:invalid_peer_node, "node"}}} =
             Node.accept_remote(nil, envelope, "node", [])

    assert {:error, %Error{reason: {:invalid_options, :invalid}}} =
             Node.accept_remote(nil, envelope, node(), :invalid)

    assert {:error, %Error{reason: {:invalid_options, :invalid}}} =
             Node.deliver(%{node_route | metadata: %{inbound_opts: :invalid}}, envelope, [])

    rest_route = Route.rest(envelope.to, "http://adapter.test/messages", id: "coverage-rest")

    assert {:error, %Error{reason: {:invalid_rest_delivery, :invalid, :invalid}}} =
             REST.deliver(:invalid, :invalid, [])

    assert {:error, %Error{reason: {:invalid_rest_url, <<255>>}}} =
             REST.deliver(%{rest_route | target: <<255>>}, envelope, [])

    assert {:error, %Error{reason: {:invalid_http_url, ":"}}} =
             REST.deliver(%{rest_route | target: "http://[x]/messages"}, envelope, [])

    assert {:error, %Error{reason: {:invalid_rest_timeout, 0}}} =
             REST.deliver(rest_route, envelope, timeout: 0)

    assert {:error, %Error{reason: {:invalid_headers, :invalid}}} =
             REST.deliver(rest_route, envelope, headers: :invalid)

    assert {:error, %Error{reason: {:invalid_header_name, 1}}} =
             REST.deliver(rest_route, envelope, headers: %{1 => "value"})

    assert {:error, %Error{reason: {:invalid_header_value, 1}}} =
             REST.deliver(rest_route, envelope, headers: %{"x-test" => 1})

    assert {:ok, %Receipt{}} =
             REST.deliver(rest_route, envelope,
               headers: [x_test: "value"],
               req_options: [adapter: ReqAdapter.respond(%Req.Response{status: 202, body: ""})]
             )

    assert {:error, %Error{reason: {:invalid_http_status, :invalid}}} =
             REST.deliver(rest_route, envelope,
               req_options: [
                 adapter: ReqAdapter.respond(%Req.Response{status: :invalid, body: ""})
               ]
             )

    assert {:ok, %Reachability{reason: :invalid_rest_options}} =
             REST.probe(rest_route, :invalid)

    assert {:ok, %Reachability{reason: :invalid_rest_route}} = REST.probe(:invalid, [])
    assert REST.handle_request(:invalid, %{}, :peer).status == 400

    malformed_error = %Error{kind: "invalid", reason: :rejected, outcome: :not_sent}

    response =
      REST.handle_request(Jason.encode!(Envelope.to_wire(envelope)), %{}, :peer,
        authenticator: fn _, _ -> {:error, malformed_error} end
      )

    assert Jason.decode!(response.body)["error"] == "inbound"

    thrown =
      REST.handle_request(Jason.encode!(Envelope.to_wire(envelope)), %{}, :peer,
        authenticator: fn _, _ -> throw(:authentication_throw) end
      )

    assert thrown.status == 401
  end

  test "DSL, Stack and execution facades fail before side effects" do
    assert_raise ArgumentError, fn -> DSL.install!(String, [:invalid]) end
    assert_raise ArgumentError, fn -> DSL.install!(String, :invalid) end
    assert_raise ArgumentError, fn -> DSL.install!(String, []) end

    assert {:ok, %{directory: nil, transports: []}} = PulseStack.compile([], nil, __ENV__)

    invalid_transport = quote do: transport("invalid", String)

    assert {:error, {:invalid_pulse_transport, "invalid", String}} =
             PulseStack.compile([], invalid_transport, __ENV__)

    invalid_directory = quote do: directory("invalid")

    assert {:error, {:invalid_pulse_directory, "invalid"}} =
             PulseStack.compile([], invalid_directory, __ENV__)

    duplicate_directory =
      quote do
        directory(String)
        directory(Enum)
      end

    assert {:error, :duplicate_pulse_directory} =
             PulseStack.compile([], duplicate_directory, __ENV__)

    assert {:error, {:invalid_pulse_stack_config, :invalid}} =
             Extension.compile(__MODULE__, stack_config: :invalid)

    assert {:error, {:invalid_pulse_directory, "invalid"}} =
             Extension.compile(__MODULE__, stack_config: %{directory: "invalid"})

    assert {:error, {:invalid_pulse_transport, %{id: :invalid}}} =
             Extension.compile(__MODULE__, stack_config: %{transports: [%{id: :invalid}]})

    assert {:error, {:invalid_pulse_result, :invalid}} = Executor.execute(String, :invalid)
    assert {:error, {:invalid_pulse_turn, :invalid}} = Executor.execute_turn(:invalid)

    assert {:error, {:invalid_pulse_execution, :invalid, String}} =
             Executor.execute_pending(:invalid, String)

    effect = %Spectre.Effect{id: Spectre.Identity.uuid7(), kind: :pulse}

    assert {:error, %Error{reason: {:invalid_pulse_effect, ^effect}, message_id: effect_id}} =
             Executor.deliver(String, effect, :invalid)

    assert effect_id == effect.id

    assert {:error, %Error{reason: {:invalid_pulse_effect, :invalid}, message_id: nil}} =
             Executor.deliver(String, :invalid, :invalid)

    assert {:error, %Error{reason: {:invalid_pulse_execution_context, effect_id}}} =
             EffectExecutor.execute(effect, %Spectre.Context{}, [])

    assert effect_id == effect.id

    assert {:error, %Error{reason: {:invalid_pulse_agent, nil}}} = Local.subscribe(nil)
    assert {:error, %Error{reason: {:invalid_pulse_agent, nil}}} = Local.subscription(nil, [])
  end
end

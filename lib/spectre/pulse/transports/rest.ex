defmodule Spectre.Pulse.Transports.REST do
  @moduledoc """
  HTTP/JSON Pulse binding.

  Outbound delivery posts the common envelope to a route URL. The server-side
  `handle_request/4` function is framework-neutral and returns a small response
  struct which Plug, Phoenix, Bandit, or another host can write to the socket.
  """

  @behaviour Spectre.Pulse.Transport

  alias Spectre.Pulse.Address
  alias Spectre.Pulse.Codec.JSON
  alias Spectre.Pulse.Endpoint
  alias Spectre.Pulse.Envelope
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Options
  alias Spectre.Pulse.Protocol
  alias Spectre.Pulse.Reachability
  alias Spectre.Pulse.Receipt
  alias Spectre.Pulse.Route
  alias Spectre.Pulse.Transports.REST.Response

  @protected_request_options [
    :url,
    :method,
    :body,
    :headers,
    :receive_timeout,
    :redirect,
    :redirect_trusted,
    :max_redirects,
    :retry,
    :into,
    :decode_body,
    :request_steps,
    :response_steps,
    :error_steps
  ]
  @reserved_request_headers ~w(
    connection
    content-length
    host
    keep-alive
    proxy-authenticate
    proxy-authorization
    te
    trailer
    transfer-encoding
    upgrade
  )
  @body_chunks_key :spectre_pulse_rest_body_chunks
  @body_bytes_key :spectre_pulse_rest_body_bytes
  @body_too_large_key :spectre_pulse_rest_body_too_large
  @invalid_body_key :spectre_pulse_rest_invalid_body

  @doc false
  @spec deliver(Route.t(), Envelope.t(), keyword()) ::
          {:ok, Receipt.t()} | {:error, Error.t()}
  @impl Spectre.Pulse.Transport
  def deliver(%Route{target: url} = route, %Envelope{} = envelope, opts)
      when is_binary(url) do
    with {:ok, opts} <- Options.keyword(opts),
         {:ok, route} <- Route.new(route),
         {:ok, envelope} <- Envelope.new(envelope, opts),
         :ok <- validate_url(url),
         {:ok, body} <- JSON.encode(envelope, opts),
         {:ok, headers} <- request_headers(route, opts),
         {:ok, timeout} <- request_timeout(opts, route, 10_000),
         {:ok, max_response_bytes} <- response_limit(opts),
         {:ok, request_opts} <- request_options(route, opts) do
      request_opts =
        request_opts
        |> Keyword.put(:url, url)
        |> Keyword.put(:method, :post)
        |> Keyword.put(:body, body)
        |> Keyword.put(:headers, headers)
        |> Keyword.put(:receive_timeout, timeout)
        |> Keyword.put(:redirect, false)
        |> Keyword.put(:redirect_trusted, false)
        |> Keyword.put(:retry, false)
        |> Keyword.put(:decode_body, false)
        |> Keyword.put(:into, limited_body(max_response_bytes))

      request_opts
      |> Req.request()
      |> normalize_delivery_request(max_response_bytes, envelope, route)
    else
      {:error, %Error{} = error} ->
        {:error, put_route_context(error, envelope, route)}

      {:error, reason} ->
        {:error, Error.not_sent(:validation, reason, message_id: envelope.id, route_id: route.id)}
    end
  rescue
    exception ->
      {:error,
       Error.outcome_unknown(:transport, {:rest_exception, exception},
         message_id: envelope.id,
         route_id: route.id,
         cause: exception
       )}
  catch
    kind, reason ->
      {:error,
       Error.outcome_unknown(:transport, {:rest_exit, kind, reason},
         message_id: envelope.id,
         route_id: route.id
       )}
  end

  def deliver(%Route{} = route, %Envelope{} = envelope, _opts) do
    {:error,
     Error.not_sent(:routing, {:invalid_rest_url, route.target},
       message_id: envelope.id,
       route_id: route.id
     )}
  end

  def deliver(route, envelope, _opts) do
    {:error,
     Error.not_sent(:validation, {:invalid_rest_delivery, route, envelope},
       message_id: message_id(envelope),
       route_id: route_id(route)
     )}
  end

  @doc false
  @spec probe(Route.t(), keyword()) :: {:ok, Reachability.t()}
  @impl Spectre.Pulse.Transport
  def probe(%Route{target: url} = route, opts) when is_binary(url) do
    with {:ok, opts} <- Options.keyword(opts),
         {:ok, route} <- Route.new(route),
         :ok <- validate_url(url),
         {:ok, timeout} <- request_timeout(opts, route, 3_000),
         {:ok, valid_for_ms} <- Options.non_negative_integer(opts, :valid_for_ms, 5_000),
         {:ok, request_opts} <- request_options(route, opts) do
      request_opts =
        request_opts
        |> Keyword.put(:url, url)
        |> Keyword.put(:method, :head)
        |> Keyword.put(:receive_timeout, timeout)
        |> Keyword.put(:redirect, false)
        |> Keyword.put(:redirect_trusted, false)
        |> Keyword.put(:retry, false)
        |> Keyword.put(:decode_body, false)
        |> Keyword.put(:into, discard_body())

      probe_request(request_opts, route, valid_for_ms)
    else
      {:error, _reason} ->
        {:ok,
         Reachability.unknown(:invalid_rest_options,
           via: :rest,
           metadata: %{route_id: route.id}
         )}
    end
  end

  def probe(%Route{} = route, _opts) do
    {:ok, Reachability.unknown(:invalid_rest_url, via: :rest, metadata: %{route_id: route.id})}
  end

  def probe(route, _opts) do
    {:ok,
     Reachability.unknown(:invalid_rest_route, via: :rest, metadata: %{route_id: route_id(route)})}
  end

  @doc """
  Handles a server request after the host has collected its body and headers.

  `authenticator` must bind request credentials to a canonical agent identity
  and return `{:ok, identity}` or `{:ok, identity, verified_facts}`.
  """
  @spec handle_request(term(), term(), term(), term()) :: Response.t()
  def handle_request(body, headers, peer, opts \\ [])

  def handle_request(body, headers, peer, opts) when is_binary(body) do
    with {:ok, opts} <- Options.keyword(opts),
         {:ok, headers} <- normalize_headers(headers),
         {:ok, envelope} <- JSON.decode(body, opts),
         {:ok, identity, verified} <- authenticate(headers, peer, opts),
         context <-
           %{
             authenticated_identity: identity,
             binding: :rest,
             peer: peer,
             target: Keyword.get(opts, :target),
             target_identity: Keyword.get(opts, :target_identity),
             resolver: Keyword.get(opts, :target_resolver),
             authorization: Keyword.get(opts, :authorize),
             verified: verified
           },
         {:ok, receipt} <-
           Endpoint.accept(
             Keyword.get(opts, :target),
             envelope,
             context,
             Keyword.put(opts, :via, :rest)
           ),
         {:ok, encoded} <- encode_receipt(receipt) do
      %Response{status: 202, body: encoded}
    else
      {:error, %Error{} = error} -> error_response(error)
      {:error, reason} -> error_response(Error.not_sent(:validation, reason))
    end
  rescue
    exception ->
      error_response(Error.not_sent(:inbound, {:rest_inbound_exception, exception}))
  catch
    kind, reason ->
      error_response(Error.not_sent(:inbound, {:rest_inbound_exit, kind, reason}))
  end

  def handle_request(body, _headers, _peer, _opts),
    do: error_response(Error.not_sent(:codec, {:invalid_request_body, body}))

  @spec response_result(Req.Response.t(), Envelope.t(), Route.t()) ::
          {:ok, Receipt.t()} | {:error, Error.t()}
  defp response_result(%Req.Response{status: status} = response, envelope, route),
    do: response_status_result(status, response, envelope, route)

  @spec response_status_result(term(), Req.Response.t(), Envelope.t(), Route.t()) ::
          {:ok, Receipt.t()} | {:error, Error.t()}
  @dialyzer {:nowarn_function, response_status_result: 4}
  defp response_status_result(status, response, envelope, route)
       when is_integer(status) and status in 200..299 do
    case receipt_from_body(response.body, envelope, route) do
      {:ok, receipt} -> {:ok, receipt}
      {:error, error} -> {:error, error}
    end
  end

  defp response_status_result(status, _response, envelope, route)
       when is_integer(status) and status in 400..499 do
    {:error,
     Error.not_sent(:transport, {:http_rejected, status},
       message_id: envelope.id,
       route_id: route.id,
       details: %{status: status}
     )}
  end

  defp response_status_result(status, _response, envelope, route) when is_integer(status) do
    {:error,
     Error.outcome_unknown(:transport, {:http_failure, status},
       message_id: envelope.id,
       route_id: route.id,
       details: %{status: status}
     )}
  end

  defp response_status_result(status, _response, envelope, route) do
    {:error,
     Error.outcome_unknown(:transport, {:invalid_http_status, status},
       message_id: envelope.id,
       route_id: route.id
     )}
  end

  @spec receipt_from_body(term(), Envelope.t(), Route.t()) ::
          {:ok, Receipt.t()} | {:error, Error.t()}
  defp receipt_from_body(body, envelope, route) when body in ["", nil] do
    {:ok, Receipt.accepted(envelope.id, via: :rest, route_id: route.id)}
  end

  defp receipt_from_body(body, envelope, route) when is_binary(body) do
    case Jason.decode(body) do
      {:ok, map} ->
        receipt_from_body(map, envelope, route)

      {:error, reason} ->
        {:error,
         Error.outcome_unknown(:transport, {:invalid_receipt_json, reason},
           message_id: envelope.id,
           route_id: route.id
         )}
    end
  end

  defp receipt_from_body(body, envelope, route) when is_map(body) do
    case Receipt.new(body) do
      {:ok, receipt} when receipt.message_id == envelope.id ->
        {:ok,
         %{
           receipt
           | via: :rest,
             route_id: route.id,
             metadata:
               Map.merge(receipt.metadata, %{
                 remote_via: receipt.via,
                 remote_route_id: receipt.route_id
               })
         }}

      {:ok, receipt} ->
        {:error,
         Error.outcome_unknown(:transport, :receipt_message_mismatch,
           message_id: envelope.id,
           route_id: route.id,
           details: %{receipt_message_id: receipt.message_id}
         )}

      {:error, error} ->
        {:error, %{error | outcome: :outcome_unknown, route_id: route.id}}
    end
  end

  defp receipt_from_body(body, envelope, route) do
    {:error,
     Error.outcome_unknown(:transport, {:invalid_receipt_body, body},
       message_id: envelope.id,
       route_id: route.id
     )}
  end

  @spec request_error(term(), Envelope.t(), Route.t()) :: {:error, Error.t()}
  defp request_error(exception, envelope, route) do
    reason = request_reason(exception)

    if definitely_not_sent?(reason) do
      {:error, Error.not_sent(:transport, reason, message_id: envelope.id, route_id: route.id)}
    else
      {:error,
       Error.outcome_unknown(:transport, reason,
         message_id: envelope.id,
         route_id: route.id
       )}
    end
  end

  @spec normalize_delivery_request(term(), pos_integer(), Envelope.t(), Route.t()) ::
          {:ok, Receipt.t()} | {:error, Error.t()}
  @dialyzer {:nowarn_function, normalize_delivery_request: 4}
  defp normalize_delivery_request(
         {:ok, %Req.Response{} = response},
         max_response_bytes,
         envelope,
         route
       ) do
    case finalize_response(response, max_response_bytes, envelope, route) do
      {:ok, response} -> response_result(response, envelope, route)
      {:error, %Error{} = error} -> {:error, error}
    end
  end

  defp normalize_delivery_request({:ok, response}, _max_response_bytes, envelope, route),
    do: request_error({:invalid_http_response, response}, envelope, route)

  defp normalize_delivery_request({:error, reason}, _max_response_bytes, envelope, route),
    do: request_error(reason, envelope, route)

  defp normalize_delivery_request(result, _max_response_bytes, envelope, route),
    do: request_error({:invalid_http_result, result}, envelope, route)

  @spec request_reason(term()) :: term()
  defp request_reason(%{reason: reason}), do: reason
  defp request_reason(reason), do: reason

  @spec definitely_not_sent?(term()) :: boolean()
  defp definitely_not_sent?(reason),
    do: reason in [:econnrefused, :nxdomain, :enetunreach, :ehostunreach, :closed]

  @spec authenticate(map(), term(), keyword()) ::
          {:ok, String.t() | nil, map()} | {:error, Error.t()}
  defp authenticate(headers, peer, opts) do
    with {:ok, allow_unauthenticated?} <- allow_unauthenticated(opts) do
      opts
      |> Keyword.get(:authenticator)
      |> invoke_authenticator(headers, peer, opts, allow_unauthenticated?)
      |> normalize_authenticator_result(allow_unauthenticated?)
    end
  rescue
    exception -> {:error, Error.not_sent(:authentication, {:authenticator_exception, exception})}
  catch
    kind, reason ->
      {:error, Error.not_sent(:authentication, {:authenticator_exit, kind, reason})}
  end

  @spec invoke_authenticator(term(), map(), term(), keyword(), boolean()) :: term()
  defp invoke_authenticator(authenticator, headers, peer, _opts, _allow_unauthenticated?)
       when is_function(authenticator, 2),
       do: authenticator.(headers, peer)

  defp invoke_authenticator(authenticator, headers, peer, opts, _allow_unauthenticated?)
       when is_function(authenticator, 3),
       do: authenticator.(headers, peer, opts)

  defp invoke_authenticator(nil, _headers, _peer, _opts, allow_unauthenticated?) do
    if allow_unauthenticated? do
      {:ok, nil, %{}}
    else
      {:error, Error.not_sent(:authentication, :rest_authenticator_required)}
    end
  end

  defp invoke_authenticator(_authenticator, _headers, _peer, _opts, _allow_unauthenticated?),
    do: {:error, Error.not_sent(:authentication, :rest_authenticator_required)}

  @spec normalize_authenticator_result(term(), boolean()) ::
          {:ok, String.t() | nil, map()} | {:error, Error.t()}
  defp normalize_authenticator_result({:ok, identity}, _allow_unauthenticated?)
       when is_binary(identity),
       do: normalize_authenticated_identity(identity, %{})

  defp normalize_authenticator_result({:ok, identity, verified}, _allow_unauthenticated?)
       when is_binary(identity) and is_map(verified),
       do: normalize_authenticated_identity(identity, verified)

  defp normalize_authenticator_result({:ok, nil, verified}, true) when is_map(verified),
    do: {:ok, nil, verified}

  defp normalize_authenticator_result({:ok, nil, _verified}, false),
    do: {:error, Error.not_sent(:authentication, :authenticated_identity_required)}

  defp normalize_authenticator_result({:error, %Error{} = error}, _allow_unauthenticated?),
    do: {:error, error}

  defp normalize_authenticator_result({:error, reason}, _allow_unauthenticated?),
    do: {:error, Error.not_sent(:authentication, reason)}

  defp normalize_authenticator_result(result, _allow_unauthenticated?),
    do: {:error, Error.not_sent(:authentication, {:invalid_auth_result, result})}

  @spec allow_unauthenticated(keyword()) :: {:ok, boolean()} | {:error, Error.t()}
  defp allow_unauthenticated(opts) do
    case Keyword.get(opts, :allow_unauthenticated, false) do
      value when is_boolean(value) ->
        {:ok, value}

      value ->
        {:error, Error.not_sent(:validation, {:invalid_allow_unauthenticated_option, value})}
    end
  end

  @spec normalize_authenticated_identity(String.t(), map()) ::
          {:ok, String.t(), map()} | {:error, Error.t()}
  defp normalize_authenticated_identity(identity, verified) do
    case Address.normalize(identity) do
      {:ok, canonical} ->
        {:ok, canonical, verified}

      {:error, _error} ->
        {:error, Error.not_sent(:authentication, {:invalid_authenticated_identity, identity})}
    end
  end

  @spec error_response(Error.t()) :: Response.t()
  defp error_response(%Error{} = error) do
    status =
      case error.kind do
        :authentication -> 401
        :authorization -> 403
        :validation -> 422
        :codec -> 400
        :routing -> 404
        _ -> 503
      end

    body =
      Jason.encode!(%{
        "error" => safe_kind(error.kind),
        "reason" => safe_reason(error.reason)
      })

    %Response{status: status, body: body}
  end

  @spec safe_reason(term()) :: String.t()
  defp safe_reason(reason) when is_atom(reason), do: Atom.to_string(reason)
  defp safe_reason({reason, _details}) when is_atom(reason), do: Atom.to_string(reason)
  defp safe_reason(_reason), do: "request_rejected"

  @spec safe_kind(term()) :: String.t()
  defp safe_kind(kind) when is_atom(kind), do: Atom.to_string(kind)
  defp safe_kind(_kind), do: "inbound"

  @spec validate_url(binary()) :: :ok | {:error, term()}
  defp validate_url(url) do
    cond do
      not String.valid?(url) ->
        {:error, {:invalid_rest_url, url}}

      Regex.match?(~r/[\x00-\x20\x7F]/u, url) ->
        {:error, {:invalid_rest_url, url}}

      Regex.match?(~r/%(?![0-9A-Fa-f]{2})/, url) ->
        {:error, {:invalid_rest_url, url}}

      true ->
        validate_parsed_url(url, URI.new(url))
    end
  end

  @spec validate_parsed_url(binary(), {:ok, URI.t()} | {:error, term()}) ::
          :ok | {:error, term()}
  defp validate_parsed_url(
         _url,
         {:ok,
          %URI{
            scheme: scheme,
            host: host,
            port: port,
            userinfo: nil,
            fragment: nil
          }}
       )
       when is_binary(scheme) and is_binary(host) and host != "" and is_integer(port) and
              port > 0 and port <= 65_535 do
    if String.downcase(scheme) in ["http", "https"], do: :ok, else: {:error, :invalid_scheme}
  end

  defp validate_parsed_url(_url, {:ok, _uri}), do: {:error, :invalid_http_url}
  defp validate_parsed_url(_url, {:error, reason}), do: {:error, {:invalid_http_url, reason}}

  @spec request_headers(Route.t(), keyword()) ::
          {:ok, [{String.t(), String.t()}]} | {:error, term()}
  defp request_headers(route, opts) do
    with {:ok, route_headers} <- normalize_outbound_headers(metadata(route, :headers, [])),
         {:ok, call_headers} <- normalize_outbound_headers(Keyword.get(opts, :headers, [])) do
      headers =
        route_headers
        |> Map.merge(call_headers)
        |> Map.put("content-type", "application/json")
        |> Map.put("accept", "application/json")
        |> Enum.sort()

      {:ok, headers}
    end
  end

  @spec normalize_outbound_headers(term()) ::
          {:ok, %{optional(String.t()) => String.t()}} | {:error, term()}
  defp normalize_outbound_headers(headers), do: normalize_header_collection(headers, :outbound)

  @spec normalize_headers(term()) ::
          {:ok, %{optional(String.t()) => String.t()}} | {:error, term()}
  defp normalize_headers(headers), do: normalize_header_collection(headers, :inbound)

  @spec normalize_header_collection(term(), :inbound | :outbound) ::
          {:ok, %{optional(String.t()) => String.t()}} | {:error, term()}
  defp normalize_header_collection(headers, direction) do
    case header_pairs(headers) do
      {:ok, pairs} -> Enum.reduce_while(pairs, {:ok, %{}}, &reduce_header(&1, &2, direction))
      {:error, reason} -> {:error, reason}
    end
  end

  @spec reduce_header(term(), {:ok, map()}, :inbound | :outbound) ::
          {:cont, {:ok, map()}} | {:halt, {:error, term()}}
  defp reduce_header(entry, {:ok, normalized}, direction) do
    case put_normalized_header(entry, normalized, direction) do
      {:ok, normalized} -> {:cont, {:ok, normalized}}
      {:error, reason} -> {:halt, {:error, reason}}
    end
  end

  @spec put_normalized_header(term(), map(), :inbound | :outbound) ::
          {:ok, map()} | {:error, term()}
  defp put_normalized_header({name, value}, normalized, direction) do
    with {:ok, name} <- normalize_header_name(name),
         :ok <- allowed_header(direction, name),
         {:ok, value} <- normalize_header_value(value),
         false <- Map.has_key?(normalized, name) do
      {:ok, Map.put(normalized, name, value)}
    else
      true -> {:error, {:duplicate_header, name}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp put_normalized_header(entry, _normalized, _direction),
    do: {:error, {:invalid_header, entry}}

  @spec allowed_header(:inbound | :outbound, String.t()) :: :ok | {:error, term()}
  defp allowed_header(:inbound, _name), do: :ok
  defp allowed_header(:outbound, name), do: allowed_request_header(name)

  @spec header_pairs(term()) :: {:ok, [{term(), term()}]} | {:error, term()}
  defp header_pairs(headers) when is_map(headers), do: {:ok, Map.to_list(headers)}

  defp header_pairs(headers) when is_list(headers) do
    if Enum.all?(headers, &match?({_, _}, &1)),
      do: {:ok, headers},
      else: {:error, {:invalid_headers, headers}}
  end

  defp header_pairs(headers), do: {:error, {:invalid_headers, headers}}

  @spec normalize_header_name(term()) :: {:ok, String.t()} | {:error, term()}
  defp normalize_header_name(name) when is_atom(name) and not is_nil(name),
    do: name |> Atom.to_string() |> normalize_header_name()

  defp normalize_header_name(name) when is_binary(name) do
    if String.valid?(name) and Regex.match?(~r/^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/, name),
      do: {:ok, String.downcase(name)},
      else: {:error, {:invalid_header_name, name}}
  end

  defp normalize_header_name(name), do: {:error, {:invalid_header_name, name}}

  @spec normalize_header_value(term()) :: {:ok, String.t()} | {:error, term()}
  defp normalize_header_value(value) when is_binary(value) do
    if String.valid?(value) and not Regex.match?(~r/[\x00-\x1F\x7F]/u, value),
      do: {:ok, value},
      else: {:error, {:invalid_header_value, value}}
  end

  defp normalize_header_value(value), do: {:error, {:invalid_header_value, value}}

  @spec allowed_request_header(String.t()) :: :ok | {:error, term()}
  defp allowed_request_header(name) do
    if name in @reserved_request_headers,
      do: {:error, {:reserved_request_header, name}},
      else: :ok
  end

  @spec request_timeout(keyword(), Route.t(), pos_integer()) ::
          {:ok, pos_integer()} | {:error, term()}
  defp request_timeout(opts, route, default) do
    case Keyword.get(opts, :timeout, metadata(route, :timeout, default)) do
      timeout when is_integer(timeout) and timeout > 0 -> {:ok, timeout}
      timeout -> {:error, {:invalid_rest_timeout, timeout}}
    end
  end

  @spec response_limit(keyword()) :: {:ok, pos_integer()} | {:error, term()}
  defp response_limit(opts) do
    Options.positive_integer(
      opts,
      :max_receipt_bytes,
      Protocol.default_limits().max_receipt_bytes
    )
  end

  @spec request_options(Route.t(), keyword()) :: {:ok, keyword()} | {:error, term()}
  defp request_options(route, opts) do
    with {:ok, route_options} <- Options.keyword(metadata(route, :req_options, [])),
         {:ok, call_options} <- Options.keyword(Keyword.get(opts, :req_options, [])) do
      request_options =
        route_options
        |> Keyword.merge(call_options)
        |> Keyword.drop(@protected_request_options)

      {:ok, request_options}
    end
  end

  @spec limited_body(pos_integer()) :: function()
  defp limited_body(max_bytes) do
    fn
      {:data, data}, {request, %Req.Response{} = response} when is_binary(data) ->
        bytes = Map.get(response.private, @body_bytes_key, 0) + byte_size(data)

        response = Req.Response.put_private(response, @body_bytes_key, bytes)

        if bytes > max_bytes do
          response = Req.Response.put_private(response, @body_too_large_key, {bytes, max_bytes})
          {:halt, {request, response}}
        else
          response =
            Req.Response.update_private(response, @body_chunks_key, [data], &[data | &1])

          {:cont, {request, response}}
        end

      event, {request, %Req.Response{} = response} ->
        response = Req.Response.put_private(response, @invalid_body_key, event)
        {:halt, {request, response}}
    end
  end

  @spec discard_body() :: function()
  defp discard_body do
    fn
      {:data, _data}, {request, response} -> {:cont, {request, response}}
      _event, {request, response} -> {:halt, {request, response}}
    end
  end

  @spec finalize_response(term(), pos_integer(), Envelope.t(), Route.t()) ::
          {:ok, Req.Response.t()} | {:error, Error.t()}
  @dialyzer {:nowarn_function, finalize_response: 4}
  defp finalize_response(%Req.Response{private: private} = response, max_bytes, envelope, route)
       when is_map(private) do
    cond do
      match?({_, _}, Map.get(private, @body_too_large_key)) ->
        {bytes, limit} = Map.fetch!(private, @body_too_large_key)
        response_too_large(bytes, limit, envelope, route)

      Map.has_key?(private, @invalid_body_key) ->
        {:error,
         Error.outcome_unknown(:transport, {:invalid_response_stream, private[@invalid_body_key]},
           message_id: envelope.id,
           route_id: route.id
         )}

      Map.has_key?(private, @body_chunks_key) ->
        case join_body_chunks(private[@body_chunks_key]) do
          {:ok, body} ->
            {:ok, %{response | body: body}}

          {:error, reason} ->
            {:error,
             Error.outcome_unknown(:transport, reason,
               message_id: envelope.id,
               route_id: route.id
             )}
        end

      true ->
        enforce_response_limit(response, max_bytes, envelope, route)
    end
  end

  defp finalize_response(response, _max_bytes, envelope, route) do
    {:error,
     Error.outcome_unknown(:transport, {:invalid_http_response, response},
       message_id: envelope.id,
       route_id: route.id
     )}
  end

  @spec join_body_chunks(term()) :: {:ok, binary()} | {:error, term()}
  defp join_body_chunks(chunks) when is_list(chunks) do
    {:ok, chunks |> Enum.reverse() |> IO.iodata_to_binary()}
  rescue
    exception -> {:error, {:invalid_response_chunks, exception}}
  catch
    kind, reason -> {:error, {:invalid_response_chunks, kind, reason}}
  end

  defp join_body_chunks(chunks), do: {:error, {:invalid_response_chunks, chunks}}

  @spec enforce_response_limit(Req.Response.t(), pos_integer(), Envelope.t(), Route.t()) ::
          {:ok, Req.Response.t()} | {:error, Error.t()}
  defp enforce_response_limit(response, max_bytes, envelope, route) do
    case encoded_body_size(response.body) do
      {:ok, bytes} when bytes > max_bytes ->
        response_too_large(bytes, max_bytes, envelope, route)

      _within_limit_or_unknown ->
        {:ok, response}
    end
  end

  @spec encoded_body_size(term()) :: {:ok, non_neg_integer()} | :unknown
  defp encoded_body_size(nil), do: {:ok, 0}
  defp encoded_body_size(body) when is_binary(body), do: {:ok, byte_size(body)}

  defp encoded_body_size(body) when is_map(body) or is_list(body) do
    case Jason.encode(body) do
      {:ok, encoded} -> {:ok, byte_size(encoded)}
      {:error, _reason} -> :unknown
    end
  rescue
    _exception -> :unknown
  catch
    _kind, _reason -> :unknown
  end

  defp encoded_body_size(_body), do: :unknown

  @spec response_too_large(non_neg_integer(), pos_integer(), Envelope.t(), Route.t()) ::
          {:error, Error.t()}
  defp response_too_large(bytes, max_bytes, envelope, route) do
    {:error,
     Error.outcome_unknown(:transport, {:receipt_too_large, bytes, max_bytes},
       message_id: envelope.id,
       route_id: route.id
     )}
  end

  @spec probe_request(keyword(), Route.t(), non_neg_integer()) :: {:ok, Reachability.t()}
  defp probe_request(request_opts, route, valid_for_ms) do
    request_opts
    |> Req.request()
    |> normalize_probe_request(route, valid_for_ms)
  rescue
    exception -> unreachable(route, {:rest_probe_exception, exception})
  catch
    kind, reason -> unreachable(route, {:rest_probe_exit, kind, reason})
  end

  @spec normalize_probe_request(term(), Route.t(), non_neg_integer()) ::
          {:ok, Reachability.t()}
  @dialyzer {:nowarn_function, normalize_probe_request: 3}
  defp normalize_probe_request(
         {:ok, %Req.Response{status: status}},
         route,
         valid_for_ms
       )
       when is_integer(status) and status in 200..499 do
    {:ok,
     Reachability.new(:reachable,
       level: :pulse_endpoint,
       via: :rest,
       valid_for_ms: valid_for_ms,
       metadata: %{route_id: route.id, status: status}
     )}
  end

  defp normalize_probe_request({:ok, %Req.Response{status: status}}, route, _valid_for_ms),
    do: unreachable(route, {:http_status, status})

  defp normalize_probe_request({:ok, response}, route, _valid_for_ms),
    do: unreachable(route, {:invalid_http_response, response})

  defp normalize_probe_request({:error, reason}, route, _valid_for_ms),
    do: unreachable(route, request_reason(reason))

  defp normalize_probe_request(result, route, _valid_for_ms),
    do: unreachable(route, {:invalid_http_result, result})

  @spec unreachable(Route.t(), term()) :: {:ok, Reachability.t()}
  defp unreachable(route, reason) do
    {:ok,
     Reachability.new(:unreachable,
       level: :pulse_endpoint,
       via: :rest,
       valid_for_ms: 1_000,
       reason: reason,
       metadata: %{route_id: route.id}
     )}
  end

  @spec encode_receipt(Receipt.t()) :: {:ok, binary()} | {:error, Error.t()}
  defp encode_receipt(receipt) do
    with {:ok, receipt} <- Receipt.new(receipt),
         {:ok, encoded} <- Jason.encode(Receipt.to_wire(receipt)) do
      {:ok, encoded}
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, Error.not_sent(:inbound, {:receipt_encode_failed, reason})}
    end
  rescue
    exception ->
      {:error, Error.not_sent(:inbound, {:receipt_encode_exception, exception})}
  catch
    kind, reason ->
      {:error, Error.not_sent(:inbound, {:receipt_encode_exit, kind, reason})}
  end

  @spec put_route_context(Error.t(), Envelope.t(), Route.t()) :: Error.t()
  defp put_route_context(error, envelope, route) do
    %{
      error
      | message_id: error.message_id || envelope.id,
        route_id: error.route_id || route.id
    }
  end

  @spec message_id(term()) :: term()
  defp message_id(%Envelope{id: id}), do: id
  defp message_id(_envelope), do: nil

  @spec route_id(term()) :: term()
  defp route_id(%Route{id: id}), do: id
  defp route_id(_route), do: nil

  @spec metadata(term(), atom(), term()) :: term()
  defp metadata(%Route{metadata: metadata}, key, default) when is_map(metadata),
    do: Map.get(metadata, key, Map.get(metadata, Atom.to_string(key), default))
end

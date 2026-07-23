defmodule Spectre.Pulse.Transports.REST do
  @moduledoc """
  HTTP/JSON Pulse binding.

  Outbound delivery posts the common envelope to a route URL. The server-side
  `handle_request/4` function is framework-neutral and returns a small response
  struct which Plug, Phoenix, Bandit, or another host can write to the socket.
  """

  @behaviour Spectre.Pulse.Transport

  alias Spectre.Pulse.Codec.JSON
  alias Spectre.Pulse.Endpoint
  alias Spectre.Pulse.Envelope
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Reachability
  alias Spectre.Pulse.Receipt
  alias Spectre.Pulse.Route
  alias Spectre.Pulse.Transports.REST.Response

  @doc false
  @spec deliver(Route.t(), Envelope.t(), keyword()) ::
          {:ok, Receipt.t()} | {:error, Error.t()}
  @impl Spectre.Pulse.Transport
  def deliver(%Route{target: url} = route, %Envelope{} = envelope, opts)
      when is_binary(url) do
    with {:ok, body} <- JSON.encode(envelope, opts) do
      headers =
        [{"content-type", "application/json"}, {"accept", "application/json"}]
        |> Kernel.++(headers(route, opts))
        |> Enum.uniq_by(fn {name, _value} -> String.downcase(to_string(name)) end)

      request_opts =
        [
          url: url,
          body: body,
          headers: headers,
          receive_timeout: Keyword.get(opts, :timeout, metadata(route, :timeout, 10_000)),
          redirect: false
        ]
        |> Keyword.merge(metadata(route, :req_options, []))
        |> Keyword.merge(Keyword.get(opts, :req_options, []))

      case Req.post(request_opts) do
        {:ok, response} -> response_result(response, envelope, route)
        {:error, exception} -> request_error(exception, envelope, route)
      end
    end
  rescue
    exception ->
      {:error,
       Error.outcome_unknown(:transport, {:rest_exception, exception},
         message_id: envelope.id,
         route_id: route.id,
         cause: exception
       )}
  end

  def deliver(%Route{} = route, %Envelope{} = envelope, _opts) do
    {:error,
     Error.not_sent(:routing, {:invalid_rest_url, route.target},
       message_id: envelope.id,
       route_id: route.id
     )}
  end

  @doc false
  @spec probe(Route.t(), keyword()) :: {:ok, Reachability.t()}
  @impl Spectre.Pulse.Transport
  def probe(%Route{target: url} = route, opts) when is_binary(url) do
    request_opts = [
      url: url,
      method: :head,
      receive_timeout: Keyword.get(opts, :timeout, 3_000),
      redirect: false
    ]

    case Req.request(request_opts) do
      {:ok, response} when response.status in 200..499 ->
        {:ok,
         Reachability.new(:reachable,
           level: :pulse_endpoint,
           via: :rest,
           valid_for_ms: Keyword.get(opts, :valid_for_ms, 5_000),
           metadata: %{route_id: route.id, status: response.status}
         )}

      {:ok, response} ->
        {:ok,
         Reachability.new(:unreachable,
           level: :pulse_endpoint,
           via: :rest,
           valid_for_ms: 1_000,
           reason: {:http_status, response.status},
           metadata: %{route_id: route.id}
         )}

      {:error, reason} ->
        {:ok,
         Reachability.new(:unreachable,
           level: :pulse_endpoint,
           via: :rest,
           valid_for_ms: 1_000,
           reason: request_reason(reason),
           metadata: %{route_id: route.id}
         )}
    end
  end

  def probe(%Route{} = route, _opts) do
    {:ok, Reachability.unknown(:invalid_rest_url, via: :rest, metadata: %{route_id: route.id})}
  end

  @doc """
  Handles a server request after the host has collected its body and headers.

  `authenticator` must bind request credentials to a canonical agent identity
  and return `{:ok, identity}` or `{:ok, identity, verified_facts}`.
  """
  @spec handle_request(binary(), map() | [{term(), term()}], term(), keyword()) :: Response.t()
  def handle_request(body, headers, peer, opts \\ []) when is_binary(body) do
    with {:ok, envelope} <- JSON.decode(body, opts),
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
         {:ok, encoded} <- Jason.encode(Receipt.to_wire(receipt)) do
      %Response{status: 202, body: encoded}
    else
      {:error, %Error{} = error} -> error_response(error)
      {:error, reason} -> error_response(Error.not_sent(:inbound, reason))
    end
  end

  @spec response_result(Req.Response.t(), Envelope.t(), Route.t()) ::
          {:ok, Receipt.t()} | {:error, Error.t()}
  defp response_result(response, envelope, route) when response.status in 200..299 do
    case receipt_from_body(response.body, envelope, route) do
      {:ok, receipt} -> {:ok, receipt}
      {:error, error} -> {:error, error}
    end
  end

  defp response_result(response, envelope, route) when response.status in 400..499 do
    {:error,
     Error.not_sent(:transport, {:http_rejected, response.status},
       message_id: envelope.id,
       route_id: route.id,
       details: %{status: response.status}
     )}
  end

  defp response_result(response, envelope, route) do
    {:error,
     Error.outcome_unknown(:transport, {:http_failure, response.status},
       message_id: envelope.id,
       route_id: route.id,
       details: %{status: response.status}
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

  @spec request_reason(term()) :: term()
  defp request_reason(%{reason: reason}), do: reason
  defp request_reason(reason), do: reason

  @spec definitely_not_sent?(term()) :: boolean()
  defp definitely_not_sent?(reason),
    do: reason in [:econnrefused, :nxdomain, :enetunreach, :ehostunreach, :closed]

  @spec authenticate(map() | [{term(), term()}], term(), keyword()) ::
          {:ok, String.t(), map()} | {:error, Error.t()}
  defp authenticate(headers, peer, opts) do
    authenticator = Keyword.get(opts, :authenticator)

    authenticator
    |> invoke_authenticator(normalize_headers(headers), peer, opts)
    |> normalize_authenticator_result()
  rescue
    exception -> {:error, Error.not_sent(:authentication, {:authenticator_exception, exception})}
  end

  @spec invoke_authenticator(term(), map(), term(), keyword()) :: term()
  defp invoke_authenticator(authenticator, headers, peer, _opts)
       when is_function(authenticator, 2),
       do: authenticator.(headers, peer)

  defp invoke_authenticator(authenticator, headers, peer, opts)
       when is_function(authenticator, 3),
       do: authenticator.(headers, peer, opts)

  defp invoke_authenticator(nil, _headers, _peer, opts) do
    if Keyword.get(opts, :allow_unauthenticated, false) do
      {:ok, nil, %{}}
    else
      {:error, Error.not_sent(:authentication, :rest_authenticator_required)}
    end
  end

  defp invoke_authenticator(_authenticator, _headers, _peer, _opts),
    do: {:error, Error.not_sent(:authentication, :rest_authenticator_required)}

  @spec normalize_authenticator_result(term()) ::
          {:ok, String.t(), map()} | {:error, Error.t()}
  defp normalize_authenticator_result({:ok, identity}) when is_binary(identity),
    do: {:ok, identity, %{}}

  defp normalize_authenticator_result({:ok, identity, verified})
       when is_binary(identity) and is_map(verified),
       do: {:ok, identity, verified}

  defp normalize_authenticator_result({:error, %Error{} = error}), do: {:error, error}

  defp normalize_authenticator_result({:error, reason}),
    do: {:error, Error.not_sent(:authentication, reason)}

  defp normalize_authenticator_result(result),
    do: {:error, Error.not_sent(:authentication, {:invalid_auth_result, result})}

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
        "error" => Atom.to_string(error.kind || :inbound),
        "reason" => safe_reason(error.reason)
      })

    %Response{status: status, body: body}
  end

  @spec safe_reason(term()) :: String.t()
  defp safe_reason(reason) when is_atom(reason), do: Atom.to_string(reason)
  defp safe_reason({reason, _details}) when is_atom(reason), do: Atom.to_string(reason)
  defp safe_reason(_reason), do: "request_rejected"

  @spec headers(Route.t(), keyword()) :: [{term(), term()}]
  defp headers(route, opts) do
    metadata(route, :headers, []) ++ Keyword.get(opts, :headers, [])
  end

  @spec normalize_headers(map() | [{term(), term()}]) :: %{optional(String.t()) => term()}
  defp normalize_headers(headers) when is_map(headers) do
    Map.new(headers, fn {name, value} -> {String.downcase(to_string(name)), value} end)
  end

  defp normalize_headers(headers) when is_list(headers) do
    Map.new(headers, fn {name, value} -> {String.downcase(to_string(name)), value} end)
  end

  @spec metadata(Route.t(), atom(), term()) :: term()
  defp metadata(route, key, default),
    do: Map.get(route.metadata, key, Map.get(route.metadata, Atom.to_string(key), default))
end

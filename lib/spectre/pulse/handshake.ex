defmodule Spectre.Pulse.Handshake do
  @moduledoc """
  Transport-neutral authentication and connection authorization boundary.

  A transport passes its opaque credential and technical context to
  `prepare/3`. Pulse invokes the callbacks from the selected `ConnectionSpec`
  and returns a credential-free ticket. The process that owns the physical
  connection then calls `open/2`.

  Authenticators receive `(credential, context)` and return
  `{:ok, principal}`. Authorizers receive `(principal, request)` and return
  `:ok`, `true`, or `{:ok, grants}`. A callback may be a function, a module
  exporting `authenticate/2` or `authorize/2`, or an MFA tuple whose extra
  arguments are appended.
  """

  alias Spectre.Pulse.Connection
  alias Spectre.Pulse.ConnectionRegistry
  alias Spectre.Pulse.ConnectionSpec
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Principal

  @enforce_keys [:spec_id, :principal, :connection_attrs, :prepared_at]
  defstruct [:spec_id, :principal, :connection_attrs, :prepared_at]

  @opaque t :: %__MODULE__{
            spec_id: atom() | String.t(),
            principal: Principal.t(),
            connection_attrs: map(),
            prepared_at: DateTime.t()
          }

  @connection_fields [
    :id,
    :peer_id,
    :direction,
    :status,
    :requested_profiles,
    :remote_agents,
    :verified,
    :metadata
  ]

  @doc "Authenticates and authorizes a credential without retaining it."
  @spec prepare(term(), term(), map() | keyword()) :: {:ok, t()} | {:error, Error.t()}
  def prepare(spec_id, credential, attrs \\ [])

  def prepare(spec_id, credential, attrs) when is_list(attrs) do
    if Keyword.keyword?(attrs),
      do: prepare(spec_id, credential, Map.new(attrs)),
      else: invalid({spec_id, attrs})
  end

  def prepare(spec_id, credential, attrs) when is_map(attrs) do
    with {:ok, %ConnectionSpec{} = spec} <- fetch_spec(spec_id),
         true <- spec.enabled,
         {:ok, context} <- context(attr(attrs, :context, %{})),
         {:ok, principal} <- authenticate(spec.authenticate, credential, context),
         request <- authorization_request(spec, attrs, context),
         {:ok, grants} <- authorize(spec.authorize, principal, request),
         {:ok, connection_attrs} <- connection_attrs(attrs, principal, grants) do
      {:ok,
       %__MODULE__{
         spec_id: spec.id,
         principal: principal,
         connection_attrs: connection_attrs,
         prepared_at: DateTime.utc_now()
       }}
    else
      false -> {:error, Error.not_sent(:authorization, {:connection_spec_disabled, spec_id})}
      :error -> {:error, Error.not_sent(:routing, {:unknown_connection_spec, spec_id})}
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, Error.not_sent(:validation, reason)}
    end
  end

  def prepare(spec_id, _credential, attrs), do: invalid({spec_id, attrs})

  @doc "Registers a prepared ticket under the process owning the physical connection."
  @spec open(t(), pid()) :: {:ok, Connection.t()} | {:error, Error.t()}
  def open(%__MODULE__{} = ticket, owner) when is_pid(owner) do
    attrs =
      ticket.connection_attrs
      |> Map.put(:owner, owner)
      |> Map.put_new(:transport_pid, owner)

    ConnectionRegistry.open(ticket.spec_id, attrs)
  end

  def open(ticket, owner),
    do: {:error, Error.not_sent(:validation, {:invalid_connection_ticket, ticket, owner})}

  @spec fetch_spec(term()) :: {:ok, ConnectionSpec.t()} | :error | {:error, Error.t()}
  defp fetch_spec(spec_id), do: ConnectionRegistry.fetch_spec(spec_id)

  @spec authenticate(term(), term(), map()) :: {:ok, Principal.t()} | {:error, Error.t()}
  defp authenticate(nil, _credential, _context),
    do: {:error, Error.not_sent(:authentication, :connection_authenticator_required)}

  defp authenticate(callback, credential, context) do
    callback
    |> invoke(:authenticate, [credential, context])
    |> normalize_authentication()
  end

  @spec normalize_authentication(term()) :: {:ok, Principal.t()} | {:error, Error.t()}
  defp normalize_authentication({:ok, principal}) do
    case Principal.new(principal) do
      {:ok, principal} -> {:ok, principal}
      {:error, %Error{} = error} -> {:error, error}
    end
  end

  defp normalize_authentication({:error, %Error{} = error}), do: {:error, error}

  defp normalize_authentication({:error, reason}),
    do: {:error, Error.not_sent(:authentication, reason)}

  defp normalize_authentication(false),
    do: {:error, Error.not_sent(:authentication, :connection_authentication_failed)}

  defp normalize_authentication(result),
    do: {:error, Error.not_sent(:authentication, {:invalid_authenticator_result, result})}

  @spec authorize(term(), Principal.t(), map()) :: {:ok, map()} | {:error, Error.t()}
  defp authorize(nil, _principal, _request), do: {:ok, %{}}

  defp authorize(callback, principal, request) do
    callback
    |> invoke(:authorize, [principal, request])
    |> normalize_authorization()
  end

  @spec normalize_authorization(term()) :: {:ok, map()} | {:error, Error.t()}
  defp normalize_authorization(result) when result in [:ok, true], do: {:ok, %{}}

  defp normalize_authorization({:ok, grants}) when is_list(grants) do
    if Keyword.keyword?(grants),
      do: {:ok, Map.new(grants)},
      else: {:error, Error.not_sent(:authorization, {:invalid_connection_grants, grants})}
  end

  defp normalize_authorization({:ok, grants}) when is_map(grants), do: {:ok, grants}
  defp normalize_authorization({:error, %Error{} = error}), do: {:error, error}

  defp normalize_authorization({:error, reason}),
    do: {:error, Error.not_sent(:authorization, reason)}

  defp normalize_authorization(result) when result in [false, :error],
    do: {:error, Error.not_sent(:authorization, :connection_forbidden)}

  defp normalize_authorization(result),
    do: {:error, Error.not_sent(:authorization, {:invalid_authorizer_result, result})}

  @spec invoke(term(), atom(), [term()]) :: term()
  defp invoke(callback, _operation, args) when is_function(callback, 2),
    do: protected(fn -> apply(callback, args) end)

  defp invoke(callback, _operation, [first, _second]) when is_function(callback, 1),
    do: protected(fn -> callback.(first) end)

  defp invoke(module, operation, args) when is_atom(module) and not is_nil(module) do
    if Code.ensure_loaded?(module) and function_exported?(module, operation, length(args)),
      do: protected(fn -> apply(module, operation, args) end),
      else: {:error, {:invalid_connection_callback, operation, module}}
  end

  defp invoke({module, function, extra}, _operation, args)
       when is_atom(module) and is_atom(function) and is_list(extra),
       do: protected(fn -> apply(module, function, args ++ extra) end)

  defp invoke(callback, operation, _args),
    do: {:error, {:invalid_connection_callback, operation, callback}}

  @spec protected((-> term())) :: term()
  defp protected(callback) do
    callback.()
  rescue
    exception -> {:error, {:connection_callback_exception, exception}}
  catch
    kind, reason -> {:error, {:connection_callback_exit, kind, reason}}
  end

  @spec authorization_request(ConnectionSpec.t(), map(), map()) :: map()
  defp authorization_request(spec, attrs, context) do
    %{
      connection_spec: ConnectionSpec.to_public_map(spec),
      context: context,
      direction: attr(attrs, :direction, :inbound),
      requested_profiles: attr(attrs, :requested_profiles, []),
      requested_scopes: attr(attrs, :requested_scopes, [])
    }
  end

  @spec connection_attrs(map(), Principal.t(), map()) :: {:ok, map()} | {:error, term()}
  defp connection_attrs(attrs, principal, grants) do
    with {:ok, grants} <- normalize_grant_keys(grants) do
      attrs =
        attrs
        |> take_connection_fields()
        |> Map.put(:principal, principal)
        |> Map.merge(grants)

      {:ok, attrs}
    end
  end

  @spec take_connection_fields(map()) :: map()
  defp take_connection_fields(attrs) do
    Enum.reduce(@connection_fields, %{}, fn field, values ->
      put_optional_field(values, field, fetch_attr(attrs, field))
    end)
  end

  @spec put_optional_field(map(), atom(), {:ok, term()} | :error) :: map()
  defp put_optional_field(values, field, {:ok, value}), do: Map.put(values, field, value)
  defp put_optional_field(values, _field, :error), do: values

  @spec normalize_grant_keys(map()) :: {:ok, map()} | {:error, term()}
  defp normalize_grant_keys(grants) do
    allowed = [:granted_profiles, :granted_scopes, "granted_profiles", "granted_scopes"]

    case Map.keys(grants) -- allowed do
      [] ->
        {:ok,
         %{
           granted_profiles: attr(grants, :granted_profiles),
           granted_scopes: attr(grants, :granted_scopes)
         }
         |> Enum.reject(fn {_key, value} -> is_nil(value) end)
         |> Map.new()}

      unknown ->
        {:error, {:unknown_connection_grants, unknown}}
    end
  end

  @spec context(term()) :: {:ok, map()} | {:error, term()}
  defp context(value) when is_map(value), do: {:ok, value}
  defp context(value), do: {:error, {:invalid_connection_context, value}}

  @spec invalid(term()) :: {:error, Error.t()}
  defp invalid(value), do: {:error, Error.not_sent(:validation, {:invalid_handshake, value})}

  @spec attr(map(), atom(), term()) :: term()
  defp attr(map, key, default \\ nil),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key), default))

  @spec fetch_attr(map(), atom()) :: {:ok, term()} | :error
  defp fetch_attr(map, key) do
    case Map.fetch(map, key) do
      {:ok, value} -> {:ok, value}
      :error -> Map.fetch(map, Atom.to_string(key))
    end
  end
end

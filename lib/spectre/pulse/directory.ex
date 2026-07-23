defmodule Spectre.Pulse.Directory do
  @moduledoc """
  Behaviour and dispatcher for application-owned identity/route resolution.

  A directory is queried as a value or callback. Pulse does not start or own a
  global registry.
  """

  alias Spectre.Pulse.Address
  alias Spectre.Pulse.Contact
  alias Spectre.Pulse.ContactBook
  alias Spectre.Pulse.Directory.Resolution
  alias Spectre.Pulse.Error
  alias Spectre.Pulse.Route

  @callback resolve(term(), keyword()) ::
              {:ok, Contact.t() | Address.t() | String.t() | Resolution.t()}
              | {:error, term()}
              | :error

  @callback routes(String.t(), keyword()) ::
              {:ok, [Route.t() | map() | keyword()]} | {:error, term()} | [Route.t()]

  @callback contacts(keyword()) :: [Contact.t()] | {:ok, [Contact.t()]} | {:error, term()}

  @optional_callbacks routes: 2, contacts: 1

  @doc "Resolves through a contact book, module, configured module, or function."
  @spec resolve(term(), term(), keyword()) :: {:ok, Resolution.t()} | {:error, Error.t()}
  def resolve(source, reference, opts \\ [])

  def resolve(%ContactBook{} = book, reference, _opts) do
    with {:ok, address} <- ContactBook.resolve(book, reference) do
      contact =
        case ContactBook.fetch(book, reference) do
          {:ok, value} -> value
          :error -> nil
        end

      {:ok,
       %Resolution{
         reference: reference,
         address: address,
         contact: contact,
         routes: if(contact, do: contact.routes, else: []),
         source: :contact_book
       }}
    end
  end

  def resolve(nil, reference, _opts) when is_binary(reference) do
    with {:ok, address} <- Address.normalize(reference) do
      {:ok, %Resolution{reference: reference, address: address, source: :address}}
    end
  end

  def resolve(nil, reference, _opts),
    do: {:error, Error.not_sent(:routing, {:unknown_contact, reference})}

  def resolve({module, source_opts}, reference, opts)
      when is_atom(module) and is_list(source_opts) do
    resolve_module(module, reference, Keyword.merge(source_opts, opts))
  end

  def resolve(module, reference, opts) when is_atom(module) do
    resolve_module(module, reference, opts)
  end

  def resolve(function, reference, opts) when is_function(function, 2) do
    function
    |> safe_call([reference, opts])
    |> normalize_resolution(reference, function, opts)
  end

  def resolve(source, _reference, _opts),
    do: {:error, Error.not_sent(:routing, {:invalid_directory, source})}

  @doc "Fetches and normalizes routes from a directory source."
  @spec routes(term(), String.t(), keyword()) :: {:ok, [Route.t()]} | {:error, Error.t()}
  def routes(source, address, opts \\ [])

  def routes(%ContactBook{} = book, address, _opts),
    do: {:ok, ContactBook.routes(book, address)}

  def routes({module, source_opts}, address, opts)
      when is_atom(module) and is_list(source_opts),
      do: routes(module, address, Keyword.merge(source_opts, opts))

  def routes(module, address, opts) when is_atom(module) do
    if Code.ensure_loaded?(module) and function_exported?(module, :routes, 2) do
      module
      |> safe_call(:routes, [address, opts])
      |> normalize_routes()
    else
      {:ok, []}
    end
  end

  def routes(_source, _address, _opts), do: {:ok, []}

  @doc "Lists contacts exposed by a source when supported."
  @spec contacts(term(), keyword()) :: {:ok, [Contact.t()]} | {:error, Error.t()}
  def contacts(%ContactBook{} = book, _opts), do: {:ok, ContactBook.contacts(book)}

  def contacts({module, source_opts}, opts) when is_atom(module) and is_list(source_opts),
    do: contacts(module, Keyword.merge(source_opts, opts))

  def contacts(module, opts) when is_atom(module) do
    if Code.ensure_loaded?(module) and function_exported?(module, :contacts, 1) do
      module
      |> safe_call(:contacts, [opts])
      |> normalize_contacts()
    else
      {:ok, []}
    end
  end

  def contacts(_source, _opts), do: {:ok, []}

  @spec resolve_module(module(), term(), keyword()) ::
          {:ok, Resolution.t()} | {:error, Error.t()}
  defp resolve_module(module, reference, opts) do
    if Code.ensure_loaded?(module) and function_exported?(module, :resolve, 2) do
      module
      |> safe_call(:resolve, [reference, opts])
      |> normalize_resolution(reference, module, opts)
    else
      {:error, Error.not_sent(:routing, {:invalid_directory, module})}
    end
  end

  @spec normalize_resolution(term(), term(), term(), keyword()) ::
          {:ok, Resolution.t()} | {:error, Error.t()}
  defp normalize_resolution({:ok, value}, reference, source, opts),
    do: normalize_resolution(value, reference, source, opts)

  defp normalize_resolution(%Resolution{} = resolution, _reference, source, _opts) do
    with {:ok, address} <- Address.normalize(resolution.address),
         {:ok, routes} <- normalize_routes(resolution.routes) do
      {:ok, %{resolution | address: address, routes: routes, source: resolution.source || source}}
    end
  end

  defp normalize_resolution(%Contact{} = contact, reference, source, opts) do
    with {:ok, extra_routes} <- routes(source, contact.identity, opts) do
      {:ok,
       %Resolution{
         reference: reference,
         address: contact.identity,
         contact: contact,
         routes: merge_routes(contact.routes, extra_routes),
         source: source
       }}
    end
  end

  defp normalize_resolution(%Address{} = address, reference, source, opts),
    do: normalize_resolution(address.value, reference, source, opts)

  defp normalize_resolution(address, reference, source, opts) when is_binary(address) do
    with {:ok, canonical} <- Address.normalize(address),
         {:ok, routes} <- routes(source, canonical, opts) do
      {:ok,
       %Resolution{
         reference: reference,
         address: canonical,
         routes: routes,
         source: source
       }}
    end
  end

  defp normalize_resolution(:error, reference, _source, _opts),
    do: {:error, Error.not_sent(:routing, {:unknown_contact, reference})}

  defp normalize_resolution({:error, %Error{} = error}, _reference, _source, _opts),
    do: {:error, error}

  defp normalize_resolution({:error, reason}, _reference, _source, _opts),
    do: {:error, Error.not_sent(:routing, reason)}

  defp normalize_resolution(value, _reference, _source, _opts),
    do: {:error, Error.not_sent(:routing, {:invalid_directory_result, value})}

  @spec normalize_routes(term()) :: {:ok, [Route.t()]} | {:error, Error.t()}
  defp normalize_routes({:ok, routes}), do: normalize_routes(routes)
  defp normalize_routes({:error, %Error{} = error}), do: {:error, error}
  defp normalize_routes({:error, reason}), do: {:error, Error.not_sent(:routing, reason)}

  defp normalize_routes(routes) when is_list(routes) do
    Enum.reduce_while(routes, {:ok, []}, fn route, {:ok, acc} ->
      case Route.new(route) do
        {:ok, value} -> {:cont, {:ok, [value | acc]}}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
    |> case do
      {:ok, normalized} -> {:ok, Enum.sort_by(normalized, & &1.priority)}
      error -> error
    end
  end

  defp normalize_routes(value),
    do: {:error, Error.not_sent(:routing, {:invalid_routes, value})}

  @spec normalize_contacts(term()) :: {:ok, [Contact.t()]} | {:error, Error.t()}
  defp normalize_contacts({:ok, contacts}), do: normalize_contacts(contacts)
  defp normalize_contacts({:error, %Error{} = error}), do: {:error, error}
  defp normalize_contacts({:error, reason}), do: {:error, Error.not_sent(:routing, reason)}

  defp normalize_contacts(contacts) when is_list(contacts) do
    with {:ok, book} <- ContactBook.new(contacts), do: {:ok, ContactBook.contacts(book)}
  end

  defp normalize_contacts(value),
    do: {:error, Error.not_sent(:routing, {:invalid_contacts, value})}

  @spec merge_routes([Route.t()], [Route.t()]) :: [Route.t()]
  defp merge_routes(left, right) do
    (left ++ right)
    |> Enum.uniq_by(& &1.id)
    |> Enum.sort_by(& &1.priority)
  end

  @spec safe_call(module(), atom(), [term()]) :: term()
  defp safe_call(module, function, args) do
    apply(module, function, args)
  rescue
    exception -> {:error, {:directory_exception, exception}}
  catch
    kind, reason -> {:error, {:directory_throw, kind, reason}}
  end

  @spec safe_call(function(), [term()]) :: term()
  defp safe_call(function, args) do
    apply(function, args)
  rescue
    exception -> {:error, {:directory_exception, exception}}
  catch
    kind, reason -> {:error, {:directory_throw, kind, reason}}
  end
end

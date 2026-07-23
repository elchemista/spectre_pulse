defmodule Spectre.Pulse.ContactBook do
  @moduledoc """
  An immutable, agent-owned address book.

  It is a value, not a registry or store. Applications may keep it in
  `Spectre.State`, configuration, or any external directory they own.
  """

  alias Spectre.Pulse.Address
  alias Spectre.Pulse.Contact
  alias Spectre.Pulse.Error

  defstruct by_key: %{}, key_by_identity: %{}

  @type t :: %__MODULE__{
          by_key: %{optional(Contact.key()) => Contact.t()},
          key_by_identity: %{optional(String.t()) => Contact.key()}
        }

  @doc "Builds a contact book from contacts."
  @spec new([Contact.t() | map() | keyword()]) :: {:ok, t()} | {:error, Error.t()}
  def new(contacts \\ []) when is_list(contacts) do
    Enum.reduce_while(contacts, {:ok, %__MODULE__{}}, fn contact, {:ok, book} ->
      case put(book, contact) do
        {:ok, updated} -> {:cont, {:ok, updated}}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
  end

  @doc "Like `new/1`, but raises for invalid or ambiguous contacts."
  @spec new!([Contact.t() | map() | keyword()]) :: t()
  def new!(contacts \\ []) do
    case new(contacts) do
      {:ok, book} -> book
      {:error, error} -> raise ArgumentError, Exception.message(error)
    end
  end

  @doc "Adds or replaces one contact while keeping identity indexes coherent."
  @spec put(t(), Contact.t() | map() | keyword()) :: {:ok, t()} | {:error, Error.t()}
  def put(%__MODULE__{} = book, contact) do
    with {:ok, contact} <- Contact.new(contact),
         :ok <- ensure_identity_not_aliased(book, contact) do
      old = Map.get(book.by_key, contact.key)

      identities =
        book.key_by_identity
        |> maybe_delete_old_identity(old)
        |> Map.put(contact.identity, contact.key)

      {:ok,
       %{
         book
         | by_key: Map.put(book.by_key, contact.key, contact),
           key_by_identity: identities
       }}
    end
  end

  @doc "Deletes a contact by key or known canonical address."
  @spec delete(t(), Contact.key() | String.t()) :: t()
  def delete(%__MODULE__{} = book, reference) do
    case fetch(book, reference) do
      {:ok, contact} ->
        %{
          book
          | by_key: Map.delete(book.by_key, contact.key),
            key_by_identity: Map.delete(book.key_by_identity, contact.identity)
        }

      :error ->
        book
    end
  end

  @doc "Fetches a known contact by its local key or canonical address."
  @spec fetch(t(), Contact.key() | String.t()) :: {:ok, Contact.t()} | :error
  def fetch(%__MODULE__{} = book, key) when is_atom(key) do
    Map.fetch(book.by_key, key)
  end

  def fetch(%__MODULE__{} = book, reference) when is_binary(reference) do
    case Map.fetch(book.by_key, reference) do
      {:ok, contact} ->
        {:ok, contact}

      :error ->
        with {:ok, address} <- Address.normalize(reference),
             {:ok, key} <- Map.fetch(book.key_by_identity, address) do
          Map.fetch(book.by_key, key)
        else
          _ -> :error
        end
    end
  end

  def fetch(%__MODULE__{}, _reference), do: :error

  @doc """
  Resolves a local key or logical address to a canonical address.

  A syntactically valid canonical address need not already be in the book.
  """
  @spec resolve(t(), Contact.key() | String.t()) :: {:ok, String.t()} | {:error, Error.t()}
  def resolve(%__MODULE__{} = book, reference) do
    case fetch(book, reference) do
      {:ok, contact} ->
        {:ok, contact.identity}

      :error when is_binary(reference) ->
        Address.normalize(reference)

      :error ->
        {:error, Error.not_sent(:routing, {:unknown_contact, reference})}
    end
  end

  @doc "Returns known routes for a contact/address, ordered by priority."
  @spec routes(t(), Contact.key() | String.t()) :: [Spectre.Pulse.Route.t()]
  def routes(%__MODULE__{} = book, reference) do
    case fetch(book, reference) do
      {:ok, contact} -> contact.routes
      :error -> []
    end
  end

  @doc "Returns every contact in deterministic key order."
  @spec contacts(t()) :: [Contact.t()]
  def contacts(%__MODULE__{} = book) do
    book.by_key
    |> Map.values()
    |> Enum.sort_by(&to_string(&1.key))
  end

  @doc "Filters contacts by exact declared fields such as capability."
  @spec find(t(), keyword()) :: [Contact.t()]
  def find(%__MODULE__{} = book, opts) do
    capability = Keyword.get(opts, :capability)
    identity = Keyword.get(opts, :identity)

    book
    |> contacts()
    |> Enum.filter(fn contact ->
      (is_nil(capability) or capability in contact.capabilities) and
        (is_nil(identity) or Address.equal?(identity, contact.identity))
    end)
  end

  @doc """
  Merges books from lowest to highest precedence.

  A later contact replaces the same key. Two different keys cannot silently
  become aliases for one identity.
  """
  @spec merge([t()]) :: {:ok, t()} | {:error, Error.t()}
  def merge(books) when is_list(books) do
    contacts = Enum.flat_map(books, &contacts/1)
    new(contacts)
  end

  @spec ensure_identity_not_aliased(t(), Contact.t()) :: :ok | {:error, Error.t()}
  defp ensure_identity_not_aliased(%__MODULE__{} = book, %Contact{} = contact) do
    case Map.get(book.key_by_identity, contact.identity) do
      nil -> :ok
      key when key == contact.key -> :ok
      other -> {:error, Error.not_sent(:validation, {:identity_already_known_as, other})}
    end
  end

  @spec maybe_delete_old_identity(
          %{optional(String.t()) => Contact.key()},
          Contact.t() | nil
        ) :: %{optional(String.t()) => Contact.key()}
  defp maybe_delete_old_identity(index, nil), do: index
  defp maybe_delete_old_identity(index, old), do: Map.delete(index, old.identity)
end

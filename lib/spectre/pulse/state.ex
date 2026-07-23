defmodule Spectre.Pulse.State do
  @moduledoc """
  Pure helpers for keeping Pulse contacts and expectations in `Spectre.State`.

  This module is not a store and performs no persistence.
  """

  alias Spectre.Pulse.Contact
  alias Spectre.Pulse.ContactBook
  alias Spectre.Pulse.Envelope
  alias Spectre.Pulse.Expectation

  @doc "Returns the dynamic contact book stored in Spectre state."
  @spec contact_book(Spectre.State.t()) :: ContactBook.t()
  def contact_book(%Spectre.State{} = state) do
    contacts =
      state.data
      |> pulse_data()
      |> Map.get(:contacts, %{})
      |> normalize_contact_values()

    case ContactBook.new(contacts) do
      {:ok, book} -> book
      {:error, _error} -> %ContactBook{}
    end
  end

  @doc "Returns static and dynamic contacts, with dynamic keys taking precedence."
  @spec contact_book(Spectre.State.t(), ContactBook.t()) :: ContactBook.t()
  def contact_book(%Spectre.State{} = state, %ContactBook{} = static) do
    dynamic = contact_book(state)

    merged =
      dynamic
      |> ContactBook.contacts()
      |> Enum.reduce_while({:ok, static}, fn contact, {:ok, book} ->
        case ContactBook.put(book, contact) do
          {:ok, updated} -> {:cont, {:ok, updated}}
          {:error, error} -> {:halt, {:error, error}}
        end
      end)

    case merged do
      {:ok, book} -> book
      _ -> static
    end
  end

  @doc "Returns a new Spectre state remembering one contact."
  @spec remember_contact(Spectre.State.t(), Contact.t() | map() | keyword()) ::
          {:ok, Spectre.State.t()} | {:error, Spectre.Pulse.Error.t()}
  def remember_contact(%Spectre.State{} = state, contact) do
    with {:ok, contact} <- Contact.new(contact),
         {:ok, book} <- ContactBook.put(contact_book(state), contact) do
      contacts = Map.new(ContactBook.contacts(book), &{&1.key, &1})
      {:ok, put_pulse_data(state, :contacts, contacts)}
    end
  end

  @doc "Returns a new Spectre state without the referenced contact."
  @spec forget_contact(Spectre.State.t(), term()) :: Spectre.State.t()
  def forget_contact(%Spectre.State{} = state, reference) do
    contacts =
      state
      |> contact_book()
      |> ContactBook.delete(reference)
      |> ContactBook.contacts()
      |> Map.new(&{&1.key, &1})

    put_pulse_data(state, :contacts, contacts)
  end

  @doc "Returns the expectation map stored in Spectre state."
  @spec expectations(Spectre.State.t()) :: %{optional(String.t()) => Expectation.t()}
  def expectations(%Spectre.State{} = state) do
    state.data
    |> pulse_data()
    |> Map.get(:expectations, %{})
  end

  @doc "Returns a new state with an expectation stored by outbound message id."
  @spec put_expectation(Spectre.State.t(), Expectation.t()) :: Spectre.State.t()
  def put_expectation(%Spectre.State{} = state, %Expectation{} = expectation) do
    updated = Map.put(expectations(state), expectation.message_id, expectation)
    put_pulse_data(state, :expectations, updated)
  end

  @doc "Applies the matching expectation reducer for an inbound envelope."
  @spec correlate(Spectre.State.t(), Envelope.t()) ::
          {:ok, Spectre.State.t(), Expectation.t()} | :unmatched
  def correlate(%Spectre.State{} = state, %Envelope{} = envelope) do
    with relation when is_binary(relation) <- envelope.relates_to,
         %Expectation{} = expectation <- Map.get(expectations(state), relation),
         {:ok, resolved} <- Expectation.resolve(expectation, envelope) do
      updated = Map.put(expectations(state), relation, resolved)
      {:ok, put_pulse_data(state, :expectations, updated), resolved}
    else
      _ -> :unmatched
    end
  end

  @doc "Drops a reminder from state without affecting any remote agent."
  @spec forget_expectation(Spectre.State.t(), String.t()) :: Spectre.State.t()
  def forget_expectation(%Spectre.State{} = state, message_id) do
    put_pulse_data(state, :expectations, Map.delete(expectations(state), message_id))
  end

  @spec pulse_data(map()) :: map()
  defp pulse_data(data) when is_map(data), do: Map.get(data, :pulse, %{})

  @spec put_pulse_data(Spectre.State.t(), atom(), term()) :: Spectre.State.t()
  defp put_pulse_data(%Spectre.State{} = state, key, value) do
    pulse = state.data |> pulse_data() |> Map.put(key, value)
    %{state | data: Map.put(state.data, :pulse, pulse)}
  end

  @spec normalize_contact_values(term()) :: [Contact.t() | map() | keyword()]
  defp normalize_contact_values(contacts) when is_map(contacts), do: Map.values(contacts)
  defp normalize_contact_values(contacts) when is_list(contacts), do: contacts
  defp normalize_contact_values(_contacts), do: []
end

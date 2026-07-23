defmodule Spectre.Pulse.Control do
  @moduledoc """
  Constructors for the minimal Pulse v1 control plane.
  """

  alias Spectre.Pulse.Envelope
  alias Spectre.Pulse.Identity

  @doc "Builds an identity-description message."
  @spec describe(Identity.t(), String.t(), keyword()) ::
          {:ok, Envelope.t()} | {:error, Spectre.Pulse.Error.t()}
  def describe(%Identity{} = identity, to, opts \\ []) do
    Envelope.new(
      from: identity.address,
      to: to,
      act: :inform,
      relates_to: Keyword.get(opts, :relates_to),
      payload: %{
        type: "pulse.identity.describe",
        data: Identity.to_public_map(identity)
      },
      metadata: Keyword.get(opts, :metadata, %{})
    )
  end

  @doc "Builds an on-demand reachability ping."
  @spec ping(String.t(), String.t(), keyword()) ::
          {:ok, Envelope.t()} | {:error, Spectre.Pulse.Error.t()}
  def ping(from, to, opts \\ []) do
    nonce = Keyword.get(opts, :nonce, Spectre.Identity.uuid7())

    Envelope.new(
      from: from,
      to: to,
      act: :query,
      payload: %{type: "pulse.reachability.ping", data: %{"nonce" => nonce}},
      metadata: Keyword.get(opts, :metadata, %{})
    )
  end

  @doc "Builds a pong correlated with a ping."
  @spec pong(Envelope.t(), keyword()) ::
          {:ok, Envelope.t()} | {:error, Spectre.Pulse.Error.t()}
  def pong(%Envelope{payload: %{type: "pulse.reachability.ping"}} = ping, opts \\ []) do
    Envelope.reply(
      ping,
      "pulse.reachability.pong",
      %{"nonce" => nonce(ping.payload.data)},
      Keyword.put_new(opts, :act, :inform)
    )
  end

  @spec nonce(term()) :: term()
  defp nonce(data) when is_map(data), do: Map.get(data, "nonce", Map.get(data, :nonce))
  defp nonce(_data), do: nil
end

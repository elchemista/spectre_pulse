defmodule Spectre.Pulse.Protocol do
  @moduledoc """
  Constants and wire-level constraints for Pulse protocol version 1.

  Protocol semantics do not depend on JSON, Elixir terms, HTTP, WebSocket, or
  any other encoding or transport.
  """

  @version 1
  @acts [:inform, :query, :request]
  @wire_acts %{"inform" => :inform, "query" => :query, "request" => :request}
  @control_types [
    "pulse.identity.describe",
    "pulse.reachability.ping",
    "pulse.reachability.pong"
  ]

  @default_limits %{
    max_envelope_bytes: 1_000_000,
    max_address_bytes: 512,
    max_type_bytes: 255,
    max_metadata_bytes: 65_536
  }

  @type act :: :inform | :query | :request

  @doc "The current Pulse protocol version."
  @spec version() :: pos_integer()
  def version, do: @version

  @doc "The complete v1 communicative-act vocabulary."
  @spec acts() :: [act()]
  def acts, do: @acts

  @doc "Reserved v1 control-plane payload types."
  @spec control_types() :: [String.t()]
  def control_types, do: @control_types

  @doc "Default defensive limits used by codecs and validators."
  @spec default_limits() :: map()
  def default_limits, do: @default_limits

  @doc "Returns a data-only description suitable for conformance tooling."
  @spec describe() :: map()
  def describe do
    %{
      name: "spectre-pulse",
      version: @version,
      acts: Enum.map(@acts, &Atom.to_string/1),
      control_types: @control_types,
      delivery: %{
        exactly_once: false,
        duplicates_possible: true,
        ordering_guaranteed: false,
        response_is_new_envelope: true
      },
      limits: @default_limits,
      json_schema: "priv/schema/pulse-envelope-v1.schema.json"
    }
  end

  @doc "Returns whether a value is a supported local act."
  @spec valid_act?(term()) :: boolean()
  def valid_act?(act), do: act in @acts

  @doc "Converts a controlled local act to its wire string."
  @spec encode_act(act()) :: {:ok, String.t()} | {:error, term()}
  def encode_act(act) when act in @acts, do: {:ok, Atom.to_string(act)}
  def encode_act(act), do: {:error, {:unsupported_act, act}}

  @doc """
  Decodes only the three pre-existing v1 act atoms.

  It never calls `String.to_atom/1` on remote data.
  """
  @spec decode_act(term()) :: {:ok, act()} | {:error, term()}
  def decode_act(act) when act in @acts, do: {:ok, act}

  def decode_act(act) when is_binary(act) do
    case @wire_acts do
      %{^act => decoded} -> {:ok, decoded}
      _ -> {:error, {:unsupported_act, act}}
    end
  end

  def decode_act(act), do: {:error, {:unsupported_act, act}}

  @doc "Merges caller overrides onto the defensive protocol limits."
  @spec limits(keyword() | map()) :: map()
  def limits(overrides \\ []) do
    Map.merge(@default_limits, Map.new(overrides))
  end
end

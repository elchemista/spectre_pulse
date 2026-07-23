defmodule SpectrePulse.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/elchemista/spectre_pulse"

  def project do
    [
      app: :spectre_pulse,
      name: "Spectre Pulse",
      version: @version,
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      description: "A transport-independent protocol for communication between Spectre agents.",
      source_url: @source_url,
      homepage_url: @source_url,
      package: package(),
      docs: docs(),
      deps: deps()
    ]
  end

  def application do
    [
      mod: {Spectre.Pulse.Application, []},
      extra_applications: [:logger, :crypto]
    ]
  end

  defp deps do
    [
      # Pulse deliberately depends on Spectre, never the other way around.
      {:spectre, github: "elchemista/spectre", ref: "38aae368aca51225e0d2e8d68b8ce10465f55ca5"},
      {:jason, "~> 1.4"},
      {:req, "~> 0.5"},
      {:ex_doc, "~> 0.40", only: :dev, runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false}
    ]
  end

  defp package do
    [
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => @source_url},
      files: ~w(lib examples priv mix.exs README.md)
    ]
  end

  defp docs do
    [
      main: "readme",
      source_ref: "v#{@version}",
      extras: ["README.md"],
      groups_for_modules: [
        Protocol: [
          Spectre.Pulse,
          Spectre.Pulse.Address,
          Spectre.Pulse.Envelope,
          Spectre.Pulse.Payload,
          Spectre.Pulse.Protocol,
          Spectre.Pulse.Validator
        ],
        "Contacts and routing": [
          Spectre.Pulse.Contact,
          Spectre.Pulse.ContactBook,
          Spectre.Pulse.Directory,
          Spectre.Pulse.Discovery,
          Spectre.Pulse.Fabric,
          Spectre.Pulse.Local,
          Spectre.Pulse.Route,
          Spectre.Pulse.Network,
          Spectre.Pulse.Reachability
        ],
        "Spectre integration": [
          Spectre.Pulse.Config,
          Spectre.Pulse.Endpoint,
          Spectre.Pulse.Inbound,
          Spectre.Pulse.InboundContext,
          Spectre.Pulse.Executor,
          Spectre.Pulse.Expectation,
          Spectre.Pulse.Runtime
        ],
        Transports: [
          Spectre.Pulse.Transport,
          Spectre.Pulse.Transports.Local,
          Spectre.Pulse.Transports.Node,
          Spectre.Pulse.Transports.PubSub,
          Spectre.Pulse.Transports.REST,
          Spectre.Pulse.Transports.WebSocket
        ]
      ]
    ]
  end
end

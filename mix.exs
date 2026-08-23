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
      test_coverage: [summary: [threshold: 91]],
      docs: docs(),
      dialyzer: [plt_add_apps: [:mix]],
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
      spectre_dep(),
      {:spectre_beam,
       github: "elchemista/spectre_beam", branch: "main", only: :test, runtime: false},
      {:spectre_directive,
       github: "elchemista/spectre_directive", branch: "main", only: :test, runtime: false},
      {:spectre_kinetic,
       github: "elchemista/spectre_kinetic", branch: "main", only: :test, runtime: false},
      {:spectre_lens,
       github: "elchemista/spectre_lens", branch: "main", only: :test, runtime: false},
      {:spectre_mnemonic,
       github: "elchemista/spectre_mnemonic", branch: "main", only: :test, runtime: false},
      {:spectre_prism,
       github: "elchemista/spectre_prism", branch: "main", only: :test, runtime: false},
      {:jason, "~> 1.4"},
      {:req, "~> 0.5"},
      {:ex_doc, "~> 0.40", only: :dev, runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false}
    ]
  end

  defp spectre_dep do
    case System.get_env("SPECTRE_PATH") do
      path when is_binary(path) and path != "" ->
        {:spectre, path: Path.expand(path, __DIR__), override: true}

      _unset ->
        {:spectre, "~> 0.3.3", override: true}
    end
  end

  defp docs do
    [
      main: "readme",
      source_ref: "v#{@version}",
      extras: [
        "README.md",
        "docs/GETTING_STARTED.md",
        "docs/PHOENIX.md",
        "docs/STUDIO_MONITORING.md",
        "docs/STUDIO_OPERATIONS.md",
        "docs/STUDIO_INSPECTION.md",
        "docs/CONNECTIONS_AND_TRANSPORTS.md",
        "docs/DELIVERY_AND_SECURITY.md",
        "docs/ARCHITECTURE.md",
        "docs/PUBLIC_API.md",
        "CHANGELOG.md",
        "LICENSE"
      ],
      groups_for_extras: [
        "Start here": [
          "README.md",
          "docs/GETTING_STARTED.md",
          "docs/PHOENIX.md"
        ],
        "Spectre Studio": [
          "docs/STUDIO_MONITORING.md",
          "docs/STUDIO_OPERATIONS.md",
          "docs/STUDIO_INSPECTION.md"
        ],
        "Protocol and infrastructure": [
          "docs/CONNECTIONS_AND_TRANSPORTS.md",
          "docs/DELIVERY_AND_SECURITY.md",
          "docs/ARCHITECTURE.md",
          "docs/PUBLIC_API.md"
        ],
        Project: ["CHANGELOG.md", "LICENSE"]
      ],
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
          Spectre.Pulse.Connection,
          Spectre.Pulse.ConnectionSpec,
          Spectre.Pulse.AgentDescriptor,
          Spectre.Pulse.Principal,
          Spectre.Pulse.Operations,
          Spectre.Pulse.RuntimeInfo,
          Spectre.Pulse.Monitoring,
          Spectre.Pulse.Handshake,
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
          Spectre.Pulse.Runtime,
          Spectre.Pulse.Studio,
          Spectre.Pulse.Phoenix,
          Spectre.Pulse.Stack
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

defmodule SpectrePulse.MixProject do
  use Mix.Project

  @version "0.1.2"
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
      {:spectre, github: "elchemista/spectre", ref: "b39b0b1e77d685c0e497cd64d7f16f20d3c1c846"},
      {:spectre_beam,
       github: "elchemista/spectre_beam",
       ref: "0ea43a2ef2bd3d5f291c585c473758ce7db1b531",
       only: :test,
       runtime: false},
      {:spectre_directive,
       github: "elchemista/spectre_directive",
       ref: "b7b1d4a4f4f60604a0f9b3f75f468d8513bc7dce",
       only: :test,
       runtime: false},
      {:spectre_kinetic,
       github: "elchemista/spectre_kinetic",
       ref: "0719f7ce26047078e0816680d01e592993365a94",
       only: :test,
       runtime: false},
      {:spectre_lens,
       github: "elchemista/spectre_lens",
       ref: "9066054f91d4c163ced0ec4ccefb81108ffdae10",
       only: :test,
       runtime: false},
      {:spectre_mnemonic,
       github: "elchemista/spectre_mnemonic",
       ref: "b460a8a1c6d8450464653e45a12a4f5e102988ef",
       only: :test,
       runtime: false},
      {:spectre_prism,
       github: "elchemista/spectre_prism",
       ref: "5754931ac224470378f5e0a32f9e480fdbcb6ade",
       only: :test,
       runtime: false},
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
          Spectre.Pulse.Runtime,
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

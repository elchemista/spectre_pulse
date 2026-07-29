defmodule SpectrePulse.MixProject do
  use Mix.Project

  @version "0.1.3"
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
      ecosystem_dep(:spectre, "SPECTRE_PATH", "spectre"),
      ecosystem_test_dep(:spectre_beam, "SPECTRE_BEAM_PATH", "spectre_beam"),
      ecosystem_test_dep(
        :spectre_directive,
        "SPECTRE_DIRECTIVE_PATH",
        "spectre_directive"
      ),
      ecosystem_test_dep(:spectre_kinetic, "SPECTRE_KINETIC_PATH", "spectre_kinetic"),
      ecosystem_test_dep(:spectre_lens, "SPECTRE_LENS_PATH", "spectre_lens"),
      ecosystem_test_dep(:spectre_mnemonic, "SPECTRE_MNEMONIC_PATH", "spectre_mnemonic"),
      ecosystem_test_dep(:spectre_prism, "SPECTRE_PRISM_PATH", "spectre_prism"),
      {:jason, "~> 1.4"},
      {:req, "~> 0.5"},
      {:ex_doc, "~> 0.40", only: :dev, runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false}
    ]
  end

  defp ecosystem_dep(name, env, repository) do
    case System.get_env(env) do
      path when is_binary(path) and path != "" -> {name, path: Path.expand(path)}
      _other -> {name, github: "elchemista/#{repository}", branch: "feature/v0.1.3-run"}
    end
  end

  defp ecosystem_test_dep(name, env, repository) do
    {^name, options} = ecosystem_dep(name, env, repository)
    {name, Keyword.merge(options, only: :test, runtime: false)}
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

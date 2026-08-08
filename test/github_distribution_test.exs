defmodule SpectrePulse.GitHubDistributionTest do
  use ExUnit.Case, async: true

  @ecosystem_repositories %{
    spectre: "spectre",
    spectre_beam: "spectre_beam",
    spectre_directive: "spectre_directive",
    spectre_kinetic: "spectre_kinetic",
    spectre_lens: "spectre_lens",
    spectre_mnemonic: "spectre_mnemonic",
    spectre_prism: "spectre_prism"
  }

  test "every Spectre dependency is a direct GitHub tuple without Hex or path fallbacks" do
    config = Mix.Project.config()
    deps = Keyword.fetch!(config, :deps)

    Enum.each(@ecosystem_repositories, fn {name, repository} ->
      dependency = Enum.find(deps, &(elem(&1, 0) == name))

      assert {^name, opts} = dependency
      assert opts[:github] == "elchemista/#{repository}"
      refute Keyword.has_key?(opts, :path)
      refute Keyword.has_key?(opts, :hex)
    end)

    assert {:spectre, spectre_opts} = Enum.find(deps, &(elem(&1, 0) == :spectre))
    assert spectre_opts[:tag] == "0.2.0"
    refute Keyword.has_key?(config, :package)
  end
end

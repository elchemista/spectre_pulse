defmodule SpectrePulse.GitHubDistributionTest do
  use ExUnit.Case, async: true

  @satellite_repositories %{
    spectre_beam: "spectre_beam",
    spectre_directive: "spectre_directive",
    spectre_kinetic: "spectre_kinetic",
    spectre_lens: "spectre_lens",
    spectre_mnemonic: "spectre_mnemonic",
    spectre_prism: "spectre_prism"
  }

  test "core comes from Hex while unpublished satellites stay on GitHub" do
    config = Mix.Project.config()
    deps = Keyword.fetch!(config, :deps)

    Enum.each(@satellite_repositories, fn {name, repository} ->
      dependency = Enum.find(deps, &(elem(&1, 0) == name))

      assert {^name, opts} = dependency
      assert opts[:github] == "elchemista/#{repository}"
      refute Keyword.has_key?(opts, :path)
      refute Keyword.has_key?(opts, :hex)
    end)

    assert {:spectre, "~> 0.3.0", spectre_opts} =
             Enum.find(deps, &(elem(&1, 0) == :spectre))

    assert spectre_opts[:override]
    refute Keyword.has_key?(spectre_opts, :github)
    refute Keyword.has_key?(spectre_opts, :path)
    refute Keyword.has_key?(config, :package)
  end
end

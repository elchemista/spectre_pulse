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

  test "core uses the selected source while unpublished satellites stay on GitHub" do
    config = Mix.Project.config()
    deps = Keyword.fetch!(config, :deps)

    Enum.each(@satellite_repositories, fn {name, repository} ->
      dependency = Enum.find(deps, &(elem(&1, 0) == name))

      assert {^name, opts} = dependency
      assert opts[:github] == "elchemista/#{repository}"
      refute Keyword.has_key?(opts, :path)
      refute Keyword.has_key?(opts, :hex)
    end)

    dependency = Enum.find(deps, &(elem(&1, 0) == :spectre))

    spectre_opts =
      case System.get_env("SPECTRE_PATH") do
        path when is_binary(path) and path != "" ->
          assert {:spectre, opts} = dependency
          assert opts[:path] == Path.expand(path, File.cwd!())
          opts

        _unset ->
          assert {:spectre, "~> 0.3.0", opts} = dependency
          refute Keyword.has_key?(opts, :path)
          opts
      end

    assert spectre_opts[:override]
    refute Keyword.has_key?(spectre_opts, :github)
    refute Keyword.has_key?(config, :package)
  end
end

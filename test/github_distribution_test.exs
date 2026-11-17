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

  test "core and satellites use explicit local overrides or their published source" do
    config = Mix.Project.config()
    assert config[:docs][:source_ref] == config[:version]
    deps = Keyword.fetch!(config, :deps)

    Enum.each(@satellite_repositories, fn {name, repository} ->
      dependency = Enum.find(deps, &(elem(&1, 0) == name))

      assert {^name, opts} = dependency
      variable = name |> Atom.to_string() |> String.upcase() |> Kernel.<>("_PATH")

      case System.get_env(variable) do
        path when is_binary(path) and path != "" ->
          assert opts[:path] == Path.expand(path, File.cwd!())
          refute Keyword.has_key?(opts, :github)

        _ ->
          assert opts[:github] == "elchemista/#{repository}"
          assert opts[:branch] == "main"
          refute Keyword.has_key?(opts, :path)
      end

      refute Keyword.has_key?(opts, :hex)
    end)

    dependency = Enum.find(deps, &(elem(&1, 0) == :spectre))

    spectre_opts =
      case System.get_env("SPECTRE_PATH") do
        path when is_binary(path) and path != "" ->
          assert {:spectre, opts} = dependency
          assert opts[:path] == Path.expand(path, File.cwd!())
          assert opts[:override]
          opts

        _unset ->
          assert {:spectre, "~> 0.4.0"} = dependency
          []
      end

    refute Keyword.has_key?(spectre_opts, :github)
    assert config[:package][:licenses] == ["Apache-2.0"]
    assert "docs" in config[:package][:files]
    refute "test" in config[:package][:files]
  end
end

defmodule DeltaQuery.MixProject do
  use Mix.Project

  @source_url "https://github.com/revelrylabs/delta_query"

  def project do
    [
      app: :delta_query,
      version: "0.1.0",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      elixirc_paths: elixirc_paths(Mix.env()),
      source_url: @source_url,
      homepage_url: @source_url,

      # Hex
      description: "An Elixir library for querying Delta Sharing tables.",
      package: package(),

      # Docs
      name: "DeltaQuery",
      docs: docs()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url},
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:explorer, "~> 0.10"},
      {:nimble_parsec, "~> 1.4"},
      {:req, "~> 0.5"},
      {:jason, "~> 1.4"},
      {:styler, "~> 1.0", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.29", only: [:dev, :test], runtime: false},
      {:opentelemetry_api, "~> 1.3", optional: true}
    ]
  end

  defp docs() do
    [
      main: "readme",
      extras:
        [
          "CODE_OF_CONDUCT.md",
          "RELEASES.md",
          "CONTRIBUTING.md",
          "README.md",
          "LICENSE",
        ]
    ]
  end
end

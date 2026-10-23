defmodule DeltaQuery.MixProject do
  use Mix.Project

  @source_url "https://github.com/revelrylabs/delta_query"
  @version "0.4.0"

  def project do
    [
      app: :delta_query,
      version: @version,
      elixir: "~> 1.16",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      elixirc_paths: elixirc_paths(Mix.env()),
      source_url: @source_url,
      homepage_url: @source_url,

      # Hex
      description:
        "Elixir client for querying Delta Sharing tables: SQL-like filters, column selection, joins and aggregations over Explorer DataFrames.",
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
      links: %{
        "GitHub" => @source_url,
        "Changelog" => "#{@source_url}/blob/main/CHANGELOG.md",
        "Delta Sharing protocol" => "https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md"
      },
      files: ~w(lib mix.exs README.md LICENSE CHANGELOG.md usage-rules.md)
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
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:styler, "~> 1.0", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.29", only: [:dev, :test], runtime: false}
    ]
  end

  defp docs do
    [
      main: "readme",
      source_ref: "v#{@version}",
      extras: [
        "README.md",
        "CHANGELOG.md",
        "CONTRIBUTING.md",
        "CODE_OF_CONDUCT.md",
        "RELEASES.md",
        "LICENSE",
        "usage-rules.md": [title: "Usage Rules"]
      ],
      groups_for_modules: [
        "Query building": [DeltaQuery.Query, DeltaQuery.Config, DeltaQuery.Column],
        Results: [DeltaQuery.Results],
        Internals: [DeltaQuery.Client, DeltaQuery.PredicateParser]
      ]
    ]
  end
end

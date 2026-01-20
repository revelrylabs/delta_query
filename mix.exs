defmodule DeltaQuery.MixProject do
  use Mix.Project

  def project do
    [
      app: :delta_query,
      version: "0.1.0",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      elixirc_paths: elixirc_paths(Mix.env())
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:explorer, "~> 0.10"},
      {:nimble_parsec, "~> 1.4"},
      {:finch, "~> 0.18"},
      {:jason, "~> 1.4"},
      {:opentelemetry_api, "~> 1.3", optional: true}
    ]
  end
end

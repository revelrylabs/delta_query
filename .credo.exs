%{
  configs: [
    %{
      name: "default",
      strict: true,
      files: %{
        included: ["lib/", "test/", "mix.exs"],
        excluded: [~r"/_build/", ~r"/deps/", "test/test_live.exs"]
      },
      checks: [
        {Credo.Check.Refactor.Nesting, max_nesting: 3}
      ]
    }
  ]
}

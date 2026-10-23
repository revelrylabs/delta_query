# Contributing and Development

## Development Setup

1. Make sure you have Elixir 1.16+ installed (see `.tool-versions` for the versions we develop against)
1. Clone the repo
1. Run `mix deps.get`
1. Run `mix test`

## Checks

CI runs these on every pull request. Run them locally before pushing:

```sh
mix format --check-formatted
mix credo --strict
mix test
```

The unit and integration tests use Parquet fixtures under `test/support/fixtures` and need no
network access. To exercise the client against a real Delta Sharing server, set
`DELTA_SHARING_ENDPOINT`, `DELTA_SHARING_BEARER_TOKEN`, `DELTA_SHARING_SHARE` and
`DELTA_SHARING_TEST_TABLE` and run:

```sh
mix run scripts/live_check.exs
```

## Submitting Changes

1. Fork the project
1. Create a new topic branch to contain your feature, change, or fix.
1. Make sure all the tests are still passing.
1. Implement your feature, change, or fix. Make sure to write tests, update and/or add documentation.
1. Add an entry under `Unreleased` in `CHANGELOG.md` if the change is visible to users.
1. Push your topic branch up to your fork.
1. Open a Pull Request with a clear title and description.

## AI-Assisted Contributions

Using AI tools to help write code, tests, issues, or pull requests is welcome
here. The quality bar is the same *regardless* of how a contribution was produced:

- You are the author. You must understand your change, be able to explain it,
  and be able to respond to review feedback about it.
- Run the checks before submitting: `mix test`, `mix format`, and
  `mix credo --strict` should all pass locally.
- Verify claims yourself. Do not submit issues or PR descriptions containing
  unverified AI output (for example, a bug report speculating about behavior
  without a reproduction, or a description of changes the diff does not make).
- I really don't care if you used AI or not: I care that your contribution is a best-effort attempt to improve the repo!

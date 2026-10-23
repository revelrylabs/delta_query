# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `usage-rules.md`, shipped in the Hex package, with rules for AI coding agents via
  [usage_rules](https://hex.pm/packages/usage_rules)
- This changelog
- Credo runs in CI
- Text files check out with LF line endings on every platform

### Changed

- README and module docs reworked
- Hexdocs "View Source" links point at the release tag instead of `main`
- `DeltaQuery.Results` and `DeltaQuery.Client` filter helpers were flattened

### Removed

- The unused optional `opentelemetry_api` dependency
- The `examples/` directory, which duplicated the test fixtures and their README

## [0.3.0] - 2026-10-12

### Changed

- CI tests across Elixir 1.16/1.18/1.20 on OTP 26/27/28, plus the minimum supported
  Explorer version ([#31](https://github.com/revelrylabs/delta_query/pull/31))
- Explorer 0.13 ([#30](https://github.com/revelrylabs/delta_query/pull/30))

### Security

- Mint 1.9.3 to 1.11.0 for published advisories
  ([#32](https://github.com/revelrylabs/delta_query/pull/32))

## [0.2.4] - 2026-09-03

### Fixed

- `DeltaQuery.Query.execute/2` returns `{:error, message}` instead of raising when an
  ordering operator (`>`, `<`, `>=`, `<=`) in a `where/2` predicate targets a column type
  that does not support ordering ([#27](https://github.com/revelrylabs/delta_query/pull/27))

### Changed

- Req 0.7, Explorer 0.12

## [0.2.3] - 2026-05-12

### Fixed

- `DeltaQuery.Results.filter/2` returns `{:error, message}` instead of raising when an
  ordering operator targets a column type that does not support ordering
  ([#12](https://github.com/revelrylabs/delta_query/pull/12))

## [0.2.2] - 2026-02-12

### Fixed

- Date comparisons in `where/2` predicates: ISO 8601 string values are converted to `Date`
  when the column is a date column ([#8](https://github.com/revelrylabs/delta_query/pull/8))

## [0.2.1] - 2026-01-30

### Added

- `DeltaQuery.list_schemas/1`, `DeltaQuery.list_tables/1` and
  `DeltaQuery.get_table_schema/1` for discovering shares before querying
  ([#5](https://github.com/revelrylabs/delta_query/pull/5))

## [0.2.0] - 2026-01-28

### Changed

- **Breaking:** the HTTP client is now Req instead of Finch. The `:finch_name` config key is
  gone; use `:req_options` to pass options to `Req.new/1`
  ([#3](https://github.com/revelrylabs/delta_query/pull/3))

## [0.1.0] - 2026-01-26

### Added

- Initial release: query Delta Sharing tables with SQL-like predicates, column selection
  and limit hints; join, filter, text-search and aggregate results; access the underlying
  Explorer DataFrame

[Unreleased]: https://github.com/revelrylabs/delta_query/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/revelrylabs/delta_query/compare/v0.2.4...v0.3.0
[0.2.4]: https://github.com/revelrylabs/delta_query/compare/v0.2.3...v0.2.4
[0.2.3]: https://github.com/revelrylabs/delta_query/compare/v0.2.2...v0.2.3
[0.2.2]: https://github.com/revelrylabs/delta_query/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/revelrylabs/delta_query/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/revelrylabs/delta_query/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/revelrylabs/delta_query/releases/tag/v0.1.0

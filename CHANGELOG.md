# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- `DeltaQuery.Results.files_processed` counts only files that were downloaded and parsed, and
  `total_files` counts files left after partition pruning. Previously both were always equal,
  so a skipped download was invisible
- **Breaking:** `DeltaQuery.Client.parse_parquet_files/3` returns `{:ok, %DeltaQuery.Results{}}`
  instead of `{:ok, dataframe}`. Read `results.dataframe` to get the previous value

### Fixed

- Partition pruning compares boolean and numeric partition values by type, so
  `where("available = true")` no longer skips every file of a table partitioned on a boolean
- A file action without a `url` is logged and skipped instead of raising `FunctionClauseError`

## [0.3.1] - 2026-10-23

### Added

- `usage-rules.md`, shipped in the Hex package, with rules for AI coding agents via
  [usage_rules](https://hex.pm/packages/usage_rules)
  ([#36](https://github.com/revelrylabs/delta_query/pull/36))
- This changelog ([#36](https://github.com/revelrylabs/delta_query/pull/36))

### Changed

- README and module docs reworked
  ([#36](https://github.com/revelrylabs/delta_query/pull/36))
- Hexdocs "View Source" links point at the release tag instead of `main`
  ([#36](https://github.com/revelrylabs/delta_query/pull/36))

### Removed

- The unused optional `opentelemetry_api` dependency
  ([#36](https://github.com/revelrylabs/delta_query/pull/36))
- The `examples/` directory, which duplicated the test fixtures
  ([#36](https://github.com/revelrylabs/delta_query/pull/36))

### Security

- Decimal 3.1.1 to 3.2.0 for a published advisory
  ([#34](https://github.com/revelrylabs/delta_query/pull/34))

## [0.3.0] - 2026-10-12

### Changed

- CI tests across Elixir 1.16/1.18/1.20 on OTP 26/27/28, plus the minimum supported
  Explorer version ([#31](https://github.com/revelrylabs/delta_query/pull/31))

### Security

- Mint 1.9.3 to 1.11.0 for published advisories
  ([#32](https://github.com/revelrylabs/delta_query/pull/32))

## [0.2.4] - 2026-09-03

### Fixed

- `DeltaQuery.Query.execute/2` returns `{:error, message}` instead of raising when an
  ordering operator (`>`, `<`, `>=`, `<=`) in a `where/2` predicate targets a column type
  that does not support ordering ([#27](https://github.com/revelrylabs/delta_query/pull/27))

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

[Unreleased]: https://github.com/revelrylabs/delta_query/compare/v0.3.1...HEAD
[0.3.1]: https://github.com/revelrylabs/delta_query/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/revelrylabs/delta_query/compare/v0.2.4...v0.3.0
[0.2.4]: https://github.com/revelrylabs/delta_query/compare/v0.2.3...v0.2.4
[0.2.3]: https://github.com/revelrylabs/delta_query/compare/v0.2.2...v0.2.3
[0.2.2]: https://github.com/revelrylabs/delta_query/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/revelrylabs/delta_query/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/revelrylabs/delta_query/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/revelrylabs/delta_query/releases/tag/v0.1.0

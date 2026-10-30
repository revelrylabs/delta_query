# DeltaQuery usage rules

DeltaQuery is an Elixir client for [Delta Sharing](https://delta.io/sharing/) tables. A query asks the
sharing server for a table, the server answers with Parquet file URLs, and DeltaQuery downloads those
files, loads them with Explorer, applies your predicates, and returns rows as maps.

## Configuration

- Required keys: `:endpoint`, `:bearer_token`, `:share`. Optional: `:schema` (default `"public"`),
  `:req_options` (passed to `Req.new/1`).
- Set them in app config under `config :delta_query, :config, ...`, or pass `config:` to any call
  as a `%DeltaQuery.Config{}` (from `DeltaQuery.configure!/1`) or a keyword list. Per-call keys
  override app config.
- A missing required key gives `{:error, "endpoint is required"}` (or `ArgumentError` from
  `configure!/1`).

## Discovery

- `DeltaQuery.list_schemas/1`, `DeltaQuery.list_tables/1` (`schema:` option) and
  `DeltaQuery.get_table_schema/1` (`table:` required) return `{:ok, list} | {:error, term}`.
- `get_table_schema/1` returns `%DeltaQuery.Column{name: "book_id", type: "long"}` structs. `type`
  is the Delta type name as a string (`"string"`, `"long"`, `"double"`, `"date"`, `"timestamp"`,
  `"boolean"`, `"decimal(10,2)"`). Struct, array and map columns report only the outer kind, so an
  array of strings comes back as `"array"`.

## Building and running a query

- Pipeline: `DeltaQuery.query("table") |> where(...) |> select([...]) |> limit(n) |> execute()`.
- `execute/2` returns `{:ok, %DeltaQuery.Results{}} | {:error, term}`. `execute!/2` raises
  `RuntimeError` instead.
- `where/2` takes one predicate string per call. Repeated calls are ANDed.
- `select/2` takes a list of column-name strings and replaces any earlier selection. Names that
  are not in the data are dropped; if none match, every column is returned.
- `limit/2` is only a hint sent to the server. The client does not truncate, so more rows than the
  limit can come back. Cap with `Enum.take/2` on rows or `Explorer.DataFrame.head/2` on
  `results.dataframe` when you need a hard limit.

## Predicate syntax

- Exactly one comparison per string: `column op value`. Operators: `=`, `!=`, `>`, `<`, `>=`, `<=`.
- Values: integers (`42`), floats (`19.99`), quoted strings (`'Fiction'` or `"Fiction"`),
  `true`/`false`, `null`. Column names may contain letters, digits, `_` and `.`.
- No `AND`/`OR`, `IN`, `LIKE`, `IS NULL`, parentheses or functions. Chain `where/2` for AND. There is
  no OR.
- Compare `date` columns with an ISO 8601 string: `"publication_date >= '2024-01-01'"`. A malformed
  date raises `ArgumentError`.
- Ordering operators (`>`, `<`, `>=`, `<=`) work on integer, float, date, time, datetime and
  duration columns. On string, boolean and decimal columns they return
  `{:error, "operator > not supported on ..."}`.
- Timestamp columns cannot be compared with a string literal yet; that raises a dtype-mismatch
  `ArgumentError`. Filter `results.dataframe` with Explorer instead.
- `where/2` predicates are sent to the server as partition hints and also applied to every row after
  download. Whole files are skipped only when the predicate names a partition column; a predicate on
  any other column still downloads every file.
- A `where/2` predicate that fails to parse is logged and ignored, not returned as an error.
  `DeltaQuery.filter/2` returns `{:error, "invalid filter: ..."}` for the same input. Check with
  `DeltaQuery.PredicateParser.parse_predicate/1` first if the predicate is built at runtime.
- A `where/2` predicate on a column that is not in the data matches every row. `filter/2` returns
  `{:error, "unknown column in filter: ..."}`.

## Working with results

- `%DeltaQuery.Results{dataframe: %Explorer.DataFrame{}, files_processed: n, total_files: m}`.
  `files_processed < total_files` means some files failed and the data is incomplete; see
  `DeltaQuery.Results` for the exact meaning of each count.
  Use `dataframe` directly for anything Explorer can do.
- `to_rows/1` returns maps with **string keys**. Values use Explorer types: `Date`,
  `NaiveDateTime`, `Decimal` for decimal columns, `nil` for null.
- Partition columns are present in every row, cast to their type from the table schema, so
  `select/2`, `filter/2` and `join/3` can use them like any other column.
- `count/1`, `empty?/1`, `first/1` (`nil` when empty) and `sum/2` (`0` when the column is missing).
  `sum/2` supports integer and float columns only; decimal columns crash inside Explorer.
- `filter/2` applies more predicates in memory. `text_search/3` is a case-insensitive substring match
  OR-ed across the given columns. Columns that do not exist are skipped, or `{:error, ...}` if none
  of them exist. An empty search string returns everything. Prefer `where/2` on the query because
  it runs before download.
- `join/3` needs `on:` (string or list) and takes `how:` (`:left` default, `:inner`, `:right`,
  `:outer`, `:cross`). Non-key columns present on both sides get a `_right` suffix on the right.
- `aggregate_by_column/2` takes the column as an **atom** and returns
  `[%{status: "Approved", count: 42}, ...]` sorted by count descending.

## Errors and logging

- HTTP errors come back as `{:error, {:api_error, status, body}}`; transport failures as
  `{:error, {:request_failed, reason}}`.
- A data file that fails to download or parse is logged with `Logger.error/1` and skipped; the
  query still succeeds with the remaining files, so watch the log.

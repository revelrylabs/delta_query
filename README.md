# DeltaQuery

[![Hex.pm](https://img.shields.io/hexpm/v/delta_query.svg)](https://hex.pm/packages/delta_query)
[![Docs](https://img.shields.io/badge/docs-hexdocs-blue.svg)](https://hexdocs.pm/delta_query)
[![Tests](https://github.com/revelrylabs/delta_query/actions/workflows/test.yml/badge.svg)](https://github.com/revelrylabs/delta_query/actions/workflows/test.yml)

Elixir client for querying [Delta Sharing](https://delta.io/sharing/) tables.

> **Note:** This library is pre-1.0. The API may change between minor versions. See the [changelog](CHANGELOG.md).

A Delta Sharing server answers a query with a list of Parquet files. This library allows you to download them, load them with [Explorer](https://hexdocs.pm/explorer), apply your filters, and finally return the data as Elixir maps. The underlying `Explorer.DataFrame` is always available when you need more than the built-in helpers.

## Installation

Add `delta_query` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:delta_query, "~> 0.3.0"}
  ]
end
```

## Configuration

Configure credentials in `config/runtime.exs`:

```elixir
config :delta_query, :config,
  endpoint: System.get_env("DELTA_SHARING_ENDPOINT"),
  bearer_token: System.get_env("DELTA_SHARING_BEARER_TOKEN"),
  share: "my_share",
  schema: "public"
```

Or build a config and pass it to any call:

```elixir
config =
  DeltaQuery.configure!(
    endpoint: "https://sharing.example.com",
    bearer_token: "your-token",
    share: "my_share"
  )

{:ok, tables} = DeltaQuery.list_tables(config: config)
```

## Usage

### Discovery

List available schemas, tables, and columns before querying:

```elixir
{:ok, schemas} = DeltaQuery.list_schemas()
# => {:ok, ["public", "analytics"]}

{:ok, tables} = DeltaQuery.list_tables()
# => {:ok, ["books", "loans", "members"]}

{:ok, tables} = DeltaQuery.list_tables(schema: "analytics")

{:ok, columns} = DeltaQuery.get_table_schema(table: "books")
# => {:ok, [
#   %DeltaQuery.Column{name: "book_id", type: "long"},
#   %DeltaQuery.Column{name: "title", type: "string"},
#   %DeltaQuery.Column{name: "published", type: "date"}
# ]}
```

`type` is the Delta type name, such as `"long"`, `"string"` or `"date"`.

### Querying

```elixir
{:ok, results} =
  "books"
  |> DeltaQuery.query()
  |> DeltaQuery.where("library_id = 100")
  |> DeltaQuery.where("genre = 'Fiction'")
  |> DeltaQuery.select(["book_id", "title", "author"])
  |> DeltaQuery.limit(100)
  |> DeltaQuery.execute()

DeltaQuery.to_rows(results)
# => [%{"book_id" => 1001, "title" => "The Great Gatsby", "author" => "F. Scott Fitzgerald"}, ...]
```

- `where/2` takes one predicate per call. See [Predicates](#predicates).
- `select/2` replaces any earlier selection.
- `limit/2` is a hint to the server, not a hard cap.
- `execute/2` returns `{:ok, results}` or `{:error, reason}` (`execute!/2` is also available)

### Working with results

`execute/2` returns a `%DeltaQuery.Results{}` with an `Explorer.DataFrame` inside. Row maps have string keys, and values use Explorer's types.

```elixir
DeltaQuery.to_rows(results)   # => [%{"book_id" => 1001, ...}, ...]
DeltaQuery.count(results)     # => 42
DeltaQuery.sum(results, "fine_amount")  # => 3.5 (integer and float columns)

# Anything else Explorer can do
results.dataframe
|> Explorer.DataFrame.sort_by(desc: page_count)
|> Explorer.DataFrame.head(10)
```

### Joining

```elixir
books = DeltaQuery.query("books") |> DeltaQuery.execute!()
publishers = DeltaQuery.query("publishers") |> DeltaQuery.execute!()

joined = DeltaQuery.join(books, publishers, on: "book_id", how: :inner)
DeltaQuery.to_rows(joined)
```

`:on` accepts a column name or a list of names. `:how` is `:left` by default, or `:inner`, `:right`, `:outer`, `:cross`.

### Filtering and searching fetched results

```elixir
results = DeltaQuery.query("books") |> DeltaQuery.execute!()

{:ok, filtered} = DeltaQuery.filter(results, ["page_count > 300"])

{:ok, searched} = DeltaQuery.text_search(results, "fiction", ["genre", "title"])
```

Prefer `where/2` on the query: it runs before download and can skip whole files on partition columns. `filter/2` runs in memory on data you already have. `text_search/3` is a case-insensitive substring match, OR-ed across the given columns.

### Aggregations

```elixir
results = DeltaQuery.query("reservations") |> DeltaQuery.execute!()

DeltaQuery.aggregate_by_column(results, :status)
# => [%{status: "Approved", count: 42}, %{status: "Pending", count: 18}]
```

## Predicates

A predicate is one comparison: `column operator value`.

- Operators: `=`, `!=`, `>`, `>=`, `<`, `<=`
- Values: integers (`123`, `-10`), floats (`99.99`), strings (`'single'` or `"double"` quoted), booleans (`true`, `false`), and `null`
- Column names may contain letters, digits, `_` and `.`

```elixir
"book_id = 123"
"status != 'Pending'"
"list_price >= 19.99"
"publication_date > '2024-01-01'"
"available = true"
```

There is no `AND`, `OR`, `IN`, `LIKE`, `IS NULL`, or grouping. Chain `where/2` calls for AND. For anything else, filter `results.dataframe` with Explorer.

## For AI coding agents

The package ships a [`usage-rules.md`](usage-rules.md): a short, dense summary of the API, predicate syntax, and gotchas written for coding agents. Sync it into your project with [usage_rules](https://hex.pm/packages/usage_rules), or copy it into your `AGENTS.md` or `CLAUDE.md`.

## Contributing and Development

Bug reports and pull requests are welcome on GitHub at https://github.com/revelrylabs/delta_query. Check out the [contributing guidelines](CONTRIBUTING.md) for more info, and [RELEASES.md](RELEASES.md) for the release process.

Everyone is welcome to participate in the project. We expect contributors to adhere to the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md).

## License

DeltaQuery is released under the MIT License. See the [LICENSE](LICENSE) file for details.

# DeltaQuery

[![Hex.pm](https://img.shields.io/hexpm/v/delta_query.svg)](https://hex.pm/packages/delta_query)
[![Docs](https://img.shields.io/badge/docs-hexdocs-blue.svg)](https://hexdocs.pm/delta_query)

> **Note:** This library is in early development (pre-1.0). The API may change between minor versions.

Elixir client for querying [Delta Sharing](https://delta.io/sharing/) tables.

Delta Sharing is an open protocol for secure data sharing across organizations. This library provides a high-level client for querying shared Delta tables. Results are returned as Elixir data structures, with optional access to the underlying Explorer DataFrames.

## Installation

Add `delta_query` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:delta_query, "~> 0.2.1"}
  ]
end
```

## Setup

Configure credentials in `config/config.exs` or `config/runtime.exs`:

```elixir
config :delta_query, :config,
  endpoint: System.get_env("DELTA_SHARING_ENDPOINT"),
  bearer_token: System.get_env("DELTA_SHARING_BEARER_TOKEN"),
  share: "my_share",
  schema: "public"
```

## Usage

### Discovery

List available schemas, tables, and columns before querying:

```elixir
# List schemas in the configured share
{:ok, schemas} = DeltaQuery.list_schemas()
# => {:ok, ["public", "analytics"]}

# List tables in the configured schema
{:ok, tables} = DeltaQuery.list_tables()
# => {:ok, ["books", "loans", "members"]}

# List tables in a specific schema
{:ok, tables} = DeltaQuery.list_tables(schema: "analytics")

# Get column names and types for a table
{:ok, columns} = DeltaQuery.get_table_schema(table: "books")
# => {:ok, [
#   %{name: "book_id", type: "long"},
#   %{name: "title", type: "string"},
#   %{name: "author", type: "string"}
# ]}
```

### Basic Query

```elixir
"books"
|> DeltaQuery.query()
|> DeltaQuery.where("library_id = 100")
|> DeltaQuery.select(["book_id", "title", "author"])
|> DeltaQuery.limit(100)
|> DeltaQuery.execute!()
|> DeltaQuery.to_rows()
```

### Per-Query Configuration

```elixir
config = DeltaQuery.configure!(
  endpoint: "https://sharing.example.com",
  bearer_token: "your-token",
  share: "my_share"
)

"books"
|> DeltaQuery.query()
|> DeltaQuery.execute!(config: config)
```

### Joining Results

```elixir
books = DeltaQuery.query("books") |> DeltaQuery.execute!()
publishers = DeltaQuery.query("publishers") |> DeltaQuery.execute!()

joined = DeltaQuery.join(books, publishers, on: "book_id", how: :inner)
DeltaQuery.to_rows(joined)
```

### Post-Query Filtering

```elixir
results = DeltaQuery.query("books") |> DeltaQuery.execute!()

# Additional filters on fetched data
{:ok, filtered} = DeltaQuery.filter(results, ["page_count > 300"])

# Text search
{:ok, searched} = DeltaQuery.text_search(results, "fiction", ["genre", "title"])
```

### Aggregations

```elixir
results = DeltaQuery.query("reservations") |> DeltaQuery.execute!()

DeltaQuery.aggregate_by_column(results, :status)
# => [%{status: "Approved", count: 42}, %{status: "Pending", count: 18}]
```

## Configuration Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `:endpoint` | Yes | - | Delta Sharing server URL |
| `:bearer_token` | Yes | - | Authentication token |
| `:share` | Yes | - | Share name to query |
| `:schema` | No | `"public"` | Schema name |
| `:req_options` | No | `[]` | Options passed to Req requests |

## Predicates

Predicates use SQL-like syntax for filtering:

- Equality: `column = value`
- Inequality: `column != value`
- Comparisons: `column > value`, `column >= value`, `column < value`, `column <= value`

Values can be:
- Integers: `123`, `-10`
- Floats: `99.99`, `-0.5`
- Strings: `'single quoted'` or `"double quoted"`
- Booleans: `true`, `false`, `TRUE`, `FALSE`
- Null: `null`, `NULL`

## License

MIT
# DeltaQuery

[![Hex.pm](https://img.shields.io/hexpm/v/delta_query.svg)](https://hex.pm/packages/delta_query)
[![Docs](https://img.shields.io/badge/docs-hexdocs-blue.svg)](https://hexdocs.pm/delta_query)

> **Note:** This library is in early development (pre-1.0). The API may change between minor versions.

Elixir client for querying [Delta Sharing](https://delta.io/sharing/) tables.

Delta Sharing is an open protocol for secure data sharing across organizations. This library provides a high-level client that queries shared Delta tables, downloads Parquet files, and returns data as Explorer DataFrames.

## Installation

Add `delta_query` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:delta_query, "~> 0.1.0"}
  ]
end
```

## Setup

1. Ensure you have a Finch pool running. If you already have one in your app, you can reuse it. Otherwise, add one to your supervision tree:

```elixir
children = [
  {Finch, name: :delta_query_finch}
]
```

2. Configure credentials in `config/config.exs` or `config/runtime.exs`:

```elixir
config :delta_query, :config,
  endpoint: System.get_env("DELTA_SHARING_ENDPOINT"),
  bearer_token: System.get_env("DELTA_SHARING_BEARER_TOKEN"),
  share: "my_share",
  schema: "public",
  finch_name: :delta_query_finch  # or your existing Finch pool name
```

## Usage

### Basic Query

```elixir
"projects"
|> DeltaQuery.query()
|> DeltaQuery.where("company_id = 100")
|> DeltaQuery.select(["project_id", "name", "status"])
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

"projects"
|> DeltaQuery.query()
|> DeltaQuery.execute!(config: config)
```

### Joining Results

```elixir
projects = DeltaQuery.query("projects") |> DeltaQuery.execute!()
contracts = DeltaQuery.query("contracts") |> DeltaQuery.execute!()

joined = DeltaQuery.join(projects, contracts, on: "project_id", how: :inner)
DeltaQuery.to_rows(joined)
```

### Post-Query Filtering

```elixir
results = DeltaQuery.query("projects") |> DeltaQuery.execute!()

# Additional filters on fetched data
{:ok, filtered} = DeltaQuery.filter(results, ["square_feet > 50000"])

# Text search
{:ok, searched} = DeltaQuery.text_search(results, "keyword", ["subject", "body"])
```

### Aggregations

```elixir
results = DeltaQuery.query("projects") |> DeltaQuery.execute!()

DeltaQuery.aggregate_by_column(results, :status)
# => [%{status: "active", count: 42}, %{status: "closed", count: 18}]
```

## Configuration Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `:endpoint` | Yes | - | Delta Sharing server URL |
| `:bearer_token` | Yes | - | Authentication token |
| `:share` | Yes | - | Share name to query |
| `:schema` | No | `"public"` | Schema name |
| `:finch_name` | No | `:delta_query_finch` | Finch pool name |

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
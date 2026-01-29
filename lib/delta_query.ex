defmodule DeltaQuery do
  @moduledoc """
  Elixir client for querying Delta Sharing tables.

  Delta Sharing is an open protocol for secure data sharing across organizations.
  This library provides a high-level client for querying shared Delta tables. Results
  are returned as Elixir data structures, with optional access to the underlying
  Explorer DataFrames.

  ## Quick Start

  1. Configure credentials (config.exs or runtime):

      config :delta_query, :config,
        endpoint: "https://sharing.example.com",
        bearer_token: "your-token",
        share: "my_share",
        schema: "public"

  2. Query data:

      "books"
      |> DeltaQuery.query()
      |> DeltaQuery.where("library_id = 100")
      |> DeltaQuery.execute!()
      |> DeltaQuery.to_rows()

  ## Per-Query Configuration

  You can also pass configuration per-query:

      config = DeltaQuery.Config.new!(
        endpoint: "https://...",
        bearer_token: "...",
        share: "my_share"
      )

      "books"
      |> DeltaQuery.query()
      |> DeltaQuery.execute!(config: config)
  """

  alias DeltaQuery.Client
  alias DeltaQuery.Config
  alias DeltaQuery.Query
  alias DeltaQuery.Results

  @doc """
  List schemas in the configured share.

  ## Options

  - `:config` - A `DeltaQuery.Config` struct or keyword options

  ## Examples

      {:ok, schemas} = DeltaQuery.list_schemas()
      # => {:ok, ["public", "analytics"]}

      {:ok, schemas} = DeltaQuery.list_schemas(config: my_config)
  """
  @spec list_schemas(keyword()) :: {:ok, [String.t()]} | {:error, term()}
  def list_schemas(opts \\ []) do
    with {:ok, config} <- resolve_config(opts),
         client = Client.from_config(config),
         {:ok, items} <- Client.list_schemas(client, config.share) do
      schema_names = Enum.map(items, & &1["name"])
      {:ok, schema_names}
    end
  end

  @doc """
  List tables in a schema.

  ## Options

  - `:schema` - Schema name (defaults to configured schema)
  - `:config` - A `DeltaQuery.Config` struct or keyword options

  ## Examples

      {:ok, tables} = DeltaQuery.list_tables()
      # => {:ok, ["books", "loans", "members"]}

      {:ok, tables} = DeltaQuery.list_tables(schema: "analytics")

      {:ok, tables} = DeltaQuery.list_tables(config: my_config, schema: "analytics")
  """
  @spec list_tables(keyword()) :: {:ok, [String.t()]} | {:error, term()}
  def list_tables(opts \\ []) do
    with {:ok, config} <- resolve_config(opts),
         schema = Keyword.get(opts, :schema, config.schema),
         client = Client.from_config(config),
         {:ok, items} <- Client.list_tables(client, config.share, schema) do
      table_names = Enum.map(items, & &1["name"])
      {:ok, table_names}
    end
  end

  @doc """
  Get column names and types for a table.

  Returns a list of maps with `:name` and `:type` keys for each column.

  ## Options

  - `:table` - Table name (required)
  - `:schema` - Schema name (defaults to configured schema)
  - `:config` - A `DeltaQuery.Config` struct or keyword options

  ## Examples

      {:ok, columns} = DeltaQuery.get_table_schema(table: "books")
      # => {:ok, [
      #   %{name: "book_id", type: "long"},
      #   %{name: "title", type: "string"},
      #   %{name: "author", type: "string"}
      # ]}

      {:ok, columns} = DeltaQuery.get_table_schema(table: "books", schema: "analytics")
  """
  @spec get_table_schema(keyword()) :: {:ok, [map()]} | {:error, term()}
  def get_table_schema(opts \\ []) do
    table = Keyword.fetch!(opts, :table)

    with {:ok, config} <- resolve_config(opts),
         schema = Keyword.get(opts, :schema, config.schema),
         client = Client.from_config(config),
         {:ok, response} <- Client.table_metadata(client, config.share, schema, table) do
      columns = extract_columns(response)
      {:ok, columns}
    end
  end

  @doc """
  Create a new query for the given table.

  ## Examples

      DeltaQuery.query("books")
  """
  @spec query(String.t()) :: Query.t()
  defdelegate query(table), to: Query, as: :new

  @doc """
  Add a filter predicate to the query.

  ## Examples

      "books"
      |> DeltaQuery.query()
      |> DeltaQuery.where("library_id = 100")
  """
  @spec where(Query.t(), String.t()) :: Query.t()
  defdelegate where(query, filter), to: Query

  @doc """
  Select specific columns to return.

  ## Examples

      "books"
      |> DeltaQuery.query()
      |> DeltaQuery.select(["book_id", "title"])
  """
  @spec select(Query.t(), list(String.t())) :: Query.t()
  defdelegate select(query, columns), to: Query

  @doc """
  Set a limit hint for the query.

  ## Examples

      "books"
      |> DeltaQuery.query()
      |> DeltaQuery.limit(100)
  """
  @spec limit(Query.t(), pos_integer()) :: Query.t()
  defdelegate limit(query, n), to: Query

  @doc """
  Execute the query and return results.

  ## Options

  - `:config` - A `DeltaQuery.Config` struct or keyword options

  ## Examples

      {:ok, results} =
        "books"
        |> DeltaQuery.query()
        |> DeltaQuery.execute()
  """
  @spec execute(Query.t(), keyword()) :: {:ok, Results.t()} | {:error, term()}
  defdelegate execute(query, opts \\ []), to: Query

  @doc """
  Execute the query, raising on error.

  ## Examples

      results =
        "books"
        |> DeltaQuery.query()
        |> DeltaQuery.execute!()
  """
  @spec execute!(Query.t(), keyword()) :: Results.t()
  defdelegate execute!(query, opts \\ []), to: Query

  @doc """
  Convert results to a list of maps.

  ## Examples

      "books"
      |> DeltaQuery.query()
      |> DeltaQuery.execute!()
      |> DeltaQuery.to_rows()
  """
  @spec to_rows(Results.t()) :: list(map())
  defdelegate to_rows(results), to: Results

  @doc """
  Join two result sets on a common column.

  ## Options

  - `:on` - Column name or list of column names to join on (required)
  - `:how` - Join type: `:left` (default), `:right`, `:inner`, `:outer`, `:cross`

  ## Examples

      books = DeltaQuery.query("books") |> DeltaQuery.execute!()
      publishers = DeltaQuery.query("publishers") |> DeltaQuery.execute!()
      joined = DeltaQuery.join(books, publishers, on: "book_id")
  """
  @spec join(Results.t(), Results.t(), keyword()) :: Results.t()
  defdelegate join(left, right, opts), to: Results

  @doc """
  Apply additional filters to already-fetched results.

  Prefer `where/2` for initial filtering - it's more efficient.
  Use this only for filtering joined results or post-query filtering.

  ## Examples

      {:ok, filtered} = DeltaQuery.filter(results, ["page_count > 300"])
  """
  @spec filter(Results.t(), list(String.t())) :: {:ok, Results.t()} | {:error, String.t()}
  defdelegate filter(results, predicates), to: Results

  @doc """
  Apply text search to results.

  Searches across specified columns using case-insensitive substring matching.

  ## Examples

      {:ok, searched} = DeltaQuery.text_search(results, "science fiction", ["genre", "title"])
  """
  @spec text_search(Results.t(), String.t(), list(String.t())) ::
          {:ok, Results.t()} | {:error, String.t()}
  defdelegate text_search(results, search_text, columns), to: Results

  @doc """
  Aggregate results by grouping on a column and counting occurrences.

  ## Examples

      DeltaQuery.aggregate_by_column(results, :book_id)
      # => [%{book_id: 1001, count: 5}, %{book_id: 1002, count: 3}]
  """
  @spec aggregate_by_column(Results.t(), atom()) :: list(map())
  defdelegate aggregate_by_column(results, column), to: Results

  @doc """
  Return the number of rows in the results.

  ## Examples

      DeltaQuery.count(results)
      # => 42
  """
  @spec count(Results.t()) :: non_neg_integer()
  defdelegate count(results), to: Results

  @doc """
  Check if results are empty.

  ## Examples

      DeltaQuery.empty?(results)
      # => false
  """
  @spec empty?(Results.t()) :: boolean()
  defdelegate empty?(results), to: Results

  @doc """
  Return the first row as a map, or nil if empty.

  ## Examples

      DeltaQuery.first(results)
      # => %{"book_id" => 1001, "title" => "The Great Gatsby"}
  """
  @spec first(Results.t()) :: map() | nil
  defdelegate first(results), to: Results

  @doc """
  Sum a numeric column, returning 0 if the column doesn't exist.

  ## Examples

      DeltaQuery.sum(results, "amount")
      # => 12500.0
  """
  @spec sum(Results.t(), String.t()) :: number()
  defdelegate sum(results, column), to: Results

  @doc """
  Create a configuration struct.

  ## Options

  - `:endpoint` - Delta Sharing server URL (required)
  - `:bearer_token` - Authentication token (required)
  - `:share` - Share name (required)
  - `:schema` - Schema name (default: "public")
  - `:req_options` - Options passed to Req requests (default: `[]`)

  ## Examples

      {:ok, config} = DeltaQuery.configure(
        endpoint: "https://...",
        bearer_token: "...",
        share: "my_share"
      )
  """
  @spec configure(keyword()) :: {:ok, Config.t()} | {:error, String.t()}
  defdelegate configure(opts), to: Config, as: :new

  @doc """
  Create a configuration struct, raising on error.

  ## Examples

      config = DeltaQuery.configure!(
        endpoint: "https://...",
        bearer_token: "...",
        share: "my_share"
      )
  """
  @spec configure!(keyword()) :: Config.t()
  defdelegate configure!(opts), to: Config, as: :new!

  defp resolve_config(opts) do
    case Keyword.get(opts, :config) do
      %Config{} = config ->
        {:ok, config}

      nil ->
        Config.new(opts)

      config_opts when is_list(config_opts) ->
        Config.new(config_opts)
    end
  end

  @doc false
  def extract_columns(%{metadata: %{"metaData" => %{"schemaString" => schema_string}}}) do
    case Jason.decode(schema_string) do
      {:ok, %{"fields" => fields}} ->
        Enum.map(fields, fn field ->
          %{
            name: field["name"],
            type: normalize_type(field["type"])
          }
        end)

      _ ->
        []
    end
  end

  @doc false
  def extract_columns(_), do: []

  defp normalize_type(type) when is_binary(type), do: type
  defp normalize_type(%{"type" => "struct"}), do: "struct"
  defp normalize_type(%{"type" => "array"}), do: "array"
  defp normalize_type(%{"type" => "map"}), do: "map"
  defp normalize_type(_), do: "unknown"
end

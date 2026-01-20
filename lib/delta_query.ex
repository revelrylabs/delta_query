defmodule DeltaQuery do
  @moduledoc """
  Elixir client for querying Delta Sharing tables.

  Delta Sharing is an open protocol for secure data sharing across organizations.
  This library provides a high-level client for querying shared Delta tables,
  downloading Parquet files, and working with the data as Explorer DataFrames.

  ## Quick Start

  1. Configure your Finch pool in your application supervision tree:

      children = [
        {Finch, name: :delta_query_finch}
      ]

  2. Configure credentials (config.exs or runtime):

      config :delta_query, :config,
        endpoint: "https://sharing.example.com",
        bearer_token: "your-token",
        share: "my_share",
        schema: "public"

  3. Query data:

      "projects"
      |> DeltaQuery.query()
      |> DeltaQuery.where("company_id = 100")
      |> DeltaQuery.execute!()
      |> DeltaQuery.to_rows()

  ## Per-Query Configuration

  You can also pass configuration per-query:

      config = DeltaQuery.Config.new!(
        endpoint: "https://...",
        bearer_token: "...",
        share: "my_share"
      )

      "projects"
      |> DeltaQuery.query()
      |> DeltaQuery.execute!(config: config)
  """

  alias DeltaQuery.Config
  alias DeltaQuery.Query
  alias DeltaQuery.Results

  @doc """
  Create a new query for the given table.

  ## Examples

      DeltaQuery.query("projects")
  """
  @spec query(String.t()) :: Query.t()
  defdelegate query(table), to: Query, as: :new

  @doc """
  Add a filter predicate to the query.

  ## Examples

      "projects"
      |> DeltaQuery.query()
      |> DeltaQuery.where("company_id = 100")
  """
  @spec where(Query.t(), String.t()) :: Query.t()
  defdelegate where(query, filter), to: Query

  @doc """
  Select specific columns to return.

  ## Examples

      "projects"
      |> DeltaQuery.query()
      |> DeltaQuery.select(["project_id", "name"])
  """
  @spec select(Query.t(), list(String.t())) :: Query.t()
  defdelegate select(query, columns), to: Query

  @doc """
  Set a limit hint for the query.

  ## Examples

      "projects"
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
        "projects"
        |> DeltaQuery.query()
        |> DeltaQuery.execute()
  """
  @spec execute(Query.t(), keyword()) :: {:ok, Results.t()} | {:error, term()}
  defdelegate execute(query, opts \\ []), to: Query

  @doc """
  Execute the query, raising on error.

  ## Examples

      results =
        "projects"
        |> DeltaQuery.query()
        |> DeltaQuery.execute!()
  """
  @spec execute!(Query.t(), keyword()) :: Results.t()
  defdelegate execute!(query, opts \\ []), to: Query

  @doc """
  Convert results to a list of maps.

  ## Examples

      "projects"
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

      projects = DeltaQuery.query("projects") |> DeltaQuery.execute!()
      contracts = DeltaQuery.query("contracts") |> DeltaQuery.execute!()
      joined = DeltaQuery.join(projects, contracts, on: "project_id")
  """
  @spec join(Results.t(), Results.t(), keyword()) :: Results.t()
  defdelegate join(left, right, opts), to: Results

  @doc """
  Apply additional filters to already-fetched results.

  Prefer `where/2` for initial filtering - it's more efficient.
  Use this only for filtering joined results or post-query filtering.

  ## Examples

      {:ok, filtered} = DeltaQuery.filter(results, ["square_feet > 50000"])
  """
  @spec filter(Results.t(), list(String.t())) :: {:ok, Results.t()} | {:error, String.t()}
  defdelegate filter(results, predicates), to: Results

  @doc """
  Apply text search to results.

  Searches across specified columns using case-insensitive substring matching.

  ## Examples

      {:ok, searched} = DeltaQuery.text_search(results, "waterproofing", ["subject", "answer"])
  """
  @spec text_search(Results.t(), String.t(), list(String.t())) ::
          {:ok, Results.t()} | {:error, String.t()}
  defdelegate text_search(results, search_text, columns), to: Results

  @doc """
  Aggregate results by grouping on a column and counting occurrences.

  ## Examples

      DeltaQuery.aggregate_by_column(results, :project_id)
      # => [%{project_id: 1001, count: 5}, %{project_id: 1002, count: 3}]
  """
  @spec aggregate_by_column(Results.t(), atom()) :: list(map())
  defdelegate aggregate_by_column(results, column), to: Results

  @doc """
  Create a configuration struct.

  ## Options

  - `:endpoint` - Delta Sharing server URL (required)
  - `:bearer_token` - Authentication token (required)
  - `:share` - Share name (required)
  - `:schema` - Schema name (default: "public")
  - `:finch_name` - Finch pool name (default: `:delta_query_finch`)

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
end

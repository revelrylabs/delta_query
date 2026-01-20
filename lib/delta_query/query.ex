defmodule DeltaQuery.Query do
  @moduledoc """
  Composable query builder for Delta Sharing tables.

  ## Example

      "projects"
      |> Query.new()
      |> Query.where("company_id = 100")
      |> Query.select(["project_id", "name"])
      |> Query.execute!()
      |> Results.to_rows()

  For post-query operations (joins, filtering, conversion to rows), see `DeltaQuery.Results`.
  """

  alias DeltaQuery.Client
  alias DeltaQuery.Config
  alias DeltaQuery.Results

  require Explorer.DataFrame
  require Logger

  @enforce_keys [:table]
  defstruct [:table, columns: nil, filters: [], limit: nil]

  @type t :: %__MODULE__{
          table: String.t(),
          columns: list(String.t()) | nil,
          filters: list(String.t()),
          limit: pos_integer() | nil
        }

  @doc """
  Create a new query for the given table.

  ## Examples

      iex> DeltaQuery.Query.new("projects")
      %DeltaQuery.Query{table: "projects"}

  """
  @spec new(String.t()) :: t()
  def new(table) when is_binary(table) do
    %__MODULE__{table: table}
  end

  @doc """
  Add a filter predicate to the query.

  Predicates use SQL-like syntax: `column operator value`

  Supported operators: `=`, `!=`, `>`, `<`, `>=`, `<=`

  ## Examples

      "projects"
      |> Query.new()
      |> Query.where("company_id = 100")
      |> Query.where("status = 'active'")

  """
  @spec where(t(), String.t()) :: t()
  def where(%__MODULE__{filters: filters} = query, filter) when is_binary(filter) do
    %{query | filters: filters ++ [filter]}
  end

  @doc """
  Select specific columns to return.

  If not called, all columns are returned. Calling multiple times overwrites previous selections.

  ## Examples

      "projects"
      |> Query.new()
      |> Query.select(["project_id", "name", "address"])

  """
  @spec select(t(), list(String.t())) :: t()
  def select(%__MODULE__{} = query, columns) when is_list(columns) do
    %{query | columns: columns}
  end

  @doc """
  Set a limit hint for the query.

  Note: This is a hint to the Delta Sharing server and may not be strictly enforced.
  Calling multiple times overwrites the previous limit.

  ## Examples

      "projects"
      |> Query.new()
      |> Query.limit(100)

  """
  @spec limit(t(), pos_integer()) :: t()
  def limit(%__MODULE__{} = query, n) when is_integer(n) and n > 0 do
    %{query | limit: n}
  end

  @doc """
  Execute the query and return results.

  ## Options

  - `:config` - A `DeltaQuery.Config` struct or keyword options for configuration.
    If not provided, configuration is read from application environment.

  Returns `{:ok, %Results{}}` on success or `{:error, reason}` on failure.

  ## Examples

      {:ok, result} =
        "projects"
        |> Query.new()
        |> Query.select(["project_id", "name"])
        |> Query.execute()

      result.dataframe        #=> %Explorer.DataFrame{...}
      result.files_processed  #=> 5
      result.total_files      #=> 5

      # With explicit config
      config = DeltaQuery.Config.new!(endpoint: "...", bearer_token: "...", share: "my_share")
      {:ok, result} = Query.execute(query, config: config)
  """
  @spec execute(t(), keyword()) :: {:ok, Results.t()} | {:error, term()}
  def execute(%__MODULE__{} = query, opts \\ []) do
    with {:ok, config} <- get_config(opts),
         client = Client.from_config(config),
         {:ok, %{files: files}} when is_list(files) <- query_table(client, config, query) do
      process_files(files, query, config)
    else
      {:ok, _empty_response} ->
        {:ok, empty_results(query.columns)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Execute the query and return results, raising on error.

  Returns `%Results{}` on success, raises on failure.

  ## Examples

      "projects"
      |> Query.new()
      |> Query.where("company_id = 100")
      |> Query.execute!()
      |> Results.to_rows()
  """
  @spec execute!(t(), keyword()) :: Results.t()
  def execute!(%__MODULE__{} = query, opts \\ []) do
    case execute(query, opts) do
      {:ok, results} -> results
      {:error, reason} -> raise "Query execution failed: #{inspect(reason)}"
    end
  end

  defp process_files([], query, _config) do
    {:ok, empty_results(query.columns)}
  end

  defp process_files(files, query, config) do
    total_files = length(files)

    {:ok, df} =
      Client.parse_parquet_files(files,
        predicates: query.filters,
        columns: query.columns,
        finch_name: config.finch_name
      )

    {:ok, %Results{dataframe: df, files_processed: total_files, total_files: total_files}}
  end

  defp empty_results(columns) do
    %Results{dataframe: empty_dataframe(columns), files_processed: 0, total_files: 0}
  end

  defp empty_dataframe(nil), do: Explorer.DataFrame.new([])

  defp empty_dataframe(columns) when is_list(columns) do
    columns
    |> Map.new(fn col -> {col, []} end)
    |> Explorer.DataFrame.new()
  end

  defp get_config(opts) do
    case Keyword.get(opts, :config) do
      %Config{} = config ->
        {:ok, config}

      config_opts when is_list(config_opts) ->
        Config.new(config_opts)

      nil ->
        Config.new()
    end
  end

  defp query_table(client, config, %__MODULE__{} = query) do
    share = config.share
    schema = config.schema

    query_opts =
      []
      |> maybe_add_limit(query.limit)
      |> maybe_add_predicates(query.filters)

    Client.query_table(client, share, schema, query.table, query_opts)
  end

  defp maybe_add_limit(opts, nil), do: opts
  defp maybe_add_limit(opts, limit), do: Keyword.put(opts, :limit, limit)

  defp maybe_add_predicates(opts, []), do: opts
  defp maybe_add_predicates(opts, filters), do: Keyword.put(opts, :predicate_hints, filters)
end

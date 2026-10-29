defmodule DeltaQuery.Query do
  @moduledoc """
  Composable query builder for Delta Sharing tables.

  ## Example

      alias DeltaQuery.{Query, Results}

      "books"
      |> Query.new()
      |> Query.where("library_id = 100")
      |> Query.select(["book_id", "title"])
      |> Query.execute!()
      |> Results.to_rows()

  Predicates passed to `where/2` are sent to the server as partition hints and applied again
  to every downloaded row.

  For post-query operations (joins, filtering, conversion to rows), see `DeltaQuery.Results`.
  """

  alias DeltaQuery.Client
  alias DeltaQuery.Config
  alias DeltaQuery.Results

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

      iex> DeltaQuery.Query.new("books")
      %DeltaQuery.Query{table: "books"}

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

      "books"
      |> Query.new()
      |> Query.where("library_id = 100")
      |> Query.where("genre = 'Fiction'")

  """
  @spec where(t(), String.t()) :: t()
  def where(%__MODULE__{filters: filters} = query, filter) when is_binary(filter) do
    %{query | filters: filters ++ [filter]}
  end

  @doc """
  Select specific columns to return.

  If not called, all columns are returned. Calling multiple times overwrites previous selections.

  ## Examples

      "books"
      |> Query.new()
      |> Query.select(["book_id", "title", "author"])

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

      "books"
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

  Returns `{:ok, %Results{}}` on success or `{:error, reason}` on failure. See `DeltaQuery.Results`
  for the meaning of `files_processed` and `total_files`.

  ## Examples

      {:ok, result} =
        "books"
        |> Query.new()
        |> Query.select(["book_id", "title"])
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
         {:ok, response} <- query_table(client, config, query) do
      Client.parse_parquet_files(client, response, predicates: query.filters, columns: query.columns)
    end
  end

  @doc """
  Execute the query and return results, raising on error.

  Returns `%Results{}` on success, raises on failure.

  ## Examples

      "books"
      |> Query.new()
      |> Query.where("library_id = 100")
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

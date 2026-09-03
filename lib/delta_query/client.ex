defmodule DeltaQuery.Client do
  @moduledoc """
  HTTP client for Delta Sharing REST API.

  Implements the Delta Sharing Protocol for reading shared Delta tables.
  See: https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md

  ## Parquet Files

  Delta Sharing returns data as Parquet files, a columnar storage format optimized for
  analytical queries. This client downloads Parquet files and parses them using Explorer.

  See: https://parquet.apache.org/docs/

  ## Predicates

  Predicates are SQL-like filter expressions used to reduce data transfer and improve performance.
  They work at two levels:

  1. **Partition filtering** - Server-side filtering that excludes entire Parquet files based on
     partition values, reducing the number of files downloaded.

  2. **Row filtering** - Client-side filtering applied after downloading Parquet files to further
     narrow results to matching rows.

  Example predicates: `["book_id = 123", "genre = 'Fiction'", "publication_date > '2024-01-01'"]`
  """

  alias DeltaQuery.Config
  alias DeltaQuery.PredicateParser
  alias Explorer.DataFrame
  alias Explorer.Series

  require Logger

  defstruct [:req, :download_req]

  @type t :: %__MODULE__{req: Req.Request.t(), download_req: Req.Request.t()}

  @doc """
  Create a new client from endpoint and bearer token.

  ## Options

  Any additional options are passed to `Req.new/1` (e.g., `:retry`, `:connect_options`).
  """
  @spec new(String.t(), String.t(), keyword()) :: t()
  def new(endpoint, bearer_token, opts \\ []) do
    req =
      opts
      |> Keyword.merge(
        base_url: endpoint,
        headers: [{"authorization", "Bearer #{bearer_token}"}]
      )
      |> Req.new()

    download_req = Req.new(opts)

    %__MODULE__{req: req, download_req: download_req}
  end

  @doc """
  Create a new client from a Config struct.
  """
  @spec from_config(Config.t()) :: t()
  def from_config(%Config{} = config) do
    new(config.endpoint, config.bearer_token, config.req_options)
  end

  @doc """
  List schemas in a share.

  Returns a list of schema metadata maps.
  """
  @spec list_schemas(t(), String.t()) :: {:ok, list(map())} | {:error, term()}
  def list_schemas(%__MODULE__{} = client, share) do
    get_request(client, "/shares/#{URI.encode(share)}/schemas")
  end

  @doc """
  List tables in a schema.

  Returns a list of table metadata maps.
  """
  @spec list_tables(t(), String.t(), String.t()) :: {:ok, list(map())} | {:error, term()}
  def list_tables(%__MODULE__{} = client, share, schema) do
    get_request(client, "/shares/#{URI.encode(share)}/schemas/#{URI.encode(schema)}/tables")
  end

  @doc """
  Get table metadata including schema (column names and types).

  Returns the protocol and metadata from the table, including the schema string.
  """
  @spec table_metadata(t(), String.t(), String.t(), String.t()) ::
          {:ok, map()} | {:error, term()}
  def table_metadata(%__MODULE__{} = client, share, schema, table) do
    path =
      "/shares/#{URI.encode(share)}/schemas/#{URI.encode(schema)}/tables/#{URI.encode(table)}/metadata"

    with {:ok, body} <- get_raw(client, path) do
      parse_metadata_response(body)
    end
  end

  @doc """
  Query table data with optional predicates and limits.

  ## Options

  - `:limit` - Maximum number of rows to return (hint to server)
  - `:predicate_hints` - SQL-like predicates for filtering (e.g., ["date > '2024-01-01'"])
  """
  @spec query_table(t(), String.t(), String.t(), String.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def query_table(%__MODULE__{} = client, share, schema, table, opts \\ []) do
    body =
      %{}
      |> maybe_add_limit(Keyword.get(opts, :limit))
      |> maybe_add_predicates(Keyword.get(opts, :predicate_hints))

    post_request(
      client,
      "/shares/#{URI.encode(share)}/schemas/#{URI.encode(schema)}/tables/#{URI.encode(table)}/query",
      body
    )
  end

  @doc """
  Download and parse Parquet files from Delta Sharing query response.

  Returns `{:ok, dataframe}` on success, where the dataframe is an `Explorer.DataFrame`
  enabling joins, grouping, and aggregations. Use `Explorer.DataFrame.to_rows/1` to
  convert it to a list of maps if needed.

  Returns `{:error, message}` when a predicate cannot be applied, for example when an
  ordering operator (`>`, `<`, `>=`, `<=`) targets a column whose type does not support
  ordering. Processing stops at the first such error. Files that fail to download or
  parse are logged and skipped rather than returned as errors.

  ## Options

  - `:predicates` - List of SQL-like filter strings (e.g., ["genre = 'Fiction'", "book_id = 123"])
  - `:columns` - List of column names to return (nil = all columns)
  """
  @spec parse_parquet_files(t(), list(map()), keyword()) ::
          {:ok, DataFrame.t()} | {:error, String.t()}
  def parse_parquet_files(%__MODULE__{} = client, files, opts \\ []) do
    predicates = Keyword.get(opts, :predicates, [])
    columns = Keyword.get(opts, :columns)

    parsed_predicates = parse_predicates(predicates)
    relevant_files = filter_files_by_partitions(files, parsed_predicates)

    process_files_to_dataframe(client, relevant_files, parsed_predicates, columns)
  end

  defp process_files_to_dataframe(client, files, parsed_predicates, columns) do
    total_files = length(files)

    result =
      files
      |> Enum.with_index(1)
      |> Enum.reduce_while({:ok, []}, fn {file, index}, {:ok, dfs_acc} ->
        case download_and_parse_parquet_df(client, file, parsed_predicates, columns) do
          {:ok, df} ->
            if DataFrame.n_rows(df) > 0 do
              {:cont, {:ok, [df | dfs_acc]}}
            else
              {:cont, {:ok, dfs_acc}}
            end

          {:error, {:invalid_predicate, message}} ->
            {:halt, {:error, message}}

          {:error, reason} ->
            Logger.error("failed to parse file #{index}/#{total_files}: #{inspect(reason)}")
            {:cont, {:ok, dfs_acc}}
        end
      end)

    with {:ok, dataframes} <- result do
      case Enum.reverse(dataframes) do
        [] ->
          {:ok, empty_dataframe(columns)}

        [single] ->
          {:ok, single}

        multiple ->
          {:ok, concat_with_common_columns(multiple)}
      end
    end
  end

  defp empty_dataframe(nil), do: DataFrame.new([])

  defp empty_dataframe(columns) when is_list(columns) do
    columns
    |> Map.new(fn col -> {col, []} end)
    |> DataFrame.new()
  end

  defp concat_with_common_columns(dataframes) do
    common_columns =
      dataframes
      |> Enum.map(&DataFrame.names/1)
      |> Enum.map(&MapSet.new/1)
      |> Enum.reduce(&MapSet.intersection/2)
      |> MapSet.to_list()

    dataframes
    |> Enum.map(&DataFrame.select(&1, common_columns))
    |> DataFrame.concat_rows()
  end

  defp get_request(client, path) do
    case Req.get(client.req, url: path) do
      {:ok, %Req.Response{status: 200, body: response_body}} ->
        items = Map.get(response_body, "items", [])
        {:ok, items}

      response ->
        handle_error_response(response)
    end
  end

  defp get_raw(client, path) do
    case Req.get(client.req, url: path, decode_body: false) do
      {:ok, %Req.Response{status: 200, body: response_body}} ->
        {:ok, response_body}

      response ->
        handle_error_response(response)
    end
  end

  defp post_request(client, path, body) do
    case Req.post(client.req, url: path, json: body, decode_body: false) do
      {:ok, %Req.Response{status: 200, body: response_body}} ->
        parse_query_response(response_body)

      response ->
        handle_error_response(response)
    end
  end

  defp handle_error_response({:ok, %Req.Response{status: status, body: response_body}}) do
    Logger.error("delta sharing api error: status=#{status} body=#{inspect(response_body)}")
    {:error, {:api_error, status, response_body}}
  end

  defp handle_error_response({:error, reason}) do
    Logger.error("delta sharing request failed: #{inspect(reason)}")
    {:error, {:request_failed, reason}}
  end

  defp parse_metadata_response(body) do
    case parse_ndjson(body) do
      [{:ok, protocol}, {:ok, metadata}] ->
        {:ok, %{protocol: protocol, metadata: metadata}}

      _ ->
        {:error, :invalid_response_format}
    end
  end

  defp parse_query_response(body) do
    case parse_ndjson(body) do
      [{:ok, protocol}, {:ok, metadata} | file_actions] ->
        files =
          file_actions
          |> Enum.map(fn
            # Delta Sharing protocol v1: {"add": {...}}
            {:ok, %{"add" => add}} -> add
            # Delta Sharing protocol v2: {"file": {...}}
            {:ok, %{"file" => file}} -> file
            _ -> nil
          end)
          |> Enum.reject(&is_nil/1)

        {:ok, %{protocol: protocol, metadata: metadata, files: files}}

      _ ->
        {:error, :invalid_response_format}
    end
  end

  defp parse_ndjson(body) do
    body
    |> String.split("\n", trim: true)
    |> Enum.map(&Jason.decode/1)
  end

  defp maybe_add_limit(body, nil), do: body
  defp maybe_add_limit(body, limit), do: Map.put(body, "limitHint", limit)

  defp maybe_add_predicates(body, nil), do: body
  defp maybe_add_predicates(body, []), do: body
  defp maybe_add_predicates(body, predicates), do: Map.put(body, "predicateHints", predicates)

  defp download_and_parse_parquet_df(client, %{"url" => url} = _file, parsed_predicates, columns) do
    case Req.get(client.download_req, url: url, decode_body: false) do
      {:ok, %Req.Response{status: 200, body: parquet_data}} ->
        parse_parquet_to_df(parquet_data, parsed_predicates, columns)

      {:ok, %Req.Response{status: status, body: body}} ->
        body_preview = if is_binary(body), do: String.slice(body, 0..100), else: inspect(body)
        Logger.error("failed to download parquet file: status=#{status} preview=#{body_preview}")

        {:error, {:download_failed, status}}

      {:error, reason} ->
        Logger.error("request failed for parquet file: #{inspect(reason)}")
        {:error, {:request_failed, reason}}
    end
  end

  defp parse_parquet_to_df(parquet_binary, parsed_predicates, columns) do
    case DataFrame.load_parquet(parquet_binary) do
      {:ok, df} ->
        with {:ok, filtered} <- apply_predicates(df, parsed_predicates) do
          {:ok, select_columns(filtered, columns)}
        end

      {:error, reason} ->
        Logger.error("failed to load parquet data: #{inspect(reason)}")
        {:error, {:parse_failed, reason}}
    end
  end

  defp parse_predicates(predicates) when is_list(predicates) do
    Enum.map(predicates, &parse_predicate/1)
  end

  defp parse_predicate(predicate) when is_binary(predicate) do
    case PredicateParser.parse_predicate(predicate) do
      {:ok, parsed} ->
        parsed

      {:error, reason} ->
        Logger.error("failed to parse predicate '#{predicate}': #{reason}")
        nil
    end
  end

  defp filter_files_by_partitions(files, []), do: files

  defp filter_files_by_partitions(files, parsed_predicates) do
    predicates_without_nil = Enum.reject(parsed_predicates, &is_nil/1)

    if Enum.empty?(predicates_without_nil) do
      files
    else
      Enum.filter(files, fn file ->
        partition_values = Map.get(file, "partitionValues", %{})
        file_matches_predicates?(partition_values, predicates_without_nil)
      end)
    end
  end

  defp file_matches_predicates?(_partition_values, []), do: true

  defp file_matches_predicates?(partition_values, predicates) do
    Enum.all?(predicates, fn {op, column, value} ->
      case Map.get(partition_values, column) do
        nil -> true
        partition_value -> matches_predicate?(op, normalize_value(partition_value), value)
      end
    end)
  end

  defp matches_predicate?(:eq, partition_value, value), do: partition_value == value
  defp matches_predicate?(:neq, partition_value, value), do: partition_value != value
  defp matches_predicate?(:gt, partition_value, value), do: partition_value > value
  defp matches_predicate?(:lt, partition_value, value), do: partition_value < value
  defp matches_predicate?(:gte, partition_value, value), do: partition_value >= value
  defp matches_predicate?(:lte, partition_value, value), do: partition_value <= value

  defp normalize_value(value) when is_binary(value) do
    cond do
      Regex.match?(~r/^-?\d+$/, value) -> String.to_integer(value)
      Regex.match?(~r/^-?\d+\.\d+$/, value) -> String.to_float(value)
      true -> value
    end
  end

  defp normalize_value(value), do: value

  defp apply_predicates(df, predicates) when is_list(predicates) do
    Enum.reduce_while(predicates, {:ok, df}, fn predicate, {:ok, acc} ->
      case apply_predicate_to_df(acc, predicate) do
        {:ok, filtered} -> {:cont, {:ok, filtered}}
        {:error, message} -> {:halt, {:error, {:invalid_predicate, message}}}
      end
    end)
  end

  defp apply_predicate_to_df(df, {op, column, value}), do: apply_filter(df, op, column, value)
  defp apply_predicate_to_df(df, nil), do: {:ok, df}

  defp select_columns(df, nil), do: df

  defp select_columns(df, columns) when is_list(columns) do
    available_columns = DataFrame.names(df)
    valid_columns = Enum.filter(columns, &(&1 in available_columns))

    if Enum.empty?(valid_columns) do
      Logger.error(
        "no valid columns found in dataframe: requested=#{inspect(columns)} available=#{inspect(available_columns)}"
      )

      df
    else
      DataFrame.select(df, valid_columns)
    end
  end

  defp apply_filter(df, operation, column, value) when is_binary(column) do
    if column in DataFrame.names(df) do
      dtypes = DataFrame.dtypes(df)
      column_type = Map.get(dtypes, column)

      if operation in [:gt, :lt, :gte, :lte] and not PredicateParser.orderable_dtype?(column_type) do
        {:error, "operator #{operator_label(operation)} not supported on #{inspect(column_type)} column '#{column}'"}
      else
        normalized_value = PredicateParser.normalize_value(column_type, value)

        filtered =
          DataFrame.filter_with(df, fn lf ->
            case operation do
              :eq -> Series.equal(lf[column], normalized_value)
              :neq -> Series.not_equal(lf[column], normalized_value)
              :gt -> Series.greater(lf[column], normalized_value)
              :lt -> Series.less(lf[column], normalized_value)
              :gte -> Series.greater_equal(lf[column], normalized_value)
              :lte -> Series.less_equal(lf[column], normalized_value)
            end
          end)

        {:ok, filtered}
      end
    else
      {:ok, df}
    end
  end

  defp operator_label(:gt), do: ">"
  defp operator_label(:lt), do: "<"
  defp operator_label(:gte), do: ">="
  defp operator_label(:lte), do: "<="
end

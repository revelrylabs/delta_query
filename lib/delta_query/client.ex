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

  Example predicates: `["project_id = 123", "status = 'open'", "created_at > '2024-01-01'"]`
  """

  require Logger

  alias DeltaQuery.Config

  defstruct [:endpoint, :bearer_token, :finch_name]

  @type t :: %__MODULE__{
          endpoint: String.t(),
          bearer_token: String.t(),
          finch_name: atom()
        }

  @doc """
  Create a new client from endpoint and bearer token.
  """
  @spec new(String.t(), String.t(), keyword()) :: t()
  def new(endpoint, bearer_token, opts \\ []) do
    finch_name = Keyword.get(opts, :finch_name, :delta_query_finch)

    %__MODULE__{
      endpoint: endpoint,
      bearer_token: bearer_token,
      finch_name: finch_name
    }
  end

  @doc """
  Create a new client from a Config struct.
  """
  @spec from_config(Config.t()) :: t()
  def from_config(%Config{} = config) do
    new(config.endpoint, config.bearer_token, finch_name: config.finch_name)
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

  Returns an Explorer DataFrame, enabling joins, grouping, and aggregations.
  Use `Explorer.DataFrame.to_rows/1` to convert to a list of maps if needed.

  ## Options

  - `:predicates` - List of SQL-like filter strings (e.g., ["status = 'open'", "project_id = 123"])
  - `:columns` - List of column names to return (nil = all columns)
  - `:finch_name` - Finch pool name for downloading files (default: `:delta_query_finch`)
  """
  @spec parse_parquet_files(list(map()), keyword()) ::
          {:ok, Explorer.DataFrame.t()} | {:error, term()}
  def parse_parquet_files(files, opts \\ []) do
    predicates = Keyword.get(opts, :predicates, [])
    columns = Keyword.get(opts, :columns)
    finch_name = Keyword.get(opts, :finch_name, :delta_query_finch)

    parsed_predicates = parse_predicates(predicates)
    relevant_files = filter_files_by_partitions(files, parsed_predicates)

    df = process_files_to_dataframe(relevant_files, parsed_predicates, columns, finch_name)

    {:ok, df}
  end

  defp process_files_to_dataframe(files, parsed_predicates, columns, finch_name) do
    total_files = length(files)

    dataframes =
      files
      |> Enum.with_index(1)
      |> Enum.reduce([], fn {file, index}, dfs_acc ->
        case download_and_parse_parquet_df(file, parsed_predicates, columns, finch_name) do
          {:ok, df} ->
            if Explorer.DataFrame.n_rows(df) > 0 do
              [df | dfs_acc]
            else
              dfs_acc
            end

          {:error, reason} ->
            Logger.error("failed to parse file #{index}/#{total_files}: #{inspect(reason)}")
            dfs_acc
        end
      end)
      |> Enum.reverse()

    case dataframes do
      [] ->
        empty_dataframe(columns)

      [single] ->
        single

      multiple ->
        concat_with_common_columns(multiple)
    end
  end

  defp empty_dataframe(nil), do: Explorer.DataFrame.new([])

  defp empty_dataframe(columns) when is_list(columns) do
    columns
    |> Map.new(fn col -> {col, []} end)
    |> Explorer.DataFrame.new()
  end

  defp concat_with_common_columns(dataframes) do
    common_columns =
      dataframes
      |> Enum.map(&Explorer.DataFrame.names/1)
      |> Enum.map(&MapSet.new/1)
      |> Enum.reduce(&MapSet.intersection/2)
      |> MapSet.to_list()

    dataframes
    |> Enum.map(&Explorer.DataFrame.select(&1, common_columns))
    |> Explorer.DataFrame.concat_rows()
  end

  defp post_request(client, path, body) do
    url = client.endpoint <> path

    headers = [
      {"authorization", "Bearer #{client.bearer_token}"},
      {"content-type", "application/json"}
    ]

    json_body = Jason.encode!(body)

    case :post
         |> Finch.build(url, headers, json_body)
         |> Finch.request(client.finch_name) do
      {:ok, %Finch.Response{status: 200, body: response_body}} ->
        parse_query_response(response_body)

      {:ok, %Finch.Response{status: status, body: response_body}} ->
        Logger.error("delta sharing api error: status=#{status} body=#{response_body}")
        {:error, {:api_error, status, response_body}}

      {:error, reason} ->
        Logger.error("delta sharing request failed: #{inspect(reason)}")
        {:error, {:request_failed, reason}}
    end
  end

  defp parse_query_response(body) do
    # Delta Sharing query response is newline-delimited JSON
    # First line: protocol metadata
    # Second line: metadata about the table
    # Following lines: add/remove file actions
    lines =
      body
      |> String.split("\n", trim: true)
      |> Enum.map(&Jason.decode/1)

    case lines do
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

  defp maybe_add_limit(body, nil), do: body
  defp maybe_add_limit(body, limit), do: Map.put(body, "limitHint", limit)

  defp maybe_add_predicates(body, nil), do: body
  defp maybe_add_predicates(body, []), do: body
  defp maybe_add_predicates(body, predicates), do: Map.put(body, "predicateHints", predicates)

  defp download_and_parse_parquet_df(
         %{"url" => url} = _file,
         parsed_predicates,
         columns,
         finch_name
       ) do
    case :get |> Finch.build(url) |> Finch.request(finch_name) do
      {:ok, %Finch.Response{status: 200, body: parquet_data}} ->
        parse_parquet_to_df(parquet_data, parsed_predicates, columns)

      {:ok, %Finch.Response{status: status, body: body}} ->
        Logger.error(
          "failed to download parquet file: status=#{status} preview=#{String.slice(body, 0..100)}"
        )

        {:error, {:download_failed, status}}

      {:error, reason} ->
        Logger.error("request failed for parquet file: #{inspect(reason)}")
        {:error, {:request_failed, reason}}
    end
  end

  defp parse_parquet_to_df(parquet_binary, parsed_predicates, columns) do
    case Explorer.DataFrame.load_parquet(parquet_binary) do
      {:ok, df} ->
        df =
          df
          |> apply_predicates(parsed_predicates)
          |> select_columns(columns)

        {:ok, df}

      {:error, reason} ->
        Logger.error("failed to load parquet data: #{inspect(reason)}")
        {:error, {:parse_failed, reason}}
    end
  end

  defp parse_predicates(predicates) when is_list(predicates) do
    Enum.map(predicates, &parse_predicate/1)
  end

  defp parse_predicate(predicate) when is_binary(predicate) do
    case DeltaQuery.PredicateParser.parse_predicate(predicate) do
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

  defp apply_predicates(df, []), do: df

  defp apply_predicates(df, predicates) when is_list(predicates) do
    Enum.reduce(predicates, df, fn predicate, acc ->
      apply_predicate_to_df(acc, predicate)
    end)
  end

  defp apply_predicate_to_df(df, {op, column, value}), do: apply_filter(df, op, column, value)
  defp apply_predicate_to_df(df, nil), do: df

  defp select_columns(df, nil), do: df

  defp select_columns(df, columns) when is_list(columns) do
    available_columns = Explorer.DataFrame.names(df)
    valid_columns = Enum.filter(columns, &(&1 in available_columns))

    if Enum.empty?(valid_columns) do
      Logger.error(
        "no valid columns found in dataframe: requested=#{inspect(columns)} available=#{inspect(available_columns)}"
      )

      df
    else
      Explorer.DataFrame.select(df, valid_columns)
    end
  end

  defp apply_filter(df, operation, column, value) when is_binary(column) do
    if column in Explorer.DataFrame.names(df) do
      Explorer.DataFrame.filter_with(df, fn lf ->
        case operation do
          :eq -> Explorer.Series.equal(lf[column], value)
          :neq -> Explorer.Series.not_equal(lf[column], value)
          :gt -> Explorer.Series.greater(lf[column], value)
          :lt -> Explorer.Series.less(lf[column], value)
          :gte -> Explorer.Series.greater_equal(lf[column], value)
          :lte -> Explorer.Series.less_equal(lf[column], value)
        end
      end)
    else
      df
    end
  end
end

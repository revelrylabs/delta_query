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

  alias DeltaQuery.Column
  alias DeltaQuery.Config
  alias DeltaQuery.PredicateParser
  alias DeltaQuery.Results
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

  Returns `{:ok, results}` on success. `results.dataframe` is an `Explorer.DataFrame` enabling
  joins, grouping, and aggregations. See `DeltaQuery.Results` for the meaning of
  `files_processed` and `total_files`.

  Delta keeps partition column values in each file action's `partitionValues` rather than in
  the Parquet data, so they are added to every row here. Pass the `:metadata` action from the
  query response to have them cast to their Delta type; without it they are strings, both in
  the rows and when predicates are checked against them to skip files.

  Returns `{:error, message}` when a predicate cannot be applied, for example when an
  ordering operator (`>`, `<`, `>=`, `<=`) targets a column whose type does not support
  ordering. Processing stops at the first such error. Files that fail to download or
  parse, including files whose partition values cannot be cast, are logged and skipped
  rather than returned as errors.

  ## Options

  - `:predicates` - List of SQL-like filter strings (e.g., ["genre = 'Fiction'", "book_id = 123"])
  - `:columns` - List of column names to return (nil = all columns)
  - `:metadata` - The metadata action (`%{"metaData" => ...}`) returned as `:metadata` by
    `query_table/5`, used to type partition columns
  """
  @spec parse_parquet_files(t(), list(map()), keyword()) ::
          {:ok, Results.t()} | {:error, String.t()}
  def parse_parquet_files(%__MODULE__{} = client, files, opts \\ []) do
    predicates = Keyword.get(opts, :predicates, [])
    columns = Keyword.get(opts, :columns)
    column_types = column_types(Keyword.get(opts, :metadata))

    parsed_predicates = parse_predicates(predicates)

    with {:ok, relevant_files} <- filter_files_by_partitions(files, parsed_predicates, column_types) do
      process_files_to_results(client, relevant_files, parsed_predicates, columns, column_types)
    end
  end

  defp process_files_to_results(client, files, parsed_predicates, columns, column_types) do
    total_files = length(files)

    result =
      files
      |> Enum.with_index(1)
      |> Enum.reduce_while({:ok, [], 0}, fn {file, index}, {:ok, dfs_acc, processed} ->
        case download_and_parse_parquet_df(client, file, parsed_predicates, columns, column_types) do
          {:ok, df} ->
            if DataFrame.n_rows(df) > 0 do
              {:cont, {:ok, [df | dfs_acc], processed + 1}}
            else
              {:cont, {:ok, dfs_acc, processed + 1}}
            end

          {:error, {:invalid_predicate, message}} ->
            {:halt, {:error, message}}

          {:error, reason} ->
            Logger.error("failed to parse file #{index}/#{total_files}: #{inspect(reason)}")
            {:cont, {:ok, dfs_acc, processed}}
        end
      end)

    with {:ok, dataframes, processed} <- result do
      df =
        case Enum.reverse(dataframes) do
          [] -> empty_dataframe(columns)
          [single] -> single
          multiple -> concat_with_common_columns(multiple)
        end

      {:ok, %Results{dataframe: df, files_processed: processed, total_files: total_files}}
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

  defp download_and_parse_parquet_df(client, %{"url" => url} = file, parsed_predicates, columns, column_types) do
    case Req.get(client.download_req, url: url, decode_body: false) do
      {:ok, %Req.Response{status: 200, body: parquet_data}} ->
        partition_values = Map.get(file, "partitionValues", %{})
        parse_parquet_to_df(parquet_data, partition_values, column_types, parsed_predicates, columns)

      {:ok, %Req.Response{status: status, body: body}} ->
        body_preview = if is_binary(body), do: String.slice(body, 0..100), else: inspect(body)
        Logger.error("failed to download parquet file: status=#{status} preview=#{body_preview}")

        {:error, {:download_failed, status}}

      {:error, reason} ->
        Logger.error("request failed for parquet file: #{inspect(reason)}")
        {:error, {:request_failed, reason}}
    end
  end

  defp download_and_parse_parquet_df(_client, file, _parsed_predicates, _columns, _column_types) do
    {:error, {:missing_url, Map.keys(file)}}
  end

  defp parse_parquet_to_df(parquet_binary, partition_values, column_types, parsed_predicates, columns) do
    case DataFrame.load_parquet(parquet_binary) do
      {:ok, df} ->
        with {:ok, with_partitions} <- add_partition_columns(df, partition_values, column_types),
             {:ok, filtered} <- apply_predicates(with_partitions, parsed_predicates) do
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

  defp filter_files_by_partitions(files, parsed_predicates, column_types) do
    case Enum.reject(parsed_predicates, &is_nil/1) do
      [] ->
        {:ok, files}

      predicates ->
        result =
          Enum.reduce_while(files, {:ok, []}, fn file, {:ok, kept} ->
            case file_matches_predicates(file, predicates, column_types) do
              {:ok, true} -> {:cont, {:ok, [file | kept]}}
              {:ok, false} -> {:cont, {:ok, kept}}
              {:error, {:invalid_predicate, message}} -> {:halt, {:error, message}}
            end
          end)

        with {:ok, files} <- result, do: {:ok, Enum.reverse(files)}
    end
  end

  defp file_matches_predicates(file, predicates, column_types) do
    case Map.get(file, "partitionValues", %{}) do
      values when map_size(values) == 0 ->
        {:ok, true}

      values ->
        case partition_series(values, column_types) do
          {:ok, series} ->
            with {:ok, filtered} <- apply_predicates(DataFrame.new(Map.new(series)), predicates) do
              {:ok, DataFrame.n_rows(filtered) == 1}
            end

          {:error, _invalid_partition_value} ->
            {:ok, true}
        end
    end
  end

  defp column_types(metadata), do: Map.new(Column.from_metadata(metadata), &{&1.name, &1.type})

  defp add_partition_columns(df, partition_values, column_types) do
    existing = DataFrame.names(df)
    missing = Map.reject(partition_values, fn {column, _value} -> column in existing end)

    with {:ok, series} <- partition_series(missing, column_types) do
      {:ok, Enum.reduce(series, df, fn {column, values}, acc -> DataFrame.put(acc, column, values) end)}
    end
  end

  defp partition_series(partition_values, column_types) do
    Enum.reduce_while(partition_values, {:ok, []}, fn {column, value}, {:ok, acc} ->
      dtype = partition_dtype(Map.get(column_types, column))

      case parse_partition_value(value, dtype) do
        {:ok, parsed} -> {:cont, {:ok, [{column, Series.from_list([parsed], dtype: dtype)} | acc]}}
        :error -> {:halt, {:error, {:invalid_partition_value, {column, value}}}}
      end
    end)
  end

  defp partition_dtype("long"), do: {:s, 64}
  defp partition_dtype("integer"), do: {:s, 32}
  defp partition_dtype("short"), do: {:s, 16}
  defp partition_dtype("byte"), do: {:s, 8}
  defp partition_dtype("double"), do: {:f, 64}
  defp partition_dtype("float"), do: {:f, 32}
  defp partition_dtype("boolean"), do: :boolean
  defp partition_dtype("date"), do: :date
  defp partition_dtype("timestamp"), do: {:naive_datetime, :microsecond}
  defp partition_dtype("timestamp_ntz"), do: {:naive_datetime, :microsecond}

  defp partition_dtype("decimal(" <> rest) do
    case Regex.run(~r/^(\d+),\s*(\d+)\)$/, rest) do
      [_, precision, scale] -> {:decimal, String.to_integer(precision), String.to_integer(scale)}
      nil -> :string
    end
  end

  defp partition_dtype(_type), do: :string

  defp parse_partition_value(nil, _dtype), do: {:ok, nil}
  defp parse_partition_value(value, :string) when is_binary(value), do: {:ok, value}
  defp parse_partition_value("true", :boolean), do: {:ok, true}
  defp parse_partition_value("false", :boolean), do: {:ok, false}

  defp parse_partition_value(value, {:s, _}) when is_binary(value) do
    case Integer.parse(value) do
      {integer, ""} -> {:ok, integer}
      _ -> :error
    end
  end

  defp parse_partition_value(value, {:f, _}) when is_binary(value) do
    case Float.parse(value) do
      {float, ""} -> {:ok, float}
      _ -> :error
    end
  end

  defp parse_partition_value(value, :date) when is_binary(value) do
    case Date.from_iso8601(value) do
      {:ok, date} -> {:ok, date}
      {:error, _} -> :error
    end
  end

  defp parse_partition_value(value, {:naive_datetime, _}) when is_binary(value) do
    case NaiveDateTime.from_iso8601(value) do
      {:ok, datetime} -> {:ok, datetime}
      {:error, _} -> :error
    end
  end

  defp parse_partition_value(value, {:decimal, _, _}) when is_binary(value) do
    case Decimal.parse(value) do
      {decimal, ""} -> {:ok, decimal}
      _ -> :error
    end
  end

  defp parse_partition_value(_value, _dtype), do: :error

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
      PredicateParser.filter_dataframe(df, operation, column, value)
    else
      {:ok, df}
    end
  end
end

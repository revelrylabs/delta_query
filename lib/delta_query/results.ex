defmodule DeltaQuery.Results do
  @moduledoc """
  In-memory operations on query results.

  A `%Results{}` struct contains a dataframe returned from `Query.execute/1`.
  All functions in this module operate on data that has already been fetched.

  ## Filtering

  Stage 1: Prefer `Query.where/2` for initial filtering** - it's more efficient.
  Stage 2: Use `Results.filter/2` to apply additional filters on already-fetched data.

  This module handles stage 2.
  """

  require Explorer.DataFrame

  @enforce_keys [:dataframe, :files_processed, :total_files]
  defstruct [:dataframe, :files_processed, :total_files]

  @type t :: %__MODULE__{
          dataframe: Explorer.DataFrame.t(),
          files_processed: non_neg_integer(),
          total_files: non_neg_integer()
        }

  @type join_how :: :inner | :left | :right | :outer | :cross
  @type join_opt :: {:on, String.t() | list(String.t())} | {:how, join_how()}
  @type join_opts :: list(join_opt())

  @doc """
  Join two result sets on a common column.

  ## Options

  - `:on` - Column name or list of column names to join on (required)
  - `:how` - Join type: `:left` (default), `:right`, `:inner`, `:outer`, `:cross`

  ## Examples

      "projects"
      |> Query.new()
      |> Query.execute!()
      |> Results.join(contracts_result, on: "project_id")
      |> Results.to_rows()
  """
  @spec join(t(), t(), join_opts()) :: t()
  def join(%__MODULE__{} = left, %__MODULE__{} = right, opts) do
    on = Keyword.fetch!(opts, :on)
    how = Keyword.get(opts, :how, :left)
    on_columns = if is_binary(on), do: [on], else: on

    left_df = normalize_join_columns(left.dataframe, on_columns)
    right_df = normalize_join_columns(right.dataframe, on_columns)

    joined_df = Explorer.DataFrame.join(left_df, right_df, on: on_columns, how: how)

    %__MODULE__{
      dataframe: joined_df,
      files_processed: left.files_processed + right.files_processed,
      total_files: left.total_files + right.total_files
    }
  end

  @doc """
  Convert results to a list of maps (rows).

  ## Examples

      "projects"
      |> Query.new()
      |> Query.execute!()
      |> Results.to_rows()
  """
  @spec to_rows(t()) :: list(map())
  def to_rows(%__MODULE__{dataframe: df}), do: Explorer.DataFrame.to_rows(df)

  @doc """
  Return the number of rows in the results.

  ## Examples

      results |> Results.count()
      # => 42
  """
  @spec count(t()) :: non_neg_integer()
  def count(%__MODULE__{dataframe: df}), do: Explorer.DataFrame.n_rows(df)

  @doc """
  Check if results are empty.

  ## Examples

      results |> Results.empty?()
      # => false
  """
  @spec empty?(t()) :: boolean()
  def empty?(%__MODULE__{} = results), do: count(results) == 0

  @doc """
  Return the first row as a map, or nil if empty.

  ## Examples

      results |> Results.first()
      # => %{"id" => 1, "name" => "Project A"}
  """
  @spec first(t()) :: map() | nil
  def first(%__MODULE__{} = results) do
    results |> to_rows() |> List.first()
  end

  @doc """
  Sum a numeric column, returning 0 if the column doesn't exist.

  ## Examples

      results |> Results.sum("amount")
      # => 12500.0
  """
  @spec sum(t(), String.t()) :: number()
  def sum(%__MODULE__{dataframe: df}, column) do
    if column in Explorer.DataFrame.names(df) do
      Explorer.Series.sum(df[column])
    else
      0
    end
  end

  @doc """
  Apply additional predicate filters to already-fetched results.

  **Prefer `Query.where/2` for initial filtering.** This function applies filters in-memory
  on data that has already been downloaded. Use it only for:
  - Filtering joined results from multiple queries
  - Applying filters determined after initial query execution

  Returns `{:ok, results}` on success or `{:error, reason}` if any predicate is invalid.

  ## Examples

      # Filter joined results
      {:ok, projects} = Query.new("projects") |> Query.execute()
      {:ok, contracts} = Query.new("contracts") |> Query.execute()
      joined = Results.join(projects, contracts, on: "project_id")
      {:ok, filtered} = Results.filter(joined, ["square_feet > 50000"])
  """
  @spec filter(t(), list(String.t())) :: {:ok, t()} | {:error, String.t()}
  def filter(%__MODULE__{} = result, []), do: {:ok, result}

  def filter(%__MODULE__{} = result, predicates) when is_list(predicates) do
    case apply_filters(result.dataframe, predicates) do
      {:ok, filtered_df} ->
        {:ok, %{result | dataframe: filtered_df}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Apply text search to results using case-insensitive substring matching.

  Searches across the specified columns and returns rows where any column contains the search text.

  ## Examples

      {:ok, searched} = Results.text_search(results, "waterproofing", ["subject", "question", "answer"])
  """
  @spec text_search(t(), String.t(), list(String.t())) :: {:ok, t()} | {:error, String.t()}
  def text_search(%__MODULE__{} = result, search_text, columns) when is_binary(search_text) and is_list(columns) do
    if search_text == "" do
      {:ok, result}
    else
      case apply_text_search(result.dataframe, search_text, columns) do
        {:ok, filtered_df} ->
          {:ok, %{result | dataframe: filtered_df}}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  @doc """
  Aggregate results by grouping on a column and counting occurrences.

  Returns a list of maps with the column value and count, sorted by count descending.

  ## Examples

      aggregated = Results.aggregate_by_column(results, :project_id)
      # => [%{project_id: 1001, count: 5}, %{project_id: 1002, count: 3}]
  """
  @spec aggregate_by_column(t(), atom()) :: list(map())
  def aggregate_by_column(%__MODULE__{} = result, column) when is_atom(column) do
    column_str = Atom.to_string(column)

    result
    |> to_rows()
    |> Enum.group_by(& &1[column_str])
    |> Enum.map(fn {value, rows} ->
      %{column => value, count: length(rows)}
    end)
    |> Enum.sort_by(& &1.count, :desc)
  end

  defp apply_filters(df, predicates) do
    Enum.reduce_while(predicates, {:ok, df}, fn predicate, {:ok, acc} ->
      case DeltaQuery.PredicateParser.parse_predicate(predicate) do
        {:ok, {op, column, value}} ->
          if column in Explorer.DataFrame.names(acc) do
            {:cont, {:ok, apply_df_filter(acc, op, column, value)}}
          else
            {:halt, {:error, "unknown column in filter: #{column}"}}
          end

        {:error, _reason} ->
          {:halt, {:error, "invalid filter: #{predicate}"}}
      end
    end)
  end

  defp apply_df_filter(df, op, column, value) do
    dtypes = Explorer.DataFrame.dtypes(df)
    column_type = Map.get(dtypes, column)
    value = normalize_filter_value(column_type, value)

    Explorer.DataFrame.filter_with(df, fn lf ->
      case op do
        :eq -> Explorer.Series.equal(lf[column], value)
        :neq -> Explorer.Series.not_equal(lf[column], value)
        :gt -> Explorer.Series.greater(lf[column], value)
        :lt -> Explorer.Series.less(lf[column], value)
        :gte -> Explorer.Series.greater_equal(lf[column], value)
        :lte -> Explorer.Series.less_equal(lf[column], value)
      end
    end)
  end

  defp normalize_filter_value(:date, value) when is_binary(value) do
    case Date.from_iso8601(value) do
      {:ok, date} -> date
      {:error, _} -> value
    end
  end

  defp normalize_filter_value(_column_type, value), do: value

  defp apply_text_search(df, search_text, columns) do
    available_columns = Explorer.DataFrame.names(df)
    valid_columns = Enum.filter(columns, &(&1 in available_columns))

    if Enum.empty?(valid_columns) do
      {:error, "none of the specified columns exist in the dataframe"}
    else
      search_lower = String.downcase(search_text)

      filtered_df =
        Explorer.DataFrame.filter_with(df, fn lf ->
          valid_columns
          |> Enum.map(fn col ->
            lf[col]
            |> Explorer.Series.cast(:string)
            |> Explorer.Series.downcase()
            |> Explorer.Series.contains(search_lower)
          end)
          |> Enum.reduce(fn series, acc ->
            Explorer.Series.or(acc, series)
          end)
        end)

      {:ok, filtered_df}
    end
  end

  defp normalize_join_columns(df, columns) do
    Enum.reduce(columns, df, fn col, acc_df ->
      dtypes = Explorer.DataFrame.dtypes(acc_df)
      col_type = Map.get(dtypes, col)

      if col_type == :null do
        col_atom = String.to_existing_atom(col)

        Explorer.DataFrame.mutate_with(acc_df, fn lf ->
          [{col_atom, Explorer.Series.cast(lf[col], :integer)}]
        end)
      else
        acc_df
      end
    end)
  end
end

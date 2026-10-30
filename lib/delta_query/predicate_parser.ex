defmodule DeltaQuery.PredicateParser do
  @moduledoc """
  Parser for predicate expressions using NimbleParsec.

  Parses SQL-like filter predicates such as:
  - `"book_id = 123"`
  - `"status != 'Pending'"`
  - `"publication_date >= '2024-01-01'"`
  - `"list_price > 19.99"`

  Returns tuples in the format `{operator, column_name, value}` where:
  - operator: `:eq`, `:neq`, `:gt`, `:lt`, `:gte`, `:lte`
  - column_name: string
  - value: parsed value (integer, float, boolean, nil, or string)
  """

  import NimbleParsec

  alias Explorer.DataFrame
  alias Explorer.Series

  @ops %{
    eq: %{fun: &Series.equal/2, label: "=", ordered: false},
    neq: %{fun: &Series.not_equal/2, label: "!=", ordered: false},
    gt: %{fun: &Series.greater/2, label: ">", ordered: true},
    lt: %{fun: &Series.less/2, label: "<", ordered: true},
    gte: %{fun: &Series.greater_equal/2, label: ">=", ordered: true},
    lte: %{fun: &Series.less_equal/2, label: "<=", ordered: true}
  }

  whitespace = ascii_string([?\s, ?\t], min: 0)

  column_name =
    [?a..?z, ?A..?Z, ?0..?9, ?_, ?.]
    |> ascii_string(min: 1)
    |> unwrap_and_tag(:column)

  operator =
    [
      ">=" |> string() |> replace(:gte),
      "<=" |> string() |> replace(:lte),
      "!=" |> string() |> replace(:neq),
      "=" |> string() |> replace(:eq),
      ">" |> string() |> replace(:gt),
      "<" |> string() |> replace(:lt)
    ]
    |> choice()
    |> unwrap_and_tag(:op)

  quoted_string =
    choice([
      "'"
      |> string()
      |> ignore()
      |> repeat(
        choice([
          "\\'" |> string() |> replace(?'),
          "\\\"" |> string() |> replace(?"),
          utf8_char(not: ?')
        ])
      )
      |> ignore(string("'"))
      |> reduce({List, :to_string, []}),
      "\""
      |> string()
      |> ignore()
      |> repeat(
        choice([
          "\\\"" |> string() |> replace(?"),
          "\\'" |> string() |> replace(?'),
          utf8_char(not: ?")
        ])
      )
      |> ignore(string("\""))
      |> reduce({List, :to_string, []})
    ])

  # Numeric values
  number =
    [?-, ?+]
    |> ascii_char()
    |> optional()
    |> ascii_string([?0..?9], min: 1)
    |> optional(
      "."
      |> string()
      |> ascii_string([?0..?9], min: 1)
    )
    |> reduce(:parse_number)

  # Boolean and null literals
  literal =
    choice([
      "true" |> string() |> replace(true),
      "TRUE" |> string() |> replace(true),
      "false" |> string() |> replace(false),
      "FALSE" |> string() |> replace(false),
      "null" |> string() |> replace(nil),
      "NULL" |> string() |> replace(nil)
    ])

  # Value can be: quoted string, literal (bool/null), or number
  value =
    [
      quoted_string,
      literal,
      number
    ]
    |> choice()
    |> unwrap_and_tag(:value)

  # Complete predicate: column operator value
  # Format: [column: "name", op: :eq, value: 123]
  predicate =
    whitespace
    |> concat(column_name)
    |> concat(whitespace)
    |> concat(operator)
    |> concat(whitespace)
    |> concat(value)
    |> concat(whitespace)
    |> eos()
    |> reduce(:build_predicate)

  defparsec(:predicate, predicate)

  @spec parse_number(list()) :: integer() | float()
  defp parse_number(parts) do
    number_string =
      Enum.map_join(parts, fn
        ?- -> "-"
        ?+ -> "+"
        part when is_binary(part) -> part
      end)

    case Integer.parse(number_string) do
      {value, ""} ->
        value

      {_value, _remainder} ->
        {value, ""} = Float.parse(number_string)
        value
    end
  end

  @spec build_predicate(list()) :: {atom(), binary(), any()}
  defp build_predicate(parts) do
    column = Keyword.get(parts, :column)
    op = Keyword.get(parts, :op)
    value = Keyword.get(parts, :value)

    {op, column, value}
  end

  @doc """
  Parse a predicate string into a tuple.

  Returns `{:ok, {operator, column, value}}` on success,
  or `{:error, reason}` on failure.

  ## Examples

      iex> DeltaQuery.PredicateParser.parse_predicate("book_id = 123")
      {:ok, {:eq, "book_id", 123}}

      iex> DeltaQuery.PredicateParser.parse_predicate("status != 'Pending'")
      {:ok, {:neq, "status", "Pending"}}

      iex> DeltaQuery.PredicateParser.parse_predicate("list_price >= 19.99")
      {:ok, {:gte, "list_price", 19.99}}

      iex> DeltaQuery.PredicateParser.parse_predicate("available = true")
      {:ok, {:eq, "available", true}}
  """
  @spec parse_predicate(binary()) :: {:ok, {atom(), binary(), any()}} | {:error, String.t()}
  def parse_predicate(predicate_string) when is_binary(predicate_string) do
    case predicate(predicate_string) do
      {:ok, [result], "", _, _, _} ->
        {:ok, result}

      {:error, reason, _rest, _context, {line, col}, _byte_offset} ->
        {:error, "parse error at line #{line}, column #{col}: #{reason}"}
    end
  end

  @doc """
  Normalize a filter value based on the column type.

  date: Converts ISO8601 date strings to Date structs when the column type is `:date`, otherwise raises `ArgumentError` for invalid date strings.
  other types: Returns the value unchanged.

  ## Examples

      iex> DeltaQuery.PredicateParser.normalize_value(:date, "2025-01-15")
      ~D[2025-01-15]

      iex> DeltaQuery.PredicateParser.normalize_value(:string, "hello")
      "hello"
  """
  @spec normalize_value(atom(), any()) :: any()
  def normalize_value(:date, value) when is_binary(value) do
    case Date.from_iso8601(value) do
      {:ok, date} ->
        date

      {:error, reason} ->
        raise ArgumentError,
              "invalid date in filter predicate: #{inspect(value)} (#{reason}). Expected ISO8601 format like '2025-01-15'"
    end
  end

  def normalize_value(_column_type, value), do: value

  @doc """
  Whether an Explorer dtype supports the ordering operators `>`, `<`, `>=` and `<=`.

  ## Examples

      iex> DeltaQuery.PredicateParser.orderable_dtype?({:s, 64})
      true

      iex> DeltaQuery.PredicateParser.orderable_dtype?(:string)
      false
  """
  @spec orderable_dtype?(term()) :: boolean()
  # float
  def orderable_dtype?({:f, _}), do: true
  # signed int
  def orderable_dtype?({:s, _}), do: true
  # unsigned int
  def orderable_dtype?({:u, _}), do: true
  def orderable_dtype?({:naive_datetime, _}), do: true
  def orderable_dtype?({:duration, _}), do: true
  def orderable_dtype?(:date), do: true
  def orderable_dtype?(:time), do: true
  def orderable_dtype?(_), do: false

  @doc """
  Filter a dataframe with a parsed predicate.

  `column` must exist in `df`. Returns `{:error, message}` when an ordering operator
  (`>`, `<`, `>=`, `<=`) targets a column type that does not support ordering, or when
  the literal's type does not match the column's type.
  """
  @spec filter_dataframe(DataFrame.t(), atom(), String.t(), term()) ::
          {:ok, DataFrame.t()} | {:error, String.t()}
  def filter_dataframe(df, op, column, value) do
    %{fun: fun, label: label, ordered: ordered} = Map.fetch!(@ops, op)
    column_type = df |> DataFrame.dtypes() |> Map.get(column)

    normalized_value = normalize_value(column_type, value)

    cond do
      ordered and not orderable_dtype?(column_type) ->
        {:error, "operator #{label} not supported on #{inspect(column_type)} column '#{column}'"}

      not literal_matches_dtype?(column_type, normalized_value) ->
        {:error, "value #{inspect(value)} does not match #{inspect(column_type)} column '#{column}'"}

      true ->
        {:ok, DataFrame.filter_with(df, fn lf -> fun.(lf[column], normalized_value) end)}
    end
  end

  defp literal_matches_dtype?({kind, _}, value) when kind in [:s, :u, :f], do: is_number(value)
  defp literal_matches_dtype?({:decimal, _, _}, value), do: is_number(value)
  defp literal_matches_dtype?({:duration, _}, value), do: is_integer(value)
  defp literal_matches_dtype?(:boolean, value), do: is_boolean(value)
  defp literal_matches_dtype?(:string, value), do: is_binary(value)
  defp literal_matches_dtype?(:category, value), do: is_binary(value)
  defp literal_matches_dtype?(:binary, value), do: is_binary(value)
  defp literal_matches_dtype?(:date, value), do: is_struct(value, Date)
  defp literal_matches_dtype?(_dtype, _value), do: false
end

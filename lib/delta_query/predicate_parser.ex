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

  Converts ISO8601 date strings to Date structs when the column type is `:date`.
  Returns the value unchanged for other types.

  ## Examples

      iex> DeltaQuery.PredicateParser.normalize_value(:date, "2025-01-15")
      ~D[2025-01-15]

      iex> DeltaQuery.PredicateParser.normalize_value(:string, "hello")
      "hello"
  """
  @spec normalize_value(atom(), any()) :: any()
  def normalize_value(:date, value) when is_binary(value) do
    case Date.from_iso8601(value) do
      {:ok, date} -> date
      {:error, _} -> value
    end
  end

  def normalize_value(_column_type, value), do: value
end

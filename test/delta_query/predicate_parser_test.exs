defmodule DeltaQuery.PredicateParserTest do
  use ExUnit.Case, async: true

  alias DeltaQuery.PredicateParser

  doctest PredicateParser

  describe "parse_predicate/1" do
    test "parses equality with integer" do
      assert {:ok, {:eq, "book_id", 123}} = PredicateParser.parse_predicate("book_id = 123")
    end

    test "parses equality with negative integer" do
      assert {:ok, {:eq, "offset", -10}} = PredicateParser.parse_predicate("offset = -10")
    end

    test "parses equality with float" do
      assert {:ok, {:eq, "price", 99.99}} = PredicateParser.parse_predicate("price = 99.99")
    end

    test "parses equality with negative float" do
      assert {:ok, {:eq, "delta", -0.5}} = PredicateParser.parse_predicate("delta = -0.5")
    end

    test "parses equality with single-quoted string" do
      assert {:ok, {:eq, "status", "active"}} =
               PredicateParser.parse_predicate("status = 'active'")
    end

    test "parses equality with double-quoted string" do
      assert {:ok, {:eq, "name", "test"}} = PredicateParser.parse_predicate("name = \"test\"")
    end

    test "parses equality with true boolean" do
      assert {:ok, {:eq, "active", true}} = PredicateParser.parse_predicate("active = true")
      assert {:ok, {:eq, "active", true}} = PredicateParser.parse_predicate("active = TRUE")
    end

    test "parses equality with false boolean" do
      assert {:ok, {:eq, "deleted", false}} = PredicateParser.parse_predicate("deleted = false")
      assert {:ok, {:eq, "deleted", false}} = PredicateParser.parse_predicate("deleted = FALSE")
    end

    test "parses equality with null" do
      assert {:ok, {:eq, "deleted_at", nil}} =
               PredicateParser.parse_predicate("deleted_at = null")

      assert {:ok, {:eq, "deleted_at", nil}} =
               PredicateParser.parse_predicate("deleted_at = NULL")
    end

    test "parses not-equal operator" do
      assert {:ok, {:neq, "status", "closed"}} =
               PredicateParser.parse_predicate("status != 'closed'")
    end

    test "parses greater-than operator" do
      assert {:ok, {:gt, "count", 10}} = PredicateParser.parse_predicate("count > 10")
    end

    test "parses less-than operator" do
      assert {:ok, {:lt, "price", 100}} = PredicateParser.parse_predicate("price < 100")
    end

    test "parses greater-than-or-equal operator" do
      assert {:ok, {:gte, "score", 50}} = PredicateParser.parse_predicate("score >= 50")
    end

    test "parses less-than-or-equal operator" do
      assert {:ok, {:lte, "priority", 5}} = PredicateParser.parse_predicate("priority <= 5")
    end

    test "parses column with dots (qualified names)" do
      assert {:ok, {:eq, "table.column", 1}} = PredicateParser.parse_predicate("table.column = 1")
    end

    test "parses column with underscores" do
      assert {:ok, {:eq, "created_at", "2024-01-01"}} =
               PredicateParser.parse_predicate("created_at = '2024-01-01'")
    end

    test "handles whitespace variations" do
      assert {:ok, {:eq, "x", 1}} = PredicateParser.parse_predicate("  x  =  1  ")
      assert {:ok, {:eq, "x", 1}} = PredicateParser.parse_predicate("x=1")
      assert {:ok, {:eq, "x", 1}} = PredicateParser.parse_predicate("\tx\t=\t1\t")
    end

    test "parses escaped quotes in strings" do
      assert {:ok, {:eq, "name", "it's"}} = PredicateParser.parse_predicate("name = 'it\\'s'")
    end

    test "returns error for invalid predicates" do
      assert {:error, _} = PredicateParser.parse_predicate("invalid")
      assert {:error, _} = PredicateParser.parse_predicate("= 123")
      assert {:error, _} = PredicateParser.parse_predicate("column =")
    end
  end

  describe "normalize_value/2" do
    test "converts valid ISO8601 date string to Date" do
      assert PredicateParser.normalize_value(:date, "2025-01-15") == ~D[2025-01-15]
    end

    test "raises ArgumentError for invalid date string" do
      assert_raise ArgumentError, ~r/invalid date in filter predicate/, fn ->
        PredicateParser.normalize_value(:date, "2025-13-45")
      end
    end

    test "raises ArgumentError for malformed date string" do
      assert_raise ArgumentError, ~r/invalid date in filter predicate.*Expected ISO8601/, fn ->
        PredicateParser.normalize_value(:date, "not-a-date")
      end
    end

    test "passes through non-date values unchanged" do
      assert PredicateParser.normalize_value(:string, "hello") == "hello"
      assert PredicateParser.normalize_value(:integer, 42) == 42
      assert PredicateParser.normalize_value(:float, 3.14) == 3.14
    end
  end
end

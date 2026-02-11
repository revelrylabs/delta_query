defmodule DeltaQuery.ResultsTest do
  use ExUnit.Case, async: true

  alias DeltaQuery.Results

  defp make_results(data) do
    df = Explorer.DataFrame.new(data)

    %Results{
      dataframe: df,
      files_processed: 1,
      total_files: 1
    }
  end

  describe "to_rows/1" do
    test "converts dataframe to list of maps" do
      results = make_results(%{"id" => [1, 2], "name" => ["a", "b"]})

      assert Results.to_rows(results) == [
               %{"id" => 1, "name" => "a"},
               %{"id" => 2, "name" => "b"}
             ]
    end

    test "handles empty dataframe" do
      results = make_results(%{"id" => [], "name" => []})
      assert Results.to_rows(results) == []
    end
  end

  describe "join/3" do
    test "joins two results on a column" do
      left = make_results(%{"id" => [1, 2], "name" => ["a", "b"]})
      right = make_results(%{"id" => [1, 2], "value" => [10, 20]})

      joined = Results.join(left, right, on: "id")
      rows = Results.to_rows(joined)

      assert length(rows) == 2
      assert Enum.find(rows, &(&1["id"] == 1))["value"] == 10
      assert Enum.find(rows, &(&1["id"] == 2))["value"] == 20
    end

    test "supports different join types" do
      left = make_results(%{"id" => [1, 2, 3], "name" => ["a", "b", "c"]})
      right = make_results(%{"id" => [2, 3, 4], "value" => [20, 30, 40]})

      inner = Results.join(left, right, on: "id", how: :inner)
      assert length(Results.to_rows(inner)) == 2

      left_join = Results.join(left, right, on: "id", how: :left)
      assert length(Results.to_rows(left_join)) == 3
    end

    test "aggregates file counts" do
      left = %Results{
        dataframe: Explorer.DataFrame.new(%{"id" => [1]}),
        files_processed: 3,
        total_files: 5
      }

      right = %Results{
        dataframe: Explorer.DataFrame.new(%{"id" => [1], "val" => [10]}),
        files_processed: 2,
        total_files: 3
      }

      joined = Results.join(left, right, on: "id")
      assert joined.files_processed == 5
      assert joined.total_files == 8
    end
  end

  describe "filter/2" do
    test "filters with equality predicate" do
      results = make_results(%{"status" => ["active", "closed", "active"], "id" => [1, 2, 3]})

      {:ok, filtered} = Results.filter(results, ["status = 'active'"])
      rows = Results.to_rows(filtered)

      assert length(rows) == 2
      assert Enum.all?(rows, &(&1["status"] == "active"))
    end

    test "filters with numeric comparison" do
      results = make_results(%{"score" => [10, 50, 100], "id" => [1, 2, 3]})

      {:ok, filtered} = Results.filter(results, ["score > 25"])
      rows = Results.to_rows(filtered)

      assert length(rows) == 2
      assert Enum.all?(rows, &(&1["score"] > 25))
    end

    test "applies multiple filters" do
      results = make_results(%{"score" => [10, 50, 100], "status" => ["a", "b", "a"]})

      {:ok, filtered} = Results.filter(results, ["score >= 50", "status = 'a'"])
      rows = Results.to_rows(filtered)

      assert length(rows) == 1
      assert hd(rows)["score"] == 100
    end

    test "returns unchanged results for empty predicates" do
      results = make_results(%{"id" => [1, 2, 3]})
      {:ok, filtered} = Results.filter(results, [])
      assert Results.to_rows(filtered) == Results.to_rows(results)
    end

    test "returns error for unknown column" do
      results = make_results(%{"id" => [1, 2, 3]})
      {:error, msg} = Results.filter(results, ["unknown = 1"])
      assert msg =~ "unknown column"
    end

    test "returns error for invalid predicate" do
      results = make_results(%{"id" => [1, 2, 3]})
      {:error, msg} = Results.filter(results, ["not a predicate"])
      assert msg =~ "invalid filter"
    end

    test "filters date column with ISO8601 string predicate" do
      results =
        make_results(%{
          "created_at" => [~D[2025-01-01], ~D[2025-02-15], ~D[2025-03-01]],
          "id" => [1, 2, 3]
        })

      {:ok, filtered} = Results.filter(results, ["created_at >= '2025-02-01'"])
      rows = Results.to_rows(filtered)

      assert length(rows) == 2

      assert Enum.all?(rows, fn row ->
               Date.compare(row["created_at"], ~D[2025-02-01]) in [:gt, :eq]
             end)
    end

    test "filters date column with less than comparison" do
      results =
        make_results(%{
          "event_date" => [~D[2025-01-10], ~D[2025-01-20], ~D[2025-01-30]],
          "id" => [1, 2, 3]
        })

      {:ok, filtered} = Results.filter(results, ["event_date < '2025-01-25'"])
      rows = Results.to_rows(filtered)

      assert length(rows) == 2
      assert Enum.all?(rows, fn row -> Date.before?(row["event_date"], ~D[2025-01-25]) end)
    end

    test "filters date column with equality" do
      results =
        make_results(%{
          "log_date" => [~D[2025-01-15], ~D[2025-01-15], ~D[2025-01-16]],
          "id" => [1, 2, 3]
        })

      {:ok, filtered} = Results.filter(results, ["log_date = '2025-01-15'"])
      rows = Results.to_rows(filtered)

      assert length(rows) == 2
      assert Enum.all?(rows, fn row -> Date.compare(row["log_date"], ~D[2025-01-15]) == :eq end)
    end
  end

  describe "text_search/3" do
    test "finds matching rows case-insensitively" do
      results =
        make_results(%{
          "subject" => ["Hello World", "Goodbye", "hello there"],
          "id" => [1, 2, 3]
        })

      {:ok, searched} = Results.text_search(results, "hello", ["subject"])
      rows = Results.to_rows(searched)

      assert length(rows) == 2
      assert Enum.all?(rows, &String.contains?(String.downcase(&1["subject"]), "hello"))
    end

    test "searches across multiple columns" do
      results =
        make_results(%{
          "title" => ["Alpha", "Beta", "Gamma"],
          "body" => ["test", "alpha content", "other"],
          "id" => [1, 2, 3]
        })

      {:ok, searched} = Results.text_search(results, "alpha", ["title", "body"])
      rows = Results.to_rows(searched)

      assert length(rows) == 2
    end

    test "returns unchanged results for empty search" do
      results = make_results(%{"text" => ["a", "b"], "id" => [1, 2]})
      {:ok, searched} = Results.text_search(results, "", ["text"])
      assert Results.to_rows(searched) == Results.to_rows(results)
    end

    test "returns error when no columns exist" do
      results = make_results(%{"id" => [1, 2]})
      {:error, msg} = Results.text_search(results, "test", ["missing"])
      assert msg =~ "none of the specified columns"
    end
  end

  describe "count/1" do
    test "returns number of rows" do
      results = make_results(%{"id" => [1, 2, 3]})
      assert Results.count(results) == 3
    end

    test "returns 0 for empty results" do
      results = make_results(%{"id" => []})
      assert Results.count(results) == 0
    end
  end

  describe "empty?/1" do
    test "returns true for empty results" do
      results = make_results(%{"id" => []})
      assert Results.empty?(results)
    end

    test "returns false for non-empty results" do
      results = make_results(%{"id" => [1]})
      refute Results.empty?(results)
    end
  end

  describe "first/1" do
    test "returns first row as map" do
      results = make_results(%{"id" => [1, 2], "name" => ["a", "b"]})
      assert Results.first(results) == %{"id" => 1, "name" => "a"}
    end

    test "returns nil for empty results" do
      results = make_results(%{"id" => []})
      assert Results.first(results) == nil
    end
  end

  describe "sum/2" do
    test "sums a numeric column" do
      results = make_results(%{"amount" => [10, 20, 30]})
      assert Results.sum(results, "amount") == 60
    end

    test "returns 0 for non-existent column" do
      results = make_results(%{"id" => [1, 2]})
      assert Results.sum(results, "missing") == 0
    end
  end

  describe "aggregate_by_column/2" do
    test "groups and counts by column" do
      results =
        make_results(%{
          "category" => ["a", "b", "a", "a", "b"],
          "id" => [1, 2, 3, 4, 5]
        })

      agg = Results.aggregate_by_column(results, :category)

      assert length(agg) == 2
      assert Enum.find(agg, &(&1.category == "a")).count == 3
      assert Enum.find(agg, &(&1.category == "b")).count == 2
    end

    test "sorts by count descending" do
      results =
        make_results(%{
          "type" => ["x", "y", "y", "y", "x"],
          "id" => [1, 2, 3, 4, 5]
        })

      agg = Results.aggregate_by_column(results, :type)

      assert hd(agg).type == "y"
      assert hd(agg).count == 3
    end
  end
end

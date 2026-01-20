defmodule DeltaQuery.QueryTest do
  use ExUnit.Case, async: true

  alias DeltaQuery.Query

  describe "new/1" do
    test "creates a query for a table" do
      query = Query.new("books")
      assert query.table == "books"
      assert query.columns == nil
      assert query.filters == []
      assert query.limit == nil
    end
  end

  describe "where/2" do
    test "adds a filter to the query" do
      query =
        "books"
        |> Query.new()
        |> Query.where("library_id = 100")

      assert query.filters == ["library_id = 100"]
    end

    test "appends multiple filters" do
      query =
        "books"
        |> Query.new()
        |> Query.where("library_id = 100")
        |> Query.where("genre = 'Fiction'")

      assert query.filters == ["library_id = 100", "genre = 'Fiction'"]
    end
  end

  describe "select/2" do
    test "sets columns to return" do
      query =
        "books"
        |> Query.new()
        |> Query.select(["id", "title"])

      assert query.columns == ["id", "title"]
    end

    test "overwrites previous selection" do
      query =
        "books"
        |> Query.new()
        |> Query.select(["id"])
        |> Query.select(["title", "author"])

      assert query.columns == ["title", "author"]
    end
  end

  describe "limit/2" do
    test "sets limit hint" do
      query =
        "books"
        |> Query.new()
        |> Query.limit(100)

      assert query.limit == 100
    end

    test "overwrites previous limit" do
      query =
        "books"
        |> Query.new()
        |> Query.limit(100)
        |> Query.limit(50)

      assert query.limit == 50
    end
  end

  describe "composability" do
    test "chains all operations" do
      query =
        "books"
        |> Query.new()
        |> Query.where("library_id = 100")
        |> Query.where("genre = 'Fiction'")
        |> Query.select(["book_id", "title", "genre"])
        |> Query.limit(50)

      assert query.table == "books"
      assert query.filters == ["library_id = 100", "genre = 'Fiction'"]
      assert query.columns == ["book_id", "title", "genre"]
      assert query.limit == 50
    end
  end
end

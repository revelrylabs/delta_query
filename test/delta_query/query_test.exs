defmodule DeltaQuery.QueryTest do
  use ExUnit.Case, async: true

  alias DeltaQuery.Query

  describe "new/1" do
    test "creates a query for a table" do
      query = Query.new("projects")
      assert query.table == "projects"
      assert query.columns == nil
      assert query.filters == []
      assert query.limit == nil
    end
  end

  describe "where/2" do
    test "adds a filter to the query" do
      query =
        "projects"
        |> Query.new()
        |> Query.where("company_id = 100")

      assert query.filters == ["company_id = 100"]
    end

    test "appends multiple filters" do
      query =
        "projects"
        |> Query.new()
        |> Query.where("company_id = 100")
        |> Query.where("status = 'active'")

      assert query.filters == ["company_id = 100", "status = 'active'"]
    end
  end

  describe "select/2" do
    test "sets columns to return" do
      query =
        "projects"
        |> Query.new()
        |> Query.select(["id", "name"])

      assert query.columns == ["id", "name"]
    end

    test "overwrites previous selection" do
      query =
        "projects"
        |> Query.new()
        |> Query.select(["id"])
        |> Query.select(["name", "status"])

      assert query.columns == ["name", "status"]
    end
  end

  describe "limit/2" do
    test "sets limit hint" do
      query =
        "projects"
        |> Query.new()
        |> Query.limit(100)

      assert query.limit == 100
    end

    test "overwrites previous limit" do
      query =
        "projects"
        |> Query.new()
        |> Query.limit(100)
        |> Query.limit(50)

      assert query.limit == 50
    end
  end

  describe "composability" do
    test "chains all operations" do
      query =
        "projects"
        |> Query.new()
        |> Query.where("company_id = 100")
        |> Query.where("status = 'active'")
        |> Query.select(["id", "name", "status"])
        |> Query.limit(50)

      assert query.table == "projects"
      assert query.filters == ["company_id = 100", "status = 'active'"]
      assert query.columns == ["id", "name", "status"]
      assert query.limit == 50
    end
  end
end

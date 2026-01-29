defmodule DeltaQueryTest do
  use ExUnit.Case

  alias DeltaQuery.Query

  describe "query/1" do
    test "creates a new query" do
      query = DeltaQuery.query("books")
      assert %Query{table: "books"} = query
    end
  end

  describe "get_table_schema/1" do
    test "requires table option" do
      assert_raise KeyError, fn ->
        DeltaQuery.get_table_schema()
      end
    end

    test "returns error when config is missing" do
      assert {:error, _} = DeltaQuery.get_table_schema(table: "books")
    end

    test "extracts columns from metadata response" do
      metadata = %{
        metadata: %{
          "metaData" => %{
            "schemaString" => ~s({"type":"struct","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"}]})
          }
        }
      }

      columns = DeltaQuery.extract_columns(metadata)

      assert columns == [
               %{name: "id", type: "long"},
               %{name: "name", type: "string"}
             ]
    end

    test "extracts columns with complex types" do
      metadata = %{
        metadata: %{
          "metaData" => %{
            "schemaString" =>
              ~s({"type":"struct","fields":[{"name":"id","type":"long"},{"name":"data","type":{"type":"struct"}},{"name":"tags","type":{"type":"array"}}]})
          }
        }
      }

      columns = DeltaQuery.extract_columns(metadata)

      assert columns == [
               %{name: "id", type: "long"},
               %{name: "data", type: "struct"},
               %{name: "tags", type: "array"}
             ]
    end

    test "returns empty list for invalid metadata" do
      assert DeltaQuery.extract_columns(%{}) == []
      assert DeltaQuery.extract_columns(%{metadata: %{}}) == []
    end
  end

  describe "configure/1" do
    test "creates config from options" do
      {:ok, config} =
        DeltaQuery.configure(
          endpoint: "https://example.com",
          bearer_token: "token",
          share: "share"
        )

      assert config.endpoint == "https://example.com"
    end
  end

  describe "configure!/1" do
    test "creates config or raises" do
      config =
        DeltaQuery.configure!(
          endpoint: "https://example.com",
          bearer_token: "token",
          share: "share"
        )

      assert config.endpoint == "https://example.com"
    end
  end
end

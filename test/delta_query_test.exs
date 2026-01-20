defmodule DeltaQueryTest do
  use ExUnit.Case

  alias DeltaQuery.Query

  describe "query/1" do
    test "creates a new query" do
      query = DeltaQuery.query("books")
      assert %Query{table: "books"} = query
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

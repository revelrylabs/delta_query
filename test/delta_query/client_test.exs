defmodule DeltaQuery.ClientTest do
  use ExUnit.Case, async: true

  alias DeltaQuery.Client
  alias DeltaQuery.Config

  describe "new/3" do
    test "creates client with required fields" do
      client = Client.new("https://example.com", "token123")

      assert client.endpoint == "https://example.com"
      assert client.bearer_token == "token123"
      assert client.finch_name == :delta_query_finch
    end

    test "allows custom finch_name" do
      client = Client.new("https://example.com", "token123", finch_name: MyApp.Finch)

      assert client.finch_name == MyApp.Finch
    end
  end

  describe "from_config/1" do
    test "creates client from config struct" do
      config =
        Config.new!(
          endpoint: "https://delta.example.com",
          bearer_token: "secret",
          share: "my_share",
          finch_name: CustomFinch
        )

      client = Client.from_config(config)

      assert client.endpoint == "https://delta.example.com"
      assert client.bearer_token == "secret"
      assert client.finch_name == CustomFinch
    end
  end

  describe "parse_parquet_files/2" do
    test "returns empty dataframe when no files provided" do
      {:ok, df} = Client.parse_parquet_files([])

      assert Explorer.DataFrame.n_rows(df) == 0
    end

    test "returns empty dataframe with specified columns when no files" do
      {:ok, df} = Client.parse_parquet_files([], columns: ["id", "name"])

      assert Explorer.DataFrame.n_rows(df) == 0
      assert Explorer.DataFrame.names(df) == ["id", "name"]
    end
  end
end

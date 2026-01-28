defmodule DeltaQuery.ClientTest do
  use ExUnit.Case, async: true

  alias DeltaQuery.Client
  alias DeltaQuery.Config

  describe "new/3" do
    test "creates client with Req structs" do
      client = Client.new("https://example.com", "token123")

      assert %Req.Request{} = client.req
      assert %Req.Request{} = client.download_req
    end

    test "configures base_url and auth on api req" do
      client = Client.new("https://example.com", "token123")

      assert client.req.options.base_url == "https://example.com"
      assert client.req.headers["authorization"] == ["Bearer token123"]
    end

    test "download req has no auth header" do
      client = Client.new("https://example.com", "token123")

      refute Map.has_key?(client.download_req.headers, "authorization")
    end
  end

  describe "from_config/1" do
    test "creates client from config struct" do
      config =
        Config.new!(
          endpoint: "https://delta.example.com",
          bearer_token: "secret",
          share: "my_share"
        )

      client = Client.from_config(config)

      assert client.req.options.base_url == "https://delta.example.com"
      assert client.req.headers["authorization"] == ["Bearer secret"]
    end
  end

  describe "parse_parquet_files/3" do
    test "returns empty dataframe when no files provided" do
      client = Client.new("https://example.com", "token")
      {:ok, df} = Client.parse_parquet_files(client, [])

      assert Explorer.DataFrame.n_rows(df) == 0
    end

    test "returns empty dataframe with specified columns when no files" do
      client = Client.new("https://example.com", "token")
      {:ok, df} = Client.parse_parquet_files(client, [], columns: ["id", "name"])

      assert Explorer.DataFrame.n_rows(df) == 0
      assert Explorer.DataFrame.names(df) == ["id", "name"]
    end
  end
end

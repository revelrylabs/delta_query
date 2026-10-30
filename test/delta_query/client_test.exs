defmodule DeltaQuery.ClientTest do
  use ExUnit.Case, async: true

  alias DeltaQuery.Client
  alias DeltaQuery.Config
  alias DeltaQuery.Results

  defmodule ParquetAdapter do
    @moduledoc false

    def run(request) do
      parquet =
        %{"revision" => ["0", "1", "2"], "id" => [1, 2, 3]}
        |> Explorer.DataFrame.new()
        |> Explorer.DataFrame.dump_parquet!()

      {request, Req.Response.new(status: 200, body: parquet)}
    end
  end

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

      {:ok, %Results{dataframe: df, files_processed: 0, total_files: 0}} =
        Client.parse_parquet_files(client, %{files: [], metadata: %{}})

      assert Explorer.DataFrame.n_rows(df) == 0
    end

    test "returns empty dataframe with specified columns when no files" do
      client = Client.new("https://example.com", "token")

      {:ok, %Results{dataframe: df}} =
        Client.parse_parquet_files(client, %{files: [], metadata: %{}}, columns: ["id", "name"])

      assert Explorer.DataFrame.n_rows(df) == 0
      assert Explorer.DataFrame.names(df) == ["id", "name"]
    end
  end

  describe "parse_parquet_files/3 row filtering" do
    setup do
      client = Client.new("https://example.com", "token", adapter: ParquetAdapter)

      %{client: client, response: %{files: [%{"url" => "https://files.example.com/part-0.parquet"}], metadata: %{}}}
    end

    test "filters rows with an ordering operator on a numeric column", %{client: client, response: response} do
      {:ok, %Results{dataframe: df}} = Client.parse_parquet_files(client, response, predicates: ["id >= 2"])

      assert Explorer.DataFrame.to_rows(df) == [
               %{"id" => 2, "revision" => "1"},
               %{"id" => 3, "revision" => "2"}
             ]
    end

    test "returns error when an ordering operator targets a string column", %{client: client, response: response} do
      {:error, msg} = Client.parse_parquet_files(client, response, predicates: ["revision >= 1"])

      assert msg =~ "operator >="
      assert msg =~ "revision"
      assert msg =~ "string"
    end
  end

  describe "parse_parquet_files/3 partition columns" do
    test "types partition columns from the response metadata" do
      client = Client.new("https://example.com", "token", adapter: ParquetAdapter)

      response = %{
        files: [%{"url" => "https://files.example.com/part-0.parquet", "partitionValues" => %{"shelf_id" => "7"}}],
        metadata: %{"metaData" => %{"schemaString" => ~s({"fields":[{"name":"shelf_id","type":"long"}]})}}
      }

      {:ok, %Results{dataframe: df}} = Client.parse_parquet_files(client, response)

      assert Explorer.DataFrame.dtypes(df)["shelf_id"] == {:s, 64}
      assert Explorer.Series.to_list(df["shelf_id"]) == [7, 7, 7]
    end
  end
end

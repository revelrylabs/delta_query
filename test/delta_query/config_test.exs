defmodule DeltaQuery.ConfigTest do
  use ExUnit.Case, async: true

  alias DeltaQuery.Config

  describe "new/1" do
    test "creates config from keyword options" do
      {:ok, config} =
        Config.new(
          endpoint: "https://example.com",
          bearer_token: "token123",
          share: "my_share"
        )

      assert config.endpoint == "https://example.com"
      assert config.bearer_token == "token123"
      assert config.share == "my_share"
      assert config.schema == "public"
      assert config.finch_name == :delta_query_finch
    end

    test "allows custom schema" do
      {:ok, config} =
        Config.new(
          endpoint: "https://example.com",
          bearer_token: "token123",
          share: "my_share",
          schema: "custom"
        )

      assert config.schema == "custom"
    end

    test "allows custom finch_name" do
      {:ok, config} =
        Config.new(
          endpoint: "https://example.com",
          bearer_token: "token123",
          share: "my_share",
          finch_name: MyApp.Finch
        )

      assert config.finch_name == MyApp.Finch
    end

    test "returns error when endpoint is missing" do
      {:error, msg} = Config.new(bearer_token: "token", share: "share")
      assert msg =~ "endpoint"
    end

    test "returns error when bearer_token is missing" do
      {:error, msg} = Config.new(endpoint: "https://example.com", share: "share")
      assert msg =~ "bearer_token"
    end

    test "returns error when share is missing" do
      {:error, msg} = Config.new(endpoint: "https://example.com", bearer_token: "token")
      assert msg =~ "share"
    end

    test "returns error for empty string values" do
      {:error, _} = Config.new(endpoint: "", bearer_token: "token", share: "share")
      {:error, _} = Config.new(endpoint: "https://example.com", bearer_token: "", share: "share")
      {:error, _} = Config.new(endpoint: "https://example.com", bearer_token: "token", share: "")
    end
  end

  describe "new!/1" do
    test "returns config on success" do
      config =
        Config.new!(
          endpoint: "https://example.com",
          bearer_token: "token123",
          share: "my_share"
        )

      assert config.endpoint == "https://example.com"
    end

    test "raises on missing required fields" do
      assert_raise ArgumentError, fn ->
        Config.new!(bearer_token: "token", share: "share")
      end
    end
  end
end

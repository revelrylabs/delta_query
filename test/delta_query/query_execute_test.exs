defmodule DeltaQuery.QueryExecuteTest do
  use ExUnit.Case

  alias DeltaQuery.Query

  describe "execute/2 config resolution" do
    test "returns error when no config provided and app env not set" do
      Application.delete_env(:delta_query, :config)

      query = Query.new("projects")
      {:error, reason} = Query.execute(query)

      assert reason =~ "endpoint"
    end

    test "returns config error when :config option is invalid" do
      query = Query.new("projects")

      result =
        Query.execute(query,
          config: [
            endpoint: "",
            bearer_token: "token",
            share: "my_share"
          ]
        )

      assert {:error, "endpoint is required"} = result
    end
  end

  describe "execute!/2" do
    test "raises on config error" do
      Application.delete_env(:delta_query, :config)

      query = Query.new("projects")

      assert_raise RuntimeError, ~r/Query execution failed/, fn ->
        Query.execute!(query)
      end
    end
  end
end

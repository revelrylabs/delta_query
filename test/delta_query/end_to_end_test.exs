defmodule DeltaQuery.EndToEndTest do
  use ExUnit.Case, async: true

  alias DeltaQuery.Column
  alias DeltaQuery.StubServer
  alias Explorer.DataFrame

  @moduletag :capture_log

  @query_path "/shares/stub_share/schemas/public/tables/books/query"

  defp books_df, do: DataFrame.from_parquet!(Path.join([File.cwd!(), "test", "support", "fixtures", "books.parquet"]))

  defp execute(query, config), do: DeltaQuery.execute(query, config: config)

  describe "execute/2" do
    test "downloads every file the server lists and returns its rows" do
      config = StubServer.config()

      {:ok, results} = "books" |> DeltaQuery.query() |> execute(config)

      assert DeltaQuery.count(results) == 3
      assert %{files_processed: 1, total_files: 1} = results
      assert_received {:stub_request, :post, @query_path, %{}, _headers}
      assert_received {:stub_request, :get, "/books.parquet", nil, _headers}
    end

    test "sends predicates and limit to the server and applies predicates and selection to rows" do
      config = StubServer.config()

      {:ok, results} =
        "books"
        |> DeltaQuery.query()
        |> DeltaQuery.where("library_id = 100")
        |> DeltaQuery.where("page_count > 200")
        |> DeltaQuery.select(["book_id", "title"])
        |> DeltaQuery.limit(10)
        |> execute(config)

      assert_received {:stub_request, :post, @query_path, body, _headers}
      assert body == %{"predicateHints" => ["library_id = 100", "page_count > 200"], "limitHint" => 10}

      assert DeltaQuery.to_rows(results) == [
               %{"book_id" => 1002, "title" => "To Kill a Mockingbird"},
               %{"book_id" => 1003, "title" => "1984"}
             ]
    end

    test "returns empty results with the selected columns when the server lists no files" do
      config = StubServer.config(%{files: []})

      {:ok, results} = "books" |> DeltaQuery.query() |> DeltaQuery.select(["book_id", "title"]) |> execute(config)

      assert DeltaQuery.empty?(results)
      assert DataFrame.names(results.dataframe) == ["book_id", "title"]
      assert %{files_processed: 0, total_files: 0} = results
    end

    test "reads protocol v2 file actions" do
      config = StubServer.config(%{format: :v2})

      {:ok, results} = "books" |> DeltaQuery.query() |> execute(config)

      assert DeltaQuery.count(results) == 3
    end

    test "sends the bearer token to the sharing server but not to the file store" do
      config = StubServer.config()

      {:ok, _results} = "books" |> DeltaQuery.query() |> execute(config)

      assert_received {:stub_request, :post, @query_path, _body, api_headers}
      assert api_headers["authorization"] == ["Bearer stub-token"]

      assert_received {:stub_request, :get, "/books.parquet", nil, download_headers}
      refute Map.has_key?(download_headers, "authorization")
    end

    test "concatenates rows from every file, keeping only columns common to all of them" do
      wider = DataFrame.put(books_df(), "extra", Explorer.Series.from_list([1, 2, 3]))

      config =
        StubServer.config(%{
          files: [StubServer.fixture_file("books.parquet", "part-1.parquet"), StubServer.file("part-2.parquet", wider)]
        })

      {:ok, results} = "books" |> DeltaQuery.query() |> execute(config)

      assert DeltaQuery.count(results) == 6
      assert DataFrame.names(results.dataframe) == DataFrame.names(books_df())
      assert %{files_processed: 2, total_files: 2} = results
    end

    test "skips files whose partition values do not match a where predicate" do
      config =
        StubServer.config(%{
          schema_string: StubServer.books_schema([{"library_id", "long"}]),
          files: [
            StubServer.fixture_file("books.parquet", "lib-100.parquet", %{"library_id" => "100"}),
            StubServer.fixture_file("books.parquet", "lib-200.parquet", %{"library_id" => "200"})
          ]
        })

      {:ok, results} = "books" |> DeltaQuery.query() |> DeltaQuery.where("library_id = 100") |> execute(config)

      assert DeltaQuery.count(results) == 3
      assert %{files_processed: 1, total_files: 1} = results
      assert_received {:stub_request, :get, "/lib-100.parquet", nil, _}
      refute_received {:stub_request, :get, "/lib-200.parquet", nil, _}
    end

    for {predicate, column, type, matching, other} <- [
          {"available = true", "available", "boolean", "true", "false"},
          {"category = 'Fiction'", "category", "string", "Fiction", "Mystery"},
          {"price > 9.5", "price", "double", "10.0", "9.0"},
          {"year >= 2024", "year", "integer", "2024", "2023"},
          {"shard_id > 9007199254740992", "shard_id", "long", "9007199254740993", "9007199254740992"},
          {"stocked_on >= '2024-03-01'", "stocked_on", "date", "2024-03-01", "2024-02-29"}
        ] do
      test "prunes files on partition value with predicate #{predicate}" do
        config =
          StubServer.config(%{
            schema_string: StubServer.books_schema([{unquote(column), unquote(type)}]),
            files: [
              StubServer.fixture_file("books.parquet", "match.parquet", %{unquote(column) => unquote(matching)}),
              StubServer.fixture_file("books.parquet", "other.parquet", %{unquote(column) => unquote(other)})
            ]
          })

        {:ok, results} = "books" |> DeltaQuery.query() |> DeltaQuery.where(unquote(predicate)) |> execute(config)

        assert DeltaQuery.count(results) == 3
        assert_received {:stub_request, :get, "/match.parquet", nil, _}
        refute_received {:stub_request, :get, "/other.parquet", nil, _}
      end
    end

    test "prunes a file whose partition value is null when the predicate needs a value" do
      config =
        StubServer.config(%{
          schema_string: StubServer.books_schema([{"shelf_id", "long"}]),
          files: [
            StubServer.fixture_file("books.parquet", "shelf-7.parquet", %{"shelf_id" => "7"}),
            StubServer.fixture_file("books.parquet", "shelf-null.parquet", %{"shelf_id" => nil})
          ]
        })

      {:ok, results} = "books" |> DeltaQuery.query() |> DeltaQuery.where("shelf_id = 7") |> execute(config)

      assert %{files_processed: 1, total_files: 1} = results
      refute_received {:stub_request, :get, "/shelf-null.parquet", nil, _}
    end

    test "returns an error when a where predicate cannot be applied to a partition column" do
      config =
        StubServer.config(%{
          schema_string: StubServer.books_schema([{"region", "string"}]),
          files: [StubServer.fixture_file("books.parquet", "east.parquet", %{"region" => "east"})]
        })

      {:error, message} = "books" |> DeltaQuery.query() |> DeltaQuery.where("region > 'a'") |> execute(config)

      assert message =~ "operator >"
      assert message =~ "region"
      refute_received {:stub_request, :get, "/east.parquet", nil, _}
    end

    test "returns an error when a where literal does not match a partition column's type" do
      config =
        StubServer.config(%{
          schema_string: StubServer.books_schema([{"library_id", "long"}]),
          files: [StubServer.fixture_file("books.parquet", "lib-100.parquet", %{"library_id" => "100"})]
        })

      {:error, message} = "books" |> DeltaQuery.query() |> DeltaQuery.where("library_id = '100'") |> execute(config)

      assert message =~ "does not match"
      assert message =~ "library_id"
      refute_received {:stub_request, :get, "/lib-100.parquet", nil, _}
    end

    test "returns an error when a where literal does not match a data column's type" do
      config = StubServer.config()

      {:error, message} = "books" |> DeltaQuery.query() |> DeltaQuery.where("book_id = '1002'") |> execute(config)

      assert message =~ "does not match"
      assert message =~ "book_id"
    end

    test "downloads a file whose partition value cannot be cast and then skips it" do
      config =
        StubServer.config(%{
          schema_string: StubServer.books_schema([{"shelf_id", "long"}]),
          files: [StubServer.fixture_file("books.parquet", "books.parquet", %{"shelf_id" => "seven"})]
        })

      {:ok, results} = "books" |> DeltaQuery.query() |> DeltaQuery.where("shelf_id = 7") |> execute(config)

      assert %{files_processed: 0, total_files: 1} = results
      assert_received {:stub_request, :get, "/books.parquet", nil, _}
    end

    test "adds partition columns to every row, typed from the table schema" do
      config =
        StubServer.config(%{
          schema_string:
            StubServer.books_schema([
              {"region", "string"},
              {"shelf_id", "long"},
              {"active", "boolean"},
              {"stocked_on", "date"},
              {"unit_price", "double"},
              {"list_price", "decimal(10,2)"},
              {"refreshed_at", "timestamp"},
              {"checked_at", "timestamp_ntz"},
              {"aisle", "integer"},
              {"rack", "short"},
              {"bin", "byte"},
              {"weight", "float"}
            ]),
          files: [
            StubServer.fixture_file("books.parquet", "east.parquet", %{
              "region" => "east",
              "shelf_id" => "7",
              "active" => "true",
              "stocked_on" => "2024-03-01",
              "unit_price" => "9.5",
              "list_price" => "12.50",
              "refreshed_at" => "2024-03-01 10:15:00",
              "checked_at" => "2024-03-02 08:00:00",
              "aisle" => "12",
              "rack" => "3",
              "bin" => "1",
              "weight" => "1.5"
            })
          ]
        })

      {:ok, results} = "books" |> DeltaQuery.query() |> execute(config)

      assert DeltaQuery.count(results) == 3

      assert %{
               "region" => "east",
               "shelf_id" => 7,
               "active" => true,
               "stocked_on" => ~D[2024-03-01],
               "unit_price" => 9.5,
               "list_price" => list_price,
               "refreshed_at" => ~N[2024-03-01 10:15:00.000000],
               "checked_at" => ~N[2024-03-02 08:00:00.000000],
               "aisle" => 12,
               "rack" => 3,
               "bin" => 1,
               "weight" => 1.5,
               "title" => "The Great Gatsby"
             } = DeltaQuery.first(results)

      assert Decimal.equal?(list_price, Decimal.new("12.50"))

      assert %{
               "region" => :string,
               "shelf_id" => {:s, 64},
               "active" => :boolean,
               "stocked_on" => :date,
               "unit_price" => {:f, 64},
               "list_price" => {:decimal, 10, 2},
               "refreshed_at" => {:naive_datetime, :microsecond},
               "checked_at" => {:naive_datetime, :microsecond},
               "aisle" => {:s, 32},
               "rack" => {:s, 16},
               "bin" => {:s, 8},
               "weight" => {:f, 32}
             } = DataFrame.dtypes(results.dataframe)
    end

    test "keeps partition columns available to select/2 and filter/2" do
      config =
        StubServer.config(%{
          schema_string: StubServer.books_schema([{"shelf_id", "long"}]),
          files: [
            StubServer.fixture_file("books.parquet", "shelf-7.parquet", %{"shelf_id" => "7"}),
            StubServer.fixture_file("books.parquet", "shelf-8.parquet", %{"shelf_id" => "8"})
          ]
        })

      {:ok, results} = "books" |> DeltaQuery.query() |> DeltaQuery.select(["shelf_id", "title"]) |> execute(config)

      assert DataFrame.names(results.dataframe) == ["shelf_id", "title"]
      assert DeltaQuery.count(results) == 6

      assert {:ok, shelf_7} = DeltaQuery.filter(results, ["shelf_id = 7"])
      assert DeltaQuery.count(shelf_7) == 3
    end

    test "keeps a null partition value as nil" do
      config =
        StubServer.config(%{
          schema_string: StubServer.books_schema([{"shelf_id", "long"}]),
          files: [StubServer.fixture_file("books.parquet", "books.parquet", %{"shelf_id" => nil})]
        })

      {:ok, results} = "books" |> DeltaQuery.query() |> execute(config)

      assert DataFrame.dtypes(results.dataframe)["shelf_id"] == {:s, 64}
      assert Enum.map(DeltaQuery.to_rows(results), & &1["shelf_id"]) == [nil, nil, nil]
    end

    for {value, expected} <- [{"NaN", :nan}, {"Infinity", :infinity}, {"-Infinity", :neg_infinity}] do
      test "keeps a #{value} float partition value" do
        config =
          StubServer.config(%{
            schema_string: StubServer.books_schema([{"weight", "double"}]),
            files: [StubServer.fixture_file("books.parquet", "books.parquet", %{"weight" => unquote(value)})]
          })

        {:ok, results} = "books" |> DeltaQuery.query() |> execute(config)

        assert Enum.map(DeltaQuery.to_rows(results), & &1["weight"]) == List.duplicate(unquote(expected), 3)
      end
    end

    test "keeps a partition column that is not in the table schema as a string" do
      config =
        StubServer.config(%{files: [StubServer.fixture_file("books.parquet", "books.parquet", %{"shelf_id" => "7"})]})

      {:ok, results} = "books" |> DeltaQuery.query() |> execute(config)

      assert %{"shelf_id" => "7"} = DeltaQuery.first(results)
    end

    test "does not overwrite a column the file already carries with its partition value" do
      config =
        StubServer.config(%{files: [StubServer.fixture_file("books.parquet", "books.parquet", %{"library_id" => "999"})]})

      {:ok, results} = "books" |> DeltaQuery.query() |> execute(config)

      assert Enum.map(DeltaQuery.to_rows(results), & &1["library_id"]) == [100, 100, 100]
    end

    test "skips a file whose partition value does not match its schema type" do
      config =
        StubServer.config(%{
          schema_string: StubServer.books_schema([{"shelf_id", "long"}]),
          files: [StubServer.fixture_file("books.parquet", "books.parquet", %{"shelf_id" => "seven"})]
        })

      {:ok, results} = "books" |> DeltaQuery.query() |> execute(config)

      assert DeltaQuery.empty?(results)
      assert %{files_processed: 0, total_files: 1} = results
    end

    test "skips a file whose partition value is out of range for its schema type" do
      config =
        StubServer.config(%{
          schema_string: StubServer.books_schema([{"rack", "short"}]),
          files: [StubServer.fixture_file("books.parquet", "books.parquet", %{"rack" => "70000"})]
        })

      {:ok, results} = "books" |> DeltaQuery.query() |> execute(config)

      assert DeltaQuery.empty?(results)
      assert %{files_processed: 0, total_files: 1} = results
    end

    test "skips files that fail to download and reports the shortfall in the counts" do
      config =
        StubServer.config(%{
          files: [
            StubServer.fixture_file("books.parquet", "part-1.parquet"),
            StubServer.fixture_file("books.parquet", "part-2.parquet")
          ],
          errors: %{"/part-2.parquet" => {403, "denied"}}
        })

      {:ok, results} = "books" |> DeltaQuery.query() |> execute(config)

      assert DeltaQuery.count(results) == 3
      assert %{files_processed: 1, total_files: 2} = results
    end

    test "skips file actions that carry no url" do
      action = %{
        "file" => %{"id" => "x", "deltaSingleAction" => %{"add" => %{"path" => "https://files.test/books.parquet"}}}
      }

      config = StubServer.config(%{query_actions: [action]})

      {:ok, results} = "books" |> DeltaQuery.query() |> execute(config)

      assert DeltaQuery.empty?(results)
      assert %{files_processed: 0, total_files: 1} = results
    end

    test "returns an error for a query response that is not NDJSON" do
      config = StubServer.config(%{raw_query_body: "<html>upstream timeout</html>"})

      assert {:error, :invalid_response_format} = "books" |> DeltaQuery.query() |> execute(config)
    end

    test "returns an error when an ordering operator targets a string column" do
      config = StubServer.config()

      assert {:error, message} = "books" |> DeltaQuery.query() |> DeltaQuery.where("title > 'M'") |> execute(config)
      assert message =~ "operator >"
    end
  end

  describe "API errors" do
    for status <- [401, 403, 404, 500] do
      test "execute returns {:error, {:api_error, #{status}, body}}" do
        body = %{"errorCode" => "E", "message" => "nope"}
        config = StubServer.config(%{errors: %{@query_path => {unquote(status), body}}})

        assert {:error, {:api_error, unquote(status), raw}} = "books" |> DeltaQuery.query() |> execute(config)
        assert raw =~ "nope"
      end
    end

    test "list_tables returns the decoded JSON error body" do
      path = "/shares/stub_share/schemas/public/tables"
      body = %{"errorCode" => "UNAUTHENTICATED", "message" => "token expired"}
      config = StubServer.config(%{errors: %{path => {401, body}}})

      assert {:error, {:api_error, 401, %{"message" => "token expired"}}} = DeltaQuery.list_tables(config: config)
    end

    test "list_tables returns a non-JSON error page as a string" do
      path = "/shares/stub_share/schemas/public/tables"
      config = StubServer.config(%{errors: %{path => {502, "<html>Bad Gateway</html>", "text/html"}}})

      assert {:error, {:api_error, 502, "<html>Bad Gateway</html>"}} = DeltaQuery.list_tables(config: config)
    end

    test "list_schemas wraps a malformed JSON success body as a failed request" do
      path = "/shares/stub_share/schemas"
      config = StubServer.config(%{errors: %{path => {200, "<html>", "application/json"}}})

      assert {:error, {:request_failed, %Jason.DecodeError{}}} = DeltaQuery.list_schemas(config: config)
    end

    test "execute returns 404 for a table the share does not contain" do
      config = StubServer.config()

      assert {:error, {:api_error, 404, body}} = "nope" |> DeltaQuery.query() |> execute(config)
      assert body =~ "does not exist"
    end
  end

  describe "discovery" do
    test "list_schemas returns schema names" do
      config = StubServer.config()

      assert {:ok, ["public", "analytics"]} = DeltaQuery.list_schemas(config: config)
      assert_received {:stub_request, :get, "/shares/stub_share/schemas", nil, _}
    end

    test "list_tables uses the configured schema by default" do
      config = StubServer.config()

      assert {:ok, ["books", "loans"]} = DeltaQuery.list_tables(config: config)
      assert_received {:stub_request, :get, "/shares/stub_share/schemas/public/tables", nil, _}
    end

    test "list_tables accepts a schema override" do
      config = StubServer.config()

      assert {:ok, ["books", "loans"]} = DeltaQuery.list_tables(config: config, schema: "analytics")
      assert_received {:stub_request, :get, "/shares/stub_share/schemas/analytics/tables", nil, _}
    end

    test "list_tables returns 404 for an unknown schema" do
      config = StubServer.config()

      assert {:error, {:api_error, 404, _}} = DeltaQuery.list_tables(config: config, schema: "missing")
    end

    test "get_table_schema returns columns parsed from table metadata" do
      config = StubServer.config()

      assert {:ok, columns} = DeltaQuery.get_table_schema(config: config, table: "books")

      assert columns == [
               %Column{name: "book_id", type: "long"},
               %Column{name: "title", type: "string"},
               %Column{name: "page_count", type: "long"}
             ]

      assert_received {:stub_request, :get, "/shares/stub_share/schemas/public/tables/books/metadata", nil, _}
    end
  end

  describe "documented behaviours" do
    test "a where predicate that fails to parse is still sent to the server and ignored locally" do
      config = StubServer.config()

      {:ok, results} = "books" |> DeltaQuery.query() |> DeltaQuery.where("book_id => 1002") |> execute(config)

      assert DeltaQuery.count(results) == 3
      assert_received {:stub_request, :post, @query_path, %{"predicateHints" => ["book_id => 1002"]}, _}
    end

    test "a where predicate on a column not in the data matches every row" do
      config = StubServer.config()

      {:ok, results} = "books" |> DeltaQuery.query() |> DeltaQuery.where("missing = 1") |> execute(config)

      assert DeltaQuery.count(results) == 3
    end

    test "select with no matching columns returns every column" do
      config = StubServer.config()

      {:ok, results} = "books" |> DeltaQuery.query() |> DeltaQuery.select(["missing"]) |> execute(config)

      assert DataFrame.names(results.dataframe) == DataFrame.names(books_df())
    end

    test "a malformed date literal against a date column raises ArgumentError" do
      config = StubServer.config(%{files: [StubServer.fixture_file("publishers.parquet")]})

      assert_raise ArgumentError, ~r/invalid date/, fn ->
        "books" |> DeltaQuery.query() |> DeltaQuery.where("publication_date > 'yesterday'") |> execute(config)
      end
    end
  end
end

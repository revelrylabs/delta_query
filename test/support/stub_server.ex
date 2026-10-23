defmodule DeltaQuery.StubServer do
  @moduledoc """
  In-process stand-in for a Delta Sharing server and its file store, used as a `Req` adapter.

  `config/1` stores a scenario in the calling process and returns a `DeltaQuery.Config` whose
  requests are answered by `run/1`. Every request is also reported back to the test process as
  `{:stub_request, method, path, decoded_body, headers}` so tests can assert on what was sent.

  ## Scenario keys

  - `:schemas` - schema names returned by the schemas endpoint
  - `:tables` - table names returned by the tables endpoint
  - `:schema_string` - Delta schema JSON returned in table metadata
  - `:files` - list of `%{name:, parquet:, partition_values:}`, served at `https://files.test/<name>`
  - `:format` - `:v1` wraps file actions in `"add"`, `:v2` in `"file"`
  - `:query_actions` - literal NDJSON action maps to return instead of generating them from `:files`
  - `:raw_query_body` - literal body to return from the query endpoint
  - `:errors` - map of request path to `{status, body}` or `{status, body, content_type}`
  """

  alias Explorer.DataFrame

  @endpoint "https://sharing.test"
  @share "stub_share"
  @files_host "files.test"
  @token "stub-token"

  @books_schema ~s({"type":"struct","fields":[{"name":"book_id","type":"long"},{"name":"title","type":"string"},{"name":"page_count","type":"long"}]})

  def endpoint, do: @endpoint
  def share, do: @share
  def token, do: @token

  def config(scenario \\ %{}) do
    Process.put(__MODULE__, Map.merge(default_scenario(), scenario))

    DeltaQuery.configure!(
      endpoint: @endpoint,
      bearer_token: @token,
      share: @share,
      req_options: [adapter: __MODULE__, retry: false]
    )
  end

  def file(name, %DataFrame{} = df, partition_values \\ %{}) do
    %{name: name, parquet: DataFrame.dump_parquet!(df), partition_values: partition_values}
  end

  def fixture_file(fixture, name \\ nil, partition_values \\ %{}) do
    path = Path.join([File.cwd!(), "test", "support", "fixtures", fixture])
    file(name || fixture, DataFrame.from_parquet!(path), partition_values)
  end

  def run(%Req.Request{} = request) do
    scenario = Process.get(__MODULE__) || default_scenario()
    path = request.url.path || "/"

    send(self(), {:stub_request, request.method, path, decode_request_body(request.body), request.headers})

    {request, respond(request.method, request.url.host, path, scenario)}
  end

  defp default_scenario do
    %{
      schemas: ["public", "analytics"],
      tables: ["books", "loans"],
      schema_string: @books_schema,
      files: [fixture_file("books.parquet")],
      format: :v1,
      query_actions: nil,
      raw_query_body: nil,
      errors: %{}
    }
  end

  defp respond(method, host, path, scenario) do
    case Map.get(scenario.errors, path) do
      nil -> route(method, host, String.split(path, "/", trim: true), scenario)
      {status, body} -> error_response(status, body, nil)
      {status, body, content_type} -> error_response(status, body, content_type)
    end
  end

  defp route(:get, @files_host, [name], scenario) do
    case Enum.find(scenario.files, &(&1.name == name)) do
      nil -> Req.Response.new(status: 404, body: "no such file: #{name}")
      file -> Req.Response.new(status: 200, body: file.parquet)
    end
  end

  defp route(:get, _host, ["shares", @share, "schemas"], scenario) do
    json(%{"items" => Enum.map(scenario.schemas, &%{"name" => &1, "share" => @share})})
  end

  defp route(:get, _host, ["shares", @share, "schemas", schema, "tables"], scenario) do
    if schema in scenario.schemas do
      json(%{"items" => Enum.map(scenario.tables, &%{"name" => &1, "schema" => schema, "share" => @share})})
    else
      not_found("schema '#{schema}' does not exist")
    end
  end

  defp route(:get, _host, ["shares", @share, "schemas", _schema, "tables", table, "metadata"], scenario) do
    if table in scenario.tables do
      ndjson([protocol_action(), metadata_action(scenario)])
    else
      not_found("table '#{table}' does not exist")
    end
  end

  defp route(:post, _host, ["shares", @share, "schemas", _schema, "tables", table, "query"], scenario) do
    cond do
      table not in scenario.tables -> not_found("table '#{table}' does not exist")
      scenario.raw_query_body -> Req.Response.new(status: 200, body: scenario.raw_query_body)
      true -> ndjson([protocol_action(), metadata_action(scenario) | file_actions(scenario)])
    end
  end

  defp route(_method, _host, _segments, _scenario), do: not_found("no such route")

  defp file_actions(%{query_actions: actions}) when is_list(actions), do: actions

  defp file_actions(scenario) do
    Enum.map(scenario.files, fn file ->
      action = %{
        "url" => "https://#{@files_host}/#{file.name}",
        "id" => file.name,
        "partitionValues" => file.partition_values,
        "size" => byte_size(file.parquet)
      }

      case scenario.format do
        :v1 -> %{"add" => action}
        :v2 -> %{"file" => action}
      end
    end)
  end

  defp protocol_action, do: %{"protocol" => %{"minReaderVersion" => 1}}

  defp metadata_action(scenario) do
    %{
      "metaData" => %{
        "id" => "stub-table",
        "format" => %{"provider" => "parquet"},
        "schemaString" => scenario.schema_string,
        "partitionColumns" => []
      }
    }
  end

  defp json(body) do
    Req.Response.new(status: 200, headers: [{"content-type", "application/json"}], body: Jason.encode!(body))
  end

  defp ndjson(actions) do
    Req.Response.new(status: 200, body: Enum.map_join(actions, "\n", &Jason.encode!/1))
  end

  defp not_found(message) do
    error_response(404, %{"errorCode" => "RESOURCE_DOES_NOT_EXIST", "message" => message}, nil)
  end

  defp error_response(status, body, content_type) when is_map(body) do
    error_response(status, Jason.encode!(body), content_type || "application/json")
  end

  defp error_response(status, body, content_type) do
    Req.Response.new(status: status, headers: [{"content-type", content_type || "text/plain"}], body: body)
  end

  defp decode_request_body(nil), do: nil
  defp decode_request_body(""), do: nil

  defp decode_request_body(body) do
    case body |> IO.iodata_to_binary() |> Jason.decode() do
      {:ok, decoded} -> decoded
      {:error, _} -> body
    end
  end
end

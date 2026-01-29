# Live test script - run with: mix run test_live.exs
#
# Requires environment variables:
#   DELTA_SHARING_ENDPOINT
#   DELTA_SHARING_BEARER_TOKEN
#   DELTA_SHARING_SHARE
#   DELTA_SHARING_SCHEMA (optional, defaults to "public")
#   DELTA_SHARING_TEST_TABLE
#   DELTA_SHARING_TEST_COLUMNS (optional, comma-separated)
#   DELTA_SHARING_TEST_FILTER (optional)

defmodule LiveTest do
  def run do
    IO.puts("=== DeltaQuery Live Test ===\n")

    # Build config from environment
    config =
      DeltaQuery.configure!(
        endpoint: System.fetch_env!("DELTA_SHARING_ENDPOINT"),
        bearer_token: System.fetch_env!("DELTA_SHARING_BEARER_TOKEN"),
        share: System.fetch_env!("DELTA_SHARING_SHARE"),
        schema: System.get_env("DELTA_SHARING_SCHEMA", "public")
      )

    IO.puts("Config:")
    IO.puts("  Endpoint: #{config.endpoint}")
    IO.puts("  Share: #{config.share}")
    IO.puts("  Schema: #{config.schema}")
    IO.puts("")

    # Test discovery
    test_discovery(config)

    table = System.fetch_env!("DELTA_SHARING_TEST_TABLE")
    columns = parse_columns(System.get_env("DELTA_SHARING_TEST_COLUMNS"))
    filter = System.get_env("DELTA_SHARING_TEST_FILTER")

    IO.puts("Query Config:")
    IO.puts("  Table: #{table}")
    IO.puts("  Columns: #{inspect(columns)}")
    IO.puts("  Filter: #{filter || "(none)"}")
    IO.puts("")

    # Build query
    query = DeltaQuery.query(table)

    query =
      if columns do
        DeltaQuery.select(query, columns)
      else
        query
      end

    query =
      if filter do
        DeltaQuery.where(query, filter)
      else
        query
      end

    query = DeltaQuery.limit(query, 10)

    IO.puts("Executing query...")
    start = System.monotonic_time(:millisecond)

    case DeltaQuery.execute(query, config: config) do
      {:ok, results} ->
        elapsed = System.monotonic_time(:millisecond) - start
        IO.puts("Query completed in #{elapsed}ms\n")

        row_count = DeltaQuery.count(results)
        IO.puts("Results: #{row_count} rows")

        if row_count > 0 do
          IO.puts("\nFirst row:")
          first = DeltaQuery.first(results)
          IO.inspect(first, pretty: true, limit: :infinity)

          IO.puts("\nAll rows:")
          rows = DeltaQuery.to_rows(results)
          IO.inspect(rows, pretty: true, limit: :infinity)
        end

        IO.puts("\n=== Test Passed ===")

      {:error, reason} ->
        IO.puts("Query failed: #{inspect(reason)}")
        IO.puts("\n=== Test Failed ===")
        System.halt(1)
    end
  end

  defp test_discovery(config) do
    IO.puts("=== Testing Discovery ===\n")

    IO.puts("Listing schemas...")

    case DeltaQuery.list_schemas(config: config) do
      {:ok, schemas} ->
        IO.puts("Found #{length(schemas)} schema(s):")
        Enum.each(schemas, fn schema -> IO.puts("  - #{schema}") end)
        IO.puts("")

      {:error, reason} ->
        IO.puts("Failed to list schemas: #{inspect(reason)}")
        IO.puts("")
    end

    IO.puts("Listing tables in schema '#{config.schema}'...")

    tables =
      case DeltaQuery.list_tables(config: config) do
        {:ok, tables} ->
          IO.puts("Found #{length(tables)} table(s):")
          Enum.each(tables, fn table -> IO.puts("  - #{table}") end)
          IO.puts("")
          tables

        {:error, reason} ->
          IO.puts("Failed to list tables: #{inspect(reason)}")
          IO.puts("")
          []
      end

    # Show schema for first table
    if tables != [] do
      first_table = List.first(tables)
      IO.puts("Getting schema for table '#{first_table}'...")

      case DeltaQuery.get_table_schema(table: first_table, config: config) do
        {:ok, columns} ->
          IO.puts("Found #{length(columns)} column(s):")

          Enum.each(columns, fn col ->
            IO.puts("  - #{col.name} (#{col.type})")
          end)

          IO.puts("")

        {:error, reason} ->
          IO.puts("Failed to get table schema: #{inspect(reason)}")
          IO.puts("")
      end
    end

    IO.puts("=== Discovery Complete ===\n")
  end

  defp parse_columns(nil), do: nil
  defp parse_columns(""), do: nil

  defp parse_columns(columns_str) do
    columns_str
    |> String.split(",")
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
  end
end

LiveTest.run()

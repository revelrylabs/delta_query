defmodule DeltaQuery.FixtureHelper do
  @moduledoc """
  Helper module for loading test fixtures in tests.
  """

  @doc """
  Load a parquet fixture file and return it as a DeltaQuery.Results struct.
  """
  def load_fixture(filename) do
    path = Path.join([File.cwd!(), "test", "support", "fixtures", filename])
    df = Explorer.DataFrame.from_parquet!(path)
    %DeltaQuery.Results{dataframe: df, files_processed: 1, total_files: 1}
  end

  @doc """
  Load multiple fixtures and return them as a map.
  """
  def load_fixtures(filenames) when is_list(filenames) do
    Map.new(filenames, fn filename ->
      key = filename |> Path.basename(".parquet") |> String.to_atom()
      {key, load_fixture(filename)}
    end)
  end
end

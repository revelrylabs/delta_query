defmodule DeltaQuery.Column do
  @moduledoc """
  Represents a column in a Delta table schema.
  """

  @type t :: %__MODULE__{
          name: String.t(),
          type: String.t()
        }

  defstruct [:name, :type]

  @doc false
  def from_metadata(%{"metaData" => %{"schemaString" => schema_string}}) when is_binary(schema_string) do
    case Jason.decode(schema_string) do
      {:ok, %{"fields" => fields}} when is_list(fields) ->
        Enum.map(fields, &%__MODULE__{name: &1["name"], type: normalize_type(&1["type"])})

      _ ->
        []
    end
  end

  def from_metadata(_metadata), do: []

  defp normalize_type(type) when is_binary(type), do: type
  defp normalize_type(%{"type" => "struct"}), do: "struct"
  defp normalize_type(%{"type" => "array"}), do: "array"
  defp normalize_type(%{"type" => "map"}), do: "map"
  defp normalize_type(_), do: "unknown"
end

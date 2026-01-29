defmodule DeltaQuery.Column do
  @moduledoc """
  Represents a column in a Delta table schema.
  """

  @type t :: %__MODULE__{
          name: String.t(),
          type: String.t()
        }

  defstruct [:name, :type]
end

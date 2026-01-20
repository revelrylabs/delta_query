defmodule DeltaQuery.Config do
  @moduledoc """
  Runtime configuration for Delta Query client.

  Configuration can be set via:

  1. Application environment (config.exs):

      config :delta_query, :config,
        endpoint: "https://...",
        bearer_token: "...",
        share: "my_share",
        schema: "public",
        finch_name: MyApp.Finch

  2. Per-query options passed to `DeltaQuery.Query.execute/2`

  ## Required Keys

  - `:endpoint` - Delta Sharing server URL
  - `:bearer_token` - Authentication token
  - `:share` - Share name to query

  ## Optional Keys

  - `:schema` - Schema name (default: "public")
  - `:finch_name` - Finch pool name (default: `:delta_query_finch`)
  """

  @type t :: %__MODULE__{
          endpoint: String.t(),
          bearer_token: String.t(),
          share: String.t(),
          schema: String.t(),
          finch_name: atom()
        }

  @enforce_keys [:endpoint, :bearer_token, :share]
  defstruct [:endpoint, :bearer_token, :share, schema: "public", finch_name: :delta_query_finch]

  @doc """
  Build a config struct from keyword options, falling back to application env.
  """
  @spec new(keyword()) :: {:ok, t()} | {:error, String.t()}
  def new(opts \\ []) do
    app_config = Application.get_env(:delta_query, :config, [])
    merged = Keyword.merge(app_config, opts)

    endpoint = Keyword.get(merged, :endpoint, "")
    bearer_token = Keyword.get(merged, :bearer_token, "")
    share = Keyword.get(merged, :share, "")
    schema = Keyword.get(merged, :schema, "public")
    finch_name = Keyword.get(merged, :finch_name, :delta_query_finch)

    cond do
      endpoint == "" or is_nil(endpoint) ->
        {:error, "endpoint is required"}

      bearer_token == "" or is_nil(bearer_token) ->
        {:error, "bearer_token is required"}

      share == "" or is_nil(share) ->
        {:error, "share is required"}

      true ->
        {:ok,
         %__MODULE__{
           endpoint: endpoint,
           bearer_token: bearer_token,
           share: share,
           schema: schema,
           finch_name: finch_name
         }}
    end
  end

  @doc """
  Build a config struct, raising on error.
  """
  @spec new!(keyword()) :: t()
  def new!(opts \\ []) do
    case new(opts) do
      {:ok, config} -> config
      {:error, reason} -> raise ArgumentError, reason
    end
  end
end

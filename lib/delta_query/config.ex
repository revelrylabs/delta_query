defmodule DeltaQuery.Config do
  @moduledoc """
  Runtime configuration for Delta Query client.

  Configuration can be set via:

  1. Application environment (config.exs):

      config :delta_query, :config,
        endpoint: "https://...",
        bearer_token: "...",
        share: "my_share",
        schema: "public"

  2. Per-query options passed to `DeltaQuery.Query.execute/2`

  ## Required Keys

  - `:endpoint` - Delta Sharing server URL
  - `:bearer_token` - Authentication token
  - `:share` - Share name to query

  ## Optional Keys

  - `:schema` - Schema name (default: "public")
  - `:req_options` - Options passed to Req requests (default: `[]`)
  """

  @type t :: %__MODULE__{
          endpoint: String.t(),
          bearer_token: String.t(),
          share: String.t(),
          schema: String.t(),
          req_options: keyword()
        }

  @enforce_keys [:endpoint, :bearer_token, :share]
  defstruct [:endpoint, :bearer_token, :share, schema: "public", req_options: []]

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
    req_options = Keyword.get(merged, :req_options, [])

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
           req_options: req_options
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

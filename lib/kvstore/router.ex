defmodule KVstore.Router do
  @moduledoc """
  App router

  methods:

    GET    `/store`      - list all keys
    POST   `/store`      - create new key
    GET    `/store/:key` - detail key data
    PUT    `/store/:key` - update key data
    DELETE `/store/:key` - delete key
  """

  defmodule KeyNotFound do
    @moduledoc """
    Custom exception
    """
    defexception message: "Key wasn't found", plug_status: 404
  end

  use Plug.Router
  use Plug.ErrorHandler

  alias KVstore.Storage

  plug Plug.Logger
  plug :match
  plug Plug.Parsers, parsers: [:urlencoded, :multipart]
  plug :dispatch

  get "/store" do
    keys =
      Storage.list()
      |> Enum.map(&format_key/1)
      |> Enum.join("<br>")

    result = """
    <h1>Keys</h1>
    #{keys}
    """

    put_resp_content_type(conn, "text/html")
    |> send_resp(200, result)
  end

  post "/store" do
    validate_request!(conn, ["key", "value", "ttl"])

    %{"key" => key, "value" => value, "ttl" => ttl} = conn.body_params
    {ttl, _} = Integer.parse(ttl, 10)

    {status, message} =
      case Storage.get(key) do
        :not_found ->
          Storage.create(key, value, ttl)
          {201, format_key({key, value, ttl})}

        _ ->
          {400, "Key already exists"}
      end

    send_resp(conn, status, message)
  end

  get "/store/:key" do
    key_info = get_key_or_404!(key)
    send_resp(conn, 200, format_key(key_info))
  end

  put "/store/:key" do
    get_key_or_404!(key)
    validate_request!(conn, ["value", "ttl"])

    %{"value" => v, "ttl" => ttl} = conn.body_params
    {ttl, _} = Integer.parse(ttl, 10)

    Storage.create(key, v, ttl)
    send_resp(conn, 200, format_key({key, v, ttl}))
  end

  delete "/store/:key" do
    get_key_or_404!(key)
    Storage.delete(key)
    send_resp(conn, 204, "")
  end

  match _ do
    send_resp(conn, 404, "oops")
  end

  def handle_errors(conn, %{reason: %{message: message}}) do
    send_resp(conn, conn.status, message || "Something went wrong")
  end

  def handle_errors(conn, _info) do
    send_resp(conn, conn.status, "Error")
  end

  defp validate_request!(%{body_params: body_params}, required_keys) do
    is_valid_request =
      # check all required keys are placed and have values
      Enum.all?(required_keys, fn k ->
        !(Map.get(body_params, k) in ["", nil])
      end) &&
      is_valid_ttl?(body_params["ttl"])

    if !is_valid_request, do: raise Plug.BadRequestError, message: "Validation error"
  end

  defp is_valid_ttl?(v) do
    # ttl (time to live) must be a string with uint
    case Integer.parse(v, 10) do
      {v, _} ->
        v > 0

      _ ->
      false
    end
  end

  defp get_key_or_404!(key) do
    case Storage.get(key) do
      :not_found ->
        raise KeyNotFound

      result ->
        result
    end
  end

  defp format_key({k, v, ttl}) do
    "#{k}:#{v}:#{ttl}"
  end
end

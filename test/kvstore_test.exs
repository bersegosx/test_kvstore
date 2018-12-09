defmodule KVstore.Test.Router do
  use ExUnit.Case
  use Plug.Test

  alias KVstore.{Router, Storage}

  @opts Router.init([])

  setup do
    Storage.delete_all()

    Enum.each(
      [
        {"one", 2, "100"},
        {"23", "string", 200},
        {"456", 32, 200},
      ],
      fn ({k, v, ttl}) -> Storage.create(k, v, ttl) end
    )

    on_exit fn ->
      Storage.delete_all()
    end

    :ok
  end

  test "list all keys" do
    conn =
      conn(:get, "/store")
      |> Router.call(@opts)

    assert_200(conn)
    assert conn.resp_body == "<h1>Keys</h1>\none:2:100<br>23:string:200<br>456:32:200\n"
  end

  describe "get key" do
    test "already exists" do
      conn =
        conn(:get, "/store/one")
        |> Router.call(@opts)

      assert_200(conn)
      assert conn.resp_body == "one:2:100"
    end

    test "missing" do
      assert_raise Router.KeyNotFound, "Key wasn't found", fn ->
        conn(:get, "/store/badabu!")
        |> Router.call(@opts)
      end
    end
  end

  describe "create key" do
    test "already exists" do
      conn =
        conn(:post, "/store", key: "one", value: 1, ttl: "10")
        |> Router.call(@opts)

      assert resp_info(conn) == {400, "Key already exists"}
    end

    test "missing required keys" do
      data = [
        # not all keys
        [key: "new_key", value: 1],
        [value: 1, ttl: 1],
        [key: "1", ttl: 1],

        # value is empty
        [key: "1", value: "", ttl: 1],

        # ttl is invalid
        [key: "1", value: "", ttl: "xxx"]
      ]

      for body <- data do
        assert_raise Plug.BadRequestError, "Validation error", fn ->
          conn(:post, "/store", body)
          |> Router.call(@opts)
        end
      end
    end

    test "will create" do
      conn =
        conn(:post, "/store", key: "new_key", value: 1, ttl: "10")
        |> Router.call(@opts)

      assert resp_info(conn) == {201, "new_key:1:10"}
    end
  end

  describe "update key" do
    test "missing" do
      key = "xxx"
      assert Storage.get(key) == :not_found

      assert_raise Router.KeyNotFound, "Key wasn't found", fn ->
        conn(:put, "/store/#{key}")
        |> Router.call(@opts)
      end
    end

    test "will update" do
      key = "one"
      old_data = Storage.get(key)

      {^key, old_value, old_ttl} = old_data
      new_data = [key: key, value: "new_value", ttl: "10"]

      assert old_data != Keyword.values(new_data) |> List.to_tuple

      conn =
        conn(:put, "/store/#{key}", new_data)
        |> Router.call(@opts)

      assert Storage.get(key) == Keyword.values(new_data) |> List.to_tuple
      assert resp_info(conn) == {200, Keyword.values(new_data) |> Enum.join(":")}
    end
  end

  describe "delete key" do
    test "missing" do
      assert_raise Router.KeyNotFound, "Key wasn't found", fn ->
        conn(:delete, "/store/xxx")
        |> Router.call(@opts)
      end
    end

    test "exists" do
      conn =
        conn(:delete, "/store/one")
        |> Router.call(@opts)

      assert resp_info(conn) == {204, ""}
    end
  end

  test "404" do
    conn =
      conn(:get, "/any_page")
      |> Router.call(@opts)

    assert resp_info(conn) == {404, "oops"}
  end

  defp assert_200(conn) do
    assert conn.state == :sent
    assert conn.status == 200
  end

  defp resp_info(conn), do: {conn.status, conn.resp_body}
end

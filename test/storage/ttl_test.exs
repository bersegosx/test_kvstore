defmodule StorageMock do
  @behaviour StorageCanDeleteKey

  def delete(key, remove_from_ttl \\ true) do
    send self(), {:delete, key, remove_from_ttl}
  end
end

defmodule KVstore.Test.Storage.TTL do
  use ExUnit.Case

  alias KVstore.Storage.TTL
  alias KVstore.Utils.PQueue

  setup_all do
    Application.stop(:kvstore)

    on_exit fn ->
      Application.start(:kvstore)
    end
  end

  setup do
    on_exit fn ->
      if Process.whereis(:storage_ttl) do
        GenServer.stop(:storage_ttl)
      end
    end

    :ok
  end

  test "Expired keys will be deleted at start" do
    now = System.system_time(:millisecond)
    dets_data = [
      {"one", 1, now - 1000},
      {"two", 2, now + 5000},
      {"six", 6, now - 700},
      {"ten", 0, now + 800},
    ]

    {_q, key_map} = TTL.from_dets(dets_data, StorageMock)

    assert_receive {:delete, "one", false}, 100
    assert_receive {:delete, "six", false}, 100
    assert Map.keys(key_map) == ["ten", "two"]
  end

  describe "set_expiration_timer" do
    test "with empty queue - nil timer" do
      assert TTL.set_expiration_timer(:gb_trees.empty(), nil) == nil
    end

    test "timer will cancel" do
      # timer will cancel
      timer_ref = Process.send_after(self(), :msg, 100)
      TTL.set_expiration_timer(:gb_trees.empty(), timer_ref)

      refute_receive :msg, 100
    end

    test "keys with negative ttl will be expired immediately" do
      now = System.system_time(:millisecond)
      q =
        :gb_trees.empty()
        |> PQueue.insert("minus", now - 50_000)

      TTL.set_expiration_timer(q, nil)
      assert_receive :key_expire, 100
    end
  end
end

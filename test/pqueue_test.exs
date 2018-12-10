defmodule KVstore.Test.Utils.PQueue do
  use ExUnit.Case, async: true

  alias KVstore.Utils.PQueue

  setup context do
    q =
      if context[:empty_queue] do
        q_empty()
      else
        {ttl, key1, key2} = {100, "Grindevald", "Monetochka"}
        q_empty()
        |> PQueue.insert(key1, ttl)
        |> PQueue.insert(key2, ttl + 100)
      end

    %{q: q}
  end

  describe "insert" do
    @tag :empty_queue
    test "ttl equals", %{q: q} do
      {ttl, key1, key2} = {100, "Grindevald", "Monetochka"}
      q =
        q
        |> PQueue.insert(key1, ttl)
        |> PQueue.insert(key2, ttl)

      assert :gb_trees.take_smallest(q) == {ttl, [key2, key1], q_empty()}
    end

    @tag :empty_queue
    test "ttl is different", %{q: q} do
      {ttl, key1, key2} = {100, "Grindevald", "Monetochka"}
      q =
        q
        |> PQueue.insert(key1, ttl)
        |> PQueue.insert(key2, ttl + 100)

      assert match? {^ttl, [^key1], {1, _}}, :gb_trees.take_smallest(q)
    end
  end

  describe "remove" do
    test "last value will delete key", %{q: q} do
      assert :gb_trees.smallest(q) == {100, ["Grindevald"]}

      q = PQueue.remove(q, "Grindevald", 100)
      assert :gb_trees.smallest(q) == {200, ["Monetochka"]}
    end

    test "not last value", %{q: q} do
      q = PQueue.insert(q, "Kot", 100)
      assert :gb_trees.smallest(q) == {100, ["Kot", "Grindevald"]}

      q = PQueue.remove(q, "Grindevald", 100)
      assert :gb_trees.smallest(q) == {100, ["Kot"]}
    end
  end

  test "remove_all", %{q: q} do
    q = PQueue.insert(q, "Kot", 100)
    assert :gb_trees.smallest(q) == {100, ["Kot", "Grindevald"]}

    q = PQueue.remove_all(q, 100)
    assert :gb_trees.smallest(q) == {200, ["Monetochka"]}
  end

  defp q_empty, do: :gb_trees.empty()
end

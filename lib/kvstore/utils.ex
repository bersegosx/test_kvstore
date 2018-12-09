defmodule KVstore.Utils do
  defmodule PQueue do
    @moduledoc """
    Priority queue based on `:gb_trees`
    """

    def insert(q, key, ttl) do
      if :gb_trees.is_defined(ttl, q) do
        values = :gb_trees.get(ttl, q)
        :gb_trees.update(ttl, [key|values], q)
      else
        :gb_trees.insert(ttl, [key], q)
      end
    end

    def remove(q, key, ttl) do
      new_values =
        :gb_trees.get(ttl, q)
        |> List.delete(key)

      if length(new_values) == 0 do
        :gb_trees.delete(ttl, q)
      else
        :gb_trees.update(ttl, new_values, q)
      end
    end

    def remove_all(q, ttl), do: :gb_trees.delete(ttl, q)
  end
end

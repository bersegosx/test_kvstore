defmodule KVstore.Utils do

  defmodule PQueue do
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

    def remove_all(q, ttl) do
      :gb_trees.delete(ttl, q)
    end
  end

  use GenServer

  require Logger

  @name :pqueue
  @initial_state %{queue: nil, timer: nil, keys: %{}}

  def insert(key, ttl) do
    GenServer.cast(@name, {:insert, key, ttl})
  end

  def remove(key) do
    GenServer.cast(@name, {:remove, key})
  end

  def start_link(queue \\ nil) do
    GenServer.start_link(__MODULE__, queue, name: @name)
  end

  def init(queue) do
    {queue, ttl_to_keys} =
      if queue do
        Enum.reduce(queue.to_list(), %{}, fn ({ttl, key}, acc) ->
          acc
        end)
        {queue, nil}
      else
        {:gb_trees.empty(), %{}}
      end

    timer_ref = set_expiration_timer(queue)

    {:ok, %{@initial_state| queue: queue, timer: timer_ref}}
  end

  def handle_cast({:insert, key, ttl}, state) do
    fire_time = System.system_time(:millisecond) + ttl
    new_queue = :gb_trees.enter(fire_time, key, state.queue)

    {:reply, %{state| queue: new_queue}}
  end

  def handle_cast({:remove, key}, %{queue: queue, timer: ref} = state) do
    {ttl, top_key} = :gb_trees.smallest(queue)

    new_queue =
      :gb_trees.delete(key, queue)
      |> :gb_trees.balance

    timer_ref =
      if top_key == key do
        set_expiration_timer(queue)
      else
        ref
      end

    {:reply, %{state| queue: new_queue, timer: timer_ref}}
  end

  def handle_info(:key_expire, %{queue: queue} = state) do
    {ttl, key, queue} = :gb_trees.smallest(queue)
    IO.inspect({key, ttl}, label: "Key expired")

    timer_ref = set_expiration_timer(queue)

    {:noreply, %{state| queue: queue, timer: timer_ref}}
  end

  defp set_expiration_timer(queue) do
    if :gb_trees.is_empty(queue) do
      nil
    else
      {ttl, _key} = :gb_trees.smallest(queue)

      time = ttl - System.system_time(:millisecond)
      ref = Process.send_after(time, :key_expire, [])

      ref
    end
  end

end

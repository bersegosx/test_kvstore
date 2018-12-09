defmodule KVstore.Storage.TTL do
  @moduledoc """
  Key ttl management
  """

  use GenServer
  require Logger

  alias KVstore.Utils.PQueue
  alias KVstore.Storage

  @name :pqueue
  @initial_state %{queue: nil, timer: nil, keys: %{}}

  def insert(key, ttl) do
    GenServer.cast(@name, {:insert, key, ttl})
  end

  def remove(key) do
    GenServer.cast(@name, {:remove, key})
  end

  def start_link(dets_list \\ []) do
    GenServer.start_link(__MODULE__, dets_list, name: @name)
  end

  def init(dets_list) do
    {queue, keys} = from_dets(dets_list)
    timer_ref = set_expiration_timer(queue, nil)

    {:ok, %{@initial_state| queue: queue, timer: timer_ref, keys: keys}}
  end

  def handle_cast({:insert, key, ttl}, state) do
    new_queue =
      if Map.has_key?(state.keys, key) do
        PQueue.remove(state.queue, key, Map.get(state.keys, key))
      else
        state.queue
      end

    new_queue = PQueue.insert(new_queue, key, ttl)
    ref = set_expiration_timer(new_queue, state.timer)

    {:noreply, %{state| queue: new_queue, timer: ref,
                      keys: Map.put(state.keys, key, ttl)}}
  end

  def handle_cast({:remove, key}, %{queue: queue, timer: ref} = state) do
    {_, top_keys} = :gb_trees.smallest(queue)

    ttl = Map.get(state.keys, key)
    new_queue = PQueue.remove(queue, key, ttl)

    timer_ref =
      if key in top_keys do
        set_expiration_timer(new_queue, ref)
      else
        ref
      end

    {:noreply, %{state| queue: new_queue, timer: timer_ref,
                        keys: Map.drop(state.keys, [key])}}
  end

  def handle_info(:key_expire, %{queue: queue} = state) do
    {_ttl, keys, new_queue} = :gb_trees.take_smallest(queue)
    timer_ref = set_expiration_timer(new_queue, state.timer)
    # remove keys in storage
    Enum.each(keys, &(Storage.delete(&1, false)))

    {:noreply, %{state| queue: new_queue, timer: timer_ref,
                        keys: Map.drop(state.keys, keys)}}
  end

  @doc """
  Load priority queue and keys from dets values
  """
  def from_dets(values) do
    Enum.reduce(values, {:gb_trees.empty(), %{}},
      fn ({key, _v, ttl}, {q, keys} = acc) ->
        now = System.system_time(:millisecond)
        if now > ttl do
          Storage.delete(key, false)
          acc
        else
          {PQueue.insert(q, key, ttl), Map.put(keys, key, ttl)}
        end
      end)
  end

  def set_expiration_timer(queue, ref) do
    if ref, do: Process.cancel_timer(ref)

    if :gb_trees.is_empty(queue) do
      nil
    else
      {ttl, _keys} = :gb_trees.smallest(queue)
      time = ttl - System.system_time(:millisecond)
      Process.send_after(self(), :key_expire, time)
    end
  end
end

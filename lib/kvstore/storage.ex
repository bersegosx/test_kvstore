defmodule StorageCanDeleteKey do
  @callback delete(key :: String.t, remove_from_ttl :: Bool.t) :: term
end

defmodule KVstore.Storage do
  @moduledoc """
  Storage for KV.

  This is `:dets` with option `ram_file: true`,
  calls `:dets.sync` on modification
  """

  @behaviour StorageCanDeleteKey
  use GenServer
  require Logger

  alias KVstore.Storage.TTL

  @name __MODULE__

  def insert(key, value, ttl) do
    GenServer.call(@name, {:insert, key, value, ttl})
  end

  def get(key) do
    case :dets.lookup(@name, key) do
      [] ->
        :not_found

      [{_, _, _} = resp] ->
        resp
    end
  end

  def list do
    select_all = [{{:"$1", :"_", :"$3"}, [], [:"$_"]}]
    :dets.select(KVstore.Storage, select_all)
  end

  def delete(key, remove_from_ttl \\ true) do
    :dets.delete(@name, key)
    :dets.sync(@name)

    if remove_from_ttl do
      TTL.remove(key)
    end
  end

  @doc false
  def delete_all do
    # only for tests
    :dets.delete_all_objects(@name)
    :dets.sync(@name)
  end

  def start_link(_opts) do
    GenServer.start_link(@name, :ok, name: @name)
  end

  def init(:ok) do
    file_path = Application.fetch_env!(:kvstore, :dets_file_path)
    case :dets.open_file(@name, file: to_charlist(file_path), type: :set, ram_file: true) do
      {:ok, _} ->
        TTL.start_link(list())

      {:error, reason} ->
        Logger.error("Can't open dets table - #{file_path}, reason - #{reason}")
        {:stop, reason}
    end
  end

  def handle_call({:insert, key, value, ttl}, _from, state) do
    ttl_end = System.system_time(:millisecond) + ttl

    :ok = :dets.insert(@name, {key, value, ttl_end})
    :dets.sync(@name)
    TTL.insert(key, ttl_end)

    {:reply, :ok, state}
  end

  def terminate(_, _), do: :dets.close(@name)
end

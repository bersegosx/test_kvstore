defmodule KVstore.Storage do
  @moduledoc """
  Storage for KV.

  This is `:dets` with option `ram_file: true` and
  calls `:dets.sync` on modify
  """

  use GenServer
  require Logger

  @name __MODULE__

  def create(key, value, ttl) do
    GenServer.call(@name, {:create, key, value, ttl})
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

  def delete(key) do
    :dets.delete(@name, key)
    :dets.sync(@name)
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
      {:ok, _} = result ->
        result

      {:error, reason} ->
        Logger.error("Can't open dets table - #{file_path}, reason - #{reason}")
        {:stop, reason}
    end
  end

  def handle_call({:create, key, value, ttl}, _from, state) do
    :ok = :dets.insert(@name, {key, value, ttl})
    :dets.sync(@name)

    {:reply, :ok, state}
  end

  def terminate(_, _) do :dets.close(@name) end
end

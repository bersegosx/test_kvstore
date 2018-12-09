defmodule KVstore do
  @moduledoc """
  KV Application
  """

  use Application

  def start(_type, _args) do
    opts = Application.fetch_env!(:kvstore, Cowboy)
    children = [
       Plug.Adapters.Cowboy.child_spec(:http, KVstore.Router, [], opts),
       {KVstore.Storage, []},
    ]

    opts = [strategy: :one_for_one, name: KVstore.Supervisor]
    Supervisor.start_link(children, opts)
  end
end

use Mix.Config

config :kvstore,
  dets_file_path: "./dets_#{Mix.env}.db"

config :kvstore, Cowboy,
  port: 8000,
  max_connections: 1_000,
  compress: true,
  timeout: 2_000


import_config "#{Mix.env}.exs"

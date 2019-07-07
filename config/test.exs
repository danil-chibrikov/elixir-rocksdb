use Mix.Config

config :elixir_rocksdb,
  ets_name: :buffer,
  path_sync: "rocksdb_sync",
  path_subs: "rocksdb_subs",
  open_options: [
    create_if_missing: true
  ]

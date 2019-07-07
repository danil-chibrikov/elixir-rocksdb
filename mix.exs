defmodule ElixirRocksdb.MixProject do
  use Mix.Project

  def project do
    [
      app: :elixir_rocksdb,
      version: "0.1.0",
      elixir: "~> 1.8",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [
        :logger,
        :rocksdb
      ]
    ]
  end

  defp deps do
    [
      {:rocksdb, "~> 1.2"}
    ]
  end
end

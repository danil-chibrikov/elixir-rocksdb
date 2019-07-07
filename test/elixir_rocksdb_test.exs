defmodule ElixirRocksdbTest do
  use ExUnit.Case
  doctest ElixirRocksdb

  test "greets the world" do
    assert ElixirRocksdb.hello() == :world
  end
end

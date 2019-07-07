defmodule TestStream do
  def get_subscribers(test, subs_test, demand) do
    do_selection(demand, %{sync: test, subs: subs_test})
  end

  defp do_selection(demand, %{sync: _sync, subs: subs} = mapdb) do
    itr_subs = ElixirRocksdb.iterator(subs, [])

    result =
      case ElixirRocksdb.iterator_move(itr_subs, :first) do
        :end_of_table ->
          []

        {key, %{offset: _offset} = value} ->
          tokens =
            get_tokens(value, mapdb, demand)
            |> check_number_of_tokens(demand, key, value, subs)

          Enum.map(tokens, fn {_, v} -> v end)
      end

    ElixirRocksdb.iterator_close(itr_subs)

    result
    |> add_subs(demand, mapdb)
  end

  defp get_tokens(
         %{offset: offset},
         %{sync: sync},
         demand
       ) do
    ElixirRocksdb.stream_iterator(sync, "/subs/1", offset)
    |> Enum.take(demand)
  end

  defp check_number_of_tokens(tokens, demand, key, _val, subs)
       when length(tokens) != demand do
    ElixirRocksdb.delete(subs, key)
    tokens
  end

  defp check_number_of_tokens(tokens, _demand, key, value, subs) do
    {token_key, _token_val} = List.last(tokens)
    upd_value = Map.update!(value, :offset, &(&1 = token_key))
    ElixirRocksdb.put(subs, key, upd_value)
    tokens
  end

  defp add_subs(tokens, demand, %{sync: sync, subs: subs})
       when length(tokens) < demand do
    if ElixirRocksdb.count(subs) != 0 do
      [tokens | do_selection(demand - length(tokens), %{sync: sync, subs: subs})]
      |> List.flatten()
    else
      tokens
    end
  end

  defp add_subs(tokens, _, _), do: tokens
end

defmodule ElixirRocksdbTest do
  use ExUnit.Case, async: false

  defp open_and_clean(path, opts) do
    with {:ok, db} <- ElixirRocksdb.open(path, opts),
         _ <- ElixirRocksdb.stream_delete_all(db),
         0 <- ElixirRocksdb.count(db) do
      {:ok, db}
    else
      error -> {:error, error}
    end
  end

  setup_all do
    options = Application.get_all_env(:elixir_rocksdb)
    sync = Keyword.get(options, :path_sync) |> IO.inspect()
    subs = Keyword.get(options, :path_subs) |> IO.inspect()
    opts = Keyword.get(options, :open_options)

    with {:ok, sync_db} <- open_and_clean(sync, opts),
         {:ok, subs_db} <- open_and_clean(subs, opts) do
      {:ok, opts: opts, path: sync, db: sync_db, test_db: subs_db}
    else
      error -> {:error, error}
    end
  end

  test "open", %{opts: opts, path: path} do
    {atom, _db} = ElixirRocksdb.open(path, opts)
    assert :error == atom
  end

  test "put/3", %{db: db} do
    assert :ok == ElixirRocksdb.put(db, :key, "val")
    ElixirRocksdb.put(db, "key", "val")
    ElixirRocksdb.put(db, "key1", "val")
    ElixirRocksdb.put(db, "key2", :erlang.term_to_binary("val"))
    ElixirRocksdb.put(db, "key3", :erlang.term_to_binary("val"))
    itr = ElixirRocksdb.iterator(db, [])
    assert {"key", "val"} == ElixirRocksdb.iterator_move(itr, :first)
    assert {"key1", "val"} == ElixirRocksdb.iterator_move(itr, :next)
    assert {"key2", "val"} == ElixirRocksdb.iterator_move(itr, :next)
    assert {"key3", "val"} == ElixirRocksdb.iterator_move(itr, :next)
    assert :end_of_table == ElixirRocksdb.iterator_move(itr, :next)
    ElixirRocksdb.iterator_close(itr)
    ElixirRocksdb.stream_delete_all(db)
    assert 0 == ElixirRocksdb.count(db)
  end

  test "put_batch/2", %{db: db} do
    assert :ok == ElixirRocksdb.put_batch(db, [{"key1", "val"}, {"key2", "val2"}])
    assert 2 == ElixirRocksdb.count(db)
    ElixirRocksdb.stream_delete_all(db)
    assert 0 == ElixirRocksdb.count(db)

    assert :ok == ElixirRocksdb.put_batch(db, [{"key1", "val"}, {:key2, "val2"}])
    assert 1 == ElixirRocksdb.count(db)
    ElixirRocksdb.stream_delete_all(db)
    assert 0 == ElixirRocksdb.count(db)

    # put elem=[]
    assert :ok == ElixirRocksdb.put_batch(db, ["qwertyuiop"])

    assert :ok == ElixirRocksdb.put_batch(db, [])
  end

  test "batch/2", %{db: db} do
    assert :ok == ElixirRocksdb.batch(db, [{:put, "key1", "val1"}, {:del, "key1"}])
    assert 0 == ElixirRocksdb.count(db)

    assert :ok == ElixirRocksdb.batch(db, [{:put, "key1", "val"}, {:put, "key2", "val2"}])
    assert 2 == ElixirRocksdb.count(db)
    ElixirRocksdb.stream_delete_all(db)
    assert 0 == ElixirRocksdb.count(db)

    assert :ok == ElixirRocksdb.batch(db, [{:put, "key1", "val"}, {:put, :key2, "val2"}])
    assert 1 == ElixirRocksdb.count(db)
    ElixirRocksdb.stream_delete_all(db)
    assert 0 == ElixirRocksdb.count(db)

    # put elem=[]
    assert :ok == ElixirRocksdb.batch(db, [{:del, "qwertyuiop"}])
    assert 0 == ElixirRocksdb.count(db)

    assert :ok == ElixirRocksdb.batch(db, [])

    key = "key"
    assert :ok == ElixirRocksdb.put(db, key, "value")
    {:ok, value} = ElixirRocksdb.get(db, key)
    new_key = "keykey" <> to_string(:erlang.unique_integer())
    assert 1 == ElixirRocksdb.count(db)

    batch = [
      {:del, key},
      {:put, new_key, value}
    ]

    assert :ok == ElixirRocksdb.batch(db, batch)
    assert 1 == ElixirRocksdb.count(db)

    {:ok, new_value} = ElixirRocksdb.get(db, new_key)
    assert value == new_value

    ElixirRocksdb.stream_delete_all(db)
    assert 0 == ElixirRocksdb.count(db)
  end

  test "get/3", %{db: db} do
    ElixirRocksdb.put(db, "key1", "val")
    assert {:ok, "val"} == ElixirRocksdb.get(db, "key1")
    assert {:ok, nil} == ElixirRocksdb.get(db, "key2")
    assert {:ok, "val"} == ElixirRocksdb.get(db, "key1")
    ElixirRocksdb.stream_delete_all(db)
    assert 0 == ElixirRocksdb.count(db)
  end

  test "iterator_move/2", %{db: db} do
    ElixirRocksdb.put(db, "key1", "val")
    itr = ElixirRocksdb.iterator(db, [])
    assert {"key1", "val"} == ElixirRocksdb.iterator_move(itr, :first)
    assert :end_of_table == ElixirRocksdb.iterator_move(itr, :next)
    assert :ok == ElixirRocksdb.iterator_close(itr)
    ElixirRocksdb.stream_delete_all(db)
    assert 0 == ElixirRocksdb.count(db)
  end

  test "stream_iterator/2", %{db: db} do
    ElixirRocksdb.put_batch(db, [{"key", "val"}, {"qwe", "val5"}, {"key3", "val2"}])
    assert 3 == ElixirRocksdb.count(db)

    assert [{"key", "val"}, {"key3", "val2"}] ==
             ElixirRocksdb.stream_iterator(db, "key")
             |> Enum.take(10)

    assert [{"key", "val"}] ==
             ElixirRocksdb.stream_iterator(db, "key")
             |> Enum.take(1)

    assert [] ==
             ElixirRocksdb.stream_iterator(db, "r")
             |> Enum.take(2)

    assert [{"key", "val"}] ==
             ElixirRocksdb.stream_iterator(db)
             |> Enum.take(1)

    ElixirRocksdb.stream_delete_all(db)
    assert 0 == ElixirRocksdb.count(db)
  end

  test "stream_delete_all/1(2)", %{db: db} do
    ElixirRocksdb.put_batch(db, [{"key1", "val"}, {"key2", "val2"}])
    assert :ok == ElixirRocksdb.stream_delete_all(db)
    assert 0 == ElixirRocksdb.count(db)

    ElixirRocksdb.put_batch(db, [{"key", "val"}, {"qwe", "val2"}, {"key3", "val2"}])
    assert :ok == ElixirRocksdb.stream_delete_all(db, "key")
    assert 1 == ElixirRocksdb.count(db)

    ElixirRocksdb.stream_delete_all(db)
    assert 0 == ElixirRocksdb.count(db)

    batch = Enum.map(1..5, fn x -> {:erlang.integer_to_binary(x), "value#{x}"} end)

    prefix_batch =
      Enum.map(1..5, fn x -> {"/subs/" <> :erlang.integer_to_binary(x), "value#{x}"} end)

    list = [batch | prefix_batch] |> List.flatten()

    ElixirRocksdb.put_batch(db, list)
    ElixirRocksdb.stream_delete_all(db, "/subs")
    assert length(batch) == ElixirRocksdb.count(db)

    ElixirRocksdb.stream_delete_all(db)
    assert 0 == ElixirRocksdb.count(db)
  end

  test "stream_iterator/3 with offset", %{db: db, test_db: test} do
    ElixirRocksdb.stream_delete_all(db)
    ElixirRocksdb.stream_delete_all(test)

    list = [
      {"1", %{offset: ""}},
      {"2", %{offset: ""}}
    ]

    ElixirRocksdb.put_batch(test, list)
    assert 2 == ElixirRocksdb.count(test)

    list =
      Enum.map(1..5, fn x ->
        [{"/subs/1/#{ElixirRocksdb.to_hex(x)}", x}, {"/subs/2/#{ElixirRocksdb.to_hex(x)}", x}]
      end)
      |> List.flatten()

    ElixirRocksdb.put_batch(db, list)
    assert 10 == ElixirRocksdb.count(db)

    assert 8 == TestStream.get_subscribers(db, test, 8) |> length

    assert 2 == TestStream.get_subscribers(db, test, 8) |> length

    ElixirRocksdb.stream_delete_all(db)
    ElixirRocksdb.stream_delete_all(test)
    assert 0 == ElixirRocksdb.count(db)
  end
end

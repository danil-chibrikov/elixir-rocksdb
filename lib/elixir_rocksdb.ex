defmodule ElixirRocksdb do
  require Logger

  @doc """
    Open Rocksdb with the default column family.
  """
  def open(path, opts), do: :rocksdb.open(to_charlist(path), opts)

  @doc """
    Open Rocksdb with the specified column family.
  """
  def open(path, opts, cf_desc), do: :rocksdb.open(to_charlist(path), opts, cf_desc)

  @doc """
  Create a new column family.
  """
  def create_cf(db_ref, name, opts),
    do: :rocksdb.create_column_family(db_ref, to_charlist(name), opts)

  @doc """
    List column families.
  """
  def list_cf(db_name, opts \\ []) do
    :rocksdb.list_column_families(db_name, opts)
  end

  @doc """
    Retrieve a key/value pair in the default column family.
    Get only value. Value is term.
  """
  def get(db, k, default \\ nil) do
    case :rocksdb.get(db, k, []) do
      {:ok, <<131, _::binary>> = value} ->
        :erlang.binary_to_term(value)

      :not_found ->
        default

      {:error, _reason} = val ->
        process(val)
    end
  end

  @doc """
    Put a key/value pair into the default column family.
    Key and value only binary.
  """
  def put(db, k, v) do
    case check_record(k, v) do
      {key, value} ->
        :rocksdb.put(db, key, value, [])
        |> process

      _ ->
        :ok
    end
  end

  @doc """
    Put a key/value pair into the default column family.
    Key and value only binary.
  """
  def put(db, cf_ref, k, v) do
    case check_record(k, v) do
      {key, value} ->
        :rocksdb.put(db, cf_ref, key, value, [])
        |> process

      _ ->
        :ok
    end
  end

  # Put helpers

  defp process(:ok), do: :ok

  defp process({:error, reason}) do
    Logger.error("#{__MODULE__}: failed processing, reason: #{inspect(reason)}")
    :error
  end

  @doc """
    Put a key/value pair batch into the default column family.
    Key and value only binary.
  """
  def put_batch(db, [_ | _] = pairs) do
    items =
      pairs
      |> Enum.map(fn
        {k, v} ->
          check_record({:put, k, v})

        _ ->
          nil
      end)

    process_batch(db, items)
  end

  def put_batch(_, []), do: :ok

  @doc """
    Delete a key batch into the default column family.
    Key only binary.
  """
  def del_batch(db, [_ | _] = pairs) do
    items =
      pairs
      |> Enum.map(fn k ->
        check_record({:del, k})
      end)

    process_batch(db, items)
  end

  def del_batch(_, []), do: :ok

  @doc """
    Put/delete a (key/value)/key batch into the default column family.
    Key and value only binary.
  """
  def batch(db, [_ | _] = pairs) do
    items =
      pairs
      |> Enum.map(fn elem ->
        check_record(elem)
      end)

    process_batch(db, items)
  end

  def batch(_, []), do: :ok

  def batch(db, cf_ref, [_ | _] = pairs) do
    items =
      pairs
      |> Enum.map(fn elem ->
        check_record(elem)
      end)

    process_batch_cf(db, cf_ref, items)
  end

  def batch(_, _, []), do: :ok

  # Batch helpers

  defp check_record(k, v) do
    case normalize_key_value(k, v) do
      {:error, msg} ->
        Logger.error("#{__MODULE__}: #{inspect(msg)}")
        nil

      {key, value} ->
        {key, value}
    end
  end

  defp check_record({m, k, v}) do
    case normalize_key_value(k, v) do
      {:error, msg} ->
        Logger.error("#{__MODULE__}: #{inspect(msg)}")
        nil

      {key, value} ->
        {m, key, value}
    end
  end

  defp check_record({m, k}) when is_binary(k), do: {m, k}

  defp check_record({_, k}) do
    Logger.info("#{__MODULE__}: key: #{inspect(k)} isn't binary")
    nil
  end

  defp check_record(_), do: nil

  defp normalize_key_value(k, v) when is_reference(k),
    do: normalize_key_value(:erlang.term_to_binary(k), v)

  defp normalize_key_value(k, <<131, _::binary>> = v) when is_binary(k), do: {k, v}
  defp normalize_key_value(k, v) when is_binary(k), do: {k, :erlang.term_to_binary(v)}
  defp normalize_key_value(k, _), do: {:error, "#{__MODULE__}: key: #{inspect(k)} isn't binary"}

  defp process_batch(db, [_ | _] = items) do
    {:ok, batch} = :rocksdb.batch()

    Enum.each(items, fn
      {:put, k, v} ->
        :rocksdb.batch_put(batch, k, v)

      {:del, k} ->
        :rocksdb.batch_delete(batch, k)

      _ ->
        :ok
    end)

    case :rocksdb.write_batch(db, batch, sync: true) do
      :ok ->
        :rocksdb.release_batch(batch)

      {:error, reason} ->
        Logger.error("#{__MODULE__}: batch: #{inspect(reason)}")
        :error
    end
  end

  defp process_batch(_, []), do: :ok

  defp process_batch_cf(db, cf_ref, [_ | _] = items) do
    {:ok, batch} = :rocksdb.batch()

    Enum.each(items, fn
      {:put, k, v} ->
        :rocksdb.batch_put(batch, cf_ref, k, v)

      {:del, k} ->
        :rocksdb.batch_delete(batch, cf_ref, k)

      _ ->
        :ok
    end)

    case :rocksdb.write_batch(db, batch, sync: true) do
      :ok ->
        :rocksdb.release_batch(batch)

      {:error, reason} ->
        Logger.error("#{__MODULE__}: batch: #{inspect(reason)}")
        :error
    end
  end

  defp process_batch_cf(_, _, []), do: :ok

  @doc """
    Return a iterator over the contents of the database.
  """
  def iterator(db, opts) do
    case :rocksdb.iterator(db, opts) do
      {:ok, iter} ->
        iter

      {:error, _} ->
        :end_of_table
    end
  end

  @doc """
    Return a iterator over the contents of the database from column family.
  """
  def iterator(db, cf_ref, opts) do
    case :rocksdb.iterator(db, cf_ref, opts) do
      {:ok, iter} ->
        iter

      {:error, _} ->
        :end_of_table
    end
  end

  @doc """
    Move to the specified place.
    Return key/value pair, when value is term.
  """
  def iterator_move(iter, opts) do
    case :rocksdb.iterator_move(iter, opts) do
      {:ok, k, v} ->
        {k, :erlang.binary_to_term(v)}

      {:error, _} ->
        :end_of_table
    end
  end

  @doc """
    Return stream with all key/value pair, when value is term
      from default column family.
  """
  def stream_iterator(db) do
    Stream.resource(
      iterate_start(db, :iterate),
      &iterate_step/1,
      &iterate_end/1
    )
  end

  @doc """
    Return stream with all key/value pair, when value is term
      from specified column family.
  """
  def stream_iterator_cf(db, cf_ref) do
    Stream.resource(
      iterate_start(db, cf_ref, :iterate),
      &iterate_step/1,
      &iterate_end/1
    )
  end

  @doc """
    Delete all key/value pair from default column family.
  """
  def stream_delete_all(db) do
    batch =
      Stream.resource(
        iterate_start(db, :delete),
        &iterate_step/1,
        &iterate_end/1
      )
      |> Enum.to_list()

    process_batch(db, batch)
  end

  @doc """
    Delete all key/value pair from specified column family.
  """
  def stream_delete_cf_all(db, cf_ref) do
    batch =
      Stream.resource(
        iterate_start(db, cf_ref, :delete),
        &iterate_step/1,
        &iterate_end/1
      )
      |> Enum.to_list()

    process_batch_cf(db, cf_ref, batch)
  end

  # Stream iterator helpers functions

  defp iterate_start(db, action) do
    fn ->
      case :rocksdb.iterator(db, []) do
        {:ok, iter} ->
          {:first, iter, action}

        {:error, _reason} ->
          :end_of_table
      end
    end
  end

  defp iterate_start(db, cf_ref, action) do
    fn ->
      case :rocksdb.iterator(db, cf_ref, []) do
        {:ok, iter} ->
          {:first, iter, action}

        {:error, _reason} ->
          :end_of_table
      end
    end
  end

  defp iterate_step({move, iter, :iterate = action}) do
    case :rocksdb.iterator_move(iter, move) do
      {:ok, k, v} ->
        return_val = {k, :erlang.binary_to_term(v)}
        {[return_val], {:next, iter, action}}

      {:error, _} ->
        {:halt, {:end_of_table, iter}}
    end
  end

  defp iterate_step({move, iter, :delete = action}) do
    case :rocksdb.iterator_move(iter, move) do
      {:ok, k, _} ->
        return_val = {:del, k}
        {[return_val], {:next, iter, action}}

      {:error, _} ->
        {:halt, {:end_of_table, iter}}
    end
  end

  defp iterate_step(:end_of_table), do: nil

  defp iterate_end(nil), do: nil
  defp iterate_end({:next, iter, _}), do: iterator_close(iter)
  defp iterate_end({_, iter}), do: iterator_close(iter)
  defp iterate_end({_, iter, _}), do: iterator_close(iter)

  @doc """
    Return stream with all key/value pair by prefix, when value is term
     from default column family.
  """
  def stream_iterator(db, prefix) do
    Stream.resource(
      iterate_start_by_prefix(db, prefix, :iterate),
      &iterate_step_by_prefix/1,
      &iterate_end_by_prefix/1
    )
  end

  @doc """
    Return stream with all key/value pair by prefix, when value is term
     from specified column family.
  """
  def stream_iterator_cf(db, cf_ref, prefix) do
    Stream.resource(
      iterate_start_by_prefix_cf(db, cf_ref, prefix, :iterate),
      &iterate_step_by_prefix/1,
      &iterate_end_by_prefix/1
    )
  end

  @doc """
    Return stream with all key/value pair by prefix with offset,
      when value is term from default column family.
  """
  def stream_iterator(db, prefix, offset) do
    Stream.resource(
      iterate_start_by_prefix(db, prefix, offset, :iterate),
      &iterate_step_by_prefix/1,
      &iterate_end_by_prefix/1
    )
  end

  @doc """
    Return stream with all key/value pair by prefix with offset,
      when value is term from specified column family.
  """
  def stream_iterator_cf(db, cf_ref, prefix, offset) do
    Stream.resource(
      iterate_start_by_prefix_cf(db, cf_ref, prefix, offset, :iterate),
      &iterate_step_by_prefix/1,
      &iterate_end_by_prefix/1
    )
  end

  @doc """
    Delete all key/value pair by prefix from default column family.
  """
  def stream_delete_all(db, prefix) do
    batch =
      Stream.resource(
        iterate_start_by_prefix(db, prefix, :delete),
        &iterate_step_by_prefix/1,
        &iterate_end_by_prefix/1
      )
      |> Enum.reject(&is_nil/1)

    process_batch(db, batch)
  end

  def stream_delete_all(db, cf_ref, prefix) do
    batch =
      Stream.resource(
        iterate_start_by_prefix_cf(db, cf_ref, prefix, :delete),
        &iterate_step_by_prefix/1,
        &iterate_end_by_prefix/1
      )
      |> Enum.reject(&is_nil/1)

    process_batch_cf(db, cf_ref, batch)
  end

  # Stream iterator helpers functions

  defp iterate_start_by_prefix(db, prefix, action) do
    fn ->
      case :rocksdb.iterator(db, prefix_same_as_start: true) do
        {:ok, iter} ->
          {:first, prefix, iter, action}

        {:error, _reason} ->
          :end_of_table
      end
    end
  end

  defp iterate_start_by_prefix(db, prefix, offset, action) do
    fn ->
      case :rocksdb.iterator(db, prefix_same_as_start: true) do
        {:ok, iter} ->
          {:first, prefix, iter, offset, action}

        {:error, _reason} ->
          :end_of_table
      end
    end
  end

  defp iterate_start_by_prefix_cf(db, cf_ref, prefix, action) do
    fn ->
      case :rocksdb.iterator(db, cf_ref, prefix_same_as_start: true) do
        {:ok, iter} ->
          {:first, prefix, iter, action}

        {:error, _reason} ->
          :end_of_table
      end
    end
  end

  defp iterate_start_by_prefix_cf(db, cf_ref, prefix, offset, action) do
    fn ->
      case :rocksdb.iterator(db, cf_ref, prefix_same_as_start: true) do
        {:ok, iter} ->
          {:first, prefix, iter, offset, action}

        {:error, _reason} ->
          :end_of_table
      end
    end
  end

  defp iterate_step_by_prefix({move, prefix, iter, :iterate = action}) do
    upd_move = check_move(move, prefix)

    case :rocksdb.iterator_move(iter, upd_move) do
      {:ok, k, v} ->
        String.starts_with?(k, prefix)
        |> start_with_prefix(k, :erlang.binary_to_term(v), prefix, iter, action)

      {:error, _} ->
        {:halt, {prefix, :end_of_table, iter}}
    end
  end

  defp iterate_step_by_prefix({move, prefix, iter, offset, :iterate = action}) do
    upd_move = check_move(move, prefix)

    case :rocksdb.iterator_move(iter, upd_move) do
      {:ok, k, v} ->
        String.starts_with?(k, prefix)
        |> start_with_prefix(k, :erlang.binary_to_term(v), prefix, iter, offset, action)

      {:error, _} ->
        {:halt, {prefix, :end_of_table, iter}}
    end
  end

  defp iterate_step_by_prefix({move, prefix, iter, :delete = action}) do
    upd_move = check_move(move, prefix)

    case :rocksdb.iterator_move(iter, upd_move) do
      {:ok, k, _} ->
        String.starts_with?(k, prefix)
        |> delete_condition(k, prefix, iter, action)

      {:error, _} ->
        {:halt, {prefix, :end_of_table, iter}}
    end
  end

  defp iterate_step_by_prefix(:end_of_table), do: nil

  defp check_move(:first, prefix), do: {:seek, prefix}
  defp check_move(:next, _), do: :next

  defp start_with_prefix(true, k, v, prefix, iter, action) do
    {[{k, v}], {:next, prefix, iter, action}}
  end

  defp start_with_prefix(false, _, _, prefix, iter, _) do
    {:halt, {prefix, :end_of_table, iter}}
  end

  defp start_with_prefix(true, k, v, prefix, iter, offset, action) do
    cond do
      k > offset ->
        {[{k, v}], {:next, prefix, iter, offset, action}}

      true ->
        {[], {:next, prefix, iter, offset, action}}
    end
  end

  defp start_with_prefix(false, _, _, prefix, iter, _, _) do
    {:halt, {prefix, :end_of_table, iter}}
  end

  defp delete_condition(true, k, prefix, iter, action) do
    {[{:del, k}], {:next, prefix, iter, action}}
  end

  defp delete_condition(false, _, prefix, iter, action) do
    {[nil], {:next, prefix, iter, action}}
  end

  defp iterate_end_by_prefix(nil), do: nil
  defp iterate_end_by_prefix({:next, _, iter, _}), do: iterator_close(iter)
  defp iterate_end_by_prefix({:next, _, iter, _, _}), do: iterator_close(iter)
  defp iterate_end_by_prefix({_, _, iter}), do: iterator_close(iter)

  @doc """
    Close a iterator.
  """
  def iterator_close(iter), do: :rocksdb.iterator_close(iter)

  @doc """
    Delete a key/value pair by key from the default column family.
  """
  def delete(db, k), do: :rocksdb.delete(db, k, [])

  @doc """
    Get count all records from the default column family.
  """
  def count(db), do: Enum.count(stream_iterator(db))

  @doc """
    Get count all records from the specified column family.
  """
  def count(db, cf_ref), do: Enum.count(stream_iterator_cf(db, cf_ref))

  @doc """
    Close Rocksdb database.
  """
  def close(db), do: :rocksdb.close(db)

  @doc """
    Create item to hex for further sorting capability from Rocksdb.
  """
  def to_hex(item) when is_integer(item) do
    <<y1::4, y2::4, y3::4, y4::4, y5::4, y6::4, y7::4, y8::4, y9::4, y10::4, y11::4, y12::4,
      y13::4, y14::4, y15::4, y16::4>> = <<item::size(64)>>

    <<byte_to_hex(y1)::binary(), byte_to_hex(y2)::binary(), byte_to_hex(y3)::binary(),
      byte_to_hex(y4)::binary(), byte_to_hex(y5)::binary(), byte_to_hex(y6)::binary(),
      byte_to_hex(y7)::binary(), byte_to_hex(y8)::binary(), byte_to_hex(y9)::binary(),
      byte_to_hex(y10)::binary(), byte_to_hex(y11)::binary(), byte_to_hex(y12)::binary(),
      byte_to_hex(y13)::binary(), byte_to_hex(y14)::binary(), byte_to_hex(y15)::binary(),
      byte_to_hex(y16)::binary()>>
  end

  defp byte_to_hex(0), do: "0"
  defp byte_to_hex(1), do: "1"
  defp byte_to_hex(2), do: "2"
  defp byte_to_hex(3), do: "3"
  defp byte_to_hex(4), do: "4"
  defp byte_to_hex(5), do: "5"
  defp byte_to_hex(6), do: "6"
  defp byte_to_hex(7), do: "7"
  defp byte_to_hex(8), do: "8"
  defp byte_to_hex(9), do: "9"
  defp byte_to_hex(10), do: "A"
  defp byte_to_hex(11), do: "B"
  defp byte_to_hex(12), do: "C"
  defp byte_to_hex(13), do: "D"
  defp byte_to_hex(14), do: "E"
  defp byte_to_hex(15), do: "F"
end

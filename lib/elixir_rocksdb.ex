defmodule ElixirRocksdb do
  require Logger

  @type cache_type() :: :lru | :clock
  @type compression_type() :: :snappy | :zlib | :bzip2 | :lz4 | :lz4h | :zstd | :none
  @type compaction_style() :: :level | :universal | :fifo | :none
  @type compaction_pri() ::
          :compensated_size | :oldest_largest_seq_first | :oldest_smallest_seq_first
  @type access_hint() :: :normal | :sequential | :willneed | :none
  @type wal_recovery_mode() ::
          :tolerate_corrupted_tail_records
          | :absolute_consistency
          | :point_in_time_recovery
          | :skip_any_corrupted_records
  @type env_type() :: :default | :memenv

  @opaque env() :: env_type() | :rocksdb.env_handle()
  @opaque db_handle() :: :rocksdb.db_handle()
  @opaque cf_handle() :: :rocksdb.cf_handle()
  @opaque itr_handle() :: :rocksdb.itr_handle()
  @opaque batch_handle() :: :rocksdb.batch_handle()
  @opaque cache_handle() :: :rocksdb.cache_handle()
  @opaque rate_limiter_handle() :: :rocksdb.rate_limiter_handle()
  @opaque write_buffer_manager() :: :rocksdb.write_buffer_manager()
  @opaque snapshot_handle() :: :rocksdb.snapshot_handle()

  @type stream_handle() :: db_handle() | {db_handle(), cf_handle()}

  @type block_based_table_options() :: [
          {:no_block_cache, boolean()}
          | {:block_size, pos_integer()}
          | {:block_cache, cache_handle()}
          | {:block_cache_size, pos_integer()}
          | {:bloom_filter_policy, pos_integer()}
          | {:format_version, 0 | 1 | 2}
          | {:cache_index_and_filter_blocks, boolean()}
        ]

  @type merge_operator() ::
          :erlang_merge_operator
          | :bitset_merge_operator
          | {:bitset_merge_operator, non_neg_integer()}
          | :counter_merge_operator

  @type cf_options() :: [
          {:block_cache_size_mb_for_point_lookup, non_neg_integer()}
          | {:memtable_memory_budget, pos_integer()}
          | {:write_buffer_size, pos_integer()}
          | {:max_write_buffer_number, pos_integer()}
          | {:min_write_buffer_number_to_merge, pos_integer()}
          | {:compression, compression_type()}
          | {:num_levels, pos_integer()}
          | {:level0_file_num_compaction_trigger, integer()}
          | {:level0_slowdown_writes_trigger, integer()}
          | {:level0_stop_writes_trigger, integer()}
          | {:max_mem_compaction_level, pos_integer()}
          | {:target_file_size_base, pos_integer()}
          | {:target_file_size_multiplier, pos_integer()}
          | {:max_bytes_for_level_base, pos_integer()}
          | {:max_bytes_for_level_multiplier, pos_integer()}
          | {:max_compaction_bytes, pos_integer()}
          | {:soft_rate_limit, float()}
          | {:hard_rate_limit, float()}
          | {:arena_block_size, integer()}
          | {:disable_auto_compactions, boolean()}
          | {:purge_redundant_kvs_while_flush, boolean()}
          | {:compaction_style, compaction_style()}
          | {:compaction_pri, compaction_pri()}
          | {:filter_deletes, boolean()}
          | {:max_sequential_skip_in_iterations, pos_integer()}
          | {:inplace_update_support, boolean()}
          | {:inplace_update_num_locks, pos_integer()}
          | {:table_factory_block_cache_size, pos_integer()}
          | {:in_memory_mode, boolean()}
          | {:block_based_table_options, block_based_table_options()}
          | {:level_compaction_dynamic_level_bytes, boolean()}
          | {:optimize_filters_for_hits, boolean()}
          | {:prefix_transform,
             {:fixed_prefix_transform, integer()}
             | {:capped_prefix_transform, integer()}}
          | {:merge_operator, merge_operator()}
        ]

  @type db_options() :: [
          {:env, env()}
          | {:total_threads, pos_integer()}
          | {:create_if_missing, boolean()}
          | {:create_missing_column_families, boolean()}
          | {:error_if_exists, boolean()}
          | {:paranoid_checks, boolean()}
          | {:max_open_files, integer()}
          | {:max_total_wal_size, non_neg_integer()}
          | {:use_fsync, boolean()}
          | {:delete_obsolete_files_period_micros, pos_integer()}
          | {:max_background_jobs, pos_integer()}
          | {:max_background_compactions, pos_integer()}
          | {:max_background_flushes, pos_integer()}
          | {:max_log_file_size, non_neg_integer()}
          | {:log_file_time_to_roll, non_neg_integer()}
          | {:keep_log_file_num, pos_integer()}
          | {:max_manifest_file_size, pos_integer()}
          | {:table_cache_numshardbits, pos_integer()}
          | {:wal_ttl_seconds, non_neg_integer()}
          | {:manual_wal_flush, boolean()}
          | {:wal_size_limit_mb, non_neg_integer()}
          | {:manifest_preallocation_size, pos_integer()}
          | {:allow_mmap_reads, boolean()}
          | {:allow_mmap_writes, boolean()}
          | {:is_fd_close_on_exec, boolean()}
          | {:skip_log_error_on_recovery, boolean()}
          | {:stats_dump_period_sec, non_neg_integer()}
          | {:advise_random_on_open, boolean()}
          | {:access_hint, access_hint()}
          | {:compaction_readahead_size, non_neg_integer()}
          | {:new_table_reader_for_compaction_inputs, boolean()}
          | {:use_adaptive_mutex, boolean()}
          | {:bytes_per_sync, non_neg_integer()}
          | {:skip_stats_update_on_db_open, boolean()}
          | {:wal_recovery_mode, wal_recovery_mode()}
          | {:allow_concurrent_memtable_write, boolean()}
          | {:enable_write_thread_adaptive_yield, boolean()}
          | {:db_write_buffer_size, non_neg_integer()}
          | {:in_memory, boolean()}
          | {:rate_limiter, rate_limiter_handle()}
          | {:write_buffer_manager, write_buffer_manager()}
          | {:max_subcompactions, non_neg_integer()}
        ]

  @type options() :: db_options() | cf_options()

  @type read_options() :: [
          {:verify_checksums, boolean()}
          | {:fill_cache, boolean()}
          | {:iterate_upper_bound, binary()}
          | {:iterate_lower_bound, binary()}
          | {:tailing, boolean()}
          | {:total_order_seek, boolean()}
          | {:prefix_same_as_start, boolean()}
          | {:snapshot, snapshot_handle()}
        ]

  @type write_options() :: [
          {:sync, boolean()}
          | {:disable_wal, boolean()}
          | {:ignore_missing_column_families, boolean()}
          | {:no_slowdown, boolean()}
          | {:low_pri, boolean()}
        ]

  @type write_actions() :: [
          {:put, binary(), any()}
          | {:put, cf_handle(), binary(), any()}
          | {:delete, binary()}
          | {:delete, cf_handle(), any()}
          | {:single_delete, binary()}
          | {:single_delete, cf_handle(), binary()}
          | :clear
        ]

  @type iterator_action() ::
          :first
          | :last
          | :next
          | :prev
          | binary()
          | {:seek, binary()}
          | {:seek_for_prev, binary()}

  @doc """
    Open Rocksdb with the default column family.
  """
  @spec open(binary(), options()) ::
          {:ok, db_handle()} | {:error, any()}
  def open(path, db_opts), do: :rocksdb.open(to_charlist(path), db_opts)

  @doc """
    Open Rocksdb with column families.
  """
  @spec open(binary(), options(), list({atom(), cf_options()})) ::
          {:ok, db_handle(), map()} | {:error, any()}
  def open(path, db_opts, cf_descriptors) do
    cf_descs = create_cf_descs(cf_descriptors)

    case :rocksdb.open(to_charlist(path), db_opts, cf_descs) do
      {:ok, db_handle, cf_ref_list} ->
        {:ok, db_handle, zip_cf_ref([{:default, []} | cf_descriptors], cf_ref_list)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Create a new column family.
  """
  @spec create_cf(db_handle(), atom(), cf_options()) ::
          {:ok, %{key: reference()}} | {:error, any()}
  def create_cf(db_handle, name, cf_opts) do
    case :rocksdb.create_column_family(db_handle, to_charlist(name), cf_opts) do
      {:ok, cf_ref} ->
        {:ok, zip_cf_ref([{name, []}], [cf_ref])}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Helpers column families

  defp create_cf_descs(list) do
    [
      {'default', []}
      | Enum.map(list, fn {cf, opts} -> {to_charlist(cf), opts} end)
    ]
  end

  defp zip_cf_ref(cf_list, cf_ref_list) do
    Enum.map(cf_list, fn {name, _} -> name end)
    |> Enum.zip(cf_ref_list)
    |> Enum.into(%{})
  end

  @doc """
    List column families.
  """
  @spec list_cf(binary(), db_options()) ::
          {:ok, list(binary())} | {:error, any()}
  def list_cf(name, db_opts), do: :rocksdb.list_column_families(to_charlist(name), db_opts)

  @doc """
    Is the database empty.
  """
  @spec is_empty?(db_handle()) ::
          true | false
  def is_empty?(db_handle), do: :rocksdb.is_empty(db_handle)

  # @spec get(stream_handle :: stream_handle(), key :: binary(), read_opts :: read_options()) ::
  #         {:ok, any()} | {:ok, nil} | {:error, any()}
  # def get(stream_handle, key, read_opts) do
  #   case stream_handle do
  #     {db_handle, cf_handle} ->
  #       :rocksdb.get(db_handle, cf_handle, key, read_opts)
  #       |> get_condition()

  #     db_handle ->
  #       :rocksdb.get(db_handle, key, read_opts)
  #       |> get_condition()
  #   end
  # end

  @doc """
    Retrieve a key/value pair in the default column family.
    Get only value. Value is term.
  """
  @spec get(db_handle(), binary(), read_options()) ::
          {:ok, any()} | {:ok, nil} | {:error, any()}
  def get(db_handle, key, read_opts) do
    :rocksdb.get(db_handle, key, read_opts)
    |> get_condition()
  end

  @doc """
    Retrieve a key/value pair in the specified column family.
    Get only value. Value is term.
  """
  @spec get(db_handle(), cf_handle(), binary(), read_options()) ::
          {:ok, any()} | {:ok, nil} | {:error, any()}
  def get(db_handle, cf_handle, key, read_opts) do
    :rocksdb.get(db_handle, cf_handle, key, read_opts)
    |> get_condition()
  end

  # Get helpers

  defp get_condition({:ok, <<131, _::binary>> = value}), do: {:ok, :erlang.binary_to_term(value)}
  defp get_condition(:not_found), do: {:ok, nil}
  defp get_condition({:error, reason}), do: {:error, reason}

  @doc """
    Put a key/value pair into the default column family.
    Key and value only binary.
  """
  @spec put(db_handle(), binary(), any(), write_options()) ::
          :ok | {:error, any()}
  def put(db_handle, key, value, write_opts) do
    case check_record(key, value) do
      {:error, reason} ->
        {:error, reason}

      {key, value} ->
        :rocksdb.put(db_handle, key, value, write_opts)
    end
  end

  @doc """
    Put a key/value pair into the specified column family.
    Key and value only binary.
  """
  @spec put(db_handle(), cf_handle(), binary(), any(), write_options()) ::
          :ok | {:error, any()}
  def put(db_handle, cf_handle, key, value, write_opts) do
    case check_record(key, value) do
      {:error, reason} ->
        {:error, reason}

      {key, value} ->
        :rocksdb.put(db_handle, cf_handle, key, value, write_opts)
    end
  end

  @doc """
    Put a key/value pair batch into the default/specified column family.
    Key and value only binary.
  """
  @spec put_batch(db_handle(), list({binary(), any()} | {cf_handle(), binary(), any()})) ::
          :ok | {:error, any()}
  def put_batch(db_handle, [_ | _] = pairs) do
    items =
      Stream.map(pairs, fn
        {key, value} ->
          check_record({:put, key, value})

        {cf_handle, key, value} ->
          check_record({:put, cf_handle, key, value})

        _ ->
          nil
      end)
      |> Stream.reject(&is_nil/1)
      |> Enum.map(&batch_condition/1)

    process_batch(db_handle, items)
  end

  def put_batch(_db_handle, []), do: :ok

  @doc """
    Delete a key batch into the default/specified column family.
    Key only binary.
  """
  @spec delete_batch(db_handle(), list({binary(), any()} | {cf_handle(), binary(), any()})) ::
          :ok | {:error, any()}
  def delete_batch(db_handle, [_ | _] = pairs) do
    items =
      Stream.map(pairs, fn
        {cf_handle, key} ->
          check_record({:delete, cf_handle, key})

        key ->
          check_record({:delete, key})
      end)
      |> Stream.reject(&is_nil/1)
      |> Enum.map(&batch_condition/1)

    process_batch(db_handle, items)
  end

  def delete_batch(_db_handle, []), do: :ok

  @doc """
    Put/delete a (key/value)/key batch into the default/specified column family.
    Key and value only binary.
  """
  @spec batch(
          db_handle(),
          list({atom(), binary(), any()} | {atom(), cf_handle(), binary(), any()})
        ) ::
          :ok | {:error, any()}
  def batch(db_handle, [_ | _] = pairs) do
    items =
      Stream.map(pairs, fn
        {_action, _key, _value} = elem ->
          check_record(elem)

        {_action, _cf_handle, _key, _value} = elem ->
          check_record(elem)

        _ ->
          nil
      end)
      |> Stream.reject(&is_nil/1)
      |> Enum.map(&batch_condition/1)

    process_batch(db_handle, items)
  end

  # Batch helpers

  defp batch_condition({:error, _reason}), do: nil
  defp batch_condition({:put, _cf_handle, _key, _value} = tuple), do: tuple
  defp batch_condition({:delete, _cf_handle, _key} = tuple), do: tuple
  defp batch_condition({:put, _key, _value} = tuple), do: tuple
  defp batch_condition({:delete, _key} = tuple), do: tuple
  defp batch_condition({:single_delete, _key} = tuple), do: tuple
  defp batch_condition({:single_delete, _cf_handle, _key} = tuple), do: tuple
  defp batch_condition(:clear), do: :clear
  defp batch_condition(_), do: nil

  defp check_record(k, v) do
    case normalize_key_value(k, v) do
      {:error, :key_is_not_binary} ->
        {:error, :key_is_not_binary}

      {key, value} ->
        {key, value}
    end
  end

  defp check_record({action, cf_handle, k}) when is_binary(k) and is_reference(cf_handle),
    do: {action, cf_handle, k}

  defp check_record({action, k, v}) when not is_reference(k) do
    case normalize_key_value(k, v) do
      {:error, :key_is_not_binary} ->
        {:error, :key_is_not_binary}

      {key, value} ->
        {action, key, value}
    end
  end

  defp check_record({_action, _cf_handle, _k}), do: {:error, :key_is_not_binary}
  defp check_record({action, k}) when is_binary(k), do: {action, k}
  defp check_record({_action, _k}), do: {:error, :key_is_not_binary}

  defp check_record({action, cf_handle, k, v}) do
    case normalize_key_value(k, v) do
      {:error, :key_is_not_binary} ->
        {:error, :key_is_not_binary}

      {key, value} ->
        {action, cf_handle, key, value}
    end
  end

  defp normalize_key_value(k, <<131, _::binary>> = v) when is_binary(k), do: {k, v}
  defp normalize_key_value(k, v) when is_binary(k), do: {k, :erlang.term_to_binary(v)}
  defp normalize_key_value(_k, _v), do: {:error, :key_is_not_binary}

  defp process_batch(db_handle, [_ | _] = items) do
    {:ok, batch} = :rocksdb.batch()

    Enum.each(items, fn
      {:put, key, value} ->
        :rocksdb.batch_put(batch, key, value)

      {:put, cf_handle, key, value} ->
        :rocksdb.batch_put(batch, cf_handle, key, value)

      {:delete, key} ->
        :rocksdb.batch_delete(batch, key)

      {:delete, cf_handle, key} ->
        :rocksdb.batch_delete(batch, cf_handle, key)

      _ ->
        :ok
    end)

    case :rocksdb.write_batch(db_handle, batch, sync: true) do
      :ok ->
        release_batch_condition(db_handle, batch)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp process_batch(_db_handle, []), do: :ok

  defp release_batch_condition(db_handle, batch) do
    case :rocksdb.write_batch(db_handle, batch, sync: true) |> IO.inspect() do
      :ok ->
        :rocksdb.release_batch(batch)

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
    Return a iterator over the contents of the database.
  """
  @spec iterator(db_handle(), read_options()) ::
          itr_handle() | :end_of_table
  def iterator(db_handle, read_opts) do
    case :rocksdb.iterator(db_handle, read_opts) do
      {:ok, itr} ->
        itr

      {:error, _} ->
        :end_of_table
    end
  end

  @doc """
    Return a iterator over the contents of the database
      from specified column family.
  """
  @spec iterator(db_handle(), cf_handle(), read_options()) ::
          itr_handle() | :end_of_table
  def iterator(db_handle, cf_handle, read_opts) do
    case :rocksdb.iterator(db_handle, cf_handle, read_opts) do
      {:ok, itr} ->
        itr

      {:error, _} ->
        :end_of_table
    end
  end

  @doc """
    Move to the specified place.
    Return key/value pair, when value is term.
  """
  @spec iterator_move(itr_handle(), iterator_action()) ::
          {binary(), any()} | :end_of_table
  def iterator_move(itr_handle, itr_action) do
    case :rocksdb.iterator_move(itr_handle, itr_action) do
      {:ok, key, value} ->
        {key, :erlang.binary_to_term(value)}

      {:error, _} ->
        :end_of_table
    end
  end

  @doc """
    Return stream with all key/value pair, when value is term
      from default/specified column family.
  """
  @spec stream_iterator(stream_handle()) ::
          Stream.t()
  def stream_iterator(stream_handle) do
    Stream.resource(
      iterate_start(stream_handle, :iterate),
      &iterate_step/1,
      &iterate_end/1
    )
  end

  @doc """
    Delete all key/value pair from default/specified column family.
  """
  @spec stream_delete_all(stream_handle()) ::
          :ok | {:error, any()}
  def stream_delete_all(stream_handle) do
    batch =
      Stream.resource(
        iterate_start(stream_handle, :delete),
        &iterate_step/1,
        &iterate_end/1
      )
      |> Enum.to_list()

    handle = parse_stream_handle(stream_handle)
    process_batch(handle, batch)
  end

  # Stream iterator helpers functions

  defp iterate_start({db_handle, cf_handle}, action) do
    fn ->
      case :rocksdb.iterator(db_handle, cf_handle, []) do
        {:ok, itr} ->
          {:first, itr, cf_handle, action}

        {:error, _reason} ->
          :end_of_table
      end
    end
  end

  defp iterate_start(db_handle, action) do
    fn ->
      case :rocksdb.iterator(db_handle, []) do
        {:ok, itr} ->
          {:first, itr, action}

        {:error, _reason} ->
          :end_of_table
      end
    end
  end

  defp iterate_step({move, itr, _cf_handle, :iterate = action}) do
    case :rocksdb.iterator_move(itr, move) do
      {:ok, key, value} ->
        return_val = {key, :erlang.binary_to_term(value)}
        {[return_val], {:next, itr, action}}

      {:error, _} ->
        {:halt, {:end_of_table, itr}}
    end
  end

  defp iterate_step({move, itr, :iterate = action}) do
    case :rocksdb.iterator_move(itr, move) do
      {:ok, key, value} ->
        return_val = {key, :erlang.binary_to_term(value)}
        {[return_val], {:next, itr, action}}

      {:error, _} ->
        {:halt, {:end_of_table, itr}}
    end
  end

  defp iterate_step({move, itr, cf_handle, :delete = action}) do
    case :rocksdb.iterator_move(itr, move) do
      {:ok, key, _} ->
        return_val = {:delete, cf_handle, key}
        {[return_val], {:next, itr, action}}

      {:error, _} ->
        {:halt, {:end_of_table, itr}}
    end
  end

  defp iterate_step({move, itr, :delete = action}) do
    case :rocksdb.iterator_move(itr, move) do
      {:ok, key, _} ->
        return_val = {:delete, key}
        {[return_val], {:next, itr, action}}

      {:error, _} ->
        {:halt, {:end_of_table, itr}}
    end
  end

  defp iterate_step(:end_of_table), do: nil

  defp iterate_end(nil), do: nil
  defp iterate_end({:next, itr, _}), do: iterator_close(itr)
  defp iterate_end({_, itr}), do: iterator_close(itr)
  defp iterate_end({_, itr, _}), do: iterator_close(itr)

  @doc """
    Return stream with all key/value pair by prefix, when value is term
     from default/specified column family.
  """
  @spec stream_iterator(stream_handle(), binary()) ::
          Stream.t()
  def stream_iterator(stream_handle, prefix) do
    Stream.resource(
      iterate_start_by_prefix(stream_handle, prefix, :iterate),
      &iterate_step_by_prefix/1,
      &iterate_end_by_prefix/1
    )
  end

  @doc """
    Return stream with all key/value pair by prefix with offset,
      when value is term from default/specified column family.
    Offset is the last key in the previous iteration.
  """
  @spec stream_iterator(stream_handle(), binary(), binary()) ::
          Stream.t()
  def stream_iterator(stream_handle, prefix, offset) do
    Stream.resource(
      iterate_start_by_prefix(stream_handle, prefix, offset, :iterate),
      &iterate_step_by_prefix/1,
      &iterate_end_by_prefix/1
    )
  end

  @doc """
    Delete all key/value pair by prefix from default column family.
  """
  @spec stream_delete_all(stream_handle(), binary()) ::
          :ok | {:error, any()}
  def stream_delete_all(stream_handle, prefix) do
    batch =
      Stream.resource(
        iterate_start_by_prefix(stream_handle, prefix, :delete),
        &iterate_step_by_prefix/1,
        &iterate_end_by_prefix/1
      )
      |> Enum.reject(&is_nil/1)

    handle = parse_stream_handle(stream_handle)
    process_batch(handle, batch)
  end

  defp parse_stream_handle({db_handle, _cf_handle}), do: db_handle
  defp parse_stream_handle(db_handle), do: db_handle

  # Stream iterator helpers functions

  defp iterate_start_by_prefix({db_handle, cf_handle}, prefix, action) do
    fn ->
      case :rocksdb.iterator(db_handle, cf_handle, prefix_same_as_start: true) do
        {:ok, itr} ->
          {:first, prefix, itr, {:cf, cf_handle}, action}

        {:error, _reason} ->
          :end_of_table
      end
    end
  end

  defp iterate_start_by_prefix(db_handle, prefix, action) do
    fn ->
      case :rocksdb.iterator(db_handle, prefix_same_as_start: true) do
        {:ok, itr} ->
          {:first, prefix, itr, action}

        {:error, _reason} ->
          :end_of_table
      end
    end
  end

  defp iterate_start_by_prefix({db_handle, cf_handle}, prefix, offset, action) do
    fn ->
      case :rocksdb.iterator(db_handle, cf_handle, prefix_same_as_start: true) do
        {:ok, itr} ->
          {:first, prefix, itr, {:cf, cf_handle}, offset, action}

        {:error, _reason} ->
          :end_of_table
      end
    end
  end

  defp iterate_start_by_prefix(db_handle, prefix, offset, action) do
    fn ->
      case :rocksdb.iterator(db_handle, prefix_same_as_start: true) do
        {:ok, itr} ->
          {:first, prefix, itr, offset, action}

        {:error, _reason} ->
          :end_of_table
      end
    end
  end

  defp iterate_step_by_prefix({move, prefix, itr, {:cf, _cf_handle}, :iterate = action}) do
    upd_move = check_move(move, prefix)

    case :rocksdb.iterator_move(itr, upd_move) do
      {:ok, k, v} ->
        String.starts_with?(k, prefix)
        |> start_with_prefix(k, :erlang.binary_to_term(v), prefix, itr, action)

      {:error, _} ->
        {:halt, {prefix, :end_of_table, itr}}
    end
  end

  defp iterate_step_by_prefix({move, prefix, itr, :iterate = action}) do
    upd_move = check_move(move, prefix)

    case :rocksdb.iterator_move(itr, upd_move) do
      {:ok, k, v} ->
        String.starts_with?(k, prefix)
        |> start_with_prefix(k, :erlang.binary_to_term(v), prefix, itr, action)

      {:error, _} ->
        {:halt, {prefix, :end_of_table, itr}}
    end
  end

  defp iterate_step_by_prefix({move, prefix, itr, {:cf, _cf_handle}, offset, :iterate = action}) do
    upd_move = check_move(move, prefix)

    case :rocksdb.iterator_move(itr, upd_move) do
      {:ok, k, v} ->
        String.starts_with?(k, prefix)
        |> start_with_prefix(k, :erlang.binary_to_term(v), prefix, itr, offset, action)

      {:error, _} ->
        {:halt, {prefix, :end_of_table, itr}}
    end
  end

  defp iterate_step_by_prefix({move, prefix, itr, offset, :iterate = action}) do
    upd_move = check_move(move, prefix)

    case :rocksdb.iterator_move(itr, upd_move) do
      {:ok, k, v} ->
        String.starts_with?(k, prefix)
        |> start_with_prefix(k, :erlang.binary_to_term(v), prefix, itr, offset, action)

      {:error, _} ->
        {:halt, {prefix, :end_of_table, itr}}
    end
  end

  defp iterate_step_by_prefix({move, prefix, itr, {:cf, cf_handle}, :delete = action}) do
    upd_move = check_move(move, prefix)

    case :rocksdb.iterator_move(itr, upd_move) do
      {:ok, k, _} ->
        String.starts_with?(k, prefix)
        |> delete_condition(k, prefix, itr, cf_handle, action)

      {:error, _} ->
        {:halt, {prefix, :end_of_table, itr}}
    end
  end

  defp iterate_step_by_prefix({move, prefix, itr, :delete = action}) do
    upd_move = check_move(move, prefix)

    case :rocksdb.iterator_move(itr, upd_move) do
      {:ok, k, _} ->
        String.starts_with?(k, prefix)
        |> delete_condition(k, prefix, itr, action)

      {:error, _} ->
        {:halt, {prefix, :end_of_table, itr}}
    end
  end

  defp iterate_step_by_prefix(:end_of_table), do: nil

  defp check_move(:first, prefix), do: {:seek, prefix}
  defp check_move(:next, _), do: :next

  defp start_with_prefix(true, k, v, prefix, itr, action) do
    {[{k, v}], {:next, prefix, itr, action}}
  end

  defp start_with_prefix(false, _, _, prefix, itr, _) do
    {:halt, {prefix, :end_of_table, itr}}
  end

  defp start_with_prefix(true, k, v, prefix, itr, offset, action) do
    cond do
      k > offset ->
        {[{k, v}], {:next, prefix, itr, offset, action}}

      true ->
        {[], {:next, prefix, itr, offset, action}}
    end
  end

  defp start_with_prefix(false, _, _, prefix, itr, _, _) do
    {:halt, {prefix, :end_of_table, itr}}
  end

  defp delete_condition(true, k, prefix, itr, cf_handle, action) do
    {[{:delete, cf_handle, k}], {:next, prefix, itr, action}}
  end

  defp delete_condition(false, _, prefix, itr, _cf_handle, action) do
    {[nil], {:next, prefix, itr, action}}
  end

  defp delete_condition(true, k, prefix, itr, action) do
    {[{:delete, k}], {:next, prefix, itr, action}}
  end

  defp delete_condition(false, _, prefix, itr, action) do
    {[nil], {:next, prefix, itr, action}}
  end

  defp iterate_end_by_prefix(nil), do: nil
  defp iterate_end_by_prefix({:next, _, itr, _}), do: iterator_close(itr)
  defp iterate_end_by_prefix({:next, _, itr, _, _}), do: iterator_close(itr)
  defp iterate_end_by_prefix({_, _, itr}), do: iterator_close(itr)

  @doc """
    Close a iterator.
  """
  @spec iterator_close(itr_handle()) :: :ok
  def iterator_close(itr_handle), do: :rocksdb.iterator_close(itr_handle)

  @doc """
    Delete a key/value pair by key from the default column family.
  """
  @spec delete(db_handle(), binary(), write_options()) ::
          :ok | {:error, any()}
  def delete(db_handle, key, write_opts), do: :rocksdb.delete(db_handle, key, write_opts)

  @doc """
    Delete a key/value pair by key from the specified column family.
  """
  @spec delete(db_handle(), cf_handle(), binary(), write_options()) ::
          :ok | {:error, any()}
  def delete(db_handle, cf_handle, key, write_opts),
    do: :rocksdb.delete(db_handle, cf_handle, key, write_opts)

  @doc """
    Delete a key/value pair by key from the default column family.
    Requires that the key exists and was not overwritten.
  """
  @spec single_delete(db_handle(), binary(), write_options()) ::
          :ok | {:error, any()}
  def single_delete(db_handle, key, write_opts),
    do: :rocksdb.single_delete(db_handle, key, write_opts)

  @doc """
    Delete a key/value pair by key from the specified column family.
    Requires that the key exists and was not overwritten.
  """
  @spec single_delete(db_handle(), cf_handle(), binary(), write_options()) ::
          :ok | {:error, any()}
  def single_delete(db_handle, cf_handle, key, write_opts),
    do: :rocksdb.single_delete(db_handle, cf_handle, key, write_opts)

  @doc """
    Get count all records from the default column family.
  """
  @spec count(db_handle()) :: integer()
  def count(db_handle), do: Enum.count(stream_iterator(db_handle))

  @doc """
    Get count all records from the specified column family.
  """
  @spec count(db_handle(), cf_handle()) :: integer()
  def count(db_handle, cf_handle), do: Enum.count(stream_iterator({db_handle, cf_handle}))

  @doc """
    Close Rocksdb database.
  """
  @spec close(db_handle()) :: :ok | {:error, any()}
  def close(db_handle), do: :rocksdb.close(db_handle)
end

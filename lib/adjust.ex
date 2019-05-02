defmodule Adjust do
  alias Adjust.DB
  require Logger

  @source db: "foo", table: "source"
  @dest db: "bar", table: "dest"
  @databases [@source, @dest]
  @batch_size 10
  @total_rows 33
  @stream_opts max_rows: 5

  def run do
    create_databases()
    fill_source()
    copy_data()
    # drop_databases()
  end

  def create_databases do
    Logger.info("Creating databases")

    DB.connect(fn conn ->
      Enum.each(@databases, &DB.create_database(conn, &1[:db]))
    end)

    Enum.each(@databases, fn [db: db, table: table] ->
      DB.connect(db, fn conn ->
        DB.create_table(conn, table)
      end)
    end)
  end

  def drop_databases do
    Logger.info("Dropping databases")

    DB.connect(fn conn ->
      Enum.each(@databases, &DB.drop_database(conn, &1[:db]))
    end)
  end

  def fill_source do
    Logger.info("Populating SOURCE")
    DB.connect(@source[:db], &insert_rows(&1, @source[:table], 1))
  end

  defp insert_rows(_, _, from) when from > @total_rows, do: :ok

  defp insert_rows(conn, table, from) do
    to = min(from + @batch_size, @total_rows)
    values = from..to |> Enum.map(&row/1)
    DB.insert_values(conn, table, values)
    insert_rows(conn, table, to + 1)
  end

  defp row(n), do: [n, Integer.mod(n, 3), Integer.mod(n, 5)]

  def copy_data do
    Logger.info("Copying data to DEST")
    {source_stream, source_task} = stream_async(@source[:db], "COPY #{@source[:table]} TO STDOUT")
    {dest_stream, dest_task} = stream_async(@dest[:db], "COPY #{@dest[:table]} FROM STDIN")
    Enum.into(source_stream, dest_stream, &result_to_iodata/1)
    send(source_task.pid, :close)
    send(dest_task.pid, :close)
  end

  defp stream_async(db, query) do
    host = self()
    task = Task.async(fn -> stream(host, db, query) end)

    receive do
      {:stream, stream} -> {stream, task}
    end
  end

  defp stream(host, db, query) do
    DB.transaction(db, fn conn ->
      stream = Postgrex.stream(conn, query, [], @stream_opts)
      send(host, {:stream, stream})

      receive do
        :close -> :ok
      end
    end)
  end

  defp result_to_iodata(%Postgrex.Result{rows: rows}), do: rows
end

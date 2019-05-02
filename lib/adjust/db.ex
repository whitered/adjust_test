defmodule Adjust.DB do
  require Logger

  @pg_config Application.get_env(:adjust, :database)
  @stream_opts max_rows: 500
  @transaction_timeout 600_000

  def connect(db, fun) do
    @pg_config
    |> Keyword.put(:database, db)
    |> exec(fun)
  end

  def connect(fun) do
    exec(@pg_config, fun)
  end

  defp exec(config, fun) do
    {:ok, pid} = Postgrex.start_link(config)
    result = fun.(pid)
    GenServer.stop(pid)
    result
  end

  def transaction(db, fun) do
    connect(db, fn conn ->
      Postgrex.transaction(conn, fun, timeout: @transaction_timeout)
    end)
  end

  def stream(db, query, fun) do
    host = self()
    task = Task.async(fn -> start_stream(host, db, query) end)

    receive do
      {:stream, stream} ->
        result = fun.(stream)
        send(task.pid, :close)
        result
    end
  end

  defp start_stream(host, db, query) do
    transaction(db, fn conn ->
      stream = Postgrex.stream(conn, query, [], @stream_opts)
      send(host, {:stream, stream})

      receive do
        :close -> Logger.debug("Task #{inspect(self())} has finished")
      end
    end)
  end

  def create_database(conn, database) do
    query(conn, "CREATE DATABASE #{database}")
  end

  def create_table(conn, table) do
    query(conn, "CREATE TABLE #{table} (a INTEGER, b INTEGER, c INTEGER)")
  end

  def drop_database(conn, database) do
    query(conn, "DROP DATABASE #{database}")
  end

  def insert_values(conn, table, values) do
    values_query =
      values
      |> Enum.map(&("(" <> Enum.join(&1, ",") <> ")"))
      |> Enum.join(",")

    query(conn, "INSERT INTO #{table} VALUES #{values_query}")
  end

  defp query(conn, query) do
    Logger.debug(query)
    Postgrex.query!(conn, query, [])
  end
end

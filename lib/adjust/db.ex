defmodule Adjust.DB do
  require Logger

  @pg_config Application.get_env(:adjust, :database)

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
    fun.(pid)
    GenServer.stop(pid)
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

  defp query(conn, query) do
    Logger.debug(query)
    Postgrex.query!(conn, query, [])
  end
end

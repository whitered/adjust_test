defmodule Adjust do
  alias Adjust.DB
  require Logger

  @source db: "foo", table: "source"
  @dest db: "bar", table: "dest"
  @databases [@source, @dest]
  @batch_size 1000
  @total_rows 1_000_000
  @port 4000

  def databases, do: @databases

  def run do
    create_databases()
    fill_source()
    copy_data()
    start_server()
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

    DB.stream(@source[:db], "COPY #{@source[:table]} TO STDOUT", fn src ->
      DB.stream(@dest[:db], "COPY #{@dest[:table]} FROM STDIN", fn dest ->
        Enum.into(src, dest, &result_to_iodata/1)
      end)
    end)
  end

  defp result_to_iodata(%Postgrex.Result{rows: rows}), do: rows

  def start_server do
    Logger.info("Starting web server at http://localhost:#{@port}")
    Plug.Cowboy.http(Adjust.Server, [], port: @port)
  end
end

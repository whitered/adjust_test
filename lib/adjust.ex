defmodule Adjust do
  alias Adjust.DB

  @source db: "foo", table: "source"
  @dest db: "bar", table: "dest"
  @databases [@source, @dest]
  @batch_size 1000
  @total_rows 1_000_000

  def run do
    DB.connect(@source[:db], &insert_rows(&1, @source[:table], 1))
  end

  def create_databases do
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
    DB.connect(fn conn ->
      Enum.each(@databases, &DB.drop_database(conn, &1[:db]))
    end)
  end

  defp insert_rows(_, _, from) when from > @total_rows, do: :ok

  defp insert_rows(conn, table, from) do
    to = min(from + @batch_size, @total_rows)
    values = from..to |> Enum.map(&row/1)
    DB.insert_values(conn, table, values)
    populate_source(conn, table, to + 1)
  end

  defp row(n), do: [n, Integer.mod(n, 3), Integer.mod(n, 5)]
end

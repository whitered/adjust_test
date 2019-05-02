defmodule Adjust do
  alias Adjust.DB

  @source db: "foo", table: "source"
  @dest db: "bar", table: "dest"
  @databases [@source, @dest]

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
end

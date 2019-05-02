defmodule Adjust.Server do
  import Plug.Conn
  alias Adjust.DB

  def init(_), do: :ok

  def call(%{path_info: ["dbs", db, "tables", table]} = conn, _) do
    case table_exists?(db, table) do
      true -> render_csv(conn, db, table)
      false -> render_404(conn)
    end
  end

  def call(conn, _), do: render_404(conn)

  defp table_exists?(db, table) do
    Adjust.databases()
    |> Enum.any?(fn [db: d, table: t] -> d == db and t == table end)
  end

  defp render_404(conn), do: send_resp(conn, 404, "Not found")

  defp render_csv(conn, db, table) do
    conn =
      conn
      |> put_resp_content_type("text/csv")
      # |> put_resp_header("content-disposition", "attachment; filename=#{table}.csv")
      |> send_chunked(200)

    DB.stream(db, "COPY #{table} TO STDOUT CSV HEADER", fn stream ->
      Enum.reduce_while(stream, conn, fn chunk, conn ->
        case chunk(conn, chunk.rows) do
          {:ok, conn} ->
            {:cont, conn}

          {:error, :closed} ->
            {:halt, conn}
        end
      end)
    end)
  end
end

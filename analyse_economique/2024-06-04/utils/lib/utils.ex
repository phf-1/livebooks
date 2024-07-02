defmodule Utils do

  @moduledoc """
  #{inspect(__MODULE__)} provides Explorer.DataFrame representing various economic data.
  """

  alias Explorer.DataFrame, as: DF
  use GenServer


  def start_link([data_sources: data_sources, db_path: db_path]) do
	GenServer.start_link(__MODULE__, %{data_sources: data_sources, db_path: db_path})
  end


  # Client API

  @doc """
  df() ≡ df is a DF that stores data in the db.
  If the db is empty, the data is fetched from the data_sources, and stored in the db.
  The db serves as a cache.
  """
  @spec df(pid(), atom()) :: DF.t()
  def df(pid, symbol) do
	GenServer.call(pid, {:df, symbol})
  end


  # Server API

  @impl true
  def init(%{data_sources: data_sources, db_path: db_path}) do
	:ok = Adbc.download_driver(:sqlite)
	{:ok, db_adbc} = Adbc.Database.start_link(driver: :sqlite, uri: db_path)
	{:ok, conn} = Adbc.Connection.start_link(database: db_adbc)
	{:ok, %{data_sources: data_sources, conn: conn}}
  end

  @impl true
  def handle_call({:df, symbol}, _from, state) do
	%{conn: conn, data_sources: data_sources} = state
	table_name = Atom.to_string(symbol)
	url = data_sources[symbol]
	if not table_exist?(table_name, conn) do
	  data = fetch_data!(url)
	  {columns, rows} = analyse_data!(data)
	  add_table!(table_name, columns, rows, conn)
	end
	{:reply, df_build!(table_name, conn), state}
  end


  # Private API

  @spec table_exist?(String.t(), Adbc.Connection.t()) :: boolean()
  defp table_exist?(table_name, adbc_conn) do
	query = "SELECT name FROM sqlite_master WHERE type='table' AND name='#{table_name}';"
	{:ok, result} = Adbc.Connection.query(adbc_conn, query)
	result.num_rows != nil
  end


  @doc """
  fetch_data!(url) ≡ data represents the data fetched from url.
  """
  @spec fetch_data!(String.t()) :: String.t()
  defp fetch_data!(url) do
	  {:ok, response} = Req.get(url)
	  response.body
  end


  @doc """
  analyse_data!(data) ≡ {columns, rows}.
  where data is a CSV string.
  """
  @type columns :: [String.t()]
  @type row :: [String.t()]
  @type rows :: [row()]
  @spec analyse_data!(String.t()) :: {columns(), rows()}
  defp analyse_data!(csv) do
	{:ok, csv_stream} = StringIO.open(csv)
	csv_stream = csv_stream |> IO.binstream(:line) |> CSV.decode!()
	columns = Enum.at(csv_stream, 0)
	rows = Enum.to_list(csv_stream)
	{columns, rows}
  end


  @spec add_table!(String.t(), columns(), rows(), Adbc.Connection.t()) :: none()
  defp add_table!(table_name, columns, rows, conn) do
	table_create = "CREATE TABLE IF NOT EXISTS #{table_name}"
	row = Enum.at(rows,0)
	columns_types = Enum.map(row, fn val -> guess_type!(val) end)
	table_create = table_create <> " ("
	add_col_spec = fn ([col, type], query) -> query <> "#{col} #{type}, " end
	table_create = Enum.zip_reduce([columns, columns_types], table_create, add_col_spec)
	table_create = String.replace_suffix(table_create, ", ", ");")
	Adbc.Connection.query!(conn, table_create)
	add_row = "INSERT INTO #{table_name} ("
	add_row = Enum.reduce(columns, add_row, fn (col, query) -> query <> "#{col}, " end)
	add_row = String.replace_suffix(add_row, ", ", ") VALUES (")
	add_row = Enum.reduce(columns, add_row, fn (col, query) -> query <> "?, " end)
	add_row = String.replace_suffix(add_row, ", ", ");")
	{:ok, add_row_prepared} = Adbc.Connection.prepare(conn, add_row)
	IO.inspect(add_row_prepared)
	Enum.each(rows, fn row -> Adbc.Connection.query(conn, add_row_prepared, row) end)
  end


  @doc """
  guess_type! col ≡ SQLite datatype.
  See: https://www.sqlite.org/datatype3.html
  """
  @spec guess_type!(String.t()) :: String.t()
  def guess_type!(str) do
	cond do
	  elem(Date.from_iso8601(str), 0) == :ok -> "TEXT"
	  Integer.parse(str) != :error -> "INTEGER" # TODO: wrong. what if str = "0.123" ?
	  Float.parse(str) != :error -> "REAL"
	  true -> "TEXT"
	end
  end


  @spec df_build!(String.t(), Adbc.Connection.t()) :: DF.t()
  defp df_build!(table_name, adbc_conn) do
	{:ok, df} = DF.from_query(adbc_conn, "SELECT * FROM #{table_name};", [])
	df
  end
end

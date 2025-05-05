import pytest
from unittest.mock import patch, MagicMock

from tvm_multi_scan_exporter.database import _db_engine, write_to_db
from tvm_multi_scan_exporter.configuration import WriteToDatabase


@pytest.fixture
@patch.dict("os.environ", {"EXTERNAL_DB_PASSWORD": "p@ssw0rd!"})
def ms_sql_config():
    return WriteToDatabase(
        user="sa",
        host_address="localhost",
        port=1433,
        database_name="mydb",
        table_name="mytable"
    )


@patch("tvm_multi_scan_exporter.database.sqlalchemy.create_engine")
def test_db_engine_creates_engine(mock_create_engine, ms_sql_config):
    mock_engine = mock_create_engine.return_value

    engine = _db_engine(ms_sql_config)

    mock_create_engine.assert_called_once()
    assert engine == mock_engine


@patch("tvm_multi_scan_exporter.database.sqlalchemy.create_engine")
def test_db_engine_connection_string_correct(mock_create_engine, ms_sql_config):
    _db_engine(ms_sql_config)

    args, kwargs = mock_create_engine.call_args
    connection_string = args[0]

    assert connection_string.startswith("mssql+pyodbc://")
    assert "localhost" in connection_string
    assert "mydb" in connection_string
    assert "driver=" in connection_string
    assert "MultiSubnetFailover=yes" in connection_string
    assert kwargs.get("fast_executemany") is True


@patch("tvm_multi_scan_exporter.database.sqlalchemy.create_engine")
@patch.dict("os.environ", {"EXTERNAL_DB_PASSWORD": "p@ss:word"})
def test_db_engine_handles_special_chars(mock_create_engine):
    config = WriteToDatabase(
        user="user!name",
        host_address="my-server",
        port=1433,
        database_name="prod_db",
        table_name="export_table"
    )

    _db_engine(config)

    args, _ = mock_create_engine.call_args
    conn_str = args[0]

    assert "user!name" in conn_str
    assert "p%40ss%3Aword" in conn_str
    assert "driver=" in conn_str


@patch.dict("os.environ", {"EXTERNAL_DB_PASSWORD": "y"})
def test_db_engine_with_unsupported_engine():
    # Future-proof test: what if someone adds a new engine and forgets to update match-case?
    config = WriteToDatabase(
        user="x",
        host_address="z",
        port=1234,
        database_name="db",
        table_name="t",
        engine=None  # simulate unsupported case
    )

    with pytest.raises(AttributeError):
        _db_engine(config)


@pytest.fixture
@patch.dict("os.environ", {"EXTERNAL_DB_PASSWORD": "secret"})
def db_config():
    return WriteToDatabase(
        user="admin",
        host_address="localhost",
        port=1433,
        database_name="mydb",
        table_name="mytable",
        write_batch_size=1000
    )


@patch("tvm_multi_scan_exporter.database.sqlalchemy.create_engine")
@patch("tvm_multi_scan_exporter.database.pyarrow.Table")
@patch("tvm_multi_scan_exporter.database.duckdb.execute")
def test_write_to_db_batch_write_flow(mock_duck_execute, mock_arrow_table, mock_create_engine, db_config):
    # Simulate multiple batches
    mock_batch1 = MagicMock(num_rows=100)
    mock_batch2 = MagicMock(num_rows=200)
    mock_duck_execute.return_value.fetch_record_batch.return_value = [mock_batch1, mock_batch2]

    # Set up chained mocks for pyarrow → pandas → to_sql
    mock_df = MagicMock()
    mock_df.to_sql = MagicMock()

    mock_table = MagicMock()
    mock_arrow_table.from_batches.return_value = mock_table
    mock_table.drop_columns.return_value.to_pandas.return_value = mock_df

    # Run
    write_to_db(db_config, "SELECT * FROM final_data")

    # Verify duckdb interaction
    mock_duck_execute.assert_called_once_with("SELECT * FROM final_data")
    mock_duck_execute.return_value.fetch_record_batch.assert_called_once_with(1000)

    # Ensure Arrow batches were built for both
    assert mock_arrow_table.from_batches.call_count == 2
    mock_arrow_table.from_batches.assert_any_call([mock_batch1])
    mock_arrow_table.from_batches.assert_any_call([mock_batch2])

    # Ensure both batches were written to DB via Pandas
    assert mock_df.to_sql.call_count == 2
    for call in mock_df.to_sql.call_args_list:
        args, kwargs = call
        assert args[0] == "mytable"
        assert kwargs["if_exists"] == "append"
        assert kwargs["index"] is False
        assert kwargs["method"] == "multi"


@patch("tvm_multi_scan_exporter.database.duckdb.execute")
def test_write_to_db_with_no_batches_does_nothing(mock_duck_execute, db_config):
    mock_duck_execute.return_value.fetch_record_batch.return_value = []

    with patch("tvm_multi_scan_exporter.database.logging.info") as mock_log:
        write_to_db(db_config, "SELECT * FROM empty_view")

        messages = [call.args[0] for call in mock_log.call_args_list]
        assert any("Database Push Operation complete." in msg for msg in messages)
        assert all("Writing" not in msg for msg in messages)  # no writes should be logged

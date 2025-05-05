from unittest.mock import MagicMock, patch, ANY
from pathlib import Path
from tvm_multi_scan_exporter.scan_transformer import ScanResultTransformer
from tvm_multi_scan_exporter.configuration import Config, Csv, Parquet, Json, WriteToDatabase


@patch("tvm_multi_scan_exporter.scan_transformer.duckdb")
@patch("tvm_multi_scan_exporter.scan_transformer.file_size_in_mb", return_value=10.5)
@patch("tvm_multi_scan_exporter.scan_transformer.write_to_db")
@patch("tvm_multi_scan_exporter.scan_transformer.logging")
def test_concatenate_exports_csv(mock_logging, mock_write_to_db, mock_file_size, mock_duckdb):
    mock_config = Config(scan_name="test-scan", allowed_memory_gb=2, export_type=Csv())
    mock_duckdb.sql.return_value.write_csv = MagicMock()

    transformer = ScanResultTransformer()
    deduped_files_folder = Path("/tmp/deduped")
    transformer.concatenate_exports(mock_config, deduped_files_folder)

    mock_duckdb.sql.assert_called_once()
    mock_duckdb.sql.return_value.write_csv.assert_called_once()
    mock_logging.info.assert_any_call(ANY)  # Use ANY to match the wildcard file name


@patch("tvm_multi_scan_exporter.scan_transformer.duckdb")
@patch("tvm_multi_scan_exporter.scan_transformer.file_size_in_mb", return_value=15.2)
@patch("tvm_multi_scan_exporter.scan_transformer.write_to_db")
@patch("tvm_multi_scan_exporter.scan_transformer.logging")
def test_concatenate_exports_parquet(mock_logging, mock_write_to_db, mock_file_size, mock_duckdb):
    mock_config = Config(scan_name="test-scan", allowed_memory_gb=2, export_type=Parquet())
    mock_duckdb.sql.return_value.write_parquet = MagicMock()

    transformer = ScanResultTransformer()
    deduped_files_folder = Path("/tmp/deduped")
    transformer.concatenate_exports(mock_config, deduped_files_folder)

    mock_duckdb.sql.assert_called_once()
    mock_duckdb.sql.return_value.write_parquet.assert_called_once()
    mock_logging.info.assert_any_call(ANY)  # Use ANY to match the wildcard file name


@patch("tvm_multi_scan_exporter.scan_transformer.duckdb")
@patch("tvm_multi_scan_exporter.scan_transformer.file_size_in_mb", return_value=5.8)
@patch("tvm_multi_scan_exporter.scan_transformer.write_to_db")
@patch("tvm_multi_scan_exporter.scan_transformer.logging")
@patch("tvm_multi_scan_exporter.scan_transformer.json.dump")
@patch("builtins.open", new_callable=MagicMock)
def test_concatenate_exports_json(mock_open, mock_json_dump, mock_logging, mock_write_to_db, mock_file_size,
                                  mock_duckdb):
    mock_config = Config(scan_name="test-scan", allowed_memory_gb=2, export_type=Json())
    mock_duckdb_result = MagicMock()
    mock_duckdb_result.fetchall.return_value = [("row1", "row2")]
    mock_duckdb_result.description = [("col1",), ("col2",)]
    mock_duckdb.sql.side_effect = [mock_duckdb_result, mock_duckdb_result]  # Allow two calls

    transformer = ScanResultTransformer()
    deduped_files_folder = Path("/tmp/deduped")
    transformer.concatenate_exports(mock_config, deduped_files_folder)

    assert mock_duckdb.sql.call_count == 2  # Ensure two calls to duckdb.sql
    mock_json_dump.assert_called_once()
    mock_logging.info.assert_any_call(ANY)  # Use ANY to match the wildcard file name


@patch("tvm_multi_scan_exporter.scan_transformer.duckdb")
@patch("tvm_multi_scan_exporter.scan_transformer.file_size_in_mb")
@patch("tvm_multi_scan_exporter.scan_transformer.write_to_db")
@patch("tvm_multi_scan_exporter.scan_transformer.logging")
@patch.dict("os.environ", {"EXTERNAL_DB_PASSWORD": "pass"})
def test_concatenate_exports_write_to_db(mock_logging, mock_write_to_db, mock_file_size, mock_duckdb):
    mock_config = Config(
        scan_name="test-scan",
        allowed_memory_gb=2,
        export_type=WriteToDatabase(
            user="user",
            host_address="localhost",
            port=5432,
            database_name="test_db",
            table_name="test_table"
        )
    )
    mock_duckdb.sql.return_value = MagicMock()

    transformer = ScanResultTransformer()
    deduped_files_folder = Path("/tmp/deduped")
    transformer.concatenate_exports(mock_config, deduped_files_folder)

    mock_duckdb.sql.assert_not_called()  # Ensure duckdb.sql is not called
    mock_write_to_db.assert_called_once()  # Ensure write_to_db is called

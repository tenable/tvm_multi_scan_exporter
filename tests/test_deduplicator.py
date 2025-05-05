from unittest.mock import MagicMock, patch
from pathlib import Path
from tvm_multi_scan_exporter.deduplicator import dedup_scan_export
from tvm_multi_scan_exporter.configuration import Config


@patch("tvm_multi_scan_exporter.deduplicator.deduplication_query_string")
@patch("tvm_multi_scan_exporter.deduplicator.duckdb.sql")
@patch("tvm_multi_scan_exporter.deduplicator.file_size_in_mb")
@patch("tvm_multi_scan_exporter.deduplicator.os.remove")
def test_dedup_scan_export(
        mock_remove, mock_file_size, mock_duckdb_sql, mock_dedup_query
):
    # Mock inputs
    source = Path("/tmp/source.csv")
    destination = Path("/tmp/destination.csv")
    config = Config(scan_name="test-scan", allowed_memory_gb=2)

    # Mock return values
    mock_dedup_query.return_value = "SELECT * FROM source"
    mock_file_size.return_value = 10.5
    mock_duckdb_sql.return_value.to_df = MagicMock()

    # Call the function
    dedup_scan_export(source, destination, config)

    # Assertions
    mock_dedup_query.assert_called_once_with(config, source)
    mock_duckdb_sql.assert_called_once_with("SELECT * FROM source")
    mock_duckdb_sql.return_value.to_df.assert_called_once_with()
    mock_file_size.assert_called_once_with(destination)
    mock_remove.assert_called_once_with(source)

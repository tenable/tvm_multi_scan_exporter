import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path
from tvm_multi_scan_exporter.duck_db import deduplication_query_string, concatenation_sql_string, _columns_sql_string
from tvm_multi_scan_exporter.configuration import Config


@pytest.fixture
def mock_config():
    return Config(
        scan_name="test-scan",
        allowed_memory_gb=2,
        columns=["Column A", "Column B"]
    )


def normalize_sql(sql):
    """Normalize SQL by removing extra spaces."""
    return " ".join(sql.split())


@patch("tvm_multi_scan_exporter.duck_db._columns_sql_string", return_value="ANY_VALUE(\"Column A\") AS column_a")
def test_deduplication_query_string(mock_columns_sql, mock_config):
    source_file = Path("/tmp/source.csv")
    result = deduplication_query_string(mock_config, source_file)
    expected = f"""
        SET memory_limit = '2GB';
        SELECT
            ANY_VALUE("Column A") AS column_a
        FROM
            read_csv_auto('{str(source_file)}') scans
        WHERE
            "Vulnerability State" != 'Fixed'
        GROUP BY identifier;
    """.strip()
    assert normalize_sql(result) == normalize_sql(expected)


def test_concatenation_sql_string(mock_config):
    source_directory_wildcard = "/tmp/*.csv"
    result = concatenation_sql_string(mock_config, source_directory_wildcard)
    expected = f"""
        SET memory_limit = '2GB';
        WITH latest AS (
            SELECT
                host AS latestHost,
                max(host_end) AS latestHostEnd
            FROM
                read_csv_auto("{source_directory_wildcard}", union_by_name=true, all_varchar=true, sample_size=-1)
            GROUP BY
                host
        )
        SELECT
            scans.*
        FROM
            read_csv_auto("{source_directory_wildcard}", union_by_name=true, all_varchar=true, sample_size=-1) scans,
            latest
        WHERE
                host = latestHost
            AND host_end = latestHostEnd;
    """.strip()
    assert result == expected

import os
from time import time
from unittest.mock import patch

import pytest

import tvm_multi_scan_exporter
from tvm_multi_scan_exporter import Csv, Json, Parquet, WriteToDatabase, DatabaseEngine
from tvm_multi_scan_exporter.configuration import get_config_from_file, _parse_export_type, Config, Window, ExportType
from tvm_multi_scan_exporter.constants import DEFAULT_WRITE_BATCH_SIZE, DEFAULT_COLUMNS_TO_BE_PART_OF_EXPORT, \
    DEFAULT_WORKERS, MAX_SINCE_SECONDS, CURRENT_TIME


def test_default_window_uses_current_time():
    window = Window()
    assert window.upto == CURRENT_TIME
    assert window.since == CURRENT_TIME - 86400


def test_valid_window_with_custom_times():
    now = int(time())
    window = Window(since=now - 3600, upto=now)
    assert window.upto == now
    assert window.since == now - 3600


def test_window_exactly_15_days_passes():
    now = int(time())
    since = now - MAX_SINCE_SECONDS
    window = Window(since=since, upto=now)
    assert window.since == since
    assert window.upto == now


def test_window_with_since_after_upto_allowed():
    now = int(time())
    window = Window(since=now + 3600, upto=now)
    # Should not raise anything — logic doesn't forbid it
    assert window.since > window.upto


def test_window_exceeds_15_day_limit_raises():
    now = int(time())
    since = now - (MAX_SINCE_SECONDS + 1)
    with pytest.raises(ValueError, match="15 days older than"):
        Window(since=since, upto=now)


def test_zero_length_window():
    now = int(time())
    window = Window(since=now, upto=now)
    assert window.upto == window.since


def test_window_with_only_since():
    now = int(time())
    window = Window(since=now - 3600)
    assert window.since == now - 3600
    assert window.upto == CURRENT_TIME  # the default


def test_window_with_only_upto():
    now = int(time())
    window = Window(upto=now)
    assert window.upto == now
    assert window.since == CURRENT_TIME - 86400  # the default


def test_window_only_since_too_old_fails():
    now = int(time())
    too_old = now - (MAX_SINCE_SECONDS + 5)
    with pytest.raises(ValueError, match="15 days"):
        Window(since=too_old)


def make_valid_window():
    now = int(time())
    return Window(since=now - 3600, upto=now)


def test_valid_config_construction_and_validation():
    config = Config(
        scan_name="scan-001",
        allowed_memory_gb=4,
        workers=5,
        window=make_valid_window(),
        export_type=Csv(),
        columns=["Asset UUID", "IP Address"]
    )
    assert config.workers == 5
    assert isinstance(config.export_type, ExportType)


def test_workers_out_of_range_low():
    with pytest.raises(ValueError, match="`workers` must be between 1 and 10"):
        Config(
            scan_name="scan-001",
            allowed_memory_gb=4,
            workers=0,  # invalid
            window=make_valid_window(),
            export_type=Csv(),
        )


def test_workers_out_of_range_high():
    with pytest.raises(ValueError, match="`workers` must be between 1 and 10"):
        Config(
            scan_name="scan-001",
            allowed_memory_gb=4,
            workers=11,  # invalid
            window=make_valid_window(),
            export_type=Csv(),
        )


def test_default_fields_apply_correctly():
    config = Config(
        scan_name="default-scan",
        allowed_memory_gb=2,
    )
    assert config.workers == DEFAULT_WORKERS
    assert config.columns == DEFAULT_COLUMNS_TO_BE_PART_OF_EXPORT
    assert isinstance(config.window, Window)
    assert isinstance(config.export_type, Csv)


def test_validate_type_errors_on_config_fields():
    valid_window = Window(since=int(time()) - 3600, upto=int(time()))

    with pytest.raises(TypeError) as e:
        Config(
            scan_name=123,
            allowed_memory_gb="four",
            workers="three",
            window=valid_window,
            export_type="csv",
        )

    assert "should be of type" in str(e.value)


def test_validate_passes_for_valid_config():
    config = Config(
        scan_name="something",
        allowed_memory_gb=2,
        window=make_valid_window()
    )
    # should not raise


def test_parse_export_type_string_csv():
    assert isinstance(_parse_export_type("csv"), Csv)


def test_parse_export_type_string_json():
    assert isinstance(_parse_export_type("json"), Json)


def test_parse_export_type_string_parquet():
    assert isinstance(_parse_export_type("parquet"), Parquet)


def test_parse_export_type_string_case_insensitive():
    assert isinstance(_parse_export_type("CSV"), Csv)
    assert isinstance(_parse_export_type("Json"), Json)


def test_parse_export_type_string_invalid():
    with pytest.raises(ValueError, match="Unknown export_type: pdf"):
        _parse_export_type("pdf")


@patch.dict("os.environ", {"EXTERNAL_DB_PASSWORD": "secret"})
def test_parse_export_type_valid_dict():
    config = {
        "type": "write_to_database",
        "user": "admin",
        "host_address": "localhost",
        "port": 5432,
        "database_name": "mydb",
        "table_name": "mytable",
        "engine": "MsSqlServer",
        "write_batch_size": 1000
    }

    result = _parse_export_type(config)
    assert isinstance(result, WriteToDatabase)
    assert result.user == "admin"
    assert result.engine == DatabaseEngine.MsSqlServer
    assert result.write_batch_size == 1000


@patch.dict("os.environ", {"EXTERNAL_DB_PASSWORD": "secret"})
def test_parse_export_type_dict_missing_type_defaults_to_write_to_database():
    config = {
        "user": "admin",
        "host_address": "localhost",
        "port": 3306,
        "database_name": "default",
        "table_name": "default_table"
    }

    result = _parse_export_type(config)
    assert isinstance(result, WriteToDatabase)
    assert result.engine == DatabaseEngine.MsSqlServer
    assert result.write_batch_size == DEFAULT_WRITE_BATCH_SIZE


def test_parse_export_type_dict_with_invalid_type():
    config = {"type": "something_else"}
    with pytest.raises(ValueError, match="Only 'write_to_database' export_type is supported"):
        _parse_export_type(config)


def test_parse_export_type_dict_missing_required_field():
    config = {
        "type": "write_to_database",
        "user": "admin",
        "password": "secret"
        # missing server, port, etc.
    }
    with pytest.raises(KeyError):
        _parse_export_type(config)


def test_parse_export_type_invalid_type():
    with pytest.raises(TypeError, match="export_type must be a string or a dictionary"):
        _parse_export_type(123)

    with pytest.raises(TypeError):
        _parse_export_type(None)


NOW = int(time())

VALID_DATA = {
    "scan_name": "test-scan",
    "allowed_memory_gb": 2,
    "workers": 4,
    "columns": ["a", "b"],
    "window": {
        "since": NOW - 3600,  # 1 hour ago
        "upto": NOW  # now
    },
    "export_type": "csv"
}


@patch("tvm_multi_scan_exporter.configuration.read_toml_from_file")
def test_valid_config(mock_read):
    mock_read.return_value = VALID_DATA
    config = get_config_from_file("config.toml")

    assert isinstance(config, Config)
    assert config.scan_name == "test-scan"
    assert config.allowed_memory_gb == 2
    assert config.workers == 4
    assert config.columns == ["a", "b"]
    assert isinstance(config.window, tvm_multi_scan_exporter.Window)
    assert isinstance(config.export_type, Csv)


@patch("tvm_multi_scan_exporter.configuration.read_toml_from_file")
def test_optional_fields_defaulted(mock_read):
    minimal_data = {
        "scan_name": "test",
        "allowed_memory_gb": 1
    }
    mock_read.return_value = minimal_data
    config = get_config_from_file("some_path.toml")

    assert config.workers == DEFAULT_WORKERS
    assert config.columns == DEFAULT_COLUMNS_TO_BE_PART_OF_EXPORT
    assert isinstance(config.window, tvm_multi_scan_exporter.Window)  # should use empty/default window
    assert isinstance(config.export_type, Csv)


@patch("tvm_multi_scan_exporter.configuration.read_toml_from_file")
def test_missing_scan_name_raises(mock_read):
    mock_read.return_value = {
        "allowed_memory_gb": 1
    }
    with pytest.raises(ValueError, match="`scan_name` is required"):
        get_config_from_file("config.toml")


@patch("tvm_multi_scan_exporter.configuration.read_toml_from_file")
def test_missing_memory_raises(mock_read):
    mock_read.return_value = {
        "scan_name": "valid-scan"
    }
    with pytest.raises(ValueError, match="`allowed_memory_gb` is required"):
        get_config_from_file("config.toml")


@patch("tvm_multi_scan_exporter.configuration.read_toml_from_file")
def test_invalid_export_type_raises(mock_read):
    data = VALID_DATA.copy()
    data["export_type"] = "pdf"
    mock_read.return_value = data
    with pytest.raises(ValueError, match="Unknown export_type: pdf"):
        get_config_from_file("config.toml")


@patch("tvm_multi_scan_exporter.configuration.read_toml_from_file")
def test_read_toml_raises_passed_through(mock_read):
    mock_read.side_effect = FileNotFoundError("File not found")
    with pytest.raises(FileNotFoundError):
        get_config_from_file("nonexistent.toml")


@patch("tvm_multi_scan_exporter.configuration.read_toml_from_file")
def test_window_since_too_old_raises(mock_read):
    mock_read.return_value = {
        "scan_name": "late-scan",
        "allowed_memory_gb": 2,
        "window": {
            "since": NOW - (MAX_SINCE_SECONDS + 1),
            "upto": NOW
        }
    }
    with pytest.raises(ValueError, match=r"`since` cannot be more than 15 days older than `upto`"):
        get_config_from_file("config.toml")


@patch.dict(os.environ, {}, clear=True)
def test_write_to_database_raises_exception_when_password_env_var_not_set():
    with pytest.raises(EnvironmentError,
                       match="In order to utilise an external database, environment variable "
                             "'EXTERNAL_DB_PASSWORD' is required."
                       ):
        WriteToDatabase(
            user="admin",
            host_address="localhost",
            port=1433,
            database_name="test_db",
            table_name="test_table",
            engine=DatabaseEngine.MsSqlServer
        )

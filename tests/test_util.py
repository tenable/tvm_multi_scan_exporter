from pathlib import Path
from typing import Any
from unittest.mock import mock_open, patch, MagicMock

import pytest
import tomli

from tvm_multi_scan_exporter.util import snake_case, read_toml_from_file, check_type, file_size_in_mb


@pytest.mark.parametrize("input_str,expected", [
    ("Asset UUID", "asset_uuid"),
    ("  Asset UUID  ", "asset_uuid"),  # leading/trailing spaces
    ("Asset@UUID", "asset_uuid"),  # special character
    ("Asset__UUID", "asset_uuid"),  # collapse underscores
    ("Asset--UUID", "asset_uuid"),  # multiple special chars
    ("Asset UUID Name", "asset_uuid_name"),  # multiple words
    ("Asset123 Name456", "asset123_name456"),  # with numbers
    ("   ", ""),  # only spaces
    ("---", ""),  # only special characters
    ("Asset___  ###UUID", "asset_uuid"),  # mix of underscores + specials + space
    ("asset_uuid", "asset_uuid"),  # already snake_case
    ("MixedCASEInput Here", "mixedcaseinput_here"),  # mixed case
    ("snakeCaseAlready", "snakecasealready"),  # camelCase (no splitting)
])
def test_snake_case(input_str, expected):
    assert snake_case(input_str) == expected


# Example TOML content
valid_toml = b"""
[project]
name = "my-app"
version = "1.0.0"
"""

invalid_toml = b"""
[project
name = "oops"
"""


def test_read_valid_toml():
    # mock open() to return valid_toml content
    with patch("builtins.open", mock_open(read_data=valid_toml)) as mocked_open:
        result = read_toml_from_file("fake_path.toml")
        mocked_open.assert_called_once_with("fake_path.toml", "rb")
        assert result["project"]["name"] == "my-app"
        assert result["project"]["version"] == "1.0.0"


def test_file_not_found():
    with patch("builtins.open", side_effect=FileNotFoundError):
        with pytest.raises(FileNotFoundError):
            read_toml_from_file("nonexistent.toml")


def test_invalid_toml_syntax():
    with patch("builtins.open", mock_open(read_data=invalid_toml)):
        with pytest.raises(tomli.TOMLDecodeError):
            read_toml_from_file("bad.toml")


# A custom class for testing
class MyClass:
    pass


class AnotherClass:
    pass


@pytest.mark.parametrize("value,expected_type", [
    (123, int),
    ("hello", str),
    (3.14, float),
    (True, bool),
    ([1, 2, 3], list),
    ((1, 2), tuple),
    ({"a": 1}, dict),
    (None, type(None)),  # checking NoneType
    (MyClass(), MyClass),
])
def test_check_type_valid(value, expected_type):
    # Should not raise
    check_type(value, expected_type)


def test_check_type_with_union():
    check_type("hello", (int, str))  # multiple allowed types
    check_type(42, (int, str))  # still valid


def test_check_type_with_custom_class_fails():
    with pytest.raises(TypeError, match=r"The value .* should be of type .*"):
        check_type(MyClass(), AnotherClass)


def test_check_type_failure_scalar():
    with pytest.raises(TypeError, match=r"The value .* should be of type .*"):
        check_type(123, str)

    with pytest.raises(TypeError):
        check_type([1, 2, 3], dict)


def test_check_type_any_accepts_everything():
    # `Any` isn’t a real runtime type — isinstance will fail if you use it directly.
    # So this fails unless you specially handle `expected_type == Any` in your function.
    with pytest.raises(TypeError):
        check_type(123, Any)


def test_check_type_type_name_in_error():
    # Verifies that the error message is meaningful
    with pytest.raises(TypeError) as exc_info:
        check_type("abc", int)
    assert "The value abc should be of type" in str(exc_info.value)


# 1 MB = 1024 * 1024 bytes = 1_048_576

@pytest.mark.parametrize("size_bytes,expected_mb", [
    (0, 0.0),
    (1, 0.0),
    (1024, 0.001),
    (1048576, 1.0),
    (5 * 1048576, 5.0),
    (1048576 + 512000, 1.488),
])
def test_file_size_in_mb(size_bytes, expected_mb):
    fake_path = Path("fake.txt")
    mock_stat_result = MagicMock(st_size=size_bytes)

    with patch.object(Path, "stat", return_value=mock_stat_result):
        assert file_size_in_mb(fake_path) == expected_mb


def test_file_size_raises_on_missing_file():
    fake_path = Path("missing.txt")

    with patch.object(Path, "stat", side_effect=FileNotFoundError):
        with pytest.raises(FileNotFoundError):
            file_size_in_mb(fake_path)


def test_file_size_with_large_file():
    fake_path = Path("huge_file.txt")
    size_bytes = 10 * 1024 ** 3  # 10 GB
    mock_stat_result = MagicMock(st_size=size_bytes)

    with patch.object(Path, "stat", return_value=mock_stat_result):
        assert file_size_in_mb(fake_path) == 10240.0

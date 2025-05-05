from unittest.mock import patch
import logging

from tvm_multi_scan_exporter.files import TempDirectory


def test_temp_directory_creation():
    temp_dir = TempDirectory(prefix="test_prefix_")
    assert temp_dir.path.exists()
    assert temp_dir.path.is_dir()
    assert f"test_prefix_" in temp_dir.path.name


def test_temp_directory_cleanup_removes_dir():
    temp_dir = TempDirectory()
    assert temp_dir.path.exists()

    temp_dir.cleanup()
    assert not temp_dir.path.exists()  # directory should be removed


def test_cleanup_logs_info(caplog):
    temp_dir = TempDirectory()
    with caplog.at_level(logging.INFO):
        temp_dir.cleanup()
    assert f"Cleaned up temporary folder at: {temp_dir.path}" in caplog.text


def test_atexit_registration():
    with patch("atexit.register") as mock_register:
        tmp = TempDirectory()
        mock_register.assert_called_once_with(tmp.cleanup)


def test_cleanup_called_multiple_times_does_not_raise():
    temp_dir = TempDirectory()
    # Call cleanup multiple times — should not raise
    temp_dir.cleanup()
    temp_dir.cleanup()

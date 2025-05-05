from unittest.mock import patch
from tvm_multi_scan_exporter.aggregated_scan_exporter import TvmMultiScanExporter
from tvm_multi_scan_exporter.configuration import Config
from tvm_multi_scan_exporter.models import ScanHistory


@patch("tvm_multi_scan_exporter.aggregated_scan_exporter.get_tenable_io_from_config")
@patch("tvm_multi_scan_exporter.aggregated_scan_exporter.ScansFetcher")
@patch("tvm_multi_scan_exporter.aggregated_scan_exporter.ScanExporter")
@patch("tvm_multi_scan_exporter.aggregated_scan_exporter.ScanResultTransformer")
@patch("tvm_multi_scan_exporter.aggregated_scan_exporter.TempDirectory")
def test_export_with_no_scan_histories(
        mock_temp_dir, mock_transformer, mock_exporter, mock_fetcher, mock_get_tio
):
    # Mock dependencies
    mock_fetcher.return_value.scan_histories.return_value = []
    mock_temp_dir.return_value.path = "/tmp/fake_path"

    exporter = TvmMultiScanExporter()
    config = Config(scan_name="test-scan", allowed_memory_gb=2)

    # Call the export method
    exporter.export(config)

    # Assertions
    mock_fetcher.return_value.scan_histories.assert_called_once_with(config)
    mock_exporter.return_value.export.assert_not_called()
    mock_transformer.return_value.concatenate_exports.assert_not_called()


@patch("tvm_multi_scan_exporter.aggregated_scan_exporter.get_tenable_io_from_config")
@patch("tvm_multi_scan_exporter.aggregated_scan_exporter.ScansFetcher")
@patch("tvm_multi_scan_exporter.aggregated_scan_exporter.ScanExporter")
@patch("tvm_multi_scan_exporter.aggregated_scan_exporter.ScanResultTransformer")
@patch("tvm_multi_scan_exporter.aggregated_scan_exporter.TempDirectory")
def test_export_with_scan_histories(
        mock_temp_dir, mock_transformer, mock_exporter, mock_fetcher, mock_get_tio
):
    # Mock dependencies
    mock_fetcher.return_value.scan_histories.return_value = [
        ScanHistory(id="1", status="completed", time_end=1234567890)
    ]
    mock_temp_dir.return_value.path = "/tmp/fake_path"
    mock_exporter.return_value.export.return_value = "/tmp/deduped_files"

    exporter = TvmMultiScanExporter()
    config = Config(scan_name="test-scan", allowed_memory_gb=2)

    # Call the export method
    exporter.export(config)

    # Assertions
    mock_fetcher.return_value.scan_histories.assert_called_once_with(config)
    mock_exporter.return_value.export.assert_called_once_with(
        [ScanHistory(id="1", status="completed", time_end=1234567890)], config, "/tmp/fake_path"
    )
    mock_transformer.return_value.concatenate_exports.assert_called_once_with(
        config, "/tmp/deduped_files"
    )


@patch("tvm_multi_scan_exporter.aggregated_scan_exporter.get_config_from_file")
@patch("tvm_multi_scan_exporter.aggregated_scan_exporter.get_tenable_io_from_config")
@patch("tvm_multi_scan_exporter.aggregated_scan_exporter.ScansFetcher")
@patch("tvm_multi_scan_exporter.aggregated_scan_exporter.ScanExporter")
@patch("tvm_multi_scan_exporter.aggregated_scan_exporter.ScanResultTransformer")
@patch("tvm_multi_scan_exporter.aggregated_scan_exporter.TempDirectory")
def test_export_with_config_file(
        mock_temp_dir, mock_transformer, mock_exporter, mock_fetcher, mock_get_tio, mock_get_config
):
    # Mock dependencies
    mock_get_config.return_value = Config(scan_name="test-scan", allowed_memory_gb=2)
    mock_fetcher.return_value.scan_histories.return_value = [
        ScanHistory(id="1", status="completed", time_end=1234567890)
    ]
    mock_temp_dir.return_value.path = "/tmp/fake_path"
    mock_exporter.return_value.export.return_value = "/tmp/deduped_files"

    exporter = TvmMultiScanExporter(config_path="config.toml")

    # Call the export method without passing a config
    exporter.export()

    # Assertions
    mock_get_config.assert_called_once_with("config.toml")
    mock_fetcher.return_value.scan_histories.assert_called_once()
    mock_exporter.return_value.export.assert_called_once()
    mock_transformer.return_value.concatenate_exports.assert_called_once()

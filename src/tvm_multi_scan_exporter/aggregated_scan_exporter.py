import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from tvm_multi_scan_exporter.configuration import Config, get_config_from_file, get_tenable_io_from_config
from tvm_multi_scan_exporter.constants import CONFIG_FILE_PATH, DEFAULT_TVM_URL
from tvm_multi_scan_exporter.files import TempDirectory
from tvm_multi_scan_exporter.models import ScanHistory
from tvm_multi_scan_exporter.scan_exporter import ScanExporter
from tvm_multi_scan_exporter.scan_fetcher import ScansFetcher
from tvm_multi_scan_exporter.scan_transformer import ScanResultTransformer


class TvmMultiScanExporter:

    def __init__(self, config_path: Optional[str] = CONFIG_FILE_PATH, tvm_url: Optional[str] = DEFAULT_TVM_URL):
        tio = get_tenable_io_from_config(tvm_url)

        # this is optional. User provides this explicitly when:
        # 1. they don't want to pass down configs through code
        # 2. they want to specify a different path than the default
        self.config_path = config_path

        self._fetcher = ScansFetcher(tio)
        self._exporter = ScanExporter(tio)
        self._transformer = ScanResultTransformer()

    def export(self, config: Optional[Config] = None):
        """
        Performs an aggregated Scans Export.

        If the user doesn't pass a `config`, we get the same from the config file.

        :param config:
        :return:
        """

        config = config if config else get_config_from_file(self.config_path)
        logging.info(f"Starting to export scans with configuration: {config}")

        # Step: 1 - Get all scan histories to download.
        start_time_for_getting_histories = datetime.now()
        scan_histories: List[ScanHistory] = self._fetcher.scan_histories(config)

        # return early if there are no histories to process.
        if not scan_histories:
            logging.info(
                "No scan histories matched the criteria. Please widen your search window, or pick another scan_name."
            )
            return

        end_time_for_getting_histories = datetime.now()
        logging.info(
            f"Time taken to get scan histories: {end_time_for_getting_histories - start_time_for_getting_histories}"
        )

        # Create a platform independent temp folder. This will be cleaned up on successful or failed script completion.
        temp_folder = TempDirectory().path
        logging.info(f"Temporary folder created at: {temp_folder}")

        # Step: 2 - Export all the scans
        deduped_files_folder: Path = self._exporter.export(scan_histories, config, temp_folder)
        logging.info(
            f"Individual Scan Exports have completed. Deduped files can be found here: {str(deduped_files_folder)}"
        )

        end_time_for_exporting = datetime.now()
        logging.info(
            f"Time taken to get exporting scans: {end_time_for_exporting - end_time_for_getting_histories}"
        )

        # Step: 3 - Concatenate Scans
        self._transformer.concatenate_exports(config, deduped_files_folder)

        end_time_for_concatenation = datetime.now()
        logging.info(
            f"Time taken to get exporting scans: {end_time_for_concatenation - end_time_for_exporting}."
            f"Total time taken for the export: {end_time_for_concatenation - start_time_for_getting_histories}"
        )

import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from pathlib import Path
from typing import List

from tenable.io import TenableIO

from tvm_multi_scan_exporter.configuration import Config
from tvm_multi_scan_exporter.constants import DOWNLOAD_FOLDER_NAME, DEDUP_FOLDER_NAME, \
    DEFAULT_MAX_RETRIES_FOR_SCAN_EXPORT, DEFAULT_RETRY_DELAY_SECONDS
from tvm_multi_scan_exporter.deduplicator import dedup_scan_export
from tvm_multi_scan_exporter.models import ScanHistory
from tvm_multi_scan_exporter.util import file_size_in_mb


class ScanExporter:
    def __init__(self, tio: TenableIO):
        self._tio = tio

    def export(self, scan_histories: List[ScanHistory], config: Config, temp_folder: Path) -> Path:
        """
        Exports all the scans in the given list.

        :param temp_folder:
        :param config:
        :param scan_histories:
        :return:
        """

        try:
            # create the downloads folder inside the temp folder
            downloads_folder: Path = temp_folder / DOWNLOAD_FOLDER_NAME
            downloads_folder.mkdir(parents=True, exist_ok=True)

            # create the de-duped scans folder inside the temp folder
            deduped_scans_folder: Path = temp_folder / DEDUP_FOLDER_NAME
            deduped_scans_folder.mkdir(parents=True, exist_ok=True)

            # stores all the futures for deduplication.
            dedup_futures: List[Future] = []

            logging.info(f"Exploratory Log: Total Exports: {len(scan_histories)}")

            # we only start 1 thread for deduplication.
            with ThreadPoolExecutor(max_workers=1) as executor_for_deduplication:

                # for exporting, we have as many threads as the user requested
                with ThreadPoolExecutor(max_workers=config.workers) as executor_for_exporting:

                    # submit all the scans to the export thread pool executor
                    futures = {
                        executor_for_exporting.submit(
                            self._export_scan_and_dedup,
                            scan_history,
                            config,
                            downloads_folder,
                            deduped_scans_folder,
                            executor_for_deduplication
                        ): scan_history for scan_history in scan_histories
                    }

                    # ensure all the exports completed.
                    for export_future in as_completed(futures):
                        try:
                            # each export future will return a future for the dedup job it submitted inside.
                            # we collect it here.
                            dedup_futures.append(export_future.result())
                        except Exception as exception:
                            logging.error("An exception occurred while exporting scan.", exception)
                            raise exception

            # ensure all the dedup jobs completed.
            for dedup_future in dedup_futures:
                dedup_future.result()

            # return where the caller can find the exported, deduped files
            return deduped_scans_folder
        except Exception as e:
            logging.error(f"An exception occurred while performing scan exports: {e}")
            raise

    def _export_scan_and_dedup(
            self,
            scan_history: ScanHistory,
            config: Config,
            download_folder: Path,
            deduped_scans_folder: Path,
            executor_for_deduplication: ThreadPoolExecutor
    ) -> Future:
        """
        Exports the given scan, submits a job for deduplication, and returns that future to the caller.

        :param scan_history:
        :return:
        """

        # we always export in CSV format, and later convert into the desirable format.
        export_file_name: str = f"{scan_history.schedule_uuid}_{scan_history.schedule_id}_{scan_history.id}.csv"

        try:
            export_file: Path = download_folder / export_file_name
            logging.info(f"Exporting {export_file_name}...")

            self._export_scan_with_retries(scan_history, export_file)

            logging.info(
                f"Export of {export_file_name} ({file_size_in_mb(export_file)}MB) "
                f"has completed successfully. Commencing deduplication..."
            )

            dedup_file: Path = deduped_scans_folder / export_file_name
            logging.info(f"De-duplicating {export_file_name} into {dedup_file}...")

            # submit the dedup function to the dedicated executor for deduplication.
            future: Future = executor_for_deduplication.submit(
                dedup_scan_export,
                export_file,
                dedup_file,
                config
            )

            logging.info(f"De-duplicating complete for export: {export_file}...")
            return future

        except Exception as ex:
            logging.error(f"An exception occurred while exporting/de-duplicating {scan_history.schedule_uuid}", ex)
            raise ex

    def _export_scan_with_retries(
            self,
            scan_history: ScanHistory,
            export_file: Path,
            max_retries: int = DEFAULT_MAX_RETRIES_FOR_SCAN_EXPORT,
            retry_delay: int = DEFAULT_RETRY_DELAY_SECONDS
    ) -> None:
        """
        Attempts to export a scan up to `max_retries` times.
        Deletes the partially written export_file on failure.

        :param scan_history:
        :param export_file:
        :param max_retries:
        :param retry_delay:
        """
        for attempt in range(1, max_retries + 1):
            try:
                with open(export_file, "wb") as file:
                    self._tio.scans.export(
                        scan_history.schedule_id,
                        history_id=scan_history.id,
                        fobj=file,
                        format="csv"
                    )
                return  # Success
            except Exception as ex:
                logging.error(
                    f"[Attempt {attempt}] Failed to export scan {scan_history.schedule_uuid} due to the exception {ex}",
                    exc_info=True
                )

                # Clean up the partially written file if it exists
                if os.path.exists(export_file):
                    try:
                        os.remove(export_file)
                    except Exception as e:
                        logging.warning(f"Failed to delete partial file {export_file} {e}", exc_info=True)

                if attempt == max_retries:
                    raise
                time.sleep(retry_delay)

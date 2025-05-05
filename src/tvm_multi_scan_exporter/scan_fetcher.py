import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List

from tenable.io import TenableIO

from tvm_multi_scan_exporter.configuration import Config
from tvm_multi_scan_exporter.constants import ALLOWED_EXPORT_STATES, CURRENT_TIME
from tvm_multi_scan_exporter.models import ScanHistory, Scan


class ScansFetcher:

    def __init__(self, tio: TenableIO):
        self._tio = tio

    def scan_histories(self, config: Config) -> List[ScanHistory]:
        """
        Gets all the scans that need to be exported.

        :param config:
        :return:
        """

        try:
            # get all scans that match the `contains` name criteria
            all_matching_scans: List[Scan] = self._scans_by_name(config)
            logging.info(
                f"{len(all_matching_scans)} scan(s) matched the scan_name filter {config.scan_name}.")

            # if there are scans, look for histories.
            if all_matching_scans:
                # get all the histories for the matching scans
                histories: List[ScanHistory] = self._histories(all_matching_scans, config)
                logging.info(f"Found {len(histories)} scan history(ies) to process.")
                return histories
            else:
                return []
        except Exception as e:
            logging.error(f"An exception occurred while fetching scan histories: {e}")
            raise

    def _scans_by_name(self, config: Config) -> List[Scan]:
        """
        Gets all the scans by name. This applies a case-insensitive `contains` check.
        :return:
        """
        return [
            Scan(
                uuid=scan.get("uuid", None),
                name=scan["name"],
                schedule_uuid=scan["schedule_uuid"],
                id=scan.get("id", scan["id"])
            )
            for scan in self._tio.scans.list()
            if config.scan_name.lower() in scan["name"].lower()
        ]

    def _histories_for_scan(self, scan: Scan, config: Config) -> List[ScanHistory]:
        """
        Gets the histories for the provided scan, and adhere to the conditions provided in the config.

        :param scan:
        :return:
        """
        if not scan.uuid:
            logging.info(
                f"No scan history found for scan: {scan.name} ID: {scan.id} Schedule UUID: {scan.schedule_uuid}"
            )
            return []

        return [
            ScanHistory(
                id=history["id"],
                status=history["status"],
                time_end=history["time_end"],
                schedule_uuid=scan.schedule_uuid,
                schedule_id=scan.id
            )
            for history in self._tio.scans.history(scan.schedule_uuid)
            if history["status"] in ALLOWED_EXPORT_STATES and history["time_end"] > config.window.since and (
                    config.window.upto == CURRENT_TIME or history["time_end"] < config.window.upto
            )
        ]

    def _histories(self, scans: List[Scan], config: Config) -> List[ScanHistory]:
        """
        Get a flattened list of histories of all the scans
        :param scans:
        :return:
        """
        with ThreadPoolExecutor(max_workers=min(8, len(scans))) as executor:
            return [
                history
                for future in as_completed(
                    executor.submit(self._histories_for_scan, scan, config) for scan in scans
                )
                for history in future.result()
            ]

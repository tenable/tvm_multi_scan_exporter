import json
import logging
from datetime import datetime
from pathlib import Path

import duckdb

from tvm_multi_scan_exporter.database import write_to_db
from tvm_multi_scan_exporter.configuration import Config, Csv, Parquet, Json, WriteToDatabase
from tvm_multi_scan_exporter.duck_db import concatenation_sql_string
from tvm_multi_scan_exporter.util import file_size_in_mb


class ScanResultTransformer:

    def __init__(self):
        # format of files we need to be looking for in the dedup folder.
        self._format_to_look: str = "csv"

    @staticmethod
    def _create_json_export(concatenation_query: str, file_name: str):
        """
        Concatenate and export to JSON file.
        """
        result = duckdb.sql(concatenation_query).fetchall()
        columns = [col[0] for col in duckdb.sql(concatenation_query).description]
        rows_as_dicts = [dict(zip(columns, row)) for row in result]
        with open(file_name, "w", encoding="utf-8") as f:
            json.dump(rows_as_dicts, f, indent=2, default=str)

    @staticmethod
    def _log_file_size(file: str):
        logging.info(f"The size of the output file {file} is {file_size_in_mb(Path(file))}MB")

    def concatenate_exports(self, config: Config, deduped_files_folder: Path):
        """
        Concatenate Scans into a single file of the specified format.

        :param config:
        :param deduped_files_folder:
        :return:
        """
        try:
            concatenation_query = concatenation_sql_string(
                config=config,

                # all the CSV files inside the deduped scans folder.
                source_directory_wildcard=f"{deduped_files_folder}/*{self._format_to_look}"
            )

            logging.debug(f"Concatenation Query: {concatenation_query}")

            # Determine the output folder
            output_folder = Path(config.output_location) if config.output_location else Path.cwd()
            output_folder.mkdir(parents=True, exist_ok=True)  # Ensure the folder exists

            # Generate the output file name
            file_name = output_folder / datetime.now().strftime(
                f"tvm_multi_scan_exporter_%Y-%m-%d_%H-%M-%S.{config.export_type.value}"
            )

            match config.export_type:
                case Csv():
                    duckdb.sql(concatenation_query).write_csv(str(file_name))
                    self._log_file_size(str(file_name))
                case Parquet():
                    duckdb.sql(concatenation_query).write_parquet(str(file_name))
                    self._log_file_size(str(file_name))
                case Json():
                    self._create_json_export(concatenation_query, str(file_name))
                    self._log_file_size(str(file_name))
                case _:
                    if isinstance(config.export_type, WriteToDatabase):
                        write_to_db(config.export_type, concatenation_query)
                    else:
                        raise Exception("Unknown format received.")

            logging.info(f"Completed processing the export: {file_name}")

        except Exception as e:
            logging.error(f"An exception occurred while transforming the scan results: {e}")
            raise

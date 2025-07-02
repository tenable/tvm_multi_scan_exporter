import logging
import os
from pathlib import Path

import duckdb
import pandas as pd

from tvm_multi_scan_exporter.duck_db import deduplication_query_string
from tvm_multi_scan_exporter.configuration import Config
from tvm_multi_scan_exporter.util import file_size_in_mb


def dedup_scan_export(source: Path, destination: Path, config: Config):
    """
    Performs deduplication based on criteria. Reads from the `source`, and writes the CSV to the `destination`.
    Also, clears the source file after the fact.

    :param config:
    :param source:
    :param destination:
    :return:
    """
    dedup_query: str = deduplication_query_string(config, source)
    logging.info(f"Generated DuckDB deduplication query for export {str(source)}: \n {dedup_query}")

    try:
        # Execute the DuckDB query
        result = duckdb.sql(dedup_query)

        # Convert the result to a Pandas DataFrame
        df = result.to_df()

        # Save the DataFrame to a CSV file
        df.to_csv(str(destination), index=False)

        logging.info(f"Exploratory Log: Original vs Deduped [{len(pd.read_csv(source))}/{len(pd.read_csv(destination))}] source: {source} Destination: {destination}")

        logging.info(f"Size of deduped file {destination} is {file_size_in_mb(destination)}MB")
        os.remove(source)
    except Exception as e:
        logging.error(f"An exception occurred while performing deduplication: {e}")
        raise

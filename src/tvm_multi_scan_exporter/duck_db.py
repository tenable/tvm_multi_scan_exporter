from pathlib import Path

from tvm_multi_scan_exporter.configuration import Config
from tvm_multi_scan_exporter.constants import ALWAYS_INCLUDE_COLUMNS, DEFAULT_MAX_BY_COLUMNS, DEFAULT_IDENTIFIER_COLUMNS

from tvm_multi_scan_exporter.util import snake_case


def deduplication_query_string(config: Config, source_file: Path) -> str:
    """
    Returns the Duck DB query for deduplication.

    :param source_file:
    :param config:
    :return:
    """
    return f"""
        SET memory_limit = '{config.allowed_memory_gb}GB';
        SELECT
            {_columns_sql_string(config)}
        FROM
            read_csv_auto('{str(source_file)}',  ignore_errors=true,  all_varchar=true, sample_size=-1, strict_mode=false) scans
        WHERE
            "Vulnerability State" != 'Fixed'
        GROUP BY identifier;
    """.strip()


def _columns_sql_string(config: Config) -> str:
    """
    Returns a string repr of columns to be included in the export.

    :param config:
    :return:
    """

    columns = sorted(set(ALWAYS_INCLUDE_COLUMNS + config.columns))
    export_columns = [f'ANY_VALUE("{col}") AS {snake_case(col)}' for col in columns]
    max_columns = [f'MAX("{col}") AS {snake_case(col)}' for col in DEFAULT_MAX_BY_COLUMNS]

    # Construct the identifier column
    identifier_expr = 'md5(concat(' + ', '.join(f'"{col}"' for col in DEFAULT_IDENTIFIER_COLUMNS) + f')) AS identifier'

    return f"""
    {", ".join(export_columns)},
    {", ".join(max_columns)},
    {identifier_expr}
    """.strip()


def concatenation_sql_string(config: Config, source_directory_wildcard: str):
    """
    Returns the Duck DB query for concatenating multiple CSV files into one.

    :param config:
    :param source_directory_wildcard:
    :return:
    """
    return f"""
        SET memory_limit = '{config.allowed_memory_gb}GB';
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

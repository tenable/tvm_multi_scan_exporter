import logging
from urllib.parse import quote_plus, quote

import duckdb
import pyarrow
import sqlalchemy
from sqlalchemy import Engine

from tvm_multi_scan_exporter.configuration import DatabaseEngine, WriteToDatabase
from tvm_multi_scan_exporter.constants import DB_EXCLUDE_COLUMNS


def _db_engine(database_config: WriteToDatabase) -> Engine:
    """
    Constructs and returns a database engine for the requested database.

    :param database_config:
    :return:
    """

    connection_string: str = ""

    match database_config.engine:
        case DatabaseEngine.MsSqlServer:
            params = "{}:%s@".format(database_config.user) \
                     + "{}:{}/".format(database_config.host_address, database_config.port) \
                     + "{}?".format(database_config.database_name) \
                     + "driver={}".format(quote_plus(database_config.engine.value[1])) \
                     + "&MultiSubnetFailover=yes"
            params = params % quote(database_config.password)
            connection_string = "{}://{}".format(database_config.engine.value[0], params)
        case _:
            raise AttributeError(f"Unsupported database engine: {database_config.engine} provided.")

    return sqlalchemy.create_engine(connection_string, fast_executemany=True)


def write_to_db(database_config: WriteToDatabase, concatenation_query: str):
    """

    :param concatenation_query:
    :param database_config:
    :return:
    """
    logging.info("Starting writing to DB process...")

    db_engine: Engine = _db_engine(database_config)
    for batch in duckdb.execute(concatenation_query).fetch_record_batch(database_config.write_batch_size):
        logging.info(f"Writing {batch.num_rows} rows to DB Table: {database_config.table_name}...")
        df = pyarrow.Table.from_batches([batch]).drop_columns(DB_EXCLUDE_COLUMNS).to_pandas()
        df.to_sql(database_config.table_name, db_engine, if_exists="append", index=False, method="multi")

    logging.info(f"Database Push Operation complete.")
